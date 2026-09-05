//! Admission at the Arrow-to-native ingest boundary. Every imported buffer is
//! copied into the native session allocator: Arrow's public capacity does not
//! establish the retained allocation size of a custom owner. Original source
//! owners, reader internals and provider allocations bypassing that allocator
//! remain outside this scope. Native copies own credits for their full lifetime;
//! no arbitrary native `ArrayRef` is charged after creation.

use std::sync::Arc;

use arrow_array::{Array as _, ArrayRef, BooleanArray, RecordBatch, cast::AsArray as _};
use shardloom_core::{Result, ShardLoomError};
use vortex::array::memory::HostAllocator as _;
use vortex::buffer::Alignment;

use crate::owned_buffers::ReservedHostAllocator;

/// Exact allocation charges for the copied buffer regions, including alignment
/// headroom and aligned empty buffers. This does not size original Arrow owners.
pub(super) fn batch_copy_allocation_bytes(batch: &RecordBatch) -> Result<u64> {
    batch.columns().iter().try_fold(0_u64, |sum, array| {
        sum.checked_add(array_copy_allocation_bytes(array)?)
            .ok_or_else(|| ownership_error("Arrow intake size overflow"))
    })
}

fn array_copy_allocation_bytes(array: &ArrayRef) -> Result<u64> {
    let data = array.to_data();
    let mut sum = 0_u64;
    for buffer in data.buffers() {
        sum = sum
            .checked_add(buffer_copy_allocation_bytes(buffer.len())?)
            .ok_or_else(|| ownership_error("Arrow intake size overflow"))?;
    }
    if let Some(nulls) = data.nulls() {
        sum = sum
            .checked_add(buffer_copy_allocation_bytes(nulls.buffer().len())?)
            .ok_or_else(|| ownership_error("Arrow validity size overflow"))?;
    }
    for child in data.child_data() {
        sum = sum
            .checked_add(array_copy_allocation_bytes(&arrow_array::make_array(
                child.clone(),
            ))?)
            .ok_or_else(|| ownership_error("Arrow child size overflow"))?;
    }
    Ok(sum)
}

fn buffer_copy_allocation_bytes(length: usize) -> Result<u64> {
    length
        .max(1)
        .checked_add(*Alignment::DEFAULT_ALIGNMENT)
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| ownership_error("Arrow copy allocation overflow"))
}

pub(super) fn copy_batch(
    batch: RecordBatch,
    allocator: &ReservedHostAllocator,
) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(|array| copy_array(array, allocator))
        .collect::<Result<Vec<_>>>()?;
    let schema = batch.schema();
    let rows = batch.num_rows();
    // Only newly admitted copies are retained beyond this boundary. Release
    // this input batch's owners before validating the replacement batch.
    drop(batch);
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(|error| ownership_error(&format!("Arrow retained batch validation failed: {error}")))
}

fn copy_array(array: &ArrayRef, allocator: &ReservedHostAllocator) -> Result<ArrayRef> {
    let data = array.to_data();
    // The inferred Arrow Buffer type keeps this bridge on the existing
    // arrow-array/bytes dependency surfaces, without a new direct dependency.
    let mut buffers = data.buffers().to_vec();
    for buffer in &mut buffers {
        // Pinned Arrow 58.3 exposes the custom owner's visible byte length as
        // capacity, so neither capacity nor pointer identity proves ownership.
        // Copy every referenced region. Empty typed buffers still need an
        // aligned pointer: retain a one-byte allocation behind an empty slice.
        let length = buffer.len();
        let mut admitted = allocator
            .allocate(length.max(1), Alignment::DEFAULT_ALIGNMENT)
            .map_err(|error| ownership_error(&error.to_string()))?;
        admitted.as_mut_slice()[..length].copy_from_slice(buffer.as_slice());
        *buffer = bytes::Bytes::from_owner(admitted.freeze()).into();
        if length == 0 {
            *buffer = buffer.slice_with_length(0, 0);
        }
    }
    let nulls = data
        .nulls()
        .map(|nulls| -> Result<_> {
            // A BooleanArray round trip preserves independent validity bit
            // offsets, including slices whose value offset differs from it.
            let bits: ArrayRef = Arc::new(BooleanArray::new(nulls.inner().clone(), None));
            let retained = copy_array(&bits, allocator)?;
            Ok(retained.as_boolean().values().clone().into())
        })
        .transpose()?;
    let children = data
        .child_data()
        .iter()
        .map(|child| {
            copy_array(&arrow_array::make_array(child.clone()), allocator)
                .map(|child| child.to_data())
        })
        .collect::<Result<Vec<_>>>()?;
    let data = data
        .into_builder()
        .buffers(buffers)
        .nulls(nulls)
        .child_data(children)
        .build()
        .map_err(|error| {
            ownership_error(&format!("Arrow retained array validation failed: {error}"))
        })?;
    Ok(arrow_array::make_array(data))
}

fn ownership_error(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native ingest ownership: {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use shardloom_exec::live_memory::LiveMemoryPool;
    use vortex::{VortexSessionDefault as _, arrow::ArrowSessionExt as _};

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("renamed_id", DataType::Int64, true),
                Field::new("renamed_text", DataType::Utf8, true),
                Field::new("renamed_flag", DataType::Boolean, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(1_i64 << 60),
                    None,
                    Some(-7),
                    Some(5),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("λ"),
                    Some(""),
                    None,
                    Some("tail"),
                ])),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    None,
                    Some(false),
                    Some(true),
                ])),
            ],
        )
        .unwrap()
        .slice(1, 2)
    }

    #[test]
    fn native_copies_and_imported_slices_retain_allocation_credit() {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let input = batch();
        let expected = input.clone();
        let bytes = batch_copy_allocation_bytes(&input).unwrap();
        let pointer = input.column(0).to_data().buffers()[0].as_ptr();
        let retained = copy_batch(input, &ReservedHostAllocator::new(memory.clone())).unwrap();
        assert_eq!(&retained, &expected);
        assert_ne!(retained.column(0).to_data().buffers()[0].as_ptr(), pointer);
        assert_eq!(memory.snapshot().reserved_bytes, bytes);
        let session = vortex::session::VortexSession::default();
        let schema = retained.schema();
        let native = session
            .arrow()
            .from_arrow_record_batch(retained, schema.as_ref())
            .unwrap();
        let imported_bytes = memory.snapshot().reserved_bytes;
        assert!(imported_bytes > 0 && imported_bytes <= bytes);
        let slice = native.slice(0..1).unwrap();
        drop(native);
        assert!(memory.snapshot().reserved_bytes > 0);
        assert!(memory.snapshot().reserved_bytes <= imported_bytes);
        drop(slice);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn repeated_intake_copies_and_denial_releases_every_credit() {
        let input = batch();
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let external =
            copy_batch(input.clone(), &ReservedHostAllocator::new(memory.clone())).unwrap();
        let original_pointer = external.column(0).to_data().buffers()[0].as_ptr();
        let copy_memory = LiveMemoryPool::new(1 << 20).unwrap();
        let copied =
            copy_batch(external, &ReservedHostAllocator::new(copy_memory.clone())).unwrap();
        assert_eq!(copied, input);
        assert_ne!(
            copied.column(0).to_data().buffers()[0].as_ptr(),
            original_pointer
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert!(copy_memory.snapshot().reserved_bytes > 0);
        drop(copied);
        assert_eq!(copy_memory.snapshot().reserved_bytes, 0);
        // Admit the first buffer, then reject a later allocation. Every earlier
        // successful allocation must release its credit on the error path.
        let denied = LiveMemoryPool::new(512).unwrap();
        assert!(copy_batch(input, &ReservedHostAllocator::new(denied.clone())).is_err());
        assert!(denied.snapshot().peak_reserved_bytes > 0);
        assert_eq!(denied.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn positive_custom_capacity_does_not_retain_a_large_hidden_owner() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct HiddenOwner {
            backing: Vec<u8>,
            dropped: Arc<AtomicBool>,
        }
        impl AsRef<[u8]> for HiddenOwner {
            fn as_ref(&self) -> &[u8] {
                &self.backing[..1]
            }
        }
        impl Drop for HiddenOwner {
            fn drop(&mut self) {
                self.dropped.store(true, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let values = BooleanArray::from(vec![true, false, true, false]);
        let data = values.to_data();
        let mut buffers = data.buffers().to_vec();
        buffers[0] = bytes::Bytes::from_owner(HiddenOwner {
            backing: vec![0b0101; 65536],
            dropped: Arc::clone(&dropped),
        })
        .into();
        // Pinned Arrow's custom capacity reports the exposed byte, despite the
        // owner retaining a much larger allocation. No zero-capacity sentinel.
        assert_eq!(buffers[0].capacity(), 1);
        let original_pointer = buffers[0].as_ptr();
        let array = arrow_array::make_array(data.into_builder().buffers(buffers).build().unwrap());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "flag",
            DataType::Boolean,
            false,
        )]));
        let input = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
        let memory = LiveMemoryPool::new(1024).unwrap();
        let copied = copy_batch(input, &ReservedHostAllocator::new(memory.clone())).unwrap();
        assert!(dropped.load(Ordering::SeqCst));
        assert_ne!(
            copied.column(0).to_data().buffers()[0].as_ptr(),
            original_pointer
        );
        assert_eq!(
            copied,
            RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap()
        );
        assert_eq!(
            memory.snapshot().reserved_bytes,
            1 + *Alignment::DEFAULT_ALIGNMENT as u64
        );
        drop(copied);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn empty_numeric_and_nullable_buffers_keep_alignment_without_opaque_owners() {
        let input = RecordBatch::new_empty(batch().schema());
        let memory = LiveMemoryPool::new(65536).unwrap();
        let retained =
            copy_batch(input.clone(), &ReservedHostAllocator::new(memory.clone())).unwrap();
        assert_eq!(retained, input);
        for buffer in retained.column(0).to_data().buffers() {
            assert_eq!(buffer.as_ptr().align_offset(std::mem::align_of::<i64>()), 0);
        }
        let schema = retained.schema();
        let native = vortex::session::VortexSession::default()
            .arrow()
            .from_arrow_record_batch(retained, schema.as_ref())
            .unwrap();
        assert_eq!(native.len(), 0);
        drop(native);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
