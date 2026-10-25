//! Admission at the Arrow-to-native ingest boundary. Every imported buffer is
//! copied into the native session allocator: Arrow's public capacity does not
//! establish the retained allocation size of a custom owner. Original source
//! owners, reader internals and provider allocations bypassing that allocator
//! remain outside this scope. Native copies own credits for their full lifetime;
//! no arbitrary native `ArrayRef` is charged after creation.

use std::sync::Arc;

use arrow_array::{Array as _, ArrayRef, BooleanArray, RecordBatch, cast::AsArray as _};
use shardloom_core::{Result, ShardLoomError};
use vortex::array::memory::HostAllocator;
use vortex::buffer::Alignment;

use super::stage_timing::ArrowOwnershipCounts;

#[derive(Default)]
struct CopyObservation {
    counts: ArrowOwnershipCounts,
    // The values keep every observed region alive until this synchronous batch
    // admission finishes. Addresses alone can be recycled (including temporary
    // buffers synthesized by Array::to_data) and cannot identify repeated work.
    held: std::collections::HashMap<(usize, usize), arrow_buffer::Buffer>,
}

impl CopyObservation {
    fn observe(&mut self, buffer: &arrow_buffer::Buffer) -> Result<()> {
        let bytes = u64::try_from(buffer.len())
            .map_err(|_| ownership_error("Arrow observed buffer length overflow"))?;
        let charge = buffer_copy_allocation_bytes(buffer.len())?;
        self.counts.buffer_regions = self
            .counts
            .buffer_regions
            .checked_add(1)
            .ok_or_else(|| ownership_error("Arrow observed region count overflow"))?;
        self.counts.copied_buffer_bytes = self
            .counts
            .copied_buffer_bytes
            .checked_add(bytes)
            .ok_or_else(|| ownership_error("Arrow observed copied bytes overflow"))?;
        self.counts.copied_allocation_bytes = self
            .counts
            .copied_allocation_bytes
            .checked_add(charge)
            .ok_or_else(|| ownership_error("Arrow observed allocation charge overflow"))?;
        match self.held.entry((buffer.as_ptr().addr(), buffer.len())) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(buffer.clone());
            }
            std::collections::hash_map::Entry::Occupied(_) => {
                self.counts.repeated_buffer_regions = self
                    .counts
                    .repeated_buffer_regions
                    .checked_add(1)
                    .ok_or_else(|| ownership_error("Arrow repeated region count overflow"))?;
                self.counts.repeated_buffer_bytes = self
                    .counts
                    .repeated_buffer_bytes
                    .checked_add(bytes)
                    .ok_or_else(|| ownership_error("Arrow repeated buffer bytes overflow"))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
use crate::owned_buffers::ReservedHostAllocator;

/// Exact allocation charges for the copied buffer regions, including alignment
/// headroom and aligned empty buffers. This does not size original Arrow owners.
pub(crate) fn batch_copy_allocation_bytes(batch: &RecordBatch) -> Result<u64> {
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
        .checked_add(Alignment::DEFAULT_ALIGNMENT.as_usize())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| ownership_error("Arrow copy allocation overflow"))
}

pub(crate) fn copy_batch(batch: RecordBatch, allocator: &dyn HostAllocator) -> Result<RecordBatch> {
    copy_batch_observed(batch, allocator, None)
}

/// Observe actual copies made by the existing credited allocator, without
/// changing ownership admission. No original owner survives the returned batch.
pub(crate) fn copy_batch_profiled(
    batch: RecordBatch,
    allocator: &crate::owned_buffers::ReservedHostAllocator,
) -> Result<(RecordBatch, ArrowOwnershipCounts)> {
    let mut observation = CopyObservation::default();
    let retained = copy_batch_observed(batch, allocator, Some(&mut observation))?;
    Ok((retained, observation.counts))
}

fn copy_batch_observed(
    batch: RecordBatch,
    allocator: &dyn HostAllocator,
    mut observation: Option<&mut CopyObservation>,
) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(|array| copy_array(array, allocator, observation.as_deref_mut()))
        .collect::<Result<Vec<_>>>()?;
    let schema = batch.schema();
    let rows = batch.num_rows();
    // Only newly admitted copies survive this boundary. The optional observer
    // retains original regions until validation finishes to prevent address
    // reuse from masquerading as repeated copy work.
    drop(batch);
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(|error| ownership_error(&format!("Arrow retained batch validation failed: {error}")))
}

fn copy_array(
    array: &ArrayRef,
    allocator: &dyn HostAllocator,
    mut observation: Option<&mut CopyObservation>,
) -> Result<ArrayRef> {
    let data = array.to_data();
    // Preserve the provider's Arrow buffer layout while replacing its owners.
    let mut buffers = data.buffers().to_vec();
    for buffer in &mut buffers {
        // Pinned Arrow 59.3 exposes the custom owner's visible byte length as
        // capacity, so neither capacity nor pointer identity proves ownership.
        // Copy every referenced region. Empty typed buffers still need an
        // aligned pointer: retain a one-byte allocation behind an empty slice.
        let length = buffer.len();
        if let Some(observation) = observation.as_deref_mut() {
            observation.observe(buffer)?;
        }
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
            let retained = copy_array(&bits, allocator, observation.as_deref_mut())?;
            Ok(retained.as_boolean().values().clone().into())
        })
        .transpose()?;
    let children = data
        .child_data()
        .iter()
        .map(|child| {
            copy_array(
                &arrow_array::make_array(child.clone()),
                allocator,
                observation.as_deref_mut(),
            )
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

/// Copied bytes at this boundary, including validity, offsets and every referenced
/// child region. Unlike the allocation charge, this excludes alignment headroom.
#[cfg(all(feature = "vortex-local-primitives", unix))]
pub(crate) fn batch_copy_buffer_bytes(batch: &RecordBatch) -> Result<u64> {
    fn array_bytes(array: &ArrayRef) -> Result<u64> {
        let data = array.to_data();
        let mut bytes = 0_u64;
        for buffer in data.buffers() {
            bytes = bytes
                .checked_add(buffer.len() as u64)
                .ok_or_else(|| ownership_error("Arrow copied buffer size overflow"))?;
        }
        if let Some(nulls) = data.nulls() {
            bytes = bytes
                .checked_add(nulls.buffer().len() as u64)
                .ok_or_else(|| ownership_error("Arrow copied validity size overflow"))?;
        }
        for child in data.child_data() {
            bytes = bytes
                .checked_add(array_bytes(&arrow_array::make_array(child.clone()))?)
                .ok_or_else(|| ownership_error("Arrow copied child size overflow"))?;
        }
        Ok(bytes)
    }
    batch.columns().iter().try_fold(0_u64, |bytes, array| {
        bytes
            .checked_add(array_bytes(array)?)
            .ok_or_else(|| ownership_error("Arrow copied batch size overflow"))
    })
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
    fn string_views_preserve_sliced_values_and_buffer_credit_until_last_owner() {
        use arrow_array::builder::StringViewBuilder;
        use vortex::array::{
            VortexSessionExecute as _,
            arrays::{Struct, struct_::StructArrayExt as _},
            dtype::{DType, Nullability},
            scalar::Scalar,
        };

        let values = [
            Some("prefix outside slice"),
            Some("λ"),
            None,
            Some("first outlined value in its own buffer"),
            Some("second outlined value in a different buffer"),
            Some("tail outside slice"),
        ];
        let mut builder = StringViewBuilder::new().with_fixed_block_size(32);
        builder.extend(values);
        let array = builder.finish();
        assert!(array.data_buffers().len() > 1);
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "text",
                DataType::Utf8View,
                true,
            )])),
            vec![Arc::new(array)],
        )
        .unwrap()
        .slice(1, 4);
        let expected = input.clone();
        let bytes = batch_copy_allocation_bytes(&input).unwrap();
        let memory = LiveMemoryPool::new(bytes).unwrap();
        let copied =
            copy_batch(input.clone(), &ReservedHostAllocator::new(memory.clone())).unwrap();
        assert_eq!(copied, expected);
        for (original, retained) in input
            .column(0)
            .to_data()
            .buffers()
            .iter()
            .zip(copied.column(0).to_data().buffers())
        {
            assert_ne!(original.as_ptr(), retained.as_ptr());
        }
        assert_eq!(memory.snapshot().reserved_bytes, bytes);
        let session = vortex::session::VortexSession::default();
        let schema = copied.schema();
        let native = session
            .arrow()
            .from_arrow_record_batch(copied, schema.as_ref())
            .unwrap();
        let structure = native.as_::<Struct>();
        let column = structure.unmasked_field(0);
        assert_eq!(column.dtype(), &DType::Utf8(Nullability::Nullable));
        let mut ctx = session.create_execution_ctx();
        for (row, value) in values[1..5].iter().enumerate() {
            let expected = value.map_or_else(
                || Scalar::null(column.dtype().clone()),
                |value| Scalar::utf8(value, Nullability::Nullable),
            );
            assert_eq!(column.execute_scalar(row, &mut ctx).unwrap(), expected);
        }
        let slice = native.slice(2..3).unwrap();
        drop(native);
        assert!(memory.snapshot().reserved_bytes > 0);
        assert!(memory.snapshot().reserved_bytes <= bytes);
        drop(slice);
        assert_eq!(memory.snapshot().reserved_bytes, 0);

        // Reject after copying some buffers, then prove partial ownership drains.
        let denied = LiveMemoryPool::new(bytes - 1).unwrap();
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
            1 + Alignment::DEFAULT_ALIGNMENT.as_usize() as u64
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

    #[test]
    fn observed_copies_count_shared_values_and_validity_without_reusing_them() {
        let shared: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), None, Some(-3)]));
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("left", DataType::Int64, true),
                Field::new("right", DataType::Int64, true),
            ])),
            vec![Arc::clone(&shared), shared],
        )
        .unwrap();
        let expected = input.clone();
        let charge = batch_copy_allocation_bytes(&input).unwrap();
        let memory = LiveMemoryPool::new(charge).unwrap();
        let (copied, counts) =
            copy_batch_profiled(input, &ReservedHostAllocator::new(memory.clone())).unwrap();
        assert_eq!(copied, expected);
        assert_eq!(counts.buffer_regions, 4);
        assert_eq!(counts.repeated_buffer_regions, 2);
        assert_eq!(counts.copied_buffer_bytes, 2 * (3 * 8 + 1));
        assert_eq!(counts.repeated_buffer_bytes, 3 * 8 + 1);
        assert_eq!(counts.copied_allocation_bytes, charge);
        assert_eq!(memory.snapshot().reserved_bytes, charge);
        for index in 0..2 {
            assert_ne!(
                copied.column(index).to_data().buffers()[0].as_ptr(),
                expected.column(index).to_data().buffers()[0].as_ptr()
            );
        }
        assert_ne!(
            copied.column(0).to_data().buffers()[0].as_ptr(),
            copied.column(1).to_data().buffers()[0].as_ptr()
        );
        drop(copied);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn observed_regions_require_the_same_visible_length_and_hold_their_owner() {
        use std::sync::atomic::{AtomicBool, Ordering};
        struct Owner(Arc<AtomicBool>, [u8; 32]);
        impl AsRef<[u8]> for Owner {
            fn as_ref(&self) -> &[u8] {
                &self.1
            }
        }
        impl Drop for Owner {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let buffer: arrow_buffer::Buffer =
            bytes::Bytes::from_owner(Owner(Arc::clone(&dropped), [7; 32])).into();
        let mut observer = CopyObservation::default();
        observer.observe(&buffer).unwrap();
        observer.observe(&buffer.slice_with_length(0, 16)).unwrap();
        observer.observe(&buffer).unwrap();
        drop(buffer);
        assert!(!dropped.load(Ordering::SeqCst));
        assert_eq!(observer.counts.buffer_regions, 3);
        assert_eq!(observer.counts.copied_buffer_bytes, 80);
        assert_eq!(observer.counts.repeated_buffer_regions, 1);
        assert_eq!(observer.counts.repeated_buffer_bytes, 32);
        drop(observer);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn failed_observed_copy_releases_partial_credits_and_the_hidden_source_owner() {
        use std::sync::atomic::{AtomicBool, Ordering};
        struct Hidden(Arc<AtomicBool>, Vec<u8>);
        impl AsRef<[u8]> for Hidden {
            fn as_ref(&self) -> &[u8] {
                &self.1[..1]
            }
        }
        impl Drop for Hidden {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let data = BooleanArray::from(vec![true, false]).to_data();
        let buffer: arrow_buffer::Buffer =
            bytes::Bytes::from_owner(Hidden(Arc::clone(&dropped), vec![1; 65536])).into();
        let array =
            arrow_array::make_array(data.into_builder().buffers(vec![buffer]).build().unwrap());
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Boolean, false),
                Field::new("b", DataType::Boolean, false),
            ])),
            vec![Arc::clone(&array), array],
        )
        .unwrap();
        let memory = LiveMemoryPool::new(buffer_copy_allocation_bytes(1).unwrap()).unwrap();
        assert!(copy_batch_profiled(input, &ReservedHostAllocator::new(memory.clone())).is_err());
        assert!(memory.snapshot().peak_reserved_bytes > 0);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert!(dropped.load(Ordering::SeqCst));
    }
}
