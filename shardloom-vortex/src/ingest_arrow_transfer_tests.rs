use super::*;
use arrow_array::{Int64Array, StringArray, StructArray};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_schema::{DataType, Field, Schema};
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{VortexSessionDefault as _, arrow::ArrowSessionExt as _};

fn primitive_batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1_i64 << 60, -7, 0, 42]))],
    )
    .unwrap()
}

#[test]
fn unique_standard_owner_transfers_and_keeps_credit_after_session_drop() {
    let input = primitive_batch();
    let pointer = input.column(0).to_data().buffers()[0].as_ptr();
    let memory = LiveMemoryPool::new(4096).unwrap();
    let allocator = ReservedHostAllocator::new(memory.clone());
    let (retained, counts) =
        admit_batch_profiled(input, &allocator, ArrowOwnershipPolicy::ControlledParquet).unwrap();
    drop(allocator);
    assert_eq!(retained, primitive_batch());
    assert_eq!(retained.column(0).to_data().buffers()[0].as_ptr(), pointer);
    assert_eq!(counts.transferred_buffer_regions, 1);
    assert_eq!(counts.transferred_buffer_bytes, 32);
    assert_eq!(counts.transferred_allocation_bytes, 32);
    assert_eq!(counts.copied_buffer_bytes, 0);
    assert_eq!(memory.snapshot().reserved_bytes, 32);
    let session = vortex::session::VortexSession::default();
    let schema = retained.schema();
    let native = session
        .arrow()
        .from_arrow_record_batch(retained, &schema)
        .unwrap();
    drop(session);
    let slice = native.slice(1..3).unwrap();
    let clone = slice.clone();
    drop(native);
    drop(slice);
    assert_eq!(memory.snapshot().reserved_bytes, 32);
    std::thread::spawn(move || drop(clone)).join().unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn generic_unique_input_still_copies_and_cannot_reuse_another_sessions_credit() {
    let memory = LiveMemoryPool::new(4096).unwrap();
    let input = primitive_batch();
    let pointer = input.column(0).to_data().buffers()[0].as_ptr();
    let (retained, counts) = admit_batch_profiled(
        input,
        &ReservedHostAllocator::new(memory.clone()),
        ArrowOwnershipPolicy::ValidatedCopy,
    )
    .unwrap();
    assert_ne!(retained.column(0).to_data().buffers()[0].as_ptr(), pointer);
    assert_eq!(counts.transferred_buffer_bytes, 0);
    assert_eq!(counts.copied_buffer_bytes, 32);
    let second = LiveMemoryPool::new(4096).unwrap();
    // A native-credited Bytes owner is custom to Arrow, even when unique. A new
    // controlled-reader admission cannot silently borrow the first pool's credit.
    let (copied, counts) = admit_batch_profiled(
        retained,
        &ReservedHostAllocator::new(second.clone()),
        ArrowOwnershipPolicy::ControlledParquet,
    )
    .unwrap();
    assert_eq!(counts.transferred_buffer_bytes, 0);
    assert_eq!(counts.copied_buffer_bytes, 32);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(copied, primitive_batch());
    drop(copied);
    assert_eq!(second.snapshot().reserved_bytes, 0);
}

#[test]
fn transfer_charges_full_capacity_and_rejects_partial_admission() {
    let mut values = Vec::<i64>::with_capacity(16);
    values.extend([7, -9]);
    let buffer = Buffer::from_vec(values);
    assert_eq!(buffer.len(), 16);
    assert_eq!(buffer.capacity(), 128);
    let memory = LiveMemoryPool::new(128).unwrap();
    let mut observation = CopyObservation::default();
    let retained = admit_buffer(
        buffer,
        Some(8),
        &ReservedHostAllocator::new(memory.clone()),
        &mut observation,
    )
    .unwrap();
    assert_eq!(observation.counts.transferred_allocation_bytes, 128);
    drop(observation);
    let slice = retained.slice_with_length(8, 8);
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 128);
    drop(slice);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![7_i64])),
            Arc::new(Int64Array::from(vec![-9_i64])),
        ],
    )
    .unwrap();
    let denied = LiveMemoryPool::new(12).unwrap();
    let error = admit_batch_profiled(
        input,
        &ReservedHostAllocator::new(denied.clone()),
        ArrowOwnershipPolicy::ControlledParquet,
    )
    .unwrap_err();
    assert!(error.to_string().contains("no fallback execution"));
    assert_eq!(denied.snapshot().peak_reserved_bytes, 8);
    assert_eq!(denied.snapshot().reserved_bytes, 0);
    assert!(denied.snapshot().denied_reservations > 0);
}

#[test]
fn shared_offset_empty_and_oversized_buffers_remain_copies() {
    let shared = Buffer::from_vec(vec![1_u8, 2, 3, 4]);
    let mut large = Vec::with_capacity(4096);
    large.extend([1_u8, 2, 3, 4]);
    let inputs = [
        shared.clone(),
        Buffer::from_vec(vec![0_u8, 1, 2, 3, 4]).slice(1),
        Buffer::from_vec(Vec::<u8>::new()),
        Buffer::from_vec(large),
    ];
    for input in inputs {
        let pointer = input.as_ptr();
        let expected = input.as_slice().to_vec();
        let memory = LiveMemoryPool::new(8192).unwrap();
        let mut observation = CopyObservation::default();
        let retained = admit_buffer(
            input,
            Some(1),
            &ReservedHostAllocator::new(memory.clone()),
            &mut observation,
        )
        .unwrap();
        assert_eq!(retained.as_slice(), expected);
        assert_ne!(retained.as_ptr(), pointer);
        assert_eq!(
            retained
                .as_ptr()
                .align_offset(Alignment::DEFAULT_ALIGNMENT.as_usize()),
            0
        );
        assert_eq!(observation.counts.transferred_buffer_regions, 0);
        assert_eq!(
            observation.counts.copied_buffer_bytes,
            expected.len() as u64
        );
        assert_eq!(
            memory.snapshot().reserved_bytes,
            buffer_copy_allocation_bytes(expected.len()).unwrap()
        );
        drop(observation);
        drop(retained);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    assert_eq!(shared.as_slice(), &[1, 2, 3, 4]);
}

#[test]
fn opaque_hidden_owner_is_released_even_on_controlled_reader_path() {
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
    let input: Buffer = bytes::Bytes::from_owner(HiddenOwner {
        backing: vec![5; 65536],
        dropped: Arc::clone(&dropped),
    })
    .into();
    assert_eq!(input.capacity(), 1);
    let memory = LiveMemoryPool::new(1024).unwrap();
    let mut observation = CopyObservation::default();
    let retained = admit_buffer(
        input,
        Some(1),
        &ReservedHostAllocator::new(memory.clone()),
        &mut observation,
    )
    .unwrap();
    assert_eq!(observation.counts.transferred_buffer_bytes, 0);
    assert_eq!(observation.counts.copied_buffer_bytes, 1);
    drop(observation);
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(retained.as_slice(), &[5]);
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn logical_and_independent_validity_offsets_preserve_values() {
    let values = Buffer::from_vec(vec![10_i64, 20, 30, 40]);
    let nulls = NullBuffer::new(BooleanBuffer::new(
        Buffer::from_vec(vec![0b0000_1010_u8]),
        1,
        2,
    ));
    let data = arrow_data::ArrayData::builder(DataType::Int64)
        .len(2)
        .offset(1)
        .add_buffer(values)
        .nulls(Some(nulls))
        .build()
        .unwrap();
    let memory = LiveMemoryPool::new(4096).unwrap();
    let mut observation = CopyObservation::default();
    let retained = admit_array_data(
        data,
        &ReservedHostAllocator::new(memory.clone()),
        &mut observation,
    )
    .unwrap();
    assert_eq!(observation.counts.transferred_buffer_regions, 0);
    let array = Int64Array::from(retained);
    assert_eq!(array.iter().collect::<Vec<_>>(), vec![Some(20), None]);
    drop(array);
    drop(observation);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn nested_shared_children_and_utf8_survive_admission() {
    fn input() -> RecordBatch {
        let text: ArrayRef = Arc::new(StringArray::from(vec![
            Some("λ"),
            None,
            Some("long unicode 😀 payload after null"),
        ]));
        let children = vec![
            Field::new("first", DataType::Utf8, true),
            Field::new("second", DataType::Utf8, true),
        ];
        let nested = StructArray::new(children.into(), vec![Arc::clone(&text), text], None);
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "nested",
                nested.data_type().clone(),
                false,
            )])),
            vec![Arc::new(nested)],
        )
        .unwrap()
    }
    let expected = input();
    let memory = LiveMemoryPool::new(65536).unwrap();
    let (retained, counts) = admit_batch_profiled(
        input(),
        &ReservedHostAllocator::new(memory.clone()),
        ArrowOwnershipPolicy::ControlledParquet,
    )
    .unwrap();
    assert_eq!(retained, expected);
    assert!(counts.repeated_buffer_regions > 0);
    assert_eq!(counts.transferred_buffer_regions, 0);
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
