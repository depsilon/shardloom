//! Frame table ownership through the actual admitted provider/session boundary.

use super::*;
use vortex::{buffer::ByteBuffer, encodings::zstd::ZstdArray};

fn owned_primitive(memory: &LiveMemoryPool) -> ArrayRef {
    let values = PrimitiveArray::from_iter((0..64i64).map(|value| value * 7 - 100));
    let data = ZstdData::from_primitive_without_dict(
        &values,
        0,
        8,
        &mut session(memory).create_execution_ctx(),
    )
    .unwrap();
    Zstd::try_new(values.dtype().clone(), data, Validity::NonNullable)
        .unwrap()
        .into_array()
}

fn deserialize(
    input: &ArrayRef,
    metadata: &[u8],
    buffers: &[BufferHandle],
    session: &VortexSession,
) -> VortexResult<ZstdArray> {
    ZstdArray::try_from_parts(Zstd.deserialize(
        input.dtype(),
        input.len(),
        metadata,
        buffers,
        &Vec::<ArrayRef>::new(),
        session,
    )?)
}

#[test]
fn native_zstd_metadata_clones_slices_and_casts_share_credit_at_a_full_grant() {
    let memory = LiveMemoryPool::new(64 << 20).unwrap();
    let input = owned_primitive(&memory);
    let retained = memory.snapshot().reserved_bytes;
    let table = input.as_::<Zstd>().data();
    let address = table.frame_metadata().as_ptr();
    let metadata = u64::try_from(table.metadata_capacity_bytes().unwrap()).unwrap();
    assert!(retained > metadata && metadata > 0);
    let competitor = memory
        .reserve(memory.snapshot().limit_bytes - retained)
        .unwrap();
    let cloned = table.clone();
    let slice = input.slice(3..61).unwrap().slice(2..8).unwrap();
    let cast = slice
        .cast(DType::Primitive(PType::I64, Nullability::Nullable))
        .unwrap();
    let cast_back = cast
        .cast(DType::Primitive(PType::I64, Nullability::NonNullable))
        .unwrap();
    for array in [&slice, &cast, &cast_back] {
        assert_eq!(array.as_::<Zstd>().frame_metadata().as_ptr(), address);
        assert_eq!(array.as_::<Zstd>().slice_start(), 5);
        assert_eq!(array.as_::<Zstd>().slice_stop(), 11);
    }
    assert_eq!(cloned.frame_metadata().as_ptr(), address);
    assert_eq!(
        memory.snapshot().reserved_bytes,
        memory.snapshot().limit_bytes
    );
    assert_eq!(memory.snapshot().denied_reservations, 0);
    drop(competitor);
    let output = cast_back
        .execute::<PrimitiveArray>(&mut VortexSession::default().create_execution_ctx())
        .unwrap();
    assert_eq!(output.as_slice::<i64>(), &[-65, -58, -51, -44, -37, -30]);
    drop((input, slice, cast, output));
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    drop(cloned);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_metadata_deserialization_is_admitted_and_survives_session_close() {
    let input = primitive(&PrimitiveArray::from_iter(0i64..33));
    let control = VortexSession::default();
    let metadata = Zstd::serialize(input.as_::<Zstd>(), &control)
        .unwrap()
        .unwrap();
    let buffers = input.buffer_handles();
    let capacity = u64::try_from(input.as_::<Zstd>().metadata_capacity_bytes().unwrap()).unwrap();
    let denied = LiveMemoryPool::new(capacity - 1).unwrap();
    let error = deserialize(&input, &metadata, &buffers, &session(&denied)).unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(denied.snapshot().peak_reserved_bytes, 0);
    assert_eq!(denied.snapshot().reserved_bytes, 0);

    let memory = LiveMemoryPool::new(capacity).unwrap();
    let consumer = session(&memory);
    let reopened = deserialize(&input, &metadata, &buffers, &consumer)
        .unwrap()
        .into_array();
    assert_eq!(memory.snapshot().reserved_bytes, capacity);
    assert_eq!(
        Zstd::serialize(reopened.as_::<Zstd>(), &control)
            .unwrap()
            .unwrap(),
        metadata
    );
    let slice = reopened.slice(7..12).unwrap();
    assert_eq!(
        slice.as_::<Zstd>().frame_metadata().as_ptr(),
        reopened.as_::<Zstd>().frame_metadata().as_ptr()
    );
    drop((consumer, reopened));
    assert_eq!(memory.snapshot().reserved_bytes, capacity);
    let output = slice
        .execute::<PrimitiveArray>(&mut control.create_execution_ctx())
        .unwrap();
    assert_eq!(output.as_slice::<i64>(), &[7, 8, 9, 10, 11]);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_metadata_deserialization_refunds_checked_constructor_failure() {
    let input = primitive(&PrimitiveArray::from_iter(0i64..33));
    let metadata = Zstd::serialize(input.as_::<Zstd>(), &VortexSession::default())
        .unwrap()
        .unwrap();
    let mut buffers = input.buffer_handles();
    buffers[0] = BufferHandle::new_host(ByteBuffer::from(vec![0u8]));
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let error = deserialize(&input, &metadata, &buffers, &session(&memory)).unwrap_err();
    assert!(error.to_string().contains("zstd frame"), "{error}");
    assert!(memory.snapshot().peak_reserved_bytes > 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let valid = input.buffer_handles();
    drop(deserialize(&input, &metadata, &valid, &session(&memory)).unwrap());
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_metadata_selected_table_uses_only_selected_capacity_and_refunds_denial() {
    let input = primitive(&PrimitiveArray::from_iter(0i64..64));
    let data = input.as_::<Zstd>().data();
    let original = data.frame_metadata().as_ptr();
    let bytes = u64::try_from(data.metadata_capacity_bytes().unwrap() / 4).unwrap();
    let memory = LiveMemoryPool::new(bytes).unwrap();
    let consumer = session(&memory);
    let selected = data
        .select_frames_in(&[0, 7], &consumer.host_allocator())
        .unwrap();
    assert_eq!(
        u64::try_from(selected.metadata_capacity_bytes().unwrap()).unwrap(),
        bytes
    );
    assert_eq!(selected.len(), 16);
    assert_ne!(selected.frame_metadata().as_ptr(), original);
    assert_eq!(memory.snapshot().reserved_bytes, bytes);
    let error = data
        .select_frames_in(&[0], &consumer.host_allocator())
        .unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().reserved_bytes, bytes);
    let array = Zstd::try_new(input.dtype().clone(), selected, Validity::NonNullable).unwrap();
    let output = array
        .into_array()
        .execute::<PrimitiveArray>(&mut VortexSession::default().create_execution_ctx())
        .unwrap();
    assert_eq!(
        output.as_slice::<i64>(),
        &[0, 1, 2, 3, 4, 5, 6, 7, 56, 57, 58, 59, 60, 61, 62, 63]
    );
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    for indices in [&[][..], &[7, 0], &[0, 0], &[8], &[usize::MAX]] {
        assert!(
            data.select_frames_in(indices, &consumer.host_allocator())
                .is_err()
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    assert_eq!(
        memory.snapshot().denied_reservations,
        1,
        "invalid indices must not ask for credit"
    );
}

#[test]
fn native_zstd_metadata_buffer_replacement_uses_the_explicit_receiving_allocator() {
    let input = primitive(&PrimitiveArray::from_iter(0i64..33))
        .slice(5..29)
        .unwrap();
    let capacity = u64::try_from(input.as_::<Zstd>().metadata_capacity_bytes().unwrap()).unwrap();
    let denied = LiveMemoryPool::new(capacity - 1).unwrap();
    let error = Zstd::with_buffers_in(
        input.as_::<Zstd>(),
        &input.buffer_handles(),
        &session(&denied).host_allocator(),
    )
    .err()
    .unwrap();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(denied.snapshot().reserved_bytes, 0);
    let receiving = LiveMemoryPool::new(capacity).unwrap();
    let parts = Zstd::with_buffers_in(
        input.as_::<Zstd>(),
        &input.buffer_handles(),
        &session(&receiving).host_allocator(),
    )
    .unwrap();
    let output = ZstdArray::try_from_parts(parts).unwrap().into_array();
    assert_eq!(output.as_::<Zstd>().slice_start(), 5);
    assert_eq!(output.as_::<Zstd>().slice_stop(), 29);
    assert_ne!(
        output.as_::<Zstd>().frame_metadata().as_ptr(),
        input.as_::<Zstd>().frame_metadata().as_ptr()
    );
    assert_eq!(receiving.snapshot().reserved_bytes, capacity);
    let decoded = output
        .execute::<PrimitiveArray>(&mut VortexSession::default().create_execution_ctx())
        .unwrap();
    assert_eq!(decoded.as_slice::<i64>(), (5i64..29).collect::<Vec<_>>());
    drop(decoded);
    assert_eq!(receiving.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_metadata_raw_parts_export_keeps_its_explicit_caller_owned_boundary() {
    let memory = LiveMemoryPool::new(64 << 20).unwrap();
    let input = owned_primitive(&memory);
    let data = input.as_::<Zstd>().data().clone();
    let address = data.frame_metadata().as_ptr();
    let capacity = u64::try_from(data.metadata_capacity_bytes().unwrap()).unwrap();
    let retained = memory.snapshot().reserved_bytes;
    // Shared export copies raw vectors and leaves the original reservation intact.
    let copied = data.clone().into_parts(Validity::NonNullable);
    assert_ne!(copied.metadata.frames.as_ptr(), address);
    assert_eq!(memory.snapshot().reserved_bytes, retained);
    drop((input, copied));
    // Unique export moves the vectors into the explicit caller-owned boundary.
    let moved = data.into_parts(Validity::NonNullable);
    assert_eq!(moved.metadata.frames.as_ptr(), address);
    assert_eq!(memory.snapshot().reserved_bytes, retained - capacity);
    let raw = ZstdData::new(moved.dictionary, moved.frames, moved.metadata, moved.n_rows);
    let cloned = raw.clone();
    assert_eq!(
        raw.frame_metadata().as_ptr(),
        cloned.frame_metadata().as_ptr()
    );
    drop(raw);
    assert_eq!(memory.snapshot().reserved_bytes, retained - capacity);
    drop(cloned);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
