use super::*;
use std::sync::Mutex;
use vortex::array::memory::{HostAllocator, WritableHostBuffer};
use vortex::buffer::ByteBuffer;

#[path = "native_provider_zstd_workspace_allocator_tests.rs"]
mod allocator_tests;

#[derive(Debug)]
struct RecordingAllocator {
    inner: crate::owned_buffers::ReservedHostAllocator,
    requests: Mutex<Vec<(usize, usize)>>,
}

impl HostAllocator for RecordingAllocator {
    fn allocate(&self, len: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        self.requests.lock().unwrap().push((len, *alignment));
        self.inner.allocate(len, alignment)
    }
}

fn recorded_session(memory: &LiveMemoryPool) -> (VortexSession, Arc<RecordingAllocator>) {
    let allocator = Arc::new(RecordingAllocator {
        inner: crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
        requests: Mutex::new(vec![]),
    });
    let session = VortexSession::default().with_allocator(allocator.clone());
    install(&session, memory.clone());
    (session, allocator)
}

fn with_dictionary(input: &ArrayRef, dictionary: Vec<u8>) -> ArrayRef {
    let mut parts = input
        .as_::<Zstd>()
        .data()
        .clone()
        .into_parts(input.validity().unwrap());
    parts.metadata.dictionary_size = u32::try_from(dictionary.len()).unwrap();
    Zstd::try_new(
        input.dtype().clone(),
        ZstdData::new(
            Some(ByteBuffer::from(dictionary)),
            parts.frames,
            parts.metadata,
            parts.n_rows,
        ),
        parts.validity,
    )
    .unwrap()
    .into_array()
}

fn with_first_frame(input: &ArrayRef, frame: Vec<u8>) -> ArrayRef {
    let mut parts = input
        .as_::<Zstd>()
        .data()
        .clone()
        .into_parts(input.validity().unwrap());
    parts.frames[0] = ByteBuffer::from(frame);
    Zstd::try_new(
        input.dtype().clone(),
        ZstdData::new(parts.dictionary, parts.frames, parts.metadata, parts.n_rows),
        parts.validity,
    )
    .unwrap()
    .into_array()
}

fn first_frame(input: &ArrayRef) -> Vec<u8> {
    input
        .as_::<Zstd>()
        .data()
        .clone()
        .into_parts(input.validity().unwrap())
        .frames[0]
        .as_slice()
        .to_vec()
}

fn decode(input: &ArrayRef, memory: &LiveMemoryPool) -> VortexResult<PrimitiveArray> {
    input
        .clone()
        .execute::<PrimitiveArray>(&mut session(memory).create_execution_ctx())
}

fn trained_primitive() -> (ArrayRef, Vec<i64>) {
    let expected: Vec<i64> = (0..4096).map(|index| index * 37 - 4096).collect();
    let values = PrimitiveArray::from_iter(expected.iter().copied());
    let data = ZstdData::from_primitive(
        &values,
        0,
        32,
        &mut VortexSession::default().create_execution_ctx(),
    )
    .unwrap();
    assert!(
        data.clone()
            .into_parts(Validity::NonNullable)
            .dictionary
            .is_some()
    );
    let encoded = Zstd::try_new(values.dtype().clone(), data, Validity::NonNullable)
        .unwrap()
        .into_array();
    (encoded, expected)
}

#[test]
fn native_zstd_tiny_payload_still_requires_decoder_workspace() {
    let encoded = primitive(&PrimitiveArray::from_iter([17i64]));
    // This grant admits the eight-byte output and its alignment allowance,
    // but cannot hold the provider's decoder context.
    let memory = LiveMemoryPool::new(4096).unwrap();
    let error = encoded
        .execute::<PrimitiveArray>(&mut session(&memory).create_execution_ctx())
        .unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert!(memory.snapshot().peak_reserved_bytes > 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_exact_workspace_grants_release_only_temporary_owners() {
    let input = primitive(&PrimitiveArray::from_iter([17i64]));
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let (session, allocator) = recorded_session(&memory);
    let output = input
        .clone()
        .execute::<PrimitiveArray>(&mut session.create_execution_ctx())
        .unwrap();
    assert_eq!(output.as_slice::<i64>(), &[17]);
    let requests = allocator.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2, "payload and actual decoder workspace");
    assert_eq!(requests[0].0, 8);
    assert!(requests[1].0 > 4096);
    assert_eq!(requests[1].1, 8);
    let peak = memory.snapshot().peak_reserved_bytes;
    assert_eq!(
        peak,
        requests
            .iter()
            .map(|(size, _)| u64::try_from(size + 256).unwrap())
            .sum::<u64>()
    );
    assert_eq!(memory.snapshot().reserved_bytes, 8 + 256);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let exact = LiveMemoryPool::new(peak).unwrap();
    let output = decode(&input, &exact).unwrap();
    assert_eq!(output.as_slice::<i64>(), &[17]);
    assert_eq!(exact.snapshot().peak_reserved_bytes, peak);
    assert_eq!(exact.snapshot().reserved_bytes, 8 + 256);
    drop(output);
    assert_eq!(exact.snapshot().reserved_bytes, 0);

    let short = LiveMemoryPool::new(peak - 1).unwrap();
    let error = decode(&input, &short).unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(short.snapshot().peak_reserved_bytes, 8 + 256);
    assert_eq!(short.snapshot().denied_reservations, 1);
    assert_eq!(short.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_trained_dictionary_is_admitted_and_released_before_output() {
    let (input, expected) = trained_primitive();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let (session, allocator) = recorded_session(&memory);
    let output = input
        .clone()
        .execute::<PrimitiveArray>(&mut session.create_execution_ctx())
        .unwrap();
    assert_eq!(output.as_slice::<i64>(), expected);
    let requests = allocator.requests.lock().unwrap().clone();
    assert_eq!(
        requests.len(),
        3,
        "payload, decoder and prepared dictionary"
    );
    assert!(requests[1].0 > 4096 && requests[2].0 > 4096);
    assert_eq!(requests[1].1, 8);
    assert_eq!(requests[2].1, 8);
    let payload = u64::try_from(expected.len() * 8 + 256).unwrap();
    let context_peak = payload + u64::try_from(requests[1].0 + 256).unwrap();
    let peak = memory.snapshot().peak_reserved_bytes;
    assert_eq!(
        peak,
        context_peak + u64::try_from(requests[2].0 + 256).unwrap()
    );
    assert_eq!(memory.snapshot().reserved_bytes, payload);
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let exact = LiveMemoryPool::new(peak).unwrap();
    let output = decode(&input, &exact).unwrap();
    assert_eq!(output.as_slice::<i64>(), expected);
    assert_eq!(exact.snapshot().peak_reserved_bytes, peak);
    drop(output);
    assert_eq!(exact.snapshot().reserved_bytes, 0);

    for limit in [context_peak, peak - 1] {
        let denied = LiveMemoryPool::new(limit).unwrap();
        let error = decode(&input, &denied).unwrap_err();
        assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
        assert_eq!(denied.snapshot().peak_reserved_bytes, context_peak);
        assert_eq!(denied.snapshot().denied_reservations, 1);
        assert_eq!(denied.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_zstd_prepared_dictionary_does_not_copy_original_bytes() {
    let base = primitive(&PrimitiveArray::from_iter([17i64]));
    let mut peaks = vec![];
    for length in [0, 16, 1 << 20] {
        let input = with_dictionary(&base, vec![b'x'; length]);
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let output = decode(&input, &memory).unwrap();
        assert_eq!(output.as_slice::<i64>(), &[17]);
        peaks.push(memory.snapshot().peak_reserved_bytes);
        assert_eq!(memory.snapshot().reserved_bytes, 8 + 256);
        drop(output);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    assert!(peaks[0] > 4096);
    assert!(peaks.iter().all(|peak| *peak == peaks[0]));
}

#[test]
fn native_zstd_trained_nullable_unicode_keeps_only_result_owners() {
    let expected: Vec<Option<String>> = (0..256)
        .map(|index| {
            (index % 11 != 0).then(|| format!("trained native Unicode λ {index:04} long value"))
        })
        .collect();
    let values = VarBinViewArray::from_iter_nullable_str(expected.iter().map(Option::as_deref));
    let encoded = Zstd::from_var_bin_view(
        &values,
        0,
        8,
        &mut VortexSession::default().create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    assert!(
        encoded
            .as_::<Zstd>()
            .data()
            .clone()
            .into_parts(values.validity().unwrap())
            .dictionary
            .is_some()
    );
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let session = session(&memory);
    let mut ctx = session.create_execution_ctx();
    let output = encoded
        .slice(11..27)
        .unwrap()
        .execute::<VarBinViewArray>(&mut ctx)
        .unwrap();
    let mask = output
        .validity()
        .unwrap()
        .execute_mask(output.len(), &mut ctx)
        .unwrap();
    for (index, value) in expected[11..27].iter().enumerate() {
        assert_eq!(mask.value(index), value.is_some());
        if let Some(value) = value {
            assert_eq!(output.bytes_at(index).as_slice(), value.as_bytes());
        }
    }
    let scalar = encoded.execute_scalar(12, &mut ctx).unwrap();
    assert_eq!(
        scalar.as_utf8().value().unwrap().as_str(),
        expected[12].as_deref().unwrap()
    );
    assert!(memory.snapshot().peak_reserved_bytes > memory.snapshot().reserved_bytes);
    drop(output);
    drop(mask);
    drop(ctx);
    drop(session);
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(scalar);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_invalid_dictionary_and_checksum_release_workspaces() {
    let base = primitive(&PrimitiveArray::from_iter([17i64]));
    // Modern frame with a raw final block and an intentionally wrong checksum.
    let mut checksum_frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0x24, 8, 0x41, 0, 0];
    checksum_frame.extend_from_slice(&17i64.to_ne_bytes());
    checksum_frame.extend_from_slice(&[0; 4]);
    // Trained-dictionary magic and ID, without the required entropy tables.
    let mut invalid_dict = 0xEC30_A437u32.to_le_bytes().to_vec();
    invalid_dict.extend_from_slice(&1u32.to_le_bytes());
    let (trained, _) = trained_primitive();
    for (input, expected_allocations) in [
        (with_dictionary(&base, invalid_dict), 3),
        (with_first_frame(&base, checksum_frame), 2),
        (with_dictionary(&trained, vec![b'x'; 16]), 3),
    ] {
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let (session, allocator) = recorded_session(&memory);
        let error = input
            .execute::<PrimitiveArray>(&mut session.create_execution_ctx())
            .unwrap_err();
        assert!(!crate::owned_buffers::is_owned_reservation_denial(&error));
        assert_eq!(
            allocator.requests.lock().unwrap().len(),
            expected_allocations
        );
        assert!(memory.snapshot().peak_reserved_bytes > 4096);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(memory.snapshot().denied_reservations, 0);
    }
}

// A complete modern frame containing one empty final raw block.
const EMPTY_FRAME: &[u8] = &[0x28, 0xb5, 0x2f, 0xfd, 0x20, 0, 1, 0, 0];
const SKIPPABLE: &[u8] = &[0x5f, 0x2a, 0x4d, 0x18, 3, 0, 0, 0, 0xff, 0, 0x7f];

#[test]
fn native_zstd_modern_concatenations_and_skippable_members_preserve_values() {
    let base = primitive(&PrimitiveArray::from_iter([17i64]));
    for members in [
        vec![EMPTY_FRAME],
        vec![SKIPPABLE],
        vec![EMPTY_FRAME, SKIPPABLE, EMPTY_FRAME],
    ] {
        let mut frame = first_frame(&base);
        for member in members {
            frame.extend_from_slice(member);
        }
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let output = decode(&with_first_frame(&base, frame), &memory).unwrap();
        assert_eq!(output.as_slice::<i64>(), &[17]);
        assert!(memory.snapshot().peak_reserved_bytes > 4096);
        drop(output);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    // Metadata still bounds the total payload; a second nonempty frame cannot
    // grow the admitted output, even when both members are independently valid.
    let mut overflow = first_frame(&base);
    overflow.extend_from_slice(&first_frame(&base));
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let error = decode(&with_first_frame(&base, overflow), &memory).unwrap_err();
    assert!(!crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_trailing_legacy_members_are_rejected_before_workspace_admission() {
    let base = primitive(&PrimitiveArray::from_iter([17i64]));
    for magic in std::iter::once(0x1EB5_2FFDu32).chain(0xFD2F_B522..=0xFD2F_B527) {
        for after_skippable in [false, true] {
            let mut frame = first_frame(&base);
            if after_skippable {
                frame.extend_from_slice(SKIPPABLE);
            }
            frame.extend_from_slice(&magic.to_le_bytes());
            let memory = LiveMemoryPool::new(4096).unwrap();
            let error = decode(&with_first_frame(&base, frame), &memory).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("Legacy Zstd frames are unsupported")
            );
            assert!(!crate::owned_buffers::is_owned_reservation_denial(&error));
            assert_eq!(memory.snapshot().peak_reserved_bytes, 8 + 256);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_zstd_initial_legacy_headers_are_explicitly_unsupported() {
    for magic in std::iter::once(0x1EB5_2FFDu32).chain(0xFD2F_B522..=0xFD2F_B527) {
        let error = Zstd::try_new(
            DType::Primitive(PType::I64, Nullability::NonNullable),
            ZstdData::new(
                None,
                vec![ByteBuffer::from(magic.to_le_bytes().to_vec())],
                ZstdMetadata {
                    dictionary_size: 0,
                    frames: vec![vortex::encodings::zstd::ZstdFrameMetadata {
                        uncompressed_size: 8,
                        n_values: 1,
                    }],
                },
                1,
            ),
            Validity::NonNullable,
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Legacy Zstd frames are unsupported")
        );
        assert!(!crate::owned_buffers::is_owned_reservation_denial(&error));
    }
}

#[test]
fn native_zstd_malformed_trailing_members_do_not_enter_the_decoder() {
    let base = primitive(&PrimitiveArray::from_iter([17i64]));
    for trailing in [&[0][..], &[1, 2, 3, 4], &SKIPPABLE[..9], &EMPTY_FRAME[..7]] {
        let mut frame = first_frame(&base);
        frame.extend_from_slice(trailing);
        let memory = LiveMemoryPool::new(4096).unwrap();
        let error = decode(&with_first_frame(&base, frame), &memory).unwrap_err();
        assert!(!crate::owned_buffers::is_owned_reservation_denial(&error));
        assert_eq!(memory.snapshot().peak_reserved_bytes, 8 + 256);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_zstd_empty_and_all_null_selections_need_no_decoder() {
    for (rows, validity) in [(0, Validity::AllValid), (9, Validity::AllInvalid)] {
        for dtype in [
            DType::Primitive(PType::I64, Nullability::Nullable),
            DType::Utf8(Nullability::Nullable),
        ] {
            let input = Zstd::try_new(
                dtype,
                ZstdData::new(
                    None,
                    vec![],
                    ZstdMetadata {
                        dictionary_size: 0,
                        frames: vec![],
                    },
                    rows,
                ),
                validity.clone(),
            )
            .unwrap()
            .into_array();
            let memory = LiveMemoryPool::new(4096).unwrap();
            let (session, allocator) = recorded_session(&memory);
            let output = input
                .execute::<vortex::array::Canonical>(&mut session.create_execution_ctx())
                .unwrap();
            assert_eq!(output.len(), rows);
            assert!(
                allocator
                    .requests
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|(len, _)| *len < 4096)
            );
            drop(output);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
    let input = primitive(&PrimitiveArray::from_iter([17i64]))
        .slice(0..0)
        .unwrap();
    let memory = LiveMemoryPool::new(4096).unwrap();
    let output = decode(&input, &memory).unwrap();
    assert!(output.is_empty());
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
