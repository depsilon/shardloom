use super::*;
use std::sync::Mutex;
use vortex::array::memory::{
    DefaultHostAllocator, HostAllocator, HostMemoryReservation, WritableHostBuffer,
};

#[path = "native_provider_zstd_compression_values_tests.rs"]
mod values_tests;

#[path = "native_provider_zstd_compression_allocator_tests.rs"]
mod allocator_tests;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Request {
    Buffer { bytes: usize, alignment: usize },
    External(usize),
}

#[derive(Debug)]
struct AdmissionAllocator {
    inner: crate::owned_buffers::ReservedHostAllocator,
    memory: LiveMemoryPool,
    requests: Mutex<Vec<Request>>,
    fail_at: Option<usize>,
}

impl AdmissionAllocator {
    fn record(&self, request: Request) -> VortexResult<()> {
        let mut requests = self.requests.lock().unwrap();
        let index = requests.len();
        requests.push(request);
        if self.fail_at == Some(index) {
            // Exercise the original typed admission error at one selected stage.
            return Err(crate::owned_buffers::reserve(&self.memory, u64::MAX).unwrap_err());
        }
        Ok(())
    }
}

impl HostAllocator for AdmissionAllocator {
    fn allocate(&self, bytes: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        self.record(Request::Buffer {
            bytes,
            alignment: alignment.as_usize(),
        })?;
        self.inner.allocate(bytes, alignment)
    }

    fn reserve_external(&self, bytes: usize) -> VortexResult<HostMemoryReservation> {
        self.record(Request::External(bytes))?;
        self.inner.reserve_external(bytes)
    }
}

fn admission_session(
    memory: &LiveMemoryPool,
    fail_at: Option<usize>,
) -> (VortexSession, Arc<AdmissionAllocator>) {
    let allocator = Arc::new(AdmissionAllocator {
        inner: crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
        memory: memory.clone(),
        requests: Mutex::new(Vec::new()),
        fail_at,
    });
    let session = VortexSession::default().with_host_allocator(allocator.clone());
    install(&session, memory.clone());
    (session, allocator)
}

fn assert_data_bytes_equal(left: &ZstdData, right: &ZstdData) {
    let left = left.clone().into_parts(Validity::NonNullable);
    let right = right.clone().into_parts(Validity::NonNullable);
    assert_eq!(left.n_rows, right.n_rows);
    assert_eq!(
        left.metadata.dictionary_size,
        right.metadata.dictionary_size
    );
    let frame_metadata = |metadata: &ZstdMetadata| {
        metadata
            .frames
            .iter()
            .map(|frame| (frame.uncompressed_size, frame.n_values))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        frame_metadata(&left.metadata),
        frame_metadata(&right.metadata)
    );
    assert_eq!(left.dictionary, right.dictionary);
    assert_eq!(left.frames, right.frames);
}

fn assert_admission_stages(
    dictionary: bool,
    operation: impl Fn(&mut ExecutionCtx) -> VortexResult<ZstdData>,
) {
    let reference = operation(&mut VortexSession::default().create_execution_ctx()).unwrap();
    let memory = LiveMemoryPool::new(64 << 20).unwrap();
    let (session, recorder) = admission_session(&memory, None);
    let mut ctx = session.create_execution_ctx();
    let data = operation(&mut ctx).unwrap();
    assert_data_bytes_equal(&data, &reference);
    let requests = recorder.requests.lock().unwrap().clone();
    let external_calls = requests
        .iter()
        .filter(|request| matches!(request, Request::External(_)))
        .count();
    assert_eq!(
        external_calls,
        if dictionary { 3 } else { 2 },
        "frame table, compression and optional dictionary training"
    );
    let peak = memory.snapshot().peak_reserved_bytes;
    let retained = memory.snapshot().reserved_bytes;
    assert!(peak > retained && retained > 0);
    drop(ctx);
    drop(session);
    assert_eq!(memory.snapshot().reserved_bytes, retained);

    let parts = data.into_parts(Validity::NonNullable);
    assert_eq!(parts.dictionary.is_some(), dictionary);
    let frame_bytes = u64::try_from(parts.frames[0].len() + 256).unwrap();
    let frame = parts.frames[0].slice(1..2);
    let dictionary = parts.dictionary.as_ref().map(|value| value.slice(1..2));
    drop(parts);
    let escaped = memory.snapshot().reserved_bytes;
    assert!(escaped >= frame_bytes);
    assert_eq!(escaped > frame_bytes, dictionary.is_some());
    drop(frame);
    assert_eq!(memory.snapshot().reserved_bytes, escaped - frame_bytes);
    drop(dictionary);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let exact = LiveMemoryPool::new(peak).unwrap();
    let data = operation(&mut self::session(&exact).create_execution_ctx()).unwrap();
    assert_data_bytes_equal(&data, &reference);
    assert_eq!(exact.snapshot().peak_reserved_bytes, peak);
    drop(data);
    assert_eq!(exact.snapshot().reserved_bytes, 0);

    let denied = LiveMemoryPool::new(peak - 1).unwrap();
    let error = operation(&mut self::session(&denied).create_execution_ctx()).unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(denied.snapshot().reserved_bytes, 0);

    let competitor = exact.reserve(1).unwrap();
    let error = operation(&mut self::session(&exact).create_execution_ctx()).unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(exact.snapshot().reserved_bytes, 1);
    drop(competitor);
    drop(operation(&mut self::session(&exact).create_execution_ctx()).unwrap());
    assert_eq!(exact.snapshot().reserved_bytes, 0);

    for target in 0..requests.len() {
        let memory = LiveMemoryPool::new(64 << 20).unwrap();
        let (session, recorder) = admission_session(&memory, Some(target));
        let error = operation(&mut session.create_execution_ctx()).unwrap_err();
        assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
        assert_eq!(*recorder.requests.lock().unwrap(), requests[..=target]);
        assert_eq!(memory.snapshot().denied_reservations, 1);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_zstd_compression_primitive_admission_and_retention() {
    let values = PrimitiveArray::from_iter([17i64, i64::MIN, i64::MAX]);
    assert_admission_stages(false, |ctx| {
        ZstdData::from_primitive_without_dict(&values, 0, 8, ctx)
    });
    let values = PrimitiveArray::from_option_iter(
        (0i64..4096).map(|index| (index % 17 != 0).then_some(index * 37 - 4096)),
    );
    assert_admission_stages(true, |ctx| ZstdData::from_primitive(&values, 0, 128, ctx));
}

#[test]
fn native_zstd_compression_binary_admission_and_retention() {
    let values: Vec<_> = (0..256)
        .map(|index| {
            (index % 19 != 0).then(|| {
                let mut value = format!("long native compression sample {index:05} λ ")
                    .repeat(4)
                    .into_bytes();
                value.extend_from_slice(&[0, 0xff, 0x80, 0]);
                value
            })
        })
        .collect();
    let input = VarBinViewArray::from_iter_nullable_bin(values.iter().map(Option::as_deref));
    for dictionary in [false, true] {
        assert_admission_stages(dictionary, |ctx| {
            if dictionary {
                ZstdData::from_var_bin_view(&input, 3, 8, ctx)
            } else {
                ZstdData::from_var_bin_view_without_dict(&input, 3, 8, ctx)
            }
        });
    }
}

#[test]
fn native_zstd_compression_small_samples_keep_default_training() {
    let values = VarBinViewArray::from_iter_str(["λ"; 8]);
    let memory = LiveMemoryPool::new(64 << 20).unwrap();
    let (session, recorder) = admission_session(&memory, None);
    let data =
        ZstdData::from_var_bin_view(&values, 0, 1, &mut session.create_execution_ctx()).unwrap();
    assert!(
        data.clone()
            .into_parts(Validity::NonNullable)
            .dictionary
            .is_some()
    );
    assert_eq!(
        recorder
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| matches!(request, Request::External(_)))
            .count(),
        3,
        "frame table, default training and dictionary compression"
    );
    drop(data);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_compression_rejects_a_short_training_partition_before_foreign_admission() {
    let values = PrimitiveArray::new(
        vortex::buffer::Buffer::copy_from_aligned([1u8; 8], Alignment::none()),
        Validity::NonNullable,
    );
    assert_eq!(
        values
            .buffer_handle()
            .try_to_host_sync()
            .unwrap()
            .alignment()
            .as_usize(),
        1
    );
    let memory = LiveMemoryPool::new(64 << 20).unwrap();
    let (session, recorder) = admission_session(&memory, None);
    let error =
        ZstdData::from_primitive(&values, 0, 1, &mut session.create_execution_ctx()).unwrap_err();
    assert!(error.to_string().contains("training partition"), "{error}");
    assert_eq!(
        recorder
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| matches!(request, Request::External(_)))
            .cloned()
            .collect::<Vec<_>>(),
        vec![Request::External(
            8 * (size_of::<vortex::buffer::ByteBuffer>()
                + size_of::<vortex::encodings::zstd::ZstdFrameMetadata>())
        )],
        "only the Rust frame table is admitted before the training-domain check"
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    // The explicit no-dictionary entry point still admits these exact values.
    drop(
        ZstdData::from_primitive_without_dict(&values, 0, 1, &mut session.create_execution_ctx())
            .unwrap(),
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[derive(Debug)]
struct BufferOnlyAllocator;

impl HostAllocator for BufferOnlyAllocator {
    fn allocate(&self, bytes: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        DefaultHostAllocator.allocate(bytes, alignment)
    }
}

#[test]
fn native_zstd_compression_unknown_allocator_cannot_bypass_foreign_admission() {
    let session = VortexSession::default().with_host_allocator(Arc::new(BufferOnlyAllocator));
    let error = ZstdData::from_primitive_without_dict(
        &PrimitiveArray::from_iter([17i64]),
        0,
        0,
        &mut session.create_execution_ctx(),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not support external memory reservations")
    );
}

#[test]
fn native_zstd_compression_tiny_payload_requires_context_admission() {
    let values = PrimitiveArray::from_iter([17i64]);
    let memory = LiveMemoryPool::new(4096).unwrap();
    let error = ZstdData::from_primitive_without_dict(
        &values,
        0,
        8,
        &mut session(&memory).create_execution_ctx(),
    )
    .unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_compression_empty_default_frame_round_trips() {
    let values = PrimitiveArray::from_iter(std::iter::empty::<i64>());
    let memory = LiveMemoryPool::new(4096).unwrap();
    let session = session(&memory);
    let mut ctx = session.create_execution_ctx();
    let data = ZstdData::from_primitive_without_dict(&values, 0, 0, &mut ctx).unwrap();
    let encoded = Zstd::try_new(values.dtype().clone(), data, Validity::NonNullable)
        .unwrap()
        .into_array();
    let output = encoded.execute::<PrimitiveArray>(&mut ctx).unwrap();
    assert!(output.is_empty());
    drop(output);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_zstd_compression_empty_strings_use_default_frame() {
    let values = VarBinViewArray::from_iter_str(std::iter::empty::<&str>());
    let mut ctx = VortexSession::default().create_execution_ctx();
    let encoded = Zstd::from_var_bin_view_without_dict(&values, 0, 0, &mut ctx).unwrap();
    let output = encoded
        .into_array()
        .execute::<VarBinViewArray>(&mut ctx)
        .unwrap();
    assert!(output.is_empty());
}

#[test]
fn native_zstd_compression_frame_limit_above_input_does_not_overflow() {
    let values = PrimitiveArray::from_iter([i64::MIN, i64::MAX]);
    let mut ctx = VortexSession::default().create_execution_ctx();
    let data = ZstdData::from_primitive_without_dict(&values, 0, usize::MAX, &mut ctx).unwrap();
    let encoded = Zstd::try_new(values.dtype().clone(), data, Validity::NonNullable)
        .unwrap()
        .into_array();
    let output = encoded.execute::<PrimitiveArray>(&mut ctx).unwrap();
    assert_eq!(output.as_slice::<i64>(), &[i64::MIN, i64::MAX]);
}

#[test]
fn native_zstd_compression_overaligned_frames_preserve_counts_and_slices() {
    let expected: Vec<i64> = (0..65).map(|index| index * 37 - 4096).collect();
    let values = PrimitiveArray::new(
        vortex::buffer::Buffer::copy_from_aligned(&expected, Alignment::new(256)),
        Validity::NonNullable,
    );
    let mut ctx = VortexSession::default().create_execution_ctx();
    let data = ZstdData::from_primitive_without_dict(&values, 0, 3, &mut ctx).unwrap();
    let parts = data.clone().into_parts(Validity::NonNullable);
    assert_eq!(
        parts
            .metadata
            .frames
            .iter()
            .map(|frame| frame.n_values)
            .sum::<u64>(),
        65
    );
    let encoded = Zstd::try_new(values.dtype().clone(), data, Validity::NonNullable)
        .unwrap()
        .into_array();
    for range in [0..65, 2..35, 31..33, 63..65] {
        let output = encoded
            .slice(range.clone())
            .unwrap()
            .execute::<PrimitiveArray>(&mut ctx)
            .unwrap();
        assert_eq!(output.as_slice::<i64>(), &expected[range]);
    }
}
