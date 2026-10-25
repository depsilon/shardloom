//! Native file segment-table admission and independently retained aliases.

use std::sync::{Arc, Mutex};

use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::PrimitiveArray,
        dtype::{DType, Nullability, PType},
        memory::{
            DefaultHostAllocator, HostAllocator, HostMemoryReservation, HostMemorySessionExt as _,
            WritableHostBuffer,
        },
    },
    buffer::{Alignment, ByteBuffer},
    error::VortexResult,
    file::{
        DeserializeStep, Footer, OpenOptionsSessionExt as _, SegmentMap, SegmentSpec, VortexFile,
        WriteOptionsSessionExt as _,
    },
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    layout::{layouts::flat::FlatLayout, segments::SegmentId},
    session::{VortexSession, registry::ReadContext},
};

#[derive(Debug)]
struct TableAllocator {
    tables: crate::owned_buffers::ReservedHostAllocator,
    requests: Arc<Mutex<Vec<usize>>>,
}

impl HostAllocator for TableAllocator {
    fn allocate(&self, bytes: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        // This fixture isolates external tables. Copied footer byte buffers have
        // separate exact-grant and lifetime regressions in file_metadata_tests.
        DefaultHostAllocator.allocate(bytes, alignment)
    }

    fn reserve_external(&self, bytes: usize) -> VortexResult<HostMemoryReservation> {
        self.requests.lock().unwrap().push(bytes);
        self.tables.reserve_external(bytes)
    }
}

fn dtype() -> DType {
    DType::Primitive(PType::I64, Nullability::NonNullable)
}

fn session(memory: &LiveMemoryPool) -> (VortexSession, Arc<Mutex<Vec<usize>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let session = VortexSession::default().with_host_allocator(Arc::new(TableAllocator {
        tables: crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
        requests: Arc::clone(&requests),
    }));
    (session, requests)
}

fn fixture(width: usize) -> ByteBuffer {
    footer_fixture((0..width).map(segment).collect())
}

fn segment(index: usize) -> SegmentSpec {
    SegmentSpec {
        offset: u64::try_from(index).unwrap(),
        length: 0,
        alignment: Alignment::none(),
    }
}

fn footer_fixture(segments: Arc<[SegmentSpec]>) -> ByteBuffer {
    let root = Arc::new(FlatLayout::new(
        0,
        dtype(),
        SegmentId::from(0),
        ReadContext::new([]),
    ));
    let buffers = Footer::new(root, segments, None, ReadContext::new([]))
        .into_serializer()
        .exclude_dtype()
        .serialize()
        .unwrap();
    ByteBuffer::from(
        buffers
            .iter()
            .flat_map(|buffer| buffer.iter().copied())
            .collect::<Vec<_>>(),
    )
}

fn reopen(bytes: ByteBuffer, consumer: &VortexSession) -> VortexResult<Footer> {
    let size = u64::try_from(bytes.len()).unwrap();
    let mut parser = Footer::deserializer(bytes, consumer.clone())
        .with_dtype(dtype())
        .with_size(size);
    match parser.deserialize()? {
        DeserializeStep::Done(footer) => Ok(footer),
        other => panic!("complete footer fixture unexpectedly requests more input: {other:?}"),
    }
}

#[test]
fn native_segment_map_parsed_table_requires_exact_admission() {
    for width in [1, 2, 7, 8, 128] {
        let expected = width * size_of::<SegmentSpec>();
        let memory = LiveMemoryPool::new(u64::try_from(expected).unwrap()).unwrap();
        let (consumer, requests) = session(&memory);
        let footer = reopen(fixture(width), &consumer).unwrap();
        assert_eq!(requests.lock().unwrap().as_slice(), &[expected]);
        assert_eq!(
            memory.snapshot().reserved_bytes,
            u64::try_from(expected).unwrap()
        );
        assert_eq!(footer.segment_map().len(), width);
        for (index, segment) in footer.segment_map().iter().enumerate() {
            assert_eq!(segment.offset, u64::try_from(index).unwrap());
            assert_eq!(segment.length, 0);
            assert_eq!(segment.alignment, Alignment::none());
        }
        drop((footer, consumer));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_segment_map_parsed_table_denies_one_byte_short() {
    let width = 128;
    let expected = width * size_of::<SegmentSpec>();
    let memory = LiveMemoryPool::new(u64::try_from(expected - 1).unwrap()).unwrap();
    let (consumer, requests) = session(&memory);
    let error = reopen(fixture(width), &consumer).unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(requests.lock().unwrap().as_slice(), &[expected]);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 1);
}

#[test]
fn native_segment_map_parsed_alias_keeps_credit_after_footer_and_session() {
    let width = 128;
    let expected = u64::try_from(width * size_of::<SegmentSpec>()).unwrap();
    let memory = LiveMemoryPool::new(expected).unwrap();
    let (consumer, _) = session(&memory);
    let footer = reopen(fixture(width), &consumer).unwrap();
    let map = footer.segment_map().clone();
    let alias = map.clone();
    let pointer = map.as_ptr();
    drop((footer, consumer, map));
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    assert_eq!(alias.as_ptr(), pointer);
    assert_eq!(alias[127].offset, 127);
    assert_eq!(memory.snapshot().peak_reserved_bytes, expected);
    drop(alias);
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
fn native_segment_map_empty_and_custom_allocator_boundaries() {
    let consumer = VortexSession::default().with_host_allocator(Arc::new(BufferOnlyAllocator));
    let empty = SegmentMap::try_from_fn(0, |_| panic!("empty table producer"), &consumer).unwrap();
    assert!(empty.is_empty());
    assert!(
        reopen(fixture(0), &consumer)
            .unwrap()
            .segment_map()
            .is_empty()
    );
    let error =
        SegmentMap::try_from_fn(1, |_| panic!("unadmitted producer"), &consumer).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not support external memory reservations")
    );
    let error = reopen(fixture(1), &consumer).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not support external memory reservations")
    );

    // Compatibility accepts an already allocated caller-owned table unchanged.
    let caller: Arc<[SegmentSpec]> = Arc::from([segment(0), segment(1)]);
    let map = SegmentMap::from(Arc::clone(&caller));
    assert_eq!(map.as_ptr(), caller.as_ptr());
    drop(caller);
    assert_eq!(map[1].offset, 1);

    let memory = LiveMemoryPool::new(1).unwrap();
    let (consumer, requests) = session(&memory);
    assert!(
        reopen(fixture(0), &consumer)
            .unwrap()
            .segment_map()
            .is_empty()
    );
    assert!(requests.lock().unwrap().is_empty());
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
}

#[test]
fn native_segment_map_overflow_precedes_admission_and_producer() {
    let memory = LiveMemoryPool::new(1).unwrap();
    let (consumer, requests) = session(&memory);
    for count in [
        usize::MAX,
        usize::MAX / size_of::<SegmentSpec>() + 1,
        (isize::MAX as usize) / size_of::<SegmentSpec>() + 1,
    ] {
        let error =
            SegmentMap::try_from_fn(count, |_| panic!("overflow producer"), &consumer).unwrap_err();
        assert!(error.to_string().contains("allocation size overflow"));
    }
    assert!(requests.lock().unwrap().is_empty());
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 0);
}

#[test]
fn native_segment_map_denial_preserves_peer_and_allows_retry() {
    let width = 8;
    let bytes = u64::try_from(width * size_of::<SegmentSpec>()).unwrap();
    let memory = LiveMemoryPool::new(bytes).unwrap();
    let (consumer, requests) = session(&memory);
    let peer = SegmentMap::try_from_fn(width, |index| Ok(segment(index)), &consumer).unwrap();
    let alias = peer.clone();
    let error =
        SegmentMap::try_from_fn(width, |_| panic!("denied producer"), &consumer).unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().reserved_bytes, bytes);
    assert_eq!(alias[7].offset, 7);
    drop(peer);
    assert_eq!(memory.snapshot().reserved_bytes, bytes);
    drop(alias);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let retried = SegmentMap::try_from_fn(width, |index| Ok(segment(index)), &consumer).unwrap();
    assert_eq!(retried.len(), width);
    assert_eq!(requests.lock().unwrap().len(), 3);
    assert_eq!(memory.snapshot().peak_reserved_bytes, bytes);
    assert_eq!(memory.snapshot().denied_reservations, 1);
    drop(retried);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_segment_map_partial_parse_failure_releases_its_reservation() {
    let width = 8;
    let bytes = u64::try_from(width * size_of::<SegmentSpec>()).unwrap();
    let memory = LiveMemoryPool::new(2 * bytes).unwrap();
    let (consumer, _) = session(&memory);
    let peer = SegmentMap::try_from_fn(width, |index| Ok(segment(index)), &consumer).unwrap();
    for bad_alignment in [17, u8::MAX] {
        let mut calls = 0;
        let error = SegmentMap::try_from_fn(
            width,
            |index| {
                calls += 1;
                let serialized = vortex::file::flatbuffers::footer::SegmentSpec::new(
                    u64::try_from(index).unwrap(),
                    0,
                    if index == 2 { bad_alignment } else { 0 },
                    0,
                    0,
                );
                SegmentSpec::try_from(&serialized)
            },
            &consumer,
        )
        .unwrap_err();
        assert!(error.to_string().contains("alignment") || error.to_string().contains("Alignment"));
        assert_eq!(calls, 3);
        assert_eq!(memory.snapshot().reserved_bytes, bytes);
        assert_eq!(memory.snapshot().peak_reserved_bytes, 2 * bytes);
        assert_eq!(peer[7].offset, 7);
    }
    let retry = SegmentMap::try_from_fn(width, |index| Ok(segment(index)), &consumer).unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, 2 * bytes);
    drop((retry, peer, consumer));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_segment_map_malformed_footer_tables_release_credit() {
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let (consumer, requests) = session(&memory);
    let error = reopen(
        footer_fixture(Arc::from([segment(2), segment(1), segment(3)])),
        &consumer,
    )
    .unwrap_err();
    assert!(error.to_string().contains("offsets are not ordered"));
    assert_eq!(
        requests.lock().unwrap().as_slice(),
        &[3 * size_of::<SegmentSpec>()]
    );
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    for (offset, length) in [(u64::MAX, 1), (u64::MAX - 1, 0)] {
        let bytes = footer_fixture(Arc::from([SegmentSpec {
            offset,
            length,
            alignment: Alignment::none(),
        }]));
        let error = consumer
            .open_options()
            .with_dtype(dtype())
            .open_buffer(bytes)
            .err()
            .expect("invalid segment range must fail during native open");
        assert!(error.to_string().contains("past the end"));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
    assert_eq!(requests.lock().unwrap().len(), 3);
}

fn data_fixture(metadata_count: usize) -> (ByteBuffer, ArrayRef) {
    let runtime = CurrentThreadRuntime::new();
    let writer = VortexSession::default().with_handle(runtime.handle());
    let array = PrimitiveArray::from_option_iter([Some(-9_i64), None, Some(17), Some(i64::MAX)])
        .into_array();
    let mut bytes = Vec::new();
    let mut options = writer.write_options().with_strategy(Arc::new(
        vortex::layout::layouts::flat::writer::FlatLayoutStrategy::default(),
    ));
    for index in 0..metadata_count {
        options = options.with_metadata_segment(
            format!("metadata-{index:02}"),
            ByteBuffer::from(vec![u8::try_from(index).unwrap(); index + 1]),
        );
    }
    runtime
        .block_on(options.write(&mut bytes, array.to_array_stream()))
        .unwrap();
    (
        ByteBuffer::copy_from_aligned(bytes, Alignment::new(64)),
        array,
    )
}

fn assert_source_values(file: &VortexFile, expected: &ArrayRef, runtime: &CurrentThreadRuntime) {
    let mut context = file.session().create_execution_ctx();
    let mut row = 0;
    for batch in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(runtime)
        .unwrap()
    {
        let batch = batch.unwrap();
        assert_eq!(batch.dtype(), expected.dtype());
        for index in 0..batch.len() {
            assert_eq!(
                batch.execute_scalar(index, &mut context).unwrap(),
                expected.execute_scalar(row, &mut context).unwrap()
            );
            row += 1;
        }
    }
    assert_eq!(row, expected.len());
}

fn check_source_owners(file_backed: bool) {
    for metadata_count in [0, 1, 16] {
        for include_metadata in [false, true] {
            let (bytes, expected) = data_fixture(metadata_count);
            let memory = LiveMemoryPool::new(1 << 20).unwrap();
            let runtime = CurrentThreadRuntime::new();
            let (consumer, requests) = session(&memory);
            let consumer = consumer.with_handle(runtime.handle());
            let options = consumer.open_options();
            let options = if include_metadata {
                options.include_metadata()
            } else {
                options
            };
            let file = if file_backed {
                runtime.block_on(options.open_read(bytes.clone())).unwrap()
            } else {
                options.open_buffer(bytes.clone()).unwrap()
            };
            let map = file.footer().segment_map().clone();
            let table_bytes = map.len() * size_of::<SegmentSpec>();
            let stats = file.footer().statistics().unwrap();
            let statistics_bytes = size_of_val(stats.stats_sets()) + size_of_val(stats.dtypes());
            let metadata_bytes = file
                .footer()
                .metadata_segments()
                .map(|(key, _)| size_of::<(String, SegmentSpec)>() + key.len())
                .sum::<usize>();
            let expected_requests = if metadata_count == 0 {
                vec![statistics_bytes, table_bytes]
            } else {
                vec![metadata_bytes, statistics_bytes, table_bytes]
            };
            assert_eq!(requests.lock().unwrap().as_slice(), expected_requests);
            assert_eq!(
                memory.snapshot().reserved_bytes,
                u64::try_from(metadata_bytes + statistics_bytes + table_bytes).unwrap()
            );
            let specs = map
                .iter()
                .copied()
                .chain(
                    file.footer()
                        .metadata_segments()
                        .map(|(_, segment)| *segment),
                )
                .collect::<Vec<_>>();
            assert_eq!(specs.len(), map.len() + metadata_count);
            let source = file.segment_source();
            for (index, spec) in specs.iter().enumerate() {
                let buffer = runtime
                    .block_on(source.request(SegmentId::from(u32::try_from(index).unwrap())))
                    .unwrap()
                    .try_into_host()
                    .unwrap();
                let buffer = runtime.block_on(buffer).unwrap();
                let start = usize::try_from(spec.offset).unwrap();
                let end = start + usize::try_from(spec.length).unwrap();
                assert_eq!(buffer.as_slice(), &bytes[start..end]);
                assert!(buffer.is_aligned(spec.alignment));
            }
            for index in 0..metadata_count {
                let metadata = file.metadata_segment(&format!("metadata-{index:02}"));
                if include_metadata {
                    assert_eq!(
                        metadata.unwrap().as_slice(),
                        vec![u8::try_from(index).unwrap(); index + 1]
                    );
                } else {
                    assert!(metadata.is_none());
                }
            }
            assert_source_values(&file, &expected, &runtime);
            let error = runtime
                .block_on(source.request(SegmentId::from(u32::try_from(specs.len()).unwrap())))
                .unwrap_err();
            assert!(error.to_string().contains("Missing segment"));
            drop((file, consumer));
            assert_eq!(
                memory.snapshot().reserved_bytes,
                u64::try_from(metadata_bytes + table_bytes).unwrap()
            );
            drop(map);
            assert_eq!(
                memory.snapshot().reserved_bytes,
                u64::try_from(metadata_bytes + table_bytes).unwrap()
            );
            // The independently retained source still owns a usable table.
            let buffer = runtime
                .block_on(source.request(SegmentId::from(0)))
                .unwrap();
            drop(buffer);
            drop(source);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            assert_eq!(requests.lock().unwrap().as_slice(), expected_requests);
        }
    }
}

#[test]
fn native_segment_map_buffer_source_preserves_metadata_ids_without_copy() {
    check_source_owners(false);
}

#[test]
fn native_segment_map_file_source_preserves_metadata_ids_without_copy() {
    check_source_owners(true);
}

#[test]
fn native_segment_map_serializer_retains_owner_and_roundtrips() {
    let width = 128;
    let expected = u64::try_from(width * size_of::<SegmentSpec>()).unwrap();
    let memory = LiveMemoryPool::new(expected).unwrap();
    let (consumer, _) = session(&memory);
    let footer = reopen(fixture(width), &consumer).unwrap();
    let serializer = footer.into_serializer().exclude_dtype();
    drop(consumer);
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    let buffers = serializer.serialize().unwrap();
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let bytes = ByteBuffer::from(
        buffers
            .iter()
            .flat_map(|b| b.iter().copied())
            .collect::<Vec<_>>(),
    );
    let (consumer, _) = session(&memory);
    let restored = reopen(bytes, &consumer).unwrap();
    assert_eq!(restored.segment_map().len(), width);
    for (index, spec) in restored.segment_map().iter().enumerate() {
        assert_eq!(spec.offset, u64::try_from(index).unwrap());
        assert_eq!(spec.length, 0);
        assert_eq!(spec.alignment, Alignment::none());
    }
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    drop((restored, consumer));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
