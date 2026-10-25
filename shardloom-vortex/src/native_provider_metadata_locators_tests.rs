//! Native user-metadata locator/name admission and independent footer aliases.

use std::sync::{Arc, Mutex};

use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _,
        arrays::PrimitiveArray,
        dtype::DType,
        memory::{
            DefaultHostAllocator, HostAllocator, HostMemoryReservation, HostMemorySessionExt as _,
            WritableHostBuffer,
        },
    },
    buffer::{Alignment, ByteBuffer},
    error::VortexResult,
    file::{DeserializeStep, Footer, SegmentSpec, WriteOptionsSessionExt as _},
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    session::VortexSession,
};

#[derive(Debug)]
struct MetadataAllocator {
    tables: crate::owned_buffers::ReservedHostAllocator,
    requests: Arc<Mutex<Vec<usize>>>,
}

impl HostAllocator for MetadataAllocator {
    fn allocate(&self, bytes: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        // Existing copied-buffer tests prove their separate admission. This
        // fixture isolates growing table and String capacity reservations.
        DefaultHostAllocator.allocate(bytes, alignment)
    }

    fn reserve_external(&self, bytes: usize) -> VortexResult<HostMemoryReservation> {
        self.requests.lock().unwrap().push(bytes);
        self.tables.reserve_external(bytes)
    }
}

fn session(memory: &LiveMemoryPool) -> (VortexSession, Arc<Mutex<Vec<usize>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let consumer = VortexSession::default().with_host_allocator(Arc::new(MetadataAllocator {
        tables: crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
        requests: Arc::clone(&requests),
    }));
    (consumer, requests)
}

fn key(index: usize) -> String {
    format!("metadata-{index:02}-東京-λ")
}

fn metadata_bytes(count: usize) -> usize {
    count * size_of::<(String, SegmentSpec)>() + (0..count).map(|i| key(i).len()).sum::<usize>()
}

fn fixture(count: usize) -> (ByteBuffer, DType, usize) {
    let runtime = CurrentThreadRuntime::new();
    let writer = VortexSession::default().with_handle(runtime.handle());
    let array = PrimitiveArray::from_option_iter([Some(-9_i64), None, Some(17)]).into_array();
    let mut options = writer
        .write_options()
        .with_file_statistics(Vec::new())
        .with_strategy(Arc::new(
            vortex::layout::layouts::flat::writer::FlatLayoutStrategy::default(),
        ));
    for index in 0..count {
        options = options.with_metadata_segment(
            key(index),
            ByteBuffer::from(vec![u8::try_from(index).unwrap(); index + 1]),
        );
    }
    let mut output = Vec::new();
    let summary = runtime
        .block_on(options.write(&mut output, array.to_array_stream()))
        .unwrap();
    let segments = summary.footer().segment_map().len() * size_of::<SegmentSpec>();
    (ByteBuffer::from(output), array.dtype().clone(), segments)
}

fn reopen(bytes: ByteBuffer, dtype: DType, consumer: &VortexSession) -> VortexResult<Footer> {
    let size = u64::try_from(bytes.len()).unwrap();
    let mut parser = Footer::deserializer(bytes, consumer.clone())
        .with_dtype(dtype)
        .with_size(size);
    match parser.deserialize()? {
        DeserializeStep::Done(footer) => Ok(footer),
        other => panic!("complete native file unexpectedly requests more input: {other:?}"),
    }
}

fn assert_metadata(footer: &Footer, count: usize, bytes: &ByteBuffer) {
    let entries = footer.metadata_segments().collect::<Vec<_>>();
    assert_eq!(entries.len(), count);
    for (index, (name, segment)) in entries.into_iter().enumerate() {
        assert_eq!(name, key(index));
        let start = usize::try_from(segment.offset).unwrap();
        let end = start + usize::try_from(segment.length).unwrap();
        assert_eq!(
            &bytes[start..end],
            vec![u8::try_from(index).unwrap(); index + 1]
        );
        assert!(std::ptr::eq(
            footer.metadata_segment(name).unwrap(),
            segment
        ));
    }
}

#[test]
fn native_metadata_locators_parsed_names_and_table_require_exact_admission() {
    for count in [0, 1, 2, 16] {
        let (bytes, dtype, segments) = fixture(count);
        let metadata = metadata_bytes(count);
        let expected = u64::try_from(metadata + segments).unwrap();
        let memory = LiveMemoryPool::new(expected).unwrap();
        let (consumer, requests) = session(&memory);
        let footer = reopen(bytes.clone(), dtype, &consumer).unwrap();
        let expected_requests = if count == 0 {
            vec![segments]
        } else {
            vec![metadata, segments]
        };
        assert_eq!(requests.lock().unwrap().as_slice(), expected_requests);
        assert_eq!(memory.snapshot().reserved_bytes, expected);
        assert_metadata(&footer, count, &bytes);
        drop((footer, consumer));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_metadata_locators_parsed_names_and_table_deny_one_byte_short() {
    for count in [1, 16] {
        let (bytes, dtype, segments) = fixture(count);
        let metadata = metadata_bytes(count);
        let memory = LiveMemoryPool::new(u64::try_from(metadata + segments - 1).unwrap()).unwrap();
        let (consumer, requests) = session(&memory);
        let error = reopen(bytes, dtype, &consumer).unwrap_err();
        assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
        assert_eq!(requests.lock().unwrap().as_slice(), &[metadata, segments]);
        assert_eq!(
            memory.snapshot().peak_reserved_bytes,
            u64::try_from(metadata).unwrap()
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(memory.snapshot().denied_reservations, 1);
    }
}

#[test]
fn native_metadata_locators_footer_clone_retains_names_after_parser_and_session() {
    let count = 16;
    let (bytes, dtype, segments) = fixture(count);
    let expected = u64::try_from(metadata_bytes(count) + segments).unwrap();
    let memory = LiveMemoryPool::new(expected).unwrap();
    let (consumer, _) = session(&memory);
    let footer = reopen(bytes.clone(), dtype, &consumer).unwrap();
    let alias = footer.clone();
    let pointers = footer
        .metadata_segments()
        .map(|(key, segment)| (key.as_ptr(), std::ptr::from_ref(segment)))
        .collect::<Vec<_>>();
    drop((footer, consumer));
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    assert_eq!(
        alias
            .metadata_segments()
            .map(|(key, segment)| (key.as_ptr(), std::ptr::from_ref(segment)))
            .collect::<Vec<_>>(),
        pointers
    );
    assert_metadata(&alias, count, &bytes);
    drop(alias);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_metadata_locators_short_name_table_grant_precedes_other_tables() {
    for count in [1, 16] {
        let (bytes, dtype, _) = fixture(count);
        let metadata = metadata_bytes(count);
        let memory = LiveMemoryPool::new(u64::try_from(metadata - 1).unwrap()).unwrap();
        let (consumer, requests) = session(&memory);
        let error = reopen(bytes, dtype, &consumer).unwrap_err();
        assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
        assert_eq!(requests.lock().unwrap().as_slice(), &[metadata]);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
        assert_eq!(memory.snapshot().denied_reservations, 1);
    }
}

#[test]
fn native_metadata_locators_incremental_parser_retains_one_owner_across_missing_input() {
    let count = 16;
    let (bytes, dtype, segments) = fixture(count);
    let metadata = metadata_bytes(count);
    let expected = u64::try_from(metadata + segments).unwrap();
    let memory = LiveMemoryPool::new(expected).unwrap();
    let (consumer, requests) = session(&memory);
    let eof = bytes.len() - 8;
    let postscript_len = usize::from(u16::from_le_bytes(
        bytes[eof + 2..eof + 4].try_into().unwrap(),
    ));
    let tail_start = eof - postscript_len;
    let mut parser = Footer::deserializer(bytes.slice_unaligned(tail_start..), consumer.clone())
        .with_dtype(dtype);
    for _ in 0..3 {
        assert!(matches!(
            parser.deserialize().unwrap(),
            DeserializeStep::NeedFileSize
        ));
        assert_eq!(requests.lock().unwrap().as_slice(), &[metadata]);
        assert_eq!(
            memory.snapshot().reserved_bytes,
            u64::try_from(metadata).unwrap()
        );
    }
    parser = parser.with_size(u64::try_from(bytes.len()).unwrap());
    let mut missing = None;
    for _ in 0..3 {
        let DeserializeStep::NeedMoreData { offset, len } = parser.deserialize().unwrap() else {
            panic!("postscript-only input must request the preceding footer bytes");
        };
        let start = usize::try_from(offset).unwrap();
        assert_eq!(start + len, tail_start);
        if let Some(previous) = missing {
            assert_eq!((start, len), previous);
        }
        missing = Some((start, len));
        assert_eq!(requests.lock().unwrap().as_slice(), &[metadata]);
        assert_eq!(
            memory.snapshot().reserved_bytes,
            u64::try_from(metadata).unwrap()
        );
    }
    let (start, len) = missing.unwrap();
    parser
        .try_prefix_data(bytes.slice_unaligned(start..start + len))
        .unwrap();
    let DeserializeStep::Done(footer) = parser.deserialize().unwrap() else {
        panic!("supplied footer bytes must complete parsing");
    };
    assert_eq!(requests.lock().unwrap().as_slice(), &[metadata, segments]);
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    assert_metadata(&footer, count, &bytes);
    drop((footer, consumer));
    // The parser still retains the postscript, but no independent segment map.
    assert_eq!(
        memory.snapshot().reserved_bytes,
        u64::try_from(metadata).unwrap()
    );
    drop(parser);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_metadata_locators_map_denial_retries_without_reallocating_parsed_names() {
    let count = 2;
    let (bytes, dtype, segments) = fixture(count);
    let metadata = metadata_bytes(count);
    let expected = u64::try_from(metadata + segments).unwrap();
    let memory = LiveMemoryPool::new(expected).unwrap();
    let (consumer, requests) = session(&memory);
    let peer = consumer.host_allocator().reserve_external(1).unwrap();
    let mut parser = Footer::deserializer(bytes.clone(), consumer.clone())
        .with_dtype(dtype)
        .with_size(u64::try_from(bytes.len()).unwrap());
    let error = parser.deserialize().unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(
        requests.lock().unwrap().as_slice(),
        &[1, metadata, segments]
    );
    assert_eq!(
        memory.snapshot().reserved_bytes,
        u64::try_from(metadata + 1).unwrap()
    );
    drop(peer);
    let DeserializeStep::Done(footer) = parser.deserialize().unwrap() else {
        panic!("releasing the competing owner must permit the same parser to retry");
    };
    assert_eq!(
        requests.lock().unwrap().as_slice(),
        &[1, metadata, segments, segments]
    );
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    assert_eq!(memory.snapshot().denied_reservations, 1);
    assert_metadata(&footer, count, &bytes);
    drop((parser, consumer));
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    drop(footer);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn assert_complete_metadata_file_values(
    file: &vortex::file::VortexFile,
    runtime: &CurrentThreadRuntime,
) {
    use vortex::array::VortexSessionExecute as _;

    let expected = PrimitiveArray::from_option_iter([Some(-9_i64), None, Some(17)]).into_array();
    let mut context = file.session().create_execution_ctx();
    let mut reference = VortexSession::default().create_execution_ctx();
    let mut seen = 0;
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
                expected.execute_scalar(seen, &mut reference).unwrap()
            );
            seen += 1;
        }
    }
    assert_eq!(seen, expected.len());
}

#[test]
fn native_metadata_locators_both_sources_share_names_and_resolve_every_metadata_id() {
    use vortex::{file::OpenOptionsSessionExt as _, layout::segments::SegmentId};

    for file_backed in [false, true] {
        for count in [0, 1, 16] {
            for include_metadata in [false, true] {
                let (bytes, dtype, segments) = fixture(count);
                let metadata = metadata_bytes(count);
                let expected = u64::try_from(metadata + segments).unwrap();
                let memory = LiveMemoryPool::new(expected).unwrap();
                let runtime = CurrentThreadRuntime::new();
                let (consumer, requests) = session(&memory);
                let consumer = consumer.with_handle(runtime.handle());
                let options = consumer.open_options().with_dtype(dtype);
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
                let expected_requests = if count == 0 {
                    vec![segments]
                } else {
                    vec![metadata, segments]
                };
                assert_eq!(requests.lock().unwrap().as_slice(), expected_requests);
                assert_eq!(memory.snapshot().reserved_bytes, expected);
                assert_metadata(file.footer(), count, &bytes);
                assert_complete_metadata_file_values(&file, &runtime);
                let map = file.footer().segment_map().clone();
                let footer = file.footer().clone();
                let pointers = footer
                    .metadata_segments()
                    .map(|(name, _)| name.as_ptr())
                    .collect::<Vec<_>>();
                let source = file.segment_source();
                for (index, (_, spec)) in footer.metadata_segments().enumerate() {
                    let id = SegmentId::from(u32::try_from(map.len() + index).unwrap());
                    let buffer = runtime
                        .block_on(source.request(id))
                        .unwrap()
                        .try_into_host()
                        .unwrap();
                    let buffer = runtime.block_on(buffer).unwrap();
                    let start = usize::try_from(spec.offset).unwrap();
                    let end = start + usize::try_from(spec.length).unwrap();
                    assert_eq!(buffer.as_slice(), &bytes[start..end]);
                    assert!(buffer.is_aligned(spec.alignment));
                    if include_metadata {
                        assert_eq!(
                            file.metadata_segment(&key(index)).unwrap().as_slice(),
                            buffer.as_slice()
                        );
                    } else {
                        assert!(file.metadata_segment(&key(index)).is_none());
                    }
                }
                let error = runtime
                    .block_on(
                        source.request(SegmentId::from(u32::try_from(map.len() + count).unwrap())),
                    )
                    .unwrap_err();
                assert!(error.to_string().contains("Missing segment"));
                drop((file, consumer));
                assert_eq!(memory.snapshot().reserved_bytes, expected);
                assert_eq!(
                    footer
                        .metadata_segments()
                        .map(|(name, _)| name.as_ptr())
                        .collect::<Vec<_>>(),
                    pointers
                );
                assert_metadata(&footer, count, &bytes);
                drop(footer);
                assert_eq!(memory.snapshot().reserved_bytes, expected);
                drop(source);
                assert_eq!(
                    memory.snapshot().reserved_bytes,
                    u64::try_from(segments).unwrap()
                );
                drop(map);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
                assert_eq!(requests.lock().unwrap().as_slice(), expected_requests);
            }
        }
    }
}

#[test]
fn native_metadata_locators_writer_exact_grant_and_footer_clone_preserve_values() {
    use vortex::file::OpenOptionsSessionExt as _;

    for count in [1, 16] {
        let expected = u64::try_from(metadata_bytes(count)).unwrap();
        for limit in [expected - 1, expected] {
            let runtime = CurrentThreadRuntime::new();
            let memory = LiveMemoryPool::new(limit).unwrap();
            let (consumer, requests) = session(&memory);
            let consumer = consumer.with_handle(runtime.handle());
            let mut options = consumer
                .write_options()
                .with_file_statistics(Vec::new())
                .with_strategy(Arc::new(
                    vortex::layout::layouts::flat::writer::FlatLayoutStrategy::default(),
                ));
            for index in 0..count {
                options = options.with_metadata_segment(
                    key(index),
                    ByteBuffer::from(vec![u8::try_from(index).unwrap(); index + 1]),
                );
            }
            let array =
                PrimitiveArray::from_option_iter([Some(-9_i64), None, Some(17)]).into_array();
            let mut output = Vec::new();
            let result = runtime.block_on(options.write(&mut output, array.to_array_stream()));
            assert_eq!(
                requests.lock().unwrap().as_slice(),
                &[usize::try_from(expected).unwrap()]
            );
            if limit < expected {
                let error = result
                    .err()
                    .expect("one-byte-short metadata writer grant must fail");
                assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
                assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
                assert_eq!(memory.snapshot().reserved_bytes, 0);
                continue;
            }
            let summary = result.unwrap();
            let footer = summary.footer().clone();
            let bytes = ByteBuffer::from(output);
            drop((summary, consumer));
            assert_eq!(memory.snapshot().reserved_bytes, expected);
            assert_metadata(&footer, count, &bytes);
            let reader = VortexSession::default().with_handle(runtime.handle());
            let file = reader
                .open_options()
                .include_metadata()
                .open_buffer(bytes.clone())
                .unwrap();
            assert_complete_metadata_file_values(&file, &runtime);
            assert_metadata(file.footer(), count, &bytes);
            for index in 0..count {
                assert_eq!(
                    file.metadata_segment(&key(index)).unwrap().as_slice(),
                    vec![u8::try_from(index).unwrap(); index + 1]
                );
            }
            drop(footer);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
        }
    }
}

#[test]
fn native_metadata_locators_serializer_retains_existing_owner_without_copying_names() {
    let count = 16;
    let (bytes, dtype, segments) = fixture(count);
    let metadata = metadata_bytes(count);
    let expected = u64::try_from(metadata + segments).unwrap();
    let memory = LiveMemoryPool::new(expected).unwrap();
    let (consumer, requests) = session(&memory);
    let footer = reopen(bytes.clone(), dtype.clone(), &consumer).unwrap();
    let alias = footer.clone();
    let serializer = footer.into_serializer();
    drop(consumer);
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    let output = serializer.serialize().unwrap();
    assert_eq!(requests.lock().unwrap().as_slice(), &[metadata, segments]);
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    assert_metadata(&alias, count, &bytes);
    let encoded = ByteBuffer::from(
        output
            .iter()
            .flat_map(|b| b.iter().copied())
            .collect::<Vec<_>>(),
    );
    let restored = reopen(encoded, dtype, &VortexSession::default()).unwrap();
    // Standalone serialization keeps absolute metadata locators in the original
    // file. Validate those locators against that file, not the footer-only blob.
    assert_metadata(&restored, count, &bytes);
    drop(alias);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn replace_metadata_postscript(
    bytes: &ByteBuffer,
    entries: &[(&str, SegmentSpec, u8)],
) -> ByteBuffer {
    use vortex::file::flatbuffers::footer as fb;

    fn copy_segment<'a>(
        builder: &mut flatbuffers::FlatBufferBuilder<'a>,
        original: fb::PostscriptSegment<'_>,
    ) -> flatbuffers::WIPOffset<fb::PostscriptSegment<'a>> {
        fb::PostscriptSegment::create(
            builder,
            &fb::PostscriptSegmentArgs {
                offset: original.offset(),
                length: original.length(),
                alignment_exponent: original.alignment_exponent(),
                ..Default::default()
            },
        )
    }
    let eof = bytes.len() - 8;
    let postscript_len = usize::from(u16::from_le_bytes(
        bytes[eof + 2..eof + 4].try_into().unwrap(),
    ));
    let start = eof - postscript_len;
    let original = flatbuffers::root::<fb::Postscript<'_>>(&bytes[start..eof]).unwrap();
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let dtype = original.dtype().map(|s| copy_segment(&mut builder, s));
    let layout = Some(copy_segment(&mut builder, original.layout().unwrap()));
    let statistics = original.statistics().map(|s| copy_segment(&mut builder, s));
    let footer = Some(copy_segment(&mut builder, original.footer().unwrap()));
    let metadata = entries
        .iter()
        .map(|(name, spec, exponent)| {
            let name = builder.create_string(name);
            let segment = fb::PostscriptSegment::create(
                &mut builder,
                &fb::PostscriptSegmentArgs {
                    offset: spec.offset,
                    length: spec.length,
                    alignment_exponent: *exponent,
                    ..Default::default()
                },
            );
            fb::PostscriptMetadata::create(
                &mut builder,
                &fb::PostscriptMetadataArgs {
                    key: Some(name),
                    segment: Some(segment),
                },
            )
        })
        .collect::<Vec<_>>();
    let metadata = Some(builder.create_vector(&metadata));
    let root = fb::Postscript::create(
        &mut builder,
        &fb::PostscriptArgs {
            dtype,
            layout,
            statistics,
            footer,
            metadata,
        },
    );
    builder.finish(root, None);
    let mut output = bytes[..start].to_vec();
    output.extend_from_slice(builder.finished_data());
    output.extend_from_slice(&bytes[eof..eof + 2]);
    output.extend_from_slice(
        &u16::try_from(builder.finished_data().len())
            .unwrap()
            .to_le_bytes(),
    );
    output.extend_from_slice(&bytes[eof + 4..]);
    ByteBuffer::from(output)
}

#[test]
fn native_metadata_locators_invalid_keys_fail_before_any_table_admission() {
    let (bytes, dtype, _) = fixture(2);
    let valid = reopen(bytes.clone(), dtype.clone(), &VortexSession::default()).unwrap();
    let spec = *valid.metadata_segment(&key(0)).unwrap();
    let cases = [
        (vec![String::new()], "must not be empty"),
        (vec!["duplicate".to_string(); 2], "duplicate metadata key"),
        (vec!["x".repeat(65)], "65 bytes"),
        (vec!["界".repeat(22)], "66 bytes"),
        (
            (0..17).map(|i| format!("key-{i}")).collect(),
            "17 metadata segments",
        ),
    ];
    for (names, expected_error) in cases {
        let entries = names
            .iter()
            .map(|n| (n.as_str(), spec, spec.alignment.exponent()))
            .collect::<Vec<_>>();
        let invalid = replace_metadata_postscript(&bytes, &entries);
        let memory = LiveMemoryPool::new(1).unwrap();
        let (consumer, requests) = session(&memory);
        let error = reopen(invalid, dtype.clone(), &consumer).unwrap_err();
        assert!(error.to_string().contains(expected_error), "{error}");
        assert!(requests.lock().unwrap().is_empty());
        assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_metadata_locators_partial_parse_and_invalid_ranges_release_credit() {
    let count = 2;
    let (bytes, dtype, segments) = fixture(count);
    let valid = reopen(bytes.clone(), dtype.clone(), &VortexSession::default()).unwrap();
    let specs = valid
        .metadata_segments()
        .map(|(_, spec)| *spec)
        .collect::<Vec<_>>();
    let names = [key(0), key(1)];
    let metadata = metadata_bytes(count);
    for (bad_spec, exponent, message) in [
        (specs[1], u8::MAX, "alignment"),
        (
            SegmentSpec {
                offset: u64::MAX,
                length: 1,
                ..specs[1]
            },
            0,
            "range overflowed",
        ),
        (
            SegmentSpec {
                offset: u64::try_from(bytes.len()).unwrap() + 4096,
                ..specs[1]
            },
            0,
            "exceeds file size",
        ),
        (
            SegmentSpec {
                offset: 1,
                ..specs[1]
            },
            3,
            "not aligned",
        ),
    ] {
        let invalid = replace_metadata_postscript(
            &bytes,
            &[
                (names[0].as_str(), specs[0], specs[0].alignment.exponent()),
                (names[1].as_str(), bad_spec, exponent),
            ],
        );
        let memory = LiveMemoryPool::new(u64::try_from(metadata + segments).unwrap()).unwrap();
        let (consumer, requests) = session(&memory);
        let error = reopen(invalid, dtype.clone(), &consumer).unwrap_err();
        assert!(
            error.to_string().to_lowercase().contains(message),
            "{error}"
        );
        assert_eq!(requests.lock().unwrap().as_slice(), &[metadata]);
        assert_eq!(
            memory.snapshot().peak_reserved_bytes,
            u64::try_from(metadata).unwrap()
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        let retry = reopen(bytes.clone(), dtype.clone(), &consumer).unwrap();
        assert_metadata(&retry, count, &bytes);
        assert_eq!(
            requests.lock().unwrap().as_slice(),
            &[metadata, metadata, segments]
        );
        drop(retry);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
