//! Allocator ownership of copied native file metadata, separate from input bytes.

use std::sync::Arc;

use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::{BoolArray, PrimitiveArray, StructArray},
        dtype::{FieldName, FieldNames},
        memory::MemorySessionExt as _,
        validity::Validity,
    },
    buffer::{Alignment, ByteBuffer},
    file::{
        DeserializeStep, EOF_SIZE, Footer, MAX_POSTSCRIPT_SIZE, OpenOptionsSessionExt as _,
        VortexFile, WriteOptionsSessionExt as _,
    },
    io::{
        runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
        session::RuntimeSessionExt as _,
    },
    session::VortexSession,
};

fn fixture() -> (Vec<u8>, Footer) {
    let column = PrimitiveArray::from_option_iter([Some(-9_i64), None, Some(17)]).into_array();
    let array = StructArray::try_new(
        FieldNames::from(["metric"]),
        vec![column],
        3,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    write_fixture(&array)
}

fn write_fixture(array: &ArrayRef) -> (Vec<u8>, Footer) {
    write_fixture_metadata(array, &[("owner-test", 257)])
}

fn write_fixture_metadata(array: &ArrayRef, metadata: &[(&str, usize)]) -> (Vec<u8>, Footer) {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let mut bytes = Vec::new();
    let mut options = session.write_options();
    for (name, len) in metadata {
        options = options.with_metadata_segment(*name, ByteBuffer::from(vec![0x5a_u8; *len]));
    }
    let summary = runtime
        .block_on(options.write(&mut bytes, array.to_array_stream()))
        .unwrap();
    (bytes, summary.footer().clone())
}

fn session(memory: &LiveMemoryPool) -> VortexSession {
    VortexSession::default().with_allocator(Arc::new(
        crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
    ))
}

fn charge(len: usize, alignment: Alignment) -> u64 {
    u64::try_from(len + *alignment.max(Alignment::DEFAULT_ALIGNMENT)).unwrap()
}

fn owned_bytes(session: &VortexSession, bytes: &[u8]) -> ByteBuffer {
    let mut buffer = session
        .allocator()
        .allocate(bytes.len(), Alignment::none())
        .unwrap();
    buffer.as_mut_slice().copy_from_slice(bytes);
    buffer.freeze()
}

fn assert_values(file: &VortexFile, expected: &ArrayRef, runtime: &CurrentThreadRuntime) {
    let mut context = file.session().create_execution_ctx();
    let mut reference = VortexSession::default().create_execution_ctx();
    let mut seen = 0;
    for array in file
        .scan()
        .unwrap()
        .with_ordered(true)
        .into_array_iter(runtime)
        .unwrap()
    {
        let array = array.unwrap();
        for row in 0..array.len() {
            assert_eq!(
                array.execute_scalar(row, &mut context).unwrap(),
                expected.execute_scalar(seen, &mut reference).unwrap()
            );
            seen += 1;
        }
    }
    assert_eq!(seen, expected.len());
}

fn assert_cached_segments(file: &VortexFile, bytes: &[u8], runtime: &CurrentThreadRuntime) {
    let source = file.segment_source();
    for (index, segment) in file
        .footer()
        .segment_map()
        .iter()
        .chain(
            file.footer()
                .metadata_segments()
                .map(|(_, segment)| segment),
        )
        .enumerate()
    {
        let handle = runtime
            .block_on(source.request(vortex::layout::segments::SegmentId::from(
                u32::try_from(index).unwrap(),
            )))
            .unwrap();
        let buffer = runtime.block_on(handle.try_into_host().unwrap()).unwrap();
        let offset = usize::try_from(segment.offset).unwrap();
        let len = usize::try_from(segment.length).unwrap();
        assert_eq!(buffer.as_slice(), &bytes[offset..offset + len]);
        assert!(buffer.is_aligned(segment.alignment));
    }
}

#[test]
fn native_file_metadata_footer_copies_require_allocator_admission() {
    let (bytes, _) = fixture();
    // The test supplies already-owned bytes, isolating new parser allocations.
    let memory = LiveMemoryPool::new(1).unwrap();
    let error = session(&memory)
        .open_options()
        .open_buffer(bytes)
        .err()
        .expect("a one-byte grant cannot admit copied native footer buffers");
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 1);
}

#[test]
fn native_file_metadata_explicit_payload_copy_requires_its_receiving_allocator() {
    let (bytes, footer) = fixture();
    // A supplied footer skips parsing and isolates the optional payload copy.
    let memory = LiveMemoryPool::new(1).unwrap();
    let error = session(&memory)
        .open_options()
        .with_footer(footer)
        .include_metadata()
        .open_buffer(bytes)
        .err()
        .expect("a one-byte grant cannot admit a 257-byte metadata copy");
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 1);
}

#[test]
fn native_file_metadata_schema_and_layout_clones_keep_their_buffer_credits() {
    let (bytes, original) = fixture();
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let consumer = session(&memory);
    let file = consumer.open_options().open_buffer(bytes).unwrap();
    assert_eq!(file.footer().dtype(), original.dtype());
    assert_eq!(file.footer().row_count(), 3);
    let dtype = file.footer().dtype().clone();
    let layout = Arc::clone(file.footer().layout());
    drop((file, consumer));
    assert!(
        memory.snapshot().reserved_bytes > 0,
        "schema and layout copies must retain the admitted buffer owners"
    );
    drop(dtype);
    assert!(memory.snapshot().reserved_bytes > 0);
    assert_eq!(layout.row_count(), 3);
    assert_eq!(layout.dtype(), original.dtype());
    drop(layout);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_file_metadata_layout_copy_has_independent_exact_admission_and_owner() {
    let array = PrimitiveArray::from_option_iter([Some(-9_i64), None, Some(17)]).into_array();
    // A flat root has no child-cache allocation. Its statistics, layout bytes
    // and segment table have independent admission and retention boundaries.
    let runtime = CurrentThreadRuntime::new();
    let writer = VortexSession::default().with_handle(runtime.handle());
    let mut bytes = Vec::new();
    let written = runtime
        .block_on(
            writer
                .write_options()
                .with_strategy(Arc::new(
                    vortex::layout::layouts::flat::writer::FlatLayoutStrategy::default(),
                ))
                .write(&mut bytes, array.to_array_stream()),
        )
        .unwrap();
    let original = written.footer();
    assert_eq!(original.layout().nchildren(), 0);
    let eof = bytes.len() - EOF_SIZE;
    let postscript_len = usize::from(u16::from_le_bytes(
        bytes[eof + 2..eof + 4].try_into().unwrap(),
    ));
    let postscript = flatbuffers::root::<vortex::flatbuffers::footer::Postscript<'_>>(
        &bytes[eof - postscript_len..eof],
    )
    .unwrap();
    let layout_bytes = usize::try_from(postscript.layout().unwrap().length()).unwrap();
    let capacity = charge(layout_bytes, vortex::flatbuffers::FlatBuffer::alignment());
    let table_capacity =
        u64::try_from(original.segment_map().len() * size_of::<vortex::file::SegmentSpec>())
            .unwrap();
    let statistics = original.statistics().unwrap();
    let statistics_capacity = u64::try_from(
        std::mem::size_of_val(statistics.stats_sets()) + std::mem::size_of_val(statistics.dtypes()),
    )
    .unwrap();
    let layout_and_segment = capacity + table_capacity;
    let statistics_and_layout = statistics_capacity + capacity;
    let total = statistics_and_layout + table_capacity;
    // Preserve the four previous grants. Statistics parse before the layout
    // copy and segment table; check each additional short/exact boundary.
    // After dropping the file, only the retained layout copy remains charged.
    let bytes = ByteBuffer::copy_from_aligned(bytes, Alignment::new(64)).aligned(Alignment::none());
    for budget in [
        capacity - 1,
        capacity,
        layout_and_segment - 1,
        layout_and_segment,
        statistics_capacity - 1,
        statistics_capacity,
        statistics_and_layout - 1,
        statistics_and_layout,
        total - 1,
        total,
    ] {
        let memory = LiveMemoryPool::new(budget).unwrap();
        let consumer = session(&memory);
        let result = consumer
            .open_options()
            .with_dtype(original.dtype().clone())
            .open_buffer(bytes.clone());
        if budget < total {
            let error = result
                .err()
                .expect("statistics, layout or segment-table admission must fail");
            assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            assert_eq!(
                memory.snapshot().peak_reserved_bytes,
                if budget < statistics_capacity {
                    0
                } else if budget < statistics_and_layout {
                    statistics_capacity
                } else {
                    statistics_and_layout
                }
            );
            assert_eq!(memory.snapshot().denied_reservations, 1);
            continue;
        }
        let file = result.unwrap();
        assert_eq!(file.dtype(), array.dtype());
        assert_eq!(memory.snapshot().reserved_bytes, total);
        let layout = Arc::clone(file.footer().layout());
        drop((file, consumer));
        assert_eq!(layout.row_count(), 3);
        assert_eq!(layout.dtype(), array.dtype());
        assert_eq!(memory.snapshot().reserved_bytes, capacity);
        assert_eq!(memory.snapshot().peak_reserved_bytes, total);
        drop(layout);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_file_metadata_prefix_exact_grant_accounts_for_both_live_inputs() {
    let old = charge(4, Alignment::none());
    let incoming = charge(3, Alignment::none());
    let joined = charge(7, Alignment::none());
    let memory = LiveMemoryPool::new(old + incoming + joined).unwrap();
    let consumer = session(&memory);
    let mut parser = Footer::deserializer(owned_bytes(&consumer, b"tail"), consumer.clone());
    let prefix = owned_bytes(&consumer, b"pre");
    parser.try_prefix_data(prefix).unwrap();
    assert_eq!(parser.buffer().as_slice(), b"pretail");
    assert_eq!(
        memory.snapshot().peak_reserved_bytes,
        old + incoming + joined
    );
    assert_eq!(memory.snapshot().reserved_bytes, joined);
    let retained = parser.buffer().slice(1..6);
    drop((parser, consumer));
    assert_eq!(retained.as_slice(), b"retai");
    assert_eq!(memory.snapshot().reserved_bytes, joined);
    drop(retained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_file_metadata_prefix_short_grant_preserves_the_existing_buffer() {
    let old = charge(4, Alignment::none());
    let incoming = charge(3, Alignment::none());
    let joined = charge(7, Alignment::none());
    let memory = LiveMemoryPool::new(old + incoming + joined - 1).unwrap();
    let consumer = session(&memory);
    let mut parser = Footer::deserializer(owned_bytes(&consumer, b"tail"), consumer.clone());
    let prefix = owned_bytes(&consumer, b"pre");
    let error = parser.try_prefix_data(prefix.clone()).unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(parser.buffer().as_slice(), b"tail");
    assert_eq!(prefix.as_slice(), b"pre");
    assert_eq!(memory.snapshot().reserved_bytes, old + incoming);
    assert_eq!(memory.snapshot().peak_reserved_bytes, old + incoming);
    assert_eq!(memory.snapshot().denied_reservations, 1);
    drop((parser, prefix, consumer));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_file_metadata_prefix_retries_only_after_competing_credit_is_released() {
    let total =
        charge(4, Alignment::none()) + charge(3, Alignment::none()) + charge(7, Alignment::none());
    let memory = LiveMemoryPool::new(total).unwrap();
    let peer = memory.reserve(1).unwrap();
    let consumer = session(&memory);
    let mut parser = Footer::deserializer(owned_bytes(&consumer, b"tail"), consumer.clone());
    let prefix = owned_bytes(&consumer, b"pre");
    let error = parser.try_prefix_data(prefix.clone()).unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(parser.buffer().as_slice(), b"tail");
    drop(peer);
    parser.try_prefix_data(prefix).unwrap();
    assert_eq!(parser.buffer().as_slice(), b"pretail");
    assert_eq!(memory.snapshot().peak_reserved_bytes, total);
    assert_eq!(memory.snapshot().denied_reservations, 1);
    drop((parser, consumer));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_file_metadata_optional_payload_exact_grant_and_clone_ownership() {
    let (bytes, footer) = fixture();
    let capacity = charge(257, Alignment::none());
    for budget in [capacity - 1, capacity] {
        let memory = LiveMemoryPool::new(budget).unwrap();
        let consumer = session(&memory);
        let result = consumer
            .open_options()
            .with_footer(footer.clone())
            .include_metadata()
            .open_buffer(bytes.clone());
        if budget < capacity {
            let error = result.err().expect("one-byte-short metadata grant");
            assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
            continue;
        }
        let file = result.unwrap();
        let payload = file.metadata_segment("owner-test").unwrap().clone();
        assert_eq!(payload.as_slice(), &[0x5a; 257]);
        let slice = payload.slice(3..250);
        drop((file, consumer, payload));
        assert_eq!(memory.snapshot().reserved_bytes, capacity);
        assert_eq!(slice.as_slice(), &[0x5a; 247]);
        drop(slice);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(memory.snapshot().peak_reserved_bytes, capacity);
    }
}

#[test]
fn native_file_metadata_disabled_payload_does_not_spend_receiving_credit() {
    let (bytes, footer) = fixture();
    let memory = LiveMemoryPool::new(1).unwrap();
    let file = session(&memory)
        .open_options()
        .with_footer(footer)
        .with_include_metadata(false)
        .open_buffer(bytes)
        .unwrap();
    assert!(file.metadata_segment("owner-test").is_none());
    assert_eq!(file.metadata_segments().count(), 0);
    drop(file);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 0);
}

#[test]
fn native_file_metadata_later_payload_denial_releases_prior_copies() {
    let array = PrimitiveArray::from_option_iter([Some(-9_i64), None, Some(17)]).into_array();
    let (bytes, footer) = write_fixture_metadata(&array, &[("first", 129), ("second", 257)]);
    let capacity = charge(129, Alignment::none()) + charge(257, Alignment::none());
    let memory = LiveMemoryPool::new(capacity - 1).unwrap();
    let error = session(&memory)
        .open_options()
        .with_footer(footer)
        .include_metadata()
        .open_buffer(bytes)
        .err()
        .expect("second metadata payload exceeds the shared grant");
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert!(memory.snapshot().peak_reserved_bytes > 0);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 1);
}

#[test]
fn native_file_metadata_empty_and_all_null_files_preserve_schema_and_values() {
    for values in [vec![], vec![None::<i64>, None, None]] {
        let len = values.len();
        let expected = StructArray::try_new(
            FieldNames::from(["metric"]),
            vec![PrimitiveArray::from_option_iter(values).into_array()],
            len,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();
        let (bytes, original) = write_fixture(&expected);
        let memory = LiveMemoryPool::new(4 << 20).unwrap();
        let runtime = CurrentThreadRuntime::new();
        let consumer = session(&memory).with_handle(runtime.handle());
        let file = consumer.open_options().open_buffer(bytes).unwrap();
        assert_eq!(file.dtype(), original.dtype());
        assert_values(&file, &expected, &runtime);
        drop((file, consumer));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert!(memory.snapshot().peak_reserved_bytes > 0);
    }
}

fn wide_fixture() -> (ArrayRef, Vec<u8>, Footer) {
    let column = PrimitiveArray::from_option_iter([Some(-9_i64), None, Some(17)]).into_array();
    let names = (0..128)
        .map(|index| FieldName::from(format!("field_{index}_{}", "méta".repeat(256))))
        .collect::<Vec<_>>();
    let array = StructArray::try_new(names.into(), vec![column; 128], 3, Validity::NonNullable)
        .unwrap()
        .into_array();
    let (bytes, footer) = write_fixture(&array);
    (array, bytes, footer)
}

#[test]
fn native_file_metadata_wide_footer_growth_requires_admission_before_copying() {
    let (_, bytes, footer) = wide_fixture();
    let tail_size = usize::from(MAX_POSTSCRIPT_SIZE) + EOF_SIZE;
    assert!(bytes.len() > tail_size);
    let tail = ByteBuffer::copy_from(&bytes[bytes.len() - tail_size..]);
    let mut parser = Footer::deserializer(tail, VortexSession::default())
        .with_size(u64::try_from(bytes.len()).unwrap());
    let DeserializeStep::NeedMoreData { len, .. } = parser.deserialize().unwrap() else {
        panic!("wide schema must extend the initial native footer read");
    };
    let capacity = charge(tail_size + len, Alignment::none());
    let metadata = u64::try_from(
        footer
            .metadata_segments()
            .map(|(key, _)| size_of::<(String, vortex::file::SegmentSpec)>() + key.len())
            .sum::<usize>(),
    )
    .unwrap();
    for budget in [capacity - 1, metadata + capacity - 1] {
        let memory = LiveMemoryPool::new(budget).unwrap();
        let error = session(&memory)
            .open_options()
            .open_buffer(bytes.clone())
            .err()
            .expect("one-byte-short footer growth grant");
        assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(memory.snapshot().peak_reserved_bytes, metadata);
        assert_eq!(memory.snapshot().denied_reservations, 1);
    }

    let memory = LiveMemoryPool::new(metadata + capacity).unwrap();
    let consumer = session(&memory);
    let tail = ByteBuffer::copy_from(&bytes[bytes.len() - tail_size..]);
    let mut parser =
        Footer::deserializer(tail, consumer.clone()).with_size(u64::try_from(bytes.len()).unwrap());
    let DeserializeStep::NeedMoreData { offset, len } = parser.deserialize().unwrap() else {
        panic!("wide schema must still require footer growth");
    };
    assert_eq!(memory.snapshot().reserved_bytes, metadata);
    let start = usize::try_from(offset).unwrap();
    parser
        .try_prefix_data(ByteBuffer::copy_from(&bytes[start..start + len]))
        .unwrap();
    assert_eq!(parser.buffer().as_slice(), &bytes[start..]);
    assert_eq!(memory.snapshot().reserved_bytes, metadata + capacity);
    assert_eq!(memory.snapshot().peak_reserved_bytes, metadata + capacity);
    drop((parser, consumer));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_file_metadata_wide_footer_preserves_complete_values_and_lazy_children() {
    let (expected, bytes, original) = wide_fixture();
    let memory = LiveMemoryPool::new(8 << 20).unwrap();
    let runtime = CurrentThreadRuntime::new();
    let consumer = session(&memory).with_handle(runtime.handle());
    let file = consumer.open_options().open_buffer(bytes).unwrap();
    assert_eq!(file.dtype(), original.dtype());
    assert_values(&file, &expected, &runtime);
    let children = file.footer().layout().children().unwrap();
    assert!(!children.is_empty());
    drop((file, consumer));
    assert!(memory.snapshot().reserved_bytes > 0);
    assert!(children.iter().all(|child| child.row_count() == 3));
    drop(children);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_file_metadata_malformed_schema_and_layout_release_copied_storage() {
    let (bytes, _) = fixture();
    let eof = bytes.len() - EOF_SIZE;
    let postscript_len = usize::from(u16::from_le_bytes(
        bytes[eof + 2..eof + 4].try_into().unwrap(),
    ));
    let postscript = flatbuffers::root::<vortex::flatbuffers::footer::Postscript<'_>>(
        &bytes[eof - postscript_len..eof],
    )
    .unwrap();
    let offsets = [
        postscript.dtype().unwrap().offset(),
        postscript.layout().unwrap().offset(),
    ];
    for offset in offsets {
        let mut malformed = bytes.clone();
        let offset = usize::try_from(offset).unwrap();
        malformed[offset..offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let memory = LiveMemoryPool::new(1 << 20).unwrap();
        let error = session(&memory)
            .open_options()
            .open_buffer(malformed)
            .err()
            .expect("malformed FlatBuffer root offset");
        assert!(!crate::owned_buffers::is_owned_reservation_denial(&error));
        assert!(memory.snapshot().peak_reserved_bytes > 0);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(memory.snapshot().denied_reservations, 0);
    }
}

#[test]
fn native_file_metadata_nested_schema_and_values_survive_native_roundtrip() {
    let nested = StructArray::try_new(
        FieldNames::from(["metric", "enabled"]),
        vec![
            PrimitiveArray::from_option_iter([Some(-9_i64), None, Some(17)]).into_array(),
            BoolArray::from_iter([Some(true), None, Some(false)]).into_array(),
        ],
        3,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let expected = StructArray::try_new(
        FieldNames::from(["nested"]),
        vec![nested],
        3,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let (bytes, original) = write_fixture(&expected);
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let runtime = CurrentThreadRuntime::new();
    let consumer = session(&memory).with_handle(runtime.handle());
    let file = consumer.open_options().open_buffer(bytes).unwrap();
    assert_eq!(file.dtype(), original.dtype());
    assert_values(&file, &expected, &runtime);
    let dtype = file.dtype().clone();
    drop((file, consumer));
    assert_eq!(&dtype, expected.dtype());
    assert!(memory.snapshot().reserved_bytes > 0);
    drop(dtype);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_file_metadata_cached_realignment_preserves_sharing_and_exact_admission() {
    let (bytes, footer) = fixture();
    let runtime = CurrentThreadRuntime::new();
    let aligned =
        ByteBuffer::copy_from_aligned(&bytes, Alignment::new(64)).aligned(Alignment::none());
    let mut prefixed = vec![0];
    prefixed.extend_from_slice(&bytes);
    let misaligned = ByteBuffer::copy_from_aligned(prefixed, Alignment::new(64))
        .aligned(Alignment::none())
        .slice(1..);
    let copies = footer
        .segment_map()
        .iter()
        .chain(footer.metadata_segments().map(|(_, segment)| segment))
        .filter(|segment| {
            let offset = usize::try_from(segment.offset).unwrap();
            assert!(aligned.slice(offset..).is_aligned(segment.alignment));
            !misaligned.slice(offset..).is_aligned(segment.alignment)
        })
        .map(|segment| charge(usize::try_from(segment.length).unwrap(), segment.alignment))
        .sum::<u64>();
    assert!(
        copies > 0,
        "fixture must exercise cached segment realignment"
    );
    let memory = LiveMemoryPool::new(4 << 20).unwrap();
    let consumer = session(&memory).with_handle(runtime.handle());
    let file = runtime
        .block_on(
            consumer
                .open_options()
                .with_initial_read_size(bytes.len())
                .open_read(aligned),
        )
        .unwrap();
    let parser_bytes = memory.snapshot().reserved_bytes;
    assert!(parser_bytes > 0);
    assert_eq!(memory.snapshot().peak_reserved_bytes, parser_bytes);
    drop((file, consumer));
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    for budget in [parser_bytes + copies - 1, parser_bytes + copies] {
        let memory = LiveMemoryPool::new(budget).unwrap();
        let consumer = session(&memory).with_handle(runtime.handle());
        let result = runtime.block_on(
            consumer
                .open_options()
                .with_initial_read_size(bytes.len())
                .open_read(misaligned.clone()),
        );
        if budget < parser_bytes + copies {
            let error = result.err().expect("one-byte-short cached segment grant");
            assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            assert_eq!(memory.snapshot().denied_reservations, 1);
            continue;
        }
        let file = result.unwrap();
        assert_eq!(file.dtype(), footer.dtype());
        assert_eq!(memory.snapshot().reserved_bytes, budget);
        assert_eq!(memory.snapshot().peak_reserved_bytes, budget);
        assert_cached_segments(&file, &bytes, &runtime);
        drop((file, consumer));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

fn aligned_metadata_fixture() -> (ArrayRef, Vec<u8>, Footer) {
    let array = PrimitiveArray::from_iter([1_i64, 2, 3]).into_array();
    let runtime = CurrentThreadRuntime::new();
    let writer = VortexSession::default().with_handle(runtime.handle());
    let metadata = ByteBuffer::copy_from_aligned(vec![0x5a; 4097], Alignment::new(4096));
    let mut bytes = Vec::new();
    let summary = runtime
        .block_on(
            writer
                .write_options()
                .with_metadata_segment("aligned-metadata", metadata)
                .write(&mut bytes, array.to_array_stream()),
        )
        .unwrap();
    (array, bytes, summary.footer().clone())
}

fn shifted_file_buffer(bytes: &[u8], shift: usize) -> ByteBuffer {
    let mut prefixed = vec![0; shift];
    prefixed.extend_from_slice(bytes);
    ByteBuffer::copy_from_aligned(prefixed, Alignment::new(4096))
        .aligned(Alignment::none())
        .slice(shift..)
}

#[test]
fn native_file_metadata_review_memory_segment_alignment_is_admitted_and_owned() {
    let (_, bytes, footer) = aligned_metadata_fixture();
    let segment = footer.metadata_segment("aligned-metadata").unwrap();
    let id = vortex::layout::segments::SegmentId::from(
        u32::try_from(footer.segment_map().len()).unwrap(),
    );
    let capacity = charge(segment.length as usize, segment.alignment);
    let offset = usize::try_from(segment.offset).unwrap();
    let misaligned = shifted_file_buffer(&bytes, 1);
    assert!(!misaligned.slice(offset..).is_aligned(segment.alignment));
    let runtime = CurrentThreadRuntime::new();
    for budget in [capacity - 1, capacity] {
        let memory = LiveMemoryPool::new(budget).unwrap();
        let consumer = session(&memory).with_handle(runtime.handle());
        let file = consumer
            .open_options()
            .with_footer(footer.clone())
            .open_buffer(misaligned.clone())
            .unwrap();
        let result = runtime.block_on(file.segment_source().request(id));
        if budget < capacity {
            let error = result.expect_err("alignment copy must be admitted first");
            assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
            assert_eq!(memory.snapshot().denied_reservations, 1);
            continue;
        }
        let buffer = runtime
            .block_on(result.unwrap().try_into_host().unwrap())
            .unwrap();
        assert!(buffer.is_aligned(segment.alignment));
        assert_eq!(buffer.as_slice(), &[0x5a; 4097]);
        let clone = buffer.clone();
        drop((file, consumer, buffer));
        assert_eq!(memory.snapshot().reserved_bytes, capacity);
        assert_eq!(memory.snapshot().peak_reserved_bytes, capacity);
        drop(clone);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }

    let memory = LiveMemoryPool::new(1).unwrap();
    let aligned = shifted_file_buffer(&bytes, 0);
    let expected_pointer = aligned.slice(offset..).as_ptr();
    let file = session(&memory)
        .open_options()
        .with_footer(footer)
        .open_buffer(aligned)
        .unwrap();
    let handle = runtime.block_on(file.segment_source().request(id)).unwrap();
    let buffer = runtime.block_on(handle.try_into_host().unwrap()).unwrap();
    assert_eq!(buffer.as_ptr(), expected_pointer);
    assert_eq!(buffer.as_slice(), &[0x5a; 4097]);
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 0);
}

#[test]
fn native_file_metadata_review_requested_payload_accounts_for_live_alignment_copy() {
    let (_, bytes, footer) = aligned_metadata_fixture();
    let segment = footer.metadata_segment("aligned-metadata").unwrap();
    let capacity = charge(segment.length as usize, segment.alignment);
    let misaligned = shifted_file_buffer(&bytes, 1);
    assert!(
        !misaligned
            .slice(usize::try_from(segment.offset).unwrap()..)
            .is_aligned(segment.alignment)
    );
    for budget in [2 * capacity - 1, 2 * capacity] {
        let memory = LiveMemoryPool::new(budget).unwrap();
        let consumer = session(&memory);
        let result = consumer
            .open_options()
            .with_footer(footer.clone())
            .include_metadata()
            .open_buffer(misaligned.clone());
        if budget < 2 * capacity {
            let error = result
                .err()
                .expect("both live metadata buffers need credit");
            assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
            assert_eq!(memory.snapshot().peak_reserved_bytes, capacity);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            assert_eq!(memory.snapshot().denied_reservations, 1);
            continue;
        }
        let file = result.unwrap();
        let metadata = file.metadata_segment("aligned-metadata").unwrap().clone();
        assert!(metadata.is_aligned(segment.alignment));
        assert_eq!(metadata.as_slice(), &[0x5a; 4097]);
        assert_eq!(memory.snapshot().peak_reserved_bytes, 2 * capacity);
        drop((file, consumer));
        assert_eq!(memory.snapshot().reserved_bytes, capacity);
        drop(metadata);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_file_metadata_review_default_open_skips_optional_alignment_copy() {
    let (expected, bytes, footer) = aligned_metadata_fixture();
    let runtime = CurrentThreadRuntime::new();
    let data_alignment = footer
        .segment_map()
        .iter()
        .map(|segment| *segment.alignment)
        .max()
        .unwrap();
    let metadata = footer.metadata_segment("aligned-metadata").unwrap();
    assert!(data_alignment < *metadata.alignment);
    let shifted = shifted_file_buffer(&bytes, data_alignment);
    for segment in footer.segment_map().iter() {
        assert!(
            shifted
                .slice(usize::try_from(segment.offset).unwrap()..)
                .is_aligned(segment.alignment)
        );
    }
    assert!(
        !shifted
            .slice(usize::try_from(metadata.offset).unwrap()..)
            .is_aligned(metadata.alignment)
    );

    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let consumer = session(&memory).with_handle(runtime.handle());
    let control = runtime
        .block_on(
            consumer
                .open_options()
                .with_initial_read_size(bytes.len())
                .open_read(shifted_file_buffer(&bytes, 0)),
        )
        .unwrap();
    let parser_bytes = memory.snapshot().reserved_bytes;
    assert!(parser_bytes > 0);
    assert_eq!(memory.snapshot().peak_reserved_bytes, parser_bytes);
    drop((control, consumer));
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let memory = LiveMemoryPool::new(parser_bytes).unwrap();
    let consumer = session(&memory).with_handle(runtime.handle());
    let file = runtime
        .block_on(
            consumer
                .open_options()
                .with_initial_read_size(bytes.len())
                .open_read(shifted.clone()),
        )
        .expect("default open must not copy unused optional metadata");
    assert_eq!(file.metadata_segments().count(), 0);
    assert_eq!(file.dtype(), expected.dtype());
    assert_eq!(file.footer().row_count(), expected.len() as u64);
    assert_eq!(memory.snapshot().reserved_bytes, parser_bytes);
    assert_eq!(memory.snapshot().peak_reserved_bytes, parser_bytes);
    assert_eq!(memory.snapshot().denied_reservations, 0);
    drop((file, consumer));
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let consumer = session(&memory).with_handle(runtime.handle());
    let file = runtime
        .block_on(
            consumer
                .open_options()
                .include_metadata()
                .with_initial_read_size(bytes.len())
                .open_read(shifted),
        )
        .unwrap();
    assert_eq!(
        file.metadata_segment("aligned-metadata")
            .unwrap()
            .as_slice(),
        &[0x5a; 4097]
    );
    assert_values(&file, &expected, &runtime);
    drop((file, consumer));
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_file_metadata_review_buffer_open_does_not_build_an_unused_cache() {
    let (bytes, footer) = fixture();
    let memory = LiveMemoryPool::new(1 << 20).unwrap();
    let control = session(&memory)
        .open_options()
        .open_buffer(shifted_file_buffer(&bytes, 0))
        .unwrap();
    let parser_bytes = memory.snapshot().reserved_bytes;
    assert!(parser_bytes > 0);
    assert_eq!(memory.snapshot().peak_reserved_bytes, parser_bytes);
    drop(control);
    assert_eq!(memory.snapshot().reserved_bytes, 0);

    let shifted = shifted_file_buffer(&bytes, 1);
    assert!(footer.segment_map().iter().any(|segment| {
        !shifted
            .slice(usize::try_from(segment.offset).unwrap()..)
            .is_aligned(segment.alignment)
    }));
    let memory = LiveMemoryPool::new(parser_bytes).unwrap();
    let file = session(&memory)
        .open_options()
        .open_buffer(shifted)
        .expect("in-memory opens must not populate the unused tail cache");
    assert_eq!(file.dtype(), footer.dtype());
    assert_eq!(file.footer().row_count(), footer.row_count());
    assert_eq!(memory.snapshot().reserved_bytes, parser_bytes);
    assert_eq!(memory.snapshot().peak_reserved_bytes, parser_bytes);
    assert_eq!(memory.snapshot().denied_reservations, 0);
    drop(file);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}
