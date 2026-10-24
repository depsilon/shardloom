//! Native file-statistics table admission, independent of scalar/schema payloads.

use std::sync::{Arc, Mutex};

use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{
    VortexSessionDefault as _,
    array::{
        dtype::{DType, FieldName, FieldNames, Nullability, PType, StructFields},
        expr::stats::{Precision, Stat},
        memory::{
            DefaultHostAllocator, HostAllocator, HostMemoryReservation, HostMemorySessionExt as _,
            WritableHostBuffer,
        },
        stats::StatsSet,
    },
    buffer::Alignment,
    error::VortexResult,
    file::FileStatistics,
    flatbuffers::{FlatBuffer, WriteFlatBufferExt as _},
    session::VortexSession,
};

#[derive(Debug)]
struct TableAllocator {
    tables: crate::owned_buffers::ReservedHostAllocator,
    requests: Arc<Mutex<Vec<usize>>>,
}

impl HostAllocator for TableAllocator {
    fn allocate(&self, bytes: usize, alignment: Alignment) -> VortexResult<WritableHostBuffer> {
        // Copied serialized bytes have their own provider regressions. Isolate
        // the independently allocated tables from those existing buffer charges.
        DefaultHostAllocator.allocate(bytes, alignment)
    }

    fn reserve_external(&self, bytes: usize) -> VortexResult<HostMemoryReservation> {
        self.requests.lock().unwrap().push(bytes);
        self.tables.reserve_external(bytes)
    }
}

fn session(memory: &LiveMemoryPool) -> (VortexSession, Arc<Mutex<Vec<usize>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let consumer = VortexSession::default().with_host_allocator(Arc::new(TableAllocator {
        tables: crate::owned_buffers::ReservedHostAllocator::new(memory.clone()),
        requests: Arc::clone(&requests),
    }));
    (consumer, requests)
}

fn field_dtype(index: usize) -> DType {
    match index % 3 {
        0 => DType::Primitive(PType::I64, Nullability::Nullable),
        1 => DType::Bool(Nullability::Nullable),
        _ => DType::Utf8(Nullability::Nullable),
    }
}

fn dtype(width: Option<usize>) -> DType {
    width.map_or_else(
        || field_dtype(0),
        |width| {
            DType::Struct(
                StructFields::new(
                    FieldNames::from(
                        (0..width)
                            .map(|index| FieldName::from(format!("field_{index}")))
                            .collect::<Vec<_>>(),
                    ),
                    (0..width).map(field_dtype).collect(),
                ),
                Nullability::NonNullable,
            )
        },
    )
}

fn stat(index: usize) -> StatsSet {
    StatsSet::of(
        Stat::NullCount,
        Precision::Exact(u64::try_from(index).unwrap().into()),
    )
}

fn fixture(width: Option<usize>) -> (FlatBuffer, DType) {
    let dtype = dtype(width);
    let count = width.unwrap_or(1);
    let statistics = FileStatistics::new_with_dtype((0..count).map(stat).collect(), &dtype);
    (statistics.write_flatbuffer_bytes().unwrap(), dtype)
}

fn reopen(
    bytes: &FlatBuffer,
    dtype: &DType,
    session: &VortexSession,
) -> VortexResult<FileStatistics> {
    let root =
        flatbuffers::root::<vortex::file::flatbuffers::footer::FileStatistics<'_>>(bytes).unwrap();
    FileStatistics::from_flatbuffer(&root, dtype, session)
}

fn table_bytes(count: usize) -> u64 {
    u64::try_from(count * (size_of::<StatsSet>() + size_of::<DType>())).unwrap()
}

fn assert_fields(statistics: &FileStatistics, count: usize) {
    assert_eq!(statistics.stats_sets().len(), count);
    assert_eq!(statistics.dtypes().len(), count);
    for (index, (stats, dtype)) in statistics.into_iter().enumerate() {
        assert_eq!(dtype, &field_dtype(index));
        assert_eq!(
            stats.get(Stat::NullCount),
            Precision::Exact(u64::try_from(index).unwrap().into())
        );
        let (indexed_stats, indexed_dtype) = statistics.get(index);
        assert!(std::ptr::eq(stats, indexed_stats));
        assert!(std::ptr::eq(dtype, indexed_dtype));
    }
}

#[test]
fn native_file_statistics_parsed_tables_require_exact_admission() {
    for width in [None, Some(1), Some(2), Some(7), Some(8), Some(128)] {
        let count = width.unwrap_or(1);
        let expected = table_bytes(count);
        let (bytes, dtype) = fixture(width);
        let memory = LiveMemoryPool::new(expected).unwrap();
        let (consumer, requests) = session(&memory);
        let parsed = reopen(&bytes, &dtype, &consumer).unwrap();
        assert_eq!(
            requests.lock().unwrap().as_slice(),
            &[usize::try_from(expected).unwrap()]
        );
        assert_eq!(memory.snapshot().reserved_bytes, expected);
        assert_fields(&parsed, count);
        drop((parsed, consumer));
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_file_statistics_parsed_tables_deny_one_byte_short() {
    for width in [None, Some(128)] {
        let count = width.unwrap_or(1);
        let expected = table_bytes(count);
        let (bytes, dtype) = fixture(width);
        let memory = LiveMemoryPool::new(expected - 1).unwrap();
        let (consumer, requests) = session(&memory);
        let error = reopen(&bytes, &dtype, &consumer).unwrap_err();
        assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
        assert_eq!(
            requests.lock().unwrap().as_slice(),
            &[usize::try_from(expected).unwrap()]
        );
        assert_eq!(memory.snapshot().reserved_bytes, 0);
        assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
        assert_eq!(memory.snapshot().denied_reservations, 1);
    }
}

#[test]
fn native_file_statistics_parsed_clone_keeps_both_tables_after_session() {
    let count = 128;
    let expected = table_bytes(count);
    let (bytes, dtype) = fixture(Some(count));
    let memory = LiveMemoryPool::new(expected).unwrap();
    let (consumer, _) = session(&memory);
    let parsed = reopen(&bytes, &dtype, &consumer).unwrap();
    let clone = parsed.clone();
    let stats_pointer = parsed.stats_sets().as_ptr();
    let dtypes_pointer = parsed.dtypes().as_ptr();
    drop((parsed, bytes, dtype, consumer));
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    assert_eq!(clone.stats_sets().as_ptr(), stats_pointer);
    assert_eq!(clone.dtypes().as_ptr(), dtypes_pointer);
    assert_fields(&clone, count);
    drop(clone);
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
fn native_file_statistics_empty_and_custom_allocator_boundaries() {
    let memory = LiveMemoryPool::new(1).unwrap();
    let (consumer, requests) = session(&memory);
    let (bytes, dtype) = fixture(Some(0));
    let empty = reopen(&bytes, &dtype, &consumer).unwrap();
    assert_fields(&empty, 0);
    assert!(requests.lock().unwrap().is_empty());
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    let consumer = VortexSession::default().with_host_allocator(Arc::new(BufferOnlyAllocator));
    assert_fields(&reopen(&bytes, &dtype, &consumer).unwrap(), 0);
    assert_fields(
        &FileStatistics::try_from_fn(0, |_| panic!("empty producer"), &consumer).unwrap(),
        0,
    );
    let (bytes, dtype) = fixture(None);
    let error = reopen(&bytes, &dtype, &consumer).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not support external memory reservations")
    );
}

#[test]
fn native_file_statistics_caller_owned_compatibility_preserves_shared_tables() {
    let stats: Arc<[StatsSet]> = Arc::from([stat(0), stat(1)]);
    let dtypes: Arc<[DType]> = Arc::from([field_dtype(0), field_dtype(1)]);
    let table = FileStatistics::new(Arc::clone(&stats), Arc::clone(&dtypes));
    let clone = table.clone();
    drop(table);
    assert_eq!(clone.stats_sets().as_ptr(), stats.as_ptr());
    assert_eq!(clone.dtypes().as_ptr(), dtypes.as_ptr());
    assert_fields(&clone, 2);
    assert_eq!(Arc::strong_count(&stats), 2);
    assert_eq!(Arc::strong_count(&dtypes), 2);
    drop(clone);
    assert_eq!(Arc::strong_count(&stats), 1);
    assert_eq!(Arc::strong_count(&dtypes), 1);
}

#[test]
fn native_file_statistics_count_overflow_precedes_admission_and_production() {
    let memory = LiveMemoryPool::new(1).unwrap();
    let (consumer, requests) = session(&memory);
    for count in [
        usize::MAX,
        (isize::MAX as usize) / size_of::<StatsSet>() + 1,
    ] {
        let error = FileStatistics::try_from_fn(count, |_| panic!("overflow producer"), &consumer)
            .unwrap_err();
        assert!(error.to_string().contains("allocation size overflow"));
    }
    assert!(requests.lock().unwrap().is_empty());
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 0);
}

#[test]
fn native_file_statistics_cardinality_mismatch_precedes_admission() {
    let memory = LiveMemoryPool::new(1).unwrap();
    let (consumer, requests) = session(&memory);
    for (supplied, expected) in [
        (Some(0), None),
        (Some(2), None),
        (None, Some(2)),
        (Some(3), Some(2)),
    ] {
        let (bytes, _) = fixture(supplied);
        let error = reopen(&bytes, &dtype(expected), &consumer).unwrap_err();
        assert!(!crate::owned_buffers::is_owned_reservation_denial(&error));
        assert!(error.to_string().contains("Assertion failed"));
    }
    assert!(requests.lock().unwrap().is_empty());
    assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 0);
}

#[test]
fn native_file_statistics_producer_failure_releases_partial_tables() {
    let expected = table_bytes(8);
    let memory = LiveMemoryPool::new(expected).unwrap();
    let (consumer, requests) = session(&memory);
    let mut visited = Vec::new();
    let error = FileStatistics::try_from_fn(
        8,
        |index| {
            visited.push(index);
            if index == 3 {
                return Err(vortex::error::vortex_err!(
                    "injected statistics producer error"
                ));
            }
            Ok((stat(index), field_dtype(index)))
        },
        &consumer,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected statistics producer error")
    );
    assert_eq!(visited, vec![0, 1, 2, 3]);
    assert_eq!(
        requests.lock().unwrap().as_slice(),
        &[usize::try_from(expected).unwrap()]
    );
    assert_eq!(memory.snapshot().peak_reserved_bytes, expected);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    assert_eq!(memory.snapshot().denied_reservations, 0);
}

#[test]
fn native_file_statistics_retained_peer_denial_and_retry_preserve_values() {
    let count = 8;
    let expected = table_bytes(count);
    let memory = LiveMemoryPool::new(expected).unwrap();
    let (consumer, _) = session(&memory);
    let (bytes, dtype) = fixture(Some(count));
    let table = reopen(&bytes, &dtype, &consumer).unwrap();
    let alias = table.clone();
    let error =
        FileStatistics::try_from_fn(count, |_| panic!("denied producer"), &consumer).unwrap_err();
    assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    assert_fields(&alias, count);
    drop(table);
    assert_eq!(memory.snapshot().reserved_bytes, expected);
    drop(alias);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let retried = reopen(&bytes, &dtype, &consumer).unwrap();
    assert_fields(&retried, count);
    assert_eq!(memory.snapshot().peak_reserved_bytes, expected);
    assert_eq!(memory.snapshot().denied_reservations, 1);
    drop(retried);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_file_statistics_parser_failure_releases_partial_tables() {
    use vortex::file::flatbuffers::footer as fb;
    use vortex::flatbuffers::array as fba;

    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let good = fba::ArrayStats::create(
        &mut builder,
        &fba::ArrayStatsArgs {
            null_count: Some(0),
            ..Default::default()
        },
    );
    // A truncated protobuf varint is structurally valid FlatBuffer bytes but
    // cannot produce the second scalar. The first table entry has already parsed.
    let invalid = builder.create_vector(&[0x80_u8]);
    let bad = fba::ArrayStats::create(
        &mut builder,
        &fba::ArrayStatsArgs {
            min: Some(invalid),
            ..Default::default()
        },
    );
    let fields = builder.create_vector(&[good, bad]);
    let root = fb::FileStatistics::create(
        &mut builder,
        &fb::FileStatisticsArgs {
            field_stats: Some(fields),
        },
    );
    builder.finish(root, None);
    let root = flatbuffers::root::<fb::FileStatistics<'_>>(builder.finished_data()).unwrap();
    let expected = table_bytes(2);
    let memory = LiveMemoryPool::new(expected).unwrap();
    let (consumer, requests) = session(&memory);
    let error = FileStatistics::from_flatbuffer(&root, &dtype(Some(2)), &consumer).unwrap_err();
    assert!(!crate::owned_buffers::is_owned_reservation_denial(&error));
    assert_eq!(
        requests.lock().unwrap().as_slice(),
        &[usize::try_from(expected).unwrap()]
    );
    assert_eq!(memory.snapshot().peak_reserved_bytes, expected);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
    let (valid, dtype) = fixture(Some(2));
    let retried = reopen(&valid, &dtype, &consumer).unwrap();
    assert_fields(&retried, 2);
    drop(retried);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_file_statistics_preserve_all_values_precision_and_serialized_bytes() {
    let dtype = DType::Primitive(PType::I64, Nullability::Nullable);
    let mut stats = StatsSet::default();
    let expected = [
        (Stat::Min, Precision::Inexact((-9_i64).into())),
        (Stat::Max, Precision::Exact(17_i64.into())),
        (Stat::Sum, Precision::Exact(8_i64.into())),
        (Stat::NullCount, Precision::Exact(1_u64.into())),
        (
            Stat::UncompressedSizeInBytes,
            Precision::Exact(33_u64.into()),
        ),
        (Stat::NaNCount, Precision::Exact(0_u64.into())),
        (Stat::IsConstant, Precision::Exact(false.into())),
        (Stat::IsSorted, Precision::Exact(true.into())),
        (Stat::IsStrictSorted, Precision::Exact(false.into())),
    ];
    for (stat, value) in &expected {
        stats.set(*stat, value.clone());
    }
    let original = FileStatistics::new(Arc::from([stats]), Arc::from([dtype.clone()]));
    let bytes = original.write_flatbuffer_bytes().unwrap();
    let memory = LiveMemoryPool::new(table_bytes(1)).unwrap();
    let (consumer, _) = session(&memory);
    let parsed = reopen(&bytes, &dtype, &consumer).unwrap();
    for (stat, value) in expected {
        assert_eq!(parsed.stats_sets()[0].get(stat), value);
    }
    assert_eq!(parsed.dtypes(), &[dtype]);
    assert_eq!(
        parsed.write_flatbuffer_bytes().unwrap().as_slice(),
        bytes.as_slice()
    );
    drop(parsed);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

fn assert_file_values(
    file: &vortex::file::VortexFile,
    expected: &vortex::array::ArrayRef,
    runtime: &vortex::io::runtime::current::CurrentThreadRuntime,
) {
    use vortex::array::VortexSessionExecute as _;

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
        for row in 0..batch.len() {
            assert_eq!(
                batch.execute_scalar(row, &mut context).unwrap(),
                expected.execute_scalar(seen, &mut reference).unwrap()
            );
            seen += 1;
        }
    }
    assert_eq!(seen, expected.len());
}

#[test]
fn native_file_statistics_writer_admits_before_table_production_and_retains_footer_clone() {
    use vortex::{
        array::{IntoArray as _, arrays::PrimitiveArray},
        file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _},
        io::{
            runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
            session::RuntimeSessionExt as _,
        },
    };

    let expected = table_bytes(1);
    let array = PrimitiveArray::from_option_iter([Some(-9_i64), None, Some(17)]).into_array();
    for limit in [expected - 1, expected] {
        let runtime = CurrentThreadRuntime::new();
        let memory = LiveMemoryPool::new(limit).unwrap();
        let (consumer, requests) = session(&memory);
        let consumer = consumer.with_handle(runtime.handle());
        let mut output = Vec::new();
        let result = runtime.block_on(
            consumer
                .write_options()
                .with_file_statistics(vec![Stat::Min, Stat::Max, Stat::NullCount])
                .with_strategy(Arc::new(
                    vortex::layout::layouts::flat::writer::FlatLayoutStrategy::default(),
                ))
                .write(&mut output, array.to_array_stream()),
        );
        assert_eq!(
            requests.lock().unwrap().as_slice(),
            &[usize::try_from(expected).unwrap()]
        );
        if limit < expected {
            let error = result.err().expect("short writer table grant must fail");
            assert!(crate::owned_buffers::is_owned_reservation_denial(&error));
            assert_eq!(memory.snapshot().peak_reserved_bytes, 0);
            assert_eq!(memory.snapshot().reserved_bytes, 0);
            continue;
        }
        let summary = result.unwrap();
        let stats = summary.footer().statistics().unwrap().clone();
        drop((summary, consumer));
        assert_eq!(memory.snapshot().reserved_bytes, expected);
        assert_eq!(stats.dtypes(), &[array.dtype().clone()]);
        assert_eq!(
            stats.stats_sets()[0].get(Stat::Min),
            Precision::Exact((-9_i64).into())
        );
        assert_eq!(
            stats.stats_sets()[0].get(Stat::Max),
            Precision::Exact(17_i64.into())
        );
        assert_eq!(
            stats.stats_sets()[0].get(Stat::NullCount),
            Precision::Exact(1_u64.into())
        );
        let reader = VortexSession::default().with_handle(runtime.handle());
        let reopened = reader.open_options().open_buffer(output).unwrap();
        assert_eq!(reopened.footer().row_count(), 3);
        assert_eq!(reopened.dtype(), array.dtype());
        assert_file_values(&reopened, &array, &runtime);
        assert_eq!(
            reopened
                .footer()
                .statistics()
                .unwrap()
                .write_flatbuffer_bytes()
                .unwrap()
                .as_slice(),
            stats.write_flatbuffer_bytes().unwrap().as_slice()
        );
        drop(stats);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}

#[test]
fn native_file_statistics_both_open_paths_preserve_values_and_independent_statistics() {
    use vortex::{
        array::{
            IntoArray as _,
            arrays::{BoolArray, PrimitiveArray, StructArray, VarBinViewArray},
            validity::Validity,
        },
        buffer::ByteBuffer,
        file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _},
        io::{
            runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
            session::RuntimeSessionExt as _,
        },
    };

    let runtime = CurrentThreadRuntime::new();
    let writer = VortexSession::default().with_handle(runtime.handle());
    let array = StructArray::try_new(
        FieldNames::from(["number", "flag", "text"]),
        vec![
            PrimitiveArray::from_option_iter([Some(-9_i64), None, Some(17)]).into_array(),
            BoolArray::from_iter([Some(true), None, Some(false)]).into_array(),
            VarBinViewArray::from_iter_nullable_str([Some("東京の長い値 λ"), None, Some("")])
                .into_array(),
        ],
        3,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let mut output = Vec::new();
    let summary = runtime
        .block_on(
            writer
                .write_options()
                .write(&mut output, array.to_array_stream()),
        )
        .unwrap();
    let statistics_bytes = summary
        .footer()
        .statistics()
        .unwrap()
        .write_flatbuffer_bytes()
        .unwrap();
    let output = ByteBuffer::from(output);
    for reader_backed in [false, true] {
        let memory = LiveMemoryPool::new(4 << 20).unwrap();
        let (consumer, _) = session(&memory);
        let consumer = consumer.with_handle(runtime.handle());
        let file = if reader_backed {
            runtime
                .block_on(consumer.open_options().open_read(output.clone()))
                .unwrap()
        } else {
            consumer.open_options().open_buffer(output.clone()).unwrap()
        };
        assert_file_values(&file, &array, &runtime);
        let statistics = file.footer().statistics().unwrap().clone();
        assert_eq!(
            statistics.write_flatbuffer_bytes().unwrap().as_slice(),
            statistics_bytes.as_slice()
        );
        let stats_pointer = statistics.stats_sets().as_ptr();
        let types_pointer = statistics.dtypes().as_ptr();
        drop((file, consumer));
        assert_eq!(memory.snapshot().reserved_bytes, table_bytes(3));
        assert_eq!(statistics.stats_sets().as_ptr(), stats_pointer);
        assert_eq!(statistics.dtypes().as_ptr(), types_pointer);
        for index in 0..3 {
            assert_eq!(statistics.get(index).1, &field_dtype(index));
            assert_eq!(
                statistics.get(index).0.get(Stat::NullCount),
                Precision::Exact(1_u64.into())
            );
        }
        drop(statistics);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
