use super::*;
use crate::{VortexAggregateOrderExpr, VortexSimpleAggregateMeasure, VortexSimpleAggregateRequest};
use shardloom_core::{ColumnRef, DatasetUri};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _,
        arrays::{PrimitiveArray, StructArray},
        dtype::Nullability,
        iter::ArrayIteratorAdapter,
        validity::Validity,
    },
    file::WriteOptionsSessionExt as _,
    io::{runtime::BlockingRuntime as _, session::RuntimeSessionExt as _},
    session::VortexSession,
};

const KEY: &str = "delivery_zone";
const VALUE: &str = "package_identifier";
const COUNT: &str = "distinct_packages";

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "shardloom-owned-aggregate-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn source(&self, keys: ArrayRef, values: Vec<u64>) -> PathBuf {
        let path = self.0.join("source.vortex");
        let runtime = super::super::local_vortex_runtime(
            VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        );
        let session = VortexSession::default().with_handle(runtime.handle());
        let rows = keys.len();
        assert_eq!(rows, values.len());
        let array = StructArray::new(
            [KEY, VALUE].into(),
            vec![
                keys,
                PrimitiveArray::new(values, Validity::NonNullable).into_array(),
            ],
            rows,
            Validity::NonNullable,
        )
        .into_array();
        let dtype = array.dtype().clone();
        // Contributions cross writer chunks; repeated keys and pairs cannot be
        // counted or ranked before all chunks have met.
        let chunks = if rows == 0 {
            vec![Ok(array)]
        } else {
            (0..rows)
                .step_by(3)
                .map(|start| array.slice(start..rows.min(start + 3)))
                .collect()
        };
        session
            .write_options()
            .blocking(&runtime)
            .write(
                fs::File::create(&path).unwrap(),
                ArrayIteratorAdapter::new(dtype, chunks.into_iter()),
            )
            .unwrap();
        path
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn request(path: &Path, offset: usize, limit: usize) -> VortexQueryPrimitiveRequest {
    let aggregate = VortexSimpleAggregateRequest::grouped(
        vec![ColumnRef::new(KEY).unwrap()],
        vec![VortexSimpleAggregateMeasure::new(
            "count_distinct",
            Some(ColumnRef::new(VALUE).unwrap()),
            COUNT.to_owned(),
        )],
    )
    .with_order_by(vec![
        VortexAggregateOrderExpr::new(COUNT, true),
        VortexAggregateOrderExpr::new(KEY, false),
    ])
    .with_offset(offset);
    VortexQueryPrimitiveRequest::simple_aggregate(
        DatasetUri::new(path.display().to_string()).unwrap(),
        aggregate,
    )
    .with_source_order_limit(limit)
}

fn rendered(result: &crate::resident_session::OwnedVortexResultBatch) -> serde_json::Value {
    let json = result
        .to_bounded_json(&[KEY.into(), COUNT.into()], 64 * 1024)
        .unwrap();
    serde_json::from_str(json.value()).unwrap()
}

fn standard(fixture: &Fixture) -> PathBuf {
    fixture.source(
        PrimitiveArray::new(
            vec![
                i64::MAX,
                i64::MIN,
                0,
                i64::MAX,
                i64::MIN,
                0,
                i64::MAX,
                i64::MIN,
                0,
                0,
            ],
            Validity::NonNullable,
        )
        .into_array(),
        vec![1, 1, 1, 2, 2, 2, 1, 1, 2, 3],
    )
}

fn expected() -> serde_json::Value {
    serde_json::json!([
        {KEY: 0, COUNT: 3},
        {KEY: i64::MIN, COUNT: 2},
        {KEY: i64::MAX, COUNT: 2},
    ])
}

#[test]
fn owned_distinct_preserves_complete_order_offsets_and_fresh_execution_under_pressure_handoff() {
    let fixture = Fixture::new();
    let path = standard(&fixture);
    for workers in [1, 2, 4] {
        for (offset, limit) in [(0, 10), (1, 1), (2, 3), (8, 2)] {
            let prepared = prepare_aggregate(
                &request(&path, offset, limit),
                VortexLocalPrimitiveExecutionPolicy::new(workers).unwrap(),
            )
            .unwrap();
            for (index, pressure) in [false, true, false].into_iter().enumerate() {
                super::super::aggregate_count_workers::ADMISSION_TEST_PRESSURE
                    .with(|flag| flag.set(pressure));
                let completed = prepared.execute_owned().unwrap();
                assert!(
                    !super::super::aggregate_count_workers::ADMISSION_TEST_PRESSURE
                        .with(std::cell::Cell::get)
                );
                assert!(completed.execution.native_io_certificate.is_certified());
                assert_eq!(prepared.snapshot().completed_executions, index as u64 + 1);
                assert_eq!(prepared.snapshot().prepared_source_opens, 1);
                let expected = expected()
                    .as_array()
                    .unwrap()
                    .iter()
                    .skip(offset)
                    .take(limit)
                    .cloned()
                    .collect::<Vec<_>>();
                assert_eq!(rendered(&completed.result), serde_json::json!(expected));
                assert!(
                    completed
                        .execution
                        .report
                        .result_summary
                        .as_ref()
                        .unwrap()
                        .contains("no_JSON_or_StatValue_output_rows")
                );
                assert_eq!(
                    completed.result.arrays()[0]
                        .dtype()
                        .as_struct_fields_opt()
                        .unwrap()
                        .field(KEY),
                    Some(DType::Primitive(PType::I64, Nullability::NonNullable))
                );
            }
        }
    }
}

#[test]
fn owned_distinct_preserves_every_original_integer_width_and_extreme_value() {
    macro_rules! check {
        ($t:ty, $ptype:expr) => {{
            let fixture = Fixture::new();
            let keys = vec![<$t>::MAX, <$t>::MIN, <$t>::MAX, <$t>::MIN];
            let path = fixture.source(PrimitiveArray::new(keys, Validity::NonNullable).into_array(), vec![1, 1, 2, 2]);
            let prepared = prepare_aggregate(&request(&path, 0, 5), VortexLocalPrimitiveExecutionPolicy::new(2).unwrap()).unwrap();
            let completed = prepared.execute_owned().unwrap();
            assert_eq!(completed.result.arrays()[0].dtype().as_struct_fields_opt().unwrap().field(KEY), Some(DType::Primitive($ptype, Nullability::NonNullable)));
            assert_eq!(rendered(&completed.result), serde_json::json!([{KEY: <$t>::MIN, COUNT: 2}, {KEY: <$t>::MAX, COUNT: 2}]));
        }};
    }
    check!(i8, PType::I8);
    check!(i16, PType::I16);
    check!(i32, PType::I32);
    check!(i64, PType::I64);
    check!(u8, PType::U8);
    check!(u16, PType::U16);
    check!(u32, PType::U32);
    check!(u64, PType::U64);
}

#[test]
fn owned_distinct_empty_and_fully_pruned_results_keep_typed_empty_schema() {
    use shardloom_core::{ComparisonOp, PredicateExpr, StatValue};
    for empty in [true, false] {
        let fixture = Fixture::new();
        let path = if empty {
            fixture.source(
                PrimitiveArray::new(Vec::<i16>::new(), Validity::NonNullable).into_array(),
                vec![],
            )
        } else {
            fixture.source(
                PrimitiveArray::new(vec![1_i16, 2], Validity::NonNullable).into_array(),
                vec![1, 2],
            )
        };
        let mut request = request(&path, 0, 4);
        if !empty {
            request.predicate = Some(PredicateExpr::Compare {
                column: ColumnRef::new(KEY).unwrap(),
                op: ComparisonOp::Lt,
                value: StatValue::Int64(-10),
            });
        }
        let prepared = prepare_aggregate(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
        )
        .unwrap();
        let completed = prepared.execute_owned().unwrap();
        assert_eq!(completed.result.row_count(), 0);
        assert_eq!(rendered(&completed.result), serde_json::json!([]));
        assert_eq!(
            completed.result.arrays()[0]
                .dtype()
                .as_struct_fields_opt()
                .unwrap()
                .field(KEY),
            Some(DType::Primitive(PType::I16, Nullability::NonNullable))
        );
        let output = fixture.0.join("empty.vortex");
        completed
            .write(
                &output,
                super::super::VortexLocalPrimitiveRowExportFormat::Vortex,
                false,
            )
            .unwrap();
        let reader = ResidentVortexSession::new(16 * 1024 * 1024, 1).unwrap();
        let source = reader.prepare_file(&output).unwrap();
        assert_eq!(source.prepare_count().execute().unwrap(), 0);
        assert_eq!(
            source.dtype().as_struct_fields_opt().unwrap().field(KEY),
            Some(DType::Primitive(PType::I16, Nullability::NonNullable))
        );
    }
}

#[test]
fn owned_distinct_declines_unsupported_shapes_and_memory_before_execution() {
    let fixture = Fixture::new();
    let path = standard(&fixture);
    let mut requests = vec![request(&path, 65_536, 1), request(&path, 0, 65_537)];
    let mut reverse = request(&path, 0, 10);
    reverse.simple_aggregate.as_mut().unwrap().order_by[0].descending = false;
    requests.push(reverse);
    for request in requests {
        let prepared = prepare_aggregate(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
        )
        .unwrap();
        assert!(prepared.execute_owned().is_err());
        assert_eq!(prepared.snapshot().completed_executions, 0);
    }
    let prepared = prepare_aggregate(
        &request(&path, 0, 10),
        VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
    )
    .unwrap();
    let memory = prepared.session.memory();
    let snapshot = memory.snapshot();
    let grant = memory
        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 1024)
        .unwrap();
    assert!(prepared.execute_owned().is_err());
    assert_eq!(prepared.snapshot().completed_executions, 0);
    drop(grant);
    assert_eq!(
        rendered(&prepared.execute_owned().unwrap().result),
        expected()
    );

    let nullable = Fixture::new();
    let path = nullable.source(
        PrimitiveArray::new(vec![1_i64, 2], Validity::from_iter([true, false])).into_array(),
        vec![1, 2],
    );
    let prepared = prepare_aggregate(
        &request(&path, 0, 10),
        VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
    )
    .unwrap();
    assert!(prepared.execute_owned().is_err());
    assert_eq!(prepared.snapshot().completed_executions, 0);
}

#[test]
fn owned_distinct_payload_and_credits_survive_source_and_handle_lifetimes() {
    let fixture = Fixture::new();
    let path = standard(&fixture);
    let prepared = prepare_aggregate(
        &request(&path, 0, 10),
        VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
    )
    .unwrap();
    let memory = prepared.session.memory().clone();
    let completed = prepared.execute_owned().unwrap();
    let array = completed.result.arrays()[0].clone();
    drop(prepared);
    fs::remove_file(path).unwrap();
    assert_eq!(rendered(&completed.result), expected());
    drop(completed);
    assert!(memory.snapshot().reserved_bytes >= array.nbytes());
    drop(array);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn owned_distinct_native_sink_roundtrips_without_reopening_source_or_executing_again() {
    let fixture = Fixture::new();
    let path = standard(&fixture);
    let prepared = prepare_aggregate(
        &request(&path, 0, 10),
        VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
    )
    .unwrap();
    let completed = prepared.execute_owned().unwrap();
    let session = prepared.session.clone();
    drop(prepared);
    fs::remove_file(path).unwrap();
    let target = fixture.0.join("result.vortex");
    let report = completed
        .write(
            &target,
            super::super::VortexLocalPrimitiveRowExportFormat::Vortex,
            false,
        )
        .unwrap();
    assert_eq!(session.snapshot().completed_executions, 1);
    assert_eq!(session.snapshot().prepared_source_opens, 1);
    assert_eq!(report.rows_written, 3);
    let evidence = report.evidence.native_array_sink.unwrap();
    assert_eq!(evidence.scalar_values_materialized, 0);
    assert_eq!(evidence.adapter_payload_bytes_copied, 0);
    let reader = ResidentVortexSession::new(16 * 1024 * 1024, 1).unwrap();
    let source = reader.prepare_file(&target).unwrap();
    let arrays = source
        .prepare_projection(&[KEY, COUNT], 10, 1024)
        .unwrap()
        .execute()
        .unwrap();
    assert_eq!(rendered(&arrays), expected());
}

#[test]
fn owned_distinct_sink_pressure_and_existing_target_preserve_destination_and_release_grants() {
    let fixture = Fixture::new();
    let path = standard(&fixture);
    let prepared = prepare_aggregate(
        &request(&path, 0, 10),
        VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
    )
    .unwrap();
    let memory = prepared.session.memory();
    let completed = prepared.execute_owned().unwrap();
    let snapshot = memory.snapshot();
    let grant = memory
        .reserve(snapshot.limit_bytes - snapshot.reserved_bytes - 1024)
        .unwrap();
    let target = fixture.0.join("pressure.vortex");
    assert!(
        completed
            .write(
                &target,
                super::super::VortexLocalPrimitiveRowExportFormat::Vortex,
                false
            )
            .is_err()
    );
    assert!(!target.exists());
    drop(grant);
    fs::write(&target, b"existing destination").unwrap();
    let completed = prepared.execute_owned().unwrap();
    assert!(
        completed
            .write(
                &target,
                super::super::VortexLocalPrimitiveRowExportFormat::Vortex,
                true
            )
            .is_err()
    );
    assert_eq!(fs::read(&target).unwrap(), b"existing destination");
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 2);
    assert_eq!(prepared.snapshot().completed_executions, 2);
}

#[cfg(feature = "universal-format-io")]
#[test]
fn owned_distinct_public_exports_roundtrip_all_values_through_existing_compatibility_writers() {
    use super::super::{
        VortexLocalPrimitiveRowExportFormat as Format,
        execute_vortex_local_primitive_row_export_with_policy,
    };
    use arrow_array::{Int64Array, UInt64Array};
    for (format, offset, limit) in [
        (Format::ArrowIpc, 0, 10),
        (Format::Parquet, 0, 10),
        (Format::ArrowIpc, 1, 1),
        (Format::Parquet, 1, 1),
        (Format::ArrowIpc, 8, 2),
        (Format::Parquet, 8, 2),
    ] {
        let fixture = Fixture::new();
        let path = standard(&fixture);
        let target = fixture.0.join(format!("result.{}", format.as_str()));
        let report = execute_vortex_local_primitive_row_export_with_policy(
            &request(&path, offset, limit),
            &target,
            format,
            false,
            VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
        )
        .unwrap();
        let expected = expected()
            .as_array()
            .unwrap()
            .iter()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(report.rows_written, expected.len() as u64);
        assert_eq!(
            report
                .evidence
                .native_array_sink
                .as_ref()
                .unwrap()
                .scalar_values_materialized,
            0
        );
        let batches = if format == Format::ArrowIpc {
            arrow_ipc::reader::FileReader::try_new(fs::File::open(&target).unwrap(), None)
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
        } else {
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                fs::File::open(&target).unwrap(),
            )
            .unwrap()
            .build()
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
        };
        let mut rows = Vec::new();
        for batch in batches {
            assert_eq!(batch.schema().field(0).name(), KEY);
            assert_eq!(batch.schema().field(1).name(), COUNT);
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let counts = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push(serde_json::json!({KEY: keys.value(row), COUNT: counts.value(row)}));
            }
        }
        assert_eq!(serde_json::json!(rows), serde_json::json!(expected));
    }
}
