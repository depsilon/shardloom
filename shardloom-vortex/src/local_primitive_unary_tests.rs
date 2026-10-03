use super::super as runtime;
use super::*;
use runtime::ProjectionRequest;
use shardloom_core::{ColumnRef, DatasetUri};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        IntoArray as _,
        arrays::{PrimitiveArray, StructArray},
        dtype::FieldNames,
        scalar::ScalarValue,
        validity::Validity,
    },
    file::WriteOptionsSessionExt as _,
    io::{runtime::BlockingRuntime as _, session::RuntimeSessionExt as _},
    session::VortexSession,
};

static NEXT: AtomicUsize = AtomicUsize::new(0);
#[path = "local_primitive_unary_bound_tests.rs"]
mod bound_tests;
#[path = "local_primitive_unary_explode_tests.rs"]
mod explode_tests;
#[path = "local_primitive_unary_pivot_tests.rs"]
mod pivot_tests;
#[cfg(feature = "universal-format-io")]
#[path = "local_primitive_unary_writer_tests.rs"]
mod writer_tests;
const KEY: &str = "account_key";
const VALUE: &str = "amount";
struct Fixture(PathBuf);
impl Fixture {
    fn new(keys: &[Option<u64>], values: &[u64], chunk_rows: usize) -> Self {
        let array = StructArray::new(
            FieldNames::from([KEY, VALUE]),
            vec![
                PrimitiveArray::new(
                    keys.iter().map(|v| v.unwrap_or(0)).collect::<Vec<_>>(),
                    Validity::from_iter(keys.iter().map(Option::is_some)),
                )
                .into_array(),
                PrimitiveArray::new(values.to_vec(), Validity::NonNullable).into_array(),
            ],
            keys.len(),
            Validity::NonNullable,
        )
        .into_array();
        Self::from_array(array, chunk_rows)
    }
    fn from_array(array: ArrayRef, chunk_rows: usize) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "shardloom-unary-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let fixture = Self(directory);
        let rows = array.len();
        let runtime =
            runtime::local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut file = fs::File::create(fixture.path()).unwrap();
        let mut writer = session
            .write_options()
            .with_strategy(
                runtime::native_flat_layout::SequentialNativeFlatLayout::strategy(
                    rows.div_ceil(chunk_rows).max(1),
                ),
            )
            .with_file_statistics(Vec::new())
            .blocking(&runtime)
            .writer(&mut file, array.dtype().clone());
        if rows == 0 {
            writer.push(array).unwrap();
        } else {
            for start in (0..rows).step_by(chunk_rows) {
                writer
                    .push(array.slice(start..rows.min(start + chunk_rows)).unwrap())
                    .unwrap();
            }
        }
        assert_eq!(writer.finish().unwrap().row_count(), rows as u64);
        fixture
    }
    fn path(&self) -> PathBuf {
        self.0.join("input.vortex")
    }
    fn uri(&self) -> DatasetUri {
        DatasetUri::new(self.path().display().to_string()).unwrap()
    }
    fn replace(&self) {
        let alternate = self.0.join("replacement.vortex");
        fs::copy(self.path(), &alternate).unwrap();
        fs::rename(alternate, self.path()).unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn projection(names: &[&str]) -> ProjectionRequest {
    ProjectionRequest::columns(
        names
            .iter()
            .map(|name| ColumnRef::new(*name).unwrap())
            .collect(),
    )
}
fn prepare(request: &VortexQueryPrimitiveRequest) -> PreparedVortexUnary {
    prepare_unary(
        request,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
    .unwrap()
}
fn json_rows(result: &OwnedVortexResultBatch) -> Vec<serde_json::Value> {
    let mut context = result.create_execution_ctx();
    let names = result.dtype().as_struct_fields_opt().unwrap().names();
    let mut rows = Vec::new();
    for array in result.arrays() {
        let columns = names
            .iter()
            .map(|name| runtime::logical_field_from_native_array(array, name.as_ref()).unwrap())
            .collect::<Vec<_>>();
        for row in 0..array.len() {
            let mut value = serde_json::Map::new();
            for (name, column) in names.iter().zip(&columns) {
                let scalar = column.execute_scalar(row, &mut context).unwrap();
                let current = scalar_json(&scalar);
                value.insert(name.to_string(), current);
            }
            rows.push(value.into());
        }
    }
    rows
}
fn scalar_json(scalar: &vortex::array::scalar::Scalar) -> serde_json::Value {
    match scalar.value() {
        None => serde_json::Value::Null,
        Some(ScalarValue::Bool(value)) => (*value).into(),
        Some(ScalarValue::Primitive(value)) => {
            Value::from(runtime::vortex_pvalue_to_stat_value(*value).unwrap())
                .into_json()
                .unwrap()
        }
        Some(ScalarValue::Utf8(value)) => value.as_str().into(),
        Some(ScalarValue::Variant(value)) => scalar_json(value),
        other => panic!("unexpected result scalar: {other:?}"),
    }
}

fn native_rows(path: &std::path::Path) -> (DType, Vec<Vec<vortex::array::scalar::Scalar>>) {
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let source = session.prepare_file(path).unwrap();
    let dtype = source.dtype().clone();
    let rows = source
        .with_native_execution_controlled(&CancellationToken::default(), |file, context| {
            let mut rows = Vec::new();
            let mut execution = context.native_session().create_execution_ctx();
            let names = file.dtype().as_struct_fields_opt().unwrap().names();
            for array in file
                .scan()
                .map_err(vortex_error)?
                .with_ordered(true)
                .into_array_iter(context.runtime())
                .map_err(vortex_error)?
            {
                let array = array.map_err(vortex_error)?;
                let columns = names
                    .iter()
                    .map(|name| runtime::logical_field_from_native_array(&array, name.as_ref()))
                    .collect::<Result<Vec<_>>>()?;
                for row in 0..array.len() {
                    rows.push(
                        columns
                            .iter()
                            .map(|column| {
                                column
                                    .execute_scalar(row, &mut execution)
                                    .map_err(vortex_error)
                            })
                            .collect::<Result<Vec<_>>>()?,
                    );
                }
            }
            Ok(rows)
        })
        .unwrap();
    (dtype, rows)
}
fn values(prepared: &PreparedVortexUnary, column: &str) -> Vec<serde_json::Value> {
    json_rows(&prepared.execute_owned().unwrap().result)
        .into_iter()
        .map(|mut row| row[column].take())
        .collect()
}

#[path = "local_primitive_unary_expression_tests.rs"]
mod expression_tests;
#[path = "local_primitive_unary_melt_tests.rs"]
mod melt_tests;

#[test]
fn unary_first_deduplicate_stops_once_limit_is_complete() {
    let fixture = Fixture::new(
        &(0..2_000).map(Some).collect::<Vec<_>>(),
        &(0..2_000).collect::<Vec<_>>(),
        128,
    );
    let request = VortexQueryPrimitiveRequest::drop_duplicate_rows(
        fixture.uri(),
        projection(&[KEY, VALUE]),
        projection(&[KEY]),
    )
    .with_duplicate_keep(super::super::VortexDuplicateKeepPolicy::First)
    .with_source_order_limit(2);
    let prepared = prepare(&request);
    let result = prepared.execute_owned().unwrap();
    assert_eq!(
        json_rows(&result.result),
        vec![
            serde_json::json!({"account_key": 0, "amount": 0}),
            serde_json::json!({"account_key": 1, "amount": 1}),
        ]
    );
    assert_eq!(result.execution.report.state_budget.observed_state_items, 2);
    assert_eq!(result.execution.report.arrays_read_count, 1);
    assert_eq!(
        result.execution.report.source_order_limit_input_rows,
        Some(2)
    );
    let report = prepared
        .write(
            &fixture.0.join("first-two.vortex"),
            runtime::VortexLocalPrimitiveRowExportFormat::Vortex,
            false,
        )
        .unwrap();
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 2);
    assert!(
        !report
            .evidence
            .native_array_sink
            .unwrap()
            .pre_limit_result_row_count_exact
    );
}

#[test]
fn unary_rolling_min_max_do_not_overflow_an_unused_sum() {
    for value in [f64::MAX, -f64::MAX] {
        let array = StructArray::new(
            FieldNames::from([VALUE]),
            vec![PrimitiveArray::new(vec![value; 2], Validity::NonNullable).into_array()],
            2,
            Validity::NonNullable,
        )
        .into_array();
        let fixture = Fixture::from_array(array, 1);
        for center in [false, true] {
            for aggregate in ["min", "max", "count", "sum", "mean"] {
                let request = VortexQueryPrimitiveRequest::rolling_window_rows(
                    fixture.uri(),
                    crate::VortexRollingWindowRequest::new(
                        ColumnRef::new(VALUE).unwrap(),
                        "result".into(),
                        2,
                        2,
                        aggregate.into(),
                    )
                    .with_center(center),
                );
                let prepared = prepare(&request);
                if matches!(aggregate, "sum" | "mean") {
                    assert!(prepared.execute_owned().is_err());
                } else {
                    let expected = if aggregate == "count" {
                        serde_json::json!(2)
                    } else {
                        serde_json::json!(value)
                    };
                    assert_eq!(values(&prepared, "result"), vec![expected]);
                }
            }
        }
    }
}

#[test]
fn unary_rolling_preserves_nullable_windows_centering_types_and_repeated_state() {
    use crate::VortexRollingWindowRequest;
    let fixture = Fixture::new(
        &[Some(10), None, Some(30), Some(40), None, Some(60)],
        &[1, 2, 3, 4, 5, 6],
        2,
    );
    for (aggregate, expected) in [
        ("sum", vec![40.0, 70.0, 70.0, 100.0]),
        ("mean", vec![20.0, 35.0, 35.0, 50.0]),
        ("min", vec![10.0, 30.0, 30.0, 40.0]),
        ("max", vec![30.0, 40.0, 40.0, 60.0]),
    ] {
        for center in [false, true] {
            let request = VortexQueryPrimitiveRequest::rolling_window_rows(
                fixture.uri(),
                VortexRollingWindowRequest::new(
                    ColumnRef::new(KEY).unwrap(),
                    "window_value".into(),
                    3,
                    2,
                    aggregate.into(),
                )
                .with_center(center),
            );
            let prepared = prepare(&request);
            let expected = expected
                .iter()
                .copied()
                .map(serde_json::Value::from)
                .collect::<Vec<_>>();
            assert_eq!(values(&prepared, "window_value"), expected);
            let second = prepared.execute_owned().unwrap();
            assert!(second.execution.native_io_certificate.is_certified());
            assert_eq!(second.execution.runtime.prepared_source_opens, 1);
            assert_eq!(second.execution.runtime.completed_executions, 2);
            assert_eq!(
                second
                    .result
                    .dtype()
                    .as_struct_fields_opt()
                    .unwrap()
                    .field("window_value"),
                Some(DType::Primitive(
                    vortex::array::dtype::PType::F64,
                    Nullability::NonNullable
                ))
            );
        }
    }
    let request = VortexQueryPrimitiveRequest::rolling_window_rows(
        fixture.uri(),
        VortexRollingWindowRequest::new(
            ColumnRef::new(KEY).unwrap(),
            "valid_count".into(),
            3,
            1,
            "count".into(),
        )
        .with_center(true),
    );
    assert_eq!(
        values(&prepare(&request), "valid_count"),
        vec![1, 2, 2, 2, 2, 1]
    );
    let empty = Fixture::new(&[], &[], 1);
    let mut empty_request = request;
    empty_request.source_uri = Some(empty.uri());
    let empty_result = prepare(&empty_request).execute_owned().unwrap();
    assert_eq!(empty_result.result.arrays().len(), 1);
    assert_eq!(empty_result.result.row_count(), 0);
    assert_eq!(
        empty_result
            .result
            .dtype()
            .as_struct_fields_opt()
            .unwrap()
            .field("valid_count"),
        Some(DType::Primitive(
            vortex::array::dtype::PType::U64,
            Nullability::NonNullable
        ))
    );
}

#[test]
fn unary_rolling_reserves_state_before_allocating_and_limits_centered_output() {
    use crate::VortexRollingWindowRequest;
    let fixture = Fixture::new(
        &[Some(10), Some(20), Some(30), Some(40), Some(50)],
        &[1, 2, 3, 4, 5],
        2,
    );
    let mut request = VortexQueryPrimitiveRequest::rolling_window_rows(
        fixture.uri(),
        VortexRollingWindowRequest::new(
            ColumnRef::new(KEY).unwrap(),
            "total".into(),
            4,
            4,
            "sum".into(),
        )
        .with_center(true),
    );
    assert_eq!(values(&prepare(&request), "total"), vec![100.0, 140.0]);
    request.source_order_limit = Some(1);
    assert_eq!(values(&prepare(&request), "total"), vec![100.0]);
    request.rolling_window.as_mut().unwrap().window_size = 100_000;
    // Source cardinality bounds the window even when its configured size is larger.
    assert_eq!(values(&prepare(&request), "total"), vec![150.0]);
    let large = Fixture::new(
        &(0..20_007).map(Some).collect::<Vec<_>>(),
        &(0..20_007).collect::<Vec<_>>(),
        4096,
    );
    request.source_uri = Some(large.uri());
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let prepared = prepare_unary_in_session(
        &request,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        &session,
    )
    .unwrap();
    let before = session.snapshot().memory.reserved_bytes;
    let error = prepared
        .execute_owned()
        .err()
        .expect("window state must reserve");
    assert!(error.to_string().contains("memory reservation denied"));
    assert_eq!(session.snapshot().memory.reserved_bytes, before);
}

#[test]
fn unary_exact_duplicate_policies_keep_null_identity_source_order_and_uint64() {
    let fixture = Fixture::new(
        &[Some(1), None, None, Some(2), None],
        &[10, 20, 99, 30, u64::MAX],
        2,
    );
    let policies = [
        VortexDuplicateKeepPolicy::First,
        VortexDuplicateKeepPolicy::Last,
        VortexDuplicateKeepPolicy::AllDuplicates,
    ];
    let expected = [vec![10, 20, 30], vec![10, 30, u64::MAX], vec![10, 30]];
    let masks = [
        vec![false, false, true, false, true],
        vec![false, true, true, false, false],
        vec![false, true, true, false, true],
    ];
    for ((keep, expected), masks) in policies.into_iter().zip(expected).zip(masks) {
        let mut request = VortexQueryPrimitiveRequest::drop_duplicate_rows(
            fixture.uri(),
            projection(&[KEY, VALUE]),
            projection(&[KEY]),
        );
        request.duplicate_keep = keep;
        let prepared = prepare(&request);
        for execution in 1..=2 {
            let result = prepared.execute_owned().unwrap();
            assert_eq!(
                json_rows(&result.result)
                    .iter()
                    .map(|row| row[VALUE].as_u64().unwrap())
                    .collect::<Vec<_>>(),
                expected
            );
            assert_eq!(result.execution.runtime.prepared_source_opens, 1);
            assert_eq!(result.execution.runtime.completed_executions, execution);
            assert!(result.execution.native_io_certificate.is_certified());
        }
        request =
            VortexQueryPrimitiveRequest::duplicate_mask_rows(fixture.uri(), projection(&[KEY]));
        request.duplicate_keep = keep;
        assert_eq!(
            values(&prepare(&request), "duplicated"),
            masks
                .into_iter()
                .map(serde_json::Value::Bool)
                .collect::<Vec<_>>()
        );
    }
    let distinct =
        VortexQueryPrimitiveRequest::distinct_rows(fixture.uri(), projection(&[KEY]), None);
    assert_eq!(
        values(&prepare(&distinct), KEY),
        vec![1.into(), serde_json::Value::Null, 2.into()]
    );
}

#[test]
fn unary_sample_matches_pinned_seed_outputs_and_keeps_weight_column_hidden() {
    let fixture = Fixture::new(
        &[Some(1), Some(2), Some(3), Some(4), Some(5)],
        &[10, 20, 30, 40, 50],
        2,
    );
    let mut request =
        VortexQueryPrimitiveRequest::sample_rows(fixture.uri(), projection(&[KEY]), None, 2, 7);
    let plain = values(&prepare(&request), KEY);
    // Literal expectations are independently calculated from the documented seeded score.
    assert_eq!(plain, serde_json::json!([2, 5]).as_array().unwrap().clone());
    request.sample_weight_column = Some(ColumnRef::new(VALUE).unwrap());
    assert_eq!(
        values(&prepare(&request), KEY),
        serde_json::json!([2, 5]).as_array().unwrap().clone()
    );
    request.sample_weight_column = None;
    request.source_order_limit = None;
    request.sample_fraction = Some(0.4);
    assert_eq!(values(&prepare(&request), KEY), plain);
    request.sample_fraction = None;
    request.source_order_limit = Some(7);
    request.sample_seed = Some(11);
    request.sample_with_replacement = true;
    assert_eq!(
        values(&prepare(&request), KEY),
        serde_json::json!([4, 2, 5, 4, 4, 2, 5])
            .as_array()
            .unwrap()
            .clone()
    );
    request.sample_weight_column = Some(ColumnRef::new(VALUE).unwrap());
    assert_eq!(
        values(&prepare(&request), KEY),
        serde_json::json!([5, 2, 2, 5, 4, 4, 3])
            .as_array()
            .unwrap()
            .clone()
    );
}

#[test]
fn unary_tail_uses_range_and_collects_only_the_suffix() {
    let fixture = Fixture::new(
        &(0..20_007).map(Some).collect::<Vec<_>>(),
        &(0..20_007).collect::<Vec<_>>(),
        4096,
    );
    let prepared = prepare(&VortexQueryPrimitiveRequest::tail_rows(
        fixture.uri(),
        projection(&[VALUE]),
        3,
    ));
    let result = prepared.execute_owned().unwrap();
    assert_eq!(
        json_rows(&result.result),
        serde_json::json!([{"amount":20004},{"amount":20005},{"amount":20006}])
            .as_array()
            .unwrap()
            .clone()
    );
    assert_eq!(result.execution.report.rows_scanned, 20_007);
    assert!(result.execution.report.max_chunk_rows <= 3);
}

#[test]
fn unary_empty_retains_schema_and_generation_failure_never_reopens() {
    let fixture = Fixture::new(&[], &[], 2);
    let prepared = prepare(&VortexQueryPrimitiveRequest::distinct_rows(
        fixture.uri(),
        projection(&[KEY, VALUE]),
        None,
    ));
    let result = prepared.execute_owned().unwrap();
    assert_eq!(result.result.row_count(), 0);
    assert_eq!(result.result.arrays().len(), 1);
    assert_eq!(
        result
            .result
            .dtype()
            .as_struct_fields_opt()
            .unwrap()
            .names()
            .len(),
        2
    );
    for mutate in [0, 1, 2] {
        let mut invalid = result.execution.report.clone();
        match mutate {
            0 => invalid.rows_projected = Some(1),
            1 => invalid.embedded_layout.footer_row_count = 1,
            _ => invalid.upstream_scan_called = false,
        }
        assert!(
            !runtime::local_primitive_native_io_certificate(&prepared.bound.request, &invalid)
                .unwrap()
                .is_certified()
        );
    }
    drop(result);
    fixture.replace();
    assert!(prepared.execute_owned().is_err());
    assert_eq!(prepared.snapshot().prepared_source_opens, 1);
    assert_eq!(prepared.snapshot().completed_executions, 1);
}

#[test]
fn unary_request_memory_and_predicate_depth_are_denied_before_opening() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let mut request = VortexQueryPrimitiveRequest::distinct_rows(
        DatasetUri::new("/absent-unary-source.vortex").unwrap(),
        projection(&[KEY]),
        Some(shardloom_core::PredicateExpr::Compare {
            column: ColumnRef::new(KEY).unwrap(),
            op: shardloom_core::ComparisonOp::Eq,
            value: StatValue::Utf8("wide".repeat(32_768)),
        }),
    );
    let error = prepare_unary_in_session(
        &request,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        &session,
    )
    .err()
    .expect("metadata must reserve before opening");
    assert!(error.to_string().contains("memory reservation denied"));
    assert_eq!(session.snapshot().prepared_source_opens, 0);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    let mut predicate = shardloom_core::PredicateExpr::AlwaysTrue;
    for _ in 0..66 {
        predicate = shardloom_core::PredicateExpr::And(vec![predicate]);
    }
    request.predicate = Some(predicate);
    let error = prepare_unary_in_session(
        &request,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        &session,
    )
    .err()
    .expect("nested request must fail before opening");
    assert!(error.to_string().contains("predicate nesting"));
    assert_eq!(session.snapshot().prepared_source_opens, 0);
}

#[test]
fn unary_variant_provider_preserves_mixed_scalars_through_native_file() {
    use vortex::array::{
        arrays::{ChunkedArray, ConstantArray},
        scalar::Scalar,
    };
    let fixture = Fixture::new(&[], &[], 1);
    let dtype = DType::Variant(Nullability::Nullable);
    let mut expected = [
        Scalar::from(i64::MIN),
        Scalar::from(u64::MAX),
        Scalar::from(0.125_f64),
        Scalar::from(true),
        Scalar::from("π—native"),
    ]
    .into_iter()
    .map(|value| Scalar::variant(value).cast(&dtype).unwrap())
    .collect::<Vec<_>>();
    expected.push(Scalar::null(dtype.clone()));
    let chunks = expected
        .iter()
        .map(|value| ConstantArray::new(value.clone(), 1).into_array())
        .collect::<Vec<_>>();
    let column = ChunkedArray::try_new(chunks, dtype).unwrap().into_array();
    let array = StructArray::new(
        ["mixed"].into(),
        vec![column],
        expected.len(),
        Validity::NonNullable,
    )
    .into_array();
    let path = fixture.0.join("mixed.vortex");
    let provider_runtime =
        runtime::local_vortex_runtime(VortexLocalPrimitiveExecutionPolicy::single_threaded());
    let provider = VortexSession::default().with_handle(provider_runtime.handle());
    let mut file = fs::File::create(&path).unwrap();
    let mut writer = provider
        .write_options()
        .with_strategy(runtime::native_flat_layout::SequentialNativeFlatLayout::strategy(1))
        .with_file_statistics(Vec::new())
        .blocking(&provider_runtime)
        .writer(&mut file, array.dtype().clone());
    writer.push(array).unwrap();
    assert_eq!(writer.finish().unwrap().row_count(), expected.len() as u64);
    drop(file);
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let source = session.prepare_file(&path).unwrap();
    let actual = source
        .with_native_execution_controlled(&CancellationToken::default(), |file, context| {
            let mut values = Vec::new();
            let mut execution = context.native_session().create_execution_ctx();
            for chunk in file
                .scan()
                .map_err(vortex_error)?
                .with_ordered(true)
                .into_array_iter(context.runtime())
                .map_err(vortex_error)?
            {
                let chunk = chunk.map_err(vortex_error)?;
                let column = runtime::logical_field_from_native_array(&chunk, "mixed")?;
                for row in 0..column.len() {
                    values.push(
                        column
                            .execute_scalar(row, &mut execution)
                            .map_err(vortex_error)?,
                    );
                }
            }
            Ok(values)
        })
        .unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn unary_result_stream_crosses_collect_bound_and_cleans_up_denied_consumers() {
    const ROWS: usize = 65_541;
    let fixture = Fixture::new(
        &(0..ROWS as u64).map(Some).collect::<Vec<_>>(),
        &(0..ROWS as u64).collect::<Vec<_>>(),
        4096,
    );
    let session = ResidentVortexSession::new(32 << 20, 1).unwrap();
    let request =
        VortexQueryPrimitiveRequest::distinct_rows(fixture.uri(), projection(&[KEY]), None);
    let prepared = prepare_unary_in_session(
        &request,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        &session,
    )
    .unwrap();
    assert!(
        prepared
            .execute_owned()
            .err()
            .expect("collection limit")
            .to_string()
            .contains("small collection")
    );
    let baseline = session.snapshot().memory.reserved_bytes;
    let error = prepared
        .collect_jsonl(&CancellationToken::default())
        .err()
        .expect("JSON collection limit");
    assert!(error.to_string().contains("collect exceeds 65,536 rows"));
    assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
    let stop = CancellationToken::default();
    let failed = prepared.for_each_batch(&stop, |_, _| {
        stop.cancel();
        Ok(())
    });
    assert!(failed.is_err());
    assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
    let mut next = 0u64;
    let execution = prepared
        .for_each_batch(&CancellationToken::default(), |array, context| {
            assert!(array.len() <= BATCH_ROWS);
            let column = runtime::logical_field_from_native_array(&array, KEY).unwrap();
            let mut execution = context.native_session().create_execution_ctx();
            for row in 0..array.len() {
                assert_eq!(
                    column
                        .execute_scalar(row, &mut execution)
                        .unwrap()
                        .as_primitive()
                        .typed_value::<u64>(),
                    Some(next)
                );
                next += 1;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(next, ROWS as u64);
    drop(execution);
    assert_eq!(session.snapshot().memory.reserved_bytes, baseline);
    let path = fixture.0.join("output.vortex");
    let report = prepared
        .write(
            &path,
            runtime::VortexLocalPrimitiveRowExportFormat::Vortex,
            false,
        )
        .unwrap();
    assert_eq!(report.rows_written, ROWS as u64);
    let (dtype, reopened) = native_rows(&path);
    assert_eq!(
        dtype.as_struct_fields_opt().unwrap().field(KEY),
        Some(prepared.bound.fields[0].1.clone())
    );
    assert_eq!(reopened.len(), ROWS);
    for (ordinal, row) in reopened.iter().enumerate() {
        assert_eq!(
            row[0].as_primitive().typed_value::<u64>(),
            Some(ordinal as u64)
        );
    }
    drop(reopened);
    drop(prepared);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn unary_reports_distinguish_retained_state_suffix_streaming_and_empty_provider_work() {
    let fixture = Fixture::new(
        &[Some(1), Some(2), Some(3), Some(4), Some(5)],
        &[10, 20, 30, 40, 50],
        2,
    );
    let sample =
        VortexQueryPrimitiveRequest::sample_rows(fixture.uri(), projection(&[KEY]), None, 2, 7);
    let result = prepare(&sample).execute_owned().unwrap();
    assert!(!result.execution.report.full_stream_collected);
    assert!(result.execution.report.state_budget.state_budget_required);
    assert_eq!(result.execution.report.state_budget.observed_state_items, 2);
    let mut replacement = sample;
    replacement.sample_with_replacement = true;
    let result = prepare(&replacement).execute_owned().unwrap();
    assert!(result.execution.report.full_stream_collected);
    assert_eq!(result.execution.report.state_budget.observed_state_items, 5);
    let tail = VortexQueryPrimitiveRequest::tail_rows(fixture.uri(), projection(&[KEY]), 2);
    assert!(
        !prepare(&tail)
            .execute_owned()
            .unwrap()
            .execution
            .report
            .full_stream_collected
    );
    let empty = Fixture::new(&[], &[], 1);
    let mut tail = tail;
    tail.source_uri = Some(empty.uri());
    let result = prepare(&tail).execute_owned().unwrap();
    assert!(result.execution.native_io_certificate.is_certified());
    assert!(!result.execution.report.data_decoded);
    assert!(!result.execution.report.data_materialized);
    assert!(!result.execution.report.full_stream_collected);
    for predicate in [
        shardloom_core::PredicateExpr::AlwaysFalse,
        shardloom_core::PredicateExpr::Compare {
            column: ColumnRef::new(KEY).unwrap(),
            op: shardloom_core::ComparisonOp::Gt,
            value: StatValue::UInt64(999),
        },
    ] {
        let constant_false = matches!(predicate, shardloom_core::PredicateExpr::AlwaysFalse);
        let request = VortexQueryPrimitiveRequest::distinct_rows(
            fixture.uri(),
            projection(&[KEY]),
            Some(predicate),
        );
        let result = prepare(&request).execute_owned().unwrap();
        assert!(result.execution.native_io_certificate.is_certified());
        assert_eq!(result.result.row_count(), 0);
        let report = result.execution.report;
        if !report.embedded_layout.metadata_pruned_entire_input && report.arrays_read_count == 0 {
            if constant_false {
                assert!(!report.data_read);
                let mut other_request = request;
                other_request.predicate = Some(shardloom_core::PredicateExpr::AlwaysTrue);
                assert!(
                    !runtime::local_primitive_native_io_certificate(&other_request, &report)
                        .unwrap()
                        .is_certified()
                );
            } else {
                assert!(report.data_read);
                assert!(
                    report
                        .result_summary
                        .unwrap()
                        .contains("provider_filter_may_read")
                );
            }
        }
    }
}
