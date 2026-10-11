use crate::{
    VortexAggregateOrderExpr, VortexExpressionProjectionRequest,
    VortexLocalPrimitiveExecutionPolicy, VortexQueryPrimitiveRequest, VortexSimpleAggregateMeasure,
    VortexSimpleAggregateRequest, VortexSortRowsRequest,
};
use shardloom_core::{ColumnRef, DatasetUri, ExecutionResourceOrigin, ExecutionResources};
use shardloom_exec::compute_pool::CancellationToken;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};
use vortex::{
    VortexSessionDefault as _,
    array::{
        ArrayRef, IntoArray as _,
        arrays::{PrimitiveArray, StructArray},
        dtype::{DType, FieldNames, Nullability, PType},
        scalar::{PValue, Scalar, ScalarValue},
        validity::Validity,
    },
    file::WriteOptionsSessionExt as _,
    io::{runtime::BlockingRuntime as _, session::RuntimeSessionExt as _},
    session::VortexSession,
};

const ROWS: usize = 65_537;
const CHUNK_ROWS: usize = 4_096;
const MEMORY_BYTES: u64 = 512 << 20;
const KEY: &str = "shipment_id";
const PAYLOAD: &str = "nullable_payload";
const COUNT: &str = "row_count";

static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);

struct Fixture {
    directory: PathBuf,
    dtype: DType,
}

impl Fixture {
    fn new(rows: usize) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "shardloom-native-results-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();

        let keys = (0..rows)
            .map(|index| i64::try_from(rows - index - 1).unwrap())
            .collect::<Vec<_>>();
        let payloads = keys.iter().map(|key| key * 10).collect::<Vec<_>>();
        let payload_validity = keys.iter().map(|key| key % 11 != 0).collect::<Vec<_>>();
        let array = StructArray::new(
            FieldNames::from([KEY, PAYLOAD]),
            vec![
                PrimitiveArray::new(keys, Validity::NonNullable).into_array(),
                PrimitiveArray::new(payloads, Validity::from_iter(payload_validity)).into_array(),
            ],
            rows,
            Validity::NonNullable,
        )
        .into_array();
        let dtype = array.dtype().clone();
        Self::write(&directory.join("source.vortex"), array, rows);
        Self { directory, dtype }
    }

    fn write(path: &Path, array: ArrayRef, rows: usize) {
        let policy = policy();
        let runtime = crate::local_primitives::local_vortex_runtime(policy);
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut file = fs::File::create(path).unwrap();
        let mut writer = session
            .write_options()
            .with_strategy(
                crate::local_primitives::native_flat_layout::SequentialNativeFlatLayout::strategy(
                    rows.div_ceil(CHUNK_ROWS).max(1),
                ),
            )
            .with_file_statistics(Vec::new())
            .blocking(&runtime)
            .writer(&mut file, array.dtype().clone());
        if rows == 0 {
            writer.push(array).unwrap();
        } else {
            for start in (0..rows).step_by(CHUNK_ROWS) {
                writer
                    .push(array.slice(start..rows.min(start + CHUNK_ROWS)).unwrap())
                    .unwrap();
            }
        }
        assert_eq!(writer.finish().unwrap().row_count(), rows as u64);
    }

    fn path(&self) -> PathBuf {
        self.directory.join("source.vortex")
    }

    fn uri(&self) -> DatasetUri {
        DatasetUri::new(self.path().display().to_string()).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.directory).unwrap();
    }
}

fn policy() -> VortexLocalPrimitiveExecutionPolicy {
    VortexLocalPrimitiveExecutionPolicy::from_resources(
        ExecutionResources::from_bytes(MEMORY_BYTES, 2, ExecutionResourceOrigin::ExecutionCall)
            .unwrap(),
    )
    .unwrap()
}

fn pass_through_request(uri: DatasetUri) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::expression_project_rows(
        uri,
        shardloom_plan::ProjectionRequest::All,
        VortexExpressionProjectionRequest::new(vec![
            crate::VortexExpressionRewrite::NumericScalarArithmetic {
                target_column: ColumnRef::new(KEY).unwrap(),
                operator: "+".into(),
                operand: shardloom_core::ScalarValue::Int64(0),
            },
        ]),
    )
}

fn grouped_count_request(uri: DatasetUri) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::simple_aggregate(
        uri,
        VortexSimpleAggregateRequest::grouped(
            vec![ColumnRef::new(KEY).unwrap()],
            vec![VortexSimpleAggregateMeasure::new(
                "count",
                None,
                COUNT.into(),
            )],
        )
        .with_order_by(vec![VortexAggregateOrderExpr::new(KEY, false)]),
    )
}

fn sort_request(uri: DatasetUri, limit: usize) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::sort_rows(
        uri,
        shardloom_plan::ProjectionRequest::All,
        None,
        VortexSortRowsRequest::new(vec![VortexAggregateOrderExpr::new(KEY, false)]),
        limit,
    )
}

fn assert_native_certificate(
    report: &crate::local_primitives::VortexLocalPrimitiveExecutionReport,
    certificate: &shardloom_core::NativeIoCertificate,
) {
    assert!(!report.fallback_execution_allowed);
    assert!(!certificate.fallback_attempted);
    assert!(certificate.is_certified());
}

fn scalar_json(value: &Scalar) -> serde_json::Value {
    match value.value() {
        None => serde_json::Value::Null,
        Some(ScalarValue::Primitive(PValue::I64(value))) => serde_json::json!(value),
        Some(ScalarValue::Primitive(PValue::U64(value))) => serde_json::json!(value),
        other => panic!("unexpected native result scalar: {other:?}"),
    }
}

fn assert_every_row(
    result: &crate::resident_session::OwnedVortexResultBatch,
    expected_dtype: &DType,
    expected_rows: usize,
    mut expected_row: impl FnMut(usize) -> serde_json::Value,
) {
    assert_eq!(result.dtype(), expected_dtype);
    assert_eq!(result.row_count(), expected_rows as u64);
    let names = result.dtype().as_struct_fields_opt().unwrap().names();
    let mut context = result.create_execution_ctx();
    let mut seen = 0_usize;
    for batch in result.arrays() {
        let columns = names
            .iter()
            .map(|name| {
                crate::local_primitives::logical_field_from_native_array(batch, name.as_ref())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        for row in 0..batch.len() {
            let values = names
                .iter()
                .zip(&columns)
                .map(|(name, column)| {
                    (
                        name.to_string(),
                        scalar_json(&column.execute_scalar(row, &mut context).unwrap()),
                    )
                })
                .collect::<serde_json::Map<_, _>>();
            assert_eq!(
                serde_json::Value::Object(values),
                expected_row(seen),
                "native result row {seen}"
            );
            seen += 1;
        }
    }
    assert_eq!(seen, expected_rows);
}

fn expected_source_row(index: usize) -> serde_json::Value {
    let key = i64::try_from(ROWS - index - 1).unwrap();
    let payload = if key % 11 == 0 {
        serde_json::Value::Null
    } else {
        serde_json::json!(key * 10)
    };
    serde_json::json!({"shipment_id": key, "nullable_payload": payload})
}

fn expected_sorted_row(index: usize) -> serde_json::Value {
    let key = i64::try_from(index).unwrap();
    let payload = if key % 11 == 0 {
        serde_json::Value::Null
    } else {
        serde_json::json!(key * 10)
    };
    serde_json::json!({"shipment_id": key, "nullable_payload": payload})
}

fn expected_count_row(index: usize) -> serde_json::Value {
    serde_json::json!({"shipment_id": i64::try_from(index).unwrap(), "row_count": 1_u64})
}

fn grouped_count_dtype() -> DType {
    DType::struct_(
        [
            (KEY, DType::Primitive(PType::I64, Nullability::NonNullable)),
            (
                COUNT,
                DType::Primitive(PType::U64, Nullability::NonNullable),
            ),
        ],
        Nullability::NonNullable,
    )
}

#[test]
#[allow(clippy::too_many_lines)] // Keep the three >65k complete-result comparisons together.
fn native_results_keep_all_rows_schema_and_owner_past_the_legacy_collection_cap() {
    let fixture = Fixture::new(ROWS);
    assert!(
        fixture
            .dtype
            .as_struct_fields_opt()
            .unwrap()
            .field(PAYLOAD)
            .unwrap()
            .is_nullable()
    );
    let policy = policy();

    let unary_session =
        crate::resident_session::ResidentVortexSession::new(MEMORY_BYTES, 2).unwrap();
    let unary_memory = unary_session.memory().clone();
    let unary_prepared = crate::local_primitives::prepared_unary::prepare_unary_in_session(
        &pass_through_request(fixture.uri()),
        policy,
        &unary_session,
    )
    .unwrap();
    let unary = unary_prepared
        .execute_native_result(&CancellationToken::default())
        .unwrap();
    assert_native_certificate(
        &unary.execution.report,
        &unary.execution.native_io_certificate,
    );
    assert_eq!(unary.execution.runtime.prepared_source_opens, 1);
    assert_eq!(unary.execution.runtime.completed_executions, 1);
    drop(unary_prepared);
    drop(unary_session);
    assert!(unary_memory.snapshot().reserved_bytes > 0);
    assert_every_row(&unary.result, &fixture.dtype, ROWS, expected_source_row);
    drop(unary);
    assert_eq!(unary_memory.snapshot().reserved_bytes, 0);

    let aggregate_session =
        crate::resident_session::ResidentVortexSession::new(MEMORY_BYTES, 2).unwrap();
    let aggregate_memory = aggregate_session.memory().clone();
    let aggregate_prepared =
        crate::local_primitives::prepared_aggregate::prepare_aggregate_in_session(
            &grouped_count_request(fixture.uri()),
            policy,
            &aggregate_session,
        )
        .unwrap();
    let aggregate = aggregate_prepared
        .execute_native_result(&CancellationToken::default())
        .unwrap();
    assert_native_certificate(
        &aggregate.execution.report,
        &aggregate.execution.native_io_certificate,
    );
    assert_eq!(aggregate.execution.runtime.prepared_source_opens, 1);
    assert_eq!(aggregate.execution.runtime.completed_executions, 1);
    drop(aggregate_prepared);
    drop(aggregate_session);
    assert!(aggregate_memory.snapshot().reserved_bytes > 0);
    assert_every_row(
        &aggregate.result,
        &grouped_count_dtype(),
        ROWS,
        expected_count_row,
    );
    drop(aggregate);
    assert_eq!(aggregate_memory.snapshot().reserved_bytes, 0);

    let sort_session =
        crate::resident_session::ResidentVortexSession::new(MEMORY_BYTES, 2).unwrap();
    let sort_memory = sort_session.memory().clone();
    let request = sort_request(fixture.uri(), ROWS);
    let source = crate::local_primitives::prepared_dispatch::prepare_source_in_session(
        &request,
        policy,
        &sort_session,
        &CancellationToken::default(),
    )
    .unwrap();
    let sorted = crate::local_primitives::prepared_dispatch::execute_sort_native_result(
        &request,
        policy,
        &source,
        &CancellationToken::default(),
    )
    .unwrap();
    assert_native_certificate(
        &sorted.execution.report,
        &sorted.execution.native_io_certificate,
    );
    assert_eq!(sorted.execution.runtime.prepared_source_opens, 1);
    assert_eq!(sorted.execution.runtime.completed_executions, 1);
    drop(source);
    drop(sort_session);
    assert!(sort_memory.snapshot().reserved_bytes > 0);
    assert_every_row(&sorted.result, &fixture.dtype, ROWS, expected_sorted_row);
    drop(sorted);
    assert_eq!(sort_memory.snapshot().reserved_bytes, 0);
}

#[test]
fn empty_native_results_retain_their_declared_schemas() {
    let fixture = Fixture::new(0);
    let policy = policy();

    let unary_session =
        crate::resident_session::ResidentVortexSession::new(MEMORY_BYTES, 2).unwrap();
    let unary_prepared = crate::local_primitives::prepared_unary::prepare_unary_in_session(
        &pass_through_request(fixture.uri()),
        policy,
        &unary_session,
    )
    .unwrap();
    let unary = unary_prepared
        .execute_native_result(&CancellationToken::default())
        .unwrap();
    assert_native_certificate(
        &unary.execution.report,
        &unary.execution.native_io_certificate,
    );
    drop(unary_prepared);
    assert_every_row(&unary.result, &fixture.dtype, 0, |_| unreachable!());
    drop(unary);
    assert_eq!(unary_session.snapshot().memory.reserved_bytes, 0);

    let aggregate_session =
        crate::resident_session::ResidentVortexSession::new(MEMORY_BYTES, 2).unwrap();
    let aggregate_prepared =
        crate::local_primitives::prepared_aggregate::prepare_aggregate_in_session(
            &grouped_count_request(fixture.uri()),
            policy,
            &aggregate_session,
        )
        .unwrap();
    let aggregate = aggregate_prepared
        .execute_native_result(&CancellationToken::default())
        .unwrap();
    assert_native_certificate(
        &aggregate.execution.report,
        &aggregate.execution.native_io_certificate,
    );
    drop(aggregate_prepared);
    assert_every_row(
        &aggregate.result,
        &grouped_count_dtype(),
        0,
        |_| unreachable!(),
    );
    drop(aggregate);
    assert_eq!(aggregate_session.snapshot().memory.reserved_bytes, 0);

    let sort_session =
        crate::resident_session::ResidentVortexSession::new(MEMORY_BYTES, 2).unwrap();
    let request = sort_request(fixture.uri(), 1);
    let source = crate::local_primitives::prepared_dispatch::prepare_source_in_session(
        &request,
        policy,
        &sort_session,
        &CancellationToken::default(),
    )
    .unwrap();
    let sorted = crate::local_primitives::prepared_dispatch::execute_sort_native_result(
        &request,
        policy,
        &source,
        &CancellationToken::default(),
    )
    .unwrap();
    assert_native_certificate(
        &sorted.execution.report,
        &sorted.execution.native_io_certificate,
    );
    drop(source);
    assert_every_row(&sorted.result, &fixture.dtype, 0, |_| unreachable!());
    assert!(!sorted.execution.report.data_read);
    assert!(!sorted.execution.report.full_stream_collected);
    for invalid in ["nonempty_footer", "absent_scan", "payload_read"] {
        let mut report = sorted.execution.report.clone();
        match invalid {
            "nonempty_footer" => report.embedded_layout.footer_row_count = 1,
            "absent_scan" => report.upstream_scan_called = false,
            "payload_read" => report.arrays_read_count = 1,
            _ => unreachable!(),
        }
        let certificate = crate::local_primitive_native_io_certificate(&request, &report).unwrap();
        assert!(!certificate.is_certified(), "{invalid}");
    }
    drop(sorted);
    assert_eq!(sort_session.snapshot().memory.reserved_bytes, 0);
}

fn assert_cancel_and_pressure_then_retry<T>(
    session: &crate::resident_session::ResidentVortexSession,
    execute: impl Fn(&CancellationToken) -> shardloom_core::Result<T>,
) -> T {
    let before = session.snapshot();
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    assert!(execute(&cancelled).is_err());
    let after_cancel = session.snapshot();
    assert_eq!(
        after_cancel.prepared_source_opens,
        before.prepared_source_opens
    );
    assert_eq!(
        after_cancel.completed_executions,
        before.completed_executions
    );
    assert_eq!(
        after_cancel.memory.reserved_bytes,
        before.memory.reserved_bytes
    );

    let available = before.memory.limit_bytes - before.memory.reserved_bytes;
    assert!(
        available > 1,
        "fixture preparation exhausted the owner budget"
    );
    let lease = session.reserve_input_scratch(available - 1).unwrap();
    let denied = execute(&CancellationToken::default());
    let Err(error) = denied else {
        panic!("one free byte must reject result ownership");
    };
    assert!(
        error.to_string().contains("memory reservation denied"),
        "{error}"
    );
    let after_denial = session.snapshot();
    assert_eq!(
        after_denial.memory.reserved_bytes,
        before.memory.limit_bytes - 1
    );
    assert_eq!(
        after_denial.prepared_source_opens,
        before.prepared_source_opens
    );
    assert_eq!(
        after_denial.completed_executions,
        before.completed_executions
    );
    drop(lease);
    assert_eq!(
        session.snapshot().memory.reserved_bytes,
        before.memory.reserved_bytes
    );

    let result = execute(&CancellationToken::default()).unwrap();
    assert!(session.snapshot().completed_executions > before.completed_executions);
    result
}

#[test]
#[allow(clippy::too_many_lines)] // Keep cancellation, denial, and retry assertions aligned across providers.
fn native_result_operators_release_failed_attempts_and_retry_in_the_same_owner() {
    let fixture = Fixture::new(5);
    let policy = policy();

    let unary_session =
        crate::resident_session::ResidentVortexSession::new(MEMORY_BYTES, 2).unwrap();
    let unary_prepared = crate::local_primitives::prepared_unary::prepare_unary_in_session(
        &pass_through_request(fixture.uri()),
        policy,
        &unary_session,
    )
    .unwrap();
    let unary = assert_cancel_and_pressure_then_retry(&unary_session, |token| {
        unary_prepared.execute_native_result(token)
    });
    assert_native_certificate(
        &unary.execution.report,
        &unary.execution.native_io_certificate,
    );
    assert_eq!(unary.result.row_count(), 5);
    drop(unary_prepared);
    drop(unary);
    assert_eq!(unary_session.snapshot().memory.reserved_bytes, 0);

    let aggregate_session =
        crate::resident_session::ResidentVortexSession::new(MEMORY_BYTES, 2).unwrap();
    let aggregate_prepared =
        crate::local_primitives::prepared_aggregate::prepare_aggregate_in_session(
            &grouped_count_request(fixture.uri()),
            policy,
            &aggregate_session,
        )
        .unwrap();
    let aggregate = assert_cancel_and_pressure_then_retry(&aggregate_session, |token| {
        aggregate_prepared.execute_native_result(token)
    });
    assert_native_certificate(
        &aggregate.execution.report,
        &aggregate.execution.native_io_certificate,
    );
    assert_eq!(aggregate.result.row_count(), 5);
    drop(aggregate_prepared);
    drop(aggregate);
    assert_eq!(aggregate_session.snapshot().memory.reserved_bytes, 0);

    let sort_session =
        crate::resident_session::ResidentVortexSession::new(MEMORY_BYTES, 2).unwrap();
    let request = sort_request(fixture.uri(), 5);
    let source = crate::local_primitives::prepared_dispatch::prepare_source_in_session(
        &request,
        policy,
        &sort_session,
        &CancellationToken::default(),
    )
    .unwrap();
    let sorted = assert_cancel_and_pressure_then_retry(&sort_session, |token| {
        crate::local_primitives::prepared_dispatch::execute_sort_native_result(
            &request, policy, &source, token,
        )
    });
    assert_native_certificate(
        &sorted.execution.report,
        &sorted.execution.native_io_certificate,
    );
    assert_eq!(sorted.result.row_count(), 5);
    drop(source);
    drop(sorted);
    assert_eq!(sort_session.snapshot().memory.reserved_bytes, 0);
}
