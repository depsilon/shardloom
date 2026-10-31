use super::*;
use arrow_array::{Array as _, cast::AsArray as _, types::Int64Type};
use shardloom_core::{ExecutionResourceLimits, ExecutionResourceOrigin};
use shardloom_vortex::resident_memory_source::{
    MemoryColumn, MemoryColumnValues, MemorySourceBounds,
};
use std::sync::Arc;

#[path = "native_runtime_application_tests.rs"]
mod application_tests;

fn allocation() -> ExecutionResources {
    ExecutionResources::from_declaration(
        64 << 20,
        1,
        ExecutionResourceOrigin::Context,
        ExecutionResourceOrigin::Platform,
        Some(ExecutionResourceLimits::new(Some(64 << 20), Some(1)).unwrap()),
    )
    .unwrap()
}

fn session() -> ResidentVortexSession {
    let allocation = allocation();
    ResidentVortexSession::new(allocation.memory_bytes(), allocation.max_parallelism()).unwrap()
}

#[test]
fn sql_file_binding_format_uses_public_normalization() {
    let mut request = sql_request(
        "SELECT COUNT(*) FROM 'declared.data'",
        allocation(),
        &[SqlSourceBinding {
            reference: "declared.data".into(),
            source: SqlSource::File {
                format: " ARROW_IPC ".into(),
                schema: None,
            },
        }],
        None,
    )
    .unwrap();
    request.infer_defaults();
    assert_eq!(request.input_format.as_deref(), Some("arrow-ipc"));
    assert_eq!(request.input_uri.as_deref(), Some("declared.data"));
}

#[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
#[test]
fn prepared_native_query_timing_excludes_preparation_and_later_row_export() {
    let session = session();
    let source = ResidentMemorySource::from_int64_range(&session, "value", 0, 1, 17).unwrap();
    let operation = prepare_sql_operation(
        "SELECT value FROM 'memory://input' WHERE value >= 15 ORDER BY value",
        allocation(),
        &session,
        &[binding(source)],
        None,
        &CancellationToken::default(),
    )
    .unwrap();
    for reused in [false, true] {
        let started = std::time::Instant::now();
        let completed = operation
            .execute_native(&CancellationToken::default(), None)
            .unwrap();
        let enclosing = started.elapsed().as_nanos();
        let fields = completed.execution.fields(reused).unwrap();
        let get = |name: &str| {
            fields
                .iter()
                .find(|(key, _)| key == name)
                .unwrap()
                .1
                .as_str()
        };
        assert_eq!(get("native_query_timing_reported"), "true");
        assert_eq!(get("native_query_timing_clock"), "monotonic_wall_not_cpu");
        assert_eq!(get("native_result_serialization_nanos"), "0");
        assert!(get("native_query_execution_nanos").parse::<u128>().unwrap() <= enclosing);
        let result = Arc::new(completed.result);
        assert_eq!(integers(&result, 0), vec![15, 16]);
        // Later report/export calls must retain this execution's original clock.
        assert_eq!(fields, completed.execution.fields(reused).unwrap());
    }
}

fn binding(source: ResidentMemorySource) -> SqlSourceBinding {
    SqlSourceBinding {
        reference: "memory://input".into(),
        source: SqlSource::Memory(source),
    }
}

fn integers(
    result: &Arc<shardloom_vortex::resident_session::OwnedVortexResultBatch>,
    column: usize,
) -> Vec<i64> {
    let mut stream = result.arrow_batches(CancellationToken::default()).unwrap();
    let mut values = Vec::new();
    while let Some(batch) = stream.next_batch().unwrap() {
        let values_column = batch.column(column).as_primitive::<Int64Type>();
        assert_eq!(values_column.null_count(), 0);
        values.extend(values_column.values().iter().copied());
    }
    assert!(stream.report().complete);
    values
}

#[test]
fn embedded_sql_retains_large_results_and_composes_without_rows() {
    let session = session();
    let memory = session.shared_memory_pool();
    let rows = 131_073;
    let source = ResidentMemorySource::from_int64_range(&session, "value", 0, 1, rows).unwrap();
    let inputs = [binding(source)];
    let prepared = prepare_sql(
        "SELECT value * 3 AS value FROM 'memory://input'",
        allocation(),
        &session,
        &inputs,
        &CancellationToken::default(),
    )
    .unwrap();
    let completed = prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    assert_eq!(completed.execution.resources, allocation());
    assert!(completed.execution.native_io_certificate.is_certified());
    assert_eq!(completed.result.row_count(), rows as u64);
    let result = Arc::new(completed.result);
    drop(completed.execution);
    let next = ResidentMemorySource::from_completed_result(&result, &CancellationToken::default())
        .unwrap();
    let next_inputs = [binding(next)];
    let chained_prepared = prepare_sql(
        "SELECT value + 1 AS value FROM 'memory://input'",
        allocation(),
        &session,
        &next_inputs,
        &CancellationToken::default(),
    )
    .unwrap();
    let chained_execution = chained_prepared
        .execute_native(&CancellationToken::default())
        .unwrap();
    assert_eq!(chained_execution.execution.resources, allocation());
    let chained = Arc::new(chained_execution.result);
    drop(chained_execution.execution);
    drop((
        session,
        prepared,
        chained_prepared,
        inputs,
        next_inputs,
        result,
    ));
    assert!(memory.snapshot().reserved_bytes > 0);
    assert_eq!(
        integers(&chained, 0),
        (0..rows)
            .map(|row| i64::try_from(row).unwrap() * 3 + 1)
            .collect::<Vec<_>>()
    );
    drop(chained);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn embedded_resource_overrides_preserve_complete_results_and_shared_report_fields() {
    let configured =
        ExecutionResources::from_bytes(8 << 20, 2, ExecutionResourceOrigin::Context).unwrap();
    let controller =
        shardloom_vortex::resident_session::ConfigurableResidentSession::new(configured).unwrap();
    let cancellation = CancellationToken::default();
    let rows = 17_003;
    let (result, pool) = controller
        .with_resources(configured, &cancellation, |session, resources| {
            let source = ResidentMemorySource::from_int64_range(session, "value", 0, 1, rows)?;
            let prepared = prepare_sql(
                "SELECT * FROM 'memory://input'",
                resources,
                session,
                &[binding(source)],
                &cancellation,
            )?;
            let completed = prepared.execute_native(&cancellation)?;
            let fields = execution_fields(&completed.execution, false);
            assert!(fields.contains(&("output_row_count".into(), rows.to_string())));
            assert!(fields.contains(&(
                "execution_resource_declared_memory_bytes".into(),
                (8_u64 << 20).to_string()
            )));
            assert!(
                fields.contains(&("execution_resource_memory_origin".into(), "context".into()))
            );
            assert!(completed.execution.native_io_certificate.is_certified());
            Ok((Arc::new(completed.result), session.shared_memory_pool()))
        })
        .unwrap();
    let before = pool.snapshot().reserved_bytes;
    assert!(before > 1024);
    let small =
        ExecutionResources::from_bytes(1024, 1, ExecutionResourceOrigin::ExecutionCall).unwrap();
    let rejected: Result<()> = controller.with_resources(small, &cancellation, |_, _| {
        panic!("cannot inspect a source before retained-owner admission")
    });
    assert!(
        rejected
            .unwrap_err()
            .to_string()
            .contains("retained owners")
    );
    assert_eq!(pool.snapshot().reserved_bytes, before);
    let larger =
        ExecutionResources::from_bytes(16 << 20, 1, ExecutionResourceOrigin::ExecutionCall)
            .unwrap();
    let chained = controller
        .with_resources(larger, &cancellation, |session, resources| {
            let source = ResidentMemorySource::from_completed_result(&result, &cancellation)?;
            let prepared = prepare_sql(
                "SELECT value + 1 AS value FROM 'memory://input'",
                resources,
                session,
                &[binding(source)],
                &cancellation,
            )?;
            let completed = prepared.execute_native(&cancellation)?;
            assert_eq!(completed.execution.resources, larger);
            assert_eq!(completed.execution.runtime.admitted_max_parallelism, 1);
            Ok(Arc::new(completed.result))
        })
        .unwrap();
    drop(result);
    let smaller =
        ExecutionResources::from_bytes(4 << 20, 1, ExecutionResourceOrigin::ExecutionCall).unwrap();
    controller
        .with_resources(smaller, &cancellation, |_, _| {
            assert_eq!(
                integers(&chained, 0),
                (1..=i64::try_from(rows).unwrap()).collect::<Vec<_>>()
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(controller.configured_resources(), configured);
    drop(controller);
    assert_eq!(
        integers(&chained, 0),
        (1..=i64::try_from(rows).unwrap()).collect::<Vec<_>>()
    );
    drop(chained);
    assert_eq!(pool.snapshot().reserved_bytes, 0);
}

#[test]
fn embedded_dynamic_sql_uses_the_same_session_for_repeated_execution() {
    let session = session();
    let source = ResidentMemorySource::from_columns(
        &session,
        &[
            MemoryColumn {
                name: "entity",
                values: MemoryColumnValues::Int64(&[Some(1), Some(1), Some(2), Some(2)]),
            },
            MemoryColumn {
                name: "category",
                values: MemoryColumnValues::Utf8(&[Some("a"), Some("b"), Some("a"), Some("b")]),
            },
            MemoryColumn {
                name: "amount",
                values: MemoryColumnValues::Int64(&[Some(2), Some(3), Some(5), Some(7)]),
            },
        ],
        MemorySourceBounds::default(),
    )
    .unwrap();
    let inputs = [binding(source)];
    let options = r#"{"index":"entity","columns":"category","values":"amount","aggregate":"sum"}"#;
    let statement = format!(
        "SELECT * FROM PIVOT((SELECT * FROM 'memory://input'), '{options}') AS p ORDER BY entity"
    );
    let prepared = prepare_sql(
        &statement,
        allocation(),
        &session,
        &inputs,
        &CancellationToken::default(),
    )
    .unwrap();
    assert!(prepared.output_dtype().is_none());
    for execution in 1..=2 {
        let result = prepared
            .collect_jsonl(&CancellationToken::default())
            .unwrap();
        let values = result
            .result_jsonl
            .value()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            vec![
                serde_json::json!({"entity": 1, "pivot_a": 2.0, "pivot_b": 3.0}),
                serde_json::json!({"entity": 2, "pivot_a": 5.0, "pivot_b": 7.0}),
            ]
        );
        assert_eq!(result.execution.resources, allocation());
        assert_eq!(session.snapshot().completed_executions, execution);
    }
}

#[test]
fn embedded_sql_rejects_cancelled_or_conflicting_resources_before_source_inspection() {
    let session = session();
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    let statement = "SELECT value FROM 'absent-input-for-embedded-source.csv'";
    let error = prepare_sql(statement, allocation(), &session, &[], &cancelled)
        .err()
        .unwrap();
    assert!(error.to_string().contains("cancel"));
    let smaller =
        ExecutionResources::from_bytes(8 << 20, 1, ExecutionResourceOrigin::ExecutionCall).unwrap();
    let error = prepare_sql(
        statement,
        smaller,
        &session,
        &[],
        &CancellationToken::default(),
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("session exceeds"));
    assert_eq!(session.snapshot().prepared_source_opens, 0);
    assert_eq!(session.snapshot().completed_executions, 0);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn embedded_sql_rejects_foreign_native_owners_and_retries_after_shared_pressure() {
    let session = session();
    let foreign = self::session();
    let source = ResidentMemorySource::from_int64_range(&foreign, "value", 0, 1, 3).unwrap();
    let error = prepare_sql(
        "SELECT * FROM 'memory://input'",
        allocation(),
        &session,
        &[binding(source)],
        &CancellationToken::default(),
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("another resource owner"));
    let source = ResidentMemorySource::from_int64_range(&session, "value", 0, 1, 3).unwrap();
    let inputs = [binding(source)];
    let memory = session.shared_memory_pool();
    let baseline = memory.snapshot().reserved_bytes;
    let occupied = memory
        .reserve(memory.snapshot().limit_bytes - baseline)
        .unwrap();
    assert!(
        prepare_sql(
            "SELECT * FROM 'memory://input'",
            allocation(),
            &session,
            &inputs,
            &CancellationToken::default()
        )
        .is_err()
    );
    drop(occupied);
    assert_eq!(memory.snapshot().reserved_bytes, baseline);
    let operation = prepare_sql(
        "SELECT * FROM 'memory://input'",
        allocation(),
        &session,
        &inputs,
        &CancellationToken::default(),
    )
    .unwrap();
    let result = Arc::new(
        operation
            .execute_native(&CancellationToken::default())
            .unwrap()
            .result,
    );
    assert_eq!(integers(&result, 0), [0, 1, 2]);
}

#[test]
fn embedded_operation_and_columnar_stream_can_detach_from_python() {
    fn send<T: Send>() {}
    send::<PreparedVortexRelational>();
    send::<shardloom_vortex::resident_session::NativeArrowBatchStream>();
}
