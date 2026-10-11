use super::*;
#[cfg(feature = "vortex-write")]
use crate::local_primitives::VortexLocalPrimitiveRowExportFormat as ExportFormat;
use crate::local_primitives::{collect, execution};
#[cfg(feature = "vortex-write")]
use crate::relational_query::{VortexRelationalPlan, VortexRelationalScan};
#[cfg(feature = "vortex-write")]
use shardloom_core::ScalarValue;
use shardloom_core::{
    ColumnRef, ComparisonOp, DatasetUri, ExecutionResourceOrigin, ExecutionResources,
    PredicateExpr, StatValue,
};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_plan::ProjectionRequest;
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);
const SESSION_BYTES: u64 = 8 << 20;

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom-command-admission-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        std::fs::copy(Self::original(), path.join("source.vortex")).unwrap();
        Self(path)
    }

    fn original() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/local_primitive_struct_five.vortex")
    }

    fn path(&self) -> PathBuf {
        self.0.join("source.vortex")
    }

    fn two_sources(&self) -> [DatasetUri; 2] {
        let second = self.0.join("second.vortex");
        std::fs::copy(self.path(), &second).unwrap();
        [Self::uri(&self.path()), Self::uri(&second)]
    }

    fn uri(path: &Path) -> DatasetUri {
        DatasetUri::new(path.display().to_string()).unwrap()
    }

    fn count_request(&self) -> VortexQueryPrimitiveRequest {
        VortexQueryPrimitiveRequest::count_all(Self::uri(&self.path()))
    }

    fn filter_project_request(&self) -> VortexQueryPrimitiveRequest {
        VortexQueryPrimitiveRequest::filter_and_project(
            Self::uri(&self.path()),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").unwrap(),
                op: ComparisonOp::Gt,
                value: StatValue::UInt64(2),
            },
            ProjectionRequest::columns(vec![ColumnRef::new("metric").unwrap()]),
        )
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn policy(memory_bytes: u64, max_parallelism: usize) -> VortexLocalPrimitiveExecutionPolicy {
    VortexLocalPrimitiveExecutionPolicy::from_resources(
        ExecutionResources::from_bytes(
            memory_bytes,
            max_parallelism,
            ExecutionResourceOrigin::ExecutionCall,
        )
        .unwrap(),
    )
    .unwrap()
}

fn assert_count(report: &VortexLocalPrimitiveExecutionReport, count: u64) {
    let expected = count.to_string();
    assert_eq!(
        report
            .result_summary
            .as_deref()
            .and_then(|summary| summary.rsplit(';').next()),
        Some(expected.as_str())
    );
    assert!(!report.has_errors());
    assert_eq!(report.rows_selected, Some(count));
    assert!(!report.fallback_execution_allowed);
}

#[cfg(feature = "vortex-write")]
fn structured_request(source: &Path) -> VortexQueryPrimitiveRequest {
    VortexQueryPrimitiveRequest::structured_project_rows(
        Fixture::uri(source),
        crate::VortexStructuredProjectionRequest::new(vec![
            crate::VortexStructuredProjectionColumn::new(
                "shipment".into(),
                crate::VortexStructuredProjectionExpr::SourceColumn(
                    ColumnRef::new("value").unwrap(),
                ),
            ),
            crate::VortexStructuredProjectionColumn::new(
                "items".into(),
                crate::VortexStructuredProjectionExpr::ArrayLiteral(vec![
                    ScalarValue::Int64(7),
                    ScalarValue::Null,
                    ScalarValue::Int64(8),
                ]),
            ),
            crate::VortexStructuredProjectionColumn::new(
                "pair".into(),
                crate::VortexStructuredProjectionExpr::StructColumns(vec![
                    ColumnRef::new("value").unwrap(),
                    ColumnRef::new("metric").unwrap(),
                ]),
            ),
        ]),
    )
    .with_source_order_limit(2)
}

#[cfg(feature = "vortex-write")]
fn structured_rows(contents: &str) -> Vec<serde_json::Value> {
    contents
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[cfg(feature = "vortex-write")]
fn expected_structured_rows() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "shipment": 1,
            "items": [7, null, 8],
            "pair": {"value": 1, "metric": 10},
        }),
        serde_json::json!({
            "shipment": 2,
            "items": [7, null, 8],
            "pair": {"value": 2, "metric": 20},
        }),
    ]
}

#[cfg(feature = "vortex-write")]
fn assert_native_write_evidence(
    report: &crate::local_primitives::VortexLocalPrimitiveRowExportReport,
) {
    assert_eq!(
        report.status,
        crate::local_primitives::VortexLocalPrimitiveExecutionStatus::Executed
    );
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.pre_limit_result_row_count, 5);
    assert_eq!(report.source_order_limit_requested, Some(2));
    assert!(report.evidence.pushdown.source_order_limit_applied);
    assert!(
        report
            .evidence
            .native_array_sink
            .as_ref()
            .unwrap()
            .pre_limit_result_row_count_exact
    );
    assert_eq!(report.projected_columns, ["shipment", "items", "pair"]);
    assert!(report.evidence.upstream_scan_called);
    assert!(report.evidence.side_effects.data_read);
    assert!(report.evidence.side_effects.data_materialized);
    assert!(report.evidence.side_effects.write_io);
    assert!(!report.evidence.side_effects.fallback_attempted);
    assert!(!report.evidence.side_effects.fallback_execution_allowed);
}

#[cfg(feature = "vortex-write")]
fn assert_export_denied_by_retained_memory(
    session: &ResidentVortexSession,
    request: &VortexQueryPrimitiveRequest,
    output: &Path,
    format: ExportFormat,
    policy: VortexLocalPrimitiveExecutionPolicy,
) {
    let lease = session.reserve_input_scratch(SESSION_BYTES - 1).unwrap();
    let before = session.snapshot();
    let error = execution::export_in_session(
        request,
        output,
        format,
        false,
        policy,
        session,
        &CancellationToken::default(),
    )
    .expect_err("the retained owner leaves too little memory for the writer");
    assert!(
        error.to_string().contains("memory reservation denied"),
        "{error:#}"
    );
    assert!(!output.exists());
    let after = session.snapshot();
    assert_eq!(after.memory.reserved_bytes, SESSION_BYTES - 1);
    assert_eq!(after.memory.reserved_bytes, before.memory.reserved_bytes);
    drop(lease);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn cancelled_and_expired_calls_reject_single_and_partitioned_counts_before_source_open() {
    let fixture = Fixture::new();
    let missing = Fixture::uri(&fixture.0.join("missing.vortex"));
    let request = VortexQueryPrimitiveRequest::count_all(missing.clone());
    let sources = [
        missing.clone(),
        Fixture::uri(&fixture.0.join("also-missing.vortex")),
    ];
    let policy = policy(SESSION_BYTES, 2);
    let session = ResidentVortexSession::new(SESSION_BYTES, 2).unwrap();

    let cancelled = CancellationToken::default();
    cancelled.cancel();
    let expired = CancellationToken::default().with_deadline(std::time::Instant::now());
    for token in [&cancelled, &expired] {
        assert!(execution::execute_in_session(&request, policy, &session, token).is_err());
        assert!(
            execution::execute_partitioned_in_session(&request, &sources, policy, &session, token,)
                .is_err()
        );
        let snapshot = session.snapshot();
        assert_eq!(snapshot.prepared_source_opens, 0);
        assert_eq!(snapshot.completed_executions, 0);
        assert_eq!(snapshot.memory.reserved_bytes, 0);
    }

    let valid = CancellationToken::default();
    let single =
        execution::execute_in_session(&fixture.count_request(), policy, &session, &valid).unwrap();
    assert_count(&single, 5);
    let sources = fixture.two_sources();
    let partitioned = execution::execute_partitioned_in_session(
        &fixture.count_request(),
        &sources,
        policy,
        &session,
        &valid,
    )
    .unwrap();
    assert_count(&partitioned, 10);
}

#[test]
fn shared_session_lease_denies_single_and_partitioned_count_until_released() {
    for max_parallelism in [1, 2] {
        assert_count_releases_shared_session_lease(max_parallelism);
    }
}

fn assert_count_releases_shared_session_lease(max_parallelism: usize) {
    let fixture = Fixture::new();
    let policy = policy(SESSION_BYTES, max_parallelism);
    // One lane leaves deferred provider destruction for the caller to drive;
    // two lanes also cover cleanup by the session's background worker.
    let session = ResidentVortexSession::new(SESSION_BYTES, max_parallelism).unwrap();
    let sources = fixture.two_sources();
    let lease = session.reserve_input_scratch(SESSION_BYTES - 1).unwrap();

    assert!(
        execution::execute_in_session(
            &fixture.count_request(),
            policy,
            &session,
            &CancellationToken::default(),
        )
        .is_err()
    );
    assert!(
        execution::execute_partitioned_in_session(
            &fixture.count_request(),
            &sources,
            policy,
            &session,
            &CancellationToken::default(),
        )
        .is_err()
    );
    let denied = session.snapshot();
    assert_eq!(denied.memory.reserved_bytes, SESSION_BYTES - 1);
    assert!(denied.memory.peak_reserved_bytes <= denied.memory.limit_bytes);
    drop(lease);

    let single = execution::execute_in_session(
        &fixture.count_request(),
        policy,
        &session,
        &CancellationToken::default(),
    )
    .unwrap();
    assert_count(&single, 5);
    drop(single);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);

    let partitioned = execution::execute_partitioned_in_session(
        &fixture.count_request(),
        &sources,
        policy,
        &session,
        &CancellationToken::default(),
    )
    .unwrap();
    assert_count(&partitioned, 10);
    drop(partitioned);
    let completed = session.snapshot();
    assert_eq!(completed.memory.reserved_bytes, 0);
    assert!(completed.memory.peak_reserved_bytes <= completed.memory.limit_bytes);
}

#[test]
fn filter_project_report_and_native_collect_cover_single_and_partitioned_results() {
    let fixture = Fixture::new();
    let policy = policy(SESSION_BYTES, 2);
    let session = ResidentVortexSession::new(SESSION_BYTES, 2).unwrap();
    let request = fixture.filter_project_request();
    let sources = fixture.two_sources();

    let partitioned = execution::execute_partitioned_in_session(
        &request,
        &sources,
        policy,
        &session,
        &CancellationToken::default(),
    )
    .unwrap();
    assert_eq!(partitioned.rows_selected, Some(6));
    assert_eq!(partitioned.rows_projected, Some(6));
    assert_eq!(partitioned.projected_columns, ["metric"]);
    assert!(!partitioned.fallback_execution_allowed);

    let single =
        execution::execute_in_session(&request, policy, &session, &CancellationToken::default())
            .unwrap();
    assert_eq!(single.rows_selected, Some(3));
    assert_eq!(single.rows_projected, Some(3));
    assert_eq!(single.projected_columns, ["metric"]);
    assert!(!single.fallback_execution_allowed);

    let collected = collect::prepare_rows_in_session(&request, &session)
        .unwrap()
        .execute()
        .unwrap();
    let rows: Vec<serde_json::Value> = serde_json::from_str(collected.values_json.value()).unwrap();
    assert_eq!(collected.rows, 3);
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"metric": 30}),
            serde_json::json!({"metric": 40}),
            serde_json::json!({"metric": 50}),
        ]
    );
    assert!(collected.native_io_certificate.is_certified());
    assert!(!collected.native_io_certificate.fallback_attempted);
}

#[test]
fn mismatched_cpu_or_memory_grant_rejects_single_and_partitioned_before_open() {
    let fixture = Fixture::new();
    let missing = Fixture::uri(&fixture.0.join("missing.vortex"));
    let request = VortexQueryPrimitiveRequest::count_all(missing.clone());
    let sources = [
        missing.clone(),
        Fixture::uri(&fixture.0.join("also-missing.vortex")),
    ];
    let session = ResidentVortexSession::new(SESSION_BYTES, 2).unwrap();

    for mismatched in [policy(SESSION_BYTES, 1), policy(SESSION_BYTES / 2, 2)] {
        assert!(
            execution::execute_in_session(
                &request,
                mismatched,
                &session,
                &CancellationToken::default(),
            )
            .is_err()
        );
        assert!(
            execution::execute_partitioned_in_session(
                &request,
                &sources,
                mismatched,
                &session,
                &CancellationToken::default(),
            )
            .is_err()
        );
        let snapshot = session.snapshot();
        assert_eq!(snapshot.prepared_source_opens, 0);
        assert_eq!(snapshot.completed_executions, 0);
        assert_eq!(snapshot.memory.reserved_bytes, 0);
    }
}

#[cfg(feature = "vortex-write")]
#[test]
fn structured_native_vortex_export_is_admitted_and_preserves_nested_results() {
    let fixture = Fixture::new();
    let policy = policy(SESSION_BYTES, 2);
    let session = ResidentVortexSession::new(SESSION_BYTES, 2).unwrap();
    let request = structured_request(&fixture.path());

    let missing_source = fixture.0.join("cancelled-missing.vortex");
    let cancelled_output = fixture.0.join("cancelled-output.vortex");
    let opens_before_cancel = session.snapshot().prepared_source_opens;
    let cancellation = CancellationToken::default();
    cancellation.cancel();
    assert!(
        execution::export_in_session(
            &structured_request(&missing_source),
            &cancelled_output,
            ExportFormat::Vortex,
            false,
            policy,
            &session,
            &cancellation,
        )
        .is_err()
    );
    assert_eq!(
        session.snapshot().prepared_source_opens,
        opens_before_cancel
    );
    assert!(!cancelled_output.exists());
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);

    let output = fixture.0.join("structured.vortex");
    assert_export_denied_by_retained_memory(
        &session,
        &request,
        &output,
        ExportFormat::Vortex,
        policy,
    );

    let report = execution::export_in_session(
        &request,
        &output,
        ExportFormat::Vortex,
        false,
        policy,
        &session,
        &CancellationToken::default(),
    )
    .unwrap();
    assert_native_write_evidence(&report);
    assert_eq!(report.output_format, "vortex");
    assert!(report.evidence.native_array_sink.is_some());
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);

    let scan = VortexRelationalPlan::Scan(VortexRelationalScan {
        source_uri: Fixture::uri(&output),
        projection: ProjectionRequest::All,
        predicate: None,
    });
    let prepared =
        crate::local_primitives::prepared_relational::prepare_relational(&scan, policy).unwrap();
    let collected = prepared
        .collect_jsonl(&CancellationToken::default())
        .unwrap();
    assert_eq!(
        structured_rows(collected.result_jsonl.value()),
        expected_structured_rows()
    );
}

#[cfg(feature = "vortex-write")]
#[test]
fn structured_jsonl_export_rejects_partial_output_under_memory_pressure() {
    let fixture = Fixture::new();
    let policy = policy(SESSION_BYTES, 2);
    let session = ResidentVortexSession::new(SESSION_BYTES, 2).unwrap();
    let request = structured_request(&fixture.path());
    let output = fixture.0.join("structured.jsonl");

    assert_export_denied_by_retained_memory(
        &session,
        &request,
        &output,
        ExportFormat::Jsonl,
        policy,
    );

    let report = execution::export_in_session(
        &request,
        &output,
        ExportFormat::Jsonl,
        false,
        policy,
        &session,
        &CancellationToken::default(),
    )
    .unwrap();
    assert_native_write_evidence(&report);
    assert_eq!(report.output_format, "jsonl");
    assert_eq!(
        structured_rows(&std::fs::read_to_string(&output).unwrap()),
        expected_structured_rows()
    );
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}
