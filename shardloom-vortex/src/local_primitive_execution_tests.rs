use super::*;
use crate::local_primitives::{collect, execution};
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
    let fixture = Fixture::new();
    let policy = policy(SESSION_BYTES, 2);
    let session = ResidentVortexSession::new(SESSION_BYTES, 2).unwrap();
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
