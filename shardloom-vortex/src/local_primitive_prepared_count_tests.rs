use super::*;
use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue};
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "shardloom-prepared-count-{}-{}",
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
    fn request(&self, threshold: u64) -> VortexQueryPrimitiveRequest {
        VortexQueryPrimitiveRequest::count_where(
            DatasetUri::new(self.path().display().to_string()).unwrap(),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").unwrap(),
                op: ComparisonOp::Gt,
                value: StatValue::UInt64(threshold),
            },
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn prepared_count_reexecutes_complete_predicate_with_one_open_and_real_certificate() {
    let fixture = Fixture::new();
    for threshold in [2, 99] {
        let session = ResidentVortexSession::new(8 << 20, 2).unwrap();
        let request = fixture.request(threshold);
        let prepared = prepare_count_where_in_session(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(2).unwrap(),
            &session,
        )
        .unwrap();
        for execution in 1..=4 {
            let result = prepared.execute().unwrap();
            // Checked fixture values are exactly 1..=5, independent of native filtering.
            let expected = (1..=5_u64).filter(|&value| value > threshold).count() as u64;
            assert_eq!(result.count, expected);
            assert_eq!(result.report.rows_selected, Some(expected));
            assert_eq!(result.runtime.prepared_source_opens, 1);
            assert_eq!(result.runtime.completed_executions, execution);
            assert_eq!(
                result.report.embedded_layout.metadata_pruned_entire_input,
                threshold == 99
            );
            assert_eq!(result.report.data_read, threshold != 99);
            assert!(result.report.projection_pushdown_applied);
            assert!(result.report.projected_columns.is_empty());
            assert!(result.native_io_certificate.is_certified());
            assert!(
                result
                    .native_io_certificate
                    .source_pushdown_report
                    .proof_basis
                    .contains("independent_oracle_not_run")
            );
            assert!(!result.report.fallback_execution_allowed);
        }
        drop(prepared);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn prepared_metadata_pruned_count_rejects_generation_change_and_never_replays_old_zero() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let prepared = prepare_count_where_in_session(
        &fixture.request(99),
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        &session,
    )
    .unwrap();
    assert_eq!(prepared.execute().unwrap().count, 0);
    let replacement = fixture.0.join("replacement.vortex");
    std::fs::copy(Fixture::original(), &replacement).unwrap();
    std::fs::rename(replacement, fixture.path()).unwrap();
    assert!(
        prepared
            .execute()
            .err()
            .unwrap()
            .to_string()
            .contains("prepared source changed")
    );
    assert_eq!(session.snapshot().completed_executions, 1);
    drop(prepared);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    let fresh = prepare_count_where_in_session(
        &fixture.request(99),
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        &session,
    )
    .unwrap();
    assert_eq!(fresh.execute().unwrap().count, 0);
    assert_eq!(session.snapshot().prepared_source_opens, 2);
}

#[test]
fn prepared_count_rejects_extra_payloads_before_open_and_rejects_wider_runtime_grants() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    for limit in [0, 1] {
        assert!(
            prepare_count_where_in_session(
                &fixture.request(2).with_source_order_limit(limit),
                policy,
                &session
            )
            .is_err()
        );
        assert_eq!(session.snapshot().prepared_source_opens, 0);
    }
    let mut policy = policy;
    policy.resource_envelope.memory_budget_bytes = 4 << 20;
    let prepared = prepare_count_where_in_session(&fixture.request(2), policy, &session).unwrap();
    assert!(
        prepared
            .execute()
            .err()
            .unwrap()
            .to_string()
            .contains("exceeds the scan resource policy")
    );
    assert_eq!(session.snapshot().completed_executions, 0);
    drop(prepared);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}
