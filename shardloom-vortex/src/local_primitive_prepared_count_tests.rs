use super::*;
use shardloom_core::{ColumnRef, ComparisonOp, DatasetUri, PredicateExpr, StatValue};
use shardloom_exec::compute_pool::CancellationToken;
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
fn filtered_count_source_handoff_reuses_one_generation() {
    let fixture = Fixture::new();
    let request = fixture.request(2);
    let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).unwrap();
    let source = super::super::prepared_dispatch::prepare_source(&request, policy).unwrap();
    let session = source.retained_session();
    let prepared = prepare_count_where_from_source(&request, policy, source).unwrap();
    for execution in 1..=2 {
        let result = prepared.execute().unwrap();
        assert_eq!(result.count, 3);
        assert_eq!(result.runtime.prepared_source_opens, 1);
        assert_eq!(result.runtime.completed_executions, execution);
        assert!(result.native_io_certificate.is_certified());
        assert!(!result.native_io_certificate.fallback_attempted);
    }
    let replacement = fixture.0.join("replacement.vortex");
    std::fs::copy(fixture.path(), &replacement).unwrap();
    std::fs::rename(replacement, fixture.path()).unwrap();
    assert!(prepared.execute().is_err());
    assert_eq!(session.snapshot().prepared_source_opens, 1);
}

#[test]
fn configurable_filtered_count_uses_current_grant_for_every_result_boundary() {
    use crate::resident_session::ConfigurableResidentSession;
    use shardloom_core::{ExecutionResourceOrigin, ExecutionResources};
    use shardloom_exec::compute_pool::bounded_cpu_parallelism;

    let resources = |lanes| {
        ExecutionResources::from_bytes(32 << 20, lanes, ExecutionResourceOrigin::ExecutionCall)
            .unwrap()
    };
    let fixture = Fixture::new();
    let controller = ConfigurableResidentSession::new(resources(2)).unwrap();
    let token = CancellationToken::default();
    let (prepared, memory) = controller
        .with_resources(resources(2), &token, |session, admitted| {
            let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(admitted)?;
            Ok((
                prepare_count_where_in_session(&fixture.request(2), policy, session)?,
                session.shared_memory_pool(),
            ))
        })
        .unwrap();
    let mut completed = 0;
    for lanes in [2, 1, 2] {
        controller
            .with_resources(resources(lanes), &token, |session, _| {
                for boundary in 0..3 {
                    let execution = match boundary {
                        0 => prepared.execute_with_cancellation(&token)?,
                        1 => prepared.collect_jsonl_controlled(&token)?.0,
                        _ => {
                            let result = prepared.execute_native_result(&token)?;
                            assert_native_count(&result.result, 3);
                            result.execution
                        }
                    };
                    completed += 1;
                    assert_eq!(execution.count, 3);
                    assert!(execution.native_io_certificate.is_certified());
                    assert!(!execution.native_io_certificate.fallback_attempted);
                    assert_eq!(
                        execution.runtime.provider_background_workers,
                        bounded_cpu_parallelism(lanes) - 1
                    );
                    assert_eq!(execution.runtime.prepared_source_opens, 1);
                    assert_eq!(execution.runtime.completed_executions, completed);
                    assert_eq!(session.snapshot().provider_background_workers, 0);
                }
                let cancelled = CancellationToken::default();
                cancelled.cancel();
                assert!(prepared.execute_with_cancellation(&cancelled).is_err());
                assert_eq!(session.snapshot().completed_executions, completed);
                Ok(())
            })
            .unwrap();
    }
    drop(prepared);
    drop(controller);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn configurable_metadata_pruned_count_does_not_start_provider_workers() {
    use crate::resident_session::ConfigurableResidentSession;
    use shardloom_core::{ExecutionResourceOrigin, ExecutionResources};

    let resources =
        ExecutionResources::from_bytes(32 << 20, 2, ExecutionResourceOrigin::ExecutionCall)
            .unwrap();
    let fixture = Fixture::new();
    let controller = ConfigurableResidentSession::new(resources).unwrap();
    let token = CancellationToken::default();
    let memory = controller
        .with_resources(resources, &token, |session, admitted| {
            let policy = VortexLocalPrimitiveExecutionPolicy::from_resources(admitted)?;
            let prepared = prepare_count_where_in_session(&fixture.request(99), policy, session)?;
            for boundary in 0..3 {
                let execution = match boundary {
                    0 => prepared.execute_with_cancellation(&token)?,
                    1 => prepared.collect_jsonl_controlled(&token)?.0,
                    _ => {
                        let result = prepared.execute_native_result(&token)?;
                        assert_native_count(&result.result, 0);
                        result.execution
                    }
                };
                assert_eq!(execution.count, 0);
                assert!(
                    execution
                        .report
                        .embedded_layout
                        .metadata_pruned_entire_input
                );
                assert!(!execution.report.data_read);
                assert!(execution.native_io_certificate.is_certified());
                assert_eq!(execution.runtime.provider_background_workers, 0);
                assert_eq!(execution.runtime.prepared_source_opens, 1);
                assert_eq!(execution.runtime.completed_executions, boundary + 1);
            }
            let cancelled = CancellationToken::default();
            cancelled.cancel();
            assert!(prepared.execute_with_cancellation(&cancelled).is_err());
            let replacement = fixture.0.join("replacement.vortex");
            std::fs::copy(fixture.path(), &replacement).unwrap();
            std::fs::rename(replacement, fixture.path()).unwrap();
            assert!(prepared.execute_with_cancellation(&token).is_err());
            assert_eq!(session.snapshot().completed_executions, 3);
            assert_eq!(session.snapshot().provider_background_workers, 0);
            Ok(session.shared_memory_pool())
        })
        .unwrap();
    drop(controller);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn prepared_count_reexecutes_complete_predicate_with_one_open_and_real_certificate() {
    let fixture = Fixture::new();
    for threshold in [2, 99] {
        let session = ResidentVortexSession::new(8 << 20, 2).unwrap();
        let request = fixture.request(threshold);
        let prepared = prepare_count_where_in_session(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).unwrap(),
            &session,
        )
        .unwrap();
        assert_eq!(prepared.source_row_count(), 5);
        assert_eq!(session.snapshot().prepared_source_opens, 1);
        assert_eq!(session.snapshot().completed_executions, 0);
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
            assert_eq!(result.report.projected_columns, [] as [String; 0]);
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

fn assert_native_count(result: &crate::resident_session::OwnedVortexResultBatch, expected: u64) {
    use vortex::array::dtype::{DType, Nullability, PType};
    assert_eq!(result.row_count(), 1);
    assert_eq!(
        result
            .dtype()
            .as_struct_fields_opt()
            .unwrap()
            .field("count_all"),
        Some(DType::Primitive(PType::U64, Nullability::NonNullable)),
    );
    let rendered = result.to_bounded_json(&["count_all".into()], 1024).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(rendered.value()).unwrap(),
        serde_json::json!([{"count_all": expected}]),
    );
}

#[test]
fn native_count_result_preserves_full_unsigned_values_and_releases_metadata() {
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    for count in [0, 5, u64::MAX] {
        let result = session
            .own_count_result(count, &CancellationToken::default())
            .unwrap();
        assert!(session.snapshot().memory.reserved_bytes > 0);
        assert_native_count(&result, count);
        drop(result);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
    let retained = session.reserve_input_scratch((1 << 20) - 1).unwrap();
    assert!(
        session
            .own_count_result(5, &CancellationToken::default())
            .is_err()
    );
    assert_eq!(session.snapshot().memory.reserved_bytes, (1 << 20) - 1);
    drop(retained);
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    assert!(session.own_count_result(5, &cancelled).is_err());
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn native_footer_count_result_retains_owner_after_source_and_session_drop() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
    let memory = session.shared_memory_pool();
    let source = session.prepare_file(fixture.path()).unwrap();
    let prepared = source.prepare_count();
    let before = session.snapshot();
    let cancelled = CancellationToken::default();
    cancelled.cancel();
    assert!(prepared.execute_native_result(&cancelled).is_err());
    assert_eq!(session.snapshot(), before);
    let (count, result) = prepared
        .execute_native_result(&CancellationToken::default())
        .unwrap();
    assert_eq!(count, 5);
    assert_eq!(session.snapshot().prepared_source_opens, 1);
    assert_eq!(session.snapshot().completed_executions, 1);
    drop((prepared, source, session));
    assert!(memory.snapshot().reserved_bytes > 0);
    assert_native_count(&result, 5);
    drop(result);
    assert_eq!(memory.snapshot().reserved_bytes, 0);
}

#[test]
fn native_filtered_count_results_keep_pruned_and_scanned_evidence_and_lifetimes() {
    let fixture = Fixture::new();
    for threshold in [2, 99] {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        let prepared = prepare_count_where_in_session(
            &fixture.request(threshold),
            VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4).unwrap(),
            &session,
        )
        .unwrap();
        let before = session.snapshot();
        let cancelled = CancellationToken::default();
        cancelled.cancel();
        assert!(prepared.execute_native_result(&cancelled).is_err());
        assert_eq!(session.snapshot(), before);
        let result = prepared
            .execute_native_result(&CancellationToken::default())
            .unwrap();
        let expected = if threshold == 99 { 0 } else { 3 };
        assert_eq!(result.execution.count, expected);
        assert!(result.execution.native_io_certificate.is_certified());
        assert!(!result.execution.native_io_certificate.fallback_attempted);
        assert_eq!(result.execution.report.data_read, threshold != 99);
        assert_eq!(
            result
                .execution
                .report
                .embedded_layout
                .metadata_pruned_entire_input,
            threshold == 99
        );
        drop(prepared);
        assert_native_count(&result.result, expected);
        drop(result);
        assert_eq!(session.snapshot().memory.reserved_bytes, 0);
    }
}

#[test]
fn prepared_unprunable_empty_count_reports_provider_work_and_certifies_zero() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let request = VortexQueryPrimitiveRequest::count_where(
        DatasetUri::new(fixture.path().display().to_string()).unwrap(),
        PredicateExpr::Compare {
            column: ColumnRef::new("metric").unwrap(),
            op: ComparisonOp::Eq,
            value: StatValue::UInt64(17),
        },
    );
    let prepared = prepare_count_where_in_session(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4)
            .expect("explicit fixture allocation"),
        &session,
    )
    .unwrap();
    for execution in 1..=3 {
        let result = prepared.execute().unwrap();
        // The fixture has metrics 10,20,30,40,50; 17 is inside its bounds.
        assert_eq!(result.count, 0);
        assert_eq!(result.report.arrays_read_count, 0);
        assert!(!result.report.embedded_layout.metadata_pruned_entire_input);
        assert!(result.report.upstream_scan_called);
        assert!(
            result.report.data_read
                && result.report.data_decoded
                && result.report.data_materialized
        );
        assert!(!result.report.row_read && !result.report.arrow_converted);
        assert!(result.report.materialization_boundary_reported);
        assert!(
            result
                .report
                .result_summary
                .as_ref()
                .unwrap()
                .contains("not_observed_bytes")
        );
        assert!(result.native_io_certificate.is_certified());
        assert_eq!(result.runtime.prepared_source_opens, 1);
        assert_eq!(result.runtime.completed_executions, execution);
    }
    drop(prepared);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn prepared_metadata_pruned_count_rejects_generation_change_and_never_replays_old_zero() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let prepared = prepare_count_where_in_session(
        &fixture.request(99),
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4)
            .expect("explicit fixture allocation"),
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
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4)
            .expect("explicit fixture allocation"),
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
    let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4)
        .expect("explicit fixture allocation");
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

#[test]
fn cancelled_count_preparation_rejects_missing_source_before_open() {
    let fixture = Fixture::new();
    let missing = fixture.0.join("missing.vortex");
    let request = VortexQueryPrimitiveRequest::count_where(
        DatasetUri::new(missing.display().to_string()).unwrap(),
        PredicateExpr::Compare {
            column: ColumnRef::new("value").unwrap(),
            op: ComparisonOp::Gt,
            value: StatValue::UInt64(2),
        },
    );
    let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4)
        .expect("explicit fixture allocation");
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();

    let cancelled = CancellationToken::default();
    cancelled.cancel();
    let expired = CancellationToken::default().with_deadline(std::time::Instant::now());
    for (token, expected) in [
        (&cancelled, "execution cancelled"),
        (&expired, "execution deadline exceeded"),
    ] {
        let error = prepare_count_where_in_session_controlled(&request, policy, &session, token)
            .err()
            .expect("cancelled preparation must fail");
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(session.snapshot().prepared_source_opens, 0);
        assert_eq!(session.snapshot().completed_executions, 0);
    }
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn cancelled_count_execution_and_collection_keep_prepared_credits_and_counters() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let prepared = prepare_count_where_in_session(
        &fixture.request(2),
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4)
            .expect("explicit fixture allocation"),
        &session,
    )
    .unwrap();
    let prepared_credits = session.snapshot().memory.reserved_bytes;
    assert!(prepared_credits > 0);
    assert_eq!(session.snapshot().completed_executions, 0);

    let cancelled = CancellationToken::default();
    cancelled.cancel();
    assert!(prepared.execute_with_cancellation(&cancelled).is_err());
    assert!(prepared.collect_jsonl_controlled(&cancelled).is_err());
    assert_eq!(session.snapshot().memory.reserved_bytes, prepared_credits);
    assert_eq!(session.snapshot().completed_executions, 0);
    assert_eq!(session.snapshot().prepared_source_opens, 1);

    drop(prepared);
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn live_count_token_preserves_exact_counts_json_and_metadata_pruned_zero() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4)
        .expect("explicit fixture allocation");
    let cancellation = CancellationToken::default();

    for (index, (threshold, expected, pruned, json)) in [
        (2, 3, false, "{\"count_all\":3}\n"),
        (99, 0, true, "{\"count_all\":0}\n"),
    ]
    .into_iter()
    .enumerate()
    {
        let prepared = prepare_count_where_in_session_controlled(
            &fixture.request(threshold),
            policy,
            &session,
            &cancellation,
        )
        .unwrap();
        let execution = prepared.execute_with_cancellation(&cancellation).unwrap();
        assert_eq!(execution.count, expected);
        assert_eq!(execution.report.rows_selected, Some(expected));
        assert_eq!(
            execution
                .report
                .embedded_layout
                .metadata_pruned_entire_input,
            pruned
        );
        assert_eq!(execution.runtime.prepared_source_opens, (index + 1) as u64);
        assert!(execution.native_io_certificate.is_certified());

        let (collected, rows) = prepared.collect_jsonl_controlled(&cancellation).unwrap();
        assert_eq!(collected.count, expected);
        let (jsonl, _ownership) = rows.result_jsonl.into_parts();
        assert_eq!(jsonl, json);
        assert_eq!(
            session.snapshot().completed_executions,
            ((index + 1) * 2) as u64
        );
        drop(prepared);
    }
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn live_count_token_preserves_null_predicate_on_nonnullable_fixture() {
    let fixture = Fixture::new();
    let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(1, 4)
        .expect("explicit fixture allocation");
    let request = VortexQueryPrimitiveRequest::count_where(
        DatasetUri::new(fixture.path().display().to_string()).unwrap(),
        PredicateExpr::IsNull {
            column: ColumnRef::new("value").unwrap(),
        },
    );
    let cancellation = CancellationToken::default();
    let prepared =
        prepare_count_where_in_session_controlled(&request, policy, &session, &cancellation)
            .unwrap();
    let (execution, rows) = prepared.collect_jsonl_controlled(&cancellation).unwrap();
    assert_eq!(execution.count, 0);
    assert_eq!(execution.report.rows_selected, Some(0));
    assert!(
        execution
            .report
            .embedded_layout
            .metadata_pruned_entire_input
    );
    let (jsonl, ownership) = rows.result_jsonl.into_parts();
    assert_eq!(jsonl, "{\"count_all\":0}\n");
    drop(prepared);
    // The exported strings remain live independently of the prepared source.
    assert!(session.snapshot().memory.reserved_bytes > 0);
    drop((jsonl, ownership, rows.result_schema_json));
    assert_eq!(session.snapshot().memory.reserved_bytes, 0);
}
