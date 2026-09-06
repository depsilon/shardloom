use super::super::{
    ProjectionRequest, VortexQueryPrimitiveRequest, attach_predicate_to_scan_plan,
    local_primitive_native_io_certificate, project_count_where_predicate_columns,
    projection_scan_plan,
};
use super::*;
use shardloom_core::{ColumnRef, ComparisonOp, PredicateExpr, StatValue};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "shardloom-prepared-scan-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/local_primitive_struct_five.vortex");
        std::fs::copy(source, directory.join("source.vortex")).unwrap();
        Self(directory)
    }
    fn path(&self) -> PathBuf {
        self.0.join("source.vortex")
    }
    fn uri(&self) -> DatasetUri {
        DatasetUri::new(self.path().display().to_string()).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn greater_than(value: u64) -> PredicateExpr {
    PredicateExpr::Compare {
        column: ColumnRef::new("value").unwrap(),
        op: ComparisonOp::Gt,
        value: StatValue::UInt64(value),
    }
}

fn count_plan(
    dtype: &vortex::array::dtype::DType,
    predicate: &PredicateExpr,
) -> Result<LocalVortexScanPlan> {
    let mut plan = LocalVortexScanPlan::passthrough();
    let lowered = attach_predicate_to_scan_plan(
        &mut plan,
        predicate,
        dtype,
        VortexQueryPrimitiveKind::CountWhere,
    )?;
    project_count_where_predicate_columns(&mut plan, &lowered, dtype);
    Ok(plan)
}

#[test]
fn resident_count_scan_preserves_exact_count_pruning_and_native_certificate() {
    let fixture = Fixture::new();
    let policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    // The checked fixture contains exactly value=1..5 and metric=10..50.
    for (predicate, expected, pruned) in [(greater_than(2), 3, false), (greater_than(99), 0, true)]
    {
        let scan = super::super::read_local_vortex_scan(
            &fixture.uri(),
            &fixture.path(),
            VortexQueryPrimitiveKind::CountWhere,
            policy,
            |dtype| count_plan(dtype, &predicate),
        )
        .unwrap();
        assert_eq!(scan.source_row_count, 5);
        assert_eq!(scan.result_row_count, expected);
        assert_eq!(scan.pre_limit_result_row_count, expected);
        assert_eq!(scan.metadata_pruned_entire_input(), pruned);
        assert_eq!(scan.data_read(), !pruned);
        assert_eq!(scan.arrays_read_count == 0, pruned);
        assert_eq!(scan.projected_columns, Vec::<String>::new());
        assert!(scan.projection_pushdown_applied);
        assert!(!scan.residual_predicate_materialized());
        assert!(scan.evidence_collector().full_replay_evidence_preserved);
        let request = VortexQueryPrimitiveRequest::count_where(fixture.uri(), predicate.clone());
        let report =
            super::super::count_where_report(&fixture.path(), &request, &scan, &predicate).unwrap();
        assert_eq!(report.rows_selected, Some(expected as u64));
        assert!(
            local_primitive_native_io_certificate(&request, &report)
                .unwrap()
                .is_certified()
        );
        assert!(!report.fallback_execution_allowed);
    }
}

#[test]
fn resident_projection_preserves_filter_limit_and_prelimit_row_evidence() {
    let fixture = Fixture::new();
    let policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    let kind = VortexQueryPrimitiveKind::FilterAndProject;
    let scan = read(&fixture.uri(), &fixture.path(), kind, policy, |dtype| {
        let mut plan = projection_scan_plan(
            dtype,
            &ProjectionRequest::columns(vec![ColumnRef::new("metric").unwrap()]),
            kind,
        )?;
        attach_predicate_to_scan_plan(&mut plan, &greater_than(2), dtype, kind)?;
        plan.source_order_limit = Some(2);
        Ok(plan)
    })
    .unwrap();
    assert_eq!(scan.result_row_count, 2);
    assert_eq!(scan.pre_limit_result_row_count, 3);
    assert_eq!(scan.projected_columns, ["metric"]);
    assert_eq!(scan.source_order_limit, Some(2));
    assert!(scan.projection_pushdown_applied && scan.filter_pushdown_applied);
    assert!(!scan.residual_predicate_materialized());
    assert_eq!(scan.reader_splits.len(), scan.arrays_read_count);
}

#[test]
fn resident_scan_preserves_declared_residual_evaluation_before_limit() {
    let fixture = Fixture::new();
    let kind = VortexQueryPrimitiveKind::FilterAndProject;
    let scan = read(
        &fixture.uri(),
        &fixture.path(),
        kind,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        |dtype| {
            let mut plan = projection_scan_plan(
                dtype,
                &ProjectionRequest::columns(vec![
                    ColumnRef::new("value").unwrap(),
                    ColumnRef::new("metric").unwrap(),
                ]),
                kind,
            )?;
            // Exercise the retained internal residual boundary explicitly; the
            // ordinary frontend currently pushes this predicate into Vortex.
            plan.residual_predicate = Some(greater_than(2));
            plan.output_columns = Some(vec!["metric".into()]);
            plan.source_order_limit = Some(2);
            Ok(plan)
        },
    )
    .unwrap();
    assert_eq!(scan.result_row_count, 2);
    assert_eq!(scan.pre_limit_result_row_count, 3);
    assert_eq!(scan.projected_columns, ["metric"]);
    assert!(!scan.filter_pushdown_applied);
    assert_ne!(
        scan.residual_predicate_materialization,
        ResidualPredicateMaterialization::None
    );
}

#[test]
fn resident_scan_rejects_replaced_generation_before_execution_and_keeps_new_destination() {
    let fixture = Fixture::new();
    let path = fixture.path();
    let original = fixture.0.join("original.vortex");
    let result = read(
        &fixture.uri(),
        &path,
        VortexQueryPrimitiveKind::ProjectColumns,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
        |dtype| {
            let plan = projection_scan_plan(
                dtype,
                &ProjectionRequest::all(),
                VortexQueryPrimitiveKind::ProjectColumns,
            )?;
            std::fs::rename(&path, &original).unwrap();
            std::fs::copy(&original, &path).unwrap();
            Ok(plan)
        },
    );
    assert!(result.is_err());
    assert_eq!(
        std::fs::read(&path).unwrap(),
        std::fs::read(original).unwrap()
    );
}

#[test]
fn resident_scan_releases_native_payload_after_success_and_rejects_zero_limit() {
    let fixture = Fixture::new();
    let policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    let resident = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let source = resident.prepare_file(fixture.path()).unwrap();
    for limit in [None, Some(0)] {
        let result = read_prepared(
            &fixture.uri(),
            &source,
            VortexQueryPrimitiveKind::ProjectColumns,
            policy,
            |dtype| {
                let mut plan = projection_scan_plan(
                    dtype,
                    &ProjectionRequest::all(),
                    VortexQueryPrimitiveKind::ProjectColumns,
                )?;
                plan.source_order_limit = limit;
                Ok(plan)
            },
        );
        assert_eq!(result.is_ok(), limit.is_none());
        drop(result);
    }
    assert_eq!(resident.snapshot().prepared_source_opens, 1);
    assert_eq!(resident.snapshot().completed_executions, 1);
    drop(source);
    assert_eq!(resident.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn retained_scan_reexecutes_exact_counts_including_pruning_without_reopening() {
    let fixture = Fixture::new();
    let policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    let resident = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let source = resident.prepare_file(fixture.path()).unwrap();
    for (index, threshold) in [2, 99, 0, 4, 99, 2].into_iter().enumerate() {
        let predicate = greater_than(threshold);
        let scan = read_prepared(
            &fixture.uri(),
            &source,
            VortexQueryPrimitiveKind::CountWhere,
            policy,
            |dtype| count_plan(dtype, &predicate),
        )
        .unwrap();
        let expected = (1_u64..=5).filter(|value| *value > threshold).count();
        assert_eq!(scan.result_row_count, expected);
        assert_eq!(scan.pre_limit_result_row_count, expected);
        assert_eq!(scan.metadata_pruned_entire_input(), threshold > 5);
        assert_eq!(resident.snapshot().prepared_source_opens, 1);
        assert_eq!(resident.snapshot().completed_executions, (index + 1) as u64);
    }
    drop(source);
    assert_eq!(resident.snapshot().memory.reserved_bytes, 0);
    assert_eq!(resident.snapshot().memory.denied_reservations, 0);
}

#[test]
fn retained_scan_rejects_changed_source_even_when_predicate_would_prune() {
    let fixture = Fixture::new();
    let policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    let resident = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let source = resident.prepare_file(fixture.path()).unwrap();
    let predicate = greater_than(99);
    let first = read_prepared(
        &fixture.uri(),
        &source,
        VortexQueryPrimitiveKind::CountWhere,
        policy,
        |dtype| count_plan(dtype, &predicate),
    )
    .unwrap();
    assert!(first.metadata_pruned_entire_input());
    let original = fixture.0.join("old.vortex");
    std::fs::rename(fixture.path(), &original).unwrap();
    std::fs::copy(&original, fixture.path()).unwrap();
    for restored in [false, true] {
        if restored {
            std::fs::remove_file(fixture.path()).unwrap();
            std::fs::rename(&original, fixture.path()).unwrap();
        }
        assert!(
            read_prepared(
                &fixture.uri(),
                &source,
                VortexQueryPrimitiveKind::CountWhere,
                policy,
                |dtype| count_plan(dtype, &predicate)
            )
            .is_err()
        );
    }
    assert_eq!(resident.snapshot().prepared_source_opens, 1);
    assert_eq!(resident.snapshot().completed_executions, 1);
    drop(source);
    assert_eq!(resident.snapshot().memory.reserved_bytes, 0);
}

#[test]
fn retained_scan_cannot_claim_tighter_resources_than_its_source_owner() {
    let fixture = Fixture::new();
    let resident = ResidentVortexSession::new(8 << 20, 1).unwrap();
    let source = resident.prepare_file(fixture.path()).unwrap();
    let mut policy = VortexLocalPrimitiveExecutionPolicy::single_threaded();
    policy.resource_envelope.memory_budget_bytes = 8 << 20;
    for smaller_memory in [true, false] {
        let mut rejected = policy;
        if smaller_memory {
            rejected.resource_envelope.memory_budget_bytes /= 2;
        } else {
            rejected.resource_envelope.max_parallelism = 0;
        }
        let error = read_prepared(
            &fixture.uri(),
            &source,
            VortexQueryPrimitiveKind::ProjectColumns,
            rejected,
            |_| panic!("resource refusal must precede planning"),
        )
        .err()
        .unwrap();
        assert!(
            error
                .to_string()
                .contains("exceeds the scan resource policy")
        );
    }
    assert_eq!(resident.snapshot().completed_executions, 0);
    let scan = read_prepared(
        &fixture.uri(),
        &source,
        VortexQueryPrimitiveKind::ProjectColumns,
        policy,
        |dtype| {
            projection_scan_plan(
                dtype,
                &ProjectionRequest::all(),
                VortexQueryPrimitiveKind::ProjectColumns,
            )
        },
    )
    .unwrap();
    assert_eq!(scan.result_row_count, 5);
    drop(source);
    assert_eq!(resident.snapshot().memory.reserved_bytes, 0);
}
