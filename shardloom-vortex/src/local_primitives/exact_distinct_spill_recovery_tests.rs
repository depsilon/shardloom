use super::*;
use crate::local_primitives::query_run_store::operator_recovery_tests as recovery;

#[test]
fn exact_distinct_recovery_process_child() {
    let Some(input) = recovery::child_input() else {
        return;
    };
    assert_eq!(input.case, "distinct");
    let request = Fixture::query_paths(&input.source, &input.workspace, 123, 7);
    execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).unwrap(),
    )
    .unwrap();
    panic!("DISTINCT completed without parking at validated native runs");
}

#[test]
fn exact_distinct_spill_process_death_cleanup_and_exact_restart() {
    let fixture = Fixture::new(131_072);
    let request = fixture.query(123, 7);
    let spill = request
        .simple_aggregate
        .as_ref()
        .unwrap()
        .spill
        .as_ref()
        .unwrap();
    recovery::recover_operator(
        concat!(module_path!(), "::exact_distinct_recovery_process_child"),
        &fixture.workspace(),
        &fixture.path(),
        "distinct",
        |directory| spill.cleanup_abandoned(directory),
    );
    let report = execute_vortex_local_primitive_with_policy(
        &request,
        VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).unwrap(),
    )
    .unwrap();
    assert_eq!(values(&report), fixture.expected(123, 7, i16::MIN));
    let evidence = report.state_budget.native_aggregate_spill.as_ref().unwrap();
    assert!(evidence.runs_written >= 2);
    assert_eq!(evidence.runs_written, evidence.runs_validated);
    assert!(evidence.peak_reserved_bytes <= evidence.memory_bytes);
    assert!(evidence.peak_disk_bytes <= evidence.quota_bytes);
    assert!(evidence.owned_cleanup_completed && report.write_io && report.spill_io_performed);
    assert!(!report.arrow_converted && !report.fallback_execution_allowed);
    assert!(local_primitive_native_io_safe(&request, &report));
    assert_eq!(local_primitive_row_count(&report), Some(7));
    fixture.empty();
}
