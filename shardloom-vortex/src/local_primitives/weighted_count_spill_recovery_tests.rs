use super::*;
use crate::local_primitives::query_run_store::operator_recovery_tests as recovery;

fn shape(
    case: &str,
) -> (
    &'static [&'static str],
    usize,
    VortexLocalPrimitiveExecutionPolicy,
) {
    let mut policy = VortexLocalPrimitiveExecutionPolicy::new_with_memory_gb(2, 4).unwrap();
    match case {
        "count_single" => {
            policy.resource_envelope.group_state_soft_item_budget = 1;
            (&["label_renamed"], 1, policy)
        }
        "count_compound" => (&["cohort_renamed", "label_renamed"], 7, policy),
        _ => panic!("unknown weighted COUNT recovery case"),
    }
}

#[test]
fn weighted_count_recovery_process_child() {
    let Some(input) = recovery::child_input() else {
        return;
    };
    let (groups, offset, policy) = shape(&input.case);
    let request = Fixture::query_paths(&input.source, &input.workspace, groups, offset, 7);
    execute_vortex_local_primitive_with_policy(&request, policy).unwrap();
    panic!("COUNT completed without parking at validated native runs");
}

#[test]
fn weighted_count_spill_process_death_cleanup_and_exact_restart() {
    for case in ["count_single", "count_compound"] {
        let fixture = if case == "count_single" {
            Fixture::with_unique_keys(16_384, 128, true, false, true)
        } else {
            Fixture::new(65_536, 128, true, false)
        };
        let (groups, offset, policy) = shape(case);
        let request = fixture.query(groups, offset, 7);
        let spill = request
            .simple_aggregate
            .as_ref()
            .unwrap()
            .spill
            .as_ref()
            .unwrap();
        recovery::recover_operator(
            concat!(module_path!(), "::weighted_count_recovery_process_child"),
            &fixture.workspace(),
            &fixture.path(),
            case,
            |directory| spill.cleanup_abandoned_weighted_count(directory),
        );
        let report = execute_vortex_local_primitive_with_policy(&request, policy).unwrap();
        let output = summary(&report);
        assert_eq!(output["values"], fixture.expected(groups, offset, 7, 0));
        let evidence = report
            .state_budget
            .native_weighted_count_spill
            .as_ref()
            .unwrap();
        assert!(evidence.runs_written >= 2);
        assert_eq!(evidence.runs_written, evidence.runs_validated);
        assert!(evidence.peak_reserved_bytes <= evidence.memory_bytes);
        assert!(evidence.peak_disk_bytes <= evidence.quota_bytes);
        assert!(evidence.owned_cleanup_completed && report.write_io && report.spill_io_performed);
        assert!(!report.arrow_converted && !report.fallback_execution_allowed);
        assert!(local_primitive_native_io_safe(&request, &report));
        if case == "count_single" {
            assert!(
                output["weighted_count_spill"]["worker_jobs_completed"]
                    .as_u64()
                    .unwrap()
                    > 0
            );
            assert_eq!(
                output["weighted_count_spill"]["fitted_partition_selection"],
                false
            );
        }
        fixture.empty();
    }
}
