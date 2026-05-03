use shardloom_core::{DatasetUri, UniversalInputSource};
use shardloom_exec::{AdaptiveSizingPolicy, MemoryBudget};
use shardloom_vortex::{
    build_vortex_runtime_task_graph, evaluate_vortex_execution_readiness,
    plan_native_vortex_universal_input, plan_vortex_memory_safety,
    plan_vortex_read_from_universal_input, plan_vortex_scheduler_queue,
    size_vortex_runtime_task_graph,
};

fn fixture_vortex_uri() -> DatasetUri {
    DatasetUri::new("file://tmp/shardloom-fixture.vortex").expect("valid .vortex dataset uri")
}

fn fixture_non_vortex_uri() -> DatasetUri {
    DatasetUri::new("file://tmp/shardloom-fixture.parquet").expect("valid non-vortex dataset uri")
}

#[test]
fn vortex_metadata_only_execution_spike_remains_side_effect_free() {
    let source =
        UniversalInputSource::from_dataset_uri(fixture_vortex_uri()).expect("source from uri");

    let input_plan = plan_native_vortex_universal_input(source).expect("input bridge plan");
    assert!(!input_plan.fallback_execution_allowed);
    assert!(!input_plan.data_read);
    assert!(!input_plan.data_materialized);
    assert!(!input_plan.object_store_io);
    assert!(!input_plan.write_io);
    assert!(!input_plan.external_effects_executed);

    let read_report =
        plan_vortex_read_from_universal_input(input_plan.clone()).expect("read planning report");
    assert!(!read_report.fallback_execution_allowed);
    assert!(!read_report.data_executed);
    assert!(!read_report.data_read);
    assert!(!read_report.data_materialized);
    assert!(!read_report.object_store_io);
    assert!(!read_report.write_io);
    assert!(!read_report.external_effects_executed);

    let runtime_report = build_vortex_runtime_task_graph(read_report).expect("runtime task graph");
    assert!(!runtime_report.fallback_execution_allowed);
    assert!(!runtime_report.data_executed);
    assert!(!runtime_report.data_read);
    assert!(!runtime_report.data_materialized);
    assert!(!runtime_report.object_store_io);
    assert!(!runtime_report.write_io);
    assert!(!runtime_report.external_effects_executed);

    let sizing =
        size_vortex_runtime_task_graph(runtime_report, AdaptiveSizingPolicy::default_local())
            .expect("adaptive sizing report");
    assert!(!sizing.fallback_execution_allowed);
    assert!(!sizing.data_read);
    assert!(!sizing.data_materialized);
    assert!(!sizing.object_store_io);
    assert!(!sizing.write_io);
    assert!(!sizing.external_effects_executed);

    let memory_budget = MemoryBudget::from_gib(1).expect("valid memory budget");
    let memory_report = plan_vortex_memory_safety(sizing, memory_budget).expect("memory plan");
    assert!(
        !memory_report
            .execution_policy_flags
            .fallback_execution_allowed
    );
    assert!(!memory_report.io_flags.data_read);
    assert!(!memory_report.io_flags.data_materialized);
    assert!(!memory_report.effect_flags.object_store_io);
    assert!(!memory_report.effect_flags.write_io);
    assert!(!memory_report.effect_flags.spill_io_performed);
    assert!(
        !memory_report
            .execution_policy_flags
            .external_effects_executed
    );

    let scheduler_report = plan_vortex_scheduler_queue(memory_report, 2).expect("schedule plan");
    assert!(!scheduler_report.fallback_execution_allowed);
    assert!(!scheduler_report.tasks_executed);
    assert!(!scheduler_report.data_read);
    assert!(!scheduler_report.data_materialized);
    assert!(!scheduler_report.object_store_io);
    assert!(!scheduler_report.write_io);
    assert!(!scheduler_report.spill_io_performed);
    assert!(!scheduler_report.external_effects_executed);

    let readiness =
        evaluate_vortex_execution_readiness(scheduler_report).expect("execution readiness");
    assert!(!readiness.fallback_execution_allowed);
    assert!(!readiness.tasks_executed);
    assert!(!readiness.data_executed);
    assert!(!readiness.data_read);
    assert!(!readiness.data_materialized);
    assert!(!readiness.object_store_io);
    assert!(!readiness.write_io);
    assert!(!readiness.spill_io_performed);
    assert!(!readiness.external_effects_executed);
    assert!(readiness.is_side_effect_free());
    assert!(readiness.dry_run_contract.is_side_effect_free());
    assert!(!readiness.dry_run_contract.fallback_execution_allowed);

    if readiness.ready_for_future_execution {
        assert_eq!(readiness.blocking_gate_count, 0);
    }
}

#[test]
fn vortex_execution_spike_rejects_non_vortex_input_without_fallback() {
    let source = UniversalInputSource::from_dataset_uri(fixture_non_vortex_uri())
        .expect("source from non-vortex uri");

    let input_plan = plan_native_vortex_universal_input(source).expect("input bridge plan");
    assert!(!input_plan.fallback_execution_allowed);
    assert!(!input_plan.data_read);
    assert!(!input_plan.data_materialized);
    assert!(!input_plan.object_store_io);
    assert!(!input_plan.write_io);
    assert!(!input_plan.external_effects_executed);
    assert!(input_plan.has_errors());
    assert!(
        input_plan
            .diagnostics
            .iter()
            .any(|d| d.severity.as_str() == "error")
    );
    assert!(input_plan.diagnostics.iter().all(|d| !d.fallback.attempted));
}
