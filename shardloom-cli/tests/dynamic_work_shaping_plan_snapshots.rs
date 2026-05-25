use std::process::Command;

fn run_dynamic_work_shaping_json(profile: Option<&str>) -> String {
    let mut args = vec!["dynamic-work-shaping-plan"];
    if let Some(profile) = profile {
        args.push(profile);
    }
    args.extend(["--format", "json"]);
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("dynamic work shaping command runs");

    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout).expect("stdout is utf8")
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

#[test]
fn dynamic_work_shaping_json_exposes_aggregate_surfaces() {
    let output = run_dynamic_work_shaping_json(None);

    assert!(output.contains("\"command\":\"dynamic-work-shaping-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "dynamic_work_shaping_plan")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.dynamic_work_shaping.v1"
    )));
    assert!(output.contains(&field("report_id", "cg8.dynamic_work_shaping.aggregate")));
    assert!(output.contains(&field("profile", "balanced")));
    assert!(output.contains(&field(
        "dynamic_work_shaping_status",
        "needs_runtime_integration"
    )));
    assert!(output.contains(&field(
        "surface_order",
        "adaptive_sizing_policy,feedback_signals,target_task_policy,backpressure_policy,bounded_memory_policy,scheduler_queue_policy,runtime_application_loop,benchmark_evidence,no_fallback_policy"
    )));
    assert!(output.contains(&field("planned_surface_count", "7")));
    assert!(output.contains(&field("blocked_surface_count", "2")));
    assert!(output.contains(&field(
        "blocked_surface_order",
        "runtime_application_loop,benchmark_evidence"
    )));
    assert!(output.contains(&field(
        "work_shaping_workload_kind",
        "repeated_independent_shard_tasks"
    )));
    assert!(output.contains(&field(
        "automatic_work_shaping_decision",
        "keep_current_shape"
    )));
    assert!(output.contains(&field("automatic_work_shaping_plan_ready", "true")));
}

#[test]
fn dynamic_work_shaping_json_reports_feedback_and_backpressure() {
    let output = run_dynamic_work_shaping_json(Some("memory-pressure"));

    assert!(output.contains(&field("profile", "memory-pressure")));
    assert!(output.contains(&field("feedback_status", "target_reduced")));
    assert!(output.contains(&field("feedback_mode", "target_adjustment")));
    assert!(output.contains(&field("signal_count", "1")));
    assert!(output.contains(&field("reduce_signal_count", "1")));
    assert!(output.contains(&field("increase_signal_count", "0")));
    assert!(output.contains(&field("current_target_task_bytes", "2147483648")));
    assert!(output.contains(&field("recommended_target_task_bytes", "1073741824")));
    assert!(output.contains(&field("target_task_bytes_changed", "true")));
    assert!(output.contains(&field("backpressure_status", "bounded")));
    assert!(output.contains(&field("backpressure_mode", "bounded_streaming")));
    assert!(output.contains(&field("bounded_backpressure", "true")));
    assert!(output.contains(&field("max_parallelism", "4")));
    assert!(output.contains(&field("max_in_flight_chunks", "4")));
    assert!(output.contains(&field("max_buffered_bytes", "8589934592")));
    assert!(output.contains(&field("estimated_chunk_bytes", "268435456")));
}

#[test]
fn dynamic_work_shaping_json_keeps_runtime_application_disabled() {
    let output = run_dynamic_work_shaping_json(Some("object-store-throttled"));

    assert!(output.contains(&field("runtime_feedback_loop_ready", "false")));
    assert!(output.contains(&field("policy_application_ready", "false")));
    assert!(output.contains(&field("benchmark_evidence_ready", "false")));
    assert!(output.contains(&field("streams_executed", "false")));
    assert!(output.contains(&field("tasks_executed", "false")));
    assert!(output.contains(&field("feedback_applied", "false")));
    assert!(output.contains(&field("policy_mutated", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("spill_io_performed", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
}

#[test]
fn dynamic_work_shaping_json_reports_repeated_independent_shard_coalescing() {
    let output = run_dynamic_work_shaping_json(Some("repeated-independent-shards"));

    assert!(output.contains(&field("profile", "repeated-independent-shards")));
    assert!(output.contains(&field(
        "work_shaping_workload_kind",
        "repeated_independent_shard_tasks"
    )));
    assert!(output.contains(&field("input_independent_shard_task_count", "32")));
    assert!(output.contains(&field("current_work_shaped_task_count", "2")));
    assert!(output.contains(&field("recommended_work_shaped_task_count", "1")));
    assert!(output.contains(&field(
        "automatic_work_shaping_decision",
        "coalesce_small_shards"
    )));
    assert!(output.contains(&field("automatic_work_shaping_plan_ready", "true")));
    assert!(output.contains(&field("automatic_work_shaping_applied", "false")));
    assert!(output.contains(&field("automatic_work_shaping_claim_allowed", "false")));
    assert!(output.contains(&field("tasks_executed", "false")));
    assert!(output.contains(&field("policy_mutated", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}
