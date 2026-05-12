use std::process::Command;

fn run_cg8_runtime_promotion_gate_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["cg8-runtime-promotion-gate", "--format", "json"])
        .output()
        .expect("CG-8 runtime promotion gate command runs");

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
fn cg8_runtime_promotion_gate_json_exposes_runtime_surfaces() {
    let output = run_cg8_runtime_promotion_gate_json();

    assert!(output.contains("\"command\":\"cg8-runtime-promotion-gate\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "cg8_runtime_promotion_gate")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.dynamic_runtime_promotion_gate.v1"
    )));
    assert!(output.contains(&field("report_id", "cg8.dynamic_runtime_promotion_gate")));
    assert!(output.contains(&field("surface_count", "8")));
    assert!(output.contains(&field("existing_limited_surface_count", "2")));
    assert!(output.contains(&field("blocked_surface_count", "6")));
    assert!(output.contains(&field("runtime_ready_surface_count", "0")));
    assert!(output.contains(&field(
        "surface_order",
        "dynamic_sizing_feedback_application,bounded_parallel_encoded_read_runtime,source_backed_reader_split_parallelism,scheduler_requeue_policy,bounded_queue_backpressure_runtime,memory_spill_reservation_runtime,object_store_request_budget_runtime,benchmark_certificate_closeout"
    )));
    assert!(output.contains(&field(
        "cg8_runtime_surface_0_name",
        "dynamic_sizing_feedback_application"
    )));
    assert!(output.contains(&field(
        "cg8_runtime_surface_1_name",
        "bounded_parallel_encoded_read_runtime"
    )));
    assert!(output.contains(&field(
        "cg8_runtime_surface_7_name",
        "benchmark_certificate_closeout"
    )));
}

#[test]
fn cg8_runtime_promotion_gate_json_blocks_runtime_and_claims() {
    let output = run_cg8_runtime_promotion_gate_json();

    assert!(output.contains(&field(
        "existing_local_streaming_scan_evidence_present",
        "true"
    )));
    assert!(output.contains(&field(
        "existing_local_bounded_metadata_noop_evidence_present",
        "true"
    )));
    assert!(output.contains(&field(
        "existing_local_filter_project_bounded_scan_evidence_present",
        "true"
    )));
    assert!(output.contains(&field("runtime_promotions_blocked", "true")));
    assert!(output.contains(&field("claim_blocked", "true")));
    assert!(output.contains(&field("dynamic_feedback_application_allowed", "false")));
    assert!(output.contains(&field("bounded_parallel_encoded_read_allowed", "false")));
    assert!(output.contains(&field("source_backed_parallel_reader_allowed", "false")));
    assert!(output.contains(&field("scheduler_requeue_allowed", "false")));
    assert!(output.contains(&field("bounded_backpressure_runtime_allowed", "false")));
    assert!(output.contains(&field("memory_spill_reservation_runtime_allowed", "false")));
    assert!(output.contains(&field(
        "object_store_request_budget_runtime_allowed",
        "false"
    )));
    assert!(output.contains(&field("runtime_policy_mutation_allowed", "false")));
    assert!(output.contains(&field("large_workload_claim_allowed", "false")));
    assert!(output.contains(&field("runtime_metrics_required", "true")));
    assert!(output.contains(&field("target_task_policy_required", "true")));
    assert!(output.contains(&field("scheduler_queue_policy_required", "true")));
    assert!(output.contains(&field("memory_reservation_evidence_required", "true")));
    assert!(output.contains(&field("spill_policy_evidence_required", "true")));
    assert!(output.contains(&field("backpressure_evidence_required", "true")));
    assert!(output.contains(&field("cancellation_retry_evidence_required", "true")));
    assert!(output.contains(&field("execution_certificate_required", "true")));
    assert!(output.contains(&field("native_io_certificate_required", "true")));
    assert!(output.contains(&field("benchmark_evidence_required", "true")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("tasks_executed", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("spill_io_performed", "false")));
    assert!(output.contains(&field("feedback_applied", "false")));
    assert!(output.contains(&field("policy_mutated", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}
