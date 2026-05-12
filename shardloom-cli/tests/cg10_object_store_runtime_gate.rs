use std::process::Command;

fn run_cg10_object_store_runtime_gate_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["cg10-object-store-runtime-gate", "--format", "json"])
        .output()
        .expect("cg10-object-store-runtime-gate command runs");

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
fn cg10_object_store_runtime_gate_exposes_surface_order_and_existing_evidence() {
    let output = run_cg10_object_store_runtime_gate_json();

    assert!(output.contains("\"command\":\"cg10-object-store-runtime-gate\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "object_store_runtime_promotion_gate")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.object_store_runtime_promotion_gate.v1"
    )));
    assert!(output.contains(&field(
        "report_id",
        "cg10.object_store_runtime_promotion_gate"
    )));
    assert!(output.contains(&field("surface_count", "12")));
    assert!(output.contains(&field("existing_evidence_surface_count", "1")));
    assert!(output.contains(&field("blocked_surface_count", "11")));
    assert!(output.contains(&field(
        "surface_order",
        "request_planner_aggregate,range_read_execution,request_coalescing_runtime,distributed_coordinator_startup,distributed_worker_startup,distributed_task_execution,checkpoint_write_execution,retry_execution,cleanup_execution,object_store_commit_execution,provider_credential_runtime,benchmark_certificate_closeout"
    )));
    assert!(output.contains(&field("existing_request_planner_evidence_present", "true")));
    assert!(output.contains(&field("existing_range_planning_evidence_present", "true")));
    assert!(output.contains(&field("existing_coalescing_evidence_present", "true")));
    assert!(output.contains(&field(
        "existing_distributed_scheduling_evidence_present",
        "true"
    )));
    assert!(output.contains(&field("existing_checkpoint_retry_evidence_present", "true")));
    assert!(output.contains(&field("existing_commit_protocol_evidence_present", "true")));
}

#[test]
fn cg10_object_store_runtime_gate_blocks_execution_io_credentials_and_claims() {
    let output = run_cg10_object_store_runtime_gate_json();

    for key in [
        "range_read_execution_allowed",
        "full_file_read_allowed",
        "request_coalescing_runtime_allowed",
        "coordinator_start_allowed",
        "worker_start_allowed",
        "task_execution_allowed",
        "retry_execution_allowed",
        "checkpoint_write_allowed",
        "cleanup_execution_allowed",
        "commit_execution_allowed",
        "credential_resolution_allowed",
        "object_store_io_allowed",
        "data_read_allowed",
        "write_io_allowed",
        "object_store_runtime_claim_allowed",
        "distributed_runtime_claim_allowed",
        "fallback_attempted",
        "fallback_execution_allowed",
    ] {
        assert!(
            output.contains(&field(key, "false")),
            "missing false field {key}"
        );
    }

    for key in [
        "range_planning_evidence_required",
        "request_budget_policy_required",
        "provider_capability_policy_required",
        "credential_effect_policy_required",
        "scheduler_policy_required",
        "worker_identity_required",
        "checkpoint_plan_required",
        "retry_policy_required",
        "idempotency_keys_required",
        "attempt_records_required",
        "cleanup_policy_required",
        "atomic_commit_evidence_required",
        "execution_certificate_required",
        "native_io_certificate_required",
        "benchmark_evidence_required",
        "runtime_promotions_blocked",
        "claim_blocked",
        "side_effect_free",
        "plan_only",
    ] {
        assert!(
            output.contains(&field(key, "true")),
            "missing true field {key}"
        );
    }
    assert!(output.contains(&field("execution", "not_performed")));
}
