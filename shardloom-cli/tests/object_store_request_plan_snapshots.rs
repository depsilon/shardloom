use std::process::Command;

fn run_object_store_request_plan_json(scenario: &str) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["object-store-request-plan", scenario, "--format", "json"])
        .output()
        .expect("object-store-request-plan command runs");

    (
        output.status.success(),
        String::from_utf8(output.stdout).expect("stdout is utf8"),
        String::from_utf8(output.stderr).expect("stderr is utf8"),
    )
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

#[test]
fn object_store_request_json_exposes_aggregate_ready_path() {
    let (success, output, stderr) = run_object_store_request_plan_json("ready");

    assert!(success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"command\":\"object-store-request-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "object_store_request_plan")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.object_store_request_planner.v1"
    )));
    assert!(output.contains(&field(
        "report_id",
        "cg10.object_store_request_planner.aggregate"
    )));
    assert!(output.contains(&field("object_store_request_status", "planned")));
    assert!(output.contains(&field(
        "surface_order",
        "range_planning,request_coalescing,distributed_scheduling,checkpoint_retry,commit_protocol"
    )));
    assert!(output.contains(&field("planned_surface_count", "5")));
    assert!(output.contains(&field("blocked_surface_count", "0")));
    assert!(output.contains(&field("range_status", "planned")));
    assert!(output.contains(&field(
        "byte_range_provider_gate_schema_version",
        "shardloom.object_store_byte_range_provider_gate.v1"
    )));
    assert!(output.contains(&field(
        "byte_range_provider_gate_report_id",
        "gar0008a.object_store_byte_range_provider_gate"
    )));
    assert!(output.contains(&field(
        "byte_range_provider_gate_status",
        "blocked_until_certified"
    )));
    assert!(output.contains(&field(
        "byte_range_provider_gate_scope",
        "s3_gcs_adls_byte_range_reads"
    )));
    assert!(output.contains(&field(
        "byte_range_provider_gate_range_planning_evidence_present",
        "true"
    )));
    assert!(output.contains(&field("scheduling_status", "planned")));
    assert!(output.contains(&field("checkpoint_retry_status", "ready")));
    assert!(output.contains(&field("commit_status", "ready")));
}

#[test]
fn object_store_request_json_preserves_report_only_runtime_boundaries() {
    let (success, output, stderr) = run_object_store_request_plan_json("ready");

    assert!(success, "stdout={output} stderr={stderr}");
    assert!(output.contains(&field("coordinator_started", "false")));
    assert!(output.contains(&field("worker_started", "false")));
    assert!(output.contains(&field("task_execution_allowed", "false")));
    assert!(output.contains(&field("retry_execution_allowed", "false")));
    assert!(output.contains(&field("checkpoint_write_allowed", "false")));
    assert!(output.contains(&field("cleanup_execution_allowed", "false")));
    assert!(output.contains(&field("commit_execution_allowed", "false")));
    for key in [
        "byte_range_provider_gate_range_read_execution_allowed",
        "byte_range_provider_gate_full_file_read_allowed",
        "byte_range_provider_gate_credential_resolution_allowed",
        "byte_range_provider_gate_credentials_resolved",
        "byte_range_provider_gate_retry_execution_allowed",
        "byte_range_provider_gate_provider_probe",
        "byte_range_provider_gate_network_probe",
        "byte_range_provider_gate_data_read",
        "byte_range_provider_gate_object_store_io",
        "byte_range_provider_gate_write_io",
        "byte_range_provider_gate_fallback_attempted",
        "byte_range_provider_gate_fallback_execution_allowed",
        "byte_range_provider_gate_external_engine_invoked",
    ] {
        assert!(
            output.contains(&field(key, "false")),
            "missing false provider-gate field {key}"
        );
    }
    for key in [
        "byte_range_provider_gate_request_budget_policy_required",
        "byte_range_provider_gate_provider_capability_policy_required",
        "byte_range_provider_gate_credential_policy_required",
        "byte_range_provider_gate_retry_policy_required",
        "byte_range_provider_gate_idempotency_key_required",
        "byte_range_provider_gate_execution_certificate_required",
        "byte_range_provider_gate_native_io_certificate_required",
        "byte_range_provider_gate_benchmark_evidence_required",
        "byte_range_provider_gate_side_effect_free",
    ] {
        assert!(
            output.contains(&field(key, "true")),
            "missing true provider-gate field {key}"
        );
    }
    assert!(output.contains(&field(
        "byte_range_provider_gate_required_evidence",
        "provider_capability_policy,credential_effect_policy,request_budget_policy,retry_policy,idempotency_key_contract,execution_certificate,native_io_certificate,benchmark_evidence"
    )));
    assert!(output.contains(&field(
        "byte_range_provider_gate_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&field(
        "vortex_object_store_io_gate_ref",
        "gar0005b.vortex_object_store_io.gate"
    )));
    assert!(output.contains(&field(
        "vortex_object_store_io_gate_support_status",
        "unsupported"
    )));
    assert!(output.contains(&field(
        "vortex_object_store_io_gate_claim_gate_status",
        "not_claim_grade"
    )));
    for key in [
        "vortex_object_store_io_gate_object_store_read_execution_allowed",
        "vortex_object_store_io_gate_object_store_write_execution_allowed",
        "vortex_object_store_io_gate_upstream_vortex_write_allowed",
        "vortex_object_store_io_gate_credential_resolution_allowed",
        "vortex_object_store_io_gate_object_store_io",
        "vortex_object_store_io_gate_write_io",
        "vortex_object_store_io_gate_external_engine_invoked",
        "vortex_object_store_io_gate_fallback_attempted",
    ] {
        assert!(
            output.contains(&field(key, "false")),
            "missing false field {key}"
        );
    }
    assert!(output.contains(&field(
        "vortex_object_store_io_gate_side_effect_free",
        "true"
    )));
    assert!(output.contains(&field("diagnostic_count", "7")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
    assert!(output.contains(&field("plan_only", "true")));
}

#[test]
fn object_store_request_json_surfaces_blocked_range_status() {
    let (success, output, stderr) = run_object_store_request_plan_json("missing-ranges");

    assert!(!success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains(&field(
        "object_store_request_status",
        "blocked_by_range_planning"
    )));
    assert!(output.contains(&field("range_status", "blocked_missing_byte_ranges")));
    assert!(output.contains(&field(
        "byte_range_provider_gate_range_planning_evidence_present",
        "false"
    )));
    assert!(output.contains(&field("requires_byte_ranges", "true")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
}
