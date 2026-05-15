use std::process::Command;

fn run_fault_tolerance_promotion_gate_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["fault-tolerance-promotion-gate", "--format", "json"])
        .output()
        .expect("fault tolerance promotion gate command runs");

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
fn fault_tolerance_promotion_gate_json_exposes_required_areas() {
    let output = run_fault_tolerance_promotion_gate_json();

    assert!(output.contains("\"command\":\"fault-tolerance-promotion-gate\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "fault_tolerance_promotion_gate")));
    assert!(output.contains(&field("gar_id", "GAR-0017-A")));
    assert!(output.contains(&field("support_status", "report_only")));
    assert!(output.contains(&field("claim_gate_status", "not_claim_grade")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.fault_tolerance_promotion_gate.v1"
    )));
    assert!(output.contains(&field(
        "report_id",
        "rfc0017.fault_tolerance_promotion_gate"
    )));
    assert!(output.contains(&field("promotion_area_count", "6")));
    assert!(output.contains(&field("blocked_area_count", "6")));
    assert!(output.contains(&field("execution_ready_area_count", "0")));
    assert!(output.contains(&field(
        "area_order",
        "retry_execution,cancellation_propagation,cleanup_execution,ambiguous_commit_resolution,idempotency_keying,recovery_execution"
    )));
    assert!(output.contains(&field(
        "execution_gate_order",
        "request_validation,cancellation_signal,retry_allowed,checkpoint_write,cleanup_execution,commit_execution"
    )));
    assert!(output.contains(&field("execution_gate_blocker_count", "11")));
    assert!(output.contains(&field(
        "execution_gate_blocker_order",
        "request_validation_policy,cancellation_signal_policy,retry_policy,idempotency_key_contract,checkpoint_plan,cleanup_policy,commit_semantics,side_effect_boundary,execution_certificate,native_io_certificate,no_fallback_evidence"
    )));
    assert!(output.contains(&field(
        "fault_tolerance_promotion_area_0_name",
        "retry_execution"
    )));
    assert!(output.contains(&field(
        "fault_tolerance_promotion_area_5_name",
        "recovery_execution"
    )));
}

#[test]
fn fault_tolerance_promotion_gate_json_blocks_claims_and_effects() {
    let output = run_fault_tolerance_promotion_gate_json();

    assert!(output.contains(&field("side_effect_boundaries_certified", "false")));
    assert!(output.contains(&field("commit_semantics_certified", "false")));
    assert!(output.contains(&field("execution_certificate_required", "true")));
    assert!(output.contains(&field("native_io_certificate_required", "true")));
    assert!(output.contains(&field("cg4_output_commit_evidence_required", "true")));
    assert!(output.contains(&field("cg8_write_recovery_evidence_required", "true")));
    assert!(output.contains(&field("cg10_object_store_evidence_required", "true")));
    assert!(output.contains(&field(
        "cg16_execution_certificate_evidence_required",
        "true"
    )));
    assert!(output.contains(&field("cg22_engine_mode_evidence_required", "true")));
    assert!(output.contains(&field("request_validation_report_only", "true")));
    assert!(output.contains(&field("cancellation_signal_required", "true")));
    assert!(output.contains(&field("retry_policy_required", "true")));
    assert!(output.contains(&field("checkpoint_plan_required", "true")));
    assert!(output.contains(&field("cleanup_policy_required", "true")));
    assert!(output.contains(&field("commit_semantics_required", "true")));
    assert!(output.contains(&field(
        "request_validation_status",
        "report_only_no_execution"
    )));
    assert!(output.contains(&field("cancellation_signal_status", "report_only_blocked")));
    assert!(output.contains(&field("retry_allowed_status", "report_only_blocked")));
    assert!(output.contains(&field("checkpoint_write_status", "report_only_blocked")));
    assert!(output.contains(&field("cleanup_execution_status", "report_only_blocked")));
    assert!(output.contains(&field("commit_execution_status", "report_only_blocked")));
    assert!(output.contains(&field("retry_execution_allowed", "false")));
    assert!(output.contains(&field("cancellation_execution_allowed", "false")));
    assert!(output.contains(&field("cleanup_execution_allowed", "false")));
    assert!(output.contains(&field("checkpoint_write_allowed", "false")));
    assert!(output.contains(&field("commit_execution_allowed", "false")));
    assert!(output.contains(&field("ambiguous_commit_resolution_allowed", "false")));
    assert!(output.contains(&field("idempotent_write_claim_allowed", "false")));
    assert!(output.contains(&field("exactly_once_claim_allowed", "false")));
    assert!(output.contains(&field("resumability_claim_allowed", "false")));
    assert!(output.contains(&field("recovery_claim_allowed", "false")));
    assert!(output.contains(&field("execution_promotions_blocked", "true")));
    assert!(output.contains(&field(
        "exactly_once_resumability_recovery_claims_blocked",
        "true"
    )));
    assert!(output.contains(&field("request_validation_performed", "false")));
    assert!(output.contains(&field("cancellation_signal_consumed", "false")));
    assert!(output.contains(&field("retry_execution_performed", "false")));
    assert!(output.contains(&field("checkpoint_write_performed", "false")));
    assert!(output.contains(&field("cleanup_execution_performed", "false")));
    assert!(output.contains(&field("commit_execution_performed", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("output_dataset_write", "false")));
    assert!(output.contains(&field("external_effects_executed", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}
