use std::process::Command;

fn run_security_governance_evidence_gate_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["security-governance-evidence-gate", "--format", "json"])
        .output()
        .expect("security governance evidence gate command runs");

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
fn security_governance_evidence_gate_json_exposes_required_areas() {
    let output = run_security_governance_evidence_gate_json();

    assert!(output.contains("\"command\":\"security-governance-evidence-gate\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "security_governance_evidence_gate")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.security_governance_evidence_gate.v1"
    )));
    assert!(output.contains(&field(
        "report_id",
        "cross_cutting.security_governance_evidence_gate"
    )));
    assert!(output.contains(&field("evidence_area_count", "8")));
    assert!(output.contains(&field("report_only_area_count", "8")));
    assert!(output.contains(&field(
        "area_order",
        "credential_reference,permission_boundary,redaction_policy,audit_trail,external_effect,destructive_operation,data_egress,agent_policy"
    )));
    assert!(output.contains(&field(
        "security_evidence_area_0_name",
        "credential_reference"
    )));
    assert!(output.contains(&field("security_evidence_area_7_name", "agent_policy")));
}

#[test]
fn security_governance_evidence_gate_json_blocks_effects_by_default() {
    let output = run_security_governance_evidence_gate_json();

    assert!(output.contains(&field("effectful_claim_allowed_count", "0")));
    assert!(output.contains(&field("all_evidence_surfaces_present", "true")));
    assert!(output.contains(&field("effectful_features_default_denied", "true")));
    assert!(output.contains(&field("dry_run_required_without_policy", "true")));
    assert!(output.contains(&field("credential_references_only", "true")));
    assert!(output.contains(&field("credentials_resolved", "false")));
    assert!(output.contains(&field("secrets_loaded", "false")));
    assert!(output.contains(&field("external_effects_executed", "false")));
    assert!(output.contains(&field("external_effect_claims_allowed", "false")));
    assert!(output.contains(&field("destructive_operations_allowed", "false")));
    assert!(output.contains(&field("data_egress_allowed", "false")));
    assert!(output.contains(&field("object_store_claims_blocked", "true")));
    assert!(output.contains(&field("api_server_claims_blocked", "true")));
    assert!(output.contains(&field("llm_media_udf_claims_blocked", "true")));
    assert!(output.contains(&field("agent_execute_write_cancel_allowed", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
    assert!(output.contains(&field("claims_blocked_by_default", "true")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}
