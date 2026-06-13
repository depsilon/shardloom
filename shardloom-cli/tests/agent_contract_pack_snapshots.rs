use std::process::Command;

fn run_agent_contract_pack_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["agent-contract-pack", "--format", "json"])
        .output()
        .expect("agent-contract-pack command runs");

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
fn agent_contract_pack_json_exposes_machine_contract_inventory() {
    let output = run_agent_contract_pack_json();

    assert!(output.contains("\"command\":\"agent-contract-pack\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "agent_contract_pack")));
    assert!(output.contains(&field("schema_version", "shardloom.agent_contract_pack.v1")));
    assert!(output.contains(&field("pack_id", "agent.contract_pack.default")));
    assert!(output.contains(&field(
        "surface_order",
        "output_envelope,diagnostics,capabilities,feature_footprint,effect_budget,doctor,support_bundle,explain_estimate,plan_portability,native_io_envelope,execution_certificate,benchmark_evidence,world_class_sufficiency,security_governance"
    )));
    assert!(output.contains(&field(
        "recommended_sequence",
        "feature-footprint --format json -> effect-budget-plan --format json -> doctor --format json -> support-bundle --format json -> capabilities certification --format json -> world-class-sufficiency-plan --format json -> benchmark-plan --format json -> benchmark-claim-evidence-plan --format json"
    )));
    assert!(output.contains("benchmark-claim-evidence-plan"));
}

#[test]
fn agent_contract_pack_json_preserves_safe_agent_defaults() {
    let output = run_agent_contract_pack_json();

    assert!(output.contains(&field("surface_count", "14")));
    assert!(output.contains(&field("available_surface_count", "14")));
    assert!(output.contains(&field("side_effect_free_surface_count", "14")));
    assert!(output.contains(&field("fallback_allowed_surface_count", "0")));
    assert!(output.contains(&field("deterministic_json_required", "true")));
    assert!(output.contains(&field("text_is_authoritative", "false")));
    assert!(output.contains(&field("no_probe_default", "true")));
    assert!(output.contains(&field("external_effects_default_denied", "true")));
    assert!(output.contains(&field("destructive_effects_default_denied", "true")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
}
