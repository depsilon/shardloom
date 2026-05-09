use std::process::Command;

fn run_effect_budget_plan_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["effect-budget-plan", "--format", "json"])
        .output()
        .expect("effect-budget-plan command runs");

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
fn effect_budget_json_exposes_cross_cutting_contract() {
    let output = run_effect_budget_plan_json();

    assert!(output.contains("\"command\":\"effect-budget-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "effect_budget_plan")));
    assert!(output.contains(&field("schema_version", "shardloom.effect_budget.v1")));
    assert!(output.contains(&field("report_id", "cross_cutting.effect_budget")));
    assert!(output.contains(&field("budget_mode", "deny_external_effects_by_default")));
    assert!(output.contains(&field(
        "scope_order",
        "local_file_read,local_file_write,object_store_read,object_store_write,catalog_read,catalog_write,api_read,api_write,llm_call,embedding_generation,vector_search,python_udf,wasm_udf,external_service_udf,plugin_execution,media_extraction,network_egress"
    )));
}

#[test]
fn effect_budget_json_preserves_no_probe_no_fallback_defaults() {
    let output = run_effect_budget_plan_json();

    assert!(output.contains(&field("entry_count", "17")));
    assert!(output.contains(&field("denied_scope_count", "17")));
    assert!(output.contains(&field("approved_scope_count", "0")));
    assert!(output.contains(&field("external_effects_allowed", "false")));
    assert!(output.contains(&field("destructive_effects_allowed", "false")));
    assert!(output.contains(&field("network_egress_allowed", "false")));
    assert!(output.contains(&field("credentials_resolved", "false")));
    assert!(output.contains(&field("secrets_loaded", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("filesystem_probe", "false")));
    assert!(output.contains(&field("network_probe", "false")));
    assert!(output.contains(&field("catalog_probe", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}
