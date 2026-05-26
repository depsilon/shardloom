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

#[test]
fn effect_budget_json_exposes_udf_external_effect_blocker_matrix() {
    let output = run_effect_budget_plan_json();

    assert!(output.contains(&field(
        "external_effect_blocker_matrix_schema_version",
        "shardloom.external_effect_blocker_matrix.v1"
    )));
    assert!(output.contains(&field(
        "external_effect_blocker_matrix_id",
        "gar-0032-c.udf_external_effect_blockers"
    )));
    assert!(output.contains(&field(
        "external_effect_blocker_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&field(
        "external_effect_blocker_row_order",
        "sql_udf,rust_udf,wasm_udf,python_udf,external_service_udf,api_call,llm_call,embedding_generation,vector_search,plugin_execution,media_extraction,network_egress"
    )));
    assert!(output.contains(&field(
        "external_effect_blocker_all_effects_blocked",
        "true"
    )));
    assert!(output.contains(&field("external_effect_blocker_runtime_execution", "false")));
    assert!(output.contains(&field(
        "external_effect_blocker_credential_resolution_performed",
        "false"
    )));
    assert!(output.contains(&field(
        "external_effect_blocker_network_probe_performed",
        "false"
    )));
    assert!(output.contains(&field(
        "external_effect_blocker_external_engine_invoked",
        "false"
    )));
    for (row, credential, network, sandbox, model) in [
        ("python_udf", "false", "false", "true", "false"),
        ("external_service_udf", "true", "true", "true", "false"),
        ("api_call", "true", "true", "false", "false"),
        ("llm_call", "true", "true", "false", "true"),
        ("embedding_generation", "true", "true", "false", "true"),
        ("network_egress", "true", "true", "false", "false"),
    ] {
        let prefix = format!("external_effect_blocker_row_{row}");
        assert!(output.contains(&field(&format!("{prefix}_support_status"), "blocked")));
        assert!(output.contains(&field(
            &format!("{prefix}_permission_status"),
            "policy_required"
        )));
        assert!(output.contains(&field(
            &format!("{prefix}_effect_status"),
            "denied_by_default"
        )));
        assert!(output.contains(&field(&format!("{prefix}_credential_required"), credential)));
        assert!(output.contains(&field(&format!("{prefix}_network_required"), network)));
        assert!(output.contains(&field(&format!("{prefix}_sandbox_required"), sandbox)));
        assert!(output.contains(&field(&format!("{prefix}_model_or_embedding_call"), model)));
        assert!(output.contains(&field(&format!("{prefix}_runtime_execution"), "false")));
        assert!(output.contains(&field(&format!("{prefix}_effect_executed"), "false")));
    }
}

#[test]
fn effect_budget_json_exposes_effectful_operation_admission_matrix() {
    let output = run_effect_budget_plan_json();

    assert!(output.contains(&field(
        "effectful_operation_admission_matrix_schema_version",
        "shardloom.effectful_operation_admission_matrix.v1"
    )));
    assert!(output.contains(&field(
        "effectful_operation_admission_claim_gate_status",
        "fixture_smoke_only"
    )));
    assert!(output.contains(&field(
        "effectful_operation_admission_admitted_local_fixture_count",
        "2"
    )));
    assert!(output.contains(&field(
        "effectful_operation_admission_metadata_only_count",
        "1"
    )));
    assert!(output.contains(&field(
        "effectful_operation_admission_all_external_and_sandboxed_paths_blocked",
        "true"
    )));
    assert!(output.contains(&field(
        "effectful_operation_admission_row_local_sqlite_import_export_support_status",
        "fixture_smoke_supported"
    )));
    assert!(output.contains(&field(
        "effectful_operation_admission_row_typed_extension_manifest_inspection_support_status",
        "metadata_only_supported"
    )));
    assert!(output.contains(&field(
        "effectful_operation_admission_row_deterministic_scalar_udf_fixture_support_status",
        "fixture_smoke_supported"
    )));
    assert!(output.contains(&field(
        "effectful_operation_admission_row_network_database_connectors_support_status",
        "blocked"
    )));
    assert!(output.contains(&field(
        "effectful_operation_admission_network_probe_performed",
        "false"
    )));
    assert!(output.contains(&field(
        "effectful_operation_admission_external_engine_invoked",
        "false"
    )));
}
