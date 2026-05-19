use std::process::Command;

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

fn run_json(args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("command runs");
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

#[test]
fn extension_registry_json_exposes_manifest_effect_matrix() {
    let output = run_json(&["extension-registry", "--format", "json"]);
    assert!(output.contains("\"command\":\"extension-registry\""));
    assert!(output.contains(&field(
        "extension_manifest_effect_matrix_schema_version",
        "shardloom.extension_manifest_effect_capability_matrix.v1"
    )));
    assert!(output.contains(&field(
        "extension_manifest_effect_matrix_id",
        "gar-0011-a.extension_manifest_external_effect_capability_matrix"
    )));
    assert!(output.contains(&field(
        "extension_manifest_effect_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&field(
        "extension_manifest_effect_all_runtime_blocked",
        "true"
    )));
    assert!(output.contains(&field(
        "extension_manifest_effect_all_external_effects_blocked",
        "true"
    )));
    assert!(output.contains(&field(
        "extension_manifest_effect_runtime_execution",
        "false"
    )));
    assert!(output.contains(&field(
        "extension_manifest_effect_extension_code_executed",
        "false"
    )));
    assert!(output.contains(&field("extension_manifest_effect_dynamic_loading", "false")));
    assert!(output.contains(&field(
        "extension_manifest_effect_external_effect_executed",
        "false"
    )));
    assert!(output.contains(&field(
        "extension_manifest_effect_fallback_attempted",
        "false"
    )));
    assert!(output.contains(&field(
        "extension_manifest_effect_external_engine_invoked",
        "false"
    )));
}

#[test]
fn extension_manifest_effect_rows_keep_effectful_paths_blocked() {
    let output = run_json(&["extension-registry", "--format", "json"]);
    for row in [
        "rust_udf_extension",
        "python_udf_extension",
        "object_store_provider_extension",
        "api_llm_effect_provider",
        "embedding_vector_provider",
    ] {
        assert!(output.contains(&field(
            &format!("extension_manifest_effect_row_{row}_support_status"),
            "blocked"
        )));
        assert!(output.contains(&field(
            &format!("extension_manifest_effect_row_{row}_runtime_execution"),
            "false"
        )));
        assert!(output.contains(&field(
            &format!("extension_manifest_effect_row_{row}_extension_code_executed"),
            "false"
        )));
        assert!(output.contains(&field(
            &format!("extension_manifest_effect_row_{row}_external_effect_executed"),
            "false"
        )));
        assert!(output.contains(&field(
            &format!("extension_manifest_effect_row_{row}_fallback_attempted"),
            "false"
        )));
        assert!(output.contains(&field(
            &format!("extension_manifest_effect_row_{row}_external_engine_invoked"),
            "false"
        )));
    }
}
