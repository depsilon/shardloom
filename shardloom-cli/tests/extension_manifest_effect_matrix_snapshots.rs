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
    assert!(output.contains(&field(
        "plugin_abi_udf_sandbox_blocker_schema_version",
        "shardloom.plugin_abi_udf_sandbox_blocker.v1"
    )));
    assert!(output.contains(&field(
        "plugin_abi_udf_sandbox_blocker_id",
        "gar-0023-a.plugin_abi_udf_sandbox_blocker"
    )));
    assert!(output.contains(&field(
        "plugin_abi_udf_sandbox_blocker_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&field(
        "plugin_abi_udf_sandbox_blocker_all_plugin_runtime_blocked",
        "true"
    )));
    assert!(output.contains(&field(
        "plugin_abi_udf_sandbox_blocker_abi_loading_supported",
        "false"
    )));
    assert!(output.contains(&field(
        "plugin_abi_udf_sandbox_blocker_dynamic_loading_performed",
        "false"
    )));
    assert!(output.contains(&field(
        "plugin_abi_udf_sandbox_blocker_extension_code_executed",
        "false"
    )));
    assert!(output.contains(&field(
        "plugin_abi_udf_sandbox_blocker_udf_execution_performed",
        "false"
    )));
    assert!(output.contains(&field(
        "plugin_abi_udf_sandbox_blocker_external_engine_invoked",
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

#[test]
fn embedding_vector_local_fixture_smoke_exposes_no_effect_vector_evidence() {
    let output = run_json(&[
        "embedding-vector-local-fixture-smoke",
        "alpha;beta;gamma",
        "--query",
        "beta",
        "--format",
        "json",
    ]);
    assert!(output.contains("\"command\":\"embedding-vector-local-fixture-smoke\""));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.deterministic_embedding_vector_fixture.v1"
    )));
    assert!(output.contains(&field("fixture_id", "sl_fixture_hash_embedding_vector")));
    assert!(output.contains(&field("embedding_model_id", "sl_fixture_hash_embedding_v1")));
    assert!(output.contains(&field("vector_index_kind", "local_bruteforce_l2_fixture")));
    assert!(output.contains(&field("output_dtype", "fixed_size_list<int64,4>")));
    assert!(output.contains(&field("vector_dimension", "4")));
    assert!(output.contains(&field("vector_metric", "squared_l2")));
    assert!(output.contains(&field("nearest_index", "1")));
    assert!(output.contains(&field("nearest_text", "beta")));
    assert!(output.contains(&field("nearest_distance_squared", "0")));
    assert!(output.contains(&field("model_call_performed", "false")));
    assert!(output.contains(&field("credential_resolution_performed", "false")));
    assert!(output.contains(&field("network_probe_performed", "false")));
    assert!(output.contains(&field("extension_code_executed", "false")));
    assert!(output.contains(&field("external_effect_executed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("no_fallback_invariant_holds", "true")));
    assert!(output.contains(&field(
        "effectful_operation_admission_row_deterministic_embedding_vector_fixture_support_status",
        "fixture_smoke_supported"
    )));
}

#[test]
fn extension_and_udf_commands_expose_plugin_abi_udf_sandbox_blocker() {
    for args in [
        ["extension-registry", "--format", "json"].as_slice(),
        ["extension-inspect", "example.plugin", "--format", "json"].as_slice(),
        ["udf-runtime-plan", "python", "--format", "json"].as_slice(),
    ] {
        let output = run_json(args);
        assert!(output.contains(&field(
            "plugin_abi_udf_sandbox_blocker_schema_version",
            "shardloom.plugin_abi_udf_sandbox_blocker.v1"
        )));
        assert!(output.contains(&field(
            "plugin_abi_udf_sandbox_blocker_support_status",
            "report_only"
        )));
        assert!(output.contains(&field(
            "plugin_abi_udf_sandbox_blocker_all_plugin_runtime_blocked",
            "true"
        )));
        assert!(output.contains(&field(
            "plugin_abi_udf_sandbox_blocker_fallback_attempted",
            "false"
        )));
        assert!(output.contains(&field(
            "plugin_abi_udf_sandbox_blocker_external_engine_invoked",
            "false"
        )));
        for row in [
            "dynamic_library_loading",
            "rust_native_udf",
            "python_udf",
            "sandbox_evidence_binding",
        ] {
            assert!(output.contains(&field(
                &format!("plugin_abi_udf_sandbox_blocker_row_{row}_support_status"),
                "blocked"
            )));
            assert!(output.contains(&field(
                &format!("plugin_abi_udf_sandbox_blocker_row_{row}_runtime_execution"),
                "false"
            )));
            assert!(output.contains(&field(
                &format!("plugin_abi_udf_sandbox_blocker_row_{row}_extension_code_executed"),
                "false"
            )));
            assert!(output.contains(&field(
                &format!("plugin_abi_udf_sandbox_blocker_row_{row}_udf_execution_performed"),
                "false"
            )));
            assert!(output.contains(&field(
                &format!("plugin_abi_udf_sandbox_blocker_row_{row}_fallback_attempted"),
                "false"
            )));
            assert!(output.contains(&field(
                &format!("plugin_abi_udf_sandbox_blocker_row_{row}_external_engine_invoked"),
                "false"
            )));
        }
    }
}

#[test]
fn udf_fixture_plan_and_smoke_expose_admitted_deterministic_scalar_fixture() {
    let plan = run_json(&["udf-runtime-plan", "fixture", "--format", "json"]);
    assert!(plan.contains(&field("udf_runtime_kind", "builtin_deterministic_fixture")));
    assert!(plan.contains(&field("udf_runtime_available_initially", "true")));
    assert!(plan.contains(&field(
        "udf_runtime_fixture_command",
        "udf-local-scalar-fixture-smoke"
    )));
    assert!(plan.contains(&field(
        "effectful_operation_admission_row_deterministic_scalar_udf_fixture_support_status",
        "fixture_smoke_supported"
    )));

    let smoke = run_json(&[
        "udf-local-scalar-fixture-smoke",
        "3,null,-4",
        "--format",
        "json",
    ]);
    assert!(smoke.contains("\"command\":\"udf-local-scalar-fixture-smoke\""));
    assert!(smoke.contains(&field(
        "schema_version",
        "shardloom.deterministic_scalar_udf_fixture.v1"
    )));
    assert!(smoke.contains(&field("udf_id", "sl_fixture_double_i64")));
    assert!(smoke.contains(&field("output_values", "6,null,-8")));
    assert!(smoke.contains(&field("determinism", "pure_deterministic")));
    assert!(smoke.contains(&field("null_policy", "null_propagating")));
    assert!(smoke.contains(&field("sandbox_required", "false")));
    assert!(smoke.contains(&field("network_allowed", "false")));
    assert!(smoke.contains(&field("credential_resolution_performed", "false")));
    assert!(smoke.contains(&field("dynamic_loading_performed", "false")));
    assert!(smoke.contains(&field("extension_code_executed", "false")));
    assert!(smoke.contains(&field("external_effect_executed", "false")));
    assert!(smoke.contains(&field("fallback_attempted", "false")));
    assert!(smoke.contains(&field("external_engine_invoked", "false")));
    assert!(smoke.contains(&field("claim_gate_status", "fixture_smoke_only")));
    assert!(smoke.contains(&field(
        "plugin_abi_udf_sandbox_blocker_row_python_udf_support_status",
        "blocked"
    )));
}
