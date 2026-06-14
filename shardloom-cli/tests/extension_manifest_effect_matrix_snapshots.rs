use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

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

fn run_raw(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("command runs")
}

fn temp_case_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "shardloom-extension-manifest-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ))
}

fn assert_safe_manifest_summary(output: &str) {
    assert!(output.contains("\"command\":\"extension-inspect\""));
    assert!(output.contains(&field(
        "extension_manifest_inspection_schema_version",
        "shardloom.extension_manifest_inspection.v1"
    )));
    assert!(output.contains(&field(
        "extension_manifest_input_kind",
        "local_manifest_file"
    )));
    assert!(output.contains(&field(
        "extension_manifest_schema_version",
        "shardloom.extension_manifest.v1"
    )));
    assert!(output.contains(&field(
        "extension_manifest_json_parse_status",
        "passed_no_code_loaded"
    )));
    assert!(output.contains(&field("extension_manifest_inspection_status", "validated")));
    assert!(output.contains(&field(
        "extension_manifest_id",
        "example.safe_observability"
    )));
    assert!(output.contains(&field(
        "extension_manifest_category",
        "observability_exporter"
    )));
    assert!(output.contains(&field("extension_manifest_capability_count", "1")));
    assert!(output.contains(&field(
        "extension_manifest_supported_capability_claim_count",
        "0"
    )));
    assert!(output.contains(&field(
        "extension_manifest_permission_names",
        "read_metadata"
    )));
    assert!(output.contains(&field("extension_manifest_effect_kinds", "none")));
    assert!(output.contains(&field(
        "extension_manifest_effect_levels",
        "pure_deterministic"
    )));
    assert!(output.contains(&field("extension_manifest_review_required", "false")));
    assert!(output.contains(&field("extension_manifest_file_read_request_count", "1")));
}

fn assert_safe_manifest_no_runtime(output: &str) {
    assert!(output.contains(&field("extension_manifest_runtime_execution", "false")));
    assert!(output.contains(&field(
        "extension_manifest_dynamic_loading_performed",
        "false"
    )));
    assert!(output.contains(&field(
        "extension_manifest_extension_code_executed",
        "false"
    )));
    assert!(output.contains(&field(
        "extension_manifest_external_effect_executed",
        "false"
    )));
    assert!(output.contains(&field(
        "extension_manifest_network_probe_performed",
        "false"
    )));
    assert!(output.contains(&field("extension_manifest_fallback_attempted", "false")));
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
fn extension_inspect_local_manifest_validates_metadata_without_loading_code() {
    let temp_dir = temp_case_dir("safe");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let manifest_path = temp_dir.join("safe-extension.json");
    fs::write(
        &manifest_path,
        r#"{
  "schema_version": "shardloom.extension_manifest.v1",
  "extension_id": "example.safe_observability",
  "name": "Safe Observability Manifest",
  "version": "0.1.0",
  "provider": "example",
  "category": "observability_exporter",
  "license": "Apache-2.0",
  "lifecycle": "discovered",
  "capabilities": [
    {"name": "metrics_manifest", "status": "planned", "notes": "metadata only"}
  ],
  "permissions": [
    {"permission": "read_metadata", "required": false, "reason": "metadata-only inspection"}
  ],
  "effects": [
    {"effect": "none", "level": "pure_deterministic"}
  ],
  "sandbox": {
    "kind": "metadata_only",
    "allow_filesystem": false,
    "allow_network": false,
    "allow_environment": false,
    "allow_secret_access": false
  }
}"#,
    )
    .expect("manifest write");

    let manifest_arg = manifest_path.to_string_lossy().to_string();
    let output = run_json(&[
        "extension-inspect",
        "--manifest",
        &manifest_arg,
        "--format",
        "json",
    ]);
    assert_safe_manifest_summary(&output);
    assert_safe_manifest_no_runtime(&output);

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn extension_inspect_effectful_manifest_requires_review_without_effects() {
    let temp_dir = temp_case_dir("review");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let manifest_path = temp_dir.join("effectful-extension.json");
    fs::write(
        &manifest_path,
        r#"{
  "schema_version": "shardloom.extension_manifest.v1",
  "extension_id": "example.effectful_api",
  "name": "Effectful API Manifest",
  "version": "0.1.0",
  "category": "effect_provider",
  "license": "Unknown",
  "capabilities": [
    {"name": "call_api", "status": "supported"}
  ],
  "permissions": [
    {"permission": "call_api", "required": true, "reason": "external API read"}
  ],
  "effects": [
    {"effect": "api_read", "level": "external_read", "dry_run_safe": true}
  ],
  "sandbox": {
    "kind": "full_sandbox_required",
    "allow_filesystem": false,
    "allow_network": false,
    "allow_environment": false,
    "allow_secret_access": false
  }
}"#,
    )
    .expect("manifest write");

    let manifest_arg = manifest_path.to_string_lossy().to_string();
    let output = run_json(&[
        "extension-inspect",
        "--manifest",
        &manifest_arg,
        "--format",
        "json",
    ]);
    assert!(output.contains(&field(
        "extension_manifest_inspection_status",
        "requires_review"
    )));
    assert!(output.contains(&field("extension_manifest_review_required", "true")));
    assert!(output.contains(&field(
        "extension_manifest_provenance_requires_review",
        "true"
    )));
    assert!(output.contains(&field(
        "extension_manifest_supported_capability_claim_count",
        "1"
    )));
    assert!(output.contains(&field("extension_manifest_effects_declared", "true")));
    assert!(output.contains(&field("extension_manifest_permission_names", "call_api")));
    assert!(output.contains(&field("extension_manifest_effect_kinds", "api_read")));
    assert!(output.contains(&field("extension_manifest_effect_levels", "external_read")));
    assert!(output.contains(&field(
        "extension_manifest_sandbox_kind",
        "full_sandbox_required"
    )));
    assert!(output.contains(&field(
        "extension_manifest_extension_code_executed",
        "false"
    )));
    assert!(output.contains(&field(
        "extension_manifest_external_effect_executed",
        "false"
    )));
    assert!(output.contains(&field(
        "extension_manifest_credential_resolution_performed",
        "false"
    )));
    assert!(output.contains(&field(
        "extension_manifest_network_probe_performed",
        "false"
    )));
    assert!(output.contains(&field(
        "extension_manifest_external_engine_invoked",
        "false"
    )));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn extension_inspect_blocks_remote_manifest_without_probe() {
    let output = run_raw(&[
        "extension-inspect",
        "--manifest",
        "s3://bucket/extension.json",
        "--format",
        "json",
    ]);
    assert!(!output.status.success());
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout utf8");
    assert!(stdout.contains("\"command\":\"extension-inspect\""));
    assert!(stdout.contains("local files only"));
    assert!(stdout.contains("\"fallback\":{\"attempted\":false"));

    let file_output = run_raw(&[
        "extension-inspect",
        "--manifest",
        "file://example.com/extension.json",
        "--format",
        "json",
    ]);
    assert!(!file_output.status.success());
    let file_stdout = String::from_utf8(file_output.stdout).expect("stdout utf8");
    assert!(file_stdout.contains("empty or localhost authority"));
    assert!(file_stdout.contains("\"fallback\":{\"attempted\":false"));
}

#[test]
fn extension_inspect_blocks_bad_manifest_schema_without_loading_code() {
    let temp_dir = temp_case_dir("bad-schema");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let manifest_path = temp_dir.join("bad-extension.json");
    fs::write(
        &manifest_path,
        r#"{
  "schema_version": "example.bad",
  "extension_id": "example.bad",
  "name": "Bad Manifest",
  "version": "0.1.0",
  "category": "frontend",
  "license": "Apache-2.0"
}"#,
    )
    .expect("manifest write");

    let manifest_arg = manifest_path.to_string_lossy().to_string();
    let output = run_raw(&[
        "extension-inspect",
        "--manifest",
        &manifest_arg,
        "--format",
        "json",
    ]);
    assert!(!output.status.success());
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout utf8");
    assert!(stdout.contains("\"command\":\"extension-inspect\""));
    assert!(stdout.contains("unsupported extension manifest schema_version"));
    assert!(stdout.contains("\"fallback\":{\"attempted\":false"));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn extension_inspect_blocks_oversized_manifest_before_parse() {
    let temp_dir = temp_case_dir("oversized");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let manifest_path = temp_dir.join("oversized-extension.json");
    fs::write(&manifest_path, vec![b' '; (256 * 1024) + 1]).expect("manifest write");

    let manifest_arg = manifest_path.to_string_lossy().to_string();
    let output = run_raw(&[
        "extension-inspect",
        "--manifest",
        &manifest_arg,
        "--format",
        "json",
    ]);
    assert!(!output.status.success());
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout utf8");
    assert!(stdout.contains("\"command\":\"extension-inspect\""));
    assert!(stdout.contains("inspection limit"));
    assert!(stdout.contains("\"fallback\":{\"attempted\":false"));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
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
