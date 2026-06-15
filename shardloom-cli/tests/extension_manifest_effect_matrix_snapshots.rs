use std::{
    fs,
    path::{Path, PathBuf},
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
    assert!(output.contains(&field(
        "extension_manifest_effect_execution_admission_status",
        "metadata_only_non_executing"
    )));
    assert!(output.contains(&field(
        "extension_manifest_effect_execution_allowed",
        "false"
    )));
    assert!(output.contains(&field("extension_manifest_review_required", "false")));
    assert!(output.contains(&field(
        "extension_manifest_sandbox_admission_status",
        "deny_by_default_no_host_access"
    )));
    assert!(output.contains(&field(
        "extension_manifest_sandbox_review_required",
        "false"
    )));
    assert!(output.contains(&field(
        "extension_manifest_sandbox_host_access_requested",
        "false"
    )));
    assert!(output.contains(&field("extension_manifest_requested_host_access", "none")));
    assert!(output.contains(&field(
        "extension_manifest_runtime_admission_status",
        "not_declared_metadata_only"
    )));
    assert!(output.contains(&field(
        "extension_manifest_runtime_review_required",
        "false"
    )));
    assert!(output.contains(&field("extension_manifest_file_read_request_count", "1")));
    assert!(output.contains(&field(
        "extension_manifest_execution_contract_complete",
        "true"
    )));
    assert!(output.contains(&field(
        "extension_manifest_determinism",
        "pure_deterministic"
    )));
    assert!(output.contains(&field(
        "extension_manifest_materialization",
        "metadata_only"
    )));
    assert!(output.contains(&field("extension_manifest_null_behavior", "null_aware")));
    assert!(output.contains(&field("extension_manifest_input_dtypes", "metadata")));
    assert!(output.contains(&field("extension_manifest_output_dtype", "metadata")));
    assert!(output.contains(&field(
        "extension_manifest_resource_contract_declared",
        "true"
    )));
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

fn assert_effectful_manifest_review_summary(output: &str) {
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
        "extension_manifest_effect_execution_admission_status",
        "denied_by_default_external_effect"
    )));
    assert!(output.contains(&field(
        "extension_manifest_effect_execution_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "extension_manifest_sandbox_kind",
        "full_sandbox_required"
    )));
    assert!(output.contains(&field(
        "extension_manifest_sandbox_admission_status",
        "deny_by_default_no_host_access"
    )));
    assert!(output.contains(&field(
        "extension_manifest_sandbox_review_required",
        "false"
    )));
    assert!(output.contains(&field(
        "extension_manifest_sandbox_host_access_requested",
        "false"
    )));
    assert!(output.contains(&field("extension_manifest_requested_host_access", "none")));
    assert_safe_manifest_no_runtime(output);
    assert!(output.contains(&field(
        "extension_manifest_credential_resolution_performed",
        "false"
    )));
    assert!(output.contains(&field(
        "extension_manifest_external_engine_invoked",
        "false"
    )));
}

fn assert_resource_audit_security_review_summary(output: &str) {
    assert!(output.contains(&field(
        "extension_manifest_inspection_status",
        "requires_review"
    )));
    assert!(
        output.contains(
            "Manifest is parseable and code-free but needs manual review before enablement"
        )
    );
    for reason in [
        "effectful_permission_declared",
        "effectful_operation_declared",
        "supported_capability_claim_declared",
        "runtime_requires_sandbox_or_bridge_review",
    ] {
        assert!(output.contains(reason), "missing review reason {reason}");
    }
    assert!(output.contains(&field(
        "extension_manifest_effect_execution_admission_status",
        "denied_by_default_external_effect"
    )));
    assert!(output.contains(&field(
        "extension_manifest_effect_execution_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "extension_manifest_runtime_admission_status",
        "blocked_wasm_requires_runtime_fuel_memory_timeout_sandbox"
    )));
    assert!(output.contains(&field(
        "extension_manifest_resource_contract",
        "timeout_ms=75,memory_bytes=4194304,cpu_ms=50"
    )));
    assert!(output.contains(&field("extension_manifest_timeout_millis", "75")));
    assert!(output.contains(&field("extension_manifest_max_memory_bytes", "4194304")));
    assert!(output.contains(&field("extension_manifest_max_cpu_millis", "50")));
    assert!(output.contains(&field(
        "extension_manifest_resource_contract_declared",
        "true"
    )));
    assert!(output.contains(&field(
        "extension_manifest_audit_policy",
        "full_audit_required"
    )));
    assert_safe_manifest_no_runtime(output);
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
fn udf_registry_json_exposes_typed_scalar_aggregate_table_contract() {
    let output = run_json(&["udf-registry", "--format", "json"]);
    assert!(output.contains("\"command\":\"udf-registry\""));
    assert!(output.contains(&field(
        "typed_udf_registry_schema_version",
        "shardloom.typed_udf_registry.v1"
    )));
    assert!(output.contains(&field(
        "typed_udf_registry_support_status",
        "scoped_fixture_supported"
    )));
    assert!(output.contains(&field(
        "typed_udf_registry_claim_gate_status",
        "fixture_smoke_only"
    )));
    assert!(output.contains(&field(
        "typed_udf_registry_admitted_local_fixture_count",
        "1"
    )));
    assert!(output.contains(&field("typed_udf_registry_scalar_count", "2")));
    assert!(output.contains(&field("typed_udf_registry_aggregate_count", "1")));
    assert!(output.contains(&field("typed_udf_registry_table_function_count", "1")));
    assert!(output.contains(&field(
        "typed_udf_registry_encoded_native_candidate_count",
        "1"
    )));
    assert!(output.contains(&field(
        "typed_udf_registry_materialization_required_count",
        "2"
    )));
    assert!(output.contains(&field(
        "typed_udf_registry_local_fixture_execution_bridge_available",
        "true"
    )));
    assert!(output.contains(&field(
        "typed_udf_registry_arbitrary_runtime_bridge_available",
        "false"
    )));
    assert!(output.contains(&field("typed_udf_registry_sandbox_policy_declared", "true")));
    for key in [
        "typed_udf_registry_filesystem_access_allowed",
        "typed_udf_registry_network_access_allowed",
        "typed_udf_registry_secret_access_allowed",
        "typed_udf_registry_dynamic_loading_allowed",
        "typed_udf_registry_runtime_execution_performed",
        "typed_udf_registry_extension_code_executed",
        "typed_udf_registry_external_effect_executed",
        "typed_udf_registry_fallback_attempted",
        "typed_udf_registry_external_engine_invoked",
    ] {
        assert!(
            output.contains(&field(key, "false")),
            "missing false field {key}"
        );
    }
    assert!(output.contains(&field(
        "typed_udf_registry_row_sl_fixture_double_i64_kind",
        "scalar"
    )));
    assert!(output.contains(&field(
        "typed_udf_registry_row_sl_fixture_double_i64_support_status",
        "admitted_local_fixture"
    )));
    assert!(output.contains(&field(
        "typed_udf_registry_row_sl_fixture_double_i64_runtime_fixture_command",
        "udf-local-scalar-fixture-smoke"
    )));
    assert!(output.contains(&field(
        "typed_udf_registry_row_sl_native_sum_i64_kind",
        "aggregate"
    )));
    assert!(output.contains(&field(
        "typed_udf_registry_row_sl_native_sum_i64_encoded_capability",
        "encoded_native_candidate"
    )));
    assert!(output.contains(&field(
        "typed_udf_registry_row_sl_table_generate_series_i64_kind",
        "table_function"
    )));
    assert!(output.contains(&field(
        "typed_udf_registry_row_sl_table_generate_series_i64_materialization_required",
        "true"
    )));
    assert!(output.contains(&field(
        "typed_udf_registry_row_external_python_scalar_boundary_support_status",
        "blocked_sandbox_policy"
    )));
}

fn write_registry_directory_manifests(temp_dir: &Path) {
    fs::write(temp_dir.join("notes.txt"), "ignored").expect("notes write");
    fs::write(
        temp_dir.join("a-safe.json"),
        r#"{
  "schema_version": "shardloom.extension_manifest.v1",
  "extension_id": "example.registry_safe",
  "name": "Registry Safe",
  "version": "0.1.0",
  "category": "observability_exporter",
  "license": "Apache-2.0",
  "capabilities": [{"name": "metrics_manifest", "status": "planned"}],
  "permissions": [{"permission": "read_metadata", "required": false, "reason": "metadata"}],
  "effects": [{"effect": "none", "level": "pure_deterministic"}],
  "execution_contract": {
    "determinism": "pure_deterministic",
    "materialization": "metadata_only",
    "null_behavior": "null_aware",
    "input_dtypes": ["metadata"],
    "output_dtype": "metadata",
    "timeout_millis": 1000,
    "max_memory_bytes": 16777216,
    "max_cpu_millis": 1000,
    "retry": "none",
    "idempotency": "not_required",
    "audit": "manifest_only"
  }
}"#,
    )
    .expect("safe write");
    fs::write(
        temp_dir.join("b-review.json"),
        r#"{
  "schema_version": "shardloom.extension_manifest.v1",
  "extension_id": "example.registry_review",
  "name": "Registry Review",
  "version": "0.1.0",
  "category": "effect_provider",
  "license": "Apache-2.0",
  "capabilities": [{"name": "api_read", "status": "supported"}],
  "permissions": [{"permission": "call_api", "required": true, "reason": "external API"}],
  "effects": [{"effect": "api_read", "level": "external_read"}],
  "execution_contract": {
    "determinism": "external_effect_bound",
    "materialization": "materialization_required",
    "null_behavior": "null_aware",
    "input_dtypes": ["utf8"],
    "output_dtype": "utf8",
    "timeout_millis": 250,
    "max_memory_bytes": 8388608,
    "max_cpu_millis": 250,
    "retry": "at_most_once",
    "idempotency": "required",
    "audit": "execution_certificate_required"
  }
}"#,
    )
    .expect("review write");
}

fn assert_registry_directory_summary(output: &str) {
    assert!(output.contains("\"command\":\"extension-registry\""));
    assert!(output.contains(&field(
        "extension_registry_snapshot_schema_version",
        "shardloom.extension_registry_snapshot.v1"
    )));
    assert!(output.contains(&field(
        "extension_registry_input_kind",
        "approved_local_manifest_directory"
    )));
    assert!(output.contains(&field(
        "extension_registry_directory_read_performed",
        "true"
    )));
    assert!(output.contains(&field("extension_registry_directory_entry_count", "3")));
    assert!(output.contains(&field("extension_registry_manifest_file_count", "2")));
    assert!(output.contains(&field(
        "extension_registry_manifest_file_read_request_count",
        "2"
    )));
    assert!(output.contains(&field("extension_registry_manifest_count", "2")));
    assert!(output.contains(&field("extension_registry_requires_review_count", "1")));
    assert!(output.contains(&field("extension_registry_contract_complete_count", "2")));
    assert!(output.contains(&field("extension_registry_contract_incomplete_count", "0")));
    assert!(output.contains(&field(
        "extension_registry_manifest_ids",
        "example.registry_safe,example.registry_review"
    )));
}

fn assert_registry_directory_no_runtime(output: &str) {
    assert!(output.contains(&field("extension_registry_runtime_execution", "false")));
    assert!(output.contains(&field(
        "extension_registry_extension_code_executed",
        "false"
    )));
    assert!(output.contains(&field(
        "extension_registry_external_effect_executed",
        "false"
    )));
    assert!(output.contains(&field(
        "extension_registry_network_probe_performed",
        "false"
    )));
    assert!(output.contains(&field("extension_registry_fallback_attempted", "false")));
    assert!(output.contains(&field(
        "extension_registry_external_engine_invoked",
        "false"
    )));
}

#[test]
fn extension_registry_manifest_directory_discovers_manifests_without_runtime() {
    let temp_dir = temp_case_dir("registry-dir");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    write_registry_directory_manifests(&temp_dir);

    let dir_arg = temp_dir.to_string_lossy().to_string();
    let output = run_json(&[
        "extension-registry",
        "--manifest-dir",
        &dir_arg,
        "--format",
        "json",
    ]);
    assert_registry_directory_summary(&output);
    assert_registry_directory_no_runtime(&output);

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn extension_registry_manifest_directory_blocks_duplicate_ids() {
    let temp_dir = temp_case_dir("registry-dupe");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let body = |name: &str| {
        format!(
            r#"{{
  "schema_version": "shardloom.extension_manifest.v1",
  "extension_id": "example.duplicate",
  "name": "{name}",
  "version": "0.1.0",
  "category": "observability_exporter",
  "license": "Apache-2.0",
  "execution_contract": {{
    "determinism": "pure_deterministic",
    "materialization": "metadata_only",
    "null_behavior": "null_aware",
    "input_dtypes": ["metadata"],
    "output_dtype": "metadata",
    "timeout_millis": 1000,
    "max_memory_bytes": 16777216,
    "max_cpu_millis": 1000,
    "retry": "none",
    "idempotency": "not_required",
    "audit": "manifest_only"
  }}
}}"#
        )
    };
    fs::write(temp_dir.join("a.json"), body("A")).expect("a write");
    fs::write(temp_dir.join("b.json"), body("B")).expect("b write");

    let dir_arg = temp_dir.to_string_lossy().to_string();
    let output = run_raw(&[
        "extension-registry",
        "--manifest-dir",
        &dir_arg,
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
    assert!(stdout.contains("\"command\":\"extension-registry\""));
    assert!(stdout.contains("duplicate extension manifest id"));
    assert!(stdout.contains("\"fallback\":{\"attempted\":false"));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
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
  },
  "execution_contract": {
    "determinism": "pure_deterministic",
    "materialization": "metadata_only",
    "null_behavior": "null_aware",
    "input_dtypes": ["metadata"],
    "output_dtype": "metadata",
    "timeout_millis": 1000,
    "max_memory_bytes": 16777216,
    "max_cpu_millis": 1000,
    "retry": "none",
    "idempotency": "not_required",
    "audit": "manifest_only"
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
  },
  "execution_contract": {
    "determinism": "external_effect_bound",
    "materialization": "materialization_required",
    "null_behavior": "null_aware",
    "input_dtypes": ["utf8"],
    "output_dtype": "utf8",
    "timeout_millis": 250,
    "max_memory_bytes": 8388608,
    "max_cpu_millis": 250,
    "retry": "at_most_once",
    "idempotency": "required",
    "audit": "execution_certificate_required"
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
    assert_effectful_manifest_review_summary(&output);

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn extension_inspect_filesystem_permission_requires_review_without_effects() {
    let temp_dir = temp_case_dir("filesystem-permission");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let manifest_path = temp_dir.join("filesystem-permission-extension.json");
    fs::write(
        &manifest_path,
        r#"{
  "schema_version": "shardloom.extension_manifest.v1",
  "extension_id": "example.filesystem_permission",
  "name": "Filesystem Permission Manifest",
  "version": "0.1.0",
  "category": "observability_exporter",
  "license": "Apache-2.0",
  "capabilities": [
    {"name": "metrics_manifest", "status": "planned"}
  ],
  "permissions": [
    {"permission": "access_filesystem", "required": true, "reason": "inspect local manifest directory"}
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
  },
  "execution_contract": {
    "determinism": "pure_deterministic",
    "materialization": "metadata_only",
    "null_behavior": "null_aware",
    "input_dtypes": ["metadata"],
    "output_dtype": "metadata",
    "timeout_millis": 1000,
    "max_memory_bytes": 16777216,
    "max_cpu_millis": 1000,
    "retry": "none",
    "idempotency": "not_required",
    "audit": "manifest_only"
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
        "extension_manifest_permission_names",
        "access_filesystem"
    )));
    assert!(output.contains(&field(
        "extension_manifest_effect_execution_admission_status",
        "denied_by_default_permission_review_required"
    )));
    assert!(output.contains(&field("extension_manifest_requested_host_access", "none")));
    assert!(output.contains(&field("extension_manifest_fallback_attempted", "false")));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn extension_inspect_security_manifest_exposes_resource_audit_review_and_no_fallback() {
    let temp_dir = temp_case_dir("security-review");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let manifest_path = temp_dir.join("security-review-extension.json");
    fs::write(
        &manifest_path,
        r#"{
  "schema_version": "shardloom.extension_manifest.v1",
  "extension_id": "example.security_review",
  "name": "Security Review Manifest",
  "version": "0.1.0",
  "category": "effect_provider",
  "license": "Apache-2.0",
  "runtime": "wasm",
  "capabilities": [
    {"name": "bounded_api_enrichment", "status": "supported"}
  ],
  "permissions": [
    {"permission": "call_api", "required": true, "reason": "external enrichment"}
  ],
  "effects": [
    {"effect": "api_write", "level": "external_write", "requires_approval": true}
  ],
  "sandbox": {
    "kind": "bounded_resources",
    "allow_filesystem": false,
    "allow_network": false,
    "allow_environment": false,
    "allow_secret_access": false,
    "max_memory_bytes": 4194304,
    "max_runtime_millis": 75
  },
  "execution_contract": {
    "determinism": "external_effect_bound",
    "materialization": "materialization_required",
    "null_behavior": "null_aware",
    "input_dtypes": ["utf8"],
    "output_dtype": "utf8",
    "timeout_millis": 75,
    "max_memory_bytes": 4194304,
    "max_cpu_millis": 50,
    "retry": "manual_replay_required",
    "idempotency": "key_required",
    "audit": "full_audit_required"
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
    assert_resource_audit_security_review_summary(&output);

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn extension_inspect_host_access_manifest_requires_review_without_effects() {
    let temp_dir = temp_case_dir("host-access");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let manifest_path = temp_dir.join("host-access-extension.json");
    fs::write(
        &manifest_path,
        r#"{
  "schema_version": "shardloom.extension_manifest.v1",
  "extension_id": "example.host_access",
  "name": "Host Access Manifest",
  "version": "0.1.0",
  "category": "observability_exporter",
  "license": "Apache-2.0",
  "capabilities": [
    {"name": "metrics_manifest", "status": "planned"}
  ],
  "permissions": [
    {"permission": "read_metadata", "required": false, "reason": "metadata inspection"}
  ],
  "sandbox": {
    "kind": "metadata_only",
    "allow_filesystem": false,
    "allow_network": true,
    "allow_environment": false,
    "allow_secret_access": true
  },
  "execution_contract": {
    "determinism": "pure_deterministic",
    "materialization": "metadata_only",
    "null_behavior": "null_aware",
    "input_dtypes": ["metadata"],
    "output_dtype": "metadata",
    "timeout_millis": 1000,
    "max_memory_bytes": 16777216,
    "max_cpu_millis": 1000,
    "retry": "none",
    "idempotency": "not_required",
    "audit": "manifest_only"
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
    assert!(output.contains(&field("extension_manifest_effects_declared", "false")));
    assert!(output.contains(&field(
        "extension_manifest_effect_execution_admission_status",
        "denied_by_default_host_access"
    )));
    assert!(output.contains(&field("extension_manifest_sandbox_safe_default", "false")));
    assert!(output.contains(&field(
        "extension_manifest_sandbox_admission_status",
        "review_required_host_access_requested"
    )));
    assert!(output.contains(&field("extension_manifest_sandbox_review_required", "true")));
    assert!(output.contains(&field(
        "extension_manifest_sandbox_host_access_requested",
        "true"
    )));
    assert!(output.contains(&field(
        "extension_manifest_requested_host_access",
        "network,secret_access"
    )));
    assert!(output.contains(&field("extension_manifest_sandbox_allow_network", "true")));
    assert!(output.contains(&field(
        "extension_manifest_sandbox_allow_secret_access",
        "true"
    )));
    assert_safe_manifest_no_runtime(&output);

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn extension_inspect_missing_contract_requires_review_without_runtime() {
    let temp_dir = temp_case_dir("missing-contract");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let manifest_path = temp_dir.join("missing-contract-extension.json");
    fs::write(
        &manifest_path,
        r#"{
  "schema_version": "shardloom.extension_manifest.v1",
  "extension_id": "example.missing_contract",
  "name": "Missing Contract",
  "version": "0.1.0",
  "category": "observability_exporter",
  "license": "Apache-2.0",
  "capabilities": [
    {"name": "metrics_manifest", "status": "planned"}
  ],
  "permissions": [
    {"permission": "read_metadata", "required": false, "reason": "metadata-only inspection"}
  ],
  "effects": [
    {"effect": "none", "level": "pure_deterministic"}
  ]
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
    assert!(output.contains(&field(
        "extension_manifest_execution_contract_complete",
        "false"
    )));
    assert!(output.contains(&field(
        "extension_manifest_resource_contract_declared",
        "false"
    )));
    assert!(output.contains(&field("extension_manifest_fallback_attempted", "false")));
    assert!(output.contains(&field(
        "extension_manifest_external_engine_invoked",
        "false"
    )));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn extension_inspect_missing_materialization_requires_review_without_runtime() {
    let temp_dir = temp_case_dir("missing-materialization");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let manifest_path = temp_dir.join("missing-materialization-extension.json");
    fs::write(
        &manifest_path,
        r#"{
  "schema_version": "shardloom.extension_manifest.v1",
  "extension_id": "example.missing_materialization",
  "name": "Missing Materialization",
  "version": "0.1.0",
  "category": "observability_exporter",
  "license": "Apache-2.0",
  "capabilities": [
    {"name": "metrics_manifest", "status": "planned"}
  ],
  "permissions": [
    {"permission": "read_metadata", "required": false, "reason": "metadata-only inspection"}
  ],
  "effects": [
    {"effect": "none", "level": "pure_deterministic"}
  ],
  "execution_contract": {
    "determinism": "pure_deterministic",
    "null_behavior": "null_aware",
    "input_dtypes": ["metadata"],
    "output_dtype": "metadata",
    "timeout_millis": 1000,
    "max_memory_bytes": 16777216,
    "max_cpu_millis": 1000,
    "retry": "none",
    "idempotency": "not_required",
    "audit": "manifest_only"
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
    assert!(output.contains(&field(
        "extension_manifest_execution_contract_complete",
        "false"
    )));
    assert!(output.contains(&field("extension_manifest_materialization", "unsupported")));

    fs::remove_dir_all(&temp_dir).expect("fixture cleanup");
}

#[test]
fn extension_inspect_rejects_mistyped_optional_manifest_fields() {
    let temp_dir = temp_case_dir("mistyped-optional");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::create_dir_all(&temp_dir).expect("temp dir");
    let manifest_path = temp_dir.join("mistyped-extension.json");
    fs::write(
        &manifest_path,
        r#"{
  "schema_version": "shardloom.extension_manifest.v1",
  "extension_id": "example.mistyped",
  "name": "Mistyped Manifest",
  "version": "0.1.0",
  "category": "observability_exporter",
  "license": "Apache-2.0",
  "capabilities": [
    {"name": "metrics_manifest", "status": "planned"}
  ],
  "permissions": [
    {"permission": "read_metadata", "required": false, "reason": "metadata-only inspection"}
  ],
  "effects": [
    {"effect": "none", "level": "pure_deterministic"}
  ],
  "sandbox": {
    "kind": "metadata_only",
    "allow_network": "true"
  },
  "execution_contract": {
    "determinism": "pure_deterministic",
    "materialization": "metadata_only",
    "null_behavior": "null_aware",
    "input_dtypes": ["metadata"],
    "output_dtype": "metadata",
    "timeout_millis": 1000,
    "max_memory_bytes": "16777216",
    "max_cpu_millis": 1000,
    "retry": "none",
    "idempotency": "not_required",
    "audit": "manifest_only"
  }
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
    assert!(stdout.contains("must be a boolean when present"));
    assert!(stdout.contains("\"fallback\":{\"attempted\":false"));

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
