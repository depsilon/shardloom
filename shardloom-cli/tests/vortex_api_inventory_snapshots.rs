use std::process::Command;

fn run_vortex_api_inventory_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["vortex-api-inventory", "--format", "json"])
        .output()
        .expect("vortex API inventory command runs");

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
fn vortex_api_inventory_exposes_source_split_admission_proof() {
    let output = run_vortex_api_inventory_json();

    assert!(output.contains("\"command\":\"vortex-api-inventory\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "vortex_api_inventory")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("plan_only", "true")));
    assert!(output.contains(&field(
        "vortex_source_split_admission_schema_version",
        "shardloom.vortex_source_split_runtime_admission.v1"
    )));
    assert!(output.contains(&field(
        "vortex_source_split_admission_proof_id",
        "gar0042a.vortex_source_split.local_fixture_scan"
    )));
    assert!(output.contains(&field(
        "vortex_source_split_admission_path_id",
        "local_vortex_file_scan_into_array_iter"
    )));
    assert!(output.contains(&field(
        "vortex_source_split_admission_selected_path_status",
        "fixture_smoke_only"
    )));
    assert!(output.contains(&field(
        "vortex_source_split_admission_generalized_runtime_status",
        "blocked_until_source_split_certificate"
    )));
    assert!(output.contains(&field(
        "vortex_source_split_admission_provider_kind",
        "vortex_scan"
    )));
    assert!(output.contains(&field(
        "vortex_source_split_admission_feature_gate",
        "vortex-local-primitives"
    )));
    assert!(output.contains(&field(
        "vortex_source_split_admission_source_surface",
        "local_vortex_file_scan"
    )));
    assert!(output.contains(&field(
        "vortex_source_split_admission_split_surface",
        "reader_chunk_split_ref"
    )));
    assert!(output.contains(&field(
        "vortex_source_split_admission_claim_gate_status",
        "fixture_smoke_only"
    )));
    assert!(output.contains(&field(
        "vortex_source_split_admission_claim_boundary",
        "local_fixture_scan_only_not_generalized_source_split_runtime"
    )));
}

#[test]
fn vortex_api_inventory_keeps_source_split_report_effect_free() {
    let output = run_vortex_api_inventory_json();

    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("data_decoded", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("table_catalog_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field(
        "vortex_source_split_admission_runtime_execution",
        "false"
    )));
    assert!(output.contains(&field(
        "vortex_source_split_admission_external_engine_invoked",
        "false"
    )));
    assert!(output.contains(&field(
        "vortex_source_split_admission_fallback_attempted",
        "false"
    )));
}
