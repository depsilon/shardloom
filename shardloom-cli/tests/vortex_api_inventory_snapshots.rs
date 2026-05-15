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

#[test]
fn vortex_api_inventory_exposes_segment_extraction_admission_report() {
    let output = run_vortex_api_inventory_json();

    assert!(output.contains(&field(
        "vortex_segment_extraction_admission_schema_version",
        "shardloom.vortex_segment_extraction_admission.v1"
    )));
    assert!(output.contains(&field(
        "vortex_segment_extraction_admission_report_id",
        "gar0003a.vortex_segment_extraction.sparse_patch_fill"
    )));
    assert!(output.contains(&field(
        "vortex_segment_extraction_selected_layout_family",
        "sparse_patch_fill"
    )));
    assert!(output.contains(&field(
        "vortex_segment_extraction_selected_layout_status",
        "blocked_until_segment_extraction_certificate"
    )));
    assert!(output.contains(&field(
        "vortex_segment_extraction_supported_layout_count",
        "0"
    )));
    assert!(output.contains(&field(
        "vortex_segment_extraction_blocked_layout_count",
        "1"
    )));
    assert!(output.contains(&field(
        "vortex_segment_extraction_unsupported_diagnostic_codes",
        "SL_UNSUPPORTED_VORTEX_SPARSE_SEGMENT_EXTRACTION"
    )));
    assert!(output.contains(&field(
        "vortex_segment_extraction_blocker_ids",
        "gar0003a.sparse_patch_fill_segment_extraction"
    )));
    assert!(output.contains(&field(
        "vortex_segment_extraction_claim_gate_status",
        "not_claim_grade"
    )));
}

#[test]
fn vortex_api_inventory_keeps_segment_extraction_report_effect_free() {
    let output = run_vortex_api_inventory_json();

    assert!(output.contains(&field(
        "vortex_segment_extraction_runtime_execution",
        "false"
    )));
    assert!(output.contains(&field("vortex_segment_extraction_data_read", "false")));
    assert!(output.contains(&field("vortex_segment_extraction_data_decoded", "false")));
    assert!(output.contains(&field(
        "vortex_segment_extraction_data_materialized",
        "false"
    )));
    assert!(output.contains(&field("vortex_segment_extraction_object_store_io", "false")));
    assert!(output.contains(&field("vortex_segment_extraction_write_io", "false")));
    assert!(output.contains(&field(
        "vortex_segment_extraction_external_engine_invoked",
        "false"
    )));
    assert!(output.contains(&field(
        "vortex_segment_extraction_fallback_attempted",
        "false"
    )));
}

#[test]
fn vortex_api_inventory_exposes_gar0005a_local_io_coverage() {
    let output = run_vortex_api_inventory_json();

    assert!(output.contains(&field(
        "vortex_local_io_schema_version",
        "shardloom.vortex_local_io_coverage.v1"
    )));
    assert!(output.contains(&field(
        "vortex_local_io_report_id",
        "gar0005a.local_vortex_io.coverage"
    )));
    assert!(output.contains(&field("vortex_local_io_gar_id", "GAR-0005-A")));
    assert!(output.contains(&field(
        "vortex_local_io_selected_reader_lane",
        "local_vortex_primitive_scan_filter_project"
    )));
    assert!(output.contains(&field(
        "vortex_local_io_selected_writer_lane",
        "native_count_output_payload_write"
    )));
    assert!(output.contains(&field("vortex_local_io_runtime_lane_count", "2")));
    assert!(output.contains(&field("vortex_local_io_blocked_lane_count", "1")));
    assert!(output.contains(&field(
        "vortex_local_io_reader_feature_gate",
        "vortex-local-primitives"
    )));
    assert!(output.contains(&field(
        "vortex_local_io_writer_feature_gate",
        "vortex-write"
    )));
    assert!(output.contains(&field(
        "vortex_local_io_writer_status",
        "feature_gated_runtime"
    )));
    assert!(output.contains(&field("vortex_local_io_broad_writer_status", "blocked")));
}

#[test]
fn vortex_api_inventory_keeps_local_io_inventory_effect_free() {
    let output = run_vortex_api_inventory_json();

    assert!(output.contains(&field(
        "vortex_local_io_inventory_runtime_execution",
        "false"
    )));
    assert!(output.contains(&field("vortex_local_io_inventory_data_read", "false")));
    assert!(output.contains(&field("vortex_local_io_inventory_data_written", "false")));
    assert!(output.contains(&field("vortex_local_io_object_store_io", "false")));
    assert!(output.contains(&field("vortex_local_io_table_catalog_io", "false")));
    assert!(output.contains(&field("vortex_local_io_external_engine_invoked", "false")));
    assert!(output.contains(&field("vortex_local_io_fallback_attempted", "false")));
}

#[test]
fn vortex_api_inventory_exposes_gar0005b_object_store_io_gate() {
    let output = run_vortex_api_inventory_json();

    assert!(output.contains(&field(
        "vortex_object_store_io_gate_schema_version",
        "shardloom.vortex_object_store_io_gate.v1"
    )));
    assert!(output.contains(&field(
        "vortex_object_store_io_gate_report_id",
        "gar0005b.vortex_object_store_io.gate"
    )));
    assert!(output.contains(&field("vortex_object_store_io_gate_gar_id", "GAR-0005-B")));
    assert!(output.contains(&field("vortex_object_store_io_gate_status", "report_only")));
    assert!(output.contains(&field(
        "vortex_object_store_io_gate_support_status",
        "unsupported"
    )));
    assert!(output.contains(&field(
        "vortex_object_store_io_gate_unsupported_surface_count",
        "7"
    )));
    assert!(output.contains(&field(
        "vortex_object_store_io_gate_report_only_surface_count",
        "1"
    )));
    assert!(output.contains(&field(
        "vortex_object_store_io_gate_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&field(
        "vortex_object_store_io_gate_row_object_store_vortex_read_provider_status",
        "unsupported_until_certified"
    )));
    assert!(output.contains(&field(
        "vortex_object_store_io_gate_row_write_idempotency_required_evidence",
        "idempotency_key_contract,commit_protocol,recovery_certificate"
    )));
}

#[test]
fn vortex_api_inventory_keeps_object_store_io_gate_effect_free() {
    let output = run_vortex_api_inventory_json();

    for key in [
        "vortex_object_store_io_gate_object_store_read_execution_allowed",
        "vortex_object_store_io_gate_object_store_write_execution_allowed",
        "vortex_object_store_io_gate_upstream_vortex_read_allowed",
        "vortex_object_store_io_gate_upstream_vortex_write_allowed",
        "vortex_object_store_io_gate_credential_resolution_allowed",
        "vortex_object_store_io_gate_credentials_resolved",
        "vortex_object_store_io_gate_provider_probe",
        "vortex_object_store_io_gate_network_probe",
        "vortex_object_store_io_gate_runtime_execution",
        "vortex_object_store_io_gate_data_read",
        "vortex_object_store_io_gate_data_written",
        "vortex_object_store_io_gate_object_store_io",
        "vortex_object_store_io_gate_write_io",
        "vortex_object_store_io_gate_external_engine_invoked",
        "vortex_object_store_io_gate_fallback_attempted",
        "vortex_object_store_io_gate_fallback_execution_allowed",
    ] {
        assert!(
            output.contains(&field(key, "false")),
            "missing false field {key}"
        );
    }
    assert!(output.contains(&field(
        "vortex_object_store_io_gate_side_effect_free",
        "true"
    )));
    assert!(output.contains(&field(
        "vortex_object_store_io_gate_deterministic_unsupported_diagnostics_ready",
        "true"
    )));
    assert!(output.contains(&field(
        "vortex_object_store_io_gate_unsupported_diagnostic_count",
        "7"
    )));
    assert!(output.contains("\"diagnostics\":[{\"code\":\"SL_OBJECT_STORE_UNSUPPORTED\""));
}
