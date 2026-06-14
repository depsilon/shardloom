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
        "flat_scalar_vortex_ingest_prepared_state_write"
    )));
    assert!(output.contains(&field("vortex_local_io_runtime_lane_count", "4")));
    assert!(output.contains("native_count_output_payload_write"));
    assert!(output.contains("flat_columnar_vortex_ingest_prepared_state_write"));
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
fn vortex_api_inventory_exposes_native_writer_schema_certification() {
    let output = run_vortex_api_inventory_json();

    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_schema_version",
        "shardloom.vortex_native_writer_schema_certification.v1"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_report_id",
        "prod-ready-1a.vortex-native-writer-schema-certification"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_scoped_runtime_row_count",
        "6"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_provider_candidate_row_count",
        "2"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_blocked_row_count",
        "1"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_scoped_runtime_row_ids",
        "flat_scalar_rows_nullable_primitives,typed_complex_scalar_rows_arrow_provider,flat_columnar_source_state_arrow_provider,nullable_columnar_validity_provider_gate,decimal128_columnar_provider_gate,dictionary_encoded_utf8_binary_provider_gate"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_provider_candidate_row_ids",
        "dictionary_encoded_primitives_provider_gate,extension_dtype_json_wkb_provider_gate"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_blocked_row_ids",
        "generalized_schema_encoding_writer"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_local_runtime_claim_allowed",
        "true"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_performance_claim_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_broad_schema_encoding_certification_complete",
        "false"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_no_external_fallback",
        "true"
    )));
    assert_native_writer_schema_certification_rows(&output);
}

fn assert_native_writer_schema_certification_rows(output: &str) {
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_row_flat_scalar_rows_nullable_primitives_status",
        "scoped_feature_gated_runtime"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_row_typed_complex_scalar_rows_arrow_provider_provider_decision",
        "use_vortex_native_provider"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_row_flat_columnar_source_state_arrow_provider_writer_lane_id",
        "flat_columnar_vortex_ingest_prepared_state_write"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_row_dictionary_encoded_primitives_provider_gate_status",
        "provider_candidate_pending_evidence"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_row_dictionary_encoded_utf8_binary_provider_gate_status",
        "scoped_feature_gated_runtime"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_row_dictionary_encoded_utf8_binary_provider_gate_replay_evidence",
        "local_flat_columnar_dictionary_source_writes_reopens_values"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_row_decimal128_columnar_provider_gate_status",
        "scoped_feature_gated_runtime"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_row_decimal128_columnar_provider_gate_replay_evidence",
        "local_flat_columnar_decimal_source_writes_reopens_precision_scale"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_row_nullable_columnar_validity_provider_gate_status",
        "scoped_feature_gated_runtime"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_row_nullable_columnar_validity_provider_gate_replay_evidence",
        "local_flat_columnar_nullable_source_writes_reopens_validity"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_row_extension_dtype_json_wkb_provider_gate_provider_surface",
        "vortex_json_wkb_extension_arrow_import_export"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_row_generalized_schema_encoding_writer_status",
        "blocked_pending_evidence"
    )));
    assert!(output.contains(&field(
        "vortex_native_writer_schema_certification_row_generalized_schema_encoding_writer_unsupported_diagnostic_code",
        "SL_UNSUPPORTED_GENERALIZED_VORTEX_PAYLOAD_WRITE"
    )));
}

#[test]
fn vortex_api_inventory_keeps_native_writer_schema_certification_effect_free() {
    let output = run_vortex_api_inventory_json();

    for key in [
        "vortex_native_writer_schema_certification_object_store_io",
        "vortex_native_writer_schema_certification_table_catalog_io",
        "vortex_native_writer_schema_certification_external_engine_invoked",
        "vortex_native_writer_schema_certification_fallback_attempted",
        "vortex_native_writer_schema_certification_row_generalized_schema_encoding_writer_object_store_io",
        "vortex_native_writer_schema_certification_row_generalized_schema_encoding_writer_table_catalog_io",
        "vortex_native_writer_schema_certification_row_generalized_schema_encoding_writer_external_engine_invoked",
        "vortex_native_writer_schema_certification_row_generalized_schema_encoding_writer_fallback_attempted",
    ] {
        assert!(
            output.contains(&field(key, "false")),
            "missing false field {key}"
        );
    }
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

#[test]
fn vortex_api_inventory_exposes_vortex075_heavy_operator_disposition() {
    let output = run_vortex_api_inventory_json();

    assert!(output.contains(&field(
        "vortex075_heavy_operator_schema_version",
        "shardloom.vortex075_heavy_operator_provider_disposition.v1"
    )));
    assert!(output.contains(&field(
        "vortex075_heavy_operator_report_id",
        "perf-runtime-7b.vortex075.heavy_operator_provider_disposition"
    )));
    assert!(output.contains(&field(
        "vortex075_heavy_operator_phase_id",
        "PERF-RUNTIME-7B"
    )));
    assert!(output.contains(&field(
        "vortex075_heavy_operator_row_order",
        "grouped_sum_count_aggregate,validity_mask_no_null,branchless_zip,dictionary_fsst_reuse,layout_child_cache,byte_length_expression,datafusion_54_integration"
    )));
    assert!(output.contains(&field(
        "vortex075_heavy_operator_provider_candidate_count",
        "5"
    )));
    assert!(output.contains(&field(
        "vortex075_heavy_operator_wrapped_shardloom_kernel_count",
        "1"
    )));
    assert!(output.contains(&field(
        "vortex075_heavy_operator_blocked_external_integration_count",
        "1"
    )));
    assert!(output.contains(&field(
        "vortex075_heavy_operator_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&field(
        "vortex075_heavy_operator_row_grouped_sum_count_aggregate_status",
        "candidate_pending_provider_gate"
    )));
    assert!(output.contains(&field(
        "vortex075_heavy_operator_row_grouped_sum_count_aggregate_required_evidence",
        "provider_gate,decoded_reference_parity,null_key_semantics,execution_certificate,native_io_certificate,claim_grade_benchmark_row"
    )));
    assert!(output.contains(&field(
        "vortex075_heavy_operator_row_byte_length_expression_status",
        "wrapped_by_existing_shardloom_kernel"
    )));
    assert!(output.contains(&field(
        "vortex075_heavy_operator_row_datafusion_54_integration_status",
        "blocked_external_integration"
    )));
    assert!(output.contains(&field(
        "vortex075_heavy_operator_row_datafusion_54_integration_shardloom_disposition",
        "baseline_or_oracle_only_not_shardloom_runtime_provider"
    )));
}

#[test]
fn vortex_api_inventory_keeps_vortex075_heavy_operator_disposition_effect_free() {
    let output = run_vortex_api_inventory_json();

    for key in [
        "vortex075_heavy_operator_runtime_execution",
        "vortex075_heavy_operator_data_read",
        "vortex075_heavy_operator_data_decoded",
        "vortex075_heavy_operator_data_materialized",
        "vortex075_heavy_operator_object_store_io",
        "vortex075_heavy_operator_write_io",
        "vortex075_heavy_operator_external_engine_invoked",
        "vortex075_heavy_operator_fallback_attempted",
        "vortex075_heavy_operator_fallback_execution_allowed",
    ] {
        assert!(
            output.contains(&field(key, "false")),
            "missing false field {key}"
        );
    }
    assert!(output.contains(&field("vortex075_heavy_operator_side_effect_free", "true")));
}

#[test]
fn vortex_api_inventory_exposes_vortex075_local_io_provider_disposition() {
    let output = run_vortex_api_inventory_json();

    assert!(output.contains(&field(
        "vortex075_local_io_schema_version",
        "shardloom.vortex075_local_io_provider_disposition.v1"
    )));
    assert!(output.contains(&field(
        "vortex075_local_io_report_id",
        "prod-ready-1a.vortex075.local_io_provider_disposition"
    )));
    assert!(output.contains(&field("vortex075_local_io_phase_id", "PROD-READY-1A")));
    assert!(output.contains(&field(
        "vortex075_local_io_row_order",
        "layout_reader_context_cache,json_extension_arrow_interop,wkb_geospatial_extension,interleave_encoding,binary_zstd_compression,row_byte_encoder,validity_mask_semantics,arrow_device_gpu_path"
    )));
    assert!(output.contains(&field("vortex075_local_io_provider_candidate_count", "7")));
    assert!(output.contains(&field(
        "vortex075_local_io_blocked_future_device_count",
        "1"
    )));
    assert!(output.contains(&field(
        "vortex075_local_io_deterministic_blocker_required_count",
        "8"
    )));
    assert!(output.contains(&field(
        "vortex075_local_io_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&field(
        "vortex075_local_io_row_layout_reader_context_cache_status",
        "candidate_pending_provider_gate"
    )));
    assert!(output.contains(&field(
        "vortex075_local_io_row_json_extension_arrow_interop_shardloom_disposition",
        "candidate_for_json_extension_preservation_and_deterministic_expression_blockers"
    )));
    assert!(output.contains(&field(
        "vortex075_local_io_row_wkb_geospatial_extension_required_evidence",
        "provider_gate,wkb_extension_fidelity_report,geo_execution_blocker,translation_report,native_io_certificate"
    )));
    assert!(output.contains(&field(
        "vortex075_local_io_row_arrow_device_gpu_path_status",
        "blocked_future_device_track"
    )));
    assert!(output.contains(&field(
        "vortex075_local_io_row_arrow_device_gpu_path_shardloom_disposition",
        "blocked_future_device_track_not_local_cpu_v1"
    )));
}

#[test]
fn vortex_api_inventory_keeps_vortex075_local_io_provider_disposition_effect_free() {
    let output = run_vortex_api_inventory_json();

    for key in [
        "vortex075_local_io_runtime_execution",
        "vortex075_local_io_data_read",
        "vortex075_local_io_data_written",
        "vortex075_local_io_data_decoded",
        "vortex075_local_io_data_materialized",
        "vortex075_local_io_object_store_io",
        "vortex075_local_io_table_catalog_io",
        "vortex075_local_io_write_io",
        "vortex075_local_io_external_engine_invoked",
        "vortex075_local_io_fallback_attempted",
        "vortex075_local_io_fallback_execution_allowed",
    ] {
        assert!(
            output.contains(&field(key, "false")),
            "missing false field {key}"
        );
    }
    assert!(output.contains(&field("vortex075_local_io_side_effect_free", "true")));
}
