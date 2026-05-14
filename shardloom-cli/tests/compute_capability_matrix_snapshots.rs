use std::process::Command;

fn run_compute_matrix_json(args: &[&str], expect_success: bool) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("shardloom command runs");

    assert_eq!(
        output.status.success(),
        expect_success,
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

fn assert_no_runtime_no_fallback_no_effects(output: &str) {
    assert!(output.contains(&field("plan_only", "true")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("query_execution", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("network_probe", "false")));
    assert!(output.contains(&field("catalog_probe", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("external_effects_executed", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("no_runtime", "true")));
    assert!(output.contains(&field("no_fallback", "true")));
    assert!(output.contains(&field("no_effects", "true")));
    assert!(output.contains("\"fallback\":{\"attempted\":false,\"allowed\":false"));
}

#[test]
fn compute_capability_matrix_exposes_rows_families_and_claim_gates() {
    let output = run_compute_matrix_json(&["compute-capability-matrix", "--format", "json"], true);

    assert!(output.contains("\"command\":\"compute-capability-matrix\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "compute_capability_matrix")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.compute_capability_matrix.v1"
    )));
    assert!(output.contains(&field("matrix_status", "report_only")));
    assert!(output.contains(&field("claim_grade_status", "evidence_incomplete")));
    assert!(output.contains(&field("compute_row_count", "13")));
    assert!(output.contains(&field("operator_family_count", "14")));
    assert!(output.contains(&field(
        "support_status_vocabulary",
        "unsupported,planned,report_only,executable_uncertified,fixture_certified,workload_certified,production_certified"
    )));
    assert!(output.contains(&field(
        "provider_kind_vocabulary",
        "shardloom_kernel,vortex_array_kernel,vortex_scan,vortex_source,vortex_sink,compatibility_boundary,external_baseline_only"
    )));
    assert!(output.contains(&field(
        "compute_row_order",
        "local_vortex_count,local_vortex_filtered_count,local_vortex_projection,local_vortex_filter_project,prepared_encoded_filter,reader_backed_dictionary_filter,compatibility_csv_import,direct_compatibility_transient,vortex_sink_write,grouped_aggregate,join,window_row_number,sql_frontend"
    )));
    assert!(output.contains(&field("fixture_certified_count", "2")));
    assert!(output.contains(&field("executable_uncertified_count", "5")));
    assert!(output.contains(&field("report_only_count", "1")));
    assert!(output.contains(&field("planned_count", "3")));
    assert!(output.contains(&field("unsupported_count", "2")));
    assert!(output.contains(&field("workload_certified_count", "0")));
    assert!(output.contains(&field("production_certified_count", "0")));
    assert!(output.contains(&field("performance_claim_allowed", "false")));
    assert!(output.contains(&field("spark_displacement_claim_allowed", "false")));
    assert!(output.contains(&field("matrix_consuming_views_status", "planned_alignment")));
    assert!(output.contains(&field("all_rows_fallback_attempted_false", "true")));
    assert!(output.contains(&field("all_rows_external_engine_invoked_false", "true")));
    assert!(output.contains("\"artifact_kind\":\"compute_capability_matrix_report\""));
    assert_no_runtime_no_fallback_no_effects(&output);
}

#[test]
fn compute_capability_matrix_rows_distinguish_provider_and_support_status() {
    let output = run_compute_matrix_json(&["compute-capability-matrix", "--format", "json"], true);

    assert!(output.contains(&field(
        "compute_row_local_vortex_count_support_status",
        "fixture_certified"
    )));
    assert!(output.contains(&field(
        "compute_row_local_vortex_count_provider_kind",
        "vortex_scan"
    )));
    assert!(output.contains(&field(
        "compute_row_local_vortex_count_execution_certificate_refs",
        "certificates/cg16/local-vortex-count/execution.json"
    )));
    assert!(output.contains(&field(
        "compute_row_local_vortex_filter_project_materialization_decode_requirement",
        "selection_vector_plus_projection_no_row_materialization"
    )));
    assert!(output.contains(&field(
        "compute_row_compatibility_csv_import_provider_kind",
        "compatibility_boundary"
    )));
    assert!(output.contains(&field(
        "compute_row_direct_compatibility_transient_support_status",
        "unsupported"
    )));
    assert!(output.contains(&field(
        "compute_row_direct_compatibility_transient_unsupported_diagnostic_code",
        "SL_UNSUPPORTED_DIRECT_COMPATIBILITY_TRANSIENT"
    )));
    assert!(output.contains(&field(
        "compute_row_direct_compatibility_transient_vortex_native_claim_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "compute_row_direct_compatibility_transient_fallback_attempted",
        "false"
    )));
    assert!(output.contains(&field(
        "compute_row_direct_compatibility_transient_external_engine_invoked",
        "false"
    )));
    assert!(output.contains(&field(
        "compute_row_vortex_sink_write_support_status",
        "report_only"
    )));
    assert!(output.contains(&field(
        "compute_row_grouped_aggregate_memory_spill_requirement",
        "hash_group_state_spill_required"
    )));
    assert!(output.contains(&field(
        "compute_row_sql_frontend_unsupported_diagnostic_code",
        "SL_UNSUPPORTED_SQL"
    )));
    assert!(output.contains(&field(
        "compute_row_sql_frontend_blocker_id",
        "cg21.workflow.sql.frontend_unsupported"
    )));
    assert!(output.contains(&field(
        "operator_family_joins_next_evidence",
        "join_null_semantics,build_probe_memory,benchmarks"
    )));
    assert!(output.contains(&field(
        "operator_family_sink_write_operators_support_status",
        "report_only"
    )));
}

#[test]
fn compute_capability_matrix_rejects_extra_arguments() {
    let output = run_compute_matrix_json(
        &["compute-capability-matrix", "extra", "--format", "json"],
        false,
    );

    assert!(output.contains("\"command\":\"compute-capability-matrix\""));
    assert!(output.contains("\"status\":\"error\""));
    assert!(output.contains("unexpected compute-capability-matrix argument"));
}
