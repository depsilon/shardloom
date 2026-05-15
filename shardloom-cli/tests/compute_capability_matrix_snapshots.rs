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

fn assert_matrix_summary_fields(output: &str) {
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
    assert!(output.contains("\"artifact_kind\":\"compute_capability_matrix_report\""));
}

fn assert_matrix_claim_counts(output: &str) {
    assert!(output.contains(&field("fixture_certified_count", "5")));
    assert!(output.contains(&field("executable_uncertified_count", "5")));
    assert!(output.contains(&field("report_only_count", "1")));
    assert!(output.contains(&field("planned_count", "0")));
    assert!(output.contains(&field("unsupported_count", "2")));
    assert!(output.contains(&field("workload_certified_count", "0")));
    assert!(output.contains(&field("production_certified_count", "0")));
    assert!(output.contains(&field("performance_claim_allowed", "false")));
    assert!(output.contains(&field("spark_displacement_claim_allowed", "false")));
    assert!(output.contains(&field("matrix_consuming_views_status", "planned_alignment")));
    assert!(output.contains(&field("all_rows_fallback_attempted_false", "true")));
    assert!(output.contains(&field("all_rows_external_engine_invoked_false", "true")));
    assert!(output.contains(&field("unadmitted_compute_row_count", "3")));
    assert!(output.contains(&field(
        "unadmitted_compute_rows_with_diagnostics_count",
        "3"
    )));
    assert!(output.contains(&field(
        "unadmitted_compute_rows_missing_diagnostics_count",
        "0"
    )));
}

fn assert_native_vortex_admission_summary_fields(output: &str) {
    assert!(output.contains(&field(
        "native_vortex_admission_schema_version",
        "shardloom.native_vortex_admission.v1"
    )));
    assert!(output.contains(&field(
        "native_vortex_admission_status",
        "scoped_fixture_lane_admitted"
    )));
    assert!(output.contains(&field("native_vortex_admission_lane_count", "1")));
    assert!(output.contains(&field(
        "native_vortex_admission_lane_order",
        "local_vortex_count_scalar"
    )));
    assert!(output.contains(&field(
        "native_vortex_admission_claim_gate_status",
        "fixture_smoke_only"
    )));
    assert!(output.contains(&field(
        "native_vortex_admission_universal_coverage_claim_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "native_vortex_admission_all_lanes_fallback_attempted_false",
        "true"
    )));
    assert!(output.contains(&field(
        "native_vortex_admission_all_lanes_external_engine_invoked_false",
        "true"
    )));
}

fn assert_native_unsupported_coverage_summary_fields(output: &str) {
    assert!(output.contains(&field(
        "native_unsupported_coverage_schema_version",
        "shardloom.native_unsupported_coverage.v1"
    )));
    assert!(output.contains(&field(
        "native_unsupported_coverage_status",
        "complete_for_current_matrix"
    )));
    assert!(output.contains(&field(
        "native_unsupported_coverage_category_vocabulary",
        "source,sink,operator,workload"
    )));
    assert!(output.contains(&field("native_unsupported_coverage_row_count", "22")));
    assert!(output.contains(&field("native_unsupported_coverage_source_count", "4")));
    assert!(output.contains(&field("native_unsupported_coverage_sink_count", "4")));
    assert!(output.contains(&field("native_unsupported_coverage_operator_count", "8")));
    assert!(output.contains(&field("native_unsupported_coverage_workload_count", "6")));
    assert!(output.contains(&field(
        "native_unsupported_coverage_current_matrix_complete",
        "true"
    )));
    assert!(output.contains(&field(
        "native_unsupported_coverage_all_rows_claim_gate_not_grade",
        "true"
    )));
    assert!(output.contains(&field(
        "native_unsupported_coverage_all_rows_fallback_attempted_false",
        "true"
    )));
    assert!(output.contains(&field(
        "native_unsupported_coverage_all_rows_external_engine_invoked_false",
        "true"
    )));
}

fn assert_predicate_dtype_coverage_summary_fields(output: &str) {
    assert!(output.contains(&field(
        "predicate_dtype_coverage_schema_version",
        "shardloom.predicate_dtype_coverage.v1"
    )));
    assert!(output.contains(&field(
        "predicate_dtype_coverage_status",
        "complete_for_current_matrix"
    )));
    assert!(output.contains(&field(
        "predicate_dtype_coverage_category_vocabulary",
        "predicate,dtype,null_semantics,nested_shape,statistics"
    )));
    assert!(output.contains(&field("predicate_dtype_coverage_row_count", "12")));
    assert!(output.contains(&field(
        "predicate_dtype_coverage_row_order",
        "predicate_i64_range,predicate_i64_equality,predicate_string_dictionary,predicate_boolean_counts,predicate_compound_or_not,dtype_int64,dtype_utf8_dictionary,dtype_decimal_timestamp,null_all_null_segments,null_mixed_segments,nested_field_pruning,statistics_missing_or_inexact"
    )));
    assert!(output.contains(&field("predicate_dtype_coverage_predicate_count", "5")));
    assert!(output.contains(&field("predicate_dtype_coverage_dtype_count", "3")));
    assert!(output.contains(&field("predicate_dtype_coverage_null_semantics_count", "2")));
    assert!(output.contains(&field("predicate_dtype_coverage_nested_shape_count", "1")));
    assert!(output.contains(&field("predicate_dtype_coverage_statistics_count", "1")));
    assert!(output.contains(&field(
        "predicate_dtype_coverage_fixture_certified_count",
        "2"
    )));
    assert!(output.contains(&field(
        "predicate_dtype_coverage_executable_uncertified_count",
        "2"
    )));
    assert!(output.contains(&field("predicate_dtype_coverage_fixture_needed_count", "5")));
    assert!(output.contains(&field("predicate_dtype_coverage_unsupported_count", "3")));
    assert!(output.contains(&field(
        "predicate_dtype_coverage_all_rows_have_evidence_gap",
        "true"
    )));
    assert!(output.contains(&field(
        "predicate_dtype_coverage_all_rows_fallback_attempted_false",
        "true"
    )));
    assert!(output.contains(&field(
        "predicate_dtype_coverage_all_rows_external_engine_invoked_false",
        "true"
    )));
}

fn assert_predicate_dtype_coverage_row_fields(output: &str) {
    assert!(output.contains(&field(
        "predicate_dtype_coverage_row_predicate_i64_range_support_status",
        "fixture_certified"
    )));
    assert!(output.contains(&field(
        "predicate_dtype_coverage_row_predicate_i64_range_runtime_surface",
        "metadata_pruning,prepared_vortex,native_vortex"
    )));
    assert!(output.contains(&field(
        "predicate_dtype_coverage_row_predicate_i64_range_claim_gate_status",
        "fixture_smoke_only"
    )));
    assert!(output.contains(&field(
        "predicate_dtype_coverage_row_predicate_string_dictionary_fixture_status",
        "fixture_needed"
    )));
    assert!(output.contains(&field(
        "predicate_dtype_coverage_row_dtype_decimal_timestamp_required_future_evidence",
        "timezone_semantics,decimal_scale_semantics,malformed_value_fixture"
    )));
    assert!(output.contains(&field(
        "predicate_dtype_coverage_row_null_mixed_segments_statistics_required",
        "row_count,null_count,min_value,max_value"
    )));
    assert!(output.contains(&field(
        "predicate_dtype_coverage_row_nested_field_pruning_unsupported_diagnostic_code",
        "SL_UNSUPPORTED_NESTED_FIELD_PRUNING"
    )));
    assert!(output.contains(&field(
        "predicate_dtype_coverage_row_statistics_missing_or_inexact_claim_boundary",
        "missing stats never prove absence or authorize fallback execution"
    )));
    assert!(output.contains(&field(
        "predicate_dtype_coverage_row_statistics_missing_or_inexact_fallback_attempted",
        "false"
    )));
    assert!(output.contains(&field(
        "predicate_dtype_coverage_row_statistics_missing_or_inexact_external_engine_invoked",
        "false"
    )));
}

fn assert_materialization_policy_fields(output: &str) {
    assert!(output.contains(&field(
        "materialization_policy_schema_version",
        "shardloom.materialization_policy.v1"
    )));
    assert!(output.contains(&field(
        "materialization_policy_report_id",
        "gar0003b.materialization_policy"
    )));
    assert!(output.contains(&field(
        "materialization_policy_report_ref",
        "compute-capability-matrix://materialization_policy.v1"
    )));
    assert!(output.contains(&field("materialization_policy_row_count", "4")));
    assert!(output.contains(&field(
        "materialization_policy_row_order",
        "encoded_native_operator_path,residual_native_operator_path,materialized_temporary_operator_path,unsupported_operator_path"
    )));
    assert!(output.contains(&field(
        "materialization_policy_operator_execution_classes",
        "encoded_native,residual_native,materialized_temporary,unsupported"
    )));
    assert!(output.contains(&field("materialization_policy_all_rows_classified", "true")));
    assert!(output.contains(&field(
        "materialization_policy_all_rows_fallback_attempted_false",
        "true"
    )));
    assert!(output.contains(&field(
        "materialization_policy_row_encoded_native_operator_path_stayed_encoded",
        "true"
    )));
    assert!(output.contains(&field(
        "materialization_policy_row_encoded_native_operator_path_data_decoded",
        "false"
    )));
    assert!(output.contains(&field(
        "materialization_policy_row_materialized_temporary_operator_path_data_decoded",
        "true"
    )));
    assert!(output.contains(&field(
        "materialization_policy_row_materialized_temporary_operator_path_data_materialized",
        "true"
    )));
    assert!(output.contains(&field(
        "materialization_policy_row_materialized_temporary_operator_path_encoded_native_claim_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "materialization_policy_row_unsupported_operator_path_unsupported_diagnostic_code",
        "SL_UNSUPPORTED_OPERATOR_MATERIALIZATION_POLICY"
    )));
    assert!(output.contains(&field(
        "materialization_policy_row_unsupported_operator_path_fallback_attempted",
        "false"
    )));
}

fn assert_local_vortex_count_admission_lane_fields(output: &str) {
    assert!(output.contains(&field(
        "native_vortex_admission_lane_local_vortex_count_scalar_admission_status",
        "admitted_fixture_certified"
    )));
    assert!(output.contains(&field(
        "native_vortex_admission_lane_local_vortex_count_scalar_provider_kind",
        "vortex_scan"
    )));
    assert!(output.contains(&field(
        "native_vortex_admission_lane_local_vortex_count_scalar_compute_row_ref",
        "compute_row.local_vortex_count"
    )));
    assert!(output.contains(&field(
        "native_vortex_admission_lane_local_vortex_count_scalar_claim_boundary",
        "local_count_all_fixture_smoke_only_not_universal_native_vortex"
    )));
    assert!(output.contains(&field(
        "native_vortex_admission_lane_local_vortex_count_scalar_fallback_attempted",
        "false"
    )));
    assert!(output.contains(&field(
        "native_vortex_admission_lane_local_vortex_count_scalar_external_engine_invoked",
        "false"
    )));
}

fn assert_local_vortex_count_row_fields(output: &str) {
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
}

fn assert_direct_transient_and_sql_fields(output: &str) {
    assert!(output.contains(&field(
        "compute_row_direct_compatibility_transient_support_status",
        "unsupported"
    )));
    assert!(output.contains(&field(
        "compute_row_direct_compatibility_transient_unsupported_diagnostic_code",
        "SL_UNSUPPORTED_DIRECT_COMPATIBILITY_TRANSIENT"
    )));
    assert!(output.contains(&field(
        "compute_row_direct_compatibility_transient_claim_gate_status",
        "not_claim_grade"
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
        "compute_row_sql_frontend_unsupported_diagnostic_code",
        "SL_UNSUPPORTED_SQL"
    )));
    assert!(output.contains(&field(
        "compute_row_sql_frontend_blocker_id",
        "cg21.workflow.sql.frontend_unsupported"
    )));
    assert!(output.contains(&field(
        "compute_row_sql_frontend_claim_gate_status",
        "not_claim_grade"
    )));
}

fn assert_native_unsupported_row_fields(output: &str) {
    assert!(output.contains(&field(
        "native_unsupported_coverage_row_native_source_object_store_range_unsupported_diagnostic_code",
        "SL_UNSUPPORTED_NATIVE_OBJECT_STORE_SOURCE"
    )));
    assert!(output.contains(&field(
        "native_unsupported_coverage_row_native_sink_table_catalog_commit_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&field(
        "native_unsupported_coverage_row_native_operator_joins_blocker_id",
        "cg21.workflow.join.operator_unsupported"
    )));
    assert!(output.contains(&field(
        "native_unsupported_coverage_row_native_workload_sql_dataframe_support_status",
        "unsupported"
    )));
    assert!(output.contains(&field(
        "native_unsupported_coverage_row_native_workload_sql_dataframe_fallback_attempted",
        "false"
    )));
    assert!(output.contains(&field(
        "native_unsupported_coverage_row_native_workload_best_default_claim_external_engine_invoked",
        "false"
    )));
}

#[test]
fn compute_capability_matrix_exposes_rows_families_and_claim_gates() {
    let output = run_compute_matrix_json(&["compute-capability-matrix", "--format", "json"], true);

    assert_matrix_summary_fields(&output);
    assert_matrix_claim_counts(&output);
    assert_native_vortex_admission_summary_fields(&output);
    assert_native_unsupported_coverage_summary_fields(&output);
    assert_predicate_dtype_coverage_summary_fields(&output);
    assert_materialization_policy_fields(&output);
    assert_no_runtime_no_fallback_no_effects(&output);
}

#[test]
fn compute_capability_matrix_rows_distinguish_provider_and_support_status() {
    let output = run_compute_matrix_json(&["compute-capability-matrix", "--format", "json"], true);

    assert_local_vortex_count_row_fields(&output);
    assert_local_vortex_count_admission_lane_fields(&output);
    assert_direct_transient_and_sql_fields(&output);
    assert!(output.contains(&field(
        "compute_row_vortex_sink_write_support_status",
        "report_only"
    )));
    assert!(output.contains(&field(
        "compute_row_grouped_aggregate_memory_spill_requirement",
        "hash_group_state_spill_required_before_broad_claim"
    )));
    assert!(output.contains(&field(
        "compute_row_grouped_aggregate_support_status",
        "fixture_certified"
    )));
    assert!(output.contains(&field(
        "compute_row_grouped_aggregate_operator_execution_class",
        "residual_native"
    )));
    assert!(output.contains(&field(
        "compute_row_grouped_aggregate_operator_admission_status",
        "residual_native_fixture_admitted"
    )));
    assert!(output.contains(&field(
        "compute_row_grouped_aggregate_operator_encoded_native_claim_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "compute_row_join_operator_execution_class",
        "residual_native"
    )));
    assert!(output.contains(&field(
        "compute_row_window_row_number_operator_execution_class",
        "residual_native"
    )));
    assert_native_unsupported_row_fields(&output);
    assert_predicate_dtype_coverage_row_fields(&output);
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
