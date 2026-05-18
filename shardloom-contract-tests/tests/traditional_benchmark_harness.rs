use std::fs;
use std::path::PathBuf;

#[test]
fn traditional_benchmark_harness_lists_all_required_engines() {
    let script = read_workspace_file("benchmarks/traditional_analytics/run.py");

    assert!(script.contains("\"spark-default\""));
    assert!(script.contains("\"spark-local-tuned\""));
    assert!(script.contains("\"shardloom-prepared-vortex\""));
    assert!(
        script.contains("ENGINE_ALIASES = {\"spark\": (\"spark-default\", \"spark-local-tuned\")}")
    );
    assert!(script.contains("\"fallback_execution_allowed\": False"));
    assert!(script.contains("\"external_engines_are_fallback\": False"));
    assert!(script.contains("\"performance_claim_allowed\": False"));
    assert!(script.contains("def render_markdown_report("));
    assert!(script.contains("def render_fairness_parameters("));
    assert!(script.contains("def render_read_this_first("));
    assert!(script.contains("def render_shardloom_native_table("));
    assert!(script.contains("def render_shardloom_why_table("));
    assert!(script.contains("def render_shardloom_work_avoidance_table("));
    assert!(script.contains("def render_shardloom_commit_table("));
    assert!(script.contains("def render_universal_io_table("));
    assert!(script.contains("def render_resource_metrics_table("));
    assert!(script.contains("def render_shardloom_effects_table("));
    assert!(script.contains("def render_coverage_table("));
    assert!(script.contains("def warmup_runner("));
    assert!(script.contains("\"startup_time_millis\""));
    assert!(script.contains("\"bytes_written\""));
    assert!(script.contains("\"shardloom_evidence\""));
    assert!(script.contains("\"native_io_certificate_status\""));
    assert!(script.contains("\"native_io_certificate_path_id\""));
    assert!(script.contains("\"native_io_per_path_certificate_emitted\""));
    assert!(script.contains("\"native_io_materializing_transitions_have_boundaries\""));
    assert!(script.contains("SHARDLOOM_CLAIM_GRADE_REQUIRED_EVIDENCE"));
    assert!(script.contains("def claim_grade_readiness("));
    assert!(script.contains("\"claim_gate_status\""));
    assert!(script.contains("\"claim_grade_requirements_met\""));
    assert!(script.contains("\"claim_grade_missing_evidence\""));
    assert!(script.contains("\"timing_row_claim_grade\""));
    assert!(script.contains("\"write_timing_present\""));
    assert!(script.contains("\"computed_result_sink_write_millis\""));
    assert!(script.contains("\"scenario_compute_millis\""));
    assert!(script.contains("\"total_runtime_millis\""));
    assert!(script.contains("\"operator_compute_millis\""));
    assert!(script.contains("\"requested_execution_mode\""));
    assert!(script.contains("\"selected_execution_mode\""));
    assert!(script.contains("\"mode_selection_reason\""));
    assert!(script.contains("\"execution_mode_family\""));
    assert!(script.contains("\"preparation_millis\""));
    assert!(script.contains("\"preparation_included_in_timing\""));
    assert!(script.contains("\"prepared_artifact_ref\""));
    assert!(script.contains("\"prepared_artifact_digest\""));
    assert!(script.contains("\"source_read_millis\""));
    assert!(script.contains("\"compatibility_parse_millis\""));
    assert!(script.contains("\"compatibility_to_vortex_import_millis\""));
    assert!(script.contains("\"vortex_write_millis\""));
    assert!(script.contains("\"vortex_reopen_millis\""));
    assert!(script.contains("\"vortex_scan_millis\""));
    assert!(script.contains("\"evidence_render_millis\""));
    assert!(script.contains("\"fusion_status\""));
    assert!(script.contains("\"filter_project_limit_fused\""));
    assert!(script.contains("\"fusion_blocker\""));
    assert!(script.contains("FUSED_PIPELINE_FIELDS"));
    assert!(script.contains("\"fused_pipeline_schema_version\""));
    assert!(script.contains("\"fused_pipeline_report_id\""));
    assert!(script.contains("\"fused_pipeline_scope\""));
    assert!(script.contains("\"fused_pipeline_planned_family_count\""));
    assert!(script.contains("\"fused_pipeline_family_statuses\""));
    assert!(script.contains("\"fused_pipeline_used\""));
    assert!(script.contains("\"fused_operator_family\""));
    assert!(script.contains("\"intermediate_materialization_avoided\""));
    assert!(script.contains("\"fused_pipeline_rows_scanned\""));
    assert!(script.contains("\"fused_pipeline_rows_selected\""));
    assert!(script.contains("\"fused_pipeline_rows_output\""));
    assert!(script.contains("\"fused_pipeline_filter_columns\""));
    assert!(script.contains("\"fused_pipeline_projection_columns\""));
    assert!(script.contains("\"fused_pipeline_correctness_digest_status\""));
    assert!(script.contains("\"fused_pipeline_unfused_correctness_digest\""));
    assert!(script.contains("\"fused_pipeline_fused_correctness_digest\""));
    assert!(script.contains("\"fused_pipeline_correctness_digest_match\""));
    assert!(script.contains("\"fused_pipeline_unfused_reference_status\""));
    assert!(script.contains("\"fused_pipeline_operator_execution_class\""));
    assert!(script.contains("\"fused_pipeline_encoded_native_claim_allowed\""));
    assert!(script.contains("\"fused_pipeline_blocker_id\""));
    assert!(script.contains("\"fused_pipeline_blocker_reason\""));
    assert!(script.contains("\"fused_pipeline_claim_boundary\""));
    assert!(script.contains("COMPRESSED_KERNEL_REGISTRY_FIELDS"));
    assert!(script.contains("\"compressed_kernel_registry_schema_version\""));
    assert!(script.contains("\"compressed_kernel_registry_pair_ids\""));
    assert!(script.contains("\"compressed_kernel_registry_pair_statuses\""));
    assert!(script.contains("\"compressed_kernel_registry_encoding_ids\""));
    assert!(script.contains("\"compressed_kernel_registry_kernel_admitted\""));
    assert!(script.contains("\"compressed_kernel_registry_kernel_executed\""));
    assert!(script.contains("\"compressed_kernel_registry_decoded\""));
    assert!(script.contains("\"compressed_kernel_registry_materialized\""));
    assert!(script.contains("\"compressed_kernel_registry_selection_vector_emitted\""));
    assert!(script.contains("\"compressed_kernel_registry_encoded_native_claim_allowed\""));
    assert!(script.contains("\"compressed_kernel_registry_fallback_attempted\""));
    assert!(script.contains("\"compressed_kernel_registry_external_engine_invoked\""));
    assert!(script.contains("SCAN_PUSHDOWN_FIELDS"));
    assert!(script.contains("\"scan_pushdown_schema_version\""));
    assert!(script.contains("\"scan_pushdown_status\""));
    assert!(script.contains("\"scan_filter_pushed_down\""));
    assert!(script.contains("\"scan_projection_pushed_down\""));
    assert!(script.contains("\"scan_limit_pushed_down\""));
    assert!(script.contains("\"scan_filter_columns_read\""));
    assert!(script.contains("\"scan_output_columns_read\""));
    assert!(script.contains("\"scan_filter_only_columns_read\""));
    assert!(script.contains("\"materialization_required\""));
    assert!(script.contains("\"decode_required\""));
    assert!(script.contains("\"scan_api_status\""));
    assert!(script.contains("\"persistent_runner_status\""));
    assert!(script.contains("SESSION_RUNTIME_FIELDS"));
    assert!(script.contains("\"session_schema_version\""));
    assert!(script.contains("\"session_id\""));
    assert!(script.contains("\"session_runtime_status\""));
    assert!(script.contains("\"session_state_scope\""));
    assert!(script.contains("\"session_close_status\""));
    assert!(script.contains("\"session_prepared_artifact_cache_hit_count\""));
    assert!(script.contains("\"session_prepared_artifact_reuse_count\""));
    assert!(script.contains("\"session_source_state_reuse_count\""));
    assert!(script.contains("\"session_hidden_global_cache\""));
    assert!(script.contains("\"session_daemon_or_service\""));
    assert!(script.contains("\"session_fallback_attempted\""));
    assert!(script.contains("\"session_external_engine_invoked\""));
    assert!(script.contains("ALLOCATION_RESOURCE_PROFILE_FIELDS"));
    assert!(script.contains("\"allocation_profile_schema_version\""));
    assert!(script.contains("\"allocation_profile_status\""));
    assert!(script.contains("\"allocation_profile_scope\""));
    assert!(script.contains("\"allocation_profile_family_status\""));
    assert!(script.contains("\"allocation_count\""));
    assert!(script.contains("\"allocation_count_status\""));
    assert!(script.contains("\"allocation_bytes\""));
    assert!(script.contains("\"allocation_bytes_status\""));
    assert!(script.contains("\"buffer_pool_enabled\""));
    assert!(script.contains("\"buffer_pool_scope\""));
    assert!(script.contains("\"buffer_reuse_count\""));
    assert!(script.contains("\"buffer_reuse_family\""));
    assert!(script.contains("\"buffer_reuse_blocker\""));
    assert!(script.contains("\"peak_rss_delta\""));
    assert!(script.contains("\"peak_rss_delta_status\""));
    assert!(script.contains("\"evidence_regression_status\""));
    assert!(script.contains("\"unsafe_lifetime_shortcut_used\""));
    assert!(script.contains("\"allocation_fallback_attempted\""));
    assert!(script.contains("\"allocation_external_engine_invoked\""));
    assert!(script.contains("\"allocation_claim_gate_status\""));
    assert!(script.contains("\"allocation_claim_boundary\""));
    assert!(script.contains("RUNTIME_EVIDENCE_LEVEL_FIELDS"));
    assert!(script.contains("\"runtime_evidence_level_schema_version\""));
    assert!(script.contains("\"requested_evidence_level\""));
    assert!(script.contains("\"selected_evidence_level\""));
    assert!(script.contains("\"evidence_level\""));
    assert!(script.contains("\"evidence_level_supported_levels\""));
    assert!(script.contains("\"evidence_level_claim_gate_status\""));
    assert!(script.contains("\"evidence_level_result_sink_replay_required\""));
    assert!(script.contains("\"evidence_level_result_sink_replay_requested\""));
    assert!(script.contains("\"evidence_level_result_sink_replay_verified\""));
    assert!(script.contains("\"evidence_level_native_io_certificate_required\""));
    assert!(script.contains("\"evidence_level_certificate_refs\""));
    assert!(script.contains("\"evidence_level_result_sink_replay_refs\""));
    assert!(script.contains("\"evidence_level_source_state_digest\""));
    assert!(script.contains("\"evidence_level_output_digest\""));
    assert!(script.contains("\"evidence_level_fallback_attempted\""));
    assert!(script.contains("\"evidence_level_external_engine_invoked\""));
    assert!(script.contains("\"evidence_level_claim_boundary\""));
    assert!(script.contains("OPTIMIZER_TRACE_FIELDS"));
    assert!(script.contains("\"optimizer_trace_schema_version\""));
    assert!(script.contains("\"optimizer_trace_id\""));
    assert!(script.contains("\"optimizer_registry_version\""));
    assert!(script.contains("\"optimizer_rule_status_vocabulary\""));
    assert!(script.contains("\"optimizer_rule_order\""));
    assert!(script.contains("\"optimizer_rule_statuses\""));
    assert!(script.contains("\"optimizer_rule_applied_count\""));
    assert!(script.contains("\"optimizer_before_plan_digest_status\""));
    assert!(script.contains("\"optimizer_after_plan_digest_status\""));
    assert!(script.contains("\"optimizer_rewrite_safety_status\""));
    assert!(script.contains("\"optimizer_evidence_preserved\""));
    assert!(script.contains("\"optimizer_no_fallback_preserved\""));
    assert!(script.contains("\"optimizer_claim_boundary_preserved\""));
    assert!(script.contains("\"optimizer_materialization_boundary_preserved\""));
    assert!(script.contains("\"optimizer_source_state_reuse_admitted\""));
    assert!(script.contains("\"optimizer_fallback_attempted\""));
    assert!(script.contains("\"optimizer_external_engine_invoked\""));
    assert!(script.contains("\"optimizer_claim_gate_status\""));
    assert!(script.contains("\"optimizer_benchmark_trace_ref\""));
    assert!(script.contains("--shardloom-evidence-level"));
    assert!(script.contains("--evidence-level"));
    assert!(script.contains("\"reproducible_benchmark_row\""));
    assert!(script.contains("\"correctness_digest_stable\""));
    assert!(script.contains("MIN_CLAIM_GRADE_ITERATIONS = 3"));
    assert!(script.contains("--claim-readiness-rerun"));
    assert!(script.contains("allow_abbrev=False"));
    assert!(script.contains("def render_shardloom_result_sink_table("));
    assert!(script.contains("\"fixture_smoke_only\""));
    assert!(script.contains("\"not_claim_grade\""));
    assert!(script.contains("\"claim_grade\""));
    assert!(script.contains("ROW_CLASSIFICATIONS"));
    assert!(script.contains("\"row_classification\""));
    assert!(script.contains("\"support_status\""));
    assert!(script.contains("\"supported\""));
    assert!(script.contains("\"benchmark_row_ref\""));
    assert!(script.contains("\"coverage_row_ref\""));
    assert!(script.contains("\"native_io_source_sink_coverage_ref\""));
    assert!(script.contains("NATIVE_IO_SOURCE_SINK_COVERAGE_REF"));
    assert!(script.contains("\"vortex_source_split_admission_ref\""));
    assert!(script.contains("VORTEX_SOURCE_SPLIT_ADMISSION_REF"));
    assert!(script.contains("\"vortex_segment_extraction_admission_ref\""));
    assert!(script.contains("VORTEX_SEGMENT_EXTRACTION_ADMISSION_REF"));
    assert!(script.contains("\"vortex_layout_device_managed_boundary_ref\""));
    assert!(script.contains("VORTEX_LAYOUT_DEVICE_MANAGED_BOUNDARY_REF"));
    assert!(script.contains("\"materialization_policy_ref\""));
    assert!(script.contains("MATERIALIZATION_POLICY_REF"));
    assert!(script.contains("\"execution_certificate_status\""));
    assert!(script.contains("\"source_native_io_certificate_status\""));
    assert!(script.contains("\"result_native_io_certificate_status\""));
    assert!(script.contains("\"materialization_decode_evidence_present\""));
    assert!(script.contains("def direct_transient_admission_coverage_row("));
    assert!(script.contains("def shardloom_direct_transient_runner("));
    assert!(script.contains("\"shardloom-direct-transient\""));
    assert!(script.contains("\"filter + projection + limit\": lambda paths, data_format"));
    assert!(script.contains("\"direct_transient_local_csv_smoke\""));
    assert!(script.contains("direct_transient_csv_filter_projection_limit"));
    assert!(script.contains("\"direct_transient_no_vortex_scan\""));
    assert!(script.contains("\"direct_compatibility_transient\""));
    assert!(script.contains("\"direct_compatibility_transient_not_implemented\""));
    assert!(script.contains(
        "direct transient smoke currently supports only selective filter or filter + projection + limit"
    ));
    assert!(script.contains("def support_status("));
    assert!(script.contains("def materialization_decode_evidence_present("));
    assert!(script.contains("SHARDLOOM_EXECUTION_MODE_VOCABULARY"));
    assert!(script.contains("EXECUTION_MODE_CONTRACT_FIELDS"));
    assert!(script.contains("STAGE_TIMING_CONTRACT_FIELDS"));
    assert!(script.contains("OPERATOR_BLOCKER_MATRIX_FIELDS"));
    assert!(script.contains("def operator_blocker_metadata("));
    assert!(script.contains("def execution_mode_attribution_contract("));
    assert!(script.contains("def validate_result_attribution_contract("));
    assert!(script.contains("def render_execution_mode_attribution_contract("));
    assert!(script.contains("def render_persistent_runner_admission_gate("));
    assert!(script.contains("\"execution_mode_attribution_contract\""));
    assert!(script.contains("\"shardloom.execution_mode_benchmark_attribution.v1\""));
    assert!(script.contains("\"persistent_runner_admission_gate\""));
    assert!(script.contains("\"gar-flow-2c.persistent_runner_admission.v1\""));
    assert!(script.contains("PERSISTENT_RUNNER_ADMISSION_FIELDS"));
    assert!(script.contains("\"hidden_fast_mode_allowed\": False"));
    assert!(script.contains("\"persistent_runner_admitted\": False"));
    assert!(script.contains("WORK_AVOIDANCE_STATUS_VOCABULARY"));
    assert!(script.contains("WORK_AVOIDANCE_EVIDENCE_FIELDS"));
    assert!(script.contains("def work_avoidance_evidence_schema("));
    assert!(script.contains("\"work_avoidance_evidence_schema\""));
    assert!(script.contains("\"gar-flow-2d.work_avoidance_evidence.v1\""));
    assert!(script.contains("\"not_available\""));
    assert!(script.contains("\"not_applicable\""));
    assert!(script.contains("\"operator_execution_class\""));
    assert!(script.contains("\"operator_blocker_id\""));
    assert!(script.contains("\"operator_encoded_native_claim_allowed\""));
    assert!(script.contains("CORRECTNESS_FLOAT_DIGITS = 4"));
    assert!(script.contains("\"status\", \"--short\", \"--untracked-files=no\""));
    assert!(script.contains("traditional-analytics-run"));
    assert!(script.contains("vortex-traditional-analytics-benchmark"));
    assert!(script.contains("--shardloom-build-profile"));
    assert!(script.contains("failed_result("));
    assert!(script.contains("\"very_wide_table\""));
    assert!(script.contains("\"null_heavy\""));
    assert!(script.contains("\"many_small_files\""));
    assert!(script.contains("\"few_large_files\""));
    assert!(script.contains("\"partitioned_by_date\""));
    assert!(script.contains("\"poorly_clustered\""));
    assert!(script.contains("\"well_clustered\""));
    assert!(script.contains("\"schema_drift\""));
    assert!(script.contains("\"dirty_csv\""));
    assert!(script.contains("\"nested_json\""));
    assert!(script.contains("\"cdc_delta_overlay\""));
    assert!(script.contains("\"dataset_file_shape\""));
    assert!(script.contains("\"fact_csv_part_count\""));
    assert!(script.contains("\"cdc_delta_csv\""));
    assert!(script.contains("\"--cdc-delta-vortex\""));
    assert!(script.contains("\"cdc_delta_preparation_millis\""));
    assert!(script.contains("\"nested_jsonl\""));
    assert!(script.contains("def generated_fact_extra_columns("));
    assert!(script.contains("def generated_extra_fact_values("));
    assert!(script.contains("def write_profile_sidecars("));
    assert!(script.contains("def write_csv_parts("));
    assert!(script.contains("def write_cdc_delta_overlay("));
    assert!(script.contains("def write_nested_json_fixture("));
    assert!(script.contains("def clean_cast_filter_write("));
    assert!(script.contains("scenario_outputs"));
    assert!(script.contains("def scenario_dataset_profile_block_reason("));
    assert!(script.contains("requires dataset_profile in"));
    assert!(script.contains("SHARDLOOM_TRADITIONAL_SCENARIOS"));
    assert!(script.contains("def shardloom_prepared_vortex_runner("));
    assert!(script.contains("does not implement benchmark scenario"));
}

#[test]
fn traditional_benchmark_harness_covers_core_and_stress_scenarios() {
    let script = read_workspace_file("benchmarks/traditional_analytics/run.py");

    for scenario in [
        "csv/file ingest",
        "selective filter",
        "filter + projection + limit",
        "group by aggregation",
        "multi-key group by",
        "sort and top-k",
        "hash join",
        "join + aggregate",
        "row number window",
        "partition pruning",
        "many-small-files scan",
        "null-heavy aggregate",
        "high-cardinality string group/distinct",
        "top-N per group",
        "clean/cast/filter/write",
        "malformed timestamp / dirty CSV",
        "small change over large base",
        "nested JSON field scan",
        "wide projection",
        "distinct count",
        "scale stress skewed join aggregation",
        "scale stress multi-stage etl",
    ] {
        assert!(script.contains(scenario), "missing scenario {scenario}");
    }
    assert!(script.contains("--include-stress"));
    assert!(script.contains("--include-taxonomy-extra"));
    assert!(script.contains("scenario_catalog.json"));
}

#[test]
fn traditional_benchmark_harness_records_fairness_and_universal_io_boundaries() {
    let script = read_workspace_file("benchmarks/traditional_analytics/run.py");

    for required_text in [
        "\"cache_mode\"",
        "\"timing_scope\"",
        "\"benchmark_suite\"",
        "\"scenario_id\"",
        "\"scenario_category\"",
        "\"dataset_profile\"",
        "\"engine_role\"",
        "\"benchmark_constitution\"",
        "\"coverage_table\"",
        "\"claim_gate_status\"",
        "\"row_classification\"",
        "\"support_status\"",
        "\"claim_grade_requirements_met\"",
        "\"claim_grade_missing_evidence\"",
        "\"timing_row_claim_grade\"",
        "\"write_timing_present\"",
        "\"computed_result_sink_write_millis\"",
        "\"scenario_compute_millis\"",
        "\"selected_execution_mode\"",
        "\"prepared_vortex\"",
        "\"compatibility_import_certified\"",
        "\"preparation_millis\"",
        "\"prepared_artifact_digest\"",
        "\"vortex_reopen_scan_included\"",
        "\"build_time_excluded\"",
        "\"reproducible_benchmark_row\"",
        "\"correctness_digest_stable\"",
        "\"claim_readiness_rerun_profile\"",
        "\"taxonomy_extra_included\"",
        "\"stress_lane_included\"",
        "\"shardloom_build_profile\"",
        "\"shardloom_build_time_excluded\"",
        "\"fused_pipeline_schema_version\"",
        "\"fused_pipeline_report_id\"",
        "\"fused_pipeline_scope\"",
        "\"fused_pipeline_family_statuses\"",
        "\"fused_pipeline_used\"",
        "\"fused_operator_family\"",
        "\"intermediate_materialization_avoided\"",
        "\"fused_pipeline_rows_scanned\"",
        "\"fused_pipeline_rows_selected\"",
        "\"fused_pipeline_rows_output\"",
        "\"fused_pipeline_correctness_digest_status\"",
        "\"fused_pipeline_unfused_correctness_digest\"",
        "\"fused_pipeline_fused_correctness_digest\"",
        "\"fused_pipeline_correctness_digest_match\"",
        "\"fused_pipeline_encoded_native_claim_allowed\"",
        "\"fused_pipeline_blocker_id\"",
        "\"fused_pipeline_blocker_reason\"",
        "\"compressed_kernel_registry_schema_version\"",
        "\"compressed_kernel_registry_pair_ids\"",
        "\"compressed_kernel_registry_pair_statuses\"",
        "\"compressed_kernel_registry_kernel_admitted\"",
        "\"compressed_kernel_registry_encoded_native_claim_allowed\"",
        "\"compressed_kernel_registry_fallback_attempted\"",
        "\"compressed_kernel_registry_external_engine_invoked\"",
        "\"scan_pushdown_schema_version\"",
        "\"scan_pushdown_status\"",
        "\"scan_filter_pushed_down\"",
        "\"scan_projection_pushed_down\"",
        "\"scan_limit_pushed_down\"",
        "\"scan_filter_columns_read\"",
        "\"scan_output_columns_read\"",
        "\"scan_filter_only_columns_read\"",
        "\"source_metadata_snapshot_status\"",
        "\"source_metadata_snapshot_reused\"",
        "\"source_metadata_digest_recompute_avoided_count\"",
        "\"source_state_reuse_status\"",
        "\"source_state_coverage_schema_version\"",
        "\"source_state_coverage_matrix_ref\"",
        "\"source_state_coverage_status_vocabulary\"",
        "\"source_state_coverage_all_requested_scenarios_classified\"",
        "\"source_state_coverage_matrix\"",
        "\"source_state_digest_status\"",
        "\"source_state_family_count\"",
        "\"source_state_dimension_label_reuse_status\"",
        "\"source_state_category_metric_reuse_status\"",
        "\"source_state_group_category_metric_reuse_status\"",
        "\"source_state_ranked_metric_reuse_status\"",
        "\"source_state_selective_filter_reuse_status\"",
        "\"source_state_dirty_input_reuse_status\"",
        "\"source_state_date_null_metric_reuse_status\"",
        "\"source_state_prepare_micros\"",
        "\"source_state_prepare_timing_scope\"",
        "\"source_state_contract\"",
        "\"source_state_matrix\"",
        "\"shardloom.traditional_analytics.source_state.v1\"",
        "\"source_state_contract_schema_version\"",
        "\"source_state_status_vocabulary\"",
        "\"source_state_status\"",
        "\"source_state_id\"",
        "\"source_state_digest\"",
        "\"source_format\"",
        "\"source_location\"",
        "\"source_fingerprint_kind\"",
        "\"schema_digest\"",
        "\"row_count_known\"",
        "\"file_count\"",
        "\"byte_size\"",
        "\"partition_columns\"",
        "\"compression\"",
        "\"source_state_reuse_allowed\"",
        "\"source_discovery_millis\"",
        "\"schema_inference_millis\"",
        "\"source_parse_millis\"",
        "\"parse_decode_plan_digest\"",
        "\"source_state_reuse_hit\"",
        "\"source_state_reuse_reason\"",
        "\"source_state_materialization_decode_boundary_ref\"",
        "\"source_state_fallback_attempted\"",
        "\"source_state_external_engine_invoked\"",
        "\"source_state_claim_gate_status\"",
        "\"source_state_claim_boundary\"",
        "SOURCE_STATE_CONTRACT_FIELDS",
        "source_state_reuse_supported",
        "not_needed",
        "external_baseline_only",
        "def render_source_state_contract(",
        "def render_source_state_matrix(",
        "\"prepared_state_contract\"",
        "\"prepared_state_matrix\"",
        "\"shardloom.traditional_analytics.vortex_prepared_state.v1\"",
        "\"prepared_state_contract_schema_version\"",
        "\"prepared_state_status_vocabulary\"",
        "\"prepared_state_status\"",
        "\"prepared_state_id\"",
        "\"prepared_state_digest\"",
        "\"prepared_state_source_state_id\"",
        "\"vortex_artifact_ref\"",
        "\"vortex_artifact_digest\"",
        "\"layout_summary\"",
        "\"encoding_summary\"",
        "\"statistics_summary\"",
        "\"prepared_state_reuse_allowed\"",
        "\"prepared_state_reuse_hit\"",
        "\"prepared_state_reuse_reason\"",
        "\"vortex_prepare_millis\"",
        "\"prepared_state_native_io_certificate_status\"",
        "\"prepared_state_fallback_attempted\"",
        "\"prepared_state_external_engine_invoked\"",
        "\"prepared_state_claim_gate_status\"",
        "\"prepared_state_claim_boundary\"",
        "PREPARED_STATE_CONTRACT_FIELDS",
        "prepared_state_reuse_supported",
        "def render_prepared_state_contract(",
        "def render_prepared_state_matrix(",
        "\"output_plan_contract\"",
        "\"output_plan_matrix\"",
        "\"shardloom.traditional_analytics.output_plan.v1\"",
        "\"output_plan_contract_schema_version\"",
        "\"output_plan_status_vocabulary\"",
        "\"output_plan_status\"",
        "\"output_plan_id\"",
        "\"output_plan_digest\"",
        "\"output_format\"",
        "\"output_location\"",
        "\"output_schema_digest\"",
        "\"output_partitioning\"",
        "\"output_compression\"",
        "\"output_encoding\"",
        "\"output_write_mode\"",
        "\"output_plan_reuse_allowed\"",
        "\"output_metadata_preservation_status\"",
        "\"output_materialization_required\"",
        "\"output_plan_reuse_hit\"",
        "\"output_plan_reuse_reason\"",
        "\"output_plan_millis\"",
        "\"output_write_millis\"",
        "\"result_replay_verified\"",
        "\"output_native_io_certificate_status\"",
        "\"sink_artifact_ref\"",
        "\"sink_artifact_digest\"",
        "\"output_plan_fallback_attempted\"",
        "\"output_plan_external_engine_invoked\"",
        "\"output_plan_claim_gate_status\"",
        "\"output_plan_claim_boundary\"",
        "OUTPUT_PLAN_CONTRACT_FIELDS",
        "output_plan_supported",
        "def render_output_plan_contract(",
        "def render_output_plan_matrix(",
        "\"fanout_benchmark_contract\"",
        "\"fanout_benchmark_matrix\"",
        "\"shardloom.traditional_analytics.io_reuse_and_fanout.v1\"",
        "\"benchmark_family\"",
        "\"fanout_case_id\"",
        "\"source_format\"",
        "\"requested_output_formats\"",
        "\"currently_proven_output_formats\"",
        "\"blocked_output_formats\"",
        "\"fanout_status\"",
        "\"fanout_blocker_id\"",
        "\"fanout_blocker_reason\"",
        "\"fanout_output_count\"",
        "FANOUT_BENCHMARK_FIELDS",
        "FANOUT_BENCHMARK_CASES",
        "csv_to_parquet_jsonl_vortex_outputs",
        "prepared_vortex_to_multiple_output_formats",
        "def render_fanout_benchmark_contract(",
        "def render_fanout_benchmark_matrix(",
        "\"cache_invalidation_contract\"",
        "\"cache_invalidation_matrix\"",
        "\"shardloom.traditional_analytics.cache_invalidation.v1\"",
        "\"cache_invalidation_status\"",
        "\"cache_invalidation_layer_scope\"",
        "\"source_content_digest\"",
        "\"source_mtime\"",
        "\"source_size\"",
        "\"object_etag\"",
        "\"manifest_version\"",
        "\"plan_digest\"",
        "\"cache_valid\"",
        "\"invalidation_reason\"",
        "\"cache_invalidation_redaction_status\"",
        "CACHE_INVALIDATION_CONTRACT_FIELDS",
        "def cache_invalidation_contract_metadata(",
        "def render_cache_invalidation_contract(",
        "def render_cache_invalidation_matrix(",
        "\"reuse_level_contract\"",
        "\"reuse_level_matrix\"",
        "\"shardloom.traditional_analytics.evidence_safe_reuse_levels.v1\"",
        "\"reuse_level\"",
        "\"reuse_status\"",
        "\"reuse_hit\"",
        "\"reuse_digest\"",
        "\"reuse_allowed\"",
        "\"reuse_blocker\"",
        "\"claim_grade_requirements_met\"",
        "REUSE_LEVEL_CONTRACT_FIELDS",
        "def reuse_level_contract_metadata(",
        "def render_reuse_level_contract(",
        "def render_reuse_level_matrix(",
        "\"runtime_evidence_level_schema_version\"",
        "\"evidence_level\"",
        "\"evidence_level_result_sink_replay_required\"",
        "\"evidence_level_result_sink_replay_verified\"",
        "\"evidence_level_fallback_attempted\"",
        "\"evidence_level_external_engine_invoked\"",
        "\"dask_blocksize\"",
        "\"dask_scheduler\"",
        "\"spark_requires_java\"",
        "\"spark_profiles\"",
        "\"startup_time_millis\"",
        "\"bytes_written\"",
        "\"data_decoded\"",
        "\"data_materialized\"",
        "\"object_store_included\": False",
        "\"compatibility_to_vortex_included\": True",
        "\"resource_auto_sizing_enabled\"",
        "\"partitioning_auto_derived\"",
        "\"target_partition_count\"",
        "\"shardloom_universal_io_smoke_included\": True",
        "\"claim_grade_requirements\"",
        "\"benchmark_row_ref\"",
        "\"coverage_row_ref\"",
        "\"native_io_source_sink_coverage_ref\"",
        "\"vortex_source_split_admission_ref\"",
        "\"vortex_segment_extraction_admission_ref\"",
        "\"vortex_layout_device_managed_boundary_ref\"",
        "\"materialization_policy_ref\"",
        "\"execution_certificate_status\"",
        "\"result_native_io_certificate_status\"",
        "\"materialization_decode_evidence_present\"",
        "CSV/JSONL/Parquet/Arrow IPC/Avro/ORC -> NativeWorkStream -> Vortex",
        "Compatibility source -> Vortex import -> encoded CountAll",
        "NativeIoCertificate",
        "SourceCapabilityReport",
        "AdapterFidelityReport",
        "MaterializationBoundaryReport",
    ] {
        assert!(
            script.contains(required_text),
            "missing required benchmark fairness text: {required_text}"
        );
    }
}

#[test]
fn traditional_benchmark_catalog_declares_taxonomy_and_planned_profiles() {
    let catalog = read_workspace_file("benchmarks/common/scenario_catalog.json");

    for required_text in [
        "\"schema_version\": \"shardloom.benchmark.scenario_catalog.v1\"",
        "\"local_analytics\"",
        "\"scan_and_pruning\"",
        "\"projection_and_layout\"",
        "\"aggregation\"",
        "\"joins\"",
        "\"sort_and_window\"",
        "\"etl_write\"",
        "\"messy_lakehouse_data\"",
        "\"incremental_state\"",
        "\"tiny_smoke\"",
        "\"narrow_fact_dim\"",
        "\"skewed_keys\"",
        "\"wide_table\"",
        "\"very_wide_table\"",
        "\"null_heavy\"",
        "\"partitioned_by_date\"",
        "\"poorly_clustered\"",
        "\"well_clustered\"",
        "\"many_small_files\"",
        "\"few_large_files\"",
        "\"schema_drift\"",
        "\"dirty_csv\"",
        "\"nested_json\"",
        "\"cdc_delta_overlay\"",
        "\"filter + projection + limit\"",
        "\"multi-key group by\"",
        "\"join + aggregate\"",
        "\"row number window\"",
        "\"partition pruning\"",
        "\"many-small-files scan\"",
        "\"null-heavy aggregate\"",
        "\"high-cardinality string group/distinct\"",
        "\"top-N per group\"",
        "\"clean/cast/filter/write\"",
        "\"malformed timestamp / dirty CSV\"",
        "\"small change over large base\"",
        "\"nested JSON field scan\"",
        "\"executable\": true",
        "\"Photon\"",
        "\"Microsoft Fabric\"",
        "\"Snowflake\"",
    ] {
        assert!(
            catalog.contains(required_text),
            "missing required benchmark catalog text: {required_text}"
        );
    }
}

#[test]
fn traditional_benchmark_harness_includes_shardloom_native_microbenchmark() {
    let script = read_workspace_file("benchmarks/traditional_analytics/run.py");

    assert!(script.contains("def run_shardloom_native_microbenchmarks("));
    assert!(script.contains("def run_shardloom_count_microbenchmark("));
    assert!(script.contains("def run_shardloom_vortex_run_microbenchmark("));
    assert!(script.contains("NATIVE_MICROBENCHMARK_SCHEMA_VERSION"));
    assert!(script.contains("\"shardloom.traditional_analytics.native_microbenchmark.v1\""));
    assert!(script.contains("\"native_microbenchmark_schema_version\""));
    assert!(script.contains("\"benchmark_category\""));
    assert!(script.contains("\"native_microbenchmark\""));
    assert!(script.contains("\"native_microbenchmark_primitive_family\""));
    assert!(script.contains("\"native_microbenchmark_support_status\""));
    assert!(script.contains("\"native_microbenchmark_subsystem\""));
    assert!(script.contains("\"native_microbenchmark_optimization_question\""));
    assert!(script.contains("\"native_microbenchmark_claim_boundary\""));
    assert!(script.contains("\"external_engine_invoked\""));
    assert!(script.contains("\"unsupported_reason\""));
    assert!(script.contains("\"rows_scanned\""));
    assert!(script.contains("\"rows_selected\""));
    assert!(script.contains("\"rows_materialized\""));
    assert!(script.contains("vortex_scan_only"));
    assert!(script.contains("filter_predicate_only"));
    assert!(script.contains("projection_only"));
    assert!(script.contains("group_by_kernel"));
    assert!(script.contains("hash_join_kernel"));
    assert!(script.contains("top_k"));
    assert!(script.contains("result_sink_write"));
    assert!(script.contains("evidence_render"));
    assert!(script.contains("def native_microbenchmark_blocked_row("));
    assert!(script.contains("def append_required_native_microbenchmark_blockers("));
    assert!(script.contains("def run_native_evidence_render_microbenchmark("));
    assert!(script.contains("def first_meaningful_field("));
    assert!(script.contains("vortex-count-benchmark"));
    assert!(script.contains("\"native_vortex_admission_lane_ref\""));
    assert!(script.contains("\"native_vortex_admission_status\""));
    assert!(script.contains("\"native_vortex_admission_claim_boundary\""));
    assert!(script.contains("vortex-run"));
    assert!(script.contains("metadata_footer_u64_20000.vortex"));
    assert!(script.contains("local primitive projection"));
    assert!(script.contains("local primitive count"));
    assert!(script.contains("\"work_avoided_metrics\""));
    assert!(script.contains("\"work_avoided_decode_avoided\""));
    assert!(script.contains("\"work_avoided_materialization_avoided\""));
    assert!(script.contains("\"work_avoided_segments_pruned\""));
    assert!(script.contains("\"work_avoided_bytes_not_read\""));
    assert!(script.contains("\"why_primary_reason\""));
    assert!(script.contains("\"why_blockers\""));
    assert!(script.contains("\"decision_trace_entries\""));
    assert!(script.contains("ShardLoom Decision / Why Evidence"));
    assert!(script.contains("ShardLoom Work-Avoidance Evidence"));
    assert!(script.contains("def run_shardloom_commit_microbenchmark("));
    assert!(script.contains("def prepare_shardloom_commit_workspace("));
    assert!(script.contains("local commit manifest"));
    assert!(script.contains("vortex-local-commit-execute"));
    assert!(script.contains("\"write_commit_latency_micros\""));
    assert!(script.contains("\"commit_executed\""));
    assert!(script.contains("ShardLoom Write/Commit Evidence"));
    assert!(script.contains("project:value"));
    assert!(script.contains("local primitive validity count"));
    assert!(script.contains("count-where:is_not_null:value"));
    assert!(script.contains("local primitive comparison count"));
    assert!(script.contains("count-where:gte:value:10000"));
    assert!(script.contains("local primitive filter projection"));
    assert!(script.contains("filter-project:gte:value:10000|value"));
    assert!(script.contains("\"timing_scope\""));
    assert!(script.contains("\"materialization_boundary_reported\""));
    assert!(script.contains("\"data_decoded\""));
    assert!(script.contains("\"data_materialized\""));
    assert!(script.contains("\"fallback_attempted\""));
}

#[test]
fn traditional_benchmark_docs_state_no_fallback_and_markdown_outputs() {
    let readme = read_workspace_file("benchmarks/traditional_analytics/README.md");
    let normalized = readme.replace(['\r', '\n'], " ");

    assert!(readme.contains("human-readable Markdown"));
    assert!(readme.contains("coverage_table"));
    assert!(readme.contains("row_classification"));
    assert!(readme.contains("support_status"));
    assert!(readme.contains("native_io_source_sink_coverage_ref"));
    assert!(readme.contains("fairness parameters"));
    assert!(readme.contains("resource metrics"));
    assert!(readme.contains("runtime-effect evidence"));
    assert!(readme.contains("DecisionTrace/WhyReport evidence"));
    assert!(readme.contains("work-avoidance evidence"));
    assert!(readme.contains("write/commit evidence"));
    assert!(readme.contains("result-sink write timing"));
    assert!(readme.contains("requested_execution_mode"));
    assert!(readme.contains("selected_execution_mode"));
    assert!(readme.contains("execution_mode_attribution_contract"));
    assert!(readme.contains("compatibility_parse_millis"));
    assert!(readme.contains("evidence_render_millis"));
    assert!(readme.contains("operator_execution_class"));
    assert!(readme.contains("operator_blocker_id"));
    assert!(readme.contains("persistent_runner_admission_gate"));
    assert!(readme.contains("process_startup_attribution"));
    assert!(readme.contains("python_harness_overhead_status"));
    assert!(readme.contains("source_metadata_snapshot_status"));
    assert!(readme.contains("per_batch_source_metadata_reused"));
    assert!(readme.contains("source_state_reuse_status"));
    assert!(readme.contains("per_batch_dimension_label_state_reused"));
    assert!(readme.contains("source_state_category_metric_reuse_status"));
    assert!(readme.contains("per_batch_category_metric_state_reused"));
    assert!(readme.contains("source_state_group_category_metric_reuse_status"));
    assert!(readme.contains("per_batch_group_category_metric_state_reused"));
    assert!(readme.contains("source_state_ranked_metric_reuse_status"));
    assert!(readme.contains("per_batch_ranked_metric_state_reused"));
    assert!(readme.contains("source_state_selective_filter_reuse_status"));
    assert!(readme.contains("per_batch_selective_filter_state_reused"));
    assert!(readme.contains("source_state_dirty_input_reuse_status"));
    assert!(readme.contains("per_batch_dirty_input_state_reused"));
    assert!(readme.contains("source_state_date_null_metric_reuse_status"));
    assert!(readme.contains("per_batch_date_null_metric_state_reused"));
    assert!(readme.contains("hidden benchmark fast mode"));
    assert!(readme.contains("work_avoidance_evidence_schema"));
    assert!(readme.contains("status/value/reason triples"));
    assert!(readme.contains("`measured`, `not_available`"));
    assert!(readme.contains("`not_applicable`"));
    assert!(normalized.contains("not interpreted as zero"));
    assert!(readme.contains("compatibility-import-certified timing"));
    assert!(readme.contains("execution_mode=prepared_vortex"));
    assert!(readme.contains("standalone `.vortex` report rows"));
    assert!(readme.contains("docs/architecture/compute-engine-flow-reference.md"));
    assert!(readme.contains("per-path certificate id/status"));
    assert!(readme.contains("row_read=true"));
    assert!(normalized.contains("never execute unsupported ShardLoom plans as fallback engines"));
    assert!(readme.contains("shardloom traditional-analytics-run"));
    assert!(readme.contains("vortex-traditional-analytics-benchmark"));
    assert!(readme.contains("universal-I/O path"));
    assert!(readme.contains("--shardloom-build-profile"));
    assert!(readme.contains("--scenario \"group by aggregation\""));
    assert!(readme.contains("--include-stress"));
    assert!(readme.contains("scale stress multi-stage etl"));
    assert!(readme.contains("Spark-style engines"));
    assert!(readme.contains("spark-default"));
    assert!(readme.contains("spark-local-tuned"));
    assert!(readme.contains("startup/warmup time"));
    assert!(readme.contains("Dask is sensitive to partitioning"));
    assert!(readme.contains("rounded to four decimal places"));
    assert!(readme.contains("appends `-dirty`"));
    assert!(readme.contains("`vortex-run` primitive evidence"));
    assert!(normalized.contains("final row evidence"));
    assert!(readme.contains("decision-trace counts"));
    assert!(readme.contains("runtime-report --format json"));
    assert!(readme.contains("report-only introspection schema"));
    assert!(readme.contains("claim blockers"));
    assert!(readme.contains("segment-prune"));
    assert!(readme.contains("bytes-not-read"));
    assert!(readme.contains("bytes avoided"));
    assert!(readme.contains("average commit latency"));
    assert!(readme.contains("It is a local smoke benchmark only"));
    assert!(readme.contains("timing scope"));
    assert!(readme.contains("claim-readiness coverage is separated from timing"));
    assert!(readme.contains("benchmarks\\traditional_analytics\\.venv\\Scripts\\python"));
}

#[test]
fn compute_engine_flow_reference_anchors_execution_modes_and_claim_gates() {
    let doc = read_workspace_file("docs/architecture/compute-engine-flow-reference.md");

    for required_text in [
        "one-shot compatibility query",
        "ingest/stage workflow",
        "prepared Vortex query",
        "native Vortex query",
        "benchmark baseline comparison",
        "Vortex-first",
        "no external fallback",
        "explicit execution mode",
        "explicit materialization/decode boundaries",
        "evidence-certified execution",
        "claim-gated benchmark/reporting",
        "compatibility_import_certified",
        "prepared_vortex",
        "native_vortex",
        "direct_compatibility_transient",
        "auto",
        "SELECTED --> DIRECT",
        "SELECTED --> COMPAT",
        "SELECTED --> PREPARED",
        "SELECTED --> NATIVE",
        "Transient compatibility boundary",
        "requested_execution_mode",
        "selected_execution_mode",
        "mode_selection_reason",
        "vortex_native_claim_allowed",
        "compatibility_import_included",
        "vortex_prepare_included",
        "vortex_write_reopen_included",
        "direct_transient_execution",
        "fallback_attempted=false",
        "external_engine_invoked=false",
        "claim_gate_status=not_claim_grade",
        "claim_gate_status",
        "use_vortex_native_provider",
        "wrap_vortex_concept",
        "implement_shardloom_kernel",
        "baseline_or_oracle_only",
        "unsupported_until_vortex_or_shardloom_evidence",
        "total_runtime_millis",
        "source_read_millis",
        "compatibility_parse_millis",
        "compatibility_to_vortex_import_millis",
        "vortex_write_millis",
        "vortex_reopen_millis",
        "vortex_scan_millis",
        "operator_compute_millis",
        "result_sink_write_millis",
        "evidence_render_millis",
        "execution_mode_attribution_contract",
        "operator_execution_class",
        "operator_blocker_id",
        "operator_encoded_native_claim_allowed",
        "persistent_runner_admission_gate",
        "process_startup_attribution",
        "source_metadata_snapshot_status=per_batch_source_metadata_reused",
        "source_state_reuse_status=per_batch_dimension_label_state_reused",
        "source_state_reuse_status=per_batch_category_metric_state_reused",
        "source_state_category_metric_*",
        "source_state_reuse_status=per_batch_group_category_metric_state_reused",
        "source_state_group_category_metric_*",
        "source_state_reuse_status=per_batch_ranked_metric_state_reused",
        "source_state_ranked_metric_*",
        "source_state_reuse_status=per_batch_selective_filter_state_reused",
        "source_state_selective_filter_*",
        "source_state_reuse_status=per_batch_dirty_input_state_reused",
        "source_state_dirty_input_*",
        "source_state_reuse_status=per_batch_date_null_metric_state_reused",
        "source_state_date_null_metric_*",
        "python_harness_overhead_status",
        "work_avoidance_evidence_schema",
        "not_available",
        "not_applicable",
        "rows avoided",
        "stable correctness digest",
        "Native I/O certificate",
        "Unsupported work must return deterministic unsupported diagnostics",
        "scoped local CSV smoke",
        "Actionable implementation work must be represented in",
        "docs/architecture/phased-execution-plan.md",
        "docs/architecture/compute-engine-flow-overhaul-review.md",
    ] {
        assert!(
            doc.contains(required_text),
            "missing required compute flow reference text: {required_text}"
        );
    }
}

#[test]
fn compute_engine_flow_overhaul_review_declares_repo_gaps_and_phase_steps() {
    let review = read_workspace_file("docs/architecture/compute-engine-flow-overhaul-review.md");
    let plan = read_workspace_file("docs/architecture/phased-execution-plan.md");
    let completed_ledger =
        read_workspace_file("docs/architecture/phased-execution-completed-ledger.md");
    let traceability = read_workspace_file("docs/architecture/rfc-phase-traceability.md");
    let persistent_runner =
        read_workspace_file("docs/architecture/benchmark-persistent-runner-decision.md");
    let protocol_parity =
        read_workspace_file("docs/architecture/execution-mode-protocol-parity.md");

    for required_text in [
        "G1 - Execution-mode selection is not a shared admission layer",
        "G2 - Prepared Vortex is a benchmark harness workflow, not a reusable artifact lifecycle",
        "G3 - Typed envelopes still carry most execution-mode evidence as flat fields",
        "G4 - Capability discovery is not execution-mode aware",
        "G5 - Native Vortex query rows still rely on temporary materialized operators",
        "G6 - Direct transient compatibility mode is parse-level only",
        "G7 - Prepared/native result-sink replay proof is incomplete",
        "G8 - Stage timing attribution is useful but still partially inferred",
        "G9 - Python and future REST surfaces do not yet select modes",
        "G10 - File-format comparisons need a preparation-focused matrix",
        "P7.5.1 - Shared Execution-Mode Admission And Selection Report",
        "P7.5.2 - Typed Envelope Evidence Routing For Flow Fields",
        "P7.5.3 - Prepared Vortex Artifact Lifecycle",
        "P7.5.4 - Mode-Aware Capability Matrix And Direct-Transient Unsupported Parity",
        "P7.5.5 - Native Provider Admission For Prepared/Native Operators",
        "P7.5.6 - Prepared/Native Result-Sink Replay Proof",
        "P7.5.7 - Benchmark Attribution And Persistent Runner Decision",
        "P7.5.8 - Python And Future REST Mode Parity",
        "P7.5.9 - File-Format Preparation Matrix",
        "P7.5.1 through P7.5.9 are complete",
        "fallback_attempted=false",
        "external engines remain baselines/oracles only",
    ] {
        assert!(
            review.contains(required_text),
            "missing required compute flow overhaul review text: {required_text}"
        );
    }

    assert!(plan.contains("docs/architecture/phased-execution-completed-ledger.md"));
    assert!(plan.contains("Global Architecture Review Carry-Forward"));
    assert!(plan.contains("docs/architecture/global-architecture-review.md"));
    assert!(plan.contains("Planned Item Detail Standard"));
    assert!(
        completed_ledger.contains(
            "Session label: GAR-FLOW-1A direct compatibility transient admission contract"
        )
    );
    assert!(
        completed_ledger
            .contains("GAR-FLOW-1B direct compatibility transient local CSV smoke path")
    );
    assert!(completed_ledger.contains("GAR-FLOW-2A execution-mode benchmark attribution contract"));
    assert!(
        completed_ledger.contains("GAR-FLOW-2B prepared/native temporary-operator blocker matrix")
    );
    assert!(completed_ledger.contains("GAR-FLOW-2C persistent benchmark runner admission gate"));
    assert!(completed_ledger.contains("GAR-FLOW-2D work-avoidance metric evidence schema"));
    assert!(completed_ledger.contains("GAR-FLOW-3A REST execution-mode parity report"));
    assert!(
        completed_ledger.contains("GAR-0002A native unsupported coverage diagnostics"),
        "completed ledger should carry completed GAR-0002A provenance"
    );
    assert!(
        completed_ledger.contains("GAR-0002B native Vortex coverage admission expansion"),
        "completed ledger should carry completed GAR-0002B provenance"
    );
    assert!(completed_ledger.contains("GAR-0031A Native I/O envelope source/sink coverage matrix"));
    assert!(completed_ledger.contains("GAR-0042A Vortex Source/Split runtime admission proof"));
    assert!(
        completed_ledger.contains("GAR-0042B layout/write/device/managed-lane evidence boundaries")
    );
    assert!(
        completed_ledger.contains("GAR-0026-J prepared/native global sort/top-k"),
        "completed ledger should carry completed GAR-0026-J provenance"
    );
    assert!(
        completed_ledger.contains("GAR-0027-A CPU/SIMD/vectorization admission slice"),
        "completed ledger should carry completed GAR-0027-A provenance"
    );
    assert!(completed_ledger.contains("GAR-0001A-A SQL/DataFrame planner readiness report"));
    assert!(
        completed_ledger
            .contains("GAR-0001A-B distributed/object-store/lakehouse architecture gate")
    );
    assert!(completed_ledger.contains("GAR-0003-A Vortex segment extraction admission slice"));
    assert!(completed_ledger.contains("GAR-0003-B materialization policy generalization"));
    assert!(completed_ledger.contains("GAR-0004-A CDC and manifest transaction planning gate"));
    assert!(
        completed_ledger.contains("GAR-0006-A predicate, dtype, nested, and null coverage matrix")
    );
    assert!(completed_ledger.contains("GAR-0008-A object-store byte-range provider gate"));
    assert!(completed_ledger.contains("GAR-0008-B object-store runtime blocker matrix"));
    assert!(completed_ledger.contains("GAR-0012-A diagnostic category and helper normalization"));
    assert!(completed_ledger.contains(
        "GAR-0012-B envelope status and distributed/object-store diagnostic propagation"
    ));
    assert!(
        completed_ledger
            .contains("GAR-0013-A streaming runtime capability and unsupported diagnostics")
    );
    assert!(completed_ledger.contains("GAR-0005-A local Vortex reader/writer coverage lane"));
    assert!(
        completed_ledger
            .contains("GAR-0005-B object-store Vortex I/O and upstream write integration gate")
    );
    assert!(completed_ledger.contains("GAR-0020-A table/catalog metadata admission gate"));
    assert!(
        completed_ledger.contains("GAR-0020-C local manifest-backed table metadata read smoke")
    );
    assert!(completed_ledger.contains(
        "GAR-0007-A/B compatibility output writer matrix and local fixture-smoke evidence"
    ));
    assert!(completed_ledger.contains("GAR-0016-A adaptive runtime gate consolidation"));
    assert!(completed_ledger.contains("GAR-0017-A fault-tolerance execution gate split"));
    assert!(
        completed_ledger.contains("GAR-0018-A live profiling and runtime introspection report")
    );
    assert!(
        completed_ledger.contains("GAR-0021-A approximate aggregate and sketch function admission")
    );
    assert!(
        completed_ledger.contains("GAR-0038-A facade compatibility and legacy boundary matrix")
    );
    assert!(
        completed_ledger
            .contains("GAR-0026-V selective-filter selection-vector-backed metric aggregation")
    );
    assert!(completed_ledger.contains("GAR-0014-A spill/OOM enforcement promotion gate closeout"));
    assert!(plan.contains("GAR-0032-A SQL parser/binder report-only readiness"));
    assert!(plan.contains("GAR-0043-A hard release-readiness validators and architecture tracker"));
    let planned_gar_slices = planned_gar_slices(&plan);
    assert!(planned_gar_slices.len() + completed_gar_session_count(&completed_ledger) >= 32);
    for required_field in [
        "Current state:",
        "Next slice outcome:",
        "User-visible surface:",
        "Implementation scope:",
        "Evidence required:",
        "Acceptance:",
        "Verification:",
        "Non-goals:",
        "Dependencies/blockers:",
    ] {
        for slice in &planned_gar_slices {
            let title = slice.lines().next().unwrap_or("unknown GAR slice");
            assert!(
                slice.contains(required_field),
                "{title} missing detailed GAR field {required_field}"
            );
        }
    }
    for slice in &planned_gar_slices {
        let title = slice.lines().next().unwrap_or("unknown GAR slice");
        assert!(
            slice.contains("Claim boundary:") || slice.contains("Fallback/claim boundary:"),
            "{title} missing claim boundary detail"
        );
    }
    assert!(!plan.contains("Priority 7.5 - compute-engine flow overhaul"));
    for child in [
        "P7.5.1", "P7.5.2", "P7.5.3", "P7.5.4", "P7.5.5", "P7.5.6", "P7.5.7", "P7.5.8", "P7.5.9",
    ] {
        assert!(
            completed_ledger.contains(&format!("Session label: {child}")),
            "missing completed {child}"
        );
    }
    assert!(completed_ledger.contains("source format, workload"));
    assert!(completed_ledger.contains("unsupported diagnostic"));
    assert!(review.contains("prepare, inspect, reuse"));
    assert!(review.contains("provider kind, semantic"));
    assert!(review.contains("use_vortex_native_provider"));
    assert!(protocol_parity.contains("result_sink_claim_gate_status"));
    assert!(protocol_parity.contains("computed_result_sink_write_micros"));
    assert!(review.contains("batched runner"));
    assert!(traceability.contains("P7.5 follow-up sequence"));
    assert!(traceability.contains("P7.5.6 completion update"));
    assert!(traceability.contains("traditional-analytics-vortex-run --workspace"));
    assert!(traceability.contains("P7.5.7 completion update"));
    assert!(traceability.contains("P7.5.8 completion update"));
    assert!(traceability.contains("P7.5.9 completion update"));
    assert!(traceability.contains("format_preparation_matrix"));
    assert!(traceability.contains("compute-engine-flow-overhaul-review.md"));
    assert!(persistent_runner.contains("cli_process_wall_millis"));
    assert!(persistent_runner.contains("python_harness_overhead_millis"));
    assert!(persistent_runner.contains("gar-flow-2c.persistent_runner_admission.v1"));
    assert!(persistent_runner.contains("persistent_runner_admitted=false"));
    assert!(persistent_runner.contains("hidden_fast_mode_allowed=false"));
    assert!(
        persistent_runner
            .contains("persistent_runner_status=process_per_scenario_attributed_not_reduced")
    );
    assert!(protocol_parity.contains("requested_execution_mode"));
    assert!(protocol_parity.contains("selected_execution_mode"));
    assert!(protocol_parity.contains("unsupported_diagnostic_code"));
    assert!(protocol_parity.contains("fallback_attempted=false"));
}

#[test]
fn source_state_reuse_coverage_matrix_classifies_every_traditional_family() {
    let matrix = read_workspace_file("docs/architecture/source-state-reuse-coverage-matrix.md");
    let compute_flow = read_workspace_file("docs/architecture/compute-engine-flow-reference.md");
    let benchmark_catalog = read_workspace_file("docs/architecture/benchmark-suite-catalog.md");
    let local_benchmark = read_workspace_file("docs/benchmarks/local-taxonomy-benchmark.md");
    let readme = read_workspace_file("benchmarks/traditional_analytics/README.md");
    let rust_source = read_workspace_file("shardloom-vortex/src/traditional_analytics.rs");
    let website_builder = read_workspace_file("website/build_static_pages.py");

    for required_text in [
        "source_state_coverage_schema_version",
        "source_state_coverage_matrix_ref",
        "source_state_coverage_status_vocabulary",
        "source_state_coverage_all_requested_scenarios_classified=true",
        "scenario_<slug>_source_state_coverage_status",
        "source_state_digest_status=not_emitted_scoped_in_memory_source_state",
        "source-state-reused",
        "source-state-not-needed",
        "blocked-with-reason",
        "unsupported-with-reason",
        "csv/file ingest",
        "selective filter",
        "filter + projection + limit",
        "wide projection",
        "group by aggregation",
        "multi-key group by",
        "distinct count",
        "high-cardinality string group/distinct",
        "hash join",
        "join + aggregate",
        "sort and top-k",
        "top-N per group",
        "row number window",
        "partition pruning",
        "null-heavy aggregate",
        "many-small-files scan",
        "clean/cast/filter/write",
        "malformed timestamp / dirty CSV",
        "small change over large base",
        "nested JSON field scan",
        "scale stress skewed join aggregation",
        "scale stress multi-stage etl",
        "source_state_fallback_attempted=false",
        "source_state_external_engine_invoked=false",
        "GAR-IOREUSE-1A",
    ] {
        assert!(
            matrix.contains(required_text),
            "missing source-state coverage matrix text: {required_text}"
        );
    }

    for required_text in [
        "source_state_coverage_schema_version",
        "source_state_coverage_matrix_ref",
        "source_state_coverage_matrix",
        "source_state_coverage_status_count",
        "source_state_coverage_status(",
        "source_state_coverage_family(",
        "source_state_coverage_reason(",
        "source-state-reused",
        "source-state-not-needed",
        "blocked-with-reason",
        "unsupported-with-reason",
        "source_state_digest_status",
        "not_emitted_scoped_in_memory_source_state",
    ] {
        assert!(
            rust_source.contains(required_text),
            "missing source-state coverage runtime field text: {required_text}"
        );
    }

    for doc in [&compute_flow, &benchmark_catalog, &local_benchmark, &readme] {
        assert!(doc.contains("docs/architecture/source-state-reuse-coverage-matrix.md"));
        assert!(doc.contains("source_state_coverage"));
        assert!(doc.contains("not_emitted_scoped_in_memory_source_state"));
    }

    assert!(website_builder.contains("source_state_coverage_all_requested_scenarios_classified"));
    assert!(website_builder.contains("source_state_coverage_reused_scenario_count"));
    assert!(website_builder.contains("source_state_digest_status"));
}

fn read_workspace_file(relative: &str) -> String {
    fs::read_to_string(workspace_root().join(relative))
        .unwrap_or_else(|error| panic!("failed to read {relative}: {error}"))
}

fn planned_gar_slices(plan: &str) -> Vec<String> {
    let lines = plan.lines().collect::<Vec<_>>();
    let mut slices = Vec::new();
    let mut start = None;
    for (index, line) in lines.iter().enumerate() {
        if line.starts_with("- [ ] GAR-") {
            if let Some(previous_start) = start {
                slices.push(lines[previous_start..index].join("\n"));
            }
            start = Some(index);
        }
    }
    if let Some(previous_start) = start {
        slices.push(lines[previous_start..].join("\n"));
    }
    slices
}

fn completed_gar_session_count(completed_ledger: &str) -> usize {
    completed_ledger
        .lines()
        .filter(|line| line.starts_with("- [x] Session label: GAR-"))
        .count()
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("contract tests crate is in workspace root")
        .to_path_buf()
}
