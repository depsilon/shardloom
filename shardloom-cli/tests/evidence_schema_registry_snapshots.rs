mod support;

use support::{assert_common_typed_slots, field, run_command};

#[test]
fn evidence_schema_registry_global_output_is_typed_and_claim_safe() {
    let output = run_command(&["evidence-schema", "--format", "json"], true);

    assert_common_typed_slots(&output, "evidence-schema", "success");
    assert!(output.contains(&field("command_family", "status_capabilities")));
    assert!(output.contains(&field(
        "evidence_schema_registry_schema_version",
        "shardloom.evidence_field_schema_registry.v1"
    )));
    assert!(output.contains(&field(
        "evidence_schema_registry_report_id",
        "review-p1-2.evidence_field_schema_registry"
    )));
    assert!(output.contains(&field(
        "evidence_schema_registry_command",
        "shardloom evidence-schema [surface] --format json"
    )));
    assert!(output.contains(&field("evidence_schema_registry_surface_count", "8")));
    assert!(output.contains(&field("evidence_schema_registry_field_count", "297")));
    assert!(output.contains(&field(
        "evidence_schema_registry_claim_gate_status",
        "metadata_only_not_claim_grade"
    )));
    assert!(output.contains(&field(
        "evidence_schema_registry_fallback_attempted",
        "false"
    )));
    assert!(output.contains(&field(
        "evidence_schema_registry_external_engine_invoked",
        "false"
    )));
    assert!(output.contains(&field(
        "evidence_schema_surface_compute_flow_evidence_python_accessor_mapping",
        "TraditionalAnalyticsRun.compute_flow_evidence_fields"
    )));
    assert!(output.contains(&field(
        "evidence_schema_field_compute_flow_evidence_computed_result_vortex_bytes_dtype",
        "integer"
    )));
    assert!(output.contains(&field(
        "evidence_schema_field_compute_flow_evidence_prepared_state_reuse_scope_key",
        "prepared_state_reuse_scope"
    )));
    assert!(output.contains(&field(
        "evidence_schema_field_compute_flow_evidence_prepared_state_reuse_hit_dtype",
        "boolean"
    )));
    assert!(output.contains(&field(
        "evidence_schema_field_execution_certificate_report_runtime_execution_dtype",
        "boolean"
    )));
    assert!(output.contains(&field(
        "evidence_schema_field_compute_capability_matrix_report_support_status_vocabulary_cardinality",
        "list_or_csv"
    )));
    assert!(output.contains(&field(
        "evidence_schema_surface_benchmark_constitution_report_command_examples",
        "benchmark-constitution"
    )));
    assert!(output.contains(&field(
        "evidence_schema_surface_benchmark_constitution_report_field_count",
        "28"
    )));
}

#[test]
fn evidence_schema_registry_selected_surface_exposes_field_contract() {
    let output = run_command(
        &[
            "evidence-schema",
            "execution_mode_selection_report",
            "--format",
            "json",
        ],
        true,
    );

    assert_common_typed_slots(&output, "evidence-schema", "success");
    assert!(output.contains(&field(
        "selected_surface",
        "execution_mode_selection_report"
    )));
    assert!(output.contains(&field("selected_surface_field_count", "26")));
    assert!(output.contains(&field(
        "selected_surface_required_no_fallback_fields",
        "fallback_attempted,external_engine_invoked"
    )));
    assert!(output.contains(&field(
        "evidence_schema_field_execution_mode_selection_report_fallback_attempted_dtype",
        "boolean"
    )));
    assert!(output.contains(&field(
        "evidence_schema_field_execution_mode_selection_report_fallback_attempted_no_fallback_semantics",
        "must_remain_false"
    )));
    assert!(output.contains(&field(
        "evidence_schema_field_execution_mode_selection_report_required_future_evidence_cardinality",
        "list_or_csv"
    )));
}
