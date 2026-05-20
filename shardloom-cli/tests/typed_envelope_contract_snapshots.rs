#[cfg(feature = "vortex-local-primitives")]
use std::path::PathBuf;

mod support;

use support::{assert_common_typed_slots, field, run_command};

#[cfg(feature = "vortex-local-primitives")]
fn local_primitive_struct_fixture() -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("shardloom-vortex")
        .join("tests")
        .join("fixtures")
        .join("local_primitive_struct_five.vortex")
        .display()
        .to_string()
}

#[test]
fn success_fixture_routes_status_into_typed_envelope() {
    let output = run_command(&["status", "--format", "json"], true);

    assert_common_typed_slots(&output, "status", "success");
    assert!(output.contains(&field("command_family", "status_capabilities")));
    assert!(output.contains("\"capability_snapshot\":{\"fields\":[]}"));
    assert!(output.contains("\"policy\":{\"fields\":[{\"key\":\"fallback_execution_allowed\""));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("cli_binary_version", env!("CARGO_PKG_VERSION"))));
    assert!(output.contains(&field("protocol_version", "shardloom.output.v2")));
    assert!(output.contains(&field("runtime_discovery_side_effect_free", "true")));
}

#[test]
fn invalid_input_fixture_preserves_typed_diagnostics_and_lifecycle() {
    let output = run_command(&["capabilities", "unknown", "--format", "json"], false);

    assert_common_typed_slots(&output, "capabilities", "error");
    assert!(output.contains(&field("command_family", "status_capabilities")));
    assert!(output.contains("\"code\":\"SL_INVALID_INPUT\""));
    assert!(output.contains("\"category\":\"invalid_input\""));
    assert!(output.contains("\"reason\":\"capabilities unknown argument/value: unknown\""));
    assert!(output.contains("\"fallback\":{\"attempted\":false,\"allowed\":false"));
}

#[test]
fn unsupported_source_backed_fixture_preserves_no_fallback_boundary() {
    let output = run_command(
        &[
            "vortex-encoded-read-boundary",
            "file://fixture.vortex",
            "local-path-only",
            "--format",
            "json",
        ],
        false,
    );

    assert_common_typed_slots(&output, "vortex-encoded-read-boundary", "unsupported");
    assert!(output.contains(&field("command_family", "prepared_source_backed_execution")));
    assert!(output.contains(&field("mode", "vortex_encoded_read_boundary")));
    assert!(output.contains(&field("local_path_only", "true")));
    assert!(output.contains(&field("read_execution_allowed", "false")));
    assert!(output.contains(&field("upstream_scan_called", "false")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
}

#[test]
fn blocked_capability_fixture_routes_claim_blockers_into_typed_slots() {
    let output = run_command(&["cg20-user-capability-gate", "--format", "json"], true);

    assert_common_typed_slots(&output, "cg20-user-capability-gate", "success");
    assert!(output.contains(&field("command_family", "evidence_certificates")));
    assert!(output.contains(&field("mode", "cg20_user_capability_promotion_gate")));
    assert!(output.contains(&field("runtime_promotions_blocked", "true")));
    assert!(output.contains(&field("claim_blocked", "true")));
    assert!(output.contains(&field("sql_runtime_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("sql_coverage_required", "true")));
    assert!(
        output.contains("\"capability_snapshot\":{\"fields\":[{\"key\":\"sql_coverage_required\"")
    );
}

#[test]
fn api_surfaces_capability_fixture_routes_registry_into_typed_slots() {
    let output = run_command(&["capabilities", "api-surfaces", "--format", "json"], true);

    assert_common_typed_slots(&output, "capabilities", "success");
    assert!(output.contains(&field("command_family", "status_capabilities")));
    assert!(output.contains(&field("scope", "api_surfaces")));
    assert!(output.contains(&field(
        "wrapper_connector_registry_schema_version",
        "shardloom.wrapper_connector_implementation_registry.v1"
    )));
    assert!(output.contains("\"artifact_kind\":\"api_surface_capability_report\""));
    assert!(output.contains("\"artifact_id\":\"capabilities.api_surfaces\""));
    assert!(output.contains("\"capability_snapshot\":{\"fields\":["));
    assert!(output.contains(&field("wrapper_connector_registry_row_count", "26")));
    assert!(output.contains(&field(
        "wrapper_connector_registry_claim_gate_status",
        "not_claim_grade"
    )));
    assert!(output.contains(&field(
        "wrapper_connector_registry_wrapper_ecosystem_claim_allowed",
        "false"
    )));
}

#[test]
fn certificate_surface_fixture_routes_certificate_plan_fields() {
    let output = run_command(&["execution-certificate-plan", "--format", "json"], true);

    assert_common_typed_slots(&output, "execution-certificate-plan", "success");
    assert!(output.contains(&field("command_family", "evidence_certificates")));
    assert!(output.contains(&field("mode", "execution_certificate_plan")));
    assert!(output.contains(&field(
        "certificate_schema_version",
        "shardloom.execution_certificate.v1"
    )));
    assert!(output.contains(&field("machine_readable_certificate_surface", "true")));
    assert!(output.contains(&field("certificate_evaluation_performed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains("\"artifact_kind\":\"execution_certificate_report\""));
    assert!(output.contains("\"artifact_id\":\"cg16.execution-certificate-evidence-surface\""));
    assert!(output.contains(
        "\"payload\":{\"fields\":[{\"key\":\"mode\",\"value\":\"execution_certificate_plan\""
    ));
}

#[test]
fn native_io_fixture_routes_inline_report_payload() {
    let output = run_command(&["native-io-envelope-plan", "--format", "json"], true);

    assert_common_typed_slots(&output, "native-io-envelope-plan", "success");
    assert!(output.contains(&field("command_family", "evidence_certificates")));
    assert!(output.contains(&field("mode", "native_io_envelope_plan")));
    assert!(output.contains(&field("schema_version", "shardloom.native_io_envelope.v1")));
    assert!(output.contains("\"artifact_kind\":\"native_io_report\""));
    assert!(output.contains("\"artifact_id\":\"cg19.native-io-envelope\""));
    assert!(output.contains(&field("certificate_path_requirement_count", "3")));
    assert!(output.contains(&field("native_io_source_sink_coverage_row_count", "14")));
    assert!(output.contains(&field(
        "native_io_source_sink_coverage_status",
        "complete_for_current_matrix"
    )));
    assert!(output.contains(&field(
        "native_io_source_sink_coverage_all_unadmitted_rows_have_diagnostics",
        "true"
    )));
}

#[test]
fn evidence_incomplete_benchmark_fixture_routes_claim_gate_fields() {
    let output = run_command(&["benchmark-plan", "foundation", "--format", "json"], true);

    assert_common_typed_slots(&output, "benchmark-plan", "success");
    assert!(output.contains(&field("command_family", "benchmarks")));
    assert!(output.contains(&field("mode", "benchmark_plan")));
    assert!(output.contains(&field("claim_gate_status", "evidence_missing")));
    assert!(output.contains(&field("benchmark_execution_implemented", "false")));
    assert!(output.contains(&field("performance_claim_allowed", "false")));
    assert!(output.contains(&field("external_baselines", "comparison_only")));
    assert!(output.contains(&field(
        "external_baseline_engine_order",
        "datafusion,vortex_integration,spark,polars,other"
    )));
    assert!(output.contains(&field("claim_gate_fallback", "not_attempted")));
    assert!(output.contains("\"artifact_kind\":\"benchmark_plan_report\""));
    assert!(output.contains("\"artifact_id\":\"benchmark-plan.report\""));
}

#[test]
fn benchmark_claim_evidence_fixture_routes_inline_report_payload() {
    let output = run_command(&["benchmark-claim-evidence-plan", "--format", "json"], true);

    assert_common_typed_slots(&output, "benchmark-claim-evidence-plan", "success");
    assert!(output.contains(&field("command_family", "evidence_certificates")));
    assert!(output.contains(&field("mode", "benchmark_claim_evidence")));
    assert!(output.contains(&field("claim_gate_status", "evidence_missing")));
    assert!(output.contains(&field("measured_benchmark_result_rows_present", "false")));
    assert!(output.contains("\"artifact_kind\":\"benchmark_claim_evidence_report\""));
    assert!(output.contains("\"artifact_id\":\"cg6.benchmark_claim_evidence.aggregate\""));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn certified_runtime_execution_fixture_routes_inline_certificates() {
    let fixture = local_primitive_struct_fixture();
    let output = run_command(
        &[
            "vortex-project",
            fixture.as_str(),
            "metric",
            "--execute-local-primitive",
            "1",
            "2",
            "--format",
            "json",
        ],
        true,
    );

    assert_common_typed_slots(&output, "vortex-project", "success");
    assert!(output.contains(&field("command_family", "vortex_primitive_execution")));
    assert!(output.contains(&field("mode", "vortex_project")));
    assert!(output.contains(&field(
        "execution",
        "local_vortex_project_primitive_performed"
    )));
    assert!(output.contains(&field("project_local_execution_status", "executed")));
    assert!(output.contains(&field(
        "project_local_execution_mode",
        "vortex_scan_pushdown"
    )));
    assert!(output.contains(&field("project_local_execution_rows_projected", "5")));
    assert!(output.contains(&field(
        "project_local_execution_projected_columns",
        "metric"
    )));
    assert!(output.contains(&field(
        "project_local_execution_native_io_certified",
        "true"
    )));
    assert!(output.contains(&field(
        "project_local_execution_correctness_certified",
        "true"
    )));
    assert!(output.contains(&field(
        "scan_pushdown_schema_version",
        "shardloom.vortex_primitive.scan_pushdown_contract.v1"
    )));
    assert!(output.contains(&field("scan_pushdown_status", "scan_pushdown_supported")));
    assert!(output.contains(&field("scan_filter_required", "false")));
    assert!(output.contains(&field("scan_projection_required", "true")));
    assert!(output.contains(&field("scan_limit_required", "false")));
    assert!(output.contains(&field("scan_filter_pushed_down", "false")));
    assert!(output.contains(&field("scan_projection_pushed_down", "true")));
    assert!(output.contains(&field("scan_limit_pushed_down", "false")));
    assert!(output.contains(&field("scan_output_columns_read", "metric")));
    assert!(output.contains(&field("scan_pushdown_blocker_id", "none")));
    assert!(output.contains(&field(
        "scan_pushdown_claim_gate_status",
        "fixture_smoke_only"
    )));
    assert!(output.contains(&field("scan_pushdown_fallback_attempted", "false")));
    assert!(output.contains(&field("scan_pushdown_external_engine_invoked", "false")));
    assert!(output.contains(&field("data_read", "true")));
    assert!(output.contains(&field("data_decoded", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("row_read", "false")));
    assert!(output.contains(&field("arrow_converted", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains("\"artifact_kind\":\"native_io_certificate\""));
    assert!(output.contains("\"artifact_kind\":\"execution_certificate\""));
    assert!(output.contains("\"artifact_kind\":\"source_report\""));
    assert!(output.contains("\"artifact_kind\":\"source_pushdown_report\""));
    assert!(output.contains("\"artifact_kind\":\"sink_report\""));
    assert!(output.contains("\"artifact_kind\":\"adapter_fidelity_report\""));
    assert!(output.contains("\"artifact_id\":\"cg19.local_primitive.project_columns.native_io\""));
    assert!(output.contains(
        "\"artifact_id\":\"vortex-local-project-struct-five.project_columns.execution-certificate\""
    ));
    assert!(output.contains(&field(
        "local_primitive_execution_certificate_fixture_id",
        "vortex-local-project-struct-five"
    )));
    assert!(output.contains(&field(
        "local_primitive_execution_certificate_status",
        "certified"
    )));
    assert!(output.contains(&field(
        "local_primitive_execution_certificate_external_query_engine_invoked",
        "false"
    )));
    assert!(output.contains(&field(
        "local_primitive_execution_certificate_fallback_attempted",
        "false"
    )));
    assert!(output.contains(&field(
        "local_primitive_execution_certificate_fallback_execution_allowed",
        "false"
    )));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn certified_filter_project_runtime_fixture_routes_scan_pushdown_fields() {
    let fixture = local_primitive_struct_fixture();
    let output = run_command(
        &[
            "vortex-filter-project",
            fixture.as_str(),
            "gte:value:3",
            "metric",
            "--execute-local-primitive",
            "1",
            "2",
            "--format",
            "json",
        ],
        true,
    );

    assert_common_typed_slots(&output, "vortex-filter-project", "success");
    assert!(output.contains(&field("command_family", "vortex_primitive_execution")));
    assert!(output.contains(&field("mode", "vortex_filter_project")));
    assert!(output.contains(&field(
        "filter_project_local_execution_mode",
        "vortex_scan_pushdown"
    )));
    assert!(output.contains(&field(
        "filter_project_local_execution_selection_vector_guarantee",
        "true"
    )));
    assert!(output.contains(&field(
        "filter_project_local_execution_projection_pushdown_guarantee",
        "true"
    )));
    assert!(output.contains(&field("scan_pushdown_status", "scan_pushdown_supported")));
    assert!(output.contains(&field("scan_filter_required", "true")));
    assert!(output.contains(&field("scan_projection_required", "true")));
    assert!(output.contains(&field("scan_filter_pushed_down", "true")));
    assert!(output.contains(&field("scan_projection_pushed_down", "true")));
    assert!(output.contains(&field("scan_filter_columns_read", "value")));
    assert!(output.contains(&field("scan_output_columns_read", "metric")));
    assert!(output.contains(&field("scan_filter_only_columns_read", "value")));
    assert!(output.contains(&field("scan_data_materialized", "false")));
    assert!(output.contains(&field("scan_data_decoded", "false")));
    assert!(output.contains(&field("scan_pushdown_blocker_id", "none")));
    assert!(output.contains(&field("scan_pushdown_fallback_attempted", "false")));
    assert!(output.contains(&field("scan_pushdown_external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn certified_filter_project_limit_fixture_routes_residual_limit_fields() {
    let fixture = local_primitive_struct_fixture();
    let output = run_command(
        &[
            "vortex-filter-project",
            fixture.as_str(),
            "gte:value:3",
            "metric",
            "--limit",
            "2",
            "--execute-local-primitive",
            "1",
            "2",
            "--format",
            "json",
        ],
        true,
    );

    assert_common_typed_slots(&output, "vortex-filter-project", "success");
    assert!(output.contains(&field("command_family", "vortex_primitive_execution")));
    assert!(output.contains(&field("mode", "vortex_filter_project")));
    assert!(output.contains(&field("source_order_limit", "2")));
    assert!(output.contains(&field(
        "scan_pushdown_status",
        "scan_pushdown_partially_supported"
    )));
    assert!(output.contains(&field("scan_filter_pushed_down", "true")));
    assert!(output.contains(&field("scan_projection_pushed_down", "true")));
    assert!(output.contains(&field("scan_limit_required", "true")));
    assert!(output.contains(&field("scan_limit_pushed_down", "false")));
    assert!(output.contains(&field(
        "scan_limit_pushdown_status",
        "blocked_no_scan_limit_admission"
    )));
    assert!(output.contains(&field("scan_limit_requested_rows", "2")));
    assert!(output.contains(&field("scan_residual_limit_required", "true")));
    assert!(output.contains(&field("scan_residual_limit_applied", "true")));
    assert!(output.contains(&field(
        "scan_residual_limit_status",
        "applied_by_shardloom_native_residual"
    )));
    assert!(output.contains(&field("scan_residual_limit_executor", "shardloom_native")));
    assert!(output.contains(&field("scan_residual_limit_input_rows", "3")));
    assert!(output.contains(&field("scan_residual_limit_rows_output", "2")));
    assert!(output.contains(&field(
        "scan_pushdown_blocker_id",
        "gar-runtime-impl-4i.limit_pushdown_not_admitted"
    )));
    assert!(output.contains(&field(
        "filter_project_local_execution_source_order_limit_requested",
        "2"
    )));
    assert!(output.contains(&field(
        "filter_project_local_execution_source_order_limit_applied",
        "true"
    )));
    assert!(output.contains(&field(
        "filter_project_local_execution_source_order_limit_input_rows",
        "3"
    )));
    assert!(output.contains(&field(
        "filter_project_local_execution_source_order_limit_rows_output",
        "2"
    )));
    assert!(output.contains(&field(
        "local_primitive_execution_certificate_fixture_id",
        "vortex-local-filter-project-limit-struct-five"
    )));
    assert!(output.contains(&field(
        "local_primitive_execution_certificate_status",
        "certified"
    )));
    assert!(output.contains(&field("scan_pushdown_fallback_attempted", "false")));
    assert!(output.contains(&field("scan_pushdown_external_engine_invoked", "false")));
}

#[cfg(feature = "vortex-local-primitives")]
#[test]
fn non_executed_vortex_primitive_fixture_routes_scan_pushdown_blocker() {
    let fixture = local_primitive_struct_fixture();
    let output = run_command(
        &[
            "vortex-project",
            fixture.as_str(),
            "metric",
            "--format",
            "json",
        ],
        true,
    );

    assert_common_typed_slots(&output, "vortex-project", "success");
    assert!(output.contains(&field("scan_pushdown_status", "not_executed")));
    assert!(output.contains(&field(
        "scan_projection_pushdown_status",
        "unsupported_no_vortex_scan"
    )));
    assert!(output.contains(&field(
        "scan_pushdown_blocker_id",
        "gar-runtime-impl-4i.local_primitive_scan_not_executed"
    )));
    assert!(output.contains(&field("scan_pushdown_fallback_attempted", "false")));
    assert!(output.contains(&field("scan_pushdown_external_engine_invoked", "false")));
}

#[test]
fn foundry_adjacent_harness_fixture_remains_optional_and_report_only() {
    let output = run_command(&["universal-harness-plan", "--format", "json"], true);

    assert_common_typed_slots(&output, "universal-harness-plan", "success");
    assert!(output.contains(&field("command_family", "evidence_certificates")));
    assert!(output.contains(&field("mode", "universal_harness_plan")));
    assert!(output.contains(&field("universal_harness_status", "evidence_incomplete")));
    assert!(output.contains(&field(
        "universal_harness_execution_gate_status",
        "blocked_missing_evidence"
    )));
    assert!(output.contains(&field("universal_harness_execution_allowed", "false")));
    assert!(output.contains(&field("universal_harness_execution_attempted", "false")));
    assert!(output.contains(&field("foundry_required", "false")));
    assert!(output.contains(&field("foundry_optional_example", "true")));
    assert!(output.contains(&field("foundry_optional_harness_required", "true")));
    assert!(output.contains(&field("external_baseline_execution", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field(
        "universal_harness_missing_evidence_refs",
        "capability_refs,execution_certificate_refs,native_io_certificate_refs,policy_no_fallback_refs,output_envelope_refs,output_artifact_refs,correctness_evidence_refs,benchmark_evidence_refs"
    )));
    assert!(output.contains("\"artifact_kind\":\"universal_harness_report\""));
    assert!(output.contains("\"artifact_id\":\"cg18.universal-harness\""));
}
