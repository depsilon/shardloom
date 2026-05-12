#[cfg(feature = "vortex-local-primitives")]
use std::path::PathBuf;
use std::process::Command;

fn run_command(args: &[&str], expect_success: bool) -> String {
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

fn assert_common_typed_slots(output: &str, command: &str, status: &str) {
    assert!(output.contains("\"schema_version\":\"shardloom.output.v2\""));
    assert!(output.contains(&format!("\"command\":\"{command}\"")));
    assert!(output.contains(&format!("\"status\":\"{status}\"")));
    assert!(output.contains("\"fallback\":{\"attempted\":false,\"allowed\":false"));
    assert!(output.contains("\"diagnostics\":["));
    assert!(output.contains("\"result\":{\"fields\":["));
    assert!(output.contains("\"result_refs\":["));
    assert!(output.contains("\"artifacts\":["));
    assert!(output.contains("\"artifact_refs\":["));
    assert!(output.contains("\"certificates\":["));
    assert!(output.contains("\"policy\":{\"fields\":["));
    assert!(output.contains("\"lifecycle\":{\"fields\":["));
    assert!(output.contains("\"capability_snapshot\":{\"fields\":["));
}

#[test]
fn success_fixture_routes_status_into_typed_envelope() {
    let output = run_command(&["status", "--format", "json"], true);

    assert_common_typed_slots(&output, "status", "success");
    assert!(output.contains(&field("command_family", "status_capabilities")));
    assert!(output.contains("\"result\":{\"fields\":[]}"));
    assert!(output.contains("\"capability_snapshot\":{\"fields\":[]}"));
    assert!(output.contains("\"policy\":{\"fields\":[{\"key\":\"fallback_execution_allowed\""));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
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

#[test]
fn foundry_adjacent_harness_fixture_remains_optional_and_report_only() {
    let output = run_command(&["universal-harness-plan", "--format", "json"], true);

    assert_common_typed_slots(&output, "universal-harness-plan", "success");
    assert!(output.contains(&field("command_family", "evidence_certificates")));
    assert!(output.contains(&field("mode", "universal_harness_plan")));
    assert!(output.contains(&field("universal_harness_status", "evidence_incomplete")));
    assert!(output.contains(&field("foundry_required", "false")));
    assert!(output.contains(&field("foundry_optional_example", "true")));
    assert!(output.contains(&field("foundry_optional_harness_required", "true")));
    assert!(output.contains(&field("external_baseline_execution", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}
