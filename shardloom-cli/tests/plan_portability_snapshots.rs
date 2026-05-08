use std::process::Command;

fn run_plan_command(args: &[&str], expect_success: bool) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("plan portability command runs");

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

#[test]
fn plan_ir_json_exposes_native_first_portability_report() {
    let output = run_plan_command(&["plan-ir", "--format", "json"], true);

    assert!(output.contains("\"command\":\"plan-ir\""));
    assert!(output.contains("\"status\":\"warning\""));
    assert!(output.contains(&field("mode", "plan_ir")));
    assert!(output.contains(&field("schema_version", "shardloom.plan_portability.v1")));
    assert!(output.contains(&field("direction", "native_review")));
    assert!(output.contains(&field("portability_status", "native_skeleton")));
    assert!(output.contains(&field("interop_format", "native")));
    assert!(output.contains(&field("native_first", "true")));
    assert!(output.contains(&field("validation_only", "true")));
    assert!(output.contains(&field("validation_required", "true")));
    assert!(output.contains(&field("capability_check_required", "true")));
}

#[test]
fn plan_import_json_reports_unsupported_residuals_without_side_effects() {
    let output = run_plan_command(
        &[
            "plan-import",
            "substrait-like",
            "fixture",
            "--format",
            "json",
        ],
        false,
    );

    assert!(output.contains("\"command\":\"plan-import\""));
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains(&field("mode", "plan_import")));
    assert!(output.contains(&field("direction", "import_validation")));
    assert!(output.contains(&field("portability_status", "not_implemented")));
    assert!(output.contains(&field("interop_format", "substrait-like")));
    assert!(output.contains(&field("unsupported_nodes", "real_plan_import")));
    assert!(output.contains(&field(
        "residual_unsupported_constructs",
        "plan_payload_parsing,native_lowering"
    )));
    assert!(output.contains(&field("import_export_serialization_performed", "false")));
    assert!(output.contains(&field("external_engine_execution", "false")));
    assert!(output.contains(&field("read_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}

#[test]
fn plan_import_json_accepts_native_serialized_payload_without_side_effects() {
    let payload = "shardloom.native_plan.v1\nid=cli-import-fixture\nschema_name=shardloom.plan_ir\nschema_version=1\nlayer=logical\nnode=scan_0|logical|scan|fixture scan|vortex_native_input:1:fixture native input|native_vortex_input";
    let output = run_plan_command(
        &["plan-import", "native", payload, "--format", "json"],
        true,
    );

    assert!(output.contains("\"command\":\"plan-import\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "plan_import")));
    assert!(output.contains(&field("direction", "import_validation")));
    assert!(output.contains(&field("portability_status", "imported")));
    assert!(output.contains(&field("interop_format", "native")));
    assert!(output.contains(&field(
        "supported_constructs",
        "format_declaration,source_label,native_plan_serialization,native_capability_check_required,diagnostics"
    )));
    assert!(output.contains(&field("substrait_like_representable_nodes", "scan_0:scan")));
    assert!(output.contains(&field("imported_plan_id", "cli-import-fixture")));
    assert!(output.contains(&field("imported_plan_node_count", "1")));
    assert!(output.contains(&field(
        "imported_plan_capability_gate_schema_version",
        "shardloom.imported_plan_capability_gate.v1"
    )));
    assert!(output.contains(&field(
        "imported_plan_capability_gate_status",
        "blocked_missing_capability_evidence"
    )));
    assert!(output.contains(&field("imported_plan_capability_checked", "true")));
    assert!(output.contains(&field("imported_plan_execution_allowed", "false")));
    assert!(output.contains(&field(
        "imported_plan_missing_certification_surfaces",
        "adapter_certification,native_io_certificate_coverage,native_plan_validation"
    )));
    assert!(output.contains(&field("imported_plan_gate_runtime_execution", "false")));
    assert!(output.contains(&field("imported_plan_gate_parser_executed", "false")));
    assert!(output.contains(&field("imported_plan_gate_filesystem_probe", "false")));
    assert!(output.contains(&field("imported_plan_gate_network_probe", "false")));
    assert!(output.contains(&field("imported_plan_gate_catalog_probe", "false")));
    assert!(output.contains(&field("imported_plan_gate_adapter_probe", "false")));
    assert!(output.contains(&field(
        "imported_plan_gate_external_engine_execution",
        "false"
    )));
    assert!(output.contains(&field("imported_plan_gate_read_io", "false")));
    assert!(output.contains(&field("imported_plan_gate_write_io", "false")));
    assert!(output.contains(&field(
        "imported_plan_gate_fallback_execution_allowed",
        "false"
    )));
    assert!(output.contains(&field("imported_plan_gate_fallback_attempted", "false")));
    assert!(output.contains(&field("import_export_serialization_performed", "true")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("external_engine_execution", "false")));
    assert!(output.contains(&field("read_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}

#[test]
fn plan_export_json_reports_validation_only_redaction_boundary() {
    let output = run_plan_command(&["plan-export", "json-like", "--format", "json"], false);

    assert!(output.contains("\"command\":\"plan-export\""));
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains(&field("mode", "plan_export")));
    assert!(output.contains(&field("direction", "export_validation")));
    assert!(output.contains(&field("portability_status", "not_implemented")));
    assert!(output.contains(&field("interop_format", "json-like")));
    assert!(output.contains(&field("redaction_required", "true")));
    assert!(output.contains(&field("unsupported_nodes", "real_plan_export")));
    assert!(output.contains(&field(
        "residual_unsupported_constructs",
        "interop_serialization"
    )));
    assert!(output.contains(&field("import_export_serialization_performed", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("external_engine_execution", "false")));
    assert!(output.contains(&field("filesystem_probe", "false")));
    assert!(output.contains(&field("network_probe", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}

#[test]
fn plan_export_json_emits_native_serialized_payload_without_side_effects() {
    let output = run_plan_command(&["plan-export", "native", "--format", "json"], true);

    assert!(output.contains("\"command\":\"plan-export\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "plan_export")));
    assert!(output.contains(&field("direction", "export_validation")));
    assert!(output.contains(&field("portability_status", "serialized")));
    assert!(output.contains(&field("interop_format", "native")));
    assert!(output.contains(&field(
        "supported_constructs",
        "format_declaration,native_plan_schema_version,native_plan_serialization,diagnostics,redaction_policy_required"
    )));
    assert!(output.contains(&field("import_export_serialization_performed", "true")));
    assert!(output.contains(&field("serialized_plan_node_count", "1")));
    assert!(output.contains("shardloom.native_plan.v1"));
    assert!(output.contains("plan-export-native-skeleton"));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("external_engine_execution", "false")));
    assert!(output.contains(&field("filesystem_probe", "false")));
    assert!(output.contains(&field("network_probe", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}
