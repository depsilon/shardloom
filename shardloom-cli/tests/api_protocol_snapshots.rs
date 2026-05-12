use std::process::Command;

fn run_api_compat_plan() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["api-compat-plan", "--format", "json"])
        .output()
        .expect("api-compat-plan command runs");

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
fn api_compat_plan_json_exposes_cli_api_protocol_contract() {
    let output = run_api_compat_plan();

    assert!(output.contains("\"command\":\"api-compat-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains("\"schema_version\":\"shardloom.output.v2\""));
    assert!(output.contains(&field("mode", "api_compat_plan")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.cli_api_json_protocol.v1"
    )));
    assert!(output.contains(&field("protocol_id", "shardloom.cli_json.v1")));
    assert!(output.contains(&field("protocol_stability", "experimental")));
    assert!(output.contains(&field(
        "output_envelope_schema_version",
        "shardloom.output.v2"
    )));
    assert!(output.contains(&field(
        "required_envelope_fields",
        "schema_version,command,status,summary,human_text,fallback,diagnostics,result,result_refs,artifacts,artifact_refs,certificates,policy,lifecycle,capability_snapshot,fields"
    )));
    assert!(output.contains(&field("required_typed_payload_fields", "fields")));
    assert!(output.contains(&field("legacy_fields_mirror_present", "true")));
    assert!(output.contains(&field("flat_fields_primary_payload_allowed", "false")));
    assert!(output.contains(&field(
        "command_status_values",
        "success,warning,error,unsupported"
    )));
    assert!(output.contains(&field("output_formats", "text,json")));
}

#[test]
fn api_compat_plan_json_is_report_only_and_no_fallback() {
    let output = run_api_compat_plan();

    assert!(output.contains(&field(
        "thin_python_wrapper_boundary",
        "cli_json_subprocess_first"
    )));
    assert!(output.contains(&field("pyo3_maturin_allowed", "false")));
    assert!(output.contains(&field("foundry_required", "false")));
    assert!(output.contains(&field("dataframe_api_implemented", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
    assert!(output.contains(&field("filesystem_probe", "false")));
    assert!(output.contains(&field("network_probe", "false")));
    assert!(output.contains(&field("catalog_probe", "false")));
    assert!(output.contains(&field("adapter_probe", "false")));
    assert!(output.contains(&field("parser_executed", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("external_publish", "not_performed")));
    assert!(output.contains(&field("external_publish_performed", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains("\"attempted\":false"));
    assert!(output.contains("\"allowed\":false"));
}

#[test]
fn api_compat_plan_json_routes_common_fields_into_typed_slots() {
    let output = run_api_compat_plan();

    assert!(
        output.contains("\"policy\":{\"fields\":[{\"key\":\"publish_allowed\",\"value\":\"false\"")
    );
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(
        "\"lifecycle\":{\"fields\":[{\"key\":\"command_family\",\"value\":\"rest_api_planning\""
    ));
    assert!(output.contains(&field("mode", "api_compat_plan")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.cli_api_json_protocol.v1"
    )));
    assert!(output.contains("\"capability_snapshot\":{\"fields\":[]}"));
    assert!(output.contains("\"fields\":[{\"key\":\"mode\",\"value\":\"api_compat_plan\""));
}
