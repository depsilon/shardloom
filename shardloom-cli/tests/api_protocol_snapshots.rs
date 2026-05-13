use std::process::Command;

fn run_cli_json(args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("shardloom command runs");

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

fn run_api_compat_plan() -> String {
    run_cli_json(&["api-compat-plan", "--format", "json"])
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

#[test]
fn rest_api_contract_plan_json_exposes_openapi_discovery_contract() {
    let output = run_cli_json(&["rest-api-contract-plan", "--format", "json"]);

    assert!(output.contains("\"command\":\"rest-api-contract-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "rest_api_contract_plan")));
    assert!(output.contains(&field("schema_version", "shardloom.rest_api_contract.v1")));
    assert!(output.contains(&field("api_version", "v1")));
    assert!(output.contains(&field("openapi_version", "3.2.0")));
    assert!(output.contains(&field(
        "openapi_contract_path",
        "docs/api/shardloom-openapi-v1.yaml"
    )));
    assert!(output.contains(&field(
        "problem_details_media_type",
        "application/problem+json"
    )));
    assert!(output.contains(&field("represented_resource_count", "15")));
    assert!(output.contains(&field(
        "execution_policy_fields",
        "engine_mode,fallback_policy,materialization_policy,result_policy,evidence_policy"
    )));
    assert!(output.contains(&field(
        "discovery_endpoint_paths",
        "/v1/health,/v1/version,/v1/capabilities,/v1/capabilities/engines,/v1/capabilities/operators,/v1/capabilities/functions,/v1/capabilities/sql,/v1/capabilities/adapters,/v1/capabilities/deployment,/v1/adapters,/v1/sources,/v1/sinks"
    )));
    assert!(output.contains(&field("discovery_endpoints_side_effect_free", "true")));
    assert!(output.contains(
        "\"lifecycle\":{\"fields\":[{\"key\":\"command_family\",\"value\":\"rest_api_planning\""
    ));
}

#[test]
fn rest_api_contract_plan_json_preserves_no_server_no_probe_policy() {
    let output = run_cli_json(&["rest-api-contract-plan", "--format", "json"]);

    assert!(output.contains(&field("server_started", "false")));
    assert!(output.contains(&field("network_listener_opened", "false")));
    assert!(output.contains(&field("network_probe", "false")));
    assert!(output.contains(&field("dataset_probe", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("catalog_probe", "false")));
    assert!(output.contains(&field("credential_resolution", "false")));
    assert!(output.contains(&field("query_execution", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}

#[test]
fn serve_discovery_mode_json_is_contract_only_without_listener() {
    let output = run_cli_json(&[
        "serve",
        "--mode",
        "discovery",
        "--bind",
        "127.0.0.1:8787",
        "--format",
        "json",
    ]);

    assert!(output.contains("\"command\":\"serve\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "rest_api_discovery_mode")));
    assert!(output.contains(&field("server_mode", "discovery")));
    assert!(output.contains(&field("bind", "127.0.0.1:8787")));
    assert!(output.contains(&field("health_endpoint", "/v1/health")));
    assert!(output.contains(&field("capabilities_endpoint", "/v1/capabilities")));
    assert!(output.contains(&field("serve_command_contract_only", "true")));
    assert!(output.contains(&field("server_started", "false")));
    assert!(output.contains(&field("network_listener_opened", "false")));
    assert!(output.contains(&field("dataset_probe", "false")));
    assert!(output.contains(&field("query_execution", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
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
    assert!(output.contains(&field("compatibility_lock_status", "locked")));
    assert!(output.contains(&field(
        "compatibility_lock_fixture_statuses",
        "success,error,unsupported,blocked,evidence_incomplete,certified_local_execution,missing_binary,foundry_optional"
    )));
    assert!(output.contains(&field("json_error_paths_enveloped", "true")));
    assert!(output.contains(&field("unknown_command_json_enveloped", "true")));
    assert!(output.contains(&field("missing_binary_error_payload_shaped", "true")));
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
