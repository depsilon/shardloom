use std::process::Command;

fn run_python_wrapper_plan() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["python-wrapper-plan", "--format", "json"])
        .output()
        .expect("python-wrapper-plan command runs");

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
fn python_wrapper_plan_json_exposes_cli_json_foundation() {
    let output = run_python_wrapper_plan();

    assert!(output.contains("\"command\":\"python-wrapper-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains("\"schema_version\":\"shardloom.output.v1\""));
    assert!(output.contains(&field("mode", "python_wrapper_plan")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.python_wrapper_foundation.v1"
    )));
    assert!(output.contains(&field("wrapper_id", "shardloom_python_cli_json_client")));
    assert!(output.contains(&field("wrapper_status", "source_tree_foundation")));
    assert!(output.contains(&field("transport_protocol_id", "shardloom.cli_json.v1")));
    assert!(output.contains(&field(
        "output_envelope_schema_version",
        "shardloom.output.v1"
    )));
    assert!(output.contains(&field("invocation_model", "subprocess_cli_json")));
    assert!(output.contains(&field(
        "initial_command_scope",
        "status,capabilities,api-compat-plan,python-wrapper-plan,vortex-run,traditional-analytics-run,traditional-analytics-vortex-run,dynamic-work-shaping-plan,sizing-feedback-plan,benchmark-plan,benchmark-claim-evidence-plan"
    )));
}

#[test]
fn python_wrapper_plan_json_defers_mature_python_surfaces() {
    let output = run_python_wrapper_plan();

    assert!(output.contains(&field("package_status", "source_tree_created")));
    assert!(output.contains(&field("native_binding_status", "not_created")));
    assert!(output.contains(&field("pyo3_maturin_allowed", "false")));
    assert!(output.contains(&field("python_package_created", "true")));
    assert!(output.contains(&field("native_extension_required", "false")));
    assert!(output.contains(&field("dataframe_api_implemented", "false")));
    assert!(output.contains(&field("notebook_api_implemented", "false")));
    assert!(output.contains(&field("python_udf_runtime_implemented", "false")));
    assert!(output.contains(&field(
        "materialization_boundary_reporting_required",
        "true"
    )));
    assert!(output.contains(&field("diagnostics_passthrough_required", "true")));
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
