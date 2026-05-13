use std::process::Command;

fn run_command_json(args: &[&str], expect_success: bool) -> String {
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

#[test]
fn explain_json_preserves_report_only_no_fallback_boundaries() {
    let output = run_command_json(
        &[
            "explain",
            "read_vortex(orders.vortex) -> filter(gte:value:3) -> select(metric,value)",
            "--format",
            "json",
        ],
        false,
    );

    assert!(output.contains("\"command\":\"explain\""));
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains(&field("mode", "plan_only")));
    assert!(output.contains(&field("plan_only", "true")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("materialization_boundary_reported", "false")));
    assert!(output.contains(&field("external_effects_executed", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains("\"attempted\":false"));
}

#[test]
fn estimate_json_preserves_report_only_no_fallback_boundaries() {
    let output = run_command_json(
        &[
            "estimate",
            "read_csv(events.csv) -> filter(id > 0) -> limit(10)",
            "--format",
            "json",
        ],
        false,
    );

    assert!(output.contains("\"command\":\"estimate\""));
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains(&field("mode", "plan_only")));
    assert!(output.contains(&field("plan_only", "true")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains("\"engine\":null"));
}

#[test]
fn certify_surfaces_json_remain_report_only_for_lazy_workflows() {
    let execution = run_command_json(&["execution-certificate-plan", "--format", "json"], true);
    assert!(execution.contains("\"command\":\"execution-certificate-plan\""));
    assert!(execution.contains(&field("certificate_evaluation_performed", "false")));
    assert!(execution.contains(&field("machine_readable_certificate_surface", "true")));
    assert!(execution.contains(&field("fallback_execution_allowed", "false")));

    let native_io = run_command_json(&["native-io-envelope-plan", "--format", "json"], true);
    assert!(native_io.contains("\"command\":\"native-io-envelope-plan\""));
    assert!(native_io.contains(&field("runtime_execution", "false")));
    assert!(native_io.contains(&field("data_read", "false")));
    assert!(native_io.contains(&field("per_path_certificate_required", "true")));
    assert!(native_io.contains(&field("fallback_execution_allowed", "false")));
}
