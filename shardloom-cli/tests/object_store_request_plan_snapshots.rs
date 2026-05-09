use std::process::Command;

fn run_object_store_request_plan_json(scenario: &str) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["object-store-request-plan", scenario, "--format", "json"])
        .output()
        .expect("object-store-request-plan command runs");

    (
        output.status.success(),
        String::from_utf8(output.stdout).expect("stdout is utf8"),
        String::from_utf8(output.stderr).expect("stderr is utf8"),
    )
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

#[test]
fn object_store_request_json_exposes_aggregate_ready_path() {
    let (success, output, stderr) = run_object_store_request_plan_json("ready");

    assert!(success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"command\":\"object-store-request-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "object_store_request_plan")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.object_store_request_planner.v1"
    )));
    assert!(output.contains(&field(
        "report_id",
        "cg10.object_store_request_planner.aggregate"
    )));
    assert!(output.contains(&field("object_store_request_status", "planned")));
    assert!(output.contains(&field(
        "surface_order",
        "range_planning,request_coalescing,distributed_scheduling,checkpoint_retry,commit_protocol"
    )));
    assert!(output.contains(&field("planned_surface_count", "5")));
    assert!(output.contains(&field("blocked_surface_count", "0")));
    assert!(output.contains(&field("range_status", "planned")));
    assert!(output.contains(&field("scheduling_status", "planned")));
    assert!(output.contains(&field("checkpoint_retry_status", "ready")));
    assert!(output.contains(&field("commit_status", "ready")));
}

#[test]
fn object_store_request_json_preserves_report_only_runtime_boundaries() {
    let (success, output, stderr) = run_object_store_request_plan_json("ready");

    assert!(success, "stdout={output} stderr={stderr}");
    assert!(output.contains(&field("coordinator_started", "false")));
    assert!(output.contains(&field("worker_started", "false")));
    assert!(output.contains(&field("task_execution_allowed", "false")));
    assert!(output.contains(&field("retry_execution_allowed", "false")));
    assert!(output.contains(&field("checkpoint_write_allowed", "false")));
    assert!(output.contains(&field("cleanup_execution_allowed", "false")));
    assert!(output.contains(&field("commit_execution_allowed", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
    assert!(output.contains(&field("plan_only", "true")));
}

#[test]
fn object_store_request_json_surfaces_blocked_range_status() {
    let (success, output, stderr) = run_object_store_request_plan_json("missing-ranges");

    assert!(!success, "stdout={output} stderr={stderr}");
    assert!(stderr.is_empty(), "stderr={stderr}");
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains(&field(
        "object_store_request_status",
        "blocked_by_range_planning"
    )));
    assert!(output.contains(&field("range_status", "blocked_missing_byte_ranges")));
    assert!(output.contains(&field("requires_byte_ranges", "true")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
}
