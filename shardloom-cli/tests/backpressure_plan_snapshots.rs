use std::process::Command;

fn run_backpressure_plan(args: &[&str]) -> (bool, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("backpressure-plan command runs");
    (
        output.status.success(),
        String::from_utf8(output.stdout).expect("stdout is utf8"),
    )
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

#[test]
fn backpressure_plan_json_exposes_bounded_memory_policy_without_execution() {
    let (success, output) =
        run_backpressure_plan(&["backpressure-plan", "8", "4", "16", "--format", "json"]);

    assert!(success, "{output}");
    assert!(output.contains(&field("mode", "backpressure_plan")));
    assert!(output.contains(&field("backpressure_status", "bounded")));
    assert!(output.contains(&field("backpressure_mode", "bounded_streaming")));
    assert!(output.contains(&field("bounded", "true")));
    assert!(output.contains(&field("memory_required", "true")));
    assert!(output.contains(&field("max_parallelism", "4")));
    assert!(output.contains(&field("max_in_flight_chunks", "4")));
    assert!(output.contains(&field("max_buffered_bytes", "8589934592")));
    assert!(output.contains(&field("estimated_chunk_bytes", "16777216")));
    assert!(output.contains(&field("streams_executed", "false")));
    assert!(output.contains(&field("tasks_executed", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("spill_io_performed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("streaming_capability_matrix_row_count", "8")));
    assert!(output.contains(&field(
        "streaming_capability_matrix_row_bounded_backpressure_plan_support_status",
        "report_only"
    )));
    assert!(output.contains(&field(
        "streaming_capability_matrix_all_blocked_rows_have_diagnostics",
        "true"
    )));
    assert!(output.contains("\"attempted\":false"));
}

#[test]
fn backpressure_plan_rejects_zero_parallelism_without_fallback() {
    let (success, output) =
        run_backpressure_plan(&["backpressure-plan", "8", "0", "--format", "json"]);

    assert!(!success, "{output}");
    assert!(output.contains("\"status\":\"error\""));
    assert!(output.contains("max_parallelism must be greater than zero"));
    assert!(output.contains("\"attempted\":false"));
    assert!(output.contains("\"allowed\":false"));
}
