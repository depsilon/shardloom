use std::process::Command;

fn field(key: &str, value: &str) -> String {
    format!("\"key\":\"{key}\",\"value\":\"{value}\"")
}

fn run_route(args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("run shardloom route");
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("stdout is utf8")
}

#[test]
fn public_route_emits_side_effect_free_local_file_route() {
    let stdout = run_route(&[
        "route",
        "dataframe",
        "--input",
        "target/input.csv",
        "--input-format",
        "csv",
        "--plan",
        "read_csv(target/input.csv) -> select(id) -> limit(10)",
        "--request",
        "collect",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"success\""));
    assert!(stdout.contains(&field(
        "public_workflow_route_schema_version",
        "shardloom.public_workflow_route.v1"
    )));
    assert!(stdout.contains(&field("route_id", "local_file_direct_query")));
    assert!(stdout.contains(&field(
        "resolved_internal_command",
        "sql-local-source-smoke"
    )));
    assert!(stdout.contains(&field("surface", "dataframe")));
    assert!(stdout.contains(&field("source_format", "csv")));
    assert!(stdout.contains(&field("start_state", "compatibility_local_source")));
    assert!(stdout.contains(&field("vortex_normalization_point", "direct_transient")));
    assert!(stdout.contains(&field("execution_mode", "direct")));
    assert!(stdout.contains(&field("preparation_included", "false")));
    assert!(stdout.contains(&field("query_timing_starts_after_preparation", "false")));
    assert!(stdout.contains(&field("runtime_execution", "false")));
    assert!(stdout.contains(&field("source_io_performed", "false")));
    assert!(stdout.contains(&field("output_io_performed", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}

#[test]
fn public_route_blocks_unbounded_collect_before_execution() {
    let stdout = run_route(&[
        "route",
        "python",
        "--input",
        "target/input.csv",
        "--input-format",
        "csv",
        "--plan",
        "read_csv(target/input.csv)",
        "--request",
        "collect",
        "--format",
        "json",
    ]);

    assert!(stdout.contains("\"command\":\"route\""));
    assert!(stdout.contains("\"status\":\"unsupported\""));
    assert!(stdout.contains(&field("route_id", "blocked")));
    assert!(stdout.contains(&field("blocker_id", "cg21.route.unbounded_collect_blocked")));
    assert!(stdout.contains(&field("resolved_internal_command", "not_resolved")));
    assert!(stdout.contains(&field("runtime_execution", "false")));
    assert!(stdout.contains(&field("fallback_attempted", "false")));
    assert!(stdout.contains(&field("external_engine_invoked", "false")));
}
