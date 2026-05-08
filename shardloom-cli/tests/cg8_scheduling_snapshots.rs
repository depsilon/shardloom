use std::{path::PathBuf, process::Command};

fn run_command(args: &[String]) -> (bool, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("shardloom command runs");
    (
        output.status.success(),
        String::from_utf8(output.stdout).expect("stdout is utf8"),
    )
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

fn fixture_path() -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("shardloom-vortex")
        .join("tests")
        .join("fixtures")
        .join("metadata_footer_u64_20000.vortex")
        .display()
        .to_string()
}

#[test]
fn adaptive_sizing_json_exposes_split_coalesce_policy_without_execution() {
    let (success, output) = run_command(&[
        "vortex-adaptive-sizing".to_string(),
        "file://tmp/data.vortex".to_string(),
        "8".to_string(),
        "--format".to_string(),
        "json".to_string(),
    ]);

    assert!(success, "{output}");
    assert!(output.contains(&field("adaptive_sizing_status", "no_tasks_required")));
    assert!(output.contains(&field("adaptive_sizing_mode", "memory_aware_read_planning")));
    assert!(output.contains(&field("adaptive_splitting_allowed", "true")));
    assert!(output.contains(&field("adaptive_coalescing_allowed", "true")));
    assert!(output.contains(&field("target_task_bytes", "2147483648")));
    assert!(output.contains(&field("max_task_bytes", "4294967296")));
    assert!(output.contains(&field("planned_task_count", "0")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains("\"attempted\":false"));
}

#[test]
fn memory_plan_json_exposes_spill_and_oom_gate_fields_without_io() {
    let (success, output) = run_command(&[
        "vortex-memory-plan".to_string(),
        "file://tmp/data.vortex".to_string(),
        "8".to_string(),
        "--format".to_string(),
        "json".to_string(),
    ]);

    assert!(success, "{output}");
    assert!(output.contains(&field("memory_bridge_status", "no_tasks_required")));
    assert!(output.contains(&field("memory_bridge_mode", "plan_only")));
    assert!(output.contains(&field("memory_budget_total_bytes", "8589934592")));
    assert!(output.contains(&field("memory_budget_soft_limit_bytes", "6871947673")));
    assert!(output.contains(&field("spill_policy", "best_effort")));
    assert!(output.contains(&field("tasks_spill_required_not_implemented", "0")));
    assert!(output.contains(&field("spill_io_performed", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
}

#[test]
fn schedule_plan_json_exposes_bounded_parallel_queue_fields() {
    let (success, output) = run_command(&[
        "vortex-schedule-plan".to_string(),
        "file://tmp/data.vortex".to_string(),
        "8".to_string(),
        "2".to_string(),
        "--format".to_string(),
        "json".to_string(),
    ]);

    assert!(success, "{output}");
    assert!(output.contains(&field("scheduler_bridge_status", "no_tasks_required")));
    assert!(output.contains(&field("scheduler_bridge_mode", "queue_planning")));
    assert!(output.contains(&field("max_parallelism", "2")));
    assert!(output.contains(&field("batch_count", "0")));
    assert!(output.contains(&field("bounded_parallelism_enforced", "true")));
    assert!(output.contains(&field("scheduled_task_count", "0")));
    assert!(output.contains(&field("blocked_task_count", "0")));
    assert!(output.contains(&field("tasks_executed", "false")));
    assert!(output.contains(&field("data_read", "false")));
}

#[test]
fn bounded_local_exec_json_exposes_blocked_guard_fields_without_fallback() {
    let (success, output) = run_command(&[
        "vortex-bounded-local-exec".to_string(),
        fixture_path(),
        "count".to_string(),
        "8".to_string(),
        "2".to_string(),
        "--format".to_string(),
        "json".to_string(),
    ]);

    assert!(!success, "{output}");
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains(&field(
        "bounded_execution_status",
        "blocked_by_missing_estimate"
    )));
    assert!(output.contains(&field("bounded_execution_mode", "blocked")));
    assert!(output.contains(&field("bounded_decision_count", "1")));
    assert!(output.contains(&field("local_execution_status", "missing_metadata")));
    assert!(output.contains(&field("tasks_executed", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("spill_io_performed", "false")));
    assert!(output.contains("\"attempted\":false"));
}
