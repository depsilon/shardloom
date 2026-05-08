use std::process::Command;

fn run_sizing_feedback_plan(args: &[&str]) -> (bool, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("sizing-feedback-plan command runs");
    (
        output.status.success(),
        String::from_utf8(output.stdout).expect("stdout is utf8"),
    )
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

#[test]
fn sizing_feedback_plan_reduces_target_without_applying_feedback() {
    let (success, output) = run_sizing_feedback_plan(&[
        "sizing-feedback-plan",
        "8",
        "memory-pressure-high",
        "--format",
        "json",
    ]);

    assert!(success, "{output}");
    assert!(output.contains(&field("mode", "sizing_feedback_plan")));
    assert!(output.contains(&field("dynamic_sizing_feedback_status", "target_reduced")));
    assert!(output.contains(&field("dynamic_sizing_feedback_mode", "target_adjustment")));
    assert!(output.contains(&field("memory_gb", "8")));
    assert!(output.contains(&field("signal_count", "1")));
    assert!(output.contains(&field("reduce_signal_count", "1")));
    assert!(output.contains(&field("increase_signal_count", "0")));
    assert!(output.contains(&field("current_target_task_bytes", "2147483648")));
    assert!(output.contains(&field("recommended_target_task_bytes", "1073741824")));
    assert!(output.contains(&field("target_task_bytes_changed", "true")));
    assert!(output.contains(&field("tasks_executed", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("feedback_applied", "false")));
    assert!(output.contains("\"attempted\":false"));
}

#[test]
fn sizing_feedback_plan_increases_target_without_execution() {
    let (success, output) = run_sizing_feedback_plan(&[
        "sizing-feedback-plan",
        "8",
        "task-too-small",
        "--format",
        "json",
    ]);

    assert!(success, "{output}");
    assert!(output.contains(&field("dynamic_sizing_feedback_status", "target_increased")));
    assert!(output.contains(&field("reduce_signal_count", "0")));
    assert!(output.contains(&field("increase_signal_count", "1")));
    assert!(output.contains(&field("recommended_target_task_bytes", "4294967296")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
}

#[test]
fn sizing_feedback_plan_rejects_unknown_signal_without_fallback() {
    let (success, output) =
        run_sizing_feedback_plan(&["sizing-feedback-plan", "8", "unknown", "--format", "json"]);

    assert!(!success, "{output}");
    assert!(output.contains("\"status\":\"error\""));
    assert!(output.contains("invalid sizing feedback signal token: unknown"));
    assert!(output.contains("\"attempted\":false"));
    assert!(output.contains("\"allowed\":false"));
}

#[test]
fn sizing_feedback_plan_rejects_extra_signal_argument_without_fallback() {
    let (success, output) = run_sizing_feedback_plan(&[
        "sizing-feedback-plan",
        "8",
        "task-too-large",
        "task-too-small",
        "--format",
        "json",
    ]);

    assert!(!success, "{output}");
    assert!(output.contains("\"status\":\"error\""));
    assert!(output.contains("sizing-feedback-plan unknown argument/value: task-too-small"));
    assert!(output.contains("\"attempted\":false"));
    assert!(output.contains("\"allowed\":false"));
}
