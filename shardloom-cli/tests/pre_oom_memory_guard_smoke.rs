use std::process::Command;

fn run_pre_oom_memory_guard_smoke_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["pre-oom-memory-guard-smoke", "--format", "json"])
        .output()
        .expect("pre-OOM memory guard smoke command runs");

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
fn pre_oom_memory_guard_smoke_denies_before_oom_and_cleans_up() {
    let output = run_pre_oom_memory_guard_smoke_json();

    assert!(output.contains("\"command\":\"pre-oom-memory-guard-smoke\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains("\"code\":\"SL_RESOURCE_BUDGET_EXCEEDED\""));
    assert!(output.contains(&field("mode", "pre_oom_memory_guard_smoke")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.pre_oom_memory_guard_fixture.v1"
    )));
    assert!(output.contains(&field(
        "report_id",
        "gar-runtime-impl-6d.pre_oom_memory_guard_fixture"
    )));
    assert!(output.contains(&field("fixture_id", "memory.pre_oom.denial.fixture.v1")));
    assert!(output.contains(&field("operator_class", "join")));
    assert!(output.contains(&field("spill_policy", "force_before_oom")));
    assert!(output.contains(&field("memory_budget_bytes", "1024")));
    assert!(output.contains(&field("memory_soft_limit_bytes", "512")));
    assert!(output.contains(&field("memory_hard_limit_bytes", "768")));
    assert!(output.contains(&field("granted_reservation_bytes", "512")));
    assert!(output.contains(&field("denied_request_bytes", "512")));
    assert!(output.contains(&field("reserved_before_denial_bytes", "512")));
    assert!(output.contains(&field("reserved_after_denial_bytes", "512")));
    assert!(output.contains(&field("reserved_after_cleanup_bytes", "0")));
    assert!(output.contains(&field("pressure_before_denial", "high")));
    assert!(output.contains(&field("pressure_after_denial", "high")));
    assert!(output.contains(&field("admission_decision", "denied_before_oom")));
    assert!(output.contains(&field("diagnostic_code", "SL_RESOURCE_BUDGET_EXCEEDED")));
    assert!(output.contains(&field("diagnostic_count", "1")));
    assert!(output.contains(&field("fail_before_oom", "true")));
    assert!(output.contains(&field("release_performed", "true")));
    assert!(output.contains(&field("cleanup_required", "true")));
    assert!(output.contains(&field("cleanup_completed", "true")));
    assert!(output.contains(&field("guard_triggered", "true")));
    assert!(output.contains(&field("has_unexpected_errors", "false")));
}

#[test]
fn pre_oom_memory_guard_smoke_keeps_spill_distribution_and_fallback_blocked() {
    let output = run_pre_oom_memory_guard_smoke_json();

    for key in [
        "real_query_spill_admitted",
        "distributed_execution_admitted",
        "native_spill_write_performed",
        "native_spill_read_performed",
        "spill_io_performed",
        "object_store_io",
        "write_io",
        "tasks_executed",
        "data_read",
        "data_materialized",
        "fallback_execution_allowed",
        "fallback_attempted",
        "external_engine_invoked",
    ] {
        assert!(
            output.contains(&field(key, "false")),
            "missing false field {key}"
        );
    }
    assert!(output.contains(&field("runtime_execution", "true")));
}
