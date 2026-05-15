use std::process::Command;

fn run_adaptive_optimizer_memory_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["optimizer-adaptive-memory-plan", "--format", "json"])
        .output()
        .expect("adaptive optimizer memory command runs");

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
fn adaptive_optimizer_memory_json_exposes_cg14_decision_contract() {
    let output = run_adaptive_optimizer_memory_json();

    assert!(output.contains("\"command\":\"optimizer-adaptive-memory-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "optimizer_adaptive_memory_plan")));
    assert!(output.contains(&field("gar_id", "GAR-0016-A")));
    assert!(output.contains(&field("support_status", "report_only")));
    assert!(output.contains(&field("claim_gate_status", "not_claim_grade")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.adaptive_optimizer_memory.v1"
    )));
    assert!(output.contains(&field("adaptive_optimizer_status", "report_only_planned")));
    assert!(output.contains(&field("optimizer_phase", "runtime_adaptive")));
    assert!(output.contains(&field("rule_decision_count", "3")));
    assert!(output.contains(&field("deferred_rule_count", "3")));
    assert!(output.contains(&field("runtime_filter_count", "1")));
    assert!(output.contains(&field("conservative_runtime_filter_count", "1")));
    assert!(output.contains(&field("adaptive_decision_count", "2")));
    assert!(output.contains(&field("skew_signal_count", "1")));
    assert!(output.contains(&field(
        "adaptive_runtime_gate_surface_order",
        "runtime_filter,dynamic_pruning,skew_signal,adaptive_parallelism,compaction_write"
    )));
    assert!(output.contains(&field("runtime_gate_prerequisite_count", "11")));
    assert!(output.contains(&field(
        "runtime_gate_prerequisite_order",
        "conservative_runtime_filter_proof,runtime_fact_evidence,bounded_memory_budget,spill_policy,skew_signal_measurement,adaptive_parallelism_policy,compaction_plan_evidence,write_intent,execution_certificate,native_io_certificate,no_fallback_evidence"
    )));
}

#[test]
fn adaptive_optimizer_memory_json_keeps_runtime_and_fallback_disabled() {
    let output = run_adaptive_optimizer_memory_json();

    assert!(output.contains(&field("conservative_runtime_filter_required", "true")));
    assert!(output.contains(&field("dynamic_pruning_requires_proof", "true")));
    assert!(output.contains(&field("memory_budget_required", "true")));
    assert!(output.contains(&field("bounded_memory_required", "true")));
    assert!(output.contains(&field("spill_policy_required", "true")));
    assert!(output.contains(&field("deterministic_oom_boundary", "true")));
    assert!(output.contains(&field("runtime_fact_required_before_adaptation", "true")));
    assert!(output.contains(&field("adaptive_parallelism_required", "true")));
    assert!(output.contains(&field("compaction_write_boundary_required", "true")));
    assert!(output.contains(&field(
        "runtime_filter_execution_status",
        "report_only_blocked"
    )));
    assert!(output.contains(&field(
        "skew_handling_execution_status",
        "report_only_blocked"
    )));
    assert!(output.contains(&field(
        "adaptive_parallelism_execution_status",
        "report_only_blocked"
    )));
    assert!(output.contains(&field(
        "compaction_write_execution_status",
        "report_only_blocked"
    )));
    assert!(output.contains(&field("optimizer_execution", "false")));
    assert!(output.contains(&field("runtime_adaptation_applied", "false")));
    assert!(output.contains(&field("runtime_filter_built", "false")));
    assert!(output.contains(&field("runtime_filter_applied", "false")));
    assert!(output.contains(&field("adaptive_parallelism_applied", "false")));
    assert!(output.contains(&field("compaction_write_allowed", "false")));
    assert!(output.contains(&field("compaction_execution_allowed", "false")));
    assert!(output.contains(&field("plan_rewritten", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("spill_io_performed", "false")));
    assert!(output.contains(&field("external_engine_execution", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("production_claim_allowed", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
}
