use std::process::Command;

fn run_optimizer_plan_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["optimizer-plan", "--format", "json"])
        .output()
        .expect("optimizer plan command runs");

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
fn optimizer_plan_json_exposes_gar_perf_2b_trace_contract() {
    let output = run_optimizer_plan_json();

    assert!(output.contains("\"command\":\"optimizer-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "evidence_aware_optimizer_trace")));
    assert!(output.contains(&field("gar_id", "GAR-PERF-2B")));
    assert!(output.contains(&field("support_status", "report_only")));
    assert!(output.contains(&field("claim_gate_status", "not_claim_grade")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.evidence_aware_optimizer_trace.v1"
    )));
    assert!(output.contains(&field(
        "optimizer_registry_version",
        "gar-perf-2b.optimizer_registry.v1"
    )));
    assert!(output.contains(&field("optimizer_phase", "logical")));
    assert!(output.contains(&field("optimizer_rule_count", "9")));
    assert!(output.contains(&field("optimizer_rule_applied_count", "0")));
    assert!(output.contains(&field("optimizer_rule_admitted_count", "1")));
    assert!(output.contains(&field("optimizer_rule_blocked_count", "3")));
    assert!(output.contains(&field("optimizer_rule_unsupported_count", "2")));
    assert!(output.contains(&field("optimizer_rule_not_applicable_count", "1")));
    assert!(output.contains(&field("optimizer_rule_report_only_count", "2")));
    assert!(output.contains(&field(
        "optimizer_rule_status_vocabulary",
        "admitted,applied,blocked,unsupported,not_applicable,report_only"
    )));
}

#[test]
fn optimizer_plan_rows_preserve_no_fallback_and_no_effects() {
    let output = run_optimizer_plan_json();

    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("optimizer_execution", "false")));
    assert!(output.contains(&field("plan_rewritten", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("data_decoded", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("performance_claim_allowed", "false")));
    assert!(output.contains(&field("broad_sql_dataframe_claim_allowed", "false")));
    assert!(output.contains(&field("all_no_fallback_no_external_engine", "true")));
    assert!(output.contains(&field(
        "optimizer_rule_common_subplan_source_state_reuse_status",
        "admitted"
    )));
    assert!(output.contains(&field(
        "optimizer_rule_common_subplan_source_state_reuse_applied",
        "false"
    )));
    assert!(output.contains(&field(
        "optimizer_rule_common_subplan_source_state_reuse_source_state_reuse_admitted",
        "true"
    )));
    assert!(output.contains(&field(
        "optimizer_rule_predicate_pushdown_before_plan_digest",
        "not_emitted_report_only"
    )));
    assert!(output.contains(&field(
        "optimizer_rule_predicate_pushdown_after_plan_digest",
        "not_emitted_report_only"
    )));
    assert!(output.contains(&field(
        "optimizer_rule_join_ordering_rewrite_safety_status",
        "blocked_unsupported_semantics"
    )));
    assert!(output.contains(&field(
        "optimizer_rule_cardinality_estimation_cardinality_estimation_status",
        "not_needed"
    )));
}
