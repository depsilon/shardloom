use std::process::Command;

fn run_claim_gate_closeout_json(args: &[&str], expect_success: bool) -> String {
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

fn assert_no_runtime_no_fallback_no_effects(output: &str) {
    assert!(output.contains(&field("plan_only", "true")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("query_execution", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("network_probe", "false")));
    assert!(output.contains(&field("catalog_probe", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("external_effects_executed", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("no_runtime", "true")));
    assert!(output.contains(&field("no_fallback", "true")));
    assert!(output.contains(&field("no_effects", "true")));
    assert!(output.contains("\"fallback\":{\"attempted\":false,\"allowed\":false"));
}

#[test]
fn claim_gate_closeout_reports_allowed_blocked_and_out_of_scope_claims() {
    let output = run_claim_gate_closeout_json(&["claim-gate-closeout", "--format", "json"], true);

    assert!(output.contains("\"command\":\"claim-gate-closeout\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "claim_gate_closeout")));
    assert!(output.contains(&field("schema_version", "shardloom.claim_gate_closeout.v1")));
    assert!(output.contains(&field(
        "report_id",
        "cg21_cg22_cg23.claim_gate_release_readiness_closeout"
    )));
    assert!(output.contains(&field("p7_closeout_status", "complete_report_only")));
    assert!(output.contains(&field("claim_gate_status", "blocked_for_broad_claims")));
    assert!(output.contains(&field(
        "release_readiness_status",
        "blocked_until_priority_8"
    )));
    assert!(output.contains(&field("claim_allowed", "false")));
    assert!(output.contains(&field("production_claim_allowed", "false")));
    assert!(output.contains(&field("public_release_claim_allowed", "false")));
    assert!(output.contains(&field("public_package_claim_allowed", "false")));
    assert!(output.contains(&field("comparative_benchmark_claim_allowed", "false")));
    assert!(output.contains(&field("foundry_integration_claim_allowed", "false")));
    assert!(output.contains("report_only_workflow_diagnostics"));
    assert!(output.contains("production_workflow_certification"));
    assert!(output.contains("external_engine_fallback"));
    assert!(output.contains(&field("local_claim_status", "partial_allowed_fixture_only")));
    assert!(output.contains(&field(
        "api_claim_status",
        "report_only_blocked_for_remote_execution"
    )));
    assert!(output.contains(&field("package_claim_status", "blocked_until_priority_8")));
    assert!(output.contains(&field(
        "benchmark_claim_status",
        "blocked_until_comparative_results"
    )));
    assert!(output.contains(&field(
        "integration_claim_status",
        "out_of_scope_until_priority_9"
    )));
    assert!(output.contains("claim_grade_correctness"));
    assert!(output.contains("p8.release.package_artifacts_missing"));
    assert!(output.contains(&field("next_planned_priority", "Priority 8")));
    assert_no_runtime_no_fallback_no_effects(&output);
}

#[test]
fn claim_gate_closeout_rejects_extra_arguments() {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["claim-gate-closeout", "extra", "--format", "json"])
        .output()
        .expect("shardloom command runs");

    assert!(!output.status.success());
    assert!(
        output.stderr.is_empty(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout is utf8");
    assert!(stdout.contains("\"command\":\"claim-gate-closeout\""));
    assert!(stdout.contains("\"status\":\"error\""));
    assert!(stdout.contains("unexpected claim-gate-closeout argument"));
}
