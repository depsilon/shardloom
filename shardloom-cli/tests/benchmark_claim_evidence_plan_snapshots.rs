use std::process::Command;

fn run_benchmark_claim_evidence_json(scope: Option<&str>) -> String {
    let mut args = vec!["benchmark-claim-evidence-plan"];
    if let Some(scope) = scope {
        args.push(scope);
    }
    args.extend(["--format", "json"]);
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("benchmark claim evidence command runs");

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
fn foundation_claim_evidence_lists_publication_blockers() {
    let output = run_benchmark_claim_evidence_json(None);

    assert!(output.contains("\"command\":\"benchmark-claim-evidence-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "benchmark_claim_evidence")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.benchmark_claim_evidence.v1"
    )));
    assert!(output.contains(&field(
        "report_id",
        "cg6.benchmark_claim_evidence.aggregate"
    )));
    assert!(output.contains(&field("scope", "foundation")));
    assert!(output.contains(&field("claim_evidence_status", "needs_evidence")));
    assert!(output.contains(&field(
        "surface_order",
        "benchmark_plan,required_metrics,correctness_evidence,benchmark_result_rows,external_comparison_results,comparison_report,reproducibility_manifest,no_fallback_policy,claim_publication_gate"
    )));
    assert!(output.contains(&field("planned_surface_count", "4")));
    assert!(output.contains(&field("blocked_surface_count", "5")));
    assert!(output.contains(&field(
        "blocked_surface_order",
        "correctness_evidence,benchmark_result_rows,external_comparison_results,reproducibility_manifest,claim_publication_gate"
    )));
    assert!(output.contains(&field("scenario_count", "7")));
    assert!(output.contains(&field("required_metric_count", "21")));
    assert!(output.contains(&field("expected_result_count", "14")));
    assert!(output.contains(&field("result_count", "0")));
    assert!(output.contains(&field("missing_result_count", "14")));
    assert!(output.contains(&field("run_manifest_status", "incomplete")));
    assert!(output.contains(&field("comparison_report_status", "evidence_missing")));
}

#[test]
fn foundation_claim_evidence_preserves_no_execution_and_no_claims() {
    let output = run_benchmark_claim_evidence_json(None);

    assert!(output.contains(&field("claim_gate_status", "evidence_missing")));
    assert!(output.contains(&field("claim_gate_correctness_evidence", "missing")));
    assert!(output.contains(&field("claim_gate_benchmark_evidence", "missing")));
    assert!(output.contains(&field("claim_gate_required_metrics", "present")));
    assert!(output.contains(&field("claim_gate_comparison_report", "present")));
    assert!(output.contains(&field("claim_gate_reproducibility_evidence", "missing")));
    assert!(output.contains(&field("benchmark_execution_implemented", "false")));
    assert!(output.contains(&field("benchmark_execution_performed", "false")));
    assert!(output.contains(&field("external_engine_execution", "false")));
    assert!(output.contains(&field("query_execution", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("baselines_fallback_free", "true")));
    assert!(output.contains(&field("performance_claim_allowed", "false")));
    assert!(output.contains(&field("superiority_claim_allowed", "false")));
    assert!(output.contains(&field("best_default_claim_allowed", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
}

#[test]
fn traditional_claim_evidence_lists_dataframe_baseline_gaps() {
    let output = run_benchmark_claim_evidence_json(Some("traditional-analytics"));

    assert!(output.contains(&field("scope", "traditional-analytics")));
    assert!(output.contains(&field("scenario_count", "5")));
    assert!(output.contains(&field(
        "scenario_name_order",
        "csv/file ingest,selective filter,group by aggregation,sort and top-k,hash join"
    )));
    assert!(output.contains(&field(
        "baseline_engine_order",
        "shardloom,pandas,polars,duckdb,spark,datafusion,dask"
    )));
    assert!(output.contains(&field(
        "external_baseline_engine_order",
        "pandas,polars,duckdb,spark,datafusion,dask"
    )));
    assert!(output.contains(&field("external_baseline_count", "6")));
    assert!(output.contains(&field("expected_result_count", "35")));
    assert!(output.contains(&field("missing_result_count", "35")));
    assert!(output.contains(&field("missing_external_result_count", "30")));
    assert!(output.contains(&field("performance_claim_allowed", "false")));
}
