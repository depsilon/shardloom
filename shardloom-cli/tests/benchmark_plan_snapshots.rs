use std::process::Command;

fn run_benchmark_plan_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["benchmark-plan", "--format", "json"])
        .output()
        .expect("benchmark plan command runs");

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
fn benchmark_plan_json_exposes_scenario_inventory() {
    let output = run_benchmark_plan_json();

    assert!(output.contains("\"command\":\"benchmark-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "benchmark_plan")));
    assert!(output.contains(&field("status", "planned")));
    assert!(output.contains(&field("scenario_count", "5")));
    assert!(output.contains(&field(
        "scenario_name_order",
        "single-node encoded execution,massive object-store scan,incremental recomputation,native output and translation,failure and unsupported behavior"
    )));
    assert!(output.contains(&field(
        "workload_class_order",
        "single_node_encoded_execution,massive_object_store_scan,incremental_recomputation,native_output_and_translation,failure_and_unsupported_behavior"
    )));
    assert!(output.contains(&field(
        "correctness_validation_order",
        "expected_output,property_based,unsupported_diagnostic_only"
    )));
    assert!(output.contains(&field("scenario_with_correctness_validation_count", "5")));
    assert!(output.contains(&field("scenario_with_required_metrics_count", "5")));
    assert!(output.contains(&field("scenario_with_baselines_count", "5")));
}

#[test]
fn benchmark_plan_json_exposes_metric_coverage_inventory() {
    let output = run_benchmark_plan_json();

    assert!(output.contains(&field("required_metric_count", "21")));
    assert!(output.contains(&field(
        "required_metric_order",
        "startup_latency_millis,wall_time_millis,query_runtime_millis,peak_memory_bytes,bytes_read,bytes_decoded,bytes_decode_avoided,rows_materialization_avoided,segments_pruned,work_avoided_units,spill_required_bytes,spill_avoided_bytes,segments_considered,object_store_requests,rows_materialized,cost_proxy,write_commit_latency_millis,bytes_written,output_files,output_bytes,segments_metadata_answered"
    )));
    assert!(output.contains(&field("required_foundation_metric_count", "21")));
    assert!(output.contains(&field("covered_required_foundation_metric_count", "21")));
    assert!(output.contains(&field("missing_required_foundation_metrics", "")));
    assert!(output.contains(&field("required_foundation_metrics_covered", "true")));
    assert!(output.contains(&field("runtime_metrics_covered", "true")));
    assert!(output.contains(&field("peak_memory_metric_covered", "true")));
    assert!(output.contains(&field("bytes_read_written_metrics_covered", "true")));
    assert!(output.contains(&field("startup_latency_metric_covered", "true")));
    assert!(output.contains(&field("query_runtime_metric_covered", "true")));
    assert!(output.contains(&field("write_commit_latency_metric_covered", "true")));
    assert!(output.contains(&field("spill_metrics_covered", "true")));
    assert!(output.contains(&field("object_store_request_metric_covered", "true")));
    assert!(output.contains(&field("materialization_metrics_covered", "true")));
}

#[test]
fn benchmark_plan_json_preserves_no_claim_no_fallback_boundaries() {
    let output = run_benchmark_plan_json();

    assert!(output.contains(&field("benchmark_execution_implemented", "false")));
    assert!(output.contains(&field("performance_claim_allowed", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("external_baselines", "comparison_only")));
    assert!(output.contains(&field(
        "baseline_engine_order",
        "shardloom,datafusion,spark,polars,vortex_integration,other"
    )));
    assert!(output.contains(&field(
        "external_baseline_engine_order",
        "datafusion,spark,polars,vortex_integration,other"
    )));
    assert!(output.contains(&field("external_baseline_count", "5")));
    assert!(output.contains(&field("expected_result_count", "10")));
    assert!(output.contains(&field("claim_gate_status", "evidence_missing")));
    assert!(output.contains(&field("claim_gate_correctness_evidence", "missing")));
    assert!(output.contains(&field("claim_gate_benchmark_evidence", "missing")));
    assert!(output.contains(&field("claim_gate_required_metrics", "present")));
    assert!(output.contains(&field("claim_gate_comparison_report", "missing")));
    assert!(output.contains(&field("claim_gate_reproducibility_evidence", "missing")));
    assert!(output.contains(&field("claim_gate_fallback", "not_attempted")));
    assert!(output.contains(&field("baselines_fallback_free", "true")));
}
