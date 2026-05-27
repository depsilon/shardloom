use std::process::Command;

fn run_benchmark_constitution_json(scope: Option<&str>) -> String {
    let mut args = vec!["benchmark-constitution"];
    if let Some(scope) = scope {
        args.push(scope);
    }
    args.extend(["--format", "json"]);
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("benchmark constitution command runs");

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
fn foundation_constitution_lists_fail_closed_requirements() {
    let output = run_benchmark_constitution_json(None);

    assert!(output.contains("\"command\":\"benchmark-constitution\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "benchmark_constitution")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.benchmark_constitution_validation.v1"
    )));
    assert!(output.contains(&field(
        "report_id",
        "review-p1-3.benchmark_constitution_validation"
    )));
    assert!(output.contains(&field("scope", "foundation")));
    assert!(output.contains(&field("benchmark_constitution_status", "missing_evidence")));
    assert!(output.contains(&field("benchmark_constitution_support_status", "blocked")));
    assert!(output.contains(&field(
        "benchmark_constitution_claim_gate_status",
        "evidence_missing"
    )));
    assert!(output.contains(&field(
        "benchmark_constitution_required_field_order",
        "benchmark_result_row,dataset_source_admission,preparation_route,execution_route,output_route,correctness_proof,hardware_profile,build_profile,cold_warm_state,stage_timings,cost_unit_fields,no_fallback_proof,external_baseline_boundary"
    )));
    assert!(output.contains(&field(
        "benchmark_constitution_missing_field_order",
        "benchmark_result_row,dataset_source_admission,preparation_route,execution_route,output_route,correctness_proof,hardware_profile,build_profile,cold_warm_state,stage_timings,cost_unit_fields"
    )));
    assert!(output.contains(&field("benchmark_constitution_required_field_count", "13")));
    assert!(output.contains(&field("benchmark_constitution_row_count", "14")));
    assert!(output.contains(&field("benchmark_constitution_shardloom_row_count", "7")));
    assert!(output.contains(&field(
        "benchmark_constitution_external_baseline_row_count",
        "7"
    )));
}

#[test]
fn foundation_constitution_preserves_no_execution_and_no_claims() {
    let output = run_benchmark_constitution_json(None);

    assert!(output.contains(&field(
        "benchmark_constitution_dataset_source_admission_present",
        "false"
    )));
    assert!(output.contains(&field(
        "benchmark_constitution_preparation_route_present",
        "false"
    )));
    assert!(output.contains(&field(
        "benchmark_constitution_execution_route_present",
        "false"
    )));
    assert!(output.contains(&field(
        "benchmark_constitution_output_route_present",
        "false"
    )));
    assert!(output.contains(&field(
        "benchmark_constitution_correctness_proof_present",
        "false"
    )));
    assert!(output.contains(&field(
        "benchmark_constitution_hardware_build_metadata_present",
        "false"
    )));
    assert!(output.contains(&field(
        "benchmark_constitution_no_fallback_proof_present",
        "true"
    )));
    assert!(output.contains(&field(
        "benchmark_constitution_external_baselines_comparison_only",
        "true"
    )));
    assert!(output.contains(&field(
        "benchmark_constitution_benchmark_execution_performed",
        "false"
    )));
    assert!(output.contains(&field(
        "benchmark_constitution_external_engine_execution",
        "false"
    )));
    assert!(output.contains(&field(
        "benchmark_constitution_performance_claim_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "benchmark_constitution_superiority_claim_allowed",
        "false"
    )));
    assert!(output.contains(&field("benchmark_constitution_fallback_attempted", "false")));
    assert!(output.contains(&field(
        "benchmark_constitution_external_engine_invoked",
        "false"
    )));
    assert!(output.contains(&field("benchmark_constitution_side_effect_free", "true")));
}

#[test]
fn traditional_constitution_keeps_external_baselines_baseline_only() {
    let output = run_benchmark_constitution_json(Some("traditional-analytics"));

    assert!(output.contains(&field("scope", "traditional-analytics")));
    assert!(output.contains(&field("benchmark_constitution_row_count", "40")));
    assert!(output.contains(&field("benchmark_constitution_shardloom_row_count", "5")));
    assert!(output.contains(&field(
        "benchmark_constitution_external_baseline_row_count",
        "35"
    )));
    assert!(output.contains(&field(
        "benchmark_constitution_external_baselines_comparison_only",
        "true"
    )));
    assert!(output.contains(&field(
        "benchmark_constitution_performance_claim_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "benchmark_constitution_row_csv_file_ingest__spark_row_classification",
        "external_baseline_only"
    )));
    assert!(output.contains(&field(
        "benchmark_constitution_row_hash_join__shardloom_fallback_attempted",
        "false"
    )));
}
