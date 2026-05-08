use std::process::Command;

fn run_cpu_specialization_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["cpu-specialization-plan", "--format", "json"])
        .output()
        .expect("cpu specialization command runs");

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
fn cpu_specialization_json_exposes_cg15_foundation_contract() {
    let output = run_cpu_specialization_json();

    assert!(output.contains("\"command\":\"cpu-specialization-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "cpu_operator_specialization_plan")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("plan_only", "true")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.cpu_operator_specialization.v1"
    )));
    assert!(output.contains(&field("cpu_specialization_status", "report_only_planned")));
    assert!(output.contains(&field("entry_count", "6")));
    assert!(output.contains(&field("specialization_candidate_count", "6")));
    assert!(output.contains(&field("simd_candidate_count", "4")));
    assert!(output.contains(&field("cache_aware_candidate_count", "4")));
    assert!(output.contains(&field("encoded_layout_aware_candidate_count", "3")));
    assert!(output.contains(&field(
        "operator_order",
        "filter,project,count_aggregate,aggregate,sort,join"
    )));
    assert!(output.contains(&field(
        "kernel_kind_order",
        "encoded,encoded,encoded,partial_decode,partial_decode,partial_decode"
    )));
}

#[test]
fn cpu_specialization_json_preserves_evidence_gates_and_no_execution() {
    let output = run_cpu_specialization_json();

    assert!(output.contains(&field("correctness_evidence_required", "true")));
    assert!(output.contains(&field("benchmark_evidence_required", "true")));
    assert!(output.contains(&field("cpu_feature_guard_required", "true")));
    assert!(output.contains(&field("portable_native_baseline_required", "true")));
    assert!(output.contains(&field("deterministic_dispatch_required", "true")));
    assert!(output.contains(&field("host_cpu_probe", "false")));
    assert!(output.contains(&field("runtime_dispatch_implemented", "false")));
    assert!(output.contains(&field("unsafe_code_required", "false")));
    assert!(output.contains(&field("gpu_required", "false")));
    assert!(output.contains(&field("fpga_required", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("data_decoded", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("row_read", "false")));
    assert!(output.contains(&field("arrow_converted", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("spill_io_performed", "false")));
    assert!(output.contains(&field("external_engine_execution", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("production_claim_allowed", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
}
