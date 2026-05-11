use std::process::Command;

fn run_generalized_encoded_primitive_gate_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "vortex-generalized-encoded-primitive-gate",
            "--format",
            "json",
        ])
        .output()
        .expect("generalized encoded primitive gate command runs");

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
fn generalized_encoded_primitive_gate_json_exposes_current_boundary() {
    let output = run_generalized_encoded_primitive_gate_json();

    assert!(output.contains("\"command\":\"vortex-generalized-encoded-primitive-gate\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "vortex_generalized_encoded_primitive_gate")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("plan_only", "true")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.vortex_generalized_encoded_primitive_gate.v1"
    )));
    assert!(output.contains(&field(
        "report_id",
        "vortex.cg2.generalized-encoded-primitive-gate"
    )));
    assert!(output.contains(&field("gate_status", "generalized_execution_blocked")));
    assert!(output.contains(&field("entry_count", "3")));
    assert!(output.contains(&field(
        "primitive_order",
        "direct_count,filtered_count,projection"
    )));
    assert!(output.contains(&field(
        "primitive_statuses",
        "local_direct_count_evidence,prepared_encoded_filter_evidence,local_projection_scan_pushdown_evidence"
    )));
}

#[test]
fn generalized_encoded_primitive_gate_json_preserves_no_runtime_widening() {
    let output = run_generalized_encoded_primitive_gate_json();

    assert!(output.contains(&field("local_count_all_only", "false")));
    assert!(output.contains(&field("entries_with_local_count_support", "1")));
    assert!(output.contains(&field(
        "entries_with_local_filter_scan_pushdown_support",
        "1"
    )));
    assert!(output.contains(&field(
        "entries_with_prepared_encoded_filter_execution_support",
        "1"
    )));
    assert!(output.contains(&field(
        "entries_with_local_projection_scan_pushdown_support",
        "1"
    )));
    assert!(output.contains(&field("entries_with_metadata_proof", "2")));
    assert!(output.contains(&field("entries_with_readiness_contract", "3")));
    assert!(output.contains(&field("implementation_blocker_count", "9")));
    assert!(output.contains(&field("required_next_evidence_count", "12")));
    assert!(output.contains(&field("generalized_count_ready", "false")));
    assert!(output.contains(&field("filtered_count_execution_ready", "false")));
    assert!(output.contains(&field("projection_execution_ready", "false")));
    assert!(output.contains(&field("requires_public_scan_or_read_start_path", "true")));
    assert!(output.contains(&field("requires_encoded_predicate_path", "true")));
    assert!(output.contains(&field("requires_encoded_projection_path", "true")));
    assert!(output.contains(&field("requires_selection_vector_pipeline", "true")));
}

#[test]
fn generalized_encoded_primitive_gate_json_preserves_no_work_and_no_fallback() {
    let output = run_generalized_encoded_primitive_gate_json();

    assert!(output.contains(&field("requires_native_io_certificate", "true")));
    assert!(output.contains(&field("requires_execution_certificate", "true")));
    assert!(output.contains(&field("requires_correctness_evidence", "true")));
    assert!(output.contains(&field("requires_benchmark_evidence", "true")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("data_decoded", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("row_read", "false")));
    assert!(output.contains(&field("arrow_converted", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("spill_io_performed", "false")));
    assert!(output.contains(&field("runtime_execution_allowed", "false")));
    assert!(output.contains(&field("external_engine_execution", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("production_claim_allowed", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
    assert!(output.contains(&field("diagnostic_count", "1")));
}
