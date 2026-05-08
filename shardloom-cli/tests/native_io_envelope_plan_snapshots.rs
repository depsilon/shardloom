use std::process::Command;

fn run_native_io_envelope_plan_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["native-io-envelope-plan", "--format", "json"])
        .output()
        .expect("native I/O envelope plan command runs");

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
fn native_io_envelope_json_exposes_cg19_contract() {
    let output = run_native_io_envelope_plan_json();

    assert!(output.contains("\"command\":\"native-io-envelope-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "native_io_envelope_plan")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("plan_only", "true")));
    assert!(output.contains(&field("schema_version", "shardloom.native_io_envelope.v1")));
    assert!(output.contains(&field("native_io_envelope_status", "report_only_planned")));
    assert!(output.contains(&field("contract_count", "9")));
    assert!(output.contains(&field("representation_state_count", "10")));
    assert!(output.contains(&field("transition_example_count", "6")));
    assert!(output.contains(&field("certificate_path_requirement_count", "3")));
    assert!(output.contains(&field(
        "contract_kind_order",
        "native_work_envelope,native_work_stream,native_result_stream,source_capability_report,source_pushdown_report,sink_requirement_report,adapter_fidelity_report,materialization_boundary_report,native_io_certificate"
    )));
    assert!(output.contains(&field(
        "representation_state_order",
        "metadata_only,pruned,vortex_encoded,foreign_encoded,selection_vector_encoded,partially_decoded,decoded_columnar,materialized_rows,external_effect,unsupported"
    )));
    assert!(output.contains(&field(
        "transition_example_order",
        "metadata_only->pruned,vortex_encoded->selection_vector_encoded,foreign_encoded->partially_decoded,partially_decoded->decoded_columnar,decoded_columnar->materialized_rows,any->unsupported"
    )));
    assert!(output.contains(&field(
        "certificate_path_order",
        "native_vortex_source_to_native_vortex_sink,compatibility_source_to_native_vortex_sink,multi_source_to_compatibility_sink"
    )));
}

#[test]
fn native_io_envelope_json_preserves_no_execution_or_materialization_effects() {
    let output = run_native_io_envelope_plan_json();

    assert!(output.contains(&field("per_path_certificate_required", "true")));
    assert!(output.contains(&field("aggregate_certificate_not_sufficient", "true")));
    assert!(output.contains(&field(
        "preserve_encoded_or_foreign_encoded_when_possible",
        "true"
    )));
    assert!(output.contains(&field("decoded_arrow_normalization_allowed", "false")));
    assert!(output.contains(&field(
        "materialization_boundary_required_for_decoded_columnar",
        "true"
    )));
    assert!(output.contains(&field("materialization_boundary_required_for_rows", "true")));
    assert!(output.contains(&field("source_pushdown_proof_required", "true")));
    assert!(output.contains(&field("sink_requirement_propagation_required", "true")));
    assert!(output.contains(&field("adapter_fidelity_report_required", "true")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("adapter_probe", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("data_decoded", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("row_read", "false")));
    assert!(output.contains(&field("arrow_converted", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("spill_io_performed", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("production_claim_allowed", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
}
