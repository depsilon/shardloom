use std::process::Command;

fn run_execution_certificate_plan_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["execution-certificate-plan", "--format", "json"])
        .output()
        .expect("execution certificate plan command runs");

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
fn execution_certificate_plan_json_exposes_cg16_artifact_surface() {
    let output = run_execution_certificate_plan_json();

    assert!(output.contains("\"command\":\"execution-certificate-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "execution_certificate_plan")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("plan_only", "true")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.execution_certificate_evidence_surface.v1"
    )));
    assert!(output.contains(&field("certificate_surface_status", "report_only_planned")));
    assert!(output.contains(&field(
        "certificate_schema_version",
        "shardloom.execution_certificate.v1"
    )));
    assert!(output.contains(&field("artifact_count", "6")));
    assert!(output.contains(&field("required_artifact_count", "6")));
    assert!(output.contains(&field("hash_required_count", "6")));
    assert!(output.contains(&field("machine_readable_required_count", "6")));
    assert!(output.contains(&field("plan_artifact_count", "1")));
    assert!(output.contains(&field("input_artifact_count", "1")));
    assert!(output.contains(&field("output_artifact_count", "1")));
    assert!(output.contains(&field("segment_trace_artifact_count", "1")));
    assert!(output.contains(&field("side_effect_manifest_artifact_count", "1")));
    assert!(output.contains(&field("reproducibility_metadata_artifact_count", "1")));
    assert!(output.contains(&field(
        "artifact_order",
        "plan,input_snapshot,output_payload,segment_trace,side_effect_manifest,reproducibility_metadata"
    )));
}

#[test]
fn execution_certificate_plan_json_preserves_no_execution_boundaries() {
    let output = run_execution_certificate_plan_json();

    assert!(output.contains(&field("plan_hash_required", "true")));
    assert!(output.contains(&field("input_snapshot_hash_required", "true")));
    assert!(output.contains(&field("output_hash_required", "true")));
    assert!(output.contains(&field("selected_segment_trace_required", "true")));
    assert!(output.contains(&field("skipped_segment_trace_required", "true")));
    assert!(output.contains(&field("side_effect_manifest_required", "true")));
    assert!(output.contains(&field("reproducibility_metadata_required", "true")));
    assert!(output.contains(&field("correctness_fixture_required", "true")));
    assert!(output.contains(&field("machine_readable_certificate_surface", "true")));
    assert!(output.contains(&field("deterministic_field_order_required", "true")));
    assert!(output.contains(&field("certificate_evaluation_performed", "false")));
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
