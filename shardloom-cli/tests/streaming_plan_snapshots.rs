use std::process::Command;

fn run_streaming_plan(dataset_uri: &str, target_uri: &str) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args([
            "streaming-plan",
            dataset_uri,
            target_uri,
            "--format",
            "json",
        ])
        .output()
        .expect("streaming-plan command runs");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("stdout is utf8")
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

#[test]
fn streaming_plan_vortex_target_preserves_encoded_native_boundary() {
    let output = run_streaming_plan("file:///tmp/input.vortex", "file:///tmp/out.vortex");

    assert!(output.contains(&field("mode", "plan_only")));
    assert!(output.contains(&field("status", "planned")));
    assert!(output.contains(&field("source_kind", "vortex_segment")));
    assert!(output.contains(&field("source_zero_decode", "preserved")));
    assert!(output.contains(&field("sink_kind", "vortex_native")));
    assert!(output.contains(&field("sink_accepts_encoded", "true")));
    assert!(output.contains(&field("sink_requires_materialization", "false")));
    assert!(output.contains(&field("materialization_required", "false")));
    assert!(output.contains(&field("best_data_work_level", "zero_decode")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field(
        "streaming_capability_matrix_schema_version",
        "shardloom.streaming_capability_matrix.v1"
    )));
    assert!(output.contains(&field(
        "streaming_capability_matrix_report_id",
        "gar0013.streaming_runtime_capability_matrix"
    )));
    assert!(output.contains(&field("streaming_capability_matrix_row_count", "8")));
    assert!(output.contains(&field("streaming_capability_matrix_blocked_row_count", "2")));
    assert!(output.contains(&field(
        "streaming_capability_matrix_row_object_store_byte_range_streaming_read_support_status",
        "blocked"
    )));
    assert!(output.contains(&field(
        "streaming_capability_matrix_row_zero_copy_compatibility_boundary_support_status",
        "requires_materialization"
    )));
    assert!(output.contains(&field(
        "streaming_capability_matrix_all_blocked_rows_have_diagnostics",
        "true"
    )));
    assert!(output.contains(&field(
        "streaming_capability_matrix_all_rows_no_fallback_no_external_engine",
        "true"
    )));
    assert!(output.contains("\"code\":\"SL_OBJECT_STORE_UNSUPPORTED\""));
    assert!(output.contains("\"code\":\"SL_MATERIALIZATION_REQUIRED\""));
    assert!(output.contains("\"code\":\"SL_NOT_IMPLEMENTED\""));
    assert!(output.contains("\"severity\":\"info\""));
    assert!(output.contains("\"artifact_kind\":\"materialization_boundary_report\""));
    assert!(output.contains("\"artifact_id\":\"streaming-plan.materialization-boundary\""));
}

#[test]
fn streaming_plan_compatibility_target_reports_materialization_boundary() {
    let output = run_streaming_plan("file:///tmp/input.vortex", "file:///tmp/out.parquet");

    assert!(output.contains(&field("status", "requires_materialization")));
    assert!(output.contains(&field("sink_kind", "parquet_compatibility")));
    assert!(output.contains(&field("sink_accepts_encoded", "false")));
    assert!(output.contains(&field("sink_requires_materialization", "true")));
    assert!(output.contains(&field("sink_preserves_metadata", "false")));
    assert!(output.contains(&field("materialization_required", "true")));
    assert!(output.contains(&field("best_data_work_level", "full_materialization")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("streaming_capability_matrix_row_count", "8")));
    assert!(output.contains("\"artifact_kind\":\"materialization_boundary_report\""));
    assert!(output.contains("\"artifact_id\":\"streaming-plan.materialization-boundary\""));
}
