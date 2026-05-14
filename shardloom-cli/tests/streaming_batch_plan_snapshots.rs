use std::process::Command;

fn run_streaming_batch_plan(args: &[&str]) -> (bool, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .output()
        .expect("streaming-batch-plan command runs");
    (
        output.status.success(),
        String::from_utf8(output.stdout).expect("stdout is utf8"),
    )
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

#[test]
fn streaming_batch_plan_preserves_vortex_encoded_batches_without_execution() {
    let (success, output) = run_streaming_batch_plan(&[
        "streaming-batch-plan",
        "file:///tmp/input.vortex",
        "file:///tmp/out.vortex",
        "8",
        "2",
        "16",
        "--format",
        "json",
    ]);

    assert!(success, "{output}");
    assert!(output.contains(&field("mode", "streaming_batch_plan")));
    assert!(output.contains(&field("encoded_streaming_batch_status", "planned")));
    assert!(output.contains(&field("streaming_mode", "plan_only")));
    assert!(output.contains(&field("source_kind", "vortex_segment")));
    assert!(output.contains(&field("sink_kind", "vortex_native")));
    assert!(output.contains(&field("representation", "vortex_encoded")));
    assert!(output.contains(&field("zero_decode", "preserved")));
    assert!(output.contains(&field("encoded_representation_preserved", "true")));
    assert!(output.contains(&field("bounded_parallelism", "true")));
    assert!(output.contains(&field("max_parallelism", "2")));
    assert!(output.contains(&field("bounded_memory", "true")));
    assert!(output.contains(&field("backpressure_bounded", "true")));
    assert!(output.contains(&field("estimated_batch_mib", "16")));
    assert!(output.contains(&field("estimated_batch_bytes", "16777216")));
    assert!(output.contains(&field("streams_executed", "false")));
    assert!(output.contains(&field("tasks_executed", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("data_decoded", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("row_read", "false")));
    assert!(output.contains(&field("arrow_converted", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("streaming_capability_matrix_row_count", "8")));
    assert!(output.contains(&field(
        "streaming_capability_matrix_row_local_vortex_streaming_batch_count_fixture_support_status",
        "fixture_smoke"
    )));
    assert!(output.contains(&field(
        "streaming_capability_matrix_all_rows_no_fallback_no_external_engine",
        "true"
    )));
    assert!(output.contains("\"attempted\":false"));
    assert!(output.contains("\"artifact_kind\":\"materialization_boundary_report\""));
    assert!(output.contains("\"artifact_id\":\"streaming-batch-plan.materialization-boundary\""));
}

#[test]
fn streaming_batch_plan_compatibility_sink_reports_materialization_boundary() {
    let (success, output) = run_streaming_batch_plan(&[
        "streaming-batch-plan",
        "file:///tmp/input.vortex",
        "file:///tmp/out.parquet",
        "8",
        "2",
        "--format",
        "json",
    ]);

    assert!(success, "{output}");
    assert!(output.contains(&field(
        "encoded_streaming_batch_status",
        "requires_materialization"
    )));
    assert!(output.contains(&field("sink_kind", "parquet_compatibility")));
    assert!(output.contains(&field("representation", "materialized_rows")));
    assert!(output.contains(&field("zero_decode", "full_decode_required")));
    assert!(output.contains(&field("encoded_representation_preserved", "false")));
    assert!(output.contains(&field("materialization_required", "true")));
    assert!(output.contains(&field(
        "materialization_boundary",
        "full_materialization_boundary"
    )));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field(
        "streaming_capability_matrix_row_zero_copy_compatibility_boundary_support_status",
        "requires_materialization"
    )));
    assert!(output.contains("\"artifact_kind\":\"materialization_boundary_report\""));
    assert!(output.contains("\"artifact_id\":\"streaming-batch-plan.materialization-boundary\""));
}

#[test]
fn streaming_batch_plan_object_store_source_blocks_without_io() {
    let (success, output) = run_streaming_batch_plan(&[
        "streaming-batch-plan",
        "s3://bucket/input.vortex",
        "file:///tmp/out.vortex",
        "8",
        "2",
        "--format",
        "json",
    ]);

    assert!(!success, "{output}");
    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains(&field(
        "encoded_streaming_batch_status",
        "blocked_by_object_store_io"
    )));
    assert!(output.contains(&field("source_kind", "object_store_byte_range")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field(
        "streaming_capability_matrix_row_object_store_byte_range_streaming_read_support_status",
        "blocked"
    )));
    assert!(output.contains(&field(
        "streaming_capability_matrix_diagnostic_code_order",
        "SL_OBJECT_STORE_UNSUPPORTED,SL_MATERIALIZATION_REQUIRED,SL_NOT_IMPLEMENTED"
    )));
    assert!(output.contains("\"attempted\":false"));
}

#[test]
fn streaming_batch_plan_rejects_zero_parallelism_without_fallback() {
    let (success, output) = run_streaming_batch_plan(&[
        "streaming-batch-plan",
        "file:///tmp/input.vortex",
        "file:///tmp/out.vortex",
        "8",
        "0",
        "--format",
        "json",
    ]);

    assert!(!success, "{output}");
    assert!(output.contains("\"status\":\"error\""));
    assert!(output.contains("max_parallelism must be a positive integer"));
    assert!(output.contains("\"attempted\":false"));
    assert!(output.contains("\"allowed\":false"));
}
