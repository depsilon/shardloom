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

#[test]
fn streaming_plan_vortex_target_preserves_encoded_native_boundary() {
    let output = run_streaming_plan("file:///tmp/input.vortex", "file:///tmp/out.vortex");

    assert!(output.contains("{\"key\":\"mode\",\"value\":\"plan_only\"}"));
    assert!(output.contains("{\"key\":\"status\",\"value\":\"planned\"}"));
    assert!(output.contains("{\"key\":\"source_kind\",\"value\":\"vortex_segment\"}"));
    assert!(output.contains("{\"key\":\"source_zero_decode\",\"value\":\"preserved\"}"));
    assert!(output.contains("{\"key\":\"sink_kind\",\"value\":\"vortex_native\"}"));
    assert!(output.contains("{\"key\":\"sink_accepts_encoded\",\"value\":\"true\"}"));
    assert!(output.contains("{\"key\":\"sink_requires_materialization\",\"value\":\"false\"}"));
    assert!(output.contains("{\"key\":\"materialization_required\",\"value\":\"false\"}"));
    assert!(output.contains("{\"key\":\"best_data_work_level\",\"value\":\"zero_decode\"}"));
    assert!(output.contains("{\"key\":\"runtime_execution\",\"value\":\"false\"}"));
    assert!(output.contains("{\"key\":\"fallback_execution_allowed\",\"value\":\"false\"}"));
    assert!(output.contains("\"artifact_kind\":\"materialization_boundary_report\""));
    assert!(output.contains("\"artifact_id\":\"streaming-plan.materialization-boundary\""));
}

#[test]
fn streaming_plan_compatibility_target_reports_materialization_boundary() {
    let output = run_streaming_plan("file:///tmp/input.vortex", "file:///tmp/out.parquet");

    assert!(output.contains("{\"key\":\"status\",\"value\":\"requires_materialization\"}"));
    assert!(output.contains("{\"key\":\"sink_kind\",\"value\":\"parquet_compatibility\"}"));
    assert!(output.contains("{\"key\":\"sink_accepts_encoded\",\"value\":\"false\"}"));
    assert!(output.contains("{\"key\":\"sink_requires_materialization\",\"value\":\"true\"}"));
    assert!(output.contains("{\"key\":\"sink_preserves_metadata\",\"value\":\"false\"}"));
    assert!(output.contains("{\"key\":\"materialization_required\",\"value\":\"true\"}"));
    assert!(
        output.contains("{\"key\":\"best_data_work_level\",\"value\":\"full_materialization\"}")
    );
    assert!(output.contains("{\"key\":\"runtime_execution\",\"value\":\"false\"}"));
    assert!(output.contains("{\"key\":\"fallback_execution_allowed\",\"value\":\"false\"}"));
    assert!(output.contains("\"artifact_kind\":\"materialization_boundary_report\""));
    assert!(output.contains("\"artifact_id\":\"streaming-plan.materialization-boundary\""));
}
