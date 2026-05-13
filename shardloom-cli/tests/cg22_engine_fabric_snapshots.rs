use std::process::Command;

fn run_json(args: &[&str], success: bool) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(args)
        .args(["--format", "json"])
        .output()
        .expect("shardloom command runs");
    assert_eq!(
        output.status.success(),
        success,
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("stdout is utf8")
}

fn field(key: &str, value: &str) -> String {
    format!("{{\"key\":\"{key}\",\"value\":\"{value}\"}}")
}

#[test]
fn engine_selection_auto_snapshot_selects_batch_without_runtime_or_fallback() {
    let output = run_json(&["engine-selection-plan"], true);

    assert!(output.contains("\"command\":\"engine-selection-plan\""));
    assert!(output.contains(&field("requested_engine_mode", "auto")));
    assert!(output.contains(&field("selection_status", "selected")));
    assert!(output.contains(&field("selected_engine_mode", "batch")));
    assert!(output.contains(&field("allowed_engine_modes", "batch")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains("\"diagnostics\":[]"));
}

#[test]
fn engine_selection_live_changelog_rejects_until_state_and_checkpoint_evidence() {
    let output = run_json(
        &[
            "engine-selection-plan",
            "live",
            "unbounded",
            "append-only",
            "changelog",
        ],
        false,
    );

    assert!(output.contains("\"status\":\"unsupported\""));
    assert!(output.contains(&field("requested_engine_mode", "live")));
    assert!(output.contains(&field("selection_status", "rejected")));
    assert!(output.contains(&field("selected_engine_mode", "none")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains("live engine is planned but blocked"));
    assert!(output.contains("\"code\":\"SL_NOT_IMPLEMENTED\""));
}

#[test]
fn engine_capability_matrix_separates_batch_live_and_hybrid_claims() {
    let output = run_json(&["engine-capability-matrix"], true);

    assert!(output.contains("\"command\":\"engine-capability-matrix\""));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.engine_capability_matrix.v1"
    )));
    assert!(output.contains(&field("engine_modes", "batch,live,hybrid")));
    assert!(output.contains(&field("batch_support_status", "partially_supported")));
    assert!(output.contains(&field("live_support_status", "planned")));
    assert!(output.contains(&field("hybrid_support_status", "planned")));
    assert!(output.contains(&field("live_hybrid_claim_blocked_count", "2")));
    assert!(output.contains(&field("live_state_required", "true")));
    assert!(output.contains(&field("hybrid_checkpoint_required", "true")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
}
