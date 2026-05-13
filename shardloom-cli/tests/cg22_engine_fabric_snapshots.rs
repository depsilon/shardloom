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
fn engine_selection_live_changelog_selects_live_fixture_without_fallback() {
    let output = run_json(
        &[
            "engine-selection-plan",
            "live",
            "unbounded",
            "append-only",
            "changelog",
        ],
        true,
    );

    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("requested_engine_mode", "live")));
    assert!(output.contains(&field("selection_status", "selected")));
    assert!(output.contains(&field("selected_engine_mode", "live")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains("\"diagnostics\":[]"));
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
    assert!(output.contains(&field("live_support_status", "partially_supported")));
    assert!(output.contains(&field("hybrid_support_status", "planned")));
    assert!(output.contains(&field("live_hybrid_claim_blocked_count", "2")));
    assert!(output.contains(&field("live_state_required", "true")));
    assert!(output.contains(&field("hybrid_checkpoint_required", "true")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
}

#[test]
fn live_change_contract_plan_declares_change_and_policy_vocabulary() {
    let output = run_json(&["live-change-contract-plan"], true);

    assert!(output.contains("\"command\":\"live-change-contract-plan\""));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.live_change_contract.v1"
    )));
    assert!(output.contains(&field("mode", "live_change_contract_plan")));
    assert!(output.contains(&field(
        "change_record_field_order",
        "key,operation,sequence,event_time_ms,processing_time_ms,source_offset,schema_digest,payload_ref"
    )));
    assert!(output.contains(&field(
        "change_operation_vocabulary",
        "append,upsert,delete,retract,tombstone"
    )));
    assert!(output.contains(&field("watermark_policy", "fixture_event_time")));
    assert!(output.contains(&field(
        "checkpoint_policy",
        "in_memory_deterministic_fixture"
    )));
    assert!(output.contains(&field(
        "fixture_operator_vocabulary",
        "filter,project,count,count_where,group_count"
    )));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}

#[test]
fn live_fixture_run_group_count_emits_state_freshness_and_certificate_evidence() {
    let output = run_json(&["live-fixture-run", "group-count", "metric"], true);

    assert!(output.contains("\"command\":\"live-fixture-run\""));
    assert!(output.contains(&field("mode", "live_fixture_run")));
    assert!(output.contains(&field("fixture_operator", "group_count")));
    assert!(output.contains(&field("input_change_record_count", "10")));
    assert!(output.contains(&field("active_state_key_count", "3")));
    assert!(output.contains(&field("output_row_count", "2")));
    assert!(output.contains(&field(
        "output_rows",
        "east:group_count:2|west:group_count:1"
    )));
    assert!(output.contains(&field("freshness_certificate_emitted", "true")));
    assert!(output.contains(&field("freshness_certificate_status", "certified")));
    assert!(output.contains(&field("state_certificate_emitted", "true")));
    assert!(output.contains(&field(
        "checkpoint_ref",
        "checkpoint://cg22/live/fixture/seq-10"
    )));
    assert!(output.contains(&field("continuous_view_certificate_emitted", "true")));
    assert!(output.contains(&field("execution_certificate_emitted", "true")));
    assert!(output.contains(&field("execution_certificate_status", "certified")));
    assert!(output.contains(&field("native_io_certificate_emitted", "true")));
    assert!(output.contains(&field("native_io_certificate_status", "certified")));
    assert!(output.contains(&field("runtime_execution", "true")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("broker_io", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}
