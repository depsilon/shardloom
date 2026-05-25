use std::process::Command;

fn run_session_cache_smoke_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["session-cache-smoke", "--format", "json"])
        .output()
        .expect("session cache smoke command runs");

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
fn session_cache_smoke_exposes_scoped_4l_5i_runtime_contract() {
    let output = run_session_cache_smoke_json();

    assert!(output.contains("\"command\":\"session-cache-smoke\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "session_cache_smoke")));
    assert!(output.contains(&field("gar_ids", "GAR-RUNTIME-IMPL-4L,GAR-RUNTIME-IMPL-5I")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.session_runtime_cache.v1"
    )));
    assert!(output.contains(&field(
        "session_runtime_status",
        "scoped_session_cache_runtime_certified"
    )));
    assert!(output.contains(&field("session_state_scope", "cli_in_process_local")));
    assert!(output.contains(&field(
        "cache_artifact_order",
        "source_state,vortex_prepared_state,output_plan,schema_cache,dictionary_cache"
    )));
    assert!(output.contains(&field("cache_hit_count", "5")));
    assert!(output.contains(&field("cache_miss_count", "8")));
    assert!(output.contains(&field("invalidation_count", "3")));
    assert!(output.contains(&field("source_state_reuse_count", "1")));
    assert!(output.contains(&field("prepared_state_reuse_count", "1")));
    assert!(output.contains(&field("output_plan_reuse_count", "1")));
    assert!(output.contains(&field("schema_cache_reuse_count", "1")));
    assert!(output.contains(&field("dictionary_cache_reuse_count", "1")));
    assert!(output.contains(&field(
        "invalidation_reason_order",
        "source_fingerprint_changed,schema_digest_changed,output_artifact_fingerprint_changed"
    )));
}

#[test]
fn session_cache_smoke_preserves_cleanup_optimizer_and_no_fallback_evidence() {
    let output = run_session_cache_smoke_json();

    assert!(output.contains(&field("source_state_reuse_hit", "true")));
    assert!(output.contains(&field("prepared_state_reuse_hit", "true")));
    assert!(output.contains(&field("output_plan_reuse_hit", "true")));
    assert!(output.contains(&field(
        "buffer_pool_status",
        "scoped_in_process_reuse_certified"
    )));
    assert!(output.contains(&field("buffer_pool_scope", "session_scratch_buffers_only")));
    assert!(output.contains(&field("buffer_allocation_count", "1")));
    assert!(output.contains(&field("buffer_reuse_count", "1")));
    assert!(output.contains(&field("explicit_close_required", "true")));
    assert!(output.contains(&field("explicit_close_performed", "true")));
    assert!(output.contains(&field("cleanup_performed", "true")));
    assert!(output.contains(&field("cleanup_cache_entries_removed", "5")));
    assert!(output.contains(&field("session_closed", "true")));
    assert!(output.contains(&field("lifecycle_closed_and_cleaned", "true")));
    assert!(output.contains(&field(
        "optimizer_trace_integration_status",
        "linked_report_only_trace"
    )));
    assert!(output.contains(&field(
        "optimizer_rule_common_subplan_source_state_reuse_status",
        "admitted"
    )));
    assert!(output.contains(&field(
        "optimizer_rule_common_subplan_source_state_reuse_source_state_reuse_admitted",
        "true"
    )));
    assert!(output.contains(&field("runtime_execution", "true")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("data_decoded", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("external_engine_invoked", "false")));
    assert!(output.contains(&field("no_fallback_no_external_engine", "true")));
    assert!(output.contains(&field("claim_gate_status", "not_claim_grade")));
    assert!(output.contains("fnv1a64:"));
}
