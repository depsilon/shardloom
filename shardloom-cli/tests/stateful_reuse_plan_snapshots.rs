use std::process::Command;

fn run_stateful_reuse_plan_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["stateful-reuse-plan", "--format", "json"])
        .output()
        .expect("stateful reuse plan command runs");

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
fn stateful_reuse_json_exposes_cg17_boundary_contract() {
    let output = run_stateful_reuse_plan_json();

    assert!(output.contains("\"command\":\"stateful-reuse-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "stateful_reuse_plan")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("plan_only", "true")));
    assert!(output.contains(&field("schema_version", "shardloom.stateful_reuse.v1")));
    assert!(output.contains(&field("stateful_reuse_status", "report_only_planned")));
    assert!(output.contains(&field("boundary_count", "7")));
    assert!(output.contains(&field("invalidation_requirement_count", "11")));
    assert!(output.contains(&field("correctness_proof_required_count", "7")));
    assert!(output.contains(&field("invalidation_proof_required_count", "7")));
    assert!(output.contains(&field("execution_certificate_required_count", "7")));
    assert!(output.contains(&field(
        "cache_kind_order",
        "segment_result,predicate_result,encoded_dictionary,encoded_filter,layout_decision,execution_certificate,incremental_manifest_diff"
    )));
    assert!(output.contains(&field(
        "invalidation_signal_order",
        "snapshot_changed,segment_added,segment_removed,segment_replaced,schema_changed,partition_changed,predicate_changed,semantic_profile_changed,function_version_changed,adapter_fidelity_changed,unknown_change"
    )));
}

#[test]
fn stateful_reuse_json_preserves_no_cache_or_execution_effects() {
    let output = run_stateful_reuse_plan_json();

    assert!(output.contains(&field("typed_cache_boundaries_required", "true")));
    assert!(output.contains(&field("deterministic_keys_required", "true")));
    assert!(output.contains(&field("invalidation_proofs_required", "true")));
    assert!(output.contains(&field("correctness_proofs_required", "true")));
    assert!(output.contains(&field("execution_certificates_required", "true")));
    assert!(output.contains(&field("manifest_diff_required", "true")));
    assert!(output.contains(&field("cache_read", "false")));
    assert!(output.contains(&field("cache_write", "false")));
    assert!(output.contains(&field("cache_replay", "false")));
    assert!(output.contains(&field("incremental_execution", "false")));
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
    assert!(output.contains(&field(
        "gar_0029_evidence_expansion_schema_version",
        "shardloom.cg5_cg6_stateful_reuse_evidence_expansion.v1"
    )));
    assert!(output.contains(&field(
        "gar_0029_evidence_expansion_stateful_reuse_runtime_supported",
        "false"
    )));
    assert!(output.contains(&field(
        "gar_0029_evidence_expansion_cache_write_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "gar_0029_evidence_expansion_incremental_execution_allowed",
        "false"
    )));
    assert!(output.contains(&field(
        "gar_0029_evidence_expansion_row_cg17_stateful_reuse_boundary_evidence_support_status",
        "blocked"
    )));
}
