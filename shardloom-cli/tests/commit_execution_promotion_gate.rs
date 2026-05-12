use std::process::Command;

fn run_commit_execution_promotion_gate_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["commit-execution-promotion-gate", "--format", "json"])
        .output()
        .expect("commit execution promotion gate command runs");

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
fn commit_execution_promotion_gate_json_exposes_surfaces() {
    let output = run_commit_execution_promotion_gate_json();

    assert!(output.contains("\"command\":\"commit-execution-promotion-gate\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "commit_execution_promotion_gate")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.commit_execution_promotion_gate.v1"
    )));
    assert!(output.contains(&field("report_id", "cg4.commit_execution_promotion_gate")));
    assert!(output.contains(&field("surface_count", "8")));
    assert!(output.contains(&field("existing_limited_surface_count", "2")));
    assert!(output.contains(&field("blocked_surface_count", "6")));
    assert!(output.contains(&field("broader_execution_ready_surface_count", "0")));
    assert!(output.contains(&field(
        "surface_order",
        "local_committed_manifest_copy,local_committed_manifest_rollback_cleanup,generalized_local_sink_commit,object_store_commit,table_catalog_commit,native_source_sink_commit,foundry_dataset_transaction_commit,live_hybrid_checkpoint_commit"
    )));
    assert!(output.contains(&field(
        "commit_promotion_surface_0_name",
        "local_committed_manifest_copy"
    )));
    assert!(output.contains(&field(
        "commit_promotion_surface_3_name",
        "object_store_commit"
    )));
    assert!(output.contains(&field(
        "commit_promotion_surface_6_name",
        "foundry_dataset_transaction_commit"
    )));
}

#[test]
fn commit_execution_promotion_gate_json_blocks_claims_and_effects() {
    let output = run_commit_execution_promotion_gate_json();

    assert!(output.contains(&field("existing_local_commit_execution_present", "true")));
    assert!(output.contains(&field("existing_local_rollback_execution_present", "true")));
    assert!(output.contains(&field("broader_execution_promotions_blocked", "true")));
    assert!(output.contains(&field("commit_claims_blocked", "true")));
    assert!(output.contains(&field("output_manifest_required", "true")));
    assert!(output.contains(&field("sink_requirement_report_required", "true")));
    assert!(output.contains(&field("materialization_fidelity_report_required", "true")));
    assert!(output.contains(&field("execution_certificate_required", "true")));
    assert!(output.contains(&field("native_io_certificate_required", "true")));
    assert!(output.contains(&field("idempotency_key_required", "true")));
    assert!(output.contains(&field("rollback_recovery_proof_required", "true")));
    assert!(output.contains(&field("ambiguous_commit_diagnostics_required", "true")));
    assert!(output.contains(&field("object_store_atomicity_policy_required", "true")));
    assert!(output.contains(&field("table_catalog_transaction_policy_required", "true")));
    assert!(output.contains(&field("credential_effect_policy_required", "true")));
    assert!(output.contains(&field("broader_commit_execution_allowed", "false")));
    assert!(output.contains(&field("object_store_commit_execution_allowed", "false")));
    assert!(output.contains(&field("foundry_dataset_commit_execution_allowed", "false")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("catalog_io", "false")));
    assert!(output.contains(&field("external_effects_executed", "false")));
    assert!(output.contains(&field("exactly_once_claim_allowed", "false")));
    assert!(output.contains(&field("atomic_commit_claim_allowed", "false")));
    assert!(output.contains(&field("recovery_claim_allowed", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}
