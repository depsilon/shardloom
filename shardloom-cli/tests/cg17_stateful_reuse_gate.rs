use std::process::Command;

fn run_cg17_stateful_reuse_gate_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["cg17-stateful-reuse-gate", "--format", "json"])
        .output()
        .expect("cg17-stateful-reuse-gate command runs");

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
fn cg17_stateful_reuse_gate_exposes_promotion_surfaces() {
    let output = run_cg17_stateful_reuse_gate_json();

    assert!(output.contains("\"command\":\"cg17-stateful-reuse-gate\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "cg17_stateful_reuse_promotion_gate")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("plan_only", "true")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.stateful_reuse_promotion_gate.v1"
    )));
    assert!(output.contains(&field("report_id", "cg17.stateful_reuse_promotion_gate")));
    assert!(output.contains(&field("surface_count", "13")));
    assert!(output.contains(&field("existing_evidence_surface_count", "2")));
    assert!(output.contains(&field("blocked_surface_count", "11")));
    assert!(output.contains(&field(
        "surface_order",
        "boundary_report_foundation,cdc_incremental_planning_foundation,stable_reuse_key_derivation,reuse_key_digest_and_scope,manifest_diff_input_evidence,invalidation_decision_matrix,cache_safety_policy,state_certificate_schema,execution_certificate_linkage,native_io_certificate_linkage,reuse_benchmark_constitution,incremental_recompute_execution,production_reuse_claim_closeout"
    )));
    assert!(output.contains(&field(
        "existing_report_refs",
        "stateful-reuse-plan,incremental-plan cdc,execution-certificate-plan,native-io-envelope-plan,benchmark-claim-evidence-plan,operational_contracts.evidence_artifact_envelope"
    )));
    assert!(output.contains(&field(
        "existing_stateful_reuse_boundary_report_present",
        "true"
    )));
    assert!(output.contains(&field(
        "existing_cdc_incremental_planning_report_present",
        "true"
    )));
}

#[test]
fn cg17_stateful_reuse_gate_blocks_runtime_cache_and_claims() {
    let output = run_cg17_stateful_reuse_gate_json();

    for key in [
        "cache_read_allowed",
        "cache_write_allowed",
        "cache_replay_allowed",
        "incremental_execution_allowed",
        "runtime_execution_allowed",
        "manifest_diff_read_allowed",
        "state_certificate_claim_allowed",
        "reuse_performance_claim_allowed",
        "incremental_performance_claim_allowed",
        "production_claim_allowed",
        "external_engine_invoked",
        "fallback_execution_allowed",
        "fallback_attempted",
    ] {
        assert!(
            output.contains(&field(key, "false")),
            "missing false field {key}"
        );
    }

    for key in [
        "stable_reuse_keys_required",
        "key_digest_and_scope_required",
        "manifest_diff_inputs_required",
        "invalidation_evidence_required",
        "cache_safety_policy_required",
        "state_certificates_required",
        "correctness_evidence_required",
        "execution_certificate_required",
        "native_io_certificate_required",
        "reuse_benchmark_required",
        "runtime_promotions_blocked",
        "claim_blocked",
        "side_effect_free",
    ] {
        assert!(
            output.contains(&field(key, "true")),
            "missing true field {key}"
        );
    }
}
