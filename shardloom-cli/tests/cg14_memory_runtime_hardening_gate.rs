use std::process::Command;

fn run_cg14_memory_runtime_hardening_gate_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["cg14-memory-runtime-hardening-gate", "--format", "json"])
        .output()
        .expect("CG-14 memory runtime hardening gate command runs");

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
fn cg14_memory_runtime_hardening_gate_exposes_existing_and_blocked_surfaces() {
    let output = run_cg14_memory_runtime_hardening_gate_json();

    assert!(output.contains("\"command\":\"cg14-memory-runtime-hardening-gate\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "cg14_memory_runtime_hardening_gate")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.memory_runtime_hardening_gate.v1"
    )));
    assert!(output.contains(&field("report_id", "cg14.memory_runtime_hardening_gate")));
    assert!(output.contains(&field("surface_count", "14")));
    assert!(output.contains(&field("existing_evidence_surface_count", "5")));
    assert!(output.contains(&field("blocked_surface_count", "9")));
    assert!(output.contains(&field(
        "surface_order",
        "memory_reservation_admission,operator_memory_spill_declaration_gate,spill_reservation_integration_plan,spill_lifecycle_plan,dynamic_runtime_promotion_reference,resource_derived_chunk_sizing_runtime,adaptive_parallelism_runtime,memory_reservation_release_runtime,pressure_reaction_runtime,native_spill_write_runtime,native_spill_read_runtime,spill_cleanup_execution,allocator_runtime_integration,benchmark_certificate_closeout"
    )));
}

#[test]
fn cg14_memory_runtime_hardening_gate_blocks_runtime_spill_and_claims() {
    let output = run_cg14_memory_runtime_hardening_gate_json();

    assert!(output.contains(&field(
        "existing_memory_reservation_admission_present",
        "true"
    )));
    assert!(output.contains(&field(
        "existing_operator_memory_spill_declaration_gate_present",
        "true"
    )));
    assert!(output.contains(&field("runtime_metrics_required", "true")));
    assert!(output.contains(&field("memory_budget_required", "true")));
    assert!(output.contains(&field("spill_policy_required", "true")));
    assert!(output.contains(&field("execution_certificate_required", "true")));
    assert!(output.contains(&field("native_io_certificate_required", "true")));
    assert!(output.contains(&field("benchmark_evidence_required", "true")));
    assert!(output.contains(&field("no_fallback_evidence_required", "true")));
    assert!(output.contains(&field("resource_derived_chunk_sizing_allowed", "false")));
    assert!(output.contains(&field("adaptive_parallelism_allowed", "false")));
    assert!(output.contains(&field("native_spill_write_allowed", "false")));
    assert!(output.contains(&field("native_spill_read_allowed", "false")));
    assert!(output.contains(&field("spill_cleanup_execution_allowed", "false")));
    assert!(output.contains(&field("large_workload_claim_allowed", "false")));
    assert!(output.contains(&field("runtime_promotions_blocked", "true")));
    assert!(output.contains(&field("claim_blocked", "true")));
    assert!(output.contains(&field("side_effect_free", "true")));
    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("spill_io_performed", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
}
