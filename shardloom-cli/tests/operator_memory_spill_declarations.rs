use std::process::Command;

fn run_operator_memory_spill_declarations_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["operator-memory-spill-declarations", "--format", "json"])
        .output()
        .expect("operator memory/spill declaration command runs");

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
fn operator_memory_spill_declaration_json_exposes_claim_gate() {
    let output = run_operator_memory_spill_declarations_json();

    assert!(output.contains("\"command\":\"operator-memory-spill-declarations\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "operator_memory_spill_declaration_gate")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.operator_memory_spill_declaration.v1"
    )));
    assert!(output.contains(&field("operator_declaration_count", "9")));
    assert!(output.contains(&field("declared_required_operator_count", "0")));
    assert!(output.contains(&field("missing_required_operator_count", "9")));
    assert!(output.contains(&field("omitted_required_operator_count", "0")));
    assert!(output.contains(&field("claim_blocker_count", "9")));
    assert!(output.contains(&field("large_workload_claim_allowed", "false")));
}

#[test]
fn operator_memory_spill_declaration_json_preserves_no_runtime_no_fallback() {
    let output = run_operator_memory_spill_declarations_json();

    assert!(output.contains(&field("runtime_execution", "false")));
    assert!(output.contains(&field("spill_io_performed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("operator_declaration_2_class", "join")));
    assert!(output.contains(&field(
        "operator_declaration_2_spill_support_required",
        "true"
    )));
    assert!(output.contains(&field("operator_declaration_6_class", "udf")));
    assert!(output.contains(&field(
        "operator_declaration_6_effect_boundary_required",
        "true"
    )));
}
