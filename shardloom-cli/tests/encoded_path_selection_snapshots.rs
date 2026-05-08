use std::process::Command;

fn run_encoded_path_selection_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["vortex-encoded-path-selection-plan", "--format", "json"])
        .output()
        .expect("encoded path selection command runs");

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
fn encoded_path_selection_json_exposes_cg13_foundation_contract() {
    let output = run_encoded_path_selection_json();

    assert!(output.contains("\"command\":\"vortex-encoded-path-selection-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "vortex_encoded_path_selection_plan")));
    assert!(output.contains(&field("execution", "not_performed")));
    assert!(output.contains(&field("plan_only", "true")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.vortex_encoded_path_selection.v1"
    )));
    assert!(output.contains(&field("selection_status", "report_only_planned")));
    assert!(output.contains(&field("entry_count", "3")));
    assert!(output.contains(&field("operator_order", "count_aggregate,filter,project")));
    assert!(output.contains(&field(
        "selected_execution_levels",
        "encoded_native,encoded_native,encoded_native"
    )));
    assert!(output.contains(&field("direct_count_candidate_present", "true")));
    assert!(output.contains(&field("direct_filter_candidate_present", "true")));
    assert!(output.contains(&field("direct_project_candidate_present", "true")));
}

#[test]
fn encoded_path_selection_json_preserves_no_work_and_no_fallback_boundaries() {
    let output = run_encoded_path_selection_json();

    assert!(output.contains(&field("decode_avoided_candidate_count", "3")));
    assert!(output.contains(&field("materialization_avoided_candidate_count", "3")));
    assert!(output.contains(&field("selection_vector_preserved_count", "1")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("data_decoded", "false")));
    assert!(output.contains(&field("data_materialized", "false")));
    assert!(output.contains(&field("row_read", "false")));
    assert!(output.contains(&field("arrow_converted", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("spill_io_performed", "false")));
    assert!(output.contains(&field("runtime_execution_allowed", "false")));
    assert!(output.contains(&field("external_engine_execution", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("production_claim_allowed", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
}
