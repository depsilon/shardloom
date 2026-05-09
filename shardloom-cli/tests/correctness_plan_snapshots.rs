use std::process::Command;

fn run_correctness_plan_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["correctness-plan", "--format", "json"])
        .output()
        .expect("correctness plan command runs");

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
fn correctness_plan_json_exposes_fixture_and_edge_case_inventory() {
    let output = run_correctness_plan_json();

    assert!(output.contains("\"command\":\"correctness-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "correctness_plan")));
    assert!(output.contains(&field("status", "planned")));
    assert!(output.contains(&field("fixture_count", "19")));
    assert!(output.contains(&field(
        "fixture_id_order",
        "vortex-metadata-footer-u64-20000,vortex-local-encoded-count-u64-20000,vortex-local-count-all-struct-five,vortex-local-count-where-struct-five,vortex-local-project-struct-five,vortex-local-filter-struct-five,vortex-local-filter-project-struct-five,null-semantics,metadata-only-correctness,pruning-correctness,encoded-vs-decoded-reference,translation-metadata-loss,unsupported-diagnostics,plan-only-no-side-effects,nested-data-edge-corpus,dictionary-encoded-edge-corpus,sparse-validity-edge-corpus,run-length-edge-corpus,temporal-semantics"
    )));
    assert!(output.contains(&field(
        "semantic_area_order",
        "metadata_only,encoded_execution,nulls,pruning,translation,unsupported_diagnostics,external_effects,nested_data,selection_vectors,temporal"
    )));
    assert!(output.contains(&field(
        "edge_case_order",
        "no_nulls,all_null,missing_statistics,approximate_statistics,unsupported_encoding,metadata_loss,unsupported_plan_shape,empty_input,nested_struct_list,dictionary_encoded,sparse_validity,run_length_encoded,temporal_values"
    )));
}

#[test]
fn correctness_plan_json_exposes_reference_and_gap_counts() {
    let output = run_correctness_plan_json();

    assert!(output.contains(&field(
        "reference_role_order",
        "golden_fixture,external_oracle"
    )));
    assert!(output.contains(&field("fixtures_with_source_ref_count", "7")));
    assert!(output.contains(&field("golden_fixture_count", "7")));
    assert!(output.contains(&field("executable_expected_output_count", "6")));
    assert!(output.contains(&field("not_yet_defined_fixture_count", "8")));
    assert!(output.contains(&field("diagnostic_expected_output_count", "1")));
    assert!(output.contains(&field("unsupported_expected_output_count", "1")));
    assert!(output.contains(&field("baseline_count", "7")));
    assert!(output.contains(&field("covered_required_foundation_edge_case_count", "7")));
    assert!(output.contains(&field("required_foundation_edge_case_count", "7")));
    assert!(output.contains(&field("missing_required_foundation_edge_cases", "")));
    assert!(output.contains(&field("required_foundation_edge_cases_covered", "true")));
}

#[test]
fn correctness_plan_json_preserves_test_only_no_fallback_boundaries() {
    let output = run_correctness_plan_json();

    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("external_baselines", "test_oracles_only")));
    assert!(output.contains(&field("reference_roles_test_only", "true")));
    assert!(output.contains(&field("baselines_fallback_free", "true")));
}
