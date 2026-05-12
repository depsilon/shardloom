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
    assert!(output.contains(&field("fixture_count", "34")));
    assert!(output.contains(&field(
        "fixture_id_order",
        "vortex-metadata-footer-u64-20000,vortex-local-encoded-count-u64-20000,vortex-local-count-all-struct-five,vortex-local-count-where-struct-five,vortex-local-project-struct-five,vortex-local-filter-struct-five,vortex-local-filter-project-struct-five,vortex-prepared-encoded-filter-dictionary-run,vortex-prepared-encoded-projection-dictionary,vortex-prepared-encoded-filter-project-selection-vector,vortex-edge-count-all-empty-input,vortex-edge-project-single-row,vortex-edge-filter-all-null,vortex-edge-filter-mixed-null-sparse,vortex-edge-filter-duplicate-low-cardinality,vortex-edge-project-high-cardinality,vortex-edge-filter-project-sorted-dictionary,vortex-edge-filter-project-unsorted-rle,vortex-edge-filter-temporal-values,property-encoded-filter-selection-vector-consistency,property-encoded-projection-preserves-row-order,property-encoded-filter-project-composition,null-semantics,metadata-only-correctness,pruning-correctness,encoded-vs-decoded-reference,translation-metadata-loss,unsupported-diagnostics,plan-only-no-side-effects,nested-data-edge-corpus,dictionary-encoded-edge-corpus,sparse-validity-edge-corpus,run-length-edge-corpus,temporal-semantics"
    )));
    assert!(output.contains(&field(
        "semantic_area_order",
        "metadata_only,encoded_execution,selection_vectors,nulls,temporal,pruning,translation,unsupported_diagnostics,external_effects,nested_data"
    )));
    assert!(output.contains(&field(
        "edge_case_order",
        "no_nulls,dictionary_encoded,run_length_encoded,sparse_validity,empty_input,single_row,all_null,mixed_nulls,duplicate_values,low_cardinality,high_cardinality,sorted_input,unsorted_input,temporal_values,missing_statistics,approximate_statistics,unsupported_encoding,metadata_loss,unsupported_plan_shape,nested_struct_list"
    )));
}

#[test]
fn correctness_plan_json_exposes_reference_and_gap_counts() {
    let output = run_correctness_plan_json();

    assert!(output.contains(&field(
        "reference_role_order",
        "golden_fixture,decoded_reference,generated_property,external_oracle"
    )));
    assert!(output.contains(&field("fixtures_with_source_ref_count", "16")));
    assert!(output.contains(&field("source_backed_edge_fixture_count", "9")));
    assert!(output.contains(&field(
        "source_backed_edge_fixture_id_order",
        "vortex-edge-count-all-empty-input,vortex-edge-project-single-row,vortex-edge-filter-all-null,vortex-edge-filter-mixed-null-sparse,vortex-edge-filter-duplicate-low-cardinality,vortex-edge-project-high-cardinality,vortex-edge-filter-project-sorted-dictionary,vortex-edge-filter-project-unsorted-rle,vortex-edge-filter-temporal-values"
    )));
    assert!(output.contains(&field("golden_fixture_count", "19")));
    assert!(output.contains(&field("reference_artifact_count", "18")));
    assert!(output.contains(&field("decoded_reference_output_count", "18")));
    assert!(output.contains(&field(
        "decoded_reference_artifact_id_order",
        "vortex-local-encoded-count-u64-20000.decoded-reference.count,vortex-local-count-all-struct-five.decoded-reference.count,vortex-local-count-where-struct-five.decoded-reference.rows,vortex-local-project-struct-five.decoded-reference.rows,vortex-local-filter-struct-five.decoded-reference.rows,vortex-local-filter-project-struct-five.decoded-reference.rows,vortex-prepared-encoded-filter-dictionary-run.decoded-reference.rows,vortex-prepared-encoded-projection-dictionary.decoded-reference.rows,vortex-prepared-encoded-filter-project-selection-vector.decoded-reference.rows,vortex-edge-count-all-empty-input.decoded-reference.count,vortex-edge-project-single-row.decoded-reference.rows,vortex-edge-filter-all-null.decoded-reference.rows,vortex-edge-filter-mixed-null-sparse.decoded-reference.rows,vortex-edge-filter-duplicate-low-cardinality.decoded-reference.rows,vortex-edge-project-high-cardinality.decoded-reference.rows,vortex-edge-filter-project-sorted-dictionary.decoded-reference.rows,vortex-edge-filter-project-unsorted-rle.decoded-reference.rows,vortex-edge-filter-temporal-values.decoded-reference.rows"
    )));
    assert!(output.contains(&field("decoded_reference_output_coverage_complete", "true")));
    assert!(output.contains(&field("executable_expected_output_count", "18")));
    assert!(output.contains(&field("not_yet_defined_fixture_count", "8")));
    assert!(output.contains(&field("diagnostic_expected_output_count", "1")));
    assert!(output.contains(&field("unsupported_expected_output_count", "1")));
    assert!(output.contains(&field("baseline_count", "7")));
    assert!(output.contains(&field("external_oracle_result_artifact_count", "63")));
    assert!(output.contains(&field("external_oracle_result_populated_count", "0")));
    assert!(output.contains(&field("external_oracle_results_populated", "false")));
    assert!(output.contains(&field(
        "external_oracle_result_artifact_status_order",
        "declared_not_executed"
    )));
    assert!(output.contains(&field("external_oracle_artifacts_test_only", "true")));
    assert!(
        output.contains("vortex-edge-count-all-empty-input.external-oracle.spark.declared-result")
    );
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
