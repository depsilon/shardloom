use std::process::Command;

fn run_correctness_harness_json() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_shardloom"))
        .args(["correctness-harness-plan", "--format", "json"])
        .output()
        .expect("correctness harness command runs");

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
fn correctness_harness_json_exposes_aggregate_status_and_surfaces() {
    let output = run_correctness_harness_json();

    assert!(output.contains("\"command\":\"correctness-harness-plan\""));
    assert!(output.contains("\"status\":\"success\""));
    assert!(output.contains(&field("mode", "correctness_differential_harness")));
    assert!(output.contains(&field(
        "schema_version",
        "shardloom.correctness_differential_harness.v1"
    )));
    assert!(output.contains(&field(
        "report_id",
        "cg5.correctness_differential_harness.aggregate"
    )));
    assert!(output.contains(&field("harness_status", "needs_evidence")));
    assert!(output.contains(&field(
        "surface_order",
        "fixture_manifest,golden_fixtures,source_backed_edge_fixtures,decoded_reference_outputs,differential_oracles,external_oracle_result_artifacts,semantic_edge_cases,unsupported_diagnostics,property_fuzzing,benchmark_claim_gate"
    )));
    assert!(output.contains(&field("planned_surface_count", "9")));
    assert!(output.contains(&field("blocked_surface_count", "1")));
    assert!(output.contains(&field("blocked_surface_order", "benchmark_claim_gate")));
}

#[test]
fn correctness_harness_json_exposes_fixtures_oracles_and_missing_modes() {
    let output = run_correctness_harness_json();

    assert!(output.contains(&field(
        "required_validation_mode_order",
        "expected_output,decoded_reference,differential_comparison,property_based,fuzz,golden_diagnostic,unsupported_diagnostic_only"
    )));
    assert!(output.contains(&field("missing_validation_mode_order", "")));
    assert!(output.contains(&field("fixture_count", "34")));
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
    assert!(output.contains(&field("not_yet_defined_fixture_count", "0")));
    assert!(output.contains(&field("deferred_fixture_family_count", "8")));
    assert!(output.contains(&field(
        "deferred_fixture_family_id_order",
        "null-semantics,pruning-correctness,encoded-vs-decoded-reference,nested-data-edge-corpus,dictionary-encoded-edge-corpus,sparse-validity-edge-corpus,run-length-edge-corpus,temporal-semantics"
    )));
    assert!(output.contains(&field("unsupported_diagnostic_fixture_count", "2")));
    assert!(output.contains(&field("baseline_count", "7")));
    assert!(output.contains(&field(
        "baseline_engine_order",
        "spark,datafusion,duckdb,polars,pandas,dask,velox"
    )));
    assert!(output.contains(&field("external_oracle_result_artifact_count", "63")));
    assert!(output.contains(&field("external_oracle_result_populated_count", "0")));
    assert!(output.contains(&field("external_oracle_results_populated", "false")));
    assert!(output.contains(&field(
        "external_oracle_result_artifact_status_order",
        "declared_not_executed"
    )));
    assert!(output.contains(&field("external_oracle_artifacts_test_only", "true")));
    assert!(output.contains(&field(
        "benchmark_claim_blocker_order",
        "deferred_fixture_families,external_oracle_results_not_populated,property_fuzz_execution_not_performed"
    )));
    assert!(output.contains(&field("property_fuzz_execution_performed", "false")));
    assert!(
        output.contains("vortex-edge-count-all-empty-input.external-oracle.spark.declared-result")
    );
    assert!(output.contains(&field(
        "reference_role_order",
        "golden_fixture,decoded_reference,generated_property,external_oracle"
    )));
    assert!(output.contains(&field("generated_property_fixture_count", "3")));
    assert!(output.contains(&field("fuzz_seed_count", "3")));
}

#[test]
fn correctness_harness_json_preserves_no_execution_no_fallback_boundaries() {
    let output = run_correctness_harness_json();

    assert!(output.contains(&field("decoded_reference_outputs_required", "true")));
    assert!(output.contains(&field("differential_oracles_required", "true")));
    assert!(output.contains(&field("property_fuzzing_required", "true")));
    assert!(output.contains(&field("benchmark_claim_gate_required", "true")));
    assert!(output.contains(&field("reference_roles_test_only", "true")));
    assert!(output.contains(&field("baselines_fallback_free", "true")));
    assert!(output.contains(&field("production_claim_allowed", "false")));
    assert!(output.contains(&field("benchmark_claims_blocked_by_correctness", "true")));
    assert!(output.contains(&field("query_execution", "false")));
    assert!(output.contains(&field("decoded_reference_execution_performed", "false")));
    assert!(output.contains(&field("external_engine_execution", "false")));
    assert!(output.contains(&field("data_read", "false")));
    assert!(output.contains(&field("object_store_io", "false")));
    assert!(output.contains(&field("write_io", "false")));
    assert!(output.contains(&field("fallback_execution_allowed", "false")));
    assert!(output.contains(&field("fallback_attempted", "false")));
    assert!(output.contains(&field("side_effect_free", "true")));
}
