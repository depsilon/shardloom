use shardloom_core::{
    CorrectnessDifferentialHarnessReport, CorrectnessDifferentialHarnessStatus,
    CorrectnessValidationPlan, plan_correctness_differential_harness,
};

#[test]
fn correctness_harness_aggregates_current_cg5_evidence_without_execution() {
    let report =
        plan_correctness_differential_harness(CorrectnessValidationPlan::default_foundation_plan());

    assert_eq!(
        report.schema_version,
        "shardloom.correctness_differential_harness.v1"
    );
    assert_eq!(
        report.report_id,
        "cg5.correctness_differential_harness.aggregate"
    );
    assert_eq!(
        report.status,
        CorrectnessDifferentialHarnessStatus::NeedsEvidence
    );
    assert_eq!(report.fixture_count, 34);
    assert_eq!(report.fixtures_with_source_ref_count, 16);
    assert_eq!(report.source_backed_edge_fixture_count, 9);
    assert_eq!(
        report.source_backed_edge_fixture_id_order,
        vec![
            "vortex-edge-count-all-empty-input".to_string(),
            "vortex-edge-project-single-row".to_string(),
            "vortex-edge-filter-all-null".to_string(),
            "vortex-edge-filter-mixed-null-sparse".to_string(),
            "vortex-edge-filter-duplicate-low-cardinality".to_string(),
            "vortex-edge-project-high-cardinality".to_string(),
            "vortex-edge-filter-project-sorted-dictionary".to_string(),
            "vortex-edge-filter-project-unsorted-rle".to_string(),
            "vortex-edge-filter-temporal-values".to_string(),
        ]
    );
    assert_eq!(report.golden_fixture_count, 19);
    assert_eq!(report.executable_expected_output_count, 18);
    assert_eq!(report.reference_artifact_count, 18);
    assert_eq!(report.decoded_reference_output_count, 18);
    assert_eq!(
        report.decoded_reference_artifact_id_order,
        vec![
            "vortex-local-encoded-count-u64-20000.decoded-reference.count".to_string(),
            "vortex-local-count-all-struct-five.decoded-reference.count".to_string(),
            "vortex-local-count-where-struct-five.decoded-reference.rows".to_string(),
            "vortex-local-project-struct-five.decoded-reference.rows".to_string(),
            "vortex-local-filter-struct-five.decoded-reference.rows".to_string(),
            "vortex-local-filter-project-struct-five.decoded-reference.rows".to_string(),
            "vortex-prepared-encoded-filter-dictionary-run.decoded-reference.rows".to_string(),
            "vortex-prepared-encoded-projection-dictionary.decoded-reference.rows".to_string(),
            "vortex-prepared-encoded-filter-project-selection-vector.decoded-reference.rows"
                .to_string(),
            "vortex-edge-count-all-empty-input.decoded-reference.count".to_string(),
            "vortex-edge-project-single-row.decoded-reference.rows".to_string(),
            "vortex-edge-filter-all-null.decoded-reference.rows".to_string(),
            "vortex-edge-filter-mixed-null-sparse.decoded-reference.rows".to_string(),
            "vortex-edge-filter-duplicate-low-cardinality.decoded-reference.rows".to_string(),
            "vortex-edge-project-high-cardinality.decoded-reference.rows".to_string(),
            "vortex-edge-filter-project-sorted-dictionary.decoded-reference.rows".to_string(),
            "vortex-edge-filter-project-unsorted-rle.decoded-reference.rows".to_string(),
            "vortex-edge-filter-temporal-values.decoded-reference.rows".to_string(),
        ]
    );
    assert!(report.decoded_reference_output_coverage_complete);
    assert_eq!(report.not_yet_defined_fixture_count, 0);
    assert_eq!(report.deferred_fixture_family_count, 8);
    assert_eq!(
        report.deferred_fixture_family_id_order,
        vec![
            "null-semantics".to_string(),
            "pruning-correctness".to_string(),
            "encoded-vs-decoded-reference".to_string(),
            "nested-data-edge-corpus".to_string(),
            "dictionary-encoded-edge-corpus".to_string(),
            "sparse-validity-edge-corpus".to_string(),
            "run-length-edge-corpus".to_string(),
            "temporal-semantics".to_string(),
        ]
    );
    assert_eq!(report.baseline_count, 7);
    assert_eq!(report.external_oracle_result_artifact_count, 63);
    assert_eq!(report.external_oracle_result_populated_count, 0);
    assert!(!report.external_oracle_results_populated);
    assert_eq!(
        report.external_oracle_result_artifact_status_order,
        vec!["declared_not_executed".to_string()]
    );
    assert!(report.external_oracle_artifacts_test_only);
    assert_eq!(
        report.benchmark_claim_blocker_order,
        vec![
            "deferred_fixture_families".to_string(),
            "external_oracle_results_not_populated".to_string(),
            "property_fuzz_execution_not_performed".to_string()
        ]
    );
    assert!(!report.property_fuzz_execution_performed);
    assert!(report.external_oracle_result_artifact_id_order.contains(
        &"vortex-edge-count-all-empty-input.external-oracle.spark.declared-result".to_string()
    ));
    assert_eq!(report.generated_property_fixture_count, 3);
    assert_eq!(report.fuzz_seed_count, 3);
    assert_eq!(report.planned_surface_count, 9);
    assert_eq!(report.blocked_surface_count, 1);
    assert_eq!(
        report.blocked_surface_order,
        vec!["benchmark_claim_gate".to_string()]
    );
    assert!(report.reference_roles_test_only);
    assert!(report.baselines_fallback_free);
    assert!(report.side_effect_free());
    assert!(!report.query_execution);
    assert!(!report.decoded_reference_execution_performed);
    assert!(!report.external_engine_execution);
    assert!(!report.data_read);
    assert!(!report.object_store_io);
    assert!(!report.write_io);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.fallback_attempted);
    assert!(!report.production_claim_allowed);
    assert!(report.benchmark_claims_blocked_by_correctness);
}

#[test]
fn correctness_harness_declares_validation_modes_and_oracle_order() {
    let report =
        plan_correctness_differential_harness(CorrectnessValidationPlan::default_foundation_plan());

    assert_eq!(
        CorrectnessDifferentialHarnessReport::surface_order(),
        vec![
            "fixture_manifest",
            "golden_fixtures",
            "source_backed_edge_fixtures",
            "decoded_reference_outputs",
            "differential_oracles",
            "external_oracle_result_artifacts",
            "semantic_edge_cases",
            "unsupported_diagnostics",
            "property_fuzzing",
            "benchmark_claim_gate"
        ]
    );
    assert!(report.missing_validation_mode_order().is_empty());
    assert_eq!(
        report.baseline_engine_order,
        vec![
            "spark".to_string(),
            "datafusion".to_string(),
            "duckdb".to_string(),
            "polars".to_string(),
            "pandas".to_string(),
            "dask".to_string(),
            "velox".to_string()
        ]
    );
}
