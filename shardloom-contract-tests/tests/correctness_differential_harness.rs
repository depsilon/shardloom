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
    assert_eq!(report.fixture_count, 22);
    assert_eq!(report.golden_fixture_count, 10);
    assert_eq!(report.executable_expected_output_count, 9);
    assert_eq!(report.reference_artifact_count, 9);
    assert_eq!(report.decoded_reference_output_count, 9);
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
        ]
    );
    assert!(report.decoded_reference_output_coverage_complete);
    assert_eq!(report.baseline_count, 7);
    assert_eq!(report.planned_surface_count, 6);
    assert_eq!(report.blocked_surface_count, 2);
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
            "decoded_reference_outputs",
            "differential_oracles",
            "semantic_edge_cases",
            "unsupported_diagnostics",
            "property_fuzzing",
            "benchmark_claim_gate"
        ]
    );
    assert_eq!(
        report.missing_validation_mode_order(),
        vec!["property_based", "fuzz"]
    );
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
