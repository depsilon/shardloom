use shardloom_core::{
    BenchmarkClaimEvidenceReport, BenchmarkClaimEvidenceStatus, BenchmarkClaimStatus,
    BenchmarkComparisonStatus, BenchmarkEvidenceState, BenchmarkPlan,
    BenchmarkReproducibilityStatus, plan_benchmark_claim_evidence,
};

#[test]
fn foundation_benchmark_claim_evidence_is_report_only() {
    let report =
        plan_benchmark_claim_evidence("foundation", &BenchmarkPlan::default_foundation_plan());

    assert_eq!(report.status, BenchmarkClaimEvidenceStatus::NeedsEvidence);
    assert_eq!(
        report.claim_gate_status,
        BenchmarkClaimStatus::EvidenceMissing
    );
    assert_eq!(
        report.run_manifest_status,
        BenchmarkReproducibilityStatus::Incomplete
    );
    assert_eq!(
        report.comparison_report_status,
        BenchmarkComparisonStatus::EvidenceMissing
    );
    assert_eq!(report.correctness_evidence, BenchmarkEvidenceState::Missing);
    assert_eq!(report.benchmark_evidence, BenchmarkEvidenceState::Missing);
    assert_eq!(
        report.required_metrics_evidence,
        BenchmarkEvidenceState::Present
    );
    assert_eq!(
        report.comparison_report_evidence,
        BenchmarkEvidenceState::Present
    );
    assert_eq!(
        report.reproducibility_evidence,
        BenchmarkEvidenceState::Missing
    );
    assert_eq!(report.required_foundation_metric_count, 21);
    assert_eq!(report.covered_required_foundation_metric_count, 21);
    assert!(report.missing_required_foundation_metrics.is_empty());
    assert_eq!(report.expected_result_count, 14);
    assert_eq!(report.result_count, 0);
    assert_eq!(report.missing_result_count, 14);
    assert!(report.missing_external_result_count > 0);
    assert!(!report.performance_claim_allowed);
    assert!(!report.superiority_claim_allowed);
    assert!(!report.best_default_claim_allowed);
    assert!(report.baselines_fallback_free);
    assert!(!report.fallback_execution_allowed);
    assert!(!report.fallback_attempted);
    assert!(!report.benchmark_execution_performed);
    assert!(!report.external_engine_execution);
    assert!(!report.query_execution);
    assert!(!report.data_read);
    assert!(!report.object_store_io);
    assert!(!report.write_io);
    assert!(report.side_effect_free());
}

#[test]
fn benchmark_claim_evidence_surfaces_publication_blockers() {
    let report =
        plan_benchmark_claim_evidence("foundation", &BenchmarkPlan::default_foundation_plan());

    assert_eq!(
        BenchmarkClaimEvidenceReport::surface_order(),
        vec![
            "benchmark_plan",
            "required_metrics",
            "correctness_evidence",
            "benchmark_result_rows",
            "external_comparison_results",
            "comparison_report",
            "reproducibility_manifest",
            "no_fallback_policy",
            "claim_publication_gate"
        ]
    );
    assert_eq!(
        report.blocked_surface_order,
        vec![
            "correctness_evidence",
            "benchmark_result_rows",
            "external_comparison_results",
            "reproducibility_manifest",
            "claim_publication_gate"
        ]
    );
    assert_eq!(report.blocked_surface_count, 5);
    assert_eq!(report.planned_surface_count, 4);
}

#[test]
fn traditional_claim_evidence_keeps_external_engines_comparison_only() {
    let report = plan_benchmark_claim_evidence(
        "traditional-analytics",
        &BenchmarkPlan::traditional_analytics_plan(),
    );

    assert_eq!(
        report.baseline_engine_order,
        vec![
            "shardloom",
            "pandas",
            "polars",
            "duckdb",
            "spark",
            "datafusion",
            "dask"
        ]
    );
    assert_eq!(
        report.external_baseline_engine_order,
        vec!["pandas", "polars", "duckdb", "spark", "datafusion", "dask"]
    );
    assert_eq!(report.external_baseline_count, 6);
    assert_eq!(report.expected_result_count, 35);
    assert_eq!(report.missing_result_count, 35);
    assert_eq!(report.missing_external_result_count, 30);
    assert!(report.baselines_fallback_free);
    assert!(!report.external_engine_execution);
    assert!(!report.fallback_attempted);
    assert!(!report.performance_claim_allowed);
}
