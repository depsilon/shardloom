use shardloom_core::{BenchmarkMetric, BenchmarkPlan, CorrectnessValidationMode};

#[test]
fn foundation_benchmark_plan_covers_cg6_evidence_categories() {
    let plan = BenchmarkPlan::default_foundation_plan();
    let required = [
        BenchmarkMetric::StartupLatencyMillis,
        BenchmarkMetric::QueryRuntimeMillis,
        BenchmarkMetric::WallTimeMillis,
        BenchmarkMetric::PeakMemoryBytes,
        BenchmarkMetric::BytesRead,
        BenchmarkMetric::BytesWritten,
        BenchmarkMetric::BytesDecoded,
        BenchmarkMetric::BytesDecodeAvoided,
        BenchmarkMetric::RowsMaterialized,
        BenchmarkMetric::RowsMaterializationAvoided,
        BenchmarkMetric::SegmentsConsidered,
        BenchmarkMetric::SegmentsPruned,
        BenchmarkMetric::SegmentsMetadataAnswered,
        BenchmarkMetric::WorkAvoidedUnits,
        BenchmarkMetric::SpillRequiredBytes,
        BenchmarkMetric::SpillAvoidedBytes,
        BenchmarkMetric::WriteCommitLatencyMillis,
        BenchmarkMetric::ObjectStoreRequests,
    ];

    assert!(plan.scenarios.len() >= 5);
    for metric in required {
        assert!(plan.covers_metric(metric), "missing {}", metric.as_str());
    }
    assert!(plan.baselines_are_fallback_free());
    assert!(
        plan.to_human_text()
            .contains("benchmark execution is not implemented yet")
    );
}

#[test]
fn benchmark_scenarios_are_correctness_gated_before_claims() {
    let plan = BenchmarkPlan::default_foundation_plan();

    for scenario in &plan.scenarios {
        assert_ne!(
            scenario.correctness_validation,
            CorrectnessValidationMode::NotYetDefined
        );
        assert!(!scenario.fallback_execution_allowed());
        assert!(!scenario.required_metrics.is_empty());
        assert!(!scenario.baselines.is_empty());
    }
}
