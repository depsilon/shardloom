use shardloom_core::{
    BenchmarkClaimGate, BenchmarkClaimStatus, BenchmarkEvidenceState, BenchmarkFallbackState,
    BenchmarkMetric, BenchmarkPlan, CorrectnessValidationMode,
};

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

#[test]
fn benchmark_claim_gate_blocks_without_correctness_and_benchmark_evidence() {
    let plan = BenchmarkPlan::default_foundation_plan();
    let gate = plan.claim_gate();

    assert_eq!(gate.required_metrics, BenchmarkEvidenceState::Present);
    assert_eq!(gate.correctness_evidence, BenchmarkEvidenceState::Missing);
    assert_eq!(gate.benchmark_evidence, BenchmarkEvidenceState::Missing);
    assert_eq!(gate.comparison_report, BenchmarkEvidenceState::Missing);
    assert_eq!(gate.fallback, BenchmarkFallbackState::NotAttempted);
    assert_eq!(gate.status, BenchmarkClaimStatus::EvidenceMissing);
    assert!(!gate.can_publish_performance_claim());
    assert!(
        plan.to_human_text()
            .contains("claim gate: evidence_missing")
    );
}

#[test]
fn benchmark_claim_gate_requires_every_publication_input() {
    let present = BenchmarkEvidenceState::Present;
    let missing = BenchmarkEvidenceState::Missing;
    let no_fallback = BenchmarkFallbackState::NotAttempted;
    let fallback = BenchmarkFallbackState::Attempted;
    let missing_correctness =
        BenchmarkClaimGate::new(missing, present, present, present, no_fallback);
    let missing_benchmark =
        BenchmarkClaimGate::new(present, missing, present, present, no_fallback);
    let missing_metrics = BenchmarkClaimGate::new(present, present, missing, present, no_fallback);
    let missing_comparison =
        BenchmarkClaimGate::new(present, present, present, missing, no_fallback);
    let fallback_attempted = BenchmarkClaimGate::new(present, present, present, present, fallback);
    let ready = BenchmarkClaimGate::new(present, present, present, present, no_fallback);

    for gate in [
        missing_correctness,
        missing_benchmark,
        missing_metrics,
        missing_comparison,
        fallback_attempted,
    ] {
        assert_eq!(gate.status, BenchmarkClaimStatus::EvidenceMissing);
        assert!(!gate.can_publish_performance_claim());
    }
    assert_eq!(ready.status, BenchmarkClaimStatus::ReadyToPublish);
    assert!(ready.can_publish_performance_claim());
}
