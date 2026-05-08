use shardloom_core::{
    BaselineEngine, BenchmarkClaimGate, BenchmarkClaimStatus, BenchmarkComparisonReport,
    BenchmarkComparisonStatus, BenchmarkEvidenceState, BenchmarkFallbackState, BenchmarkMetric,
    BenchmarkPlan, BenchmarkResult, BenchmarkScenario, CorrectnessValidationMode, MetricValue,
    WorkloadClass,
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

#[test]
fn benchmark_comparison_report_is_emitted_but_claim_blocked_without_results() {
    let plan = BenchmarkPlan::default_foundation_plan();
    let report = BenchmarkComparisonReport::from_plan(&plan);
    let gate = report.claim_gate();

    assert_eq!(report.status, BenchmarkComparisonStatus::EvidenceMissing);
    assert_eq!(report.scenario_count, plan.scenarios.len());
    assert_eq!(report.expected_result_count, plan.expected_result_count());
    assert_eq!(report.missing_results.len(), plan.expected_result_count());
    assert!(report.missing_metrics.is_empty());
    assert!(!report.fallback_execution_allowed());
    assert_eq!(gate.comparison_report, BenchmarkEvidenceState::Present);
    assert_eq!(gate.benchmark_evidence, BenchmarkEvidenceState::Missing);
    assert_eq!(gate.correctness_evidence, BenchmarkEvidenceState::Missing);
    assert_eq!(gate.status, BenchmarkClaimStatus::EvidenceMissing);
    assert!(!gate.can_publish_performance_claim());
    assert!(!report.diagnostics.is_empty());
    assert!(
        report
            .to_human_text()
            .contains("comparison report emitted: true")
    );
}

#[test]
fn benchmark_comparison_report_requires_known_metrics_for_each_baseline() {
    let mut plan = one_scenario_benchmark_plan();
    let scenario = &mut plan.scenarios[0];
    scenario.add_required_metric(BenchmarkMetric::WallTimeMillis);
    scenario.add_required_metric(BenchmarkMetric::QueryRuntimeMillis);

    let mut shardloom =
        BenchmarkResult::new("metadata count", BaselineEngine::ShardLoom).expect("valid");
    shardloom.add_metric(BenchmarkMetric::WallTimeMillis, MetricValue::U64(10));

    let report = BenchmarkComparisonReport::from_plan_and_results(
        &plan,
        vec![shardloom],
        BenchmarkEvidenceState::Present,
    );

    assert_eq!(report.status, BenchmarkComparisonStatus::EvidenceMissing);
    assert_eq!(report.missing_results.len(), 1);
    assert_eq!(report.missing_results[0].engine, BaselineEngine::DataFusion);
    assert_eq!(report.missing_metrics.len(), 1);
    assert_eq!(
        report.missing_metrics[0].metric,
        BenchmarkMetric::QueryRuntimeMillis
    );
    assert_eq!(
        report.claim_gate().benchmark_evidence,
        BenchmarkEvidenceState::Missing
    );
    assert!(!report.claim_gate().can_publish_performance_claim());
}

#[test]
fn complete_benchmark_comparison_report_becomes_ready_for_claim_review() {
    let mut plan = one_scenario_benchmark_plan();
    plan.scenarios[0].add_required_metric(BenchmarkMetric::WallTimeMillis);

    let mut shardloom =
        BenchmarkResult::new("metadata count", BaselineEngine::ShardLoom).expect("valid");
    shardloom.add_metric(BenchmarkMetric::WallTimeMillis, MetricValue::U64(10));
    let mut datafusion =
        BenchmarkResult::new("metadata count", BaselineEngine::DataFusion).expect("valid");
    datafusion.add_metric(BenchmarkMetric::WallTimeMillis, MetricValue::U64(20));

    let report = BenchmarkComparisonReport::from_plan_and_results(
        &plan,
        vec![shardloom, datafusion],
        BenchmarkEvidenceState::Present,
    );

    assert_eq!(
        report.status,
        BenchmarkComparisonStatus::ReadyForClaimReview
    );
    assert!(report.missing_results.is_empty());
    assert!(report.missing_metrics.is_empty());
    assert_eq!(report.benchmark_evidence, BenchmarkEvidenceState::Present);
    assert_eq!(
        report.claim_gate().status,
        BenchmarkClaimStatus::ReadyToPublish
    );
    assert!(report.claim_gate().can_publish_performance_claim());
    assert!(
        !report
            .to_human_text()
            .contains("fallback execution: enabled")
    );
}

fn one_scenario_benchmark_plan() -> BenchmarkPlan {
    let mut plan = BenchmarkPlan::new();
    let mut scenario =
        BenchmarkScenario::new("metadata count", WorkloadClass::SingleNodeEncodedExecution)
            .expect("valid");
    scenario.correctness_validation = CorrectnessValidationMode::ExpectedOutput;
    scenario.add_baseline(BaselineEngine::ShardLoom);
    scenario.add_baseline(BaselineEngine::DataFusion);
    plan.add_scenario(scenario);
    plan
}
