use shardloom_core::{
    BaselineEngine, BenchmarkCacheState, BenchmarkClaimGate, BenchmarkClaimStatus,
    BenchmarkComparisonReport, BenchmarkComparisonStatus, BenchmarkConstitutionValidationStatus,
    BenchmarkEngineVersion, BenchmarkEvidenceBundle, BenchmarkEvidenceState,
    BenchmarkFallbackState, BenchmarkMetric, BenchmarkPlan, BenchmarkReproducibilityStatus,
    BenchmarkResult, BenchmarkRunManifest, BenchmarkScenario, CorrectnessValidationMode,
    MetricValue, WorkloadClass, benchmark_constitution_validation_from_parts,
    plan_benchmark_constitution_validation,
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

    assert!(plan.scenarios.len() >= 7);
    for metric in required {
        assert!(plan.covers_metric(metric), "missing {}", metric.as_str());
    }
    assert_eq!(plan.scenario_count(), 7);
    assert_eq!(plan.required_metrics().len(), 21);
    assert_eq!(BenchmarkPlan::required_foundation_metrics().len(), 21);
    assert_eq!(plan.covered_required_foundation_metric_count(), 21);
    assert!(plan.required_foundation_metrics_covered());
    assert!(plan.missing_required_foundation_metrics().is_empty());
    assert_eq!(plan.scenario_with_correctness_validation_count(), 7);
    assert_eq!(plan.scenario_with_required_metrics_count(), 7);
    assert_eq!(plan.scenario_with_baselines_count(), 7);
    assert_eq!(plan.expected_result_count(), 14);
    assert_eq!(plan.external_baseline_count(), 5);
    assert_eq!(
        plan.baseline_engine_order(),
        vec![
            "shardloom",
            "datafusion",
            "vortex_integration",
            "spark",
            "polars",
            "other"
        ]
    );
    assert!(plan.runtime_metrics_covered());
    assert!(plan.peak_memory_metric_covered());
    assert!(plan.bytes_read_written_metrics_covered());
    assert!(plan.startup_latency_metric_covered());
    assert!(plan.query_runtime_metric_covered());
    assert!(plan.write_commit_latency_metric_covered());
    assert!(plan.spill_metrics_covered());
    assert!(plan.object_store_request_metric_covered());
    assert!(plan.materialization_metrics_covered());
    assert!(!plan.benchmark_execution_implemented());
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
    assert_eq!(
        gate.reproducibility_evidence,
        BenchmarkEvidenceState::Missing
    );
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
        BenchmarkClaimGate::new(missing, present, present, present, present, no_fallback);
    let missing_benchmark =
        BenchmarkClaimGate::new(present, missing, present, present, present, no_fallback);
    let missing_metrics =
        BenchmarkClaimGate::new(present, present, missing, present, present, no_fallback);
    let missing_comparison =
        BenchmarkClaimGate::new(present, present, present, missing, present, no_fallback);
    let missing_reproducibility =
        BenchmarkClaimGate::new(present, present, present, present, missing, no_fallback);
    let fallback_attempted =
        BenchmarkClaimGate::new(present, present, present, present, present, fallback);
    let ready = BenchmarkClaimGate::new(present, present, present, present, present, no_fallback);

    for gate in [
        missing_correctness,
        missing_benchmark,
        missing_metrics,
        missing_comparison,
        missing_reproducibility,
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
    assert_eq!(
        gate.reproducibility_evidence,
        BenchmarkEvidenceState::Missing
    );
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
fn complete_benchmark_comparison_report_still_needs_reproducibility_for_claims() {
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
        BenchmarkComparisonStatus::ReadyForComparisonReview
    );
    assert!(report.missing_results.is_empty());
    assert!(report.missing_metrics.is_empty());
    assert_eq!(report.benchmark_evidence, BenchmarkEvidenceState::Present);
    assert_eq!(
        report.claim_gate().status,
        BenchmarkClaimStatus::EvidenceMissing
    );
    assert_eq!(
        report.claim_gate().reproducibility_evidence,
        BenchmarkEvidenceState::Missing
    );
    assert!(!report.claim_gate().can_publish_performance_claim());
    assert!(
        !report
            .to_human_text()
            .contains("fallback execution: enabled")
    );
}

#[test]
fn benchmark_evidence_bundle_requires_reproducible_manifest() {
    let mut plan = one_scenario_benchmark_plan();
    plan.scenarios[0].add_required_metric(BenchmarkMetric::WallTimeMillis);
    let report = complete_comparison_report(&plan);
    let manifest = BenchmarkRunManifest::from_plan(&plan);

    let bundle = BenchmarkEvidenceBundle::from_reports(manifest, report);

    assert_eq!(
        bundle.claim_gate.status,
        BenchmarkClaimStatus::EvidenceMissing
    );
    assert_eq!(
        bundle.claim_gate.reproducibility_evidence,
        BenchmarkEvidenceState::Missing
    );
    assert!(!bundle.can_publish_performance_claim());
    assert!(!bundle.diagnostics.is_empty());
}

#[test]
fn complete_benchmark_evidence_bundle_allows_publication_gate() {
    let mut plan = one_scenario_benchmark_plan();
    plan.scenarios[0].dataset_name = Some("metadata-footer-u64".to_string());
    plan.scenarios[0].dataset_scale = Some("20k_rows".to_string());
    plan.scenarios[0].storage_format = Some("vortex".to_string());
    plan.scenarios[0].add_required_metric(BenchmarkMetric::WallTimeMillis);
    let report = complete_comparison_report(&plan);
    let manifest = complete_run_manifest(&plan);

    let bundle = BenchmarkEvidenceBundle::from_reports(manifest, report);

    assert_eq!(
        bundle.claim_gate.status,
        BenchmarkClaimStatus::ReadyToPublish
    );
    assert_eq!(
        bundle.claim_gate.reproducibility_evidence,
        BenchmarkEvidenceState::Present
    );
    assert!(bundle.can_publish_performance_claim());
    assert!(bundle.diagnostics.is_empty());
    assert!(
        bundle
            .to_human_text()
            .contains("claim gate: ready_to_publish")
    );
}

#[test]
fn benchmark_run_manifest_defaults_to_incomplete_reproducibility() {
    let plan = BenchmarkPlan::default_foundation_plan();
    let manifest = BenchmarkRunManifest::from_plan(&plan);

    assert_eq!(manifest.status, BenchmarkReproducibilityStatus::Incomplete);
    assert_eq!(manifest.scenario_count, plan.scenarios.len());
    assert_eq!(manifest.required_metrics, plan.required_metrics());
    assert_eq!(manifest.missing_engine_versions, plan.baseline_engines());
    assert!(!manifest.fallback_execution_allowed());
    assert!(!manifest.required_metadata_present(&plan));
    assert!(!manifest.diagnostics.is_empty());
    assert!(
        manifest
            .to_human_text()
            .contains("reproducibility status: incomplete")
    );
}

#[test]
fn benchmark_run_manifest_requires_environment_dataset_versions_and_steps() {
    let mut plan = one_scenario_benchmark_plan();
    plan.scenarios[0].dataset_name = Some("metadata-footer-u64".to_string());
    plan.scenarios[0].dataset_scale = Some("20k_rows".to_string());
    plan.scenarios[0].storage_format = Some("vortex".to_string());
    plan.scenarios[0].add_required_metric(BenchmarkMetric::WallTimeMillis);

    let manifest = complete_run_manifest(&plan);

    assert_eq!(
        manifest.status,
        BenchmarkReproducibilityStatus::Reproducible
    );
    assert!(manifest.required_metadata_present(&plan));
    assert!(manifest.missing_engine_versions.is_empty());
    assert!(manifest.diagnostics.is_empty());
    assert!(
        manifest
            .to_human_text()
            .contains("reproducibility status: reproducible")
    );
}

#[test]
fn benchmark_constitution_validator_rejects_missing_claim_grade_fields() {
    let plan = BenchmarkPlan::default_foundation_plan();
    let report = plan_benchmark_constitution_validation("foundation", &plan);

    assert_eq!(
        report.status,
        BenchmarkConstitutionValidationStatus::MissingEvidence
    );
    assert_eq!(
        report.claim_gate_status,
        BenchmarkClaimStatus::EvidenceMissing
    );
    assert_eq!(report.row_count, plan.expected_result_count());
    assert_eq!(report.complete_row_count, 0);
    for required in [
        "benchmark_result_row",
        "dataset_source_admission",
        "preparation_route",
        "execution_route",
        "output_route",
        "correctness_proof",
        "hardware_profile",
        "build_profile",
        "cold_warm_state",
        "stage_timings",
    ] {
        assert!(
            report.missing_field_order.contains(&required.to_string()),
            "constitution validator did not report missing {required}"
        );
    }
    assert!(report.external_baselines_comparison_only);
    assert!(!report.performance_claim_allowed);
    assert!(!report.superiority_claim_allowed);
    assert!(!report.fallback_attempted);
    assert!(!report.external_engine_invoked);
}

#[test]
fn benchmark_constitution_validator_accepts_complete_synthetic_row() {
    let mut plan = one_scenario_benchmark_plan();
    plan.scenarios[0].dataset_name = Some("metadata-footer-u64".to_string());
    plan.scenarios[0].dataset_scale = Some("20k_rows".to_string());
    plan.scenarios[0].storage_format = Some("vortex".to_string());
    plan.scenarios[0].query_or_operation = Some("count".to_string());
    plan.scenarios[0].add_required_metric(BenchmarkMetric::WallTimeMillis);
    plan.scenarios[0].add_required_metric(BenchmarkMetric::RowsMaterialized);
    let mut manifest = complete_run_manifest(&plan);
    manifest.add_engine_version(
        BenchmarkEngineVersion::new(BaselineEngine::ShardLoom, "0.1.0").expect("valid"),
    );
    manifest.add_engine_version(
        BenchmarkEngineVersion::new(BaselineEngine::DataFusion, "comparison-only").expect("valid"),
    );
    manifest.refresh_against_plan(&plan);
    let mut shardloom =
        BenchmarkResult::new("metadata count", BaselineEngine::ShardLoom).expect("valid");
    shardloom.add_metric(BenchmarkMetric::WallTimeMillis, MetricValue::U64(10));
    shardloom.add_metric(BenchmarkMetric::RowsMaterialized, MetricValue::U64(1));
    let mut datafusion =
        BenchmarkResult::new("metadata count", BaselineEngine::DataFusion).expect("valid");
    datafusion.add_metric(BenchmarkMetric::WallTimeMillis, MetricValue::U64(20));
    datafusion.add_metric(BenchmarkMetric::RowsMaterialized, MetricValue::U64(1));
    let comparison = BenchmarkComparisonReport::from_plan_and_results(
        &plan,
        vec![shardloom, datafusion],
        BenchmarkEvidenceState::Present,
    );

    let report =
        benchmark_constitution_validation_from_parts("complete", &plan, &manifest, &comparison);

    assert_eq!(
        report.status,
        BenchmarkConstitutionValidationStatus::ReadyForClaimReview
    );
    assert!(report.missing_field_order.is_empty());
    assert_eq!(report.complete_row_count, 2);
    assert!(report.performance_claim_allowed);
    assert!(report.external_baselines_comparison_only);
    assert!(report.no_fallback_proof_present);
}

#[test]
fn benchmark_engine_version_labels_are_comparison_only() {
    let version =
        BenchmarkEngineVersion::new(BaselineEngine::Spark, "comparison-only").expect("valid");

    assert!(!version.fallback_execution_allowed());
    assert!(BenchmarkEngineVersion::new(BaselineEngine::Spark, " ").is_err());
}

#[test]
fn traditional_analytics_plan_covers_dataframe_sql_baselines() {
    let plan = BenchmarkPlan::traditional_analytics_plan();

    assert_eq!(plan.scenario_count(), 5);
    assert_eq!(
        plan.scenario_name_order(),
        vec![
            "csv/file ingest",
            "selective filter",
            "group by aggregation",
            "sort and top-k",
            "hash join"
        ]
    );
    assert_eq!(
        plan.baseline_engine_order(),
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
    assert_eq!(plan.external_baseline_count(), 6);
    assert!(plan.baselines_are_fallback_free());
    assert!(plan.covers_metric(BenchmarkMetric::PeakMemoryBytes));
    assert!(plan.covers_metric(BenchmarkMetric::RowsScanned));
    assert!(plan.covers_metric(BenchmarkMetric::RowsMaterialized));
    assert!(plan.covers_metric(BenchmarkMetric::SpillRequiredBytes));
    assert!(plan.covers_metric(BenchmarkMetric::ObjectStoreRequests));
    assert!(!plan.benchmark_execution_implemented());
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

fn complete_comparison_report(plan: &BenchmarkPlan) -> BenchmarkComparisonReport {
    let mut shardloom =
        BenchmarkResult::new("metadata count", BaselineEngine::ShardLoom).expect("valid");
    shardloom.add_metric(BenchmarkMetric::WallTimeMillis, MetricValue::U64(10));
    let mut datafusion =
        BenchmarkResult::new("metadata count", BaselineEngine::DataFusion).expect("valid");
    datafusion.add_metric(BenchmarkMetric::WallTimeMillis, MetricValue::U64(20));
    BenchmarkComparisonReport::from_plan_and_results(
        plan,
        vec![shardloom, datafusion],
        BenchmarkEvidenceState::Present,
    )
}

fn complete_run_manifest(plan: &BenchmarkPlan) -> BenchmarkRunManifest {
    let mut manifest = BenchmarkRunManifest::from_plan(plan);
    manifest.dataset_profiles[0].schema_profile = Some("single u64 column".to_string());
    manifest.dataset_profiles[0].compression = Some("fixture default".to_string());
    manifest.hardware_profile = Some("local-ci-x64".to_string());
    manifest.operating_system_profile = Some("windows-latest".to_string());
    manifest.runtime_configuration = Some("release=false; toolchain=1.91.1".to_string());
    manifest.cache_state = BenchmarkCacheState::Cold;
    manifest.correctness_evidence = BenchmarkEvidenceState::Present;
    manifest.add_reproduction_step("build workspace");
    manifest.add_reproduction_step("run approved benchmark harness");
    manifest.add_engine_version(
        BenchmarkEngineVersion::new(BaselineEngine::ShardLoom, "0.1.0").expect("valid"),
    );
    manifest.add_engine_version(
        BenchmarkEngineVersion::new(BaselineEngine::DataFusion, "comparison-only").expect("valid"),
    );
    manifest.refresh_against_plan(plan);
    manifest
}
