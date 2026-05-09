use shardloom_core::{
    BaselineEngine, CorrectnessValidationPlan, DifferentialBaseline, ReferenceRole,
};

#[test]
fn foundation_plan_declares_external_oracles_as_comparison_only() {
    let plan = CorrectnessValidationPlan::default_foundation_plan();
    let expected = [
        BaselineEngine::Spark,
        BaselineEngine::DataFusion,
        BaselineEngine::DuckDb,
        BaselineEngine::Polars,
        BaselineEngine::Pandas,
        BaselineEngine::Dask,
        BaselineEngine::Velox,
    ];

    assert!(plan.baseline_count() >= expected.len());
    assert!(plan.baselines_are_fallback_free());
    for engine in expected {
        assert!(plan.has_baseline(engine), "missing {}", engine.as_str());
        let baseline = plan
            .baselines
            .iter()
            .find(|baseline| baseline.engine == engine)
            .expect("baseline");
        assert_eq!(baseline.role, ReferenceRole::ExternalOracle);
        assert!(!baseline.is_fallback_allowed());
        assert!(baseline.summary().contains("test/comparison only"));
        assert!(
            baseline
                .notes
                .as_deref()
                .is_some_and(|notes| notes.contains("no runtime fallback"))
        );
    }
}

#[test]
fn differential_baseline_oracle_constructor_is_never_fallback_capable() {
    let baseline = DifferentialBaseline::external_correctness_oracle(BaselineEngine::Spark);

    assert_eq!(baseline.engine, BaselineEngine::Spark);
    assert_eq!(baseline.role, ReferenceRole::ExternalOracle);
    assert!(!baseline.is_fallback_allowed());
    assert!(baseline.version.is_none());
    assert!(
        baseline
            .notes
            .as_deref()
            .is_some_and(|notes| notes.contains("external correctness oracle"))
    );
}
