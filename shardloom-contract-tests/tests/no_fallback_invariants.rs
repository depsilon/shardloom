use shardloom_core::{
    BaselineEngine, CorrectnessValidationPlan, Diagnostic, DiagnosticCode, DifferentialBaseline,
    EngineCapabilities, NoFallbackReleaseCheck, OutputEnvelope, ReleasePlan,
};

#[test]
fn fallback_execution_remains_disabled_everywhere() {
    assert!(!EngineCapabilities::current().fallback_execution_allowed());
    assert!(!BaselineEngine::Spark.is_fallback_allowed());
    assert!(!BaselineEngine::DataFusion.is_fallback_allowed());
    assert!(!BaselineEngine::DuckDb.is_fallback_allowed());
    assert!(!DifferentialBaseline::new(BaselineEngine::Spark).is_fallback_allowed());
    assert!(!CorrectnessValidationPlan::default_foundation_plan().fallback_execution_allowed());

    let envelope = OutputEnvelope::success("status", "ok", "ok");
    assert!(!envelope.fallback.allowed);

    let d = Diagnostic::unsupported(DiagnosticCode::NotImplemented, "x", "y", Some("z".into()));
    assert!(!d.fallback.attempted);

    assert!(NoFallbackReleaseCheck::clean().is_clean());
    let plan = ReleasePlan::default_foundation_plan();
    assert!(plan.no_fallback_check.is_clean());
    assert!(!plan.publish_allowed());
}
