use shardloom_core::{
    Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity, FallbackStatus,
    OutputEnvelope,
};

#[test]
fn machine_readable_diagnostic_strings_are_stable() {
    assert_eq!(
        DiagnosticCode::NoFallbackExecution.as_str(),
        "SL_NO_FALLBACK_EXECUTION"
    );
    assert_eq!(DiagnosticCode::MetadataLoss.as_str(), "SL_METADATA_LOSS");
    assert_eq!(
        DiagnosticCategory::NoFallbackPolicy.as_str(),
        "no_fallback_policy"
    );
    assert_eq!(DiagnosticCategory::MetadataLoss.as_str(), "metadata_loss");
    assert_eq!(DiagnosticSeverity::Error.as_str(), "error");
    assert_eq!(DiagnosticSeverity::Warning.as_str(), "warning");

    let fallback_json = FallbackStatus::disabled_by_policy().to_json();
    assert!(fallback_json.contains("\"attempted\":false"));
    assert!(fallback_json.contains("\"allowed\":false"));

    let diag_json = Diagnostic::unsupported(
        DiagnosticCode::NotImplemented,
        "feature",
        "reason",
        Some("action".into()),
    )
    .to_json();
    assert!(diag_json.contains("\"code\""));
    assert!(diag_json.contains("\"severity\""));
    assert!(diag_json.contains("\"category\""));
    assert!(diag_json.contains("\"fallback\""));

    let env_json = OutputEnvelope::success("status", "ok", "ok").to_json();
    for key in [
        "schema_version",
        "command",
        "status",
        "fallback",
        "diagnostics",
        "fields",
    ] {
        assert!(env_json.contains(&format!("\"{key}\"")));
    }
}
