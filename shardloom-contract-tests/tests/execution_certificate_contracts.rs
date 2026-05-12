use shardloom_core::{
    Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity, ExecutionCertificate,
    ExecutionCertificateInput, ExecutionCertificateStatus, ExecutionProviderKind, ExpectedOutcome,
    FallbackStatus,
};

fn certified_input() -> ExecutionCertificateInput {
    let mut input =
        ExecutionCertificateInput::new("cg16-local-count", "vortex.local_encoded_count")
            .expect("certificate input");
    input.correctness_fixture_id = Some("vortex-local-encoded-count-u64-20000".to_string());
    input.input_ref =
        Some("shardloom-vortex/tests/fixtures/metadata_footer_u64_20000.vortex".to_string());
    input.output_ref = Some("count_all_result=20000".to_string());
    input.expected_outcome = Some(ExpectedOutcome::EncodedCount { count: 20000 });
    input.actual_outcome = Some(ExpectedOutcome::EncodedCount { count: 20000 });
    input.data_read = true;
    input.correctness_passed = true;
    input
}

#[test]
fn execution_certificate_certifies_matching_reference_output() {
    let certificate = ExecutionCertificate::evaluate(certified_input());

    assert_eq!(
        certificate.schema_version,
        "shardloom.execution_certificate.v1"
    );
    assert_eq!(certificate.status, ExecutionCertificateStatus::Certified);
    assert!(certificate.is_certified());
    assert!(certificate.fallback_free());
    assert_eq!(
        certificate.correctness_fixture_id.as_deref(),
        Some("vortex-local-encoded-count-u64-20000")
    );
    assert_eq!(
        certificate.expected_outcome,
        Some(ExpectedOutcome::EncodedCount { count: 20000 })
    );
    assert_eq!(certificate.expected_outcome, certificate.actual_outcome);
    assert_eq!(
        certificate.execution_provider_kind,
        ExecutionProviderKind::ShardLoomKernel
    );
    assert_eq!(certificate.provider_scope, "native");
    assert!(!certificate.external_query_engine_invoked);
}

#[test]
fn execution_certificate_blocks_fallback_attempts() {
    let mut input = certified_input();
    input.fallback_attempted = true;

    let certificate = ExecutionCertificate::evaluate(input);

    assert_eq!(certificate.status, ExecutionCertificateStatus::Blocked);
    assert!(!certificate.fallback_free());
}

#[test]
fn execution_certificate_blocks_unsafe_effects() {
    let mut input = certified_input();
    input.unsafe_effect_detected = true;
    input.data_materialized = true;

    let certificate = ExecutionCertificate::evaluate(input);

    assert_eq!(certificate.status, ExecutionCertificateStatus::Blocked);
    assert!(certificate.data_materialized);
}

#[test]
fn execution_certificate_blocks_diagnostic_errors() {
    let mut input = certified_input();
    input.diagnostics.push(Diagnostic::new(
        DiagnosticCode::NotImplemented,
        DiagnosticSeverity::Error,
        DiagnosticCategory::UnsupportedFeature,
        "certificate blocker",
        None,
        None,
        None,
        FallbackStatus::disabled_by_policy(),
    ));

    let certificate = ExecutionCertificate::evaluate(input);

    assert_eq!(certificate.status, ExecutionCertificateStatus::Blocked);
}

#[test]
fn execution_certificate_blocks_external_query_engine_providers() {
    let mut input = certified_input();
    input.execution_provider_kind = ExecutionProviderKind::ExternalBaseline;

    let certificate = ExecutionCertificate::evaluate(input);

    assert_eq!(certificate.status, ExecutionCertificateStatus::Blocked);
    assert_eq!(
        certificate.execution_provider_kind,
        ExecutionProviderKind::ExternalBaseline
    );
}
