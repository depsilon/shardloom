//! Evidence-first execution certificate contracts.
//!
//! Certificates record what a supported execution path did, what it avoided,
//! and which correctness reference output it matched. They are evidence objects,
//! not permission to use external fallback engines.

#![allow(clippy::must_use_candidate, clippy::return_self_not_must_use)]

use crate::{
    Diagnostic, DiagnosticSeverity, ExpectedOutcome, Result, ShardLoomError,
    architecture_spine::ExecutionProviderKind,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionCertificateStatus {
    EvidenceIncomplete,
    Certified,
    Blocked,
}
impl ExecutionCertificateStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::EvidenceIncomplete => "evidence_incomplete",
            Self::Certified => "certified",
            Self::Blocked => "blocked",
        }
    }
    pub const fn is_certified(&self) -> bool {
        matches!(self, Self::Certified)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionEvidenceArtifactKind {
    Plan,
    InputSnapshot,
    OutputPayload,
    SegmentTrace,
    SideEffectManifest,
    ReproducibilityMetadata,
}
impl ExecutionEvidenceArtifactKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::InputSnapshot => "input_snapshot",
            Self::OutputPayload => "output_payload",
            Self::SegmentTrace => "segment_trace",
            Self::SideEffectManifest => "side_effect_manifest",
            Self::ReproducibilityMetadata => "reproducibility_metadata",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionEvidenceArtifactStatus {
    Required,
    Present,
    Deferred,
    Blocked,
}
impl ExecutionEvidenceArtifactStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Required => "required",
            Self::Present => "present",
            Self::Deferred => "deferred",
            Self::Blocked => "blocked",
        }
    }
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Blocked)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ExecutionEvidenceArtifactRequirement {
    pub artifact_id: String,
    pub kind: ExecutionEvidenceArtifactKind,
    pub status: ExecutionEvidenceArtifactStatus,
    pub stable_ref_required: bool,
    pub content_hash_required: bool,
    pub machine_readable_required: bool,
    pub diagnostic_ref_required: bool,
}
impl ExecutionEvidenceArtifactRequirement {
    pub fn required(artifact_id: impl Into<String>, kind: ExecutionEvidenceArtifactKind) -> Self {
        Self {
            artifact_id: artifact_id.into(),
            kind,
            status: ExecutionEvidenceArtifactStatus::Required,
            stable_ref_required: true,
            content_hash_required: true,
            machine_readable_required: true,
            diagnostic_ref_required: true,
        }
    }

    pub const fn has_errors(&self) -> bool {
        self.status.is_error()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionCertificateEvidenceSurfaceStatus {
    ReportOnlyPlanned,
    EvidenceIncomplete,
    Certified,
    Blocked,
}
impl ExecutionCertificateEvidenceSurfaceStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnlyPlanned => "report_only_planned",
            Self::EvidenceIncomplete => "evidence_incomplete",
            Self::Certified => "certified",
            Self::Blocked => "blocked",
        }
    }

    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Blocked)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ExecutionCertificateEvidenceSurfaceReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub status: ExecutionCertificateEvidenceSurfaceStatus,
    pub certificate_schema_version: &'static str,
    pub artifacts: Vec<ExecutionEvidenceArtifactRequirement>,
    pub plan_hash_required: bool,
    pub input_snapshot_hash_required: bool,
    pub output_hash_required: bool,
    pub selected_segment_trace_required: bool,
    pub skipped_segment_trace_required: bool,
    pub side_effect_manifest_required: bool,
    pub reproducibility_metadata_required: bool,
    pub correctness_fixture_required: bool,
    pub machine_readable_certificate_surface: bool,
    pub deterministic_field_order_required: bool,
    pub certificate_evaluation_performed: bool,
    pub runtime_execution: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_engine_execution: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub production_claim_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl ExecutionCertificateEvidenceSurfaceReport {
    pub fn cg16_foundation() -> Self {
        Self {
            schema_version: "shardloom.execution_certificate_evidence_surface.v1",
            report_id: "cg16.execution-certificate-evidence-surface".to_string(),
            status: ExecutionCertificateEvidenceSurfaceStatus::ReportOnlyPlanned,
            certificate_schema_version: "shardloom.execution_certificate.v1",
            artifacts: vec![
                ExecutionEvidenceArtifactRequirement::required(
                    "execution.plan_hash",
                    ExecutionEvidenceArtifactKind::Plan,
                ),
                ExecutionEvidenceArtifactRequirement::required(
                    "execution.input_snapshot_hash",
                    ExecutionEvidenceArtifactKind::InputSnapshot,
                ),
                ExecutionEvidenceArtifactRequirement::required(
                    "execution.output_hash",
                    ExecutionEvidenceArtifactKind::OutputPayload,
                ),
                ExecutionEvidenceArtifactRequirement::required(
                    "execution.selected_skipped_segment_trace",
                    ExecutionEvidenceArtifactKind::SegmentTrace,
                ),
                ExecutionEvidenceArtifactRequirement::required(
                    "execution.side_effect_manifest",
                    ExecutionEvidenceArtifactKind::SideEffectManifest,
                ),
                ExecutionEvidenceArtifactRequirement::required(
                    "execution.reproducibility_metadata",
                    ExecutionEvidenceArtifactKind::ReproducibilityMetadata,
                ),
            ],
            plan_hash_required: true,
            input_snapshot_hash_required: true,
            output_hash_required: true,
            selected_segment_trace_required: true,
            skipped_segment_trace_required: true,
            side_effect_manifest_required: true,
            reproducibility_metadata_required: true,
            correctness_fixture_required: true,
            machine_readable_certificate_surface: true,
            deterministic_field_order_required: true,
            certificate_evaluation_performed: false,
            runtime_execution: false,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_engine_execution: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            production_claim_allowed: false,
            diagnostics: Vec::new(),
        }
    }

    pub fn artifact_count(&self) -> usize {
        self.artifacts.len()
    }

    pub fn required_artifact_count(&self) -> usize {
        self.artifacts
            .iter()
            .filter(|artifact| artifact.status == ExecutionEvidenceArtifactStatus::Required)
            .count()
    }

    pub fn hash_required_count(&self) -> usize {
        self.artifacts
            .iter()
            .filter(|artifact| artifact.content_hash_required)
            .count()
    }

    pub fn machine_readable_required_count(&self) -> usize {
        self.artifacts
            .iter()
            .filter(|artifact| artifact.machine_readable_required)
            .count()
    }

    pub fn artifact_kind_count(&self, kind: ExecutionEvidenceArtifactKind) -> usize {
        self.artifacts
            .iter()
            .filter(|artifact| artifact.kind == kind)
            .count()
    }

    pub fn artifact_order(&self) -> String {
        self.artifacts
            .iter()
            .map(|artifact| artifact.kind.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub const fn is_side_effect_free(&self) -> bool {
        !self.certificate_evaluation_performed
            && !self.runtime_execution
            && !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_engine_execution
            && !self.fallback_execution_allowed
            && !self.fallback_attempted
    }

    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self
                .artifacts
                .iter()
                .any(ExecutionEvidenceArtifactRequirement::has_errors)
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    pub fn to_human_text(&self) -> String {
        format!(
            "execution certificate evidence surface\nschema_version: {}\nreport: {}\nstatus: {}\nartifacts: {}\ncertificate evaluation: disabled\nruntime execution: disabled\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.status.as_str(),
            self.artifact_count(),
        )
    }
}

pub fn plan_execution_certificate_evidence_surface() -> ExecutionCertificateEvidenceSurfaceReport {
    ExecutionCertificateEvidenceSurfaceReport::cg16_foundation()
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ExecutionCertificateInput {
    pub certificate_id: String,
    pub execution_kind: String,
    pub execution_provider_kind: ExecutionProviderKind,
    pub provider_scope: String,
    pub provider_crate: Option<String>,
    pub provider_version: Option<String>,
    pub provider_api_surface: Option<String>,
    pub shardloom_admission_policy: Option<String>,
    pub plan_ref: Option<String>,
    pub input_ref: Option<String>,
    pub output_ref: Option<String>,
    pub correctness_fixture_id: Option<String>,
    pub expected_outcome: Option<ExpectedOutcome>,
    pub actual_outcome: Option<ExpectedOutcome>,
    pub selected_segment_count: usize,
    pub skipped_segment_count: usize,
    pub side_effects_performed: Vec<String>,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub external_query_engine_invoked: bool,
    pub unsafe_effect_detected: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub correctness_passed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl ExecutionCertificateInput {
    /// # Errors
    /// Returns an error if `certificate_id` or `execution_kind` is empty.
    pub fn new(
        certificate_id: impl Into<String>,
        execution_kind: impl Into<String>,
    ) -> Result<Self> {
        let certificate_id = certificate_id.into();
        if certificate_id.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "execution certificate id cannot be empty".to_string(),
            ));
        }
        let execution_kind = execution_kind.into();
        if execution_kind.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "execution certificate kind cannot be empty".to_string(),
            ));
        }
        Ok(Self {
            certificate_id,
            execution_kind,
            execution_provider_kind: ExecutionProviderKind::ShardLoomKernel,
            provider_scope: "native".to_string(),
            provider_crate: None,
            provider_version: None,
            provider_api_surface: None,
            shardloom_admission_policy: None,
            plan_ref: None,
            input_ref: None,
            output_ref: None,
            correctness_fixture_id: None,
            expected_outcome: None,
            actual_outcome: None,
            selected_segment_count: 0,
            skipped_segment_count: 0,
            side_effects_performed: vec![],
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            external_query_engine_invoked: false,
            unsafe_effect_detected: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            correctness_passed: false,
            diagnostics: vec![],
        })
    }
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ExecutionCertificate {
    pub schema_version: &'static str,
    pub certificate_id: String,
    pub execution_kind: String,
    pub execution_provider_kind: ExecutionProviderKind,
    pub provider_scope: String,
    pub provider_crate: Option<String>,
    pub provider_version: Option<String>,
    pub provider_api_surface: Option<String>,
    pub shardloom_admission_policy: Option<String>,
    pub status: ExecutionCertificateStatus,
    pub plan_ref: Option<String>,
    pub input_ref: Option<String>,
    pub output_ref: Option<String>,
    pub correctness_fixture_id: Option<String>,
    pub expected_outcome: Option<ExpectedOutcome>,
    pub actual_outcome: Option<ExpectedOutcome>,
    pub selected_segment_count: usize,
    pub skipped_segment_count: usize,
    pub side_effects_performed: Vec<String>,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub external_query_engine_invoked: bool,
    pub unsafe_effect_detected: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub correctness_passed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl ExecutionCertificate {
    pub fn evaluate(input: ExecutionCertificateInput) -> Self {
        let expected_matches_actual =
            input.expected_outcome.is_some() && input.expected_outcome == input.actual_outcome;
        let external_provider_kind = matches!(
            input.execution_provider_kind,
            ExecutionProviderKind::ExternalBaseline
                | ExecutionProviderKind::ProhibitedExternalFallback
        );
        let status = if input.fallback_attempted
            || input.fallback_execution_allowed
            || input.external_query_engine_invoked
            || external_provider_kind
            || input.unsafe_effect_detected
            || input.has_errors()
        {
            ExecutionCertificateStatus::Blocked
        } else if input.correctness_passed && expected_matches_actual {
            ExecutionCertificateStatus::Certified
        } else {
            ExecutionCertificateStatus::EvidenceIncomplete
        };
        Self {
            schema_version: "shardloom.execution_certificate.v1",
            certificate_id: input.certificate_id,
            execution_kind: input.execution_kind,
            execution_provider_kind: input.execution_provider_kind,
            provider_scope: input.provider_scope,
            provider_crate: input.provider_crate,
            provider_version: input.provider_version,
            provider_api_surface: input.provider_api_surface,
            shardloom_admission_policy: input.shardloom_admission_policy,
            status,
            plan_ref: input.plan_ref,
            input_ref: input.input_ref,
            output_ref: input.output_ref,
            correctness_fixture_id: input.correctness_fixture_id,
            expected_outcome: input.expected_outcome,
            actual_outcome: input.actual_outcome,
            selected_segment_count: input.selected_segment_count,
            skipped_segment_count: input.skipped_segment_count,
            side_effects_performed: input.side_effects_performed,
            data_read: input.data_read,
            data_decoded: input.data_decoded,
            data_materialized: input.data_materialized,
            row_read: input.row_read,
            arrow_converted: input.arrow_converted,
            object_store_io: input.object_store_io,
            write_io: input.write_io,
            spill_io_performed: input.spill_io_performed,
            external_effects_executed: input.external_effects_executed,
            external_query_engine_invoked: input.external_query_engine_invoked,
            unsafe_effect_detected: input.unsafe_effect_detected,
            fallback_attempted: input.fallback_attempted,
            fallback_execution_allowed: input.fallback_execution_allowed,
            correctness_passed: input.correctness_passed,
            diagnostics: input.diagnostics,
        }
    }
    pub const fn is_certified(&self) -> bool {
        self.status.is_certified()
    }
    pub const fn fallback_free(&self) -> bool {
        !self.fallback_attempted && !self.fallback_execution_allowed
    }
    pub const fn external_query_engine_free(&self) -> bool {
        !self.external_query_engine_invoked
    }
    pub fn to_human_text(&self) -> String {
        format!(
            "execution certificate\nschema_version: {}\ncertificate: {}\nexecution_kind: {}\nexecution_provider_kind: {}\nstatus: {}\ncorrectness_passed: {}\nexternal query engine invoked: {}\nfallback attempted: {}\nfallback execution: disabled",
            self.schema_version,
            self.certificate_id,
            self.execution_kind,
            self.execution_provider_kind.as_str(),
            self.status.as_str(),
            self.correctness_passed,
            self.external_query_engine_invoked,
            self.fallback_attempted
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DiagnosticCategory, DiagnosticCode, FallbackStatus};

    fn certified_input() -> ExecutionCertificateInput {
        let mut input =
            ExecutionCertificateInput::new("fixture.execution-certificate", "local_encoded_count")
                .expect("input");
        input.correctness_fixture_id = Some("fixture".to_string());
        input.expected_outcome = Some(ExpectedOutcome::EncodedCount { count: 7 });
        input.actual_outcome = Some(ExpectedOutcome::EncodedCount { count: 7 });
        input.data_read = true;
        input.correctness_passed = true;
        input
    }

    #[test]
    fn matching_reference_output_certifies_without_fallback() {
        let certificate = ExecutionCertificate::evaluate(certified_input());

        assert_eq!(certificate.status, ExecutionCertificateStatus::Certified);
        assert!(certificate.is_certified());
        assert!(certificate.fallback_free());
        assert_eq!(
            certificate.execution_provider_kind,
            ExecutionProviderKind::ShardLoomKernel
        );
        assert!(certificate.external_query_engine_free());
        assert!(
            certificate
                .to_human_text()
                .contains("execution_provider_kind: shardloom_kernel")
        );
        assert!(
            certificate
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }

    #[test]
    fn mismatched_reference_output_is_incomplete() {
        let mut input = certified_input();
        input.actual_outcome = Some(ExpectedOutcome::EncodedCount { count: 8 });

        let certificate = ExecutionCertificate::evaluate(input);

        assert_eq!(
            certificate.status,
            ExecutionCertificateStatus::EvidenceIncomplete
        );
        assert!(!certificate.is_certified());
    }

    #[test]
    fn fallback_attempt_blocks_certificate() {
        let mut input = certified_input();
        input.fallback_attempted = true;

        let certificate = ExecutionCertificate::evaluate(input);

        assert_eq!(certificate.status, ExecutionCertificateStatus::Blocked);
        assert!(!certificate.fallback_free());
    }

    #[test]
    fn external_query_engine_or_baseline_provider_blocks_certificate() {
        let mut query_engine_input = certified_input();
        query_engine_input.external_query_engine_invoked = true;

        let query_engine_certificate = ExecutionCertificate::evaluate(query_engine_input);

        assert_eq!(
            query_engine_certificate.status,
            ExecutionCertificateStatus::Blocked
        );
        assert!(!query_engine_certificate.external_query_engine_free());

        let mut baseline_input = certified_input();
        baseline_input.execution_provider_kind = ExecutionProviderKind::ExternalBaseline;

        let baseline_certificate = ExecutionCertificate::evaluate(baseline_input);

        assert_eq!(
            baseline_certificate.status,
            ExecutionCertificateStatus::Blocked
        );
        assert_eq!(
            baseline_certificate.execution_provider_kind,
            ExecutionProviderKind::ExternalBaseline
        );
    }

    #[test]
    fn diagnostic_error_blocks_certificate() {
        let mut input = certified_input();
        input.diagnostics.push(Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "blocked",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        ));

        let certificate = ExecutionCertificate::evaluate(input);

        assert_eq!(certificate.status, ExecutionCertificateStatus::Blocked);
    }

    #[test]
    fn evidence_surface_foundation_is_report_only_and_machine_readable() {
        let report = ExecutionCertificateEvidenceSurfaceReport::cg16_foundation();

        assert_eq!(
            report.status,
            ExecutionCertificateEvidenceSurfaceStatus::ReportOnlyPlanned
        );
        assert_eq!(report.artifact_count(), 6);
        assert_eq!(report.required_artifact_count(), 6);
        assert_eq!(report.hash_required_count(), 6);
        assert_eq!(report.machine_readable_required_count(), 6);
        assert_eq!(
            report.artifact_order(),
            "plan,input_snapshot,output_payload,segment_trace,side_effect_manifest,reproducibility_metadata"
        );
        assert_eq!(
            report.artifact_kind_count(ExecutionEvidenceArtifactKind::Plan),
            1
        );
        assert_eq!(
            report.artifact_kind_count(ExecutionEvidenceArtifactKind::InputSnapshot),
            1
        );
        assert_eq!(
            report.artifact_kind_count(ExecutionEvidenceArtifactKind::OutputPayload),
            1
        );
        assert!(report.plan_hash_required);
        assert!(report.input_snapshot_hash_required);
        assert!(report.output_hash_required);
        assert!(report.selected_segment_trace_required);
        assert!(report.skipped_segment_trace_required);
        assert!(report.side_effect_manifest_required);
        assert!(report.reproducibility_metadata_required);
        assert!(report.correctness_fixture_required);
        assert!(report.machine_readable_certificate_surface);
        assert!(report.deterministic_field_order_required);
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
        assert!(!report.certificate_evaluation_performed);
        assert!(!report.runtime_execution);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.fallback_attempted);
        assert!(!report.production_claim_allowed);
    }
}
