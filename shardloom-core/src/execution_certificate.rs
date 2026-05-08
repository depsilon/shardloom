//! Evidence-first execution certificate contracts.
//!
//! Certificates record what a supported execution path did, what it avoided,
//! and which correctness reference output it matched. They are evidence objects,
//! not permission to use external fallback engines.

#![allow(clippy::must_use_candidate, clippy::return_self_not_must_use)]

use crate::{Diagnostic, DiagnosticSeverity, ExpectedOutcome, Result, ShardLoomError};

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

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ExecutionCertificateInput {
    pub certificate_id: String,
    pub execution_kind: String,
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
        let status = if input.fallback_attempted
            || input.fallback_execution_allowed
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
    pub fn to_human_text(&self) -> String {
        format!(
            "execution certificate\nschema_version: {}\ncertificate: {}\nexecution_kind: {}\nstatus: {}\ncorrectness_passed: {}\nfallback attempted: {}\nfallback execution: disabled",
            self.schema_version,
            self.certificate_id,
            self.execution_kind,
            self.status.as_str(),
            self.correctness_passed,
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
}
