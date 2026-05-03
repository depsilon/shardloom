use std::fmt::Write;

use shardloom_core::{Diagnostic, DiagnosticCode, DiagnosticSeverity, Result, ShardLoomError};

const MAX_SYNTHETIC_SPILL_PAYLOAD_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillPayloadEffect {
    PayloadWritten,
    PayloadRead,
    CleanupPerformed,
    ObjectStoreIo,
    OutputDatasetWrite,
    FallbackExecution,
}

impl SpillPayloadEffect {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PayloadWritten => "payload_written",
            Self::PayloadRead => "payload_read",
            Self::CleanupPerformed => "cleanup_performed",
            Self::ObjectStoreIo => "object_store_io",
            Self::OutputDatasetWrite => "output_dataset_write",
            Self::FallbackExecution => "fallback_execution",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillPayloadId(String);

impl SpillPayloadId {
    /// Creates a validated spill payload identifier.
    ///
    /// # Errors
    /// Returns an error when the identifier is empty/whitespace-only or contains
    /// unsupported path-like tokens (`/`, `\\`, or `..`).
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "spill payload id must not be empty".to_string(),
            ));
        }

        if value.contains('/') || value.contains('\\') || value.contains("..") {
            return Err(ShardLoomError::InvalidOperation(
                "spill payload id must not contain '/', '\\', or '..'".to_string(),
            ));
        }

        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillPayloadRef {
    payload_id: SpillPayloadId,
    workspace_label: String,
    file_name: String,
}

impl SpillPayloadRef {
    /// Creates a side-effect-free spill payload reference.
    ///
    /// # Errors
    /// Returns an error when `workspace_label` is empty or whitespace-only.
    pub fn new(payload_id: SpillPayloadId, workspace_label: impl Into<String>) -> Result<Self> {
        let workspace_label = workspace_label.into();
        if workspace_label.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "spill payload workspace label must not be empty".to_string(),
            ));
        }

        let file_name = format!("{}.spill", payload_id.as_str());

        Ok(Self {
            payload_id,
            workspace_label,
            file_name,
        })
    }

    #[must_use]
    pub fn payload_id(&self) -> &SpillPayloadId {
        &self.payload_id
    }

    #[must_use]
    pub fn workspace_label(&self) -> &str {
        &self.workspace_label
    }

    #[must_use]
    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "payload_id={}, workspace_label={}, file_name={}",
            self.payload_id.as_str(),
            self.workspace_label,
            self.file_name
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntheticSpillPayload {
    bytes: Vec<u8>,
}

impl SyntheticSpillPayload {
    /// Creates a `SyntheticSpillPayload` from bytes.
    ///
    /// # Errors
    /// Returns an error when the payload is empty or larger than 1 MiB.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        if bytes.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "synthetic spill payload must not be empty".to_string(),
            ));
        }
        if bytes.len() > MAX_SYNTHETIC_SPILL_PAYLOAD_BYTES {
            return Err(ShardLoomError::InvalidOperation(format!(
                "synthetic spill payload exceeds 1 MiB limit: {} bytes",
                bytes.len()
            )));
        }
        Ok(Self { bytes })
    }

    /// Creates a `SyntheticSpillPayload` from text.
    ///
    /// # Errors
    /// Returns an error when text encodes to an empty payload or larger than 1 MiB.
    pub fn from_text(text: impl Into<String>) -> Result<Self> {
        Self::from_bytes(text.into().into_bytes())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    #[must_use]
    pub fn checksum_u64(&self) -> u64 {
        self.bytes.iter().fold(0_u64, |acc, byte| {
            acc.wrapping_mul(16_777_619) ^ u64::from(*byte)
        })
    }

    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "payload_len={}, checksum={}",
            self.len(),
            self.checksum_u64()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillPayloadStatus {
    Planned,
    FeatureDisabled,
    PayloadPrepared,
    BlockedByMissingWorkspace,
    BlockedByInvalidPayload,
    Unsupported,
}

impl SpillPayloadStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::FeatureDisabled => "feature_disabled",
            Self::PayloadPrepared => "payload_prepared",
            Self::BlockedByMissingWorkspace => "blocked_by_missing_workspace",
            Self::BlockedByInvalidPayload => "blocked_by_invalid_payload",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::BlockedByMissingWorkspace | Self::BlockedByInvalidPayload | Self::Unsupported
        )
    }

    #[must_use]
    pub const fn implies_payload_io(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillPayloadMode {
    ReportOnly,
    SyntheticPayloadPlan,
    Unsupported,
}

impl SpillPayloadMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::SyntheticPayloadPlan => "synthetic_payload_plan",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn writes_payload(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn reads_payload(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn touches_filesystem(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillPayloadPlanRequest {
    pub payload_ref: SpillPayloadRef,
    pub payload: SyntheticSpillPayload,
    pub diagnostics: Vec<Diagnostic>,
}

impl SpillPayloadPlanRequest {
    #[must_use]
    pub fn new(payload_ref: SpillPayloadRef, payload: SyntheticSpillPayload) -> Self {
        Self {
            payload_ref,
            payload,
            diagnostics: Vec::new(),
        }
    }

    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }

    #[must_use]
    pub fn summary(&self) -> String {
        format!("{}, {}", self.payload_ref.summary(), self.payload.summary())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillPayloadPlanReport {
    pub status: SpillPayloadStatus,
    pub mode: SpillPayloadMode,
    pub request: SpillPayloadPlanRequest,
    pub payload_len: usize,
    pub checksum: u64,
    pub effects_performed: Vec<SpillPayloadEffect>,
    pub diagnostics: Vec<Diagnostic>,
}

impl SpillPayloadPlanReport {
    #[must_use]
    pub fn planned(request: SpillPayloadPlanRequest) -> Self {
        Self {
            status: SpillPayloadStatus::Planned,
            mode: SpillPayloadMode::SyntheticPayloadPlan,
            payload_len: request.payload.len(),
            checksum: request.payload.checksum_u64(),
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn feature_disabled(request: SpillPayloadPlanRequest) -> Self {
        Self {
            status: SpillPayloadStatus::FeatureDisabled,
            mode: SpillPayloadMode::ReportOnly,
            payload_len: request.payload.len(),
            checksum: request.payload.checksum_u64(),
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn unsupported(
        request: SpillPayloadPlanRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        let diagnostic = Diagnostic::unsupported(
            DiagnosticCode::UnsupportedEffect,
            feature,
            format!("Unsupported spill payload operation: {reason}"),
            Some("Use report-only planning without external effects.".to_string()),
        );

        let mut report = Self {
            status: SpillPayloadStatus::Unsupported,
            mode: SpillPayloadMode::Unsupported,
            payload_len: request.payload.len(),
            checksum: request.payload.checksum_u64(),
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        };
        report.add_diagnostic(diagnostic);
        report
    }

    /// Builds a report-only `SpillPayloadPlanReport` from request metadata.
    ///
    /// # Errors
    /// Returns an error if report construction invariants fail.
    pub fn from_request(request: SpillPayloadPlanRequest) -> Result<Self> {
        if request.payload.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "spill payload request contains empty payload".to_string(),
            ));
        }
        Ok(Self::planned(request))
    }

    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self
                .diagnostics
                .iter()
                .chain(self.request.diagnostics.iter())
                .any(|d| {
                    matches!(
                        d.severity,
                        DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                    )
                })
    }

    #[must_use]
    pub fn payload_written(&self) -> bool {
        self.effects_performed
            .contains(&SpillPayloadEffect::PayloadWritten)
    }

    #[must_use]
    pub fn payload_read(&self) -> bool {
        self.effects_performed
            .contains(&SpillPayloadEffect::PayloadRead)
    }

    #[must_use]
    pub fn cleanup_performed(&self) -> bool {
        self.effects_performed
            .contains(&SpillPayloadEffect::CleanupPerformed)
    }

    #[must_use]
    pub fn object_store_io(&self) -> bool {
        self.effects_performed
            .contains(&SpillPayloadEffect::ObjectStoreIo)
    }

    #[must_use]
    pub fn output_dataset_write(&self) -> bool {
        self.effects_performed
            .contains(&SpillPayloadEffect::OutputDatasetWrite)
    }

    #[must_use]
    pub fn fallback_execution_allowed(&self) -> bool {
        self.effects_performed
            .contains(&SpillPayloadEffect::FallbackExecution)
    }

    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty() && !self.fallback_execution_allowed()
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut text = String::new();
        let _ = writeln!(text, "spill payload status: {}", self.status.as_str());
        let _ = writeln!(text, "mode: {}", self.mode.as_str());
        let _ = writeln!(
            text,
            "payload id: {}",
            self.request.payload_ref.payload_id().as_str()
        );
        let _ = writeln!(
            text,
            "workspace label: {}",
            self.request.payload_ref.workspace_label()
        );
        let _ = writeln!(text, "file name: {}", self.request.payload_ref.file_name());
        let _ = writeln!(text, "payload length: {}", self.payload_len);
        let _ = writeln!(text, "checksum: {}", self.checksum);
        let _ = writeln!(text, "payload written: {}", self.payload_written());
        let _ = writeln!(text, "payload read: {}", self.payload_read());
        let _ = writeln!(text, "cleanup performed: {}", self.cleanup_performed());
        let _ = writeln!(text, "object-store IO: {}", self.object_store_io());
        let _ = writeln!(
            text,
            "output dataset write: {}",
            self.output_dataset_write()
        );
        let _ = writeln!(text, "fallback execution disabled");

        if !self.request.diagnostics.is_empty() || !self.diagnostics.is_empty() {
            let _ = writeln!(text, "diagnostics:");
            for diagnostic in self
                .request
                .diagnostics
                .iter()
                .chain(self.diagnostics.iter())
            {
                let _ = writeln!(
                    text,
                    "- [{}] {} ({})",
                    diagnostic.code.as_str(),
                    diagnostic.message,
                    diagnostic.severity.as_str()
                );
            }
        }

        text
    }
}

/// Plans a report-only spill payload contract response.
///
/// # Errors
/// Returns errors forwarded from `SpillPayloadPlanReport::from_request`.
pub fn plan_spill_payload(request: SpillPayloadPlanRequest) -> Result<SpillPayloadPlanReport> {
    SpillPayloadPlanReport::from_request(request)
}

#[must_use]
pub fn spill_payload_plan_is_side_effect_free(report: &SpillPayloadPlanReport) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use shardloom_core::Diagnostic;

    use super::*;

    fn sample_request() -> SpillPayloadPlanRequest {
        let payload_id = SpillPayloadId::new("payload-1").expect("valid payload id");
        let payload_ref =
            SpillPayloadRef::new(payload_id, "workspace-a").expect("valid payload ref");
        let payload = SyntheticSpillPayload::from_text("abc123").expect("valid payload");
        SpillPayloadPlanRequest::new(payload_ref, payload)
    }

    #[test]
    fn spill_payload_id_rejects_empty() {
        assert!(SpillPayloadId::new("   ").is_err());
    }

    #[test]
    fn spill_payload_id_rejects_forward_slash() {
        assert!(SpillPayloadId::new("a/b").is_err());
    }

    #[test]
    fn spill_payload_id_rejects_backslash() {
        assert!(SpillPayloadId::new("a\\b").is_err());
    }

    #[test]
    fn spill_payload_id_rejects_double_dot() {
        assert!(SpillPayloadId::new("a..b").is_err());
    }

    #[test]
    fn spill_payload_ref_rejects_empty_workspace_label() {
        let payload_id = SpillPayloadId::new("abc").expect("valid");
        assert!(SpillPayloadRef::new(payload_id, " ").is_err());
    }

    #[test]
    fn spill_payload_ref_derives_deterministic_file_name() {
        let payload_id = SpillPayloadId::new("abc").expect("valid");
        let payload_ref = SpillPayloadRef::new(payload_id, "ws").expect("valid");
        assert_eq!(payload_ref.file_name(), "abc.spill");
    }

    #[test]
    fn synthetic_spill_payload_rejects_empty_bytes() {
        assert!(SyntheticSpillPayload::from_bytes(Vec::new()).is_err());
    }

    #[test]
    fn synthetic_spill_payload_rejects_oversized_payload() {
        let bytes = vec![0_u8; MAX_SYNTHETIC_SPILL_PAYLOAD_BYTES + 1];
        assert!(SyntheticSpillPayload::from_bytes(bytes).is_err());
    }

    #[test]
    fn synthetic_spill_payload_from_text_works() {
        let payload = SyntheticSpillPayload::from_text("hello").expect("valid");
        assert_eq!(payload.len(), 5);
    }

    #[test]
    fn checksum_is_deterministic() {
        let a = SyntheticSpillPayload::from_text("same").expect("valid");
        let b = SyntheticSpillPayload::from_text("same").expect("valid");
        assert_eq!(a.checksum_u64(), b.checksum_u64());
    }

    #[test]
    fn spill_payload_status_unsupported_is_error() {
        assert!(SpillPayloadStatus::Unsupported.is_error());
    }

    #[test]
    fn spill_payload_status_variants_do_not_imply_payload_io() {
        let variants = [
            SpillPayloadStatus::Planned,
            SpillPayloadStatus::FeatureDisabled,
            SpillPayloadStatus::PayloadPrepared,
            SpillPayloadStatus::BlockedByMissingWorkspace,
            SpillPayloadStatus::BlockedByInvalidPayload,
            SpillPayloadStatus::Unsupported,
        ];
        assert!(variants.iter().all(|status| !status.implies_payload_io()));
    }

    #[test]
    fn spill_payload_mode_report_only_has_no_io() {
        let mode = SpillPayloadMode::ReportOnly;
        assert!(!mode.writes_payload());
        assert!(!mode.reads_payload());
        assert!(!mode.touches_filesystem());
    }

    #[test]
    fn spill_payload_plan_request_new_performs_no_io() {
        let request = sample_request();
        assert!(!request.has_errors());
    }

    #[test]
    fn spill_payload_plan_report_planned_side_effects_are_false() {
        let report = SpillPayloadPlanReport::planned(sample_request());
        assert!(!report.payload_written());
        assert!(!report.payload_read());
        assert!(!report.cleanup_performed());
        assert!(!report.object_store_io());
        assert!(!report.output_dataset_write());
        assert!(!report.fallback_execution_allowed());
    }

    #[test]
    fn unsupported_has_errors_and_fallback_false() {
        let report =
            SpillPayloadPlanReport::unsupported(sample_request(), "spill_payload", "not supported");
        assert!(report.has_errors());
        assert!(!report.fallback_execution_allowed());
    }

    #[test]
    fn plan_spill_payload_returns_planned_report() {
        let report = plan_spill_payload(sample_request()).expect("planned report");
        assert_eq!(report.status, SpillPayloadStatus::Planned);
    }

    #[test]
    fn report_is_side_effect_free() {
        let report = SpillPayloadPlanReport::planned(sample_request());
        assert!(spill_payload_plan_is_side_effect_free(&report));
    }

    #[test]
    fn to_human_text_includes_disabled_and_side_effect_flags() {
        let report = SpillPayloadPlanReport::planned(sample_request());
        let text = report.to_human_text();
        assert!(text.contains("fallback execution disabled"));
        assert!(text.contains("payload written: false"));
        assert!(text.contains("payload read: false"));
    }

    #[test]
    fn to_human_text_renders_diagnostics_details() {
        let mut request = sample_request();
        request.add_diagnostic(Diagnostic::no_fallback_execution("still disabled"));
        let report = SpillPayloadPlanReport::planned(request);
        let text = report.to_human_text();
        assert!(text.contains("diagnostics:"));
        assert!(text.contains("SL_NO_FALLBACK_EXECUTION"));
        assert!(text.contains("still disabled"));
    }
}
