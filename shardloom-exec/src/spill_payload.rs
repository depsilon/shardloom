use std::{fmt::Write, fs, path::Path};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillPayloadFsFeatureStatus {
    Disabled,
    Enabled,
}

impl SpillPayloadFsFeatureStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Enabled => "enabled",
        }
    }

    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        matches!(self, Self::Enabled)
    }
}

#[must_use]
pub const fn spill_payload_fs_feature_enabled() -> bool {
    cfg!(feature = "spill-payload-fs")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillPayloadPath(String);

impl SpillPayloadPath {
    /// Creates a validated spill payload workspace path string.
    ///
    /// # Errors
    /// Returns an error when the path is empty, whitespace-only, or contains `\0`.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "spill payload workspace path must not be empty".to_string(),
            ));
        }
        if value.contains('\0') {
            return Err(ShardLoomError::InvalidOperation(
                "spill payload workspace path must not contain NUL".to_string(),
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn summary(&self) -> String {
        format!("workspace_path={}", self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillPayloadFsRef {
    payload_ref: SpillPayloadRef,
    workspace_path: SpillPayloadPath,
}

impl SpillPayloadFsRef {
    #[must_use]
    pub fn new(payload_ref: SpillPayloadRef, workspace_path: SpillPayloadPath) -> Self {
        Self {
            payload_ref,
            workspace_path,
        }
    }

    #[must_use]
    pub fn payload_ref(&self) -> &SpillPayloadRef {
        &self.payload_ref
    }

    #[must_use]
    pub fn workspace_path(&self) -> &SpillPayloadPath {
        &self.workspace_path
    }

    #[must_use]
    pub fn file_name(&self) -> &str {
        self.payload_ref.file_name()
    }

    #[must_use]
    pub fn path_string(&self) -> String {
        Path::new(self.workspace_path.as_str())
            .join(self.file_name())
            .to_string_lossy()
            .into_owned()
    }

    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "{}, payload_id={}, file_name={}, path={}",
            self.workspace_path.summary(),
            self.payload_ref.payload_id().as_str(),
            self.file_name(),
            self.path_string()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillPayloadFsPlanStatus {
    FeatureDisabled,
    Planned,
    Unsupported,
}

impl SpillPayloadFsPlanStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::Planned => "planned",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillPayloadFsPlanMode {
    ReportOnly,
    FilesystemFeatureAvailable,
    Unsupported,
}

impl SpillPayloadFsPlanMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::FilesystemFeatureAvailable => "filesystem_feature_available",
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
pub struct SpillPayloadFsPlanReport {
    pub feature_status: SpillPayloadFsFeatureStatus,
    pub status: SpillPayloadFsPlanStatus,
    pub mode: SpillPayloadFsPlanMode,
    pub fs_ref: SpillPayloadFsRef,
    pub effects_performed: Vec<SpillPayloadEffect>,
    pub diagnostics: Vec<Diagnostic>,
}

impl SpillPayloadFsPlanReport {
    #[must_use]
    pub fn planned(fs_ref: SpillPayloadFsRef) -> Self {
        Self {
            feature_status: SpillPayloadFsFeatureStatus::Enabled,
            status: SpillPayloadFsPlanStatus::Planned,
            mode: SpillPayloadFsPlanMode::FilesystemFeatureAvailable,
            fs_ref,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn feature_disabled(fs_ref: SpillPayloadFsRef) -> Self {
        Self {
            feature_status: SpillPayloadFsFeatureStatus::Disabled,
            status: SpillPayloadFsPlanStatus::FeatureDisabled,
            mode: SpillPayloadFsPlanMode::ReportOnly,
            fs_ref,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn unsupported(
        fs_ref: SpillPayloadFsRef,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let diagnostic = Diagnostic::unsupported(
            DiagnosticCode::UnsupportedEffect,
            feature,
            format!(
                "Unsupported spill payload filesystem operation: {}",
                reason.into()
            ),
            Some("fallback_attempted=false".to_string()),
        );
        let mut report = Self {
            feature_status: if spill_payload_fs_feature_enabled() {
                SpillPayloadFsFeatureStatus::Enabled
            } else {
                SpillPayloadFsFeatureStatus::Disabled
            },
            status: SpillPayloadFsPlanStatus::Unsupported,
            mode: SpillPayloadFsPlanMode::Unsupported,
            fs_ref,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        };
        report.add_diagnostic(diagnostic);
        report
    }

    #[must_use]
    pub fn from_fs_ref(fs_ref: SpillPayloadFsRef) -> Self {
        if spill_payload_fs_feature_enabled() {
            Self::planned(fs_ref)
        } else {
            Self::feature_disabled(fs_ref)
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub fn payload_written(&self) -> bool {
        false
    }
    #[must_use]
    pub fn payload_read(&self) -> bool {
        false
    }
    #[must_use]
    pub fn cleanup_performed(&self) -> bool {
        false
    }
    #[must_use]
    pub fn object_store_io(&self) -> bool {
        false
    }
    #[must_use]
    pub fn output_dataset_write(&self) -> bool {
        false
    }
    #[must_use]
    pub fn fallback_execution_allowed(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        true
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut text = String::new();
        let _ = writeln!(text, "feature status: {}", self.feature_status.as_str());
        let _ = writeln!(text, "plan status: {}", self.status.as_str());
        let _ = writeln!(text, "mode: {}", self.mode.as_str());
        let _ = writeln!(
            text,
            "workspace path: {}",
            self.fs_ref.workspace_path().as_str()
        );
        let _ = writeln!(
            text,
            "payload id: {}",
            self.fs_ref.payload_ref().payload_id().as_str()
        );
        let _ = writeln!(text, "file name: {}", self.fs_ref.file_name());
        let _ = writeln!(text, "path string: {}", self.fs_ref.path_string());
        let _ = writeln!(text, "payload written: false");
        let _ = writeln!(text, "payload read: false");
        let _ = writeln!(text, "cleanup performed: false");
        let _ = writeln!(text, "object-store IO: false");
        let _ = writeln!(text, "output dataset write: false");
        let _ = writeln!(text, "fallback execution disabled");
        if !self.diagnostics.is_empty() {
            let _ = writeln!(text, "diagnostics:");
            for d in &self.diagnostics {
                let _ = writeln!(
                    text,
                    "- [{}] {} ({})",
                    d.code.as_str(),
                    d.message,
                    d.severity.as_str()
                );
            }
        }
        text
    }
}

#[must_use]
pub fn plan_spill_payload_filesystem_ref(fs_ref: SpillPayloadFsRef) -> SpillPayloadFsPlanReport {
    SpillPayloadFsPlanReport::from_fs_ref(fs_ref)
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
        checksum_bytes_u64(&self.bytes)
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

fn checksum_bytes_u64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0_u64, |acc, byte| {
        acc.wrapping_mul(16_777_619) ^ u64::from(*byte)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillPayloadStatus {
    Planned,
    FeatureDisabled,
    PayloadPrepared,
    BlockedByMissingWorkspace,
    BlockedByExistingPayload,
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
            Self::BlockedByExistingPayload => "blocked_by_existing_payload",
            Self::BlockedByInvalidPayload => "blocked_by_invalid_payload",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::BlockedByMissingWorkspace
                | Self::BlockedByExistingPayload
                | Self::BlockedByInvalidPayload
                | Self::Unsupported
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillPayloadWriteOption {
    AllowOverwrite,
    CreateWorkspace,
}

impl SpillPayloadWriteOption {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::AllowOverwrite => "allow_overwrite",
            Self::CreateWorkspace => "create_workspace",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillPayloadWriteRequest {
    pub fs_ref: SpillPayloadFsRef,
    pub payload: SyntheticSpillPayload,
    pub options: Vec<SpillPayloadWriteOption>,
    pub diagnostics: Vec<Diagnostic>,
}

impl SpillPayloadWriteRequest {
    #[must_use]
    pub fn new(fs_ref: SpillPayloadFsRef, payload: SyntheticSpillPayload) -> Self {
        Self {
            fs_ref,
            payload,
            options: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn allow_overwrite(mut self, value: bool) -> Self {
        Self::set_option(
            &mut self.options,
            SpillPayloadWriteOption::AllowOverwrite,
            value,
        );
        self
    }
    #[must_use]
    pub fn create_workspace(mut self, value: bool) -> Self {
        Self::set_option(
            &mut self.options,
            SpillPayloadWriteOption::CreateWorkspace,
            value,
        );
        self
    }
    fn set_option(
        options: &mut Vec<SpillPayloadWriteOption>,
        option: SpillPayloadWriteOption,
        value: bool,
    ) {
        if value && !options.contains(&option) {
            options.push(option);
        } else if !value {
            options.retain(|item| *item != option);
        }
    }
    #[must_use]
    pub fn has_option(&self, option: SpillPayloadWriteOption) -> bool {
        self.options.contains(&option)
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
        format!(
            "{}, {}, options={:?}",
            self.fs_ref.summary(),
            self.payload.summary(),
            self.options
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillPayloadWriteReport {
    pub status: SpillPayloadStatus,
    pub mode: SpillPayloadMode,
    pub request: SpillPayloadWriteRequest,
    pub bytes_written: usize,
    pub checksum: Option<u64>,
    pub effects_performed: Vec<SpillPayloadEffect>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillPayloadReadRequest {
    pub fs_ref: SpillPayloadFsRef,
    pub expected_len: Option<usize>,
    pub expected_checksum: Option<u64>,
    pub diagnostics: Vec<Diagnostic>,
}

impl SpillPayloadReadRequest {
    #[must_use]
    pub fn new(fs_ref: SpillPayloadFsRef) -> Self {
        Self {
            fs_ref,
            expected_len: None,
            expected_checksum: None,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn with_expected_len(mut self, len: usize) -> Self {
        self.expected_len = Some(len);
        self
    }
    #[must_use]
    pub fn with_expected_checksum(mut self, checksum: u64) -> Self {
        self.expected_checksum = Some(checksum);
        self
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
        format!(
            "{}, expected_len={:?}, expected_checksum={:?}",
            self.fs_ref.summary(),
            self.expected_len,
            self.expected_checksum
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillPayloadReadReport {
    pub status: SpillPayloadStatus,
    pub mode: SpillPayloadMode,
    pub request: SpillPayloadReadRequest,
    pub bytes_read: usize,
    pub checksum: Option<u64>,
    pub verification_passed: bool,
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

impl SpillPayloadWriteReport {
    #[must_use]
    pub fn feature_disabled(request: SpillPayloadWriteRequest) -> Self {
        Self {
            status: SpillPayloadStatus::FeatureDisabled,
            mode: SpillPayloadMode::ReportOnly,
            request,
            bytes_written: 0,
            checksum: None,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn planned(request: SpillPayloadWriteRequest) -> Self {
        Self {
            status: SpillPayloadStatus::Planned,
            mode: SpillPayloadMode::SyntheticPayloadPlan,
            request,
            bytes_written: 0,
            checksum: None,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn written(request: SpillPayloadWriteRequest, bytes_written: usize, checksum: u64) -> Self {
        Self {
            status: SpillPayloadStatus::PayloadPrepared,
            mode: SpillPayloadMode::SyntheticPayloadPlan,
            request,
            bytes_written,
            checksum: Some(checksum),
            effects_performed: vec![SpillPayloadEffect::PayloadWritten],
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn blocked_existing_payload(
        request: SpillPayloadWriteRequest,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self::planned(request);
        report.status = SpillPayloadStatus::BlockedByExistingPayload;
        report.add_diagnostic(Diagnostic::invalid_input(
            "spill_payload_write",
            reason.into(),
            "Use allow_overwrite(true) or choose a new payload id.",
        ));
        report
    }
    #[must_use]
    pub fn blocked_missing_workspace(
        request: SpillPayloadWriteRequest,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self::planned(request);
        report.status = SpillPayloadStatus::BlockedByMissingWorkspace;
        report.add_diagnostic(Diagnostic::invalid_input(
            "spill_payload_write",
            reason.into(),
            "Use create_workspace(true) or provide an existing workspace path.",
        ));
        report
    }
    #[must_use]
    pub fn unsupported(
        request: SpillPayloadWriteRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self::planned(request);
        report.status = SpillPayloadStatus::Unsupported;
        report.mode = SpillPayloadMode::Unsupported;
        report.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::UnsupportedEffect,
            feature,
            format!(
                "Unsupported spill payload write operation: {}",
                reason.into()
            ),
            Some("fallback_attempted=false".to_string()),
        ));
        report
    }
    /// Builds a `SpillPayloadWriteReport` and performs gated synthetic local writes.
    ///
    /// # Errors
    /// Returns an error for invalid requests or filesystem operation failures.
    pub fn from_request(request: SpillPayloadWriteRequest) -> Result<Self> {
        if request.payload.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "spill payload write request contains empty payload".to_string(),
            ));
        }
        if !spill_payload_fs_feature_enabled() {
            return Ok(Self::feature_disabled(request));
        }
        let workspace_path = request.fs_ref.workspace_path().as_str().to_string();
        let workspace = Path::new(&workspace_path);
        if !workspace.exists() {
            if request.has_option(SpillPayloadWriteOption::CreateWorkspace) {
                fs::create_dir_all(workspace).map_err(|e| {
                    ShardLoomError::InvalidOperation(format!(
                        "failed to create spill payload workspace '{}': {e}",
                        workspace.display()
                    ))
                })?;
            } else {
                return Ok(Self::blocked_missing_workspace(
                    request,
                    format!(
                        "spill payload workspace does not exist: {}",
                        workspace.display()
                    ),
                ));
            }
        }
        let target_path = Path::new(&request.fs_ref.path_string()).to_path_buf();
        if target_path.exists() && !request.has_option(SpillPayloadWriteOption::AllowOverwrite) {
            return Ok(Self::blocked_existing_payload(
                request,
                format!(
                    "spill payload target already exists: {}",
                    target_path.display()
                ),
            ));
        }
        fs::write(&target_path, &request.payload.bytes).map_err(|e| {
            ShardLoomError::InvalidOperation(format!(
                "failed to write spill payload '{}': {e}",
                target_path.display()
            ))
        })?;
        Ok(Self::written(
            request.clone(),
            request.payload.len(),
            request.payload.checksum_u64(),
        ))
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
        false
    }
    #[must_use]
    pub fn cleanup_performed(&self) -> bool {
        false
    }
    #[must_use]
    pub fn object_store_io(&self) -> bool {
        false
    }
    #[must_use]
    pub fn output_dataset_write(&self) -> bool {
        false
    }
    #[must_use]
    pub fn fallback_execution_allowed(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        !self.payload_written()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut text = String::new();
        let _ = writeln!(text, "spill payload write status: {}", self.status.as_str());
        let _ = writeln!(text, "mode: {}", self.mode.as_str());
        let _ = writeln!(
            text,
            "payload id: {}",
            self.request.fs_ref.payload_ref().payload_id().as_str()
        );
        let _ = writeln!(
            text,
            "workspace label: {}",
            self.request.fs_ref.payload_ref().workspace_label()
        );
        let _ = writeln!(
            text,
            "workspace path: {}",
            self.request.fs_ref.workspace_path().as_str()
        );
        let _ = writeln!(text, "file name: {}", self.request.fs_ref.file_name());
        let _ = writeln!(text, "path string: {}", self.request.fs_ref.path_string());
        let _ = writeln!(text, "bytes written: {}", self.bytes_written);
        if let Some(checksum) = self.checksum {
            let _ = writeln!(text, "checksum: {checksum}");
        }
        let _ = writeln!(text, "payload written: {}", self.payload_written());
        let _ = writeln!(text, "payload read: false");
        let _ = writeln!(text, "cleanup performed: false");
        let _ = writeln!(text, "object-store IO: false");
        let _ = writeln!(text, "output dataset write: false");
        let _ = writeln!(text, "fallback execution disabled");
        if !self.request.diagnostics.is_empty() || !self.diagnostics.is_empty() {
            let _ = writeln!(text, "diagnostics:");
            for d in self
                .request
                .diagnostics
                .iter()
                .chain(self.diagnostics.iter())
            {
                let _ = writeln!(
                    text,
                    "- [{}] {} ({})",
                    d.code.as_str(),
                    d.message,
                    d.severity.as_str()
                );
            }
        }
        text
    }
}

impl SpillPayloadReadReport {
    #[must_use]
    pub fn feature_disabled(request: SpillPayloadReadRequest) -> Self {
        Self {
            status: SpillPayloadStatus::FeatureDisabled,
            mode: SpillPayloadMode::ReportOnly,
            request,
            bytes_read: 0,
            checksum: None,
            verification_passed: false,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn read_back(
        request: SpillPayloadReadRequest,
        bytes_read: usize,
        checksum: u64,
        verification_passed: bool,
    ) -> Self {
        Self {
            status: SpillPayloadStatus::PayloadPrepared,
            mode: SpillPayloadMode::SyntheticPayloadPlan,
            request,
            bytes_read,
            checksum: Some(checksum),
            verification_passed,
            effects_performed: vec![SpillPayloadEffect::PayloadRead],
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn blocked_missing_payload(
        request: SpillPayloadReadRequest,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self::feature_disabled(request);
        report.status = SpillPayloadStatus::BlockedByMissingWorkspace;
        report.mode = SpillPayloadMode::SyntheticPayloadPlan;
        report.add_diagnostic(Diagnostic::invalid_input(
            "spill_payload_read",
            reason.into(),
            "Provide an existing synthetic spill payload file.",
        ));
        report
    }
    #[must_use]
    pub fn verification_failed(
        request: SpillPayloadReadRequest,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self::feature_disabled(request);
        report.status = SpillPayloadStatus::BlockedByInvalidPayload;
        report.mode = SpillPayloadMode::SyntheticPayloadPlan;
        report.add_diagnostic(Diagnostic::invalid_input(
            "spill_payload_read",
            reason.into(),
            "Update the expected length/checksum to match the payload.",
        ));
        report
    }
    #[must_use]
    pub fn unsupported(
        request: SpillPayloadReadRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self::feature_disabled(request);
        report.status = SpillPayloadStatus::Unsupported;
        report.mode = SpillPayloadMode::Unsupported;
        report.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::UnsupportedEffect,
            feature,
            format!(
                "Unsupported spill payload read operation: {}",
                reason.into()
            ),
            Some("fallback_attempted=false".to_string()),
        ));
        report
    }
    /// Builds a `SpillPayloadReadReport` and performs gated synthetic local reads.
    ///
    /// # Errors
    /// Returns an error when read operations fail.
    pub fn from_request(request: SpillPayloadReadRequest) -> Result<Self> {
        if !spill_payload_fs_feature_enabled() {
            return Ok(Self::feature_disabled(request));
        }
        let target_path = Path::new(&request.fs_ref.path_string()).to_path_buf();
        if !target_path.exists() {
            return Ok(Self::blocked_missing_payload(
                request,
                format!(
                    "spill payload target does not exist: {}",
                    target_path.display()
                ),
            ));
        }
        let bytes = fs::read(&target_path).map_err(|e| {
            ShardLoomError::InvalidOperation(format!(
                "failed to read spill payload '{}': {e}",
                target_path.display()
            ))
        })?;
        let bytes_read = bytes.len();
        let checksum = checksum_bytes_u64(&bytes);
        if let Some(expected_len) = request.expected_len
            && expected_len != bytes_read
        {
            return Ok(Self::verification_failed(
                request,
                format!("expected length {expected_len} but read {bytes_read}"),
            ));
        }
        if let Some(expected_checksum) = request.expected_checksum
            && expected_checksum != checksum
        {
            return Ok(Self::verification_failed(
                request,
                format!("expected checksum {expected_checksum} but read {checksum}"),
            ));
        }
        Ok(Self::read_back(request, bytes_read, checksum, true))
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
        false
    }
    #[must_use]
    pub fn payload_read(&self) -> bool {
        self.effects_performed
            .contains(&SpillPayloadEffect::PayloadRead)
    }
    #[must_use]
    pub fn cleanup_performed(&self) -> bool {
        false
    }
    #[must_use]
    pub fn object_store_io(&self) -> bool {
        false
    }
    #[must_use]
    pub fn output_dataset_write(&self) -> bool {
        false
    }
    #[must_use]
    pub fn fallback_execution_allowed(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        !self.payload_read()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut text = String::new();
        let _ = writeln!(text, "spill payload read status: {}", self.status.as_str());
        let _ = writeln!(text, "mode: {}", self.mode.as_str());
        let _ = writeln!(
            text,
            "payload id: {}",
            self.request.fs_ref.payload_ref().payload_id().as_str()
        );
        let _ = writeln!(
            text,
            "workspace label: {}",
            self.request.fs_ref.payload_ref().workspace_label()
        );
        let _ = writeln!(
            text,
            "workspace path: {}",
            self.request.fs_ref.workspace_path().as_str()
        );
        let _ = writeln!(text, "file name: {}", self.request.fs_ref.file_name());
        let _ = writeln!(text, "path string: {}", self.request.fs_ref.path_string());
        let _ = writeln!(text, "bytes read: {}", self.bytes_read);
        if let Some(checksum) = self.checksum {
            let _ = writeln!(text, "checksum: {checksum}");
        }
        let _ = writeln!(text, "verification passed: {}", self.verification_passed);
        let _ = writeln!(text, "payload written: false");
        let _ = writeln!(text, "payload read: {}", self.payload_read());
        let _ = writeln!(text, "cleanup performed: false");
        let _ = writeln!(text, "object-store IO: false");
        let _ = writeln!(text, "output dataset write: false");
        let _ = writeln!(text, "fallback execution disabled");
        if !self.request.diagnostics.is_empty() || !self.diagnostics.is_empty() {
            let _ = writeln!(text, "diagnostics:");
            for d in self
                .request
                .diagnostics
                .iter()
                .chain(self.diagnostics.iter())
            {
                let _ = writeln!(
                    text,
                    "- [{}] {} ({})",
                    d.code.as_str(),
                    d.message,
                    d.severity.as_str()
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

/// Writes a synthetic spill payload behind the `spill-payload-fs` feature gate.
///
/// # Errors
/// Returns an error when request invariants fail or filesystem write operations fail.
pub fn write_spill_payload(request: SpillPayloadWriteRequest) -> Result<SpillPayloadWriteReport> {
    SpillPayloadWriteReport::from_request(request)
}

/// Reads a synthetic spill payload behind the `spill-payload-fs` feature gate.
///
/// # Errors
/// Returns an error when filesystem read operations fail.
pub fn read_spill_payload(request: SpillPayloadReadRequest) -> Result<SpillPayloadReadReport> {
    SpillPayloadReadReport::from_request(request)
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
            SpillPayloadStatus::BlockedByExistingPayload,
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

    fn sample_fs_ref() -> SpillPayloadFsRef {
        let payload_id = SpillPayloadId::new("payload-fs-1").expect("valid payload id");
        let payload_ref =
            SpillPayloadRef::new(payload_id, "workspace-a").expect("valid payload ref");
        let workspace_path = SpillPayloadPath::new("relative/workspace").expect("valid path");
        SpillPayloadFsRef::new(payload_ref, workspace_path)
    }
    fn sample_write_request() -> SpillPayloadWriteRequest {
        let payload = SyntheticSpillPayload::from_text("tiny-write").expect("valid payload");
        SpillPayloadWriteRequest::new(sample_fs_ref(), payload)
    }
    fn sample_read_request() -> SpillPayloadReadRequest {
        SpillPayloadReadRequest::new(sample_fs_ref())
    }

    #[cfg(not(feature = "spill-payload-fs"))]
    #[test]
    fn spill_payload_fs_feature_disabled_by_default() {
        assert!(!spill_payload_fs_feature_enabled());
        assert!(!SpillPayloadFsFeatureStatus::Disabled.is_enabled());
    }

    #[test]
    fn spill_payload_path_rejects_empty_whitespace_and_nul() {
        assert!(SpillPayloadPath::new("").is_err());
        assert!(SpillPayloadPath::new("   ").is_err());
        assert!(SpillPayloadPath::new("a\0b").is_err());
    }

    #[test]
    fn spill_payload_path_constructor_is_pure_validation() {
        let path = SpillPayloadPath::new("does/not/need/to/exist").expect("valid path");
        assert_eq!(path.as_str(), "does/not/need/to/exist");
    }

    #[test]
    fn spill_payload_fs_ref_path_string_is_deterministic() {
        let fs_ref = sample_fs_ref();
        assert_eq!(
            fs_ref.path_string(),
            "relative/workspace/payload-fs-1.spill".to_string()
        );
    }

    #[test]
    fn spill_payload_fs_plan_mode_report_only_no_effects() {
        let mode = SpillPayloadFsPlanMode::ReportOnly;
        assert!(!mode.writes_payload());
        assert!(!mode.reads_payload());
        assert!(!mode.touches_filesystem());
    }

    #[cfg(not(feature = "spill-payload-fs"))]
    #[test]
    fn plan_spill_payload_filesystem_ref_default_is_feature_disabled() {
        let report = plan_spill_payload_filesystem_ref(sample_fs_ref());
        assert_eq!(report.status, SpillPayloadFsPlanStatus::FeatureDisabled);
        assert!(!report.payload_written());
        assert!(!report.payload_read());
        assert!(!report.cleanup_performed());
        assert!(!report.object_store_io());
        assert!(!report.output_dataset_write());
        assert!(!report.fallback_execution_allowed());
        assert!(report.is_side_effect_free());
        let text = report.to_human_text();
        assert!(text.contains("fallback execution disabled"));
        assert!(text.contains("payload written: false"));
        assert!(text.contains("payload read: false"));
    }

    #[cfg(feature = "spill-payload-fs")]
    #[test]
    fn spill_payload_fs_feature_enabled_when_feature_selected() {
        assert!(spill_payload_fs_feature_enabled());
        let report = plan_spill_payload_filesystem_ref(sample_fs_ref());
        assert_eq!(report.status, SpillPayloadFsPlanStatus::Planned);
        assert!(!report.payload_written());
        assert!(!report.payload_read());
        assert!(!report.cleanup_performed());
        assert!(!report.object_store_io());
        assert!(!report.output_dataset_write());
        assert!(!report.fallback_execution_allowed());
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn write_request_default_and_builder_options() {
        let request = sample_write_request();
        assert!(!request.has_option(SpillPayloadWriteOption::AllowOverwrite));
        assert!(!request.has_option(SpillPayloadWriteOption::CreateWorkspace));
        let request = request.allow_overwrite(true).create_workspace(true);
        assert!(request.has_option(SpillPayloadWriteOption::AllowOverwrite));
        assert!(request.has_option(SpillPayloadWriteOption::CreateWorkspace));
    }

    #[cfg(not(feature = "spill-payload-fs"))]
    #[test]
    fn write_spill_payload_disabled_report_is_side_effect_free() {
        let report = write_spill_payload(sample_write_request()).expect("report");
        assert_eq!(report.status, SpillPayloadStatus::FeatureDisabled);
        assert_eq!(report.bytes_written, 0);
        assert_eq!(report.checksum, None);
        assert!(!report.payload_written());
        assert!(!report.payload_read());
        assert!(!report.cleanup_performed());
        assert!(!report.object_store_io());
        assert!(!report.output_dataset_write());
        assert!(!report.fallback_execution_allowed());
        assert!(report.is_side_effect_free());
        let text = report.to_human_text();
        assert!(text.contains("fallback execution disabled"));
        assert!(text.contains("payload written: false"));
    }

    #[cfg(not(feature = "spill-payload-fs"))]
    #[test]
    fn read_spill_payload_disabled_report_is_side_effect_free() {
        let report = read_spill_payload(sample_read_request()).expect("report");
        assert_eq!(report.status, SpillPayloadStatus::FeatureDisabled);
        assert_eq!(report.bytes_read, 0);
        assert_eq!(report.checksum, None);
        assert!(!report.verification_passed);
        assert!(!report.payload_written());
        assert!(!report.payload_read());
        assert!(!report.cleanup_performed());
        assert!(!report.object_store_io());
        assert!(!report.output_dataset_write());
        assert!(!report.fallback_execution_allowed());
        assert!(report.is_side_effect_free());
        let text = report.to_human_text();
        assert!(text.contains("fallback execution disabled"));
        assert!(text.contains("payload read: false"));
    }

    #[test]
    fn read_request_builders_set_expectations() {
        let request = sample_read_request()
            .with_expected_len(5)
            .with_expected_checksum(33);
        assert_eq!(request.expected_len, Some(5));
        assert_eq!(request.expected_checksum, Some(33));
    }

    #[cfg(feature = "spill-payload-fs")]
    #[test]
    fn write_spill_payload_feature_flow() {
        let unique = format!("spill-payload-test-{}-{}", std::process::id(), 42);
        let workspace = std::env::temp_dir().join(unique);
        let payload_id = SpillPayloadId::new("payload-write").expect("id");
        let payload_ref = SpillPayloadRef::new(payload_id, "workspace-write").expect("ref");
        let fs_ref = SpillPayloadFsRef::new(
            payload_ref,
            SpillPayloadPath::new(workspace.to_string_lossy().into_owned()).expect("path"),
        );
        let payload = SyntheticSpillPayload::from_text("tiny-write").expect("payload");
        let first = write_spill_payload(SpillPayloadWriteRequest::new(
            fs_ref.clone(),
            payload.clone(),
        ))
        .expect("first report");
        assert_eq!(first.status, SpillPayloadStatus::BlockedByMissingWorkspace);
        let written = write_spill_payload(
            SpillPayloadWriteRequest::new(fs_ref.clone(), payload.clone()).create_workspace(true),
        )
        .expect("written report");
        assert_eq!(written.bytes_written, payload.len());
        assert_eq!(written.checksum, Some(payload.checksum_u64()));
        assert!(written.payload_written());
        let blocked = write_spill_payload(SpillPayloadWriteRequest::new(
            fs_ref.clone(),
            payload.clone(),
        ))
        .expect("blocked report");
        assert_eq!(blocked.status, SpillPayloadStatus::BlockedByExistingPayload);
        let overwritten = write_spill_payload(
            SpillPayloadWriteRequest::new(fs_ref.clone(), payload).allow_overwrite(true),
        )
        .expect("overwrite report");
        assert!(overwritten.payload_written());
        let payload_file = workspace.join(fs_ref.file_name());
        if payload_file.exists() {
            fs::remove_file(&payload_file).expect("remove payload file");
        }
        if workspace.exists() {
            fs::remove_dir(&workspace).expect("remove workspace directory");
        }
    }

    #[cfg(feature = "spill-payload-fs")]
    #[test]
    fn read_spill_payload_feature_flow_and_verification() {
        let unique = format!("spill-payload-read-{}-{}", std::process::id(), 7);
        let workspace = std::env::temp_dir().join(unique);
        let payload_id = SpillPayloadId::new("payload-read").expect("id");
        let payload_ref = SpillPayloadRef::new(payload_id, "workspace-read").expect("ref");
        let fs_ref = SpillPayloadFsRef::new(
            payload_ref,
            SpillPayloadPath::new(workspace.to_string_lossy().into_owned()).expect("path"),
        );
        let missing = read_spill_payload(SpillPayloadReadRequest::new(fs_ref.clone()))
            .expect("missing report");
        assert_eq!(
            missing.status,
            SpillPayloadStatus::BlockedByMissingWorkspace
        );
        assert!(!missing.payload_read());
        assert!(missing.is_side_effect_free());

        let payload = SyntheticSpillPayload::from_text("tiny-read").expect("payload");
        let write = write_spill_payload(
            SpillPayloadWriteRequest::new(fs_ref.clone(), payload.clone()).create_workspace(true),
        )
        .expect("write");
        assert!(write.payload_written());

        let read = read_spill_payload(
            SpillPayloadReadRequest::new(fs_ref.clone())
                .with_expected_len(payload.len())
                .with_expected_checksum(payload.checksum_u64()),
        )
        .expect("read");
        assert_eq!(read.bytes_read, payload.len());
        assert_eq!(read.checksum, Some(payload.checksum_u64()));
        assert!(read.verification_passed);
        assert!(read.payload_read());
        assert!(!read.payload_written());
        assert!(!read.cleanup_performed());
        assert!(!read.object_store_io());
        assert!(!read.output_dataset_write());
        assert!(!read.fallback_execution_allowed());

        let checksum_mismatch = read_spill_payload(
            SpillPayloadReadRequest::new(fs_ref.clone())
                .with_expected_checksum(payload.checksum_u64() + 1),
        )
        .expect("checksum mismatch");
        assert_eq!(
            checksum_mismatch.status,
            SpillPayloadStatus::BlockedByInvalidPayload
        );

        let len_mismatch = read_spill_payload(
            SpillPayloadReadRequest::new(fs_ref.clone()).with_expected_len(payload.len() + 1),
        )
        .expect("len mismatch");
        assert_eq!(
            len_mismatch.status,
            SpillPayloadStatus::BlockedByInvalidPayload
        );

        let payload_file = workspace.join(fs_ref.file_name());
        if payload_file.exists() {
            fs::remove_file(&payload_file).expect("remove payload file");
        }
        if workspace.exists() {
            fs::remove_dir(&workspace).expect("remove workspace");
        }
    }
}
