use std::fmt::Write as _;
#[cfg(feature = "vortex-staged-output-fs")]
use std::{
    collections::hash_map::DefaultHasher,
    fs,
    hash::{Hash, Hasher},
    path::Path,
};

use shardloom_core::{
    DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, Result, ShardLoomError,
};

use crate::{
    VortexFinalizedManifestArtifactWriteReport, VortexStagedOutputReport,
    VortexStagedWorkspacePath, VortexWriteIntentReport,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexOutputPayloadStatus {
    Planned,
    PayloadReady,
    BlockedByWriteIntent,
    BlockedByStagedOutput,
    BlockedByFinalizedManifest,
    BlockedByMissingPayloadContent,
    BlockedByObjectStoreTarget,
    BlockedByUpstreamVortexWrite,
    BlockedByFeatureGate,
    Unsupported,
}
impl VortexOutputPayloadStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::PayloadReady => "payload_ready",
            Self::BlockedByWriteIntent => "blocked_by_write_intent",
            Self::BlockedByStagedOutput => "blocked_by_staged_output",
            Self::BlockedByFinalizedManifest => "blocked_by_finalized_manifest",
            Self::BlockedByMissingPayloadContent => "blocked_by_missing_payload_content",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByUpstreamVortexWrite => "blocked_by_upstream_vortex_write",
            Self::BlockedByFeatureGate => "blocked_by_feature_gate",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::Planned | Self::PayloadReady)
    }
    #[must_use]
    pub const fn allows_payload_write(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexOutputPayloadMode {
    ReportOnly,
    PayloadPlanning,
    Unsupported,
}
impl VortexOutputPayloadMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::PayloadPlanning => "payload_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn writes_output_payload(self) -> bool {
        false
    }
    #[must_use]
    pub const fn writes_vortex_file(self) -> bool {
        false
    }
    #[must_use]
    pub const fn writes_manifest(self) -> bool {
        false
    }
    #[must_use]
    pub const fn commits_manifest(self) -> bool {
        false
    }
    #[must_use]
    pub const fn writes_object_store(self) -> bool {
        false
    }
    #[must_use]
    pub const fn calls_upstream_vortex_write(self) -> bool {
        false
    }
    #[must_use]
    pub const fn executes_recovery_action(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexOutputPayloadSignal {
    WriteIntentReady,
    WriteIntentBlocked,
    StagedOutputReady,
    StagedOutputBlocked,
    FinalizedManifestReady,
    FinalizedManifestMissing,
    PayloadContentAvailable,
    PayloadContentMissing,
    LocalWorkspace,
    ObjectStoreTarget,
    UpstreamVortexWriteRequired,
    FeatureGateEnabled,
}
impl VortexOutputPayloadSignal {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WriteIntentReady => "write_intent_ready",
            Self::WriteIntentBlocked => "write_intent_blocked",
            Self::StagedOutputReady => "staged_output_ready",
            Self::StagedOutputBlocked => "staged_output_blocked",
            Self::FinalizedManifestReady => "finalized_manifest_ready",
            Self::FinalizedManifestMissing => "finalized_manifest_missing",
            Self::PayloadContentAvailable => "payload_content_available",
            Self::PayloadContentMissing => "payload_content_missing",
            Self::LocalWorkspace => "local_workspace",
            Self::ObjectStoreTarget => "object_store_target",
            Self::UpstreamVortexWriteRequired => "upstream_vortex_write_required",
            Self::FeatureGateEnabled => "feature_gate_enabled",
        }
    }
    #[must_use]
    pub const fn is_blocking(self) -> bool {
        matches!(
            self,
            Self::WriteIntentBlocked
                | Self::StagedOutputBlocked
                | Self::FinalizedManifestMissing
                | Self::PayloadContentMissing
                | Self::ObjectStoreTarget
                | Self::UpstreamVortexWriteRequired
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexOutputPayloadEffect {
    OutputPayloadWritten,
    VortexFileWritten,
    ManifestWritten,
    ManifestCommitted,
    ObjectStoreIo,
    UpstreamVortexWriteCalled,
    RecoveryActionExecuted,
    FallbackExecution,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexOutputPayloadArtifactWriteStatus {
    FeatureDisabled,
    Planned,
    OutputPayloadArtifactWritten,
    BlockedByPayloadPlan,
    BlockedByObjectStoreTarget,
    BlockedByMissingWorkspace,
    BlockedByExistingOutputPayload,
    BlockedByExistingNonDirectory,
    BlockedByUpstreamVortexWrite,
    BlockedByFeatureGate,
    Unsupported,
}
impl VortexOutputPayloadArtifactWriteStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::Planned => "planned",
            Self::OutputPayloadArtifactWritten => "output_payload_artifact_written",
            Self::BlockedByPayloadPlan => "blocked_by_payload_plan",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByMissingWorkspace => "blocked_by_missing_workspace",
            Self::BlockedByExistingOutputPayload => "blocked_by_existing_output_payload",
            Self::BlockedByExistingNonDirectory => "blocked_by_existing_non_directory",
            Self::BlockedByUpstreamVortexWrite => "blocked_by_upstream_vortex_write",
            Self::BlockedByFeatureGate => "blocked_by_feature_gate",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(
            self,
            Self::FeatureDisabled | Self::Planned | Self::OutputPayloadArtifactWritten
        )
    }
    #[must_use]
    pub const fn output_payload_artifact_written(self) -> bool {
        matches!(self, Self::OutputPayloadArtifactWritten)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexOutputPayloadArtifactWriteMode {
    ReportOnly,
    LocalOutputPayloadArtifactWrite,
    Unsupported,
}
impl VortexOutputPayloadArtifactWriteMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::LocalOutputPayloadArtifactWrite => "local_output_payload_artifact_write",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn writes_output_payload(self) -> bool {
        matches!(self, Self::LocalOutputPayloadArtifactWrite)
    }
    #[must_use]
    pub const fn writes_vortex_file(self) -> bool {
        false
    }
    #[must_use]
    pub const fn writes_manifest(self) -> bool {
        false
    }
    #[must_use]
    pub const fn commits_manifest(self) -> bool {
        false
    }
    #[must_use]
    pub const fn writes_object_store(self) -> bool {
        false
    }
    #[must_use]
    pub const fn calls_upstream_vortex_write(self) -> bool {
        false
    }
    #[must_use]
    pub const fn executes_recovery_action(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexOutputPayloadArtifactWriteOption {
    AllowOverwrite,
}
impl VortexOutputPayloadArtifactWriteOption {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        "allow_overwrite"
    }
}
impl VortexOutputPayloadEffect {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OutputPayloadWritten => "output_payload_written",
            Self::VortexFileWritten => "vortex_file_written",
            Self::ManifestWritten => "manifest_written",
            Self::ManifestCommitted => "manifest_committed",
            Self::ObjectStoreIo => "object_store_io",
            Self::UpstreamVortexWriteCalled => "upstream_vortex_write_called",
            Self::RecoveryActionExecuted => "recovery_action_executed",
            Self::FallbackExecution => "fallback_execution",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexOutputPayloadFileName(String);
impl VortexOutputPayloadFileName {
    /// # Errors
    /// Returns an error when the payload file name is empty or unsafe.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let t = value.trim();
        if t.is_empty() || t.contains('/') || t.contains('\\') || t.contains("..") {
            return Err(ShardLoomError::InvalidOperation(
                "invalid output payload file name".to_string(),
            ));
        }
        Ok(Self(t.to_string()))
    }
    #[must_use]
    pub fn default_payload() -> Self {
        Self("_shardloom_output_payload.vortex".to_string())
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!("output_payload_file_name={}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexOutputPayloadFileRef {
    pub workspace_path: VortexStagedWorkspacePath,
    pub file_name: VortexOutputPayloadFileName,
}
impl VortexOutputPayloadFileRef {
    #[must_use]
    pub fn new(
        workspace_path: VortexStagedWorkspacePath,
        file_name: VortexOutputPayloadFileName,
    ) -> Self {
        Self {
            workspace_path,
            file_name,
        }
    }
    #[must_use]
    pub fn default_for_workspace(workspace_path: VortexStagedWorkspacePath) -> Self {
        Self::new(
            workspace_path,
            VortexOutputPayloadFileName::default_payload(),
        )
    }
    #[must_use]
    pub fn workspace_path(&self) -> &VortexStagedWorkspacePath {
        &self.workspace_path
    }
    #[must_use]
    pub fn file_name(&self) -> &VortexOutputPayloadFileName {
        &self.file_name
    }
    #[must_use]
    pub fn path_string(&self) -> String {
        format!(
            "{}/{}",
            self.workspace_path.as_str().trim_end_matches('/'),
            self.file_name.as_str()
        )
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!("output_payload_path={}", self.path_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexOutputPayloadContentKind {
    SyntheticPlaceholder,
    NativeVortexPayload,
    EncodedBatchPayload,
    Unknown,
}
impl VortexOutputPayloadContentKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SyntheticPlaceholder => "synthetic_placeholder",
            Self::NativeVortexPayload => "native_vortex_payload",
            Self::EncodedBatchPayload => "encoded_batch_payload",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexOutputPayloadContentDescriptor {
    pub content_kind: VortexOutputPayloadContentKind,
    pub logical_rows: Option<u64>,
    pub estimated_bytes: Option<u64>,
    pub checksum: Option<u64>,
    pub summary: String,
}
impl VortexOutputPayloadContentDescriptor {
    fn new(
        content_kind: VortexOutputPayloadContentKind,
        summary: impl Into<String>,
    ) -> Result<Self> {
        let summary = summary.into();
        if summary.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "invalid output payload content summary".to_string(),
            ));
        }
        Ok(Self {
            content_kind,
            logical_rows: None,
            estimated_bytes: None,
            checksum: None,
            summary,
        })
    }
    /// # Errors
    /// Returns an error when the summary is empty or whitespace.
    pub fn synthetic_placeholder(summary: impl Into<String>) -> Result<Self> {
        Self::new(
            VortexOutputPayloadContentKind::SyntheticPlaceholder,
            summary,
        )
    }
    /// # Errors
    /// Returns an error when the summary is empty or whitespace.
    pub fn native_vortex_placeholder(summary: impl Into<String>) -> Result<Self> {
        Self::new(VortexOutputPayloadContentKind::NativeVortexPayload, summary)
    }
    /// # Errors
    /// Returns an error when the summary is empty or whitespace.
    pub fn unknown(summary: impl Into<String>) -> Result<Self> {
        Self::new(VortexOutputPayloadContentKind::Unknown, summary)
    }
    #[must_use]
    pub const fn content_kind(&self) -> VortexOutputPayloadContentKind {
        self.content_kind
    }
    #[must_use]
    pub fn has_payload_content(&self) -> bool {
        matches!(
            self.content_kind,
            VortexOutputPayloadContentKind::NativeVortexPayload
                | VortexOutputPayloadContentKind::EncodedBatchPayload
        )
    }
    #[must_use]
    pub const fn estimated_bytes(&self) -> Option<u64> {
        self.estimated_bytes
    }
    #[must_use]
    pub const fn checksum(&self) -> Option<u64> {
        self.checksum
    }
    #[must_use]
    pub fn summary(&self) -> &str {
        &self.summary
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexOutputPayloadRequest {
    pub target_uri: DatasetUri,
    pub payload_ref: VortexOutputPayloadFileRef,
    pub payload_content: VortexOutputPayloadContentDescriptor,
    pub signals: Vec<VortexOutputPayloadSignal>,
    pub write_intent_summary: Option<String>,
    pub staged_output_summary: Option<String>,
    pub finalized_manifest_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexOutputPayloadRequest {
    #[must_use]
    pub fn new(
        target_uri: DatasetUri,
        payload_ref: VortexOutputPayloadFileRef,
        payload_content: VortexOutputPayloadContentDescriptor,
    ) -> Self {
        Self {
            target_uri,
            payload_ref,
            payload_content,
            signals: Vec::new(),
            write_intent_summary: None,
            staged_output_summary: None,
            finalized_manifest_summary: None,
            diagnostics: Vec::new(),
        }
    }
    pub fn add_signal(&mut self, signal: VortexOutputPayloadSignal, value: bool) {
        if value {
            if !self.signals.contains(&signal) {
                self.signals.push(signal);
            }
        } else {
            self.signals.retain(|s| *s != signal);
        }
    }
    fn with(mut self, s: VortexOutputPayloadSignal, v: bool) -> Self {
        self.add_signal(s, v);
        self
    }
    #[must_use]
    pub fn write_intent_ready(self, v: bool) -> Self {
        self.with(VortexOutputPayloadSignal::WriteIntentReady, v)
    }
    #[must_use]
    pub fn write_intent_blocked(self, v: bool) -> Self {
        self.with(VortexOutputPayloadSignal::WriteIntentBlocked, v)
    }
    #[must_use]
    pub fn staged_output_ready(self, v: bool) -> Self {
        self.with(VortexOutputPayloadSignal::StagedOutputReady, v)
    }
    #[must_use]
    pub fn staged_output_blocked(self, v: bool) -> Self {
        self.with(VortexOutputPayloadSignal::StagedOutputBlocked, v)
    }
    #[must_use]
    pub fn finalized_manifest_ready(self, v: bool) -> Self {
        self.with(VortexOutputPayloadSignal::FinalizedManifestReady, v)
    }
    #[must_use]
    pub fn finalized_manifest_missing(self, v: bool) -> Self {
        self.with(VortexOutputPayloadSignal::FinalizedManifestMissing, v)
    }
    #[must_use]
    pub fn payload_content_available(self, v: bool) -> Self {
        self.with(VortexOutputPayloadSignal::PayloadContentAvailable, v)
    }
    #[must_use]
    pub fn payload_content_missing(self, v: bool) -> Self {
        self.with(VortexOutputPayloadSignal::PayloadContentMissing, v)
    }
    #[must_use]
    pub fn local_workspace(self, v: bool) -> Self {
        self.with(VortexOutputPayloadSignal::LocalWorkspace, v)
    }
    #[must_use]
    pub fn object_store_target(self, v: bool) -> Self {
        self.with(VortexOutputPayloadSignal::ObjectStoreTarget, v)
    }
    #[must_use]
    pub fn upstream_vortex_write_required(self, v: bool) -> Self {
        self.with(VortexOutputPayloadSignal::UpstreamVortexWriteRequired, v)
    }
    #[must_use]
    pub fn feature_gate_enabled(self, v: bool) -> Self {
        self.with(VortexOutputPayloadSignal::FeatureGateEnabled, v)
    }
    #[must_use]
    pub fn with_write_intent_summary(mut self, v: impl Into<String>) -> Self {
        self.write_intent_summary = Some(v.into());
        self
    }
    #[must_use]
    pub fn with_staged_output_summary(mut self, v: impl Into<String>) -> Self {
        self.staged_output_summary = Some(v.into());
        self
    }
    #[must_use]
    pub fn with_finalized_manifest_summary(mut self, v: impl Into<String>) -> Self {
        self.finalized_manifest_summary = Some(v.into());
        self
    }
    #[must_use]
    pub fn has_signal(&self, s: VortexOutputPayloadSignal) -> bool {
        self.signals.contains(&s)
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
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
            "target_uri={} signals={}",
            self.target_uri.as_str(),
            self.signals.len()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexOutputPayloadReport {
    pub status: VortexOutputPayloadStatus,
    pub mode: VortexOutputPayloadMode,
    pub request: VortexOutputPayloadRequest,
    pub effects_performed: Vec<VortexOutputPayloadEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexOutputPayloadReport {
    /// # Errors
    /// Returns an error only if human text rendering fails unexpectedly.
    pub fn from_request(request: VortexOutputPayloadRequest) -> Result<Self> {
        let mut report = Self {
            status: VortexOutputPayloadStatus::Planned,
            mode: VortexOutputPayloadMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        };
        report.status = if report.object_store_target() {
            VortexOutputPayloadStatus::BlockedByObjectStoreTarget
        } else if report.upstream_vortex_write_required() {
            VortexOutputPayloadStatus::BlockedByUpstreamVortexWrite
        } else if report
            .request
            .has_signal(VortexOutputPayloadSignal::WriteIntentBlocked)
            || !report.write_intent_ready()
        {
            VortexOutputPayloadStatus::BlockedByWriteIntent
        } else if report
            .request
            .has_signal(VortexOutputPayloadSignal::StagedOutputBlocked)
            || !report.staged_output_ready()
        {
            VortexOutputPayloadStatus::BlockedByStagedOutput
        } else if report
            .request
            .has_signal(VortexOutputPayloadSignal::FinalizedManifestMissing)
            || !report.finalized_manifest_ready()
        {
            VortexOutputPayloadStatus::BlockedByFinalizedManifest
        } else if report
            .request
            .has_signal(VortexOutputPayloadSignal::PayloadContentMissing)
            || !report.payload_content_available()
        {
            VortexOutputPayloadStatus::BlockedByMissingPayloadContent
        } else if !report
            .request
            .has_signal(VortexOutputPayloadSignal::FeatureGateEnabled)
        {
            VortexOutputPayloadStatus::BlockedByFeatureGate
        } else {
            VortexOutputPayloadStatus::PayloadReady
        };
        if report.status == VortexOutputPayloadStatus::Unsupported {
            report.mode = VortexOutputPayloadMode::Unsupported;
        }
        let _ = report.to_human_text()?;
        Ok(report)
    }
    #[must_use]
    pub fn unsupported(
        request: VortexOutputPayloadRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self {
            status: VortexOutputPayloadStatus::Unsupported,
            mode: VortexOutputPayloadMode::Unsupported,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        };
        report.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            None,
        ));
        report
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.request.has_errors()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub fn write_intent_ready(&self) -> bool {
        self.request
            .has_signal(VortexOutputPayloadSignal::WriteIntentReady)
    }
    #[must_use]
    pub fn staged_output_ready(&self) -> bool {
        self.request
            .has_signal(VortexOutputPayloadSignal::StagedOutputReady)
    }
    #[must_use]
    pub fn finalized_manifest_ready(&self) -> bool {
        self.request
            .has_signal(VortexOutputPayloadSignal::FinalizedManifestReady)
    }
    #[must_use]
    pub fn payload_content_available(&self) -> bool {
        self.request
            .has_signal(VortexOutputPayloadSignal::PayloadContentAvailable)
    }
    #[must_use]
    pub fn local_workspace(&self) -> bool {
        self.request
            .has_signal(VortexOutputPayloadSignal::LocalWorkspace)
    }
    #[must_use]
    pub fn object_store_target(&self) -> bool {
        self.request
            .has_signal(VortexOutputPayloadSignal::ObjectStoreTarget)
    }
    #[must_use]
    pub fn upstream_vortex_write_required(&self) -> bool {
        self.request
            .has_signal(VortexOutputPayloadSignal::UpstreamVortexWriteRequired)
    }
    #[must_use]
    pub fn output_payload_written(&self) -> bool {
        self.effects_performed
            .contains(&VortexOutputPayloadEffect::OutputPayloadWritten)
    }
    #[must_use]
    pub fn vortex_file_written(&self) -> bool {
        self.effects_performed
            .contains(&VortexOutputPayloadEffect::VortexFileWritten)
    }
    #[must_use]
    pub fn manifest_written(&self) -> bool {
        self.effects_performed
            .contains(&VortexOutputPayloadEffect::ManifestWritten)
    }
    #[must_use]
    pub fn manifest_committed(&self) -> bool {
        self.effects_performed
            .contains(&VortexOutputPayloadEffect::ManifestCommitted)
    }
    #[must_use]
    pub fn object_store_io(&self) -> bool {
        self.effects_performed
            .contains(&VortexOutputPayloadEffect::ObjectStoreIo)
    }
    #[must_use]
    pub fn upstream_vortex_write_called(&self) -> bool {
        self.effects_performed
            .contains(&VortexOutputPayloadEffect::UpstreamVortexWriteCalled)
    }
    #[must_use]
    pub fn recovery_action_executed(&self) -> bool {
        self.effects_performed
            .contains(&VortexOutputPayloadEffect::RecoveryActionExecuted)
    }
    #[must_use]
    pub fn fallback_execution_allowed(&self) -> bool {
        self.effects_performed
            .contains(&VortexOutputPayloadEffect::FallbackExecution)
    }
    #[must_use]
    pub const fn allows_payload_write(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty() && !self.fallback_execution_allowed()
    }
    /// # Errors
    /// Returns an error only if formatting into the output string fails unexpectedly.
    pub fn to_human_text(&self) -> Result<String> {
        let mut out = String::new();
        for line in self.human_lines() {
            writeln!(&mut out, "{line}")
                .map_err(|e| ShardLoomError::InvalidOperation(e.to_string()))?;
        }
        if !self.request.diagnostics.is_empty() || !self.diagnostics.is_empty() {
            writeln!(&mut out, "diagnostics:")
                .map_err(|e| ShardLoomError::InvalidOperation(e.to_string()))?;
            for d in self
                .request
                .diagnostics
                .iter()
                .chain(self.diagnostics.iter())
            {
                writeln!(&mut out, "- {:?} {}", d.severity, d.message)
                    .map_err(|e| ShardLoomError::InvalidOperation(e.to_string()))?;
            }
        }
        Ok(out)
    }

    fn human_lines(&self) -> Vec<String> {
        vec![
            format!("output payload status: {}", self.status.as_str()),
            format!("mode: {}", self.mode.as_str()),
            format!("target URI: {}", self.request.target_uri.as_str()),
            format!("payload path: {}", self.request.payload_ref.path_string()),
            format!(
                "content kind: {}",
                self.request.payload_content.content_kind().as_str()
            ),
            format!(
                "estimated bytes: {:?}",
                self.request.payload_content.estimated_bytes()
            ),
            format!("write intent ready: {}", self.write_intent_ready()),
            format!("staged output ready: {}", self.staged_output_ready()),
            format!(
                "finalized manifest ready: {}",
                self.finalized_manifest_ready()
            ),
            format!(
                "payload content available: {}",
                self.payload_content_available()
            ),
            format!("local workspace: {}", self.local_workspace()),
            format!("object-store target: {}", self.object_store_target()),
            format!(
                "upstream Vortex write required: {}",
                self.upstream_vortex_write_required()
            ),
            format!("output payload written: {}", self.output_payload_written()),
            format!("Vortex file written: {}", self.vortex_file_written()),
            format!("manifest written: {}", self.manifest_written()),
            format!("manifest committed: {}", self.manifest_committed()),
            format!("object-store IO: {}", self.object_store_io()),
            format!(
                "upstream Vortex write called: {}",
                self.upstream_vortex_write_called()
            ),
            format!(
                "recovery action executed: {}",
                self.recovery_action_executed()
            ),
            "fallback execution disabled".to_string(),
        ]
    }
}

/// # Errors
/// Returns errors propagated from `VortexOutputPayloadReport::from_request`.
pub fn plan_vortex_output_payload(
    request: VortexOutputPayloadRequest,
) -> Result<VortexOutputPayloadReport> {
    VortexOutputPayloadReport::from_request(request)
}
#[must_use]
pub fn vortex_output_payload_is_side_effect_free(report: &VortexOutputPayloadReport) -> bool {
    report.is_side_effect_free()
}

/// # Errors
/// Returns errors when default payload names or placeholder descriptors fail validation.
pub fn output_payload_request_from_reports(
    target_uri: DatasetUri,
    workspace_path: VortexStagedWorkspacePath,
    write_intent: &VortexWriteIntentReport,
    staged_output: &VortexStagedOutputReport,
    finalized_manifest: &VortexFinalizedManifestArtifactWriteReport,
) -> Result<VortexOutputPayloadRequest> {
    let payload_ref = VortexOutputPayloadFileRef::default_for_workspace(workspace_path);
    let payload_content = VortexOutputPayloadContentDescriptor::synthetic_placeholder(
        "payload readiness placeholder",
    )?;
    let mut req = VortexOutputPayloadRequest::new(target_uri, payload_ref, payload_content);
    req.add_signal(
        VortexOutputPayloadSignal::WriteIntentReady,
        !write_intent.status.is_error() && !write_intent.has_errors(),
    );
    req.add_signal(
        VortexOutputPayloadSignal::WriteIntentBlocked,
        write_intent.status.is_error() || write_intent.has_errors(),
    );
    req.add_signal(
        VortexOutputPayloadSignal::StagedOutputReady,
        !staged_output.status.is_error() && !staged_output.has_errors(),
    );
    req.add_signal(
        VortexOutputPayloadSignal::StagedOutputBlocked,
        staged_output.status.is_error() || staged_output.has_errors(),
    );
    req.add_signal(
        VortexOutputPayloadSignal::FinalizedManifestReady,
        finalized_manifest.finalized_manifest_written() && !finalized_manifest.has_errors(),
    );
    req.add_signal(
        VortexOutputPayloadSignal::FinalizedManifestMissing,
        !finalized_manifest.finalized_manifest_written() || finalized_manifest.has_errors(),
    );
    req.add_signal(
        VortexOutputPayloadSignal::PayloadContentAvailable,
        req.payload_content.has_payload_content(),
    );
    req.add_signal(
        VortexOutputPayloadSignal::PayloadContentMissing,
        !req.payload_content.has_payload_content(),
    );
    req.add_signal(
        VortexOutputPayloadSignal::ObjectStoreTarget,
        write_intent.object_store_target()
            || write_intent.object_store_io()
            || staged_output.object_store_workspace()
            || finalized_manifest.object_store_io(),
    );
    req.add_signal(
        VortexOutputPayloadSignal::UpstreamVortexWriteRequired,
        write_intent.upstream_vortex_write_called()
            || finalized_manifest.upstream_vortex_write_called(),
    );
    req.write_intent_summary = Some(write_intent.request.summary());
    req.staged_output_summary = Some(staged_output.request.summary());
    req.finalized_manifest_summary = Some(finalized_manifest.request.summary());
    Ok(req)
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexOutputPayloadArtifactWriteRequest {
    pub payload_ref: VortexOutputPayloadFileRef,
    pub payload_content: VortexOutputPayloadContentDescriptor,
    pub options: Vec<VortexOutputPayloadArtifactWriteOption>,
    pub payload_plan_summary: Option<String>,
    pub payload_plan_feature_gate_ready: bool,
    pub upstream_vortex_write_required: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexOutputPayloadArtifactWriteRequest {
    #[must_use]
    pub fn new(
        payload_ref: VortexOutputPayloadFileRef,
        payload_content: VortexOutputPayloadContentDescriptor,
    ) -> Self {
        Self {
            payload_ref,
            payload_content,
            options: Vec::new(),
            payload_plan_summary: None,
            payload_plan_feature_gate_ready: false,
            upstream_vortex_write_required: false,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn from_payload_request(request: &VortexOutputPayloadRequest) -> Self {
        Self::new(request.payload_ref.clone(), request.payload_content.clone())
            .with_payload_plan_summary(request.summary())
            .feature_gate_ready(request.has_signal(VortexOutputPayloadSignal::FeatureGateEnabled))
            .upstream_vortex_write_required(
                request.has_signal(VortexOutputPayloadSignal::UpstreamVortexWriteRequired),
            )
    }
    #[must_use]
    pub fn allow_overwrite(mut self, value: bool) -> Self {
        self.set_option(
            VortexOutputPayloadArtifactWriteOption::AllowOverwrite,
            value,
        );
        self
    }
    #[must_use]
    pub fn with_payload_plan_summary(mut self, value: impl Into<String>) -> Self {
        self.payload_plan_summary = Some(value.into());
        self
    }
    #[must_use]
    pub fn feature_gate_ready(mut self, value: bool) -> Self {
        self.payload_plan_feature_gate_ready = value;
        self
    }
    #[must_use]
    pub fn upstream_vortex_write_required(mut self, value: bool) -> Self {
        self.upstream_vortex_write_required = value;
        self
    }
    fn set_option(&mut self, option: VortexOutputPayloadArtifactWriteOption, value: bool) {
        if value {
            if !self.options.contains(&option) {
                self.options.push(option);
            }
        } else {
            self.options.retain(|v| *v != option);
        }
    }
    #[must_use]
    pub fn has_option(&self, option: VortexOutputPayloadArtifactWriteOption) -> bool {
        self.options.contains(&option)
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
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
            "payload_path={} feature_gate_ready={}",
            self.payload_ref.path_string(),
            self.payload_plan_feature_gate_ready
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexOutputPayloadArtifactWriteReport {
    pub status: VortexOutputPayloadArtifactWriteStatus,
    pub mode: VortexOutputPayloadArtifactWriteMode,
    pub request: VortexOutputPayloadArtifactWriteRequest,
    pub effects_performed: Vec<VortexOutputPayloadEffect>,
    pub bytes_written: usize,
    pub checksum: Option<u64>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexOutputPayloadArtifactWriteReport {
    /// # Errors
    /// Returns an error only if human text rendering fails unexpectedly.
    pub fn from_request(request: VortexOutputPayloadArtifactWriteRequest) -> Result<Self> {
        write_vortex_output_payload_artifact(request)
    }
    #[must_use]
    pub fn feature_disabled(request: VortexOutputPayloadArtifactWriteRequest) -> Self {
        Self {
            status: VortexOutputPayloadArtifactWriteStatus::FeatureDisabled,
            mode: VortexOutputPayloadArtifactWriteMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            bytes_written: 0,
            checksum: None,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn planned(request: VortexOutputPayloadArtifactWriteRequest) -> Self {
        Self {
            status: VortexOutputPayloadArtifactWriteStatus::Planned,
            mode: VortexOutputPayloadArtifactWriteMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            bytes_written: 0,
            checksum: None,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn written(
        request: VortexOutputPayloadArtifactWriteRequest,
        bytes_written: usize,
        checksum: u64,
    ) -> Self {
        Self {
            status: VortexOutputPayloadArtifactWriteStatus::OutputPayloadArtifactWritten,
            mode: VortexOutputPayloadArtifactWriteMode::LocalOutputPayloadArtifactWrite,
            request,
            effects_performed: vec![VortexOutputPayloadEffect::OutputPayloadWritten],
            bytes_written,
            checksum: Some(checksum),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn blocked(
        request: VortexOutputPayloadArtifactWriteRequest,
        status: VortexOutputPayloadArtifactWriteStatus,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self::planned(request);
        report.status = status;
        report.add_diagnostic(Diagnostic::invalid_input(
            "output_payload_artifact_write",
            reason.into(),
            "keep writes local and feature-gated",
        ));
        report
    }
    #[must_use]
    pub fn unsupported(
        request: VortexOutputPayloadArtifactWriteRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self {
            status: VortexOutputPayloadArtifactWriteStatus::Unsupported,
            mode: VortexOutputPayloadArtifactWriteMode::Unsupported,
            request,
            effects_performed: Vec::new(),
            bytes_written: 0,
            checksum: None,
            diagnostics: Vec::new(),
        };
        report.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            None,
        ));
        report
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.request.has_errors()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub fn output_payload_artifact_written(&self) -> bool {
        self.status.output_payload_artifact_written()
    }
    #[must_use]
    pub fn output_payload_written(&self) -> bool {
        self.effects_performed
            .contains(&VortexOutputPayloadEffect::OutputPayloadWritten)
    }
    #[must_use]
    pub fn vortex_file_written(&self) -> bool {
        false
    }
    #[must_use]
    pub fn manifest_written(&self) -> bool {
        false
    }
    #[must_use]
    pub fn manifest_committed(&self) -> bool {
        false
    }
    #[must_use]
    pub fn object_store_io(&self) -> bool {
        false
    }
    #[must_use]
    pub fn upstream_vortex_write_called(&self) -> bool {
        false
    }
    #[must_use]
    pub fn recovery_action_executed(&self) -> bool {
        false
    }
    #[must_use]
    pub fn fallback_execution_allowed(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        !self.output_payload_written() && !self.fallback_execution_allowed()
    }
    /// # Errors
    /// Returns an error only if formatting into the output string fails unexpectedly.
    pub fn to_human_text(&self) -> Result<String> {
        let mut out = String::new();
        for line in [
            format!(
                "output payload artifact write status: {}",
                self.status.as_str()
            ),
            format!("mode: {}", self.mode.as_str()),
            format!("payload path: {}", self.request.payload_ref.path_string()),
            format!(
                "content kind: {}",
                self.request.payload_content.content_kind().as_str()
            ),
            format!("bytes written: {}", self.bytes_written),
            format!("checksum: {:?}", self.checksum),
            format!(
                "feature gate ready: {}",
                self.request.payload_plan_feature_gate_ready
            ),
            format!(
                "output payload artifact written: {}",
                self.output_payload_artifact_written()
            ),
            format!("output payload written: {}", self.output_payload_written()),
            format!("Vortex file written: {}", self.vortex_file_written()),
            format!("manifest written: {}", self.manifest_written()),
            format!("manifest committed: {}", self.manifest_committed()),
            format!("object-store IO: {}", self.object_store_io()),
            format!(
                "upstream Vortex write called: {}",
                self.upstream_vortex_write_called()
            ),
            format!(
                "recovery action executed: {}",
                self.recovery_action_executed()
            ),
            "placeholder artifact only; real Vortex payload not written".to_string(),
            "fallback execution disabled".to_string(),
        ] {
            writeln!(&mut out, "{line}")
                .map_err(|e| ShardLoomError::InvalidOperation(e.to_string()))?;
        }
        Ok(out)
    }
}

/// # Errors
/// Returns errors only if placeholder rendering fails unexpectedly.
pub fn write_vortex_output_payload_artifact(
    request: VortexOutputPayloadArtifactWriteRequest,
) -> Result<VortexOutputPayloadArtifactWriteReport> {
    #[cfg(not(feature = "vortex-staged-output-fs"))]
    {
        Ok(VortexOutputPayloadArtifactWriteReport::feature_disabled(
            request,
        ))
    }
    #[cfg(feature = "vortex-staged-output-fs")]
    {
        if request.payload_plan_summary.is_none() {
            return Ok(VortexOutputPayloadArtifactWriteReport::blocked(
                request,
                VortexOutputPayloadArtifactWriteStatus::BlockedByPayloadPlan,
                "output payload plan is not ready for artifact write",
            ));
        }
        if !request.payload_plan_feature_gate_ready {
            return Ok(VortexOutputPayloadArtifactWriteReport::blocked(
                request,
                VortexOutputPayloadArtifactWriteStatus::BlockedByFeatureGate,
                "output payload feature gate readiness missing",
            ));
        }
        if request.upstream_vortex_write_required {
            return Ok(VortexOutputPayloadArtifactWriteReport::blocked(
                request,
                VortexOutputPayloadArtifactWriteStatus::BlockedByUpstreamVortexWrite,
                "upstream `Vortex` write APIs are deferred",
            ));
        }
        let path_string = request.payload_ref.path_string();
        let local_path_string = if let Some(rest) = path_string.strip_prefix("file://") {
            rest.to_string()
        } else if path_string.contains("://") {
            return Ok(VortexOutputPayloadArtifactWriteReport::blocked(
                request,
                VortexOutputPayloadArtifactWriteStatus::BlockedByObjectStoreTarget,
                "object-store target is unsupported for local output payload artifact",
            ));
        } else {
            path_string
        };
        let payload_path = Path::new(&local_path_string);
        let Some(parent) = payload_path.parent() else {
            return Ok(VortexOutputPayloadArtifactWriteReport::blocked(
                request,
                VortexOutputPayloadArtifactWriteStatus::BlockedByMissingWorkspace,
                "workspace path is missing",
            ));
        };
        if !parent.exists() {
            return Ok(VortexOutputPayloadArtifactWriteReport::blocked(
                request,
                VortexOutputPayloadArtifactWriteStatus::BlockedByMissingWorkspace,
                "workspace path does not exist",
            ));
        }
        if !parent.is_dir() {
            return Ok(VortexOutputPayloadArtifactWriteReport::blocked(
                request,
                VortexOutputPayloadArtifactWriteStatus::BlockedByExistingNonDirectory,
                "workspace path exists and is not a directory",
            ));
        }
        if payload_path.exists()
            && !request.has_option(VortexOutputPayloadArtifactWriteOption::AllowOverwrite)
        {
            return Ok(VortexOutputPayloadArtifactWriteReport::blocked(
                request,
                VortexOutputPayloadArtifactWriteStatus::BlockedByExistingOutputPayload,
                "output payload artifact exists and overwrite is disabled",
            ));
        }
        let content = format!(
            "shardloom_output_payload_placeholder=true\nreal_vortex_payload=false\nupstream_vortex_write_called=false\noutput_data_from_query=false\nfallback_execution_allowed=false\ncontent_kind={}\ncontent_summary={}\n",
            request.payload_content.content_kind().as_str(),
            request.payload_content.summary()
        );
        fs::write(payload_path, &content).map_err(|e| {
            ShardLoomError::InvalidOperation(format!(
                "failed to write output payload artifact: {e}"
            ))
        })?;
        let mut hasher = DefaultHasher::new();
        content.hash(&mut hasher);
        Ok(VortexOutputPayloadArtifactWriteReport::written(
            request,
            content.len(),
            hasher.finish(),
        ))
    }
}

#[must_use]
pub fn vortex_output_payload_artifact_write_is_side_effect_free(
    report: &VortexOutputPayloadArtifactWriteReport,
) -> bool {
    report.is_side_effect_free()
}

/// # Errors
/// Returns errors propagated by payload-descriptor construction.
pub fn output_payload_artifact_write_request_from_plan(
    plan: &VortexOutputPayloadReport,
) -> Result<VortexOutputPayloadArtifactWriteRequest> {
    let mut request = VortexOutputPayloadArtifactWriteRequest::from_payload_request(&plan.request);
    request = request.feature_gate_ready(
        plan.request
            .has_signal(VortexOutputPayloadSignal::FeatureGateEnabled),
    );
    request = request.upstream_vortex_write_required(plan.upstream_vortex_write_required());
    if plan.status == VortexOutputPayloadStatus::PayloadReady {
        request = request.with_payload_plan_summary(plan.request.summary());
    }
    if plan.has_errors() {
        for d in &plan.request.diagnostics {
            request.add_diagnostic(d.clone());
        }
        for d in &plan.diagnostics {
            request.add_diagnostic(d.clone());
        }
    }
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        VortexFinalizedManifestArtifactWriteRequest, VortexFinalizedManifestContent,
        VortexFinalizedManifestFileRef, VortexStagedOutputRequest, VortexWriteIntentRequest,
    };
    fn sample_uri() -> DatasetUri {
        DatasetUri::new("file:///tmp/out.vortex").unwrap()
    }
    fn ws() -> VortexStagedWorkspacePath {
        VortexStagedWorkspacePath::new("/tmp/ws").unwrap()
    }
    #[test]
    fn basic_contracts() {
        assert!(!VortexOutputPayloadStatus::PayloadReady.allows_payload_write());
        assert!(VortexOutputPayloadStatus::BlockedByMissingPayloadContent.is_error());
        assert!(!VortexOutputPayloadMode::ReportOnly.writes_output_payload());
        assert!(!VortexOutputPayloadMode::ReportOnly.calls_upstream_vortex_write());
        assert!(VortexOutputPayloadFileName::new(" ").is_err());
        assert!(VortexOutputPayloadFileName::new("a/b").is_err());
        assert!(VortexOutputPayloadFileName::new("../x").is_err());
        assert_eq!(
            VortexOutputPayloadFileName::default_payload().as_str(),
            "_shardloom_output_payload.vortex"
        );
        let pref = VortexOutputPayloadFileRef::default_for_workspace(ws());
        assert_eq!(
            pref.path_string(),
            "/tmp/ws/_shardloom_output_payload.vortex"
        );
        assert!(VortexOutputPayloadContentDescriptor::unknown(" ").is_err());
        assert_eq!(
            VortexOutputPayloadContentDescriptor::synthetic_placeholder("x")
                .unwrap()
                .content_kind(),
            VortexOutputPayloadContentKind::SyntheticPlaceholder
        );
        assert!(
            !VortexOutputPayloadContentDescriptor::unknown("x")
                .unwrap()
                .has_payload_content()
        );
    }
    #[test]
    fn report_and_signals() {
        let payload = VortexOutputPayloadContentDescriptor::synthetic_placeholder("x").unwrap();
        let mut req = VortexOutputPayloadRequest::new(
            sample_uri(),
            VortexOutputPayloadFileRef::default_for_workspace(ws()),
            payload,
        );
        req.add_signal(VortexOutputPayloadSignal::WriteIntentReady, true);
        req.add_signal(VortexOutputPayloadSignal::WriteIntentReady, true);
        assert!(req.has_signal(VortexOutputPayloadSignal::WriteIntentReady));
        req.add_signal(VortexOutputPayloadSignal::WriteIntentReady, false);
        assert!(!req.has_signal(VortexOutputPayloadSignal::WriteIntentReady));
        let rep = VortexOutputPayloadReport::from_request(req).unwrap();
        assert_eq!(rep.status, VortexOutputPayloadStatus::BlockedByWriteIntent);
        let ready = VortexOutputPayloadReport::from_request(
            VortexOutputPayloadRequest::new(
                sample_uri(),
                VortexOutputPayloadFileRef::default_for_workspace(ws()),
                VortexOutputPayloadContentDescriptor::native_vortex_placeholder("x").unwrap(),
            )
            .write_intent_ready(true)
            .staged_output_ready(true)
            .finalized_manifest_ready(true)
            .payload_content_available(true)
            .feature_gate_enabled(true),
        )
        .unwrap();
        assert_eq!(ready.status, VortexOutputPayloadStatus::PayloadReady);
        assert!(!ready.allows_payload_write());
        assert!(!ready.output_payload_written());
        assert!(!ready.vortex_file_written());
        assert!(!ready.manifest_written());
        assert!(!ready.manifest_committed());
        assert!(!ready.object_store_io());
        assert!(!ready.upstream_vortex_write_called());
        assert!(!ready.recovery_action_executed());
        assert!(!ready.fallback_execution_allowed());
        assert!(ready.is_side_effect_free());
        let txt = ready.to_human_text().unwrap();
        assert!(txt.contains("fallback execution disabled"));
        assert!(txt.contains("output payload written: false"));
        let mut dready = ready.clone();
        dready.add_diagnostic(Diagnostic::no_fallback_execution("x"));
        assert!(dready.to_human_text().unwrap().contains("diagnostics:"));
    }
    #[test]
    fn planning_priorities_and_helpers() {
        let base = VortexOutputPayloadRequest::new(
            sample_uri(),
            VortexOutputPayloadFileRef::default_for_workspace(ws()),
            VortexOutputPayloadContentDescriptor::synthetic_placeholder("x").unwrap(),
        );
        assert_eq!(
            VortexOutputPayloadReport::from_request(base.clone().object_store_target(true))
                .unwrap()
                .status,
            VortexOutputPayloadStatus::BlockedByObjectStoreTarget
        );
        assert_eq!(
            VortexOutputPayloadReport::from_request(
                base.clone().upstream_vortex_write_required(true)
            )
            .unwrap()
            .status,
            VortexOutputPayloadStatus::BlockedByUpstreamVortexWrite
        );
        assert_eq!(
            VortexOutputPayloadReport::from_request(base.clone().write_intent_ready(true))
                .unwrap()
                .status,
            VortexOutputPayloadStatus::BlockedByStagedOutput
        );
        assert_eq!(
            VortexOutputPayloadReport::from_request(
                base.clone()
                    .write_intent_ready(true)
                    .staged_output_ready(true)
            )
            .unwrap()
            .status,
            VortexOutputPayloadStatus::BlockedByFinalizedManifest
        );
        assert_eq!(
            VortexOutputPayloadReport::from_request(
                base.clone()
                    .write_intent_ready(true)
                    .staged_output_ready(true)
                    .finalized_manifest_ready(true)
            )
            .unwrap()
            .status,
            VortexOutputPayloadStatus::BlockedByMissingPayloadContent
        );
        assert_eq!(
            plan_vortex_output_payload(base).unwrap().mode,
            VortexOutputPayloadMode::ReportOnly
        );
        let wi = crate::VortexWriteIntentReport::from_request(
            VortexWriteIntentRequest::new(sample_uri())
                .target_is_native_vortex(true)
                .schema_known(true)
                .schema_compatible(true)
                .delete_semantics_known(true)
                .tombstone_semantics_known(true)
                .commit_protocol_available(true),
        )
        .unwrap();
        let so = crate::VortexStagedOutputReport::from_request(
            VortexStagedOutputRequest::new(
                crate::VortexStagedWorkspaceId::new("ws").unwrap(),
                sample_uri(),
            )
            .with_workspace_path(ws())
            .write_intent_planned(true)
            .workspace_required(true)
            .workspace_path_known(true)
            .local_workspace(true)
            .commit_protocol_available(true)
            .feature_gate_enabled(true),
        )
        .unwrap();
        let fm = crate::VortexFinalizedManifestArtifactWriteReport::from_request(
            VortexFinalizedManifestArtifactWriteRequest::new(
                VortexFinalizedManifestFileRef::default_for_workspace(ws()),
                VortexFinalizedManifestContent::new("{}").unwrap(),
            )
            .feature_gate_ready(true),
        )
        .unwrap();
        let req = output_payload_request_from_reports(sample_uri(), ws(), &wi, &so, &fm).unwrap();
        assert!(req.has_signal(VortexOutputPayloadSignal::PayloadContentMissing));
        assert!(!req.has_signal(VortexOutputPayloadSignal::PayloadContentAvailable));
    }

    #[cfg(not(feature = "vortex-staged-output-fs"))]
    #[test]
    fn artifact_write_feature_disabled_by_default() {
        let request = VortexOutputPayloadArtifactWriteRequest::new(
            VortexOutputPayloadFileRef::default_for_workspace(ws()),
            VortexOutputPayloadContentDescriptor::synthetic_placeholder("placeholder").unwrap(),
        )
        .feature_gate_ready(true);
        let report = write_vortex_output_payload_artifact(request).unwrap();
        assert_eq!(
            report.status,
            VortexOutputPayloadArtifactWriteStatus::FeatureDisabled
        );
        assert!(!report.output_payload_artifact_written());
        assert!(!report.output_payload_written());
        assert!(!report.vortex_file_written());
        assert!(!report.manifest_written());
        assert!(!report.manifest_committed());
        assert!(!report.object_store_io());
        assert!(!report.upstream_vortex_write_called());
        assert!(!report.fallback_execution_allowed());
        let text = report.to_human_text().unwrap();
        assert!(text.contains("fallback execution disabled"));
        assert!(text.contains("Vortex file written: false"));
        assert!(text.contains("placeholder artifact only; real Vortex payload not written"));
        let req = VortexOutputPayloadArtifactWriteRequest::new(
            VortexOutputPayloadFileRef::default_for_workspace(ws()),
            VortexOutputPayloadContentDescriptor::synthetic_placeholder("placeholder").unwrap(),
        );
        assert!(!req.has_option(VortexOutputPayloadArtifactWriteOption::AllowOverwrite));
        assert!(
            req.allow_overwrite(true)
                .has_option(VortexOutputPayloadArtifactWriteOption::AllowOverwrite)
        );
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    #[test]
    fn artifact_write_feature_paths() {
        use std::fs;
        use std::time::{SystemTime, UNIX_EPOCH};
        let unique = format!(
            "shardloom-output-payload-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::temp_dir().join(unique);
        let workspace_dir = root.join("ws");
        fs::create_dir_all(&workspace_dir).unwrap();
        let workspace =
            VortexStagedWorkspacePath::new(workspace_dir.to_string_lossy().to_string()).unwrap();
        let payload_ref = VortexOutputPayloadFileRef::default_for_workspace(workspace.clone());
        let payload =
            VortexOutputPayloadContentDescriptor::synthetic_placeholder("placeholder").unwrap();
        let base =
            VortexOutputPayloadArtifactWriteRequest::new(payload_ref.clone(), payload.clone());
        assert_eq!(
            write_vortex_output_payload_artifact(base.clone())
                .unwrap()
                .status,
            VortexOutputPayloadArtifactWriteStatus::BlockedByPayloadPlan
        );
        assert_eq!(
            write_vortex_output_payload_artifact(
                base.clone()
                    .with_payload_plan_summary("payload_ready")
                    .feature_gate_ready(true)
                    .upstream_vortex_write_required(true)
            )
            .unwrap()
            .status,
            VortexOutputPayloadArtifactWriteStatus::BlockedByUpstreamVortexWrite
        );
        let missing_workspace =
            VortexStagedWorkspacePath::new(root.join("missing").to_string_lossy().to_string())
                .unwrap();
        assert_eq!(
            write_vortex_output_payload_artifact(
                VortexOutputPayloadArtifactWriteRequest::new(
                    VortexOutputPayloadFileRef::default_for_workspace(missing_workspace),
                    payload.clone()
                )
                .with_payload_plan_summary("payload_ready")
                .feature_gate_ready(true)
            )
            .unwrap()
            .status,
            VortexOutputPayloadArtifactWriteStatus::BlockedByMissingWorkspace
        );
        let obj_workspace = VortexStagedWorkspacePath::new("s3://bucket/key".to_string()).unwrap();
        assert_eq!(
            write_vortex_output_payload_artifact(
                VortexOutputPayloadArtifactWriteRequest::new(
                    VortexOutputPayloadFileRef::default_for_workspace(obj_workspace),
                    payload.clone()
                )
                .with_payload_plan_summary("payload_ready")
                .feature_gate_ready(true)
            )
            .unwrap()
            .status,
            VortexOutputPayloadArtifactWriteStatus::BlockedByObjectStoreTarget
        );
        let report = write_vortex_output_payload_artifact(
            base.clone()
                .with_payload_plan_summary("payload_ready")
                .feature_gate_ready(true),
        )
        .unwrap();
        assert!(report.output_payload_artifact_written());
        assert!(report.output_payload_written());
        assert!(!report.vortex_file_written());
        let file_path = std::path::PathBuf::from(payload_ref.path_string());
        let content = fs::read_to_string(&file_path).unwrap();
        assert!(content.contains("shardloom_output_payload_placeholder=true"));
        assert!(content.contains("real_vortex_payload=false"));
        assert!(content.contains("upstream_vortex_write_called=false"));
        assert!(content.contains("fallback_execution_allowed=false"));
        assert_eq!(
            write_vortex_output_payload_artifact(
                base.clone()
                    .with_payload_plan_summary("payload_ready")
                    .feature_gate_ready(true),
            )
            .unwrap()
            .status,
            VortexOutputPayloadArtifactWriteStatus::BlockedByExistingOutputPayload
        );
        assert!(
            write_vortex_output_payload_artifact(
                base.with_payload_plan_summary("payload_ready")
                    .feature_gate_ready(true)
                    .allow_overwrite(true)
            )
            .unwrap()
            .output_payload_artifact_written()
        );
        let file_uri_workspace =
            VortexStagedWorkspacePath::new(format!("file://{}", workspace_dir.to_string_lossy()))
                .unwrap();
        let file_uri_request = VortexOutputPayloadArtifactWriteRequest::new(
            VortexOutputPayloadFileRef::default_for_workspace(file_uri_workspace),
            payload,
        )
        .with_payload_plan_summary("payload_ready")
        .feature_gate_ready(true)
        .allow_overwrite(true);
        assert!(
            write_vortex_output_payload_artifact(file_uri_request)
                .unwrap()
                .output_payload_artifact_written()
        );
        fs::remove_file(file_path).unwrap();
        fs::remove_dir(workspace_dir).unwrap();
        fs::remove_dir(root).unwrap();
    }
}
