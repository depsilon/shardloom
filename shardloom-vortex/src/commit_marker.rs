use std::{
    collections::hash_map::DefaultHasher,
    fmt::Write as _,
    hash::{Hash, Hasher},
};
#[cfg(feature = "vortex-staged-output-fs")]
use std::{fs, fs::OpenOptions, io::Write as _, path::PathBuf};

#[cfg(feature = "vortex-staged-output-fs")]
use shardloom_core::{DatasetUri, UriScheme};
use shardloom_core::{
    Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity, FallbackStatus, Result,
    ShardLoomError,
};

use crate::{
    VortexCommitProtocolReport, VortexCommitProtocolState, VortexCommitProtocolStatus,
    VortexCommitProtocolTransition, VortexStagedWorkspacePath,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCommitMarkerStatus {
    Planned,
    MarkerReady,
    BlockedByCommitProtocol,
    BlockedByManifestFinalization,
    BlockedByObjectStoreTarget,
    BlockedByFeatureGate,
    Unsupported,
}
impl VortexCommitMarkerStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::MarkerReady => "marker_ready",
            Self::BlockedByCommitProtocol => "blocked_by_commit_protocol",
            Self::BlockedByManifestFinalization => "blocked_by_manifest_finalization",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByFeatureGate => "blocked_by_feature_gate",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::Planned | Self::MarkerReady)
    }
    #[must_use]
    pub const fn allows_marker_write(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCommitMarkerMode {
    ReportOnly,
    MarkerPlanning,
    Unsupported,
}
impl VortexCommitMarkerMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::MarkerPlanning => "marker_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn writes_commit_marker(self) -> bool {
        false
    }
    #[must_use]
    pub const fn finalizes_manifest(self) -> bool {
        false
    }
    #[must_use]
    pub const fn commits_manifest(self) -> bool {
        false
    }
    #[must_use]
    pub const fn writes_output_data(self) -> bool {
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
pub enum VortexCommitMarkerSignal {
    CommitProtocolReady,
    CommitProtocolBlocked,
    ManifestFinalizationAvailable,
    ManifestFinalizationMissing,
    LocalWorkspace,
    ObjectStoreTarget,
    FeatureGateEnabled,
}
impl VortexCommitMarkerSignal {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CommitProtocolReady => "commit_protocol_ready",
            Self::CommitProtocolBlocked => "commit_protocol_blocked",
            Self::ManifestFinalizationAvailable => "manifest_finalization_available",
            Self::ManifestFinalizationMissing => "manifest_finalization_missing",
            Self::LocalWorkspace => "local_workspace",
            Self::ObjectStoreTarget => "object_store_target",
            Self::FeatureGateEnabled => "feature_gate_enabled",
        }
    }
    #[must_use]
    pub const fn is_blocking(self) -> bool {
        matches!(
            self,
            Self::CommitProtocolBlocked
                | Self::ManifestFinalizationMissing
                | Self::ObjectStoreTarget
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCommitMarkerEffect {
    CommitMarkerWritten,
    ManifestFinalized,
    ManifestCommitted,
    OutputDataWritten,
    ObjectStoreIo,
    UpstreamVortexWriteCalled,
    RecoveryActionExecuted,
    FallbackExecution,
}
impl VortexCommitMarkerEffect {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CommitMarkerWritten => "commit_marker_written",
            Self::ManifestFinalized => "manifest_finalized",
            Self::ManifestCommitted => "manifest_committed",
            Self::OutputDataWritten => "output_data_written",
            Self::ObjectStoreIo => "object_store_io",
            Self::UpstreamVortexWriteCalled => "upstream_vortex_write_called",
            Self::RecoveryActionExecuted => "recovery_action_executed",
            Self::FallbackExecution => "fallback_execution",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCommitMarkerWriteStatus {
    FeatureDisabled,
    Planned,
    CommitMarkerWritten,
    BlockedByMarkerPlan,
    BlockedByObjectStoreTarget,
    BlockedByMissingWorkspace,
    BlockedByExistingCommitMarker,
    BlockedByExistingNonDirectory,
    BlockedByFeatureGate,
    Unsupported,
}
impl VortexCommitMarkerWriteStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::Planned => "planned",
            Self::CommitMarkerWritten => "commit_marker_written",
            Self::BlockedByMarkerPlan => "blocked_by_marker_plan",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByMissingWorkspace => "blocked_by_missing_workspace",
            Self::BlockedByExistingCommitMarker => "blocked_by_existing_commit_marker",
            Self::BlockedByExistingNonDirectory => "blocked_by_existing_non_directory",
            Self::BlockedByFeatureGate => "blocked_by_feature_gate",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(
            self,
            Self::FeatureDisabled | Self::Planned | Self::CommitMarkerWritten
        )
    }
    #[must_use]
    pub const fn commit_marker_written(self) -> bool {
        matches!(self, Self::CommitMarkerWritten)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCommitMarkerWriteMode {
    ReportOnly,
    LocalCommitMarkerWrite,
    Unsupported,
}
impl VortexCommitMarkerWriteMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::LocalCommitMarkerWrite => "local_commit_marker_write",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn writes_commit_marker(self) -> bool {
        matches!(self, Self::LocalCommitMarkerWrite)
    }
    #[must_use]
    pub const fn finalizes_manifest(self) -> bool {
        false
    }
    #[must_use]
    pub const fn commits_manifest(self) -> bool {
        false
    }
    #[must_use]
    pub const fn writes_output_data(self) -> bool {
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
pub enum VortexCommitMarkerWriteOption {
    AllowOverwrite,
}
impl VortexCommitMarkerWriteOption {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AllowOverwrite => "allow_overwrite",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCommitMarkerWriteSignal {
    MarkerPlanReady,
    MarkerPlanBlocked,
    FeatureGateReady,
    ObjectStoreTarget,
    ExistingCommitMarker,
}
impl VortexCommitMarkerWriteSignal {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MarkerPlanReady => "marker_plan_ready",
            Self::MarkerPlanBlocked => "marker_plan_blocked",
            Self::FeatureGateReady => "feature_gate_ready",
            Self::ObjectStoreTarget => "object_store_target",
            Self::ExistingCommitMarker => "existing_commit_marker",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexCommitMarkerWriteRequest {
    pub marker_ref: VortexCommitMarkerFileRef,
    pub marker_content: VortexCommitMarkerContent,
    pub options: Vec<VortexCommitMarkerWriteOption>,
    pub signals: Vec<VortexCommitMarkerWriteSignal>,
    pub marker_plan_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexCommitMarkerWriteRequest {
    #[must_use]
    pub fn new(
        marker_ref: VortexCommitMarkerFileRef,
        marker_content: VortexCommitMarkerContent,
    ) -> Self {
        Self {
            marker_ref,
            marker_content,
            options: Vec::new(),
            signals: Vec::new(),
            marker_plan_summary: None,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn from_marker_request(request: &VortexCommitMarkerRequest) -> Self {
        let mut write_request =
            Self::new(request.marker_ref.clone(), request.marker_content.clone());
        if request.has_signal(VortexCommitMarkerSignal::FeatureGateEnabled) {
            write_request.add_signal(VortexCommitMarkerWriteSignal::FeatureGateReady, true);
        }
        if request.has_signal(VortexCommitMarkerSignal::ObjectStoreTarget) {
            write_request.add_signal(VortexCommitMarkerWriteSignal::ObjectStoreTarget, true);
        }
        write_request
    }
    #[must_use]
    pub fn allow_overwrite(mut self, value: bool) -> Self {
        if value {
            if !self
                .options
                .contains(&VortexCommitMarkerWriteOption::AllowOverwrite)
            {
                self.options
                    .push(VortexCommitMarkerWriteOption::AllowOverwrite);
            }
        } else {
            self.options
                .retain(|o| *o != VortexCommitMarkerWriteOption::AllowOverwrite);
        }
        self
    }
    pub fn add_signal(&mut self, signal: VortexCommitMarkerWriteSignal, value: bool) {
        if value {
            if !self.signals.contains(&signal) {
                self.signals.push(signal);
            }
        } else {
            self.signals.retain(|s| *s != signal);
        }
    }
    #[must_use]
    pub fn marker_plan_ready(mut self, value: bool) -> Self {
        self.add_signal(VortexCommitMarkerWriteSignal::MarkerPlanReady, value);
        self
    }
    #[must_use]
    pub fn marker_plan_blocked(mut self, value: bool) -> Self {
        self.add_signal(VortexCommitMarkerWriteSignal::MarkerPlanBlocked, value);
        self
    }
    #[must_use]
    pub fn feature_gate_ready(mut self, value: bool) -> Self {
        self.add_signal(VortexCommitMarkerWriteSignal::FeatureGateReady, value);
        self
    }
    #[must_use]
    pub fn object_store_target(mut self, value: bool) -> Self {
        self.add_signal(VortexCommitMarkerWriteSignal::ObjectStoreTarget, value);
        self
    }
    #[must_use]
    pub fn existing_commit_marker(mut self, value: bool) -> Self {
        self.add_signal(VortexCommitMarkerWriteSignal::ExistingCommitMarker, value);
        self
    }
    #[must_use]
    pub fn with_marker_plan_summary(mut self, summary: impl Into<String>) -> Self {
        self.marker_plan_summary = Some(summary.into());
        self
    }
    #[must_use]
    pub fn has_option(&self, option: VortexCommitMarkerWriteOption) -> bool {
        self.options.contains(&option)
    }
    #[must_use]
    pub fn has_signal(&self, signal: VortexCommitMarkerWriteSignal) -> bool {
        self.signals.contains(&signal)
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
            "{} {} signals={}",
            self.marker_ref.summary(),
            self.marker_content.summary(),
            self.signals.len()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexCommitMarkerWriteReport {
    pub status: VortexCommitMarkerWriteStatus,
    pub mode: VortexCommitMarkerWriteMode,
    pub request: VortexCommitMarkerWriteRequest,
    pub effects_performed: Vec<VortexCommitMarkerEffect>,
    pub bytes_written: usize,
    pub checksum: Option<u64>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexCommitMarkerWriteReport {
    /// # Errors
    /// Returns an error if commit marker write report construction fails.
    pub fn from_request(request: VortexCommitMarkerWriteRequest) -> Result<Self> {
        #[cfg(not(feature = "vortex-staged-output-fs"))]
        {
            Ok(Self::feature_disabled(request))
        }
        #[cfg(feature = "vortex-staged-output-fs")]
        {
            let mut report = Self::planned(request);
            if report
                .request
                .has_signal(VortexCommitMarkerWriteSignal::ObjectStoreTarget)
            {
                report = Self::blocked(
                    report.request,
                    VortexCommitMarkerWriteStatus::BlockedByObjectStoreTarget,
                    "commit marker path targets object-store storage",
                );
            } else if report
                .request
                .has_signal(VortexCommitMarkerWriteSignal::MarkerPlanBlocked)
                || !report
                    .request
                    .has_signal(VortexCommitMarkerWriteSignal::MarkerPlanReady)
            {
                report = Self::blocked(
                    report.request,
                    VortexCommitMarkerWriteStatus::BlockedByMarkerPlan,
                    "commit marker plan is not ready",
                );
            } else if !report
                .request
                .has_signal(VortexCommitMarkerWriteSignal::FeatureGateReady)
            {
                report = Self::blocked(
                    report.request,
                    VortexCommitMarkerWriteStatus::BlockedByFeatureGate,
                    "commit marker planning feature-gate readiness is missing",
                );
            }
            Ok(report)
        }
    }
    #[must_use]
    pub fn feature_disabled(request: VortexCommitMarkerWriteRequest) -> Self {
        Self {
            status: VortexCommitMarkerWriteStatus::FeatureDisabled,
            mode: VortexCommitMarkerWriteMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            bytes_written: 0,
            checksum: None,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn planned(request: VortexCommitMarkerWriteRequest) -> Self {
        Self {
            status: VortexCommitMarkerWriteStatus::Planned,
            mode: VortexCommitMarkerWriteMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            bytes_written: 0,
            checksum: None,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn commit_marker_written_report(
        request: VortexCommitMarkerWriteRequest,
        bytes_written: usize,
        checksum: u64,
    ) -> Self {
        Self {
            status: VortexCommitMarkerWriteStatus::CommitMarkerWritten,
            mode: VortexCommitMarkerWriteMode::LocalCommitMarkerWrite,
            request,
            effects_performed: vec![VortexCommitMarkerEffect::CommitMarkerWritten],
            bytes_written,
            checksum: Some(checksum),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn blocked(
        request: VortexCommitMarkerWriteRequest,
        status: VortexCommitMarkerWriteStatus,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self::planned(request);
        report.status = status;
        report.add_diagnostic(Diagnostic::invalid_input(
            "commit_marker_write",
            reason,
            "adjust commit marker write request signals or local workspace state",
        ));
        report
    }
    #[must_use]
    pub fn unsupported(
        request: VortexCommitMarkerWriteRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        Self {
            status: VortexCommitMarkerWriteStatus::Unsupported,
            mode: VortexCommitMarkerWriteMode::Unsupported,
            request,
            effects_performed: Vec::new(),
            bytes_written: 0,
            checksum: None,
            diagnostics: vec![Diagnostic::new(
                DiagnosticCode::UnsupportedEffect,
                DiagnosticSeverity::Error,
                DiagnosticCategory::UnsupportedFeature,
                format!("unsupported commit marker write feature: {feature}"),
                Some(feature),
                Some(reason),
                Some("Use feature-gated local commit marker writes only.".to_string()),
                FallbackStatus::disabled_by_policy(),
            )],
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.request.has_errors()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub fn commit_marker_written(&self) -> bool {
        self.effects_performed
            .contains(&VortexCommitMarkerEffect::CommitMarkerWritten)
    }
    #[must_use]
    pub const fn manifest_finalized(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn manifest_committed(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn output_data_written(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn object_store_io(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn upstream_vortex_write_called(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn recovery_action_executed(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty() && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "commit marker write status: {}", self.status.as_str());
        let _ = writeln!(out, "mode: {}", self.mode.as_str());
        let _ = writeln!(
            out,
            "marker path: {}",
            self.request.marker_ref.path_string()
        );
        let _ = writeln!(out, "bytes written: {}", self.bytes_written);
        let _ = writeln!(
            out,
            "checksum: {}",
            self.checksum
                .map_or_else(|| "none".to_string(), |v| v.to_string())
        );
        let _ = writeln!(
            out,
            "feature gate ready: {}",
            self.request
                .has_signal(VortexCommitMarkerWriteSignal::FeatureGateReady)
        );
        let _ = writeln!(
            out,
            "commit marker written: {}",
            self.commit_marker_written()
        );
        let _ = writeln!(out, "manifest finalized: false");
        let _ = writeln!(out, "manifest committed: false");
        let _ = writeln!(out, "output data written: false");
        let _ = writeln!(out, "object-store IO: false");
        let _ = writeln!(out, "upstream Vortex write called: false");
        let _ = writeln!(out, "recovery action executed: false");
        let _ = writeln!(out, "fallback execution disabled");
        if !self.request.diagnostics.is_empty() || !self.diagnostics.is_empty() {
            let _ = writeln!(out, "diagnostics:");
            for d in self
                .request
                .diagnostics
                .iter()
                .chain(self.diagnostics.iter())
            {
                let _ = writeln!(out, "- [{}] {}", d.code.as_str(), d.message);
            }
        }
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexCommitMarkerFileName(String);
impl VortexCommitMarkerFileName {
    /// # Errors
    /// Returns an error when the marker file name is empty, uses path separators, or contains traversal markers.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let v = value.into();
        let t = v.trim();
        if t.is_empty() || t.contains('/') || t.contains('\\') || t.contains("..") {
            return Err(ShardLoomError::InvalidOperation(
                "invalid commit marker file name".to_string(),
            ));
        }
        Ok(Self(t.to_string()))
    }
    #[must_use]
    pub fn default_marker() -> Self {
        Self(".shardloom-commit-marker".to_string())
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!("commit_marker_file_name={}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexCommitMarkerFileRef {
    workspace_path: VortexStagedWorkspacePath,
    file_name: VortexCommitMarkerFileName,
}
impl VortexCommitMarkerFileRef {
    #[must_use]
    pub fn new(
        workspace_path: VortexStagedWorkspacePath,
        file_name: VortexCommitMarkerFileName,
    ) -> Self {
        Self {
            workspace_path,
            file_name,
        }
    }
    #[must_use]
    pub fn default_for_workspace(workspace_path: VortexStagedWorkspacePath) -> Self {
        Self::new(workspace_path, VortexCommitMarkerFileName::default_marker())
    }
    #[must_use]
    pub fn workspace_path(&self) -> &VortexStagedWorkspacePath {
        &self.workspace_path
    }
    #[must_use]
    pub fn file_name(&self) -> &VortexCommitMarkerFileName {
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
        format!("marker_path={}", self.path_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexCommitMarkerContent {
    content: String,
}
impl VortexCommitMarkerContent {
    /// # Errors
    /// Returns an error when marker content is empty/whitespace or larger than 64 KiB.
    pub fn new(content: impl Into<String>) -> Result<Self> {
        let c = content.into();
        if c.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "invalid commit marker content".to_string(),
            ));
        }
        if c.len() > 64 * 1024 {
            return Err(ShardLoomError::InvalidOperation(
                "commit marker content exceeds 64 KiB".to_string(),
            ));
        }
        Ok(Self { content: c })
    }
    /// # Errors
    /// Returns an error when derived marker content fails validation.
    pub fn from_protocol_report(report: &VortexCommitProtocolReport) -> Result<Self> {
        let content = format!(
            "shardloom_commit_marker_draft=true\ncommit_marker_written=false\nmanifest_finalized=false\nmanifest_committed=false\noutput_data_written=false\nfallback_execution_allowed=false\ncommit_protocol_next_state={}\ncommit_protocol_status={}\n",
            report.next_state().as_str(),
            report.status.as_str()
        );
        Self::new(content)
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.content
    }
    #[must_use]
    pub fn len(&self) -> usize {
        self.content.len()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.content.is_empty()
    }
    #[must_use]
    pub fn checksum_u64(&self) -> u64 {
        let mut h = DefaultHasher::new();
        self.content.hash(&mut h);
        h.finish()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "marker_content_bytes={} checksum_u64={}",
            self.len(),
            self.checksum_u64()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexCommitMarkerRequest {
    pub marker_ref: VortexCommitMarkerFileRef,
    pub marker_content: VortexCommitMarkerContent,
    pub signals: Vec<VortexCommitMarkerSignal>,
    pub protocol_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexCommitMarkerRequest {
    #[must_use]
    pub fn new(
        marker_ref: VortexCommitMarkerFileRef,
        marker_content: VortexCommitMarkerContent,
    ) -> Self {
        Self {
            marker_ref,
            marker_content,
            signals: Vec::new(),
            protocol_summary: None,
            diagnostics: Vec::new(),
        }
    }
    pub fn add_signal(&mut self, signal: VortexCommitMarkerSignal, value: bool) {
        if value {
            if !self.signals.contains(&signal) {
                self.signals.push(signal);
            }
        } else {
            self.signals.retain(|s| *s != signal);
        }
    }
    #[must_use]
    pub fn commit_protocol_ready(mut self, value: bool) -> Self {
        self.add_signal(VortexCommitMarkerSignal::CommitProtocolReady, value);
        self
    }
    #[must_use]
    pub fn commit_protocol_blocked(mut self, value: bool) -> Self {
        self.add_signal(VortexCommitMarkerSignal::CommitProtocolBlocked, value);
        self
    }
    #[must_use]
    pub fn manifest_finalization_available(mut self, value: bool) -> Self {
        self.add_signal(
            VortexCommitMarkerSignal::ManifestFinalizationAvailable,
            value,
        );
        self
    }
    #[must_use]
    pub fn manifest_finalization_missing(mut self, value: bool) -> Self {
        self.add_signal(VortexCommitMarkerSignal::ManifestFinalizationMissing, value);
        self
    }
    #[must_use]
    pub fn local_workspace(mut self, value: bool) -> Self {
        self.add_signal(VortexCommitMarkerSignal::LocalWorkspace, value);
        self
    }
    #[must_use]
    pub fn object_store_target(mut self, value: bool) -> Self {
        self.add_signal(VortexCommitMarkerSignal::ObjectStoreTarget, value);
        self
    }
    #[must_use]
    pub fn feature_gate_enabled(mut self, value: bool) -> Self {
        self.add_signal(VortexCommitMarkerSignal::FeatureGateEnabled, value);
        self
    }
    #[must_use]
    pub fn with_protocol_summary(mut self, summary: impl Into<String>) -> Self {
        self.protocol_summary = Some(summary.into());
        self
    }
    #[must_use]
    pub fn has_signal(&self, signal: VortexCommitMarkerSignal) -> bool {
        self.signals.contains(&signal)
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
            "{} signals={}",
            self.marker_ref.summary(),
            self.signals.len()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexCommitMarkerReport {
    pub status: VortexCommitMarkerStatus,
    pub mode: VortexCommitMarkerMode,
    pub request: VortexCommitMarkerRequest,
    pub effects_performed: Vec<VortexCommitMarkerEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexCommitMarkerReport {
    /// # Errors
    /// Returns an error if report construction fails.
    pub fn from_request(request: VortexCommitMarkerRequest) -> Result<Self> {
        Ok(Self {
            status: derive_status(&request),
            mode: VortexCommitMarkerMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        })
    }
    #[must_use]
    pub fn unsupported(
        request: VortexCommitMarkerRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        Self {
            status: VortexCommitMarkerStatus::Unsupported,
            mode: VortexCommitMarkerMode::Unsupported,
            request,
            effects_performed: Vec::new(),
            diagnostics: vec![Diagnostic::new(
                DiagnosticCode::UnsupportedEffect,
                DiagnosticSeverity::Error,
                DiagnosticCategory::UnsupportedFeature,
                format!("unsupported commit marker feature: {feature}"),
                Some(feature),
                Some(reason),
                Some("Use report-only commit marker planning in this phase.".to_string()),
                FallbackStatus::disabled_by_policy(),
            )],
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
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
    pub fn commit_protocol_ready(&self) -> bool {
        self.request
            .has_signal(VortexCommitMarkerSignal::CommitProtocolReady)
    }
    #[must_use]
    pub fn manifest_finalization_available(&self) -> bool {
        self.request
            .has_signal(VortexCommitMarkerSignal::ManifestFinalizationAvailable)
    }
    #[must_use]
    pub fn local_workspace(&self) -> bool {
        self.request
            .has_signal(VortexCommitMarkerSignal::LocalWorkspace)
    }
    #[must_use]
    pub fn object_store_target(&self) -> bool {
        self.request
            .has_signal(VortexCommitMarkerSignal::ObjectStoreTarget)
    }
    #[must_use]
    pub fn feature_gate_enabled(&self) -> bool {
        self.request
            .has_signal(VortexCommitMarkerSignal::FeatureGateEnabled)
    }
    #[must_use]
    pub fn commit_marker_written(&self) -> bool {
        self.effects_performed
            .contains(&VortexCommitMarkerEffect::CommitMarkerWritten)
    }
    #[must_use]
    pub fn manifest_finalized(&self) -> bool {
        self.effects_performed
            .contains(&VortexCommitMarkerEffect::ManifestFinalized)
    }
    #[must_use]
    pub fn manifest_committed(&self) -> bool {
        self.effects_performed
            .contains(&VortexCommitMarkerEffect::ManifestCommitted)
    }
    #[must_use]
    pub fn output_data_written(&self) -> bool {
        self.effects_performed
            .contains(&VortexCommitMarkerEffect::OutputDataWritten)
    }
    #[must_use]
    pub fn object_store_io(&self) -> bool {
        self.effects_performed
            .contains(&VortexCommitMarkerEffect::ObjectStoreIo)
    }
    #[must_use]
    pub fn upstream_vortex_write_called(&self) -> bool {
        self.effects_performed
            .contains(&VortexCommitMarkerEffect::UpstreamVortexWriteCalled)
    }
    #[must_use]
    pub fn recovery_action_executed(&self) -> bool {
        self.effects_performed
            .contains(&VortexCommitMarkerEffect::RecoveryActionExecuted)
    }
    #[must_use]
    pub fn fallback_execution_allowed(&self) -> bool {
        self.effects_performed
            .contains(&VortexCommitMarkerEffect::FallbackExecution)
    }
    #[must_use]
    pub const fn allows_marker_write(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty() && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "commit marker status: {}", self.status.as_str());
        let _ = writeln!(out, "mode: {}", self.mode.as_str());
        let _ = writeln!(
            out,
            "marker path: {}",
            self.request.marker_ref.path_string()
        );
        let _ = writeln!(out, "content length: {}", self.request.marker_content.len());
        let _ = writeln!(
            out,
            "content checksum_u64: {}",
            self.request.marker_content.checksum_u64()
        );
        let _ = writeln!(
            out,
            "commit protocol ready: {}",
            self.commit_protocol_ready()
        );
        let _ = writeln!(
            out,
            "manifest finalization available: {}",
            self.manifest_finalization_available()
        );
        let _ = writeln!(out, "local workspace: {}", self.local_workspace());
        let _ = writeln!(out, "object-store target: {}", self.object_store_target());
        let _ = writeln!(
            out,
            "commit marker written: {}",
            self.commit_marker_written()
        );
        let _ = writeln!(out, "manifest finalized: {}", self.manifest_finalized());
        let _ = writeln!(out, "manifest committed: {}", self.manifest_committed());
        let _ = writeln!(out, "output data written: {}", self.output_data_written());
        let _ = writeln!(out, "object-store IO: {}", self.object_store_io());
        let _ = writeln!(
            out,
            "upstream Vortex write called: {}",
            self.upstream_vortex_write_called()
        );
        let _ = writeln!(
            out,
            "recovery action executed: {}",
            self.recovery_action_executed()
        );
        let _ = writeln!(out, "fallback execution disabled");
        if !self.request.diagnostics.is_empty() || !self.diagnostics.is_empty() {
            let _ = writeln!(out, "diagnostics:");
            for d in self
                .request
                .diagnostics
                .iter()
                .chain(self.diagnostics.iter())
            {
                let _ = writeln!(out, "- [{}] {}", d.code.as_str(), d.message);
            }
        }
        out
    }
}

fn derive_status(request: &VortexCommitMarkerRequest) -> VortexCommitMarkerStatus {
    if request.has_signal(VortexCommitMarkerSignal::ObjectStoreTarget) {
        return VortexCommitMarkerStatus::BlockedByObjectStoreTarget;
    }
    if request.has_signal(VortexCommitMarkerSignal::CommitProtocolBlocked)
        || !request.has_signal(VortexCommitMarkerSignal::CommitProtocolReady)
    {
        return VortexCommitMarkerStatus::BlockedByCommitProtocol;
    }
    if request.has_signal(VortexCommitMarkerSignal::ManifestFinalizationMissing)
        || !request.has_signal(VortexCommitMarkerSignal::ManifestFinalizationAvailable)
    {
        return VortexCommitMarkerStatus::BlockedByManifestFinalization;
    }
    if !request.has_signal(VortexCommitMarkerSignal::FeatureGateEnabled) {
        return VortexCommitMarkerStatus::BlockedByFeatureGate;
    }
    VortexCommitMarkerStatus::MarkerReady
}

/// # Errors
/// Propagates request validation and report construction errors.
pub fn plan_vortex_commit_marker(
    request: VortexCommitMarkerRequest,
) -> Result<VortexCommitMarkerReport> {
    VortexCommitMarkerReport::from_request(request)
}
#[must_use]
pub fn vortex_commit_marker_is_side_effect_free(report: &VortexCommitMarkerReport) -> bool {
    report.is_side_effect_free()
}

/// # Errors
/// Returns an error if marker content derived from the protocol report is invalid.
pub fn commit_marker_request_from_protocol_report(
    workspace_path: VortexStagedWorkspacePath,
    protocol: &VortexCommitProtocolReport,
) -> Result<VortexCommitMarkerRequest> {
    let workspace_path_string = workspace_path.as_str().to_string();
    let marker_ref = VortexCommitMarkerFileRef::default_for_workspace(workspace_path);
    let marker_content = VortexCommitMarkerContent::from_protocol_report(protocol)?;
    let mut request = VortexCommitMarkerRequest::new(marker_ref, marker_content)
        .with_protocol_summary(protocol.to_human_text())
        .manifest_finalization_available(protocol.manifest_finalization_available())
        .object_store_target(protocol.object_store_target());
    let waiting_for_commit_marker = protocol.status
        == VortexCommitProtocolStatus::BlockedByCommitMarker
        && protocol.request.transition.requires_commit_marker();
    let ready = protocol.next_state() == VortexCommitProtocolState::CommitReady
        || waiting_for_commit_marker
        || (protocol.status == VortexCommitProtocolStatus::TransitionAllowed
            && protocol.request.transition == VortexCommitProtocolTransition::MarkCommitReady);
    request = request.commit_protocol_ready(ready);
    let protocol_has_blocking_diagnostics = protocol
        .request
        .diagnostics
        .iter()
        .chain(protocol.diagnostics.iter())
        .any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        });
    let protocol_status_blocks_marker = protocol.status.is_error() && !waiting_for_commit_marker;
    request = request.commit_protocol_blocked(
        protocol_status_blocks_marker || protocol_has_blocking_diagnostics,
    );
    for diagnostic in protocol
        .request
        .diagnostics
        .iter()
        .chain(protocol.diagnostics.iter())
    {
        request.add_diagnostic(diagnostic.clone());
    }
    if workspace_path_string.starts_with('/') || workspace_path_string.starts_with('.') {
        request = request.local_workspace(true);
    }
    Ok(request)
}

/// Writes only the local commit marker file addressed by `request.marker_ref` when
/// `vortex-staged-output-fs` is enabled and marker-plan readiness is explicit.
///
/// The helper never finalizes manifests, commits manifests, writes output payload data,
/// calls upstream `Vortex` write APIs, performs object-store IO, executes recovery, or
/// enables fallback execution.
///
/// # Errors
/// Returns an error only when local path normalization or the exact local file write fails.
pub fn write_vortex_commit_marker(
    request: VortexCommitMarkerWriteRequest,
) -> Result<VortexCommitMarkerWriteReport> {
    #[cfg(not(feature = "vortex-staged-output-fs"))]
    {
        Ok(VortexCommitMarkerWriteReport::feature_disabled(request))
    }
    #[cfg(feature = "vortex-staged-output-fs")]
    {
        let mut report = VortexCommitMarkerWriteReport::from_request(request)?;
        if report.status != VortexCommitMarkerWriteStatus::Planned {
            return Ok(report);
        }
        let workspace_uri = DatasetUri::new(report.request.marker_ref.workspace_path().as_str())?;
        if !matches!(
            workspace_uri.scheme(),
            UriScheme::LocalPath | UriScheme::File
        ) {
            report
                .request
                .add_signal(VortexCommitMarkerWriteSignal::ObjectStoreTarget, true);
            report = VortexCommitMarkerWriteReport::blocked(
                report.request,
                VortexCommitMarkerWriteStatus::BlockedByObjectStoreTarget,
                "commit marker path targets object-store storage",
            );
            return Ok(report);
        }
        let marker_path = commit_marker_local_path(&report.request.marker_ref)?;
        let workspace_path = marker_path.parent().ok_or_else(|| {
            ShardLoomError::InvalidOperation("missing commit marker parent path".to_string())
        })?;
        if !workspace_path.exists() {
            report = VortexCommitMarkerWriteReport::blocked(
                report.request,
                VortexCommitMarkerWriteStatus::BlockedByMissingWorkspace,
                "commit marker workspace is missing",
            );
            return Ok(report);
        }
        if !workspace_path.is_dir() {
            report = VortexCommitMarkerWriteReport::blocked(
                report.request,
                VortexCommitMarkerWriteStatus::BlockedByExistingNonDirectory,
                "commit marker workspace path exists but is not a directory",
            );
            return Ok(report);
        }
        if report
            .request
            .has_option(VortexCommitMarkerWriteOption::AllowOverwrite)
        {
            fs::write(
                &marker_path,
                report.request.marker_content.as_str().as_bytes(),
            )
            .map_err(|err| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to write commit marker file: {err}"
                ))
            })?;
        } else {
            let mut file = match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&marker_path)
            {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let mut request = report.request;
                    request.add_signal(VortexCommitMarkerWriteSignal::ExistingCommitMarker, true);
                    report = VortexCommitMarkerWriteReport::blocked(
                        request,
                        VortexCommitMarkerWriteStatus::BlockedByExistingCommitMarker,
                        "commit marker file already exists",
                    );
                    return Ok(report);
                }
                Err(error) => {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "failed to open commit marker file for atomic create: {error}"
                    )));
                }
            };
            file.write_all(report.request.marker_content.as_str().as_bytes())
                .map_err(|error| {
                    ShardLoomError::InvalidOperation(format!(
                        "failed to write commit marker file: {error}"
                    ))
                })?;
        }
        let bytes_written = report.request.marker_content.len();
        let checksum = report.request.marker_content.checksum_u64();
        Ok(VortexCommitMarkerWriteReport::commit_marker_written_report(
            report.request,
            bytes_written,
            checksum,
        ))
    }
}

#[cfg(feature = "vortex-staged-output-fs")]
fn commit_marker_local_path(marker_ref: &VortexCommitMarkerFileRef) -> Result<PathBuf> {
    let workspace_raw = marker_ref.workspace_path().as_str();
    let workspace = match DatasetUri::new(workspace_raw.to_string())?.scheme() {
        UriScheme::LocalPath => PathBuf::from(workspace_raw),
        UriScheme::File => {
            if let Some(local) = workspace_raw.strip_prefix("file:///") {
                PathBuf::from(format!("/{local}"))
            } else {
                return Err(ShardLoomError::InvalidOperation(
                    "workspace file URI must use file:/// absolute local path".to_string(),
                ));
            }
        }
        _ => {
            return Err(ShardLoomError::InvalidOperation(
                "commit marker path looks like object-store target".to_string(),
            ));
        }
    };
    Ok(workspace.join(marker_ref.file_name().as_str()))
}

#[must_use]
pub fn vortex_commit_marker_write_is_side_effect_free(
    report: &VortexCommitMarkerWriteReport,
) -> bool {
    report.is_side_effect_free()
}

/// Converts a report-only marker plan into a local commit marker write request without IO.
///
/// # Errors
/// Returns an error if the copied `VortexCommitMarkerContent` or marker reference is invalid.
pub fn commit_marker_write_request_from_plan(
    plan: &VortexCommitMarkerReport,
) -> Result<VortexCommitMarkerWriteRequest> {
    let mut request = VortexCommitMarkerWriteRequest::from_marker_request(&plan.request)
        .with_marker_plan_summary(plan.request.summary());
    if plan.has_errors() {
        request.add_signal(VortexCommitMarkerWriteSignal::MarkerPlanBlocked, true);
        for d in plan
            .request
            .diagnostics
            .iter()
            .chain(plan.diagnostics.iter())
        {
            request.add_diagnostic(d.clone());
        }
    }
    if matches!(plan.status, VortexCommitMarkerStatus::MarkerReady) {
        request.add_signal(VortexCommitMarkerWriteSignal::MarkerPlanReady, true);
        request = request.with_marker_plan_summary(plan.to_human_text());
    }
    if plan
        .request
        .has_signal(VortexCommitMarkerSignal::FeatureGateEnabled)
    {
        request.add_signal(VortexCommitMarkerWriteSignal::FeatureGateReady, true);
    }
    if plan.object_store_target() {
        request.add_signal(VortexCommitMarkerWriteSignal::ObjectStoreTarget, true);
    }
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{VortexCommitProtocolRequest, plan_vortex_commit_protocol};
    use shardloom_core::DatasetUri;
    fn base_req() -> VortexCommitMarkerRequest {
        VortexCommitMarkerRequest::new(
            VortexCommitMarkerFileRef::default_for_workspace(
                VortexStagedWorkspacePath::new("/tmp/w").unwrap(),
            ),
            VortexCommitMarkerContent::new("ok").unwrap(),
        )
    }

    fn write_req() -> VortexCommitMarkerWriteRequest {
        VortexCommitMarkerWriteRequest::new(
            VortexCommitMarkerFileRef::default_for_workspace(
                VortexStagedWorkspacePath::new("/tmp/commit-marker-write-test").unwrap(),
            ),
            VortexCommitMarkerContent::new("commit_marker=true\n").unwrap(),
        )
    }

    #[test]
    fn write_request_defaults_and_feature_disabled_report() {
        let request = write_req();
        assert!(!request.has_option(VortexCommitMarkerWriteOption::AllowOverwrite));
        assert!(
            request
                .clone()
                .allow_overwrite(true)
                .has_option(VortexCommitMarkerWriteOption::AllowOverwrite)
        );
        let report = write_vortex_commit_marker(
            request
                .marker_plan_ready(true)
                .feature_gate_ready(true)
                .allow_overwrite(true),
        )
        .unwrap();
        #[cfg(not(feature = "vortex-staged-output-fs"))]
        assert_eq!(
            report.status,
            VortexCommitMarkerWriteStatus::FeatureDisabled
        );
        assert!(!report.commit_marker_written());
        assert!(!report.manifest_finalized());
        assert!(!report.manifest_committed());
        assert!(!report.output_data_written());
        assert!(!report.object_store_io());
        assert!(!report.upstream_vortex_write_called());
        assert!(!report.recovery_action_executed());
        assert!(!report.fallback_execution_allowed());
        let text = report.to_human_text();
        assert!(text.contains("fallback execution disabled"));
        assert!(text.contains("output data written: false"));
    }

    #[test]
    fn write_request_from_plan_preserves_diagnostics_and_feature_gate() {
        let mut blocked_request = base_req();
        blocked_request.add_diagnostic(Diagnostic::invalid_input(
            "commit_marker_plan",
            "test diagnostic",
            "fix test input",
        ));
        let blocked_plan = plan_vortex_commit_marker(blocked_request).unwrap();
        let from_blocked = commit_marker_write_request_from_plan(&blocked_plan).unwrap();
        assert!(from_blocked.has_signal(VortexCommitMarkerWriteSignal::MarkerPlanBlocked));
        assert!(from_blocked.has_errors());

        let ready_plan = plan_vortex_commit_marker(
            base_req()
                .commit_protocol_ready(true)
                .manifest_finalization_available(true)
                .feature_gate_enabled(true),
        )
        .unwrap();
        let ready_write = commit_marker_write_request_from_plan(&ready_plan).unwrap();
        assert!(ready_write.has_signal(VortexCommitMarkerWriteSignal::MarkerPlanReady));
        assert!(ready_write.has_signal(VortexCommitMarkerWriteSignal::FeatureGateReady));
        assert!(ready_write.marker_plan_summary.is_some());
        assert!(!std::path::Path::new(&ready_write.marker_ref.path_string()).exists());
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    fn unique_commit_marker_workspace(name: &str) -> std::path::PathBuf {
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("shardloom-commit-marker-{name}-{nanos}"))
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    fn feature_write_req(workspace: &std::path::Path) -> VortexCommitMarkerWriteRequest {
        VortexCommitMarkerWriteRequest::new(
            VortexCommitMarkerFileRef::default_for_workspace(
                VortexStagedWorkspacePath::new(workspace.to_str().unwrap()).unwrap(),
            ),
            VortexCommitMarkerContent::new("commit_marker_written=true\n").unwrap(),
        )
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    #[test]
    fn feature_write_blocks_without_feature_gate_or_workspace() {
        let workspace = unique_commit_marker_workspace("blocked");
        let no_gate =
            write_vortex_commit_marker(feature_write_req(&workspace).marker_plan_ready(true))
                .unwrap();
        assert_eq!(
            no_gate.status,
            VortexCommitMarkerWriteStatus::BlockedByFeatureGate
        );
        let missing = write_vortex_commit_marker(
            feature_write_req(&workspace)
                .marker_plan_ready(true)
                .feature_gate_ready(true),
        )
        .unwrap();
        assert_eq!(
            missing.status,
            VortexCommitMarkerWriteStatus::BlockedByMissingWorkspace
        );
        assert!(!workspace.exists());
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    #[test]
    fn feature_write_blocks_object_store_and_non_directory_parent() {
        let object_store = VortexCommitMarkerWriteRequest::new(
            VortexCommitMarkerFileRef::default_for_workspace(
                VortexStagedWorkspacePath::new("s3://bucket/workspace").unwrap(),
            ),
            VortexCommitMarkerContent::new("commit_marker=true\n").unwrap(),
        )
        .marker_plan_ready(true)
        .feature_gate_ready(true);
        let object_report = write_vortex_commit_marker(object_store).unwrap();
        assert_eq!(
            object_report.status,
            VortexCommitMarkerWriteStatus::BlockedByObjectStoreTarget
        );

        let parent_file = unique_commit_marker_workspace("parent-file");
        std::fs::write(&parent_file, b"not a directory").unwrap();
        let non_dir = write_vortex_commit_marker(
            feature_write_req(&parent_file)
                .marker_plan_ready(true)
                .feature_gate_ready(true),
        )
        .unwrap();
        assert_eq!(
            non_dir.status,
            VortexCommitMarkerWriteStatus::BlockedByExistingNonDirectory
        );
        std::fs::remove_file(parent_file).unwrap();
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    #[test]
    fn feature_write_writes_exact_marker_and_blocks_overwrite_by_default() {
        let workspace = unique_commit_marker_workspace("exact");
        std::fs::create_dir(&workspace).unwrap();
        let request = feature_write_req(&workspace)
            .marker_plan_ready(true)
            .feature_gate_ready(true);
        let marker_path = workspace.join(request.marker_ref.file_name().as_str());
        let committed_manifest = workspace.join(".shardloom-committed-manifest");
        let output_payload = workspace.join(".shardloom-output-data");
        let report = write_vortex_commit_marker(request.clone()).unwrap();
        assert_eq!(
            report.status,
            VortexCommitMarkerWriteStatus::CommitMarkerWritten
        );
        assert!(report.commit_marker_written());
        assert_eq!(report.bytes_written, request.marker_content.len());
        assert_eq!(report.checksum, Some(request.marker_content.checksum_u64()));
        assert!(!report.manifest_finalized());
        assert!(!report.manifest_committed());
        assert!(!report.output_data_written());
        assert!(!report.object_store_io());
        assert!(!report.upstream_vortex_write_called());
        assert!(!report.recovery_action_executed());
        assert!(!report.fallback_execution_allowed());
        assert_eq!(
            std::fs::read_to_string(&marker_path).unwrap(),
            request.marker_content.as_str()
        );
        assert!(!committed_manifest.exists());
        assert!(!output_payload.exists());

        let blocked = write_vortex_commit_marker(request.clone()).unwrap();
        assert_eq!(
            blocked.status,
            VortexCommitMarkerWriteStatus::BlockedByExistingCommitMarker
        );
        let overwrite_content = VortexCommitMarkerContent::new("overwritten=true\n").unwrap();
        let overwrite_request = VortexCommitMarkerWriteRequest::new(
            request.marker_ref.clone(),
            overwrite_content.clone(),
        )
        .marker_plan_ready(true)
        .feature_gate_ready(true)
        .allow_overwrite(true);
        let overwritten = write_vortex_commit_marker(overwrite_request).unwrap();
        assert_eq!(
            overwritten.status,
            VortexCommitMarkerWriteStatus::CommitMarkerWritten
        );
        assert_eq!(
            std::fs::read_to_string(&marker_path).unwrap(),
            overwrite_content.as_str()
        );
        assert!(!committed_manifest.exists());
        assert!(!output_payload.exists());
        std::fs::remove_file(marker_path).unwrap();
        std::fs::remove_dir(workspace).unwrap();
    }

    #[test]
    fn status_mode_and_validations() {
        assert!(!VortexCommitMarkerStatus::MarkerReady.allows_marker_write());
        assert!(VortexCommitMarkerStatus::BlockedByCommitProtocol.is_error());
        assert!(!VortexCommitMarkerMode::ReportOnly.writes_commit_marker());
        assert!(!VortexCommitMarkerMode::ReportOnly.finalizes_manifest());
        assert!(VortexCommitMarkerWriteStatus::CommitMarkerWritten.commit_marker_written());
        assert!(VortexCommitMarkerWriteStatus::BlockedByFeatureGate.is_error());
        assert!(VortexCommitMarkerWriteMode::LocalCommitMarkerWrite.writes_commit_marker());
        assert!(!VortexCommitMarkerWriteMode::LocalCommitMarkerWrite.finalizes_manifest());
        assert!(!VortexCommitMarkerWriteMode::LocalCommitMarkerWrite.commits_manifest());
        assert!(!VortexCommitMarkerWriteMode::LocalCommitMarkerWrite.writes_output_data());
        assert!(!VortexCommitMarkerWriteMode::LocalCommitMarkerWrite.writes_object_store());
        assert!(!VortexCommitMarkerWriteMode::LocalCommitMarkerWrite.calls_upstream_vortex_write());
        assert!(!VortexCommitMarkerWriteMode::LocalCommitMarkerWrite.executes_recovery_action());
        assert_eq!(
            VortexCommitMarkerWriteOption::AllowOverwrite.as_str(),
            "allow_overwrite"
        );
        assert!(VortexCommitMarkerFileName::new("").is_err());
        assert!(VortexCommitMarkerFileName::new("a/b").is_err());
        assert!(VortexCommitMarkerFileName::new("..a").is_err());
        assert_eq!(
            VortexCommitMarkerFileName::default_marker().as_str(),
            ".shardloom-commit-marker"
        );
        assert_eq!(
            VortexCommitMarkerFileRef::default_for_workspace(
                VortexStagedWorkspacePath::new("/tmp/w").unwrap()
            )
            .path_string(),
            "/tmp/w/.shardloom-commit-marker"
        );
        assert!(VortexCommitMarkerContent::new(" ").is_err());
        assert!(VortexCommitMarkerContent::new("a".repeat(64 * 1024 + 1)).is_err());
        let c = VortexCommitMarkerContent::new("abc").unwrap();
        assert_eq!(
            c.checksum_u64(),
            VortexCommitMarkerContent::new("abc")
                .unwrap()
                .checksum_u64()
        );
    }
    #[test]
    fn request_and_report_behaviors() {
        let mut req = base_req();
        req.add_signal(VortexCommitMarkerSignal::CommitProtocolReady, true);
        req.add_signal(VortexCommitMarkerSignal::CommitProtocolReady, true);
        assert_eq!(req.signals.len(), 1);
        req.add_signal(VortexCommitMarkerSignal::CommitProtocolReady, false);
        assert_eq!(req.signals.len(), 0);
        let rep =
            VortexCommitMarkerReport::from_request(base_req().object_store_target(true)).unwrap();
        assert_eq!(
            rep.status,
            VortexCommitMarkerStatus::BlockedByObjectStoreTarget
        );
        let rep = VortexCommitMarkerReport::from_request(
            base_req().manifest_finalization_available(true),
        )
        .unwrap();
        assert_eq!(
            rep.status,
            VortexCommitMarkerStatus::BlockedByCommitProtocol
        );
        let rep =
            VortexCommitMarkerReport::from_request(base_req().commit_protocol_ready(true)).unwrap();
        assert_eq!(
            rep.status,
            VortexCommitMarkerStatus::BlockedByManifestFinalization
        );
        let rep = VortexCommitMarkerReport::from_request(
            base_req()
                .commit_protocol_ready(true)
                .manifest_finalization_available(true),
        )
        .unwrap();
        assert_eq!(rep.status, VortexCommitMarkerStatus::BlockedByFeatureGate);
        let rep = VortexCommitMarkerReport::from_request(
            base_req()
                .commit_protocol_ready(true)
                .manifest_finalization_available(true)
                .feature_gate_enabled(true),
        )
        .unwrap();
        assert_eq!(rep.status, VortexCommitMarkerStatus::MarkerReady);
        assert!(!rep.allows_marker_write());
        assert!(!rep.commit_marker_written());
        assert!(!rep.manifest_finalized());
        assert!(!rep.manifest_committed());
        assert!(!rep.output_data_written());
        assert!(!rep.object_store_io());
        assert!(!rep.upstream_vortex_write_called());
        assert!(!rep.recovery_action_executed());
        assert!(!rep.fallback_execution_allowed());
        assert!(rep.is_side_effect_free());
        let mut rep2 = rep.clone();
        rep2.add_diagnostic(Diagnostic::no_fallback_execution("x"));
        let txt = rep2.to_human_text();
        assert!(txt.contains("fallback execution disabled"));
        assert!(txt.contains("commit marker written: false"));
        assert!(txt.contains("diagnostics:"));
    }
    #[test]
    fn helper_and_protocol_mapping() {
        let req = base_req()
            .commit_protocol_ready(true)
            .manifest_finalization_available(true)
            .feature_gate_enabled(true);
        let _ = plan_vortex_commit_marker(req).unwrap();
        let uri = DatasetUri::new("file://tmp/a").unwrap();
        let protocol = plan_vortex_commit_protocol(
            VortexCommitProtocolRequest::new(
                uri.clone(),
                VortexCommitProtocolState::AwaitingCommitMarker,
                VortexCommitProtocolTransition::MarkCommitReady,
            )
            .commit_intent_ready(true)
            .draft_manifest_ready(true)
            .manifest_finalization_available(true)
            .commit_marker_available(true)
            .recovery_ready(true),
        )
        .unwrap();
        let mapped = commit_marker_request_from_protocol_report(
            VortexStagedWorkspacePath::new("/tmp/w").unwrap(),
            &protocol,
        )
        .unwrap();
        assert!(mapped.has_signal(VortexCommitMarkerSignal::CommitProtocolReady));
        let blocked = plan_vortex_commit_protocol(VortexCommitProtocolRequest::new(
            uri,
            VortexCommitProtocolState::AwaitingCommitMarker,
            VortexCommitProtocolTransition::MarkCommitReady,
        ))
        .unwrap();
        let mapped2 = commit_marker_request_from_protocol_report(
            VortexStagedWorkspacePath::new("/tmp/w").unwrap(),
            &blocked,
        )
        .unwrap();
        assert!(mapped2.has_signal(VortexCommitMarkerSignal::CommitProtocolBlocked));
        let waiting = plan_vortex_commit_protocol(
            VortexCommitProtocolRequest::new(
                DatasetUri::new("file://tmp/b").unwrap(),
                VortexCommitProtocolState::AwaitingCommitMarker,
                VortexCommitProtocolTransition::PrepareCommitMarker,
            )
            .commit_intent_ready(true)
            .recovery_ready(true)
            .manifest_finalization_available(true),
        )
        .unwrap();
        assert_eq!(
            waiting.status,
            VortexCommitProtocolStatus::BlockedByCommitMarker
        );
        let mapped_waiting = commit_marker_request_from_protocol_report(
            VortexStagedWorkspacePath::new("/tmp/w").unwrap(),
            &waiting,
        )
        .unwrap();
        assert!(mapped_waiting.has_signal(VortexCommitMarkerSignal::CommitProtocolReady));
        assert!(!mapped_waiting.has_signal(VortexCommitMarkerSignal::CommitProtocolBlocked));
        assert_eq!(
            plan_vortex_commit_marker(mapped_waiting).unwrap().status,
            VortexCommitMarkerStatus::BlockedByFeatureGate
        );
        let mut invalid_waiting = waiting.clone();
        invalid_waiting.add_diagnostic(Diagnostic::invalid_input(
            "commit_protocol",
            "unrelated protocol diagnostic",
            "fix protocol request",
        ));
        let mapped_invalid_waiting = commit_marker_request_from_protocol_report(
            VortexStagedWorkspacePath::new("/tmp/w").unwrap(),
            &invalid_waiting,
        )
        .unwrap();
        assert!(mapped_invalid_waiting.has_signal(VortexCommitMarkerSignal::CommitProtocolReady));
        assert!(mapped_invalid_waiting.has_signal(VortexCommitMarkerSignal::CommitProtocolBlocked));
        assert_eq!(mapped_invalid_waiting.diagnostics.len(), 1);
        assert_eq!(
            plan_vortex_commit_marker(mapped_invalid_waiting.feature_gate_enabled(true))
                .unwrap()
                .status,
            VortexCommitMarkerStatus::BlockedByCommitProtocol
        );
        let rep = plan_vortex_commit_marker(mapped).unwrap();
        assert!(!rep.commit_marker_written());
    }
}
