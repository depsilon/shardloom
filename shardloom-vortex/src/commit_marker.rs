use std::{
    collections::hash_map::DefaultHasher,
    fmt::Write as _,
    hash::{Hash, Hasher},
};

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
    request = request.commit_protocol_blocked(protocol.has_errors() && !waiting_for_commit_marker);
    if workspace_path_string.starts_with('/') || workspace_path_string.starts_with('.') {
        request = request.local_workspace(true);
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
    #[test]
    fn status_mode_and_validations() {
        assert!(!VortexCommitMarkerStatus::MarkerReady.allows_marker_write());
        assert!(VortexCommitMarkerStatus::BlockedByCommitProtocol.is_error());
        assert!(!VortexCommitMarkerMode::ReportOnly.writes_commit_marker());
        assert!(!VortexCommitMarkerMode::ReportOnly.finalizes_manifest());
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
        let rep = plan_vortex_commit_marker(mapped).unwrap();
        assert!(!rep.commit_marker_written());
    }
}
