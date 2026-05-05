use std::fmt::Write as _;

use shardloom_core::{
    DatasetUri, Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity, FallbackStatus,
    Result,
};

use crate::{
    VortexCommitMarkerWriteReport, VortexCommitProtocolReport, VortexCommitProtocolState,
    VortexFinalizedManifestArtifactWriteReport, VortexManifestFinalizationReport,
    VortexManifestFinalizationStatus,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLocalCommitExecutionGateStatus {
    Planned,
    CommitExecutionReady,
    BlockedByCommitProtocol,
    BlockedByCommitMarker,
    BlockedByManifestFinalization,
    BlockedByFinalizedManifestArtifact,
    BlockedByOutputPayload,
    BlockedByObjectStoreTarget,
    BlockedByUpstreamVortexWrite,
    BlockedByFeatureGate,
    Unsupported,
}
impl VortexLocalCommitExecutionGateStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::CommitExecutionReady => "commit_execution_ready",
            Self::BlockedByCommitProtocol => "blocked_by_commit_protocol",
            Self::BlockedByCommitMarker => "blocked_by_commit_marker",
            Self::BlockedByManifestFinalization => "blocked_by_manifest_finalization",
            Self::BlockedByFinalizedManifestArtifact => "blocked_by_finalized_manifest_artifact",
            Self::BlockedByOutputPayload => "blocked_by_output_payload",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByUpstreamVortexWrite => "blocked_by_upstream_vortex_write",
            Self::BlockedByFeatureGate => "blocked_by_feature_gate",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::Planned | Self::CommitExecutionReady)
    }
    #[must_use]
    pub const fn allows_commit_execution(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLocalCommitExecutionGateMode {
    ReportOnly,
    CommitExecutionGatePlanning,
    Unsupported,
}
impl VortexLocalCommitExecutionGateMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::CommitExecutionGatePlanning => "commit_execution_gate_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn executes_commit(self) -> bool {
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
pub enum VortexLocalCommitExecutionGateSignal {
    CommitProtocolReady,
    CommitProtocolBlocked,
    CommitMarkerWritten,
    CommitMarkerMissing,
    ManifestFinalizationReady,
    ManifestFinalizationBlocked,
    FinalizedManifestArtifactWritten,
    FinalizedManifestArtifactMissing,
    OutputPayloadReady,
    OutputPayloadMissing,
    LocalWorkspace,
    ObjectStoreTarget,
    UpstreamVortexWriteRequired,
    FeatureGateEnabled,
}
impl VortexLocalCommitExecutionGateSignal {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CommitProtocolReady => "commit_protocol_ready",
            Self::CommitProtocolBlocked => "commit_protocol_blocked",
            Self::CommitMarkerWritten => "commit_marker_written",
            Self::CommitMarkerMissing => "commit_marker_missing",
            Self::ManifestFinalizationReady => "manifest_finalization_ready",
            Self::ManifestFinalizationBlocked => "manifest_finalization_blocked",
            Self::FinalizedManifestArtifactWritten => "finalized_manifest_artifact_written",
            Self::FinalizedManifestArtifactMissing => "finalized_manifest_artifact_missing",
            Self::OutputPayloadReady => "output_payload_ready",
            Self::OutputPayloadMissing => "output_payload_missing",
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
            Self::CommitProtocolBlocked
                | Self::CommitMarkerMissing
                | Self::ManifestFinalizationBlocked
                | Self::FinalizedManifestArtifactMissing
                | Self::OutputPayloadMissing
                | Self::ObjectStoreTarget
                | Self::UpstreamVortexWriteRequired
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLocalCommitExecutionGateEffect {
    CommitExecuted,
    ManifestCommitted,
    OutputDataWritten,
    ObjectStoreIo,
    UpstreamVortexWriteCalled,
    RecoveryActionExecuted,
    FallbackExecution,
}
impl VortexLocalCommitExecutionGateEffect {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CommitExecuted => "commit_executed",
            Self::ManifestCommitted => "manifest_committed",
            Self::OutputDataWritten => "output_data_written",
            Self::ObjectStoreIo => "object_store_io",
            Self::UpstreamVortexWriteCalled => "upstream_vortex_write_called",
            Self::RecoveryActionExecuted => "recovery_action_executed",
            Self::FallbackExecution => "fallback_execution",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexLocalCommitExecutionGateRequest {
    pub target_uri: DatasetUri,
    pub signals: Vec<VortexLocalCommitExecutionGateSignal>,
    pub commit_protocol_summary: Option<String>,
    pub commit_marker_summary: Option<String>,
    pub manifest_finalization_summary: Option<String>,
    pub finalized_manifest_artifact_summary: Option<String>,
    pub output_payload_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexLocalCommitExecutionGateRequest {
    #[must_use]
    pub fn new(target_uri: DatasetUri) -> Self {
        Self {
            target_uri,
            signals: Vec::new(),
            commit_protocol_summary: None,
            commit_marker_summary: None,
            manifest_finalization_summary: None,
            finalized_manifest_artifact_summary: None,
            output_payload_summary: None,
            diagnostics: Vec::new(),
        }
    }
    fn toggle_signal(&mut self, s: VortexLocalCommitExecutionGateSignal, v: bool) {
        if v {
            if !self.signals.contains(&s) {
                self.signals.push(s);
            }
        } else {
            self.signals.retain(|x| *x != s);
        }
    }
    #[must_use]
    pub fn add_signal(mut self, s: VortexLocalCommitExecutionGateSignal, v: bool) -> Self {
        self.toggle_signal(s, v);
        self
    }
    #[must_use]
    pub fn commit_protocol_ready(mut self, v: bool) -> Self {
        self.toggle_signal(VortexLocalCommitExecutionGateSignal::CommitProtocolReady, v);
        self
    }
    #[must_use]
    pub fn commit_protocol_blocked(mut self, v: bool) -> Self {
        self.toggle_signal(
            VortexLocalCommitExecutionGateSignal::CommitProtocolBlocked,
            v,
        );
        self
    }
    #[must_use]
    pub fn commit_marker_written(mut self, v: bool) -> Self {
        self.toggle_signal(VortexLocalCommitExecutionGateSignal::CommitMarkerWritten, v);
        self
    }
    #[must_use]
    pub fn commit_marker_missing(mut self, v: bool) -> Self {
        self.toggle_signal(VortexLocalCommitExecutionGateSignal::CommitMarkerMissing, v);
        self
    }
    #[must_use]
    pub fn manifest_finalization_ready(mut self, v: bool) -> Self {
        self.toggle_signal(
            VortexLocalCommitExecutionGateSignal::ManifestFinalizationReady,
            v,
        );
        self
    }
    #[must_use]
    pub fn manifest_finalization_blocked(mut self, v: bool) -> Self {
        self.toggle_signal(
            VortexLocalCommitExecutionGateSignal::ManifestFinalizationBlocked,
            v,
        );
        self
    }
    #[must_use]
    pub fn finalized_manifest_artifact_written(mut self, v: bool) -> Self {
        self.toggle_signal(
            VortexLocalCommitExecutionGateSignal::FinalizedManifestArtifactWritten,
            v,
        );
        self
    }
    #[must_use]
    pub fn finalized_manifest_artifact_missing(mut self, v: bool) -> Self {
        self.toggle_signal(
            VortexLocalCommitExecutionGateSignal::FinalizedManifestArtifactMissing,
            v,
        );
        self
    }
    #[must_use]
    pub fn output_payload_ready(mut self, v: bool) -> Self {
        self.toggle_signal(VortexLocalCommitExecutionGateSignal::OutputPayloadReady, v);
        self
    }
    #[must_use]
    pub fn output_payload_missing(mut self, v: bool) -> Self {
        self.toggle_signal(
            VortexLocalCommitExecutionGateSignal::OutputPayloadMissing,
            v,
        );
        self
    }
    #[must_use]
    pub fn local_workspace(mut self, v: bool) -> Self {
        self.toggle_signal(VortexLocalCommitExecutionGateSignal::LocalWorkspace, v);
        self
    }
    #[must_use]
    pub fn object_store_target(mut self, v: bool) -> Self {
        self.toggle_signal(VortexLocalCommitExecutionGateSignal::ObjectStoreTarget, v);
        self
    }
    #[must_use]
    pub fn upstream_vortex_write_required(mut self, v: bool) -> Self {
        self.toggle_signal(
            VortexLocalCommitExecutionGateSignal::UpstreamVortexWriteRequired,
            v,
        );
        self
    }
    #[must_use]
    pub fn feature_gate_enabled(mut self, v: bool) -> Self {
        self.toggle_signal(VortexLocalCommitExecutionGateSignal::FeatureGateEnabled, v);
        self
    }
    #[must_use]
    pub fn with_commit_protocol_summary(mut self, s: impl Into<String>) -> Self {
        self.commit_protocol_summary = Some(s.into());
        self
    }
    #[must_use]
    pub fn with_commit_marker_summary(mut self, s: impl Into<String>) -> Self {
        self.commit_marker_summary = Some(s.into());
        self
    }
    #[must_use]
    pub fn with_manifest_finalization_summary(mut self, s: impl Into<String>) -> Self {
        self.manifest_finalization_summary = Some(s.into());
        self
    }
    #[must_use]
    pub fn with_finalized_manifest_artifact_summary(mut self, s: impl Into<String>) -> Self {
        self.finalized_manifest_artifact_summary = Some(s.into());
        self
    }
    #[must_use]
    pub fn with_output_payload_summary(mut self, s: impl Into<String>) -> Self {
        self.output_payload_summary = Some(s.into());
        self
    }
    #[must_use]
    pub fn has_signal(&self, s: VortexLocalCommitExecutionGateSignal) -> bool {
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
pub struct VortexLocalCommitExecutionGateReport {
    pub status: VortexLocalCommitExecutionGateStatus,
    pub mode: VortexLocalCommitExecutionGateMode,
    pub request: VortexLocalCommitExecutionGateRequest,
    pub effects_performed: Vec<VortexLocalCommitExecutionGateEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexLocalCommitExecutionGateReport {
    /// # Errors
    /// Returns an error if building text summaries fails.
    pub fn from_request(request: VortexLocalCommitExecutionGateRequest) -> Result<Self> {
        Ok(Self {
            status: derive_status(&request),
            mode: VortexLocalCommitExecutionGateMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        })
    }
    #[must_use]
    pub fn unsupported(
        request: VortexLocalCommitExecutionGateRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        Self{status:VortexLocalCommitExecutionGateStatus::Unsupported,mode:VortexLocalCommitExecutionGateMode::Unsupported,request,effects_performed:Vec::new(),diagnostics:vec![Diagnostic::new(DiagnosticCode::UnsupportedEffect,DiagnosticSeverity::Error,DiagnosticCategory::UnsupportedFeature,format!("unsupported local commit execution gate feature: {feature}"),Some(feature),Some(reason),Some("Use report-only local commit execution gate planning until Phase 12D commit protocol execution.".to_string()),FallbackStatus::disabled_by_policy())]}
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
    pub fn commit_protocol_ready(&self) -> bool {
        self.request
            .has_signal(VortexLocalCommitExecutionGateSignal::CommitProtocolReady)
    }
    #[must_use]
    pub fn commit_marker_written(&self) -> bool {
        self.request
            .has_signal(VortexLocalCommitExecutionGateSignal::CommitMarkerWritten)
    }
    #[must_use]
    pub fn manifest_finalization_ready(&self) -> bool {
        self.request
            .has_signal(VortexLocalCommitExecutionGateSignal::ManifestFinalizationReady)
    }
    #[must_use]
    pub fn finalized_manifest_artifact_written(&self) -> bool {
        self.request
            .has_signal(VortexLocalCommitExecutionGateSignal::FinalizedManifestArtifactWritten)
    }
    #[must_use]
    pub fn output_payload_ready(&self) -> bool {
        self.request
            .has_signal(VortexLocalCommitExecutionGateSignal::OutputPayloadReady)
    }
    #[must_use]
    pub fn local_workspace(&self) -> bool {
        self.request
            .has_signal(VortexLocalCommitExecutionGateSignal::LocalWorkspace)
    }
    #[must_use]
    pub fn object_store_target(&self) -> bool {
        self.request
            .has_signal(VortexLocalCommitExecutionGateSignal::ObjectStoreTarget)
    }
    #[must_use]
    pub fn upstream_vortex_write_required(&self) -> bool {
        self.request
            .has_signal(VortexLocalCommitExecutionGateSignal::UpstreamVortexWriteRequired)
    }
    #[must_use]
    pub fn commit_executed(&self) -> bool {
        false
    }
    #[must_use]
    pub fn manifest_committed(&self) -> bool {
        false
    }
    #[must_use]
    pub fn output_data_written(&self) -> bool {
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
    pub const fn allows_commit_execution(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty() && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "local commit execution gate status: {}",
            self.status.as_str()
        );
        let _ = writeln!(out, "mode: {}", self.mode.as_str());
        let _ = writeln!(out, "target URI: {}", self.request.target_uri.as_str());
        let _ = writeln!(
            out,
            "commit protocol ready: {}",
            self.commit_protocol_ready()
        );
        let _ = writeln!(
            out,
            "commit marker written: {}",
            self.commit_marker_written()
        );
        let _ = writeln!(
            out,
            "manifest finalization ready: {}",
            self.manifest_finalization_ready()
        );
        let _ = writeln!(
            out,
            "finalized manifest artifact written: {}",
            self.finalized_manifest_artifact_written()
        );
        let _ = writeln!(out, "output payload ready: {}", self.output_payload_ready());
        let _ = writeln!(out, "local workspace: {}", self.local_workspace());
        let _ = writeln!(out, "object-store target: {}", self.object_store_target());
        let _ = writeln!(
            out,
            "upstream Vortex write required: {}",
            self.upstream_vortex_write_required()
        );
        let _ = writeln!(out, "commit executed: {}", self.commit_executed());
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
        for d in self
            .request
            .diagnostics
            .iter()
            .chain(self.diagnostics.iter())
        {
            let _ = writeln!(out, "diagnostic [{}] {}", d.severity.as_str(), d.message);
        }
        out
    }
}

fn derive_status(
    request: &VortexLocalCommitExecutionGateRequest,
) -> VortexLocalCommitExecutionGateStatus {
    use VortexLocalCommitExecutionGateSignal as S;
    if request.has_signal(S::ObjectStoreTarget) {
        return VortexLocalCommitExecutionGateStatus::BlockedByObjectStoreTarget;
    }
    if request.has_signal(S::UpstreamVortexWriteRequired) {
        return VortexLocalCommitExecutionGateStatus::BlockedByUpstreamVortexWrite;
    }
    if request.has_signal(S::CommitProtocolBlocked) || !request.has_signal(S::CommitProtocolReady) {
        return VortexLocalCommitExecutionGateStatus::BlockedByCommitProtocol;
    }
    if request.has_signal(S::CommitMarkerMissing) || !request.has_signal(S::CommitMarkerWritten) {
        return VortexLocalCommitExecutionGateStatus::BlockedByCommitMarker;
    }
    if request.has_signal(S::ManifestFinalizationBlocked)
        || !request.has_signal(S::ManifestFinalizationReady)
    {
        return VortexLocalCommitExecutionGateStatus::BlockedByManifestFinalization;
    }
    if request.has_signal(S::FinalizedManifestArtifactMissing)
        || !request.has_signal(S::FinalizedManifestArtifactWritten)
    {
        return VortexLocalCommitExecutionGateStatus::BlockedByFinalizedManifestArtifact;
    }
    if request.has_signal(S::OutputPayloadMissing) || !request.has_signal(S::OutputPayloadReady) {
        return VortexLocalCommitExecutionGateStatus::BlockedByOutputPayload;
    }
    if !request.has_signal(S::FeatureGateEnabled) {
        return VortexLocalCommitExecutionGateStatus::BlockedByFeatureGate;
    }
    VortexLocalCommitExecutionGateStatus::CommitExecutionReady
}

/// # Errors
/// Returns errors propagated from `VortexLocalCommitExecutionGateReport::from_request`.
pub fn plan_vortex_local_commit_execution_gate(
    request: VortexLocalCommitExecutionGateRequest,
) -> Result<VortexLocalCommitExecutionGateReport> {
    VortexLocalCommitExecutionGateReport::from_request(request)
}
#[must_use]
pub fn vortex_local_commit_execution_gate_is_side_effect_free(
    report: &VortexLocalCommitExecutionGateReport,
) -> bool {
    report.is_side_effect_free()
}

#[must_use]
pub fn local_commit_execution_gate_request_from_reports(
    target_uri: DatasetUri,
    commit_protocol: &VortexCommitProtocolReport,
    commit_marker: &VortexCommitMarkerWriteReport,
    manifest_finalization: &VortexManifestFinalizationReport,
    finalized_manifest_artifact: &VortexFinalizedManifestArtifactWriteReport,
) -> VortexLocalCommitExecutionGateRequest {
    let mut req = VortexLocalCommitExecutionGateRequest::new(target_uri)
        .with_commit_protocol_summary(commit_protocol.to_human_text())
        .with_commit_marker_summary(commit_marker.to_human_text())
        .with_manifest_finalization_summary(manifest_finalization.to_human_text())
        .with_finalized_manifest_artifact_summary(finalized_manifest_artifact.to_human_text())
        .with_output_payload_summary("output payload write path not implemented until Phase 12C")
        .output_payload_missing(true);
    req = req.commit_protocol_ready(
        commit_protocol.next_state() == VortexCommitProtocolState::CommitReady
            && !commit_protocol.has_errors(),
    );
    req = req.commit_protocol_blocked(
        commit_protocol.next_state() != VortexCommitProtocolState::CommitReady
            || commit_protocol.has_errors(),
    );
    req = req.commit_marker_written(commit_marker.commit_marker_written());
    req = req.commit_marker_missing(!commit_marker.commit_marker_written());
    req = req.manifest_finalization_ready(
        manifest_finalization.status == VortexManifestFinalizationStatus::FinalizationReady
            && !manifest_finalization.has_errors(),
    );
    req = req.manifest_finalization_blocked(
        manifest_finalization.status != VortexManifestFinalizationStatus::FinalizationReady
            || manifest_finalization.has_errors(),
    );
    req = req.finalized_manifest_artifact_written(
        finalized_manifest_artifact.finalized_manifest_artifact_written(),
    );
    req = req.finalized_manifest_artifact_missing(
        !finalized_manifest_artifact.finalized_manifest_artifact_written(),
    );
    req = req.object_store_target(
        commit_protocol.object_store_target()
            || manifest_finalization.object_store_target()
            || commit_marker
                .request
                .marker_ref
                .path_string()
                .starts_with("s3://")
            || finalized_manifest_artifact
                .request
                .finalized_manifest_ref
                .path_string()
                .starts_with("s3://"),
    );
    req.upstream_vortex_write_required(
        commit_protocol.upstream_vortex_write_called()
            || manifest_finalization.upstream_vortex_write_called()
            || finalized_manifest_artifact.upstream_vortex_write_called(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        VortexCommitMarkerFileRef, VortexCommitMarkerWriteRequest,
        VortexFinalizedManifestArtifactWriteRequest, VortexFinalizedManifestContent,
        VortexFinalizedManifestFileRef, VortexManifestFinalizationRequest,
        VortexStagedWorkspacePath,
    };
    fn uri() -> DatasetUri {
        DatasetUri::new("file:///tmp/gate.vortex").unwrap()
    }
    fn ws() -> VortexStagedWorkspacePath {
        VortexStagedWorkspacePath::new("/tmp/ws").unwrap()
    }
    fn base_req() -> VortexLocalCommitExecutionGateRequest {
        VortexLocalCommitExecutionGateRequest::new(uri())
    }
    #[test]
    fn basics() {
        assert!(
            !VortexLocalCommitExecutionGateStatus::CommitExecutionReady.allows_commit_execution()
        );
        assert!(VortexLocalCommitExecutionGateStatus::BlockedByOutputPayload.is_error());
        assert!(!VortexLocalCommitExecutionGateMode::ReportOnly.executes_commit());
        assert!(!VortexLocalCommitExecutionGateMode::ReportOnly.writes_output_data());
        let req = base_req()
            .add_signal(VortexLocalCommitExecutionGateSignal::LocalWorkspace, true)
            .add_signal(VortexLocalCommitExecutionGateSignal::LocalWorkspace, true)
            .add_signal(VortexLocalCommitExecutionGateSignal::LocalWorkspace, false);
        assert!(!req.has_signal(VortexLocalCommitExecutionGateSignal::LocalWorkspace));
    }
    #[test]
    fn priorities_and_ready() {
        let rep = VortexLocalCommitExecutionGateReport::from_request(
            base_req().object_store_target(true),
        )
        .unwrap();
        assert_eq!(
            rep.status,
            VortexLocalCommitExecutionGateStatus::BlockedByObjectStoreTarget
        );
        let rep = VortexLocalCommitExecutionGateReport::from_request(
            base_req().upstream_vortex_write_required(true),
        )
        .unwrap();
        assert_eq!(
            rep.status,
            VortexLocalCommitExecutionGateStatus::BlockedByUpstreamVortexWrite
        );
        let rep = VortexLocalCommitExecutionGateReport::from_request(base_req()).unwrap();
        assert_eq!(
            rep.status,
            VortexLocalCommitExecutionGateStatus::BlockedByCommitProtocol
        );
        let rep = VortexLocalCommitExecutionGateReport::from_request(
            base_req().commit_protocol_ready(true),
        )
        .unwrap();
        assert_eq!(
            rep.status,
            VortexLocalCommitExecutionGateStatus::BlockedByCommitMarker
        );
        let rep = VortexLocalCommitExecutionGateReport::from_request(
            base_req()
                .commit_protocol_ready(true)
                .commit_marker_written(true),
        )
        .unwrap();
        assert_eq!(
            rep.status,
            VortexLocalCommitExecutionGateStatus::BlockedByManifestFinalization
        );
        let rep = VortexLocalCommitExecutionGateReport::from_request(
            base_req()
                .commit_protocol_ready(true)
                .commit_marker_written(true)
                .manifest_finalization_ready(true),
        )
        .unwrap();
        assert_eq!(
            rep.status,
            VortexLocalCommitExecutionGateStatus::BlockedByFinalizedManifestArtifact
        );
        let rep = VortexLocalCommitExecutionGateReport::from_request(
            base_req()
                .commit_protocol_ready(true)
                .commit_marker_written(true)
                .manifest_finalization_ready(true)
                .finalized_manifest_artifact_written(true),
        )
        .unwrap();
        assert_eq!(
            rep.status,
            VortexLocalCommitExecutionGateStatus::BlockedByOutputPayload
        );
        let rep = VortexLocalCommitExecutionGateReport::from_request(
            base_req()
                .commit_protocol_ready(true)
                .commit_marker_written(true)
                .manifest_finalization_ready(true)
                .finalized_manifest_artifact_written(true)
                .output_payload_ready(true)
                .feature_gate_enabled(true),
        )
        .unwrap();
        assert_eq!(
            rep.status,
            VortexLocalCommitExecutionGateStatus::CommitExecutionReady
        );
        assert!(!rep.allows_commit_execution());
        assert!(rep.is_side_effect_free());
        assert!(rep.to_human_text().contains("fallback execution disabled"));
    }
    #[test]
    fn report_flags_and_diagnostics() {
        let mut rep = VortexLocalCommitExecutionGateReport::from_request(
            base_req()
                .commit_protocol_ready(true)
                .commit_marker_written(true)
                .manifest_finalization_ready(true)
                .finalized_manifest_artifact_written(true)
                .output_payload_missing(true),
        )
        .unwrap();
        rep.add_diagnostic(Diagnostic::invalid_input("gate", "bad", "fix"));
        assert!(
            !rep.commit_executed()
                && !rep.manifest_committed()
                && !rep.output_data_written()
                && !rep.object_store_io()
                && !rep.upstream_vortex_write_called()
                && !rep.recovery_action_executed()
                && !rep.fallback_execution_allowed()
        );
        assert!(rep.to_human_text().contains("output payload ready: false"));
        assert!(rep.to_human_text().contains("diagnostic"));
        assert!(plan_vortex_local_commit_execution_gate(base_req()).is_ok());
    }
    #[test]
    fn from_reports_output_missing() {
        let cp = VortexCommitProtocolReport::from_request(
            crate::VortexCommitProtocolRequest::new(
                uri(),
                VortexCommitProtocolState::AwaitingCommitMarker,
                crate::VortexCommitProtocolTransition::MarkCommitReady,
            )
            .commit_intent_ready(true)
            .draft_manifest_ready(true)
            .manifest_finalization_available(true)
            .commit_marker_available(true)
            .recovery_ready(true)
            .feature_gate_enabled(true),
        )
        .unwrap();
        let cm = VortexCommitMarkerWriteReport::from_request(
            VortexCommitMarkerWriteRequest::new(
                VortexCommitMarkerFileRef::default_for_workspace(ws()),
                crate::VortexCommitMarkerContent::new("ok").unwrap(),
            )
            .marker_plan_ready(true)
            .feature_gate_ready(true)
            .object_store_target(false),
        )
        .unwrap();
        let mf = VortexManifestFinalizationReport::from_request(
            VortexManifestFinalizationRequest::new(
                uri(),
                VortexFinalizedManifestFileRef::default_for_workspace(ws()),
                VortexFinalizedManifestContent::new("x").unwrap(),
            )
            .draft_manifest_written(true)
            .commit_marker_written(true)
            .commit_protocol_ready(true)
            .schema_known(true)
            .schema_compatible(true)
            .delete_semantics_known(true)
            .tombstone_semantics_known(true)
            .feature_gate_enabled(true),
        )
        .unwrap();
        let fa = VortexFinalizedManifestArtifactWriteReport::from_request(
            VortexFinalizedManifestArtifactWriteRequest::new(
                VortexFinalizedManifestFileRef::default_for_workspace(ws()),
                VortexFinalizedManifestContent::new("x").unwrap(),
            )
            .feature_gate_ready(true),
        )
        .unwrap();
        let req = local_commit_execution_gate_request_from_reports(uri(), &cp, &cm, &mf, &fa);
        assert!(req.has_signal(VortexLocalCommitExecutionGateSignal::OutputPayloadMissing));
        let rep = VortexLocalCommitExecutionGateReport::from_request(req).unwrap();
        assert!(!rep.manifest_committed());
    }
}
