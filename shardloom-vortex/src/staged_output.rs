use std::fmt::Write as _;

use shardloom_core::{
    DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, Result, ShardLoomError, UriScheme,
};

use crate::{VortexWriteIntentReport, VortexWriteIntentStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStagedOutputStatus {
    Planned,
    WorkspaceRequired,
    WorkspaceKnown,
    BlockedByMissingWorkspace,
    BlockedByObjectStoreTarget,
    BlockedByWriteIntent,
    BlockedByCommitProtocol,
    BlockedByFeatureGate,
    Unsupported,
}
impl VortexStagedOutputStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::WorkspaceRequired => "workspace_required",
            Self::WorkspaceKnown => "workspace_known",
            Self::BlockedByMissingWorkspace => "blocked_by_missing_workspace",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByWriteIntent => "blocked_by_write_intent",
            Self::BlockedByCommitProtocol => "blocked_by_commit_protocol",
            Self::BlockedByFeatureGate => "blocked_by_feature_gate",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(
            self,
            Self::Planned | Self::WorkspaceRequired | Self::WorkspaceKnown
        )
    }
    #[must_use]
    pub const fn allows_workspace_creation(self) -> bool {
        false
    }
    #[must_use]
    pub const fn allows_output_write(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStagedOutputMode {
    ReportOnly,
    WorkspacePlanning,
    Unsupported,
}
impl VortexStagedOutputMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::WorkspacePlanning => "workspace_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn creates_workspace(self) -> bool {
        false
    }
    #[must_use]
    pub const fn writes_output_data(self) -> bool {
        false
    }
    #[must_use]
    pub const fn writes_marker(self) -> bool {
        false
    }
    #[must_use]
    pub const fn writes_manifest(self) -> bool {
        false
    }
    #[must_use]
    pub const fn writes_object_store(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStagedOutputSignal {
    WriteIntentPlanned,
    WriteIntentBlocked,
    WorkspaceRequired,
    WorkspacePathKnown,
    LocalWorkspace,
    ObjectStoreWorkspace,
    CommitProtocolAvailable,
    FeatureGateEnabled,
}
impl VortexStagedOutputSignal {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WriteIntentPlanned => "write_intent_planned",
            Self::WriteIntentBlocked => "write_intent_blocked",
            Self::WorkspaceRequired => "workspace_required",
            Self::WorkspacePathKnown => "workspace_path_known",
            Self::LocalWorkspace => "local_workspace",
            Self::ObjectStoreWorkspace => "object_store_workspace",
            Self::CommitProtocolAvailable => "commit_protocol_available",
            Self::FeatureGateEnabled => "feature_gate_enabled",
        }
    }
    #[must_use]
    pub const fn is_blocking(self) -> bool {
        matches!(self, Self::WriteIntentBlocked | Self::ObjectStoreWorkspace)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStagedOutputEffect {
    WorkspaceCreated,
    MarkerWritten,
    OutputDataWritten,
    ManifestWritten,
    ObjectStoreIo,
    FallbackExecution,
}
impl VortexStagedOutputEffect {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WorkspaceCreated => "workspace_created",
            Self::MarkerWritten => "marker_written",
            Self::OutputDataWritten => "output_data_written",
            Self::ManifestWritten => "manifest_written",
            Self::ObjectStoreIo => "object_store_io",
            Self::FallbackExecution => "fallback_execution",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexStagedWorkspaceId(String);
impl VortexStagedWorkspaceId {
    /// # Errors
    /// Returns an error when the workspace id is empty or contains invalid traversal/path separator content.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let t = value.trim();
        if t.is_empty() || t.contains('/') || t.contains('\\') || t.contains("..") {
            return Err(ShardLoomError::InvalidOperation(
                "invalid staged workspace id".to_string(),
            ));
        }
        Ok(Self(t.to_string()))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!("workspace_id={}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexStagedWorkspacePath(String);
impl VortexStagedWorkspacePath {
    /// # Errors
    /// Returns an error when the workspace path is empty or contains a NUL byte.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let t = value.trim();
        if t.is_empty() || value.contains('\0') {
            return Err(ShardLoomError::InvalidOperation(
                "invalid staged workspace path".to_string(),
            ));
        }
        Ok(Self(t.to_string()))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!("workspace_path={}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexStagedOutputRequest {
    pub workspace_id: VortexStagedWorkspaceId,
    pub workspace_path: Option<VortexStagedWorkspacePath>,
    pub target_uri: DatasetUri,
    pub signals: Vec<VortexStagedOutputSignal>,
    pub write_intent_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexStagedOutputRequest {
    #[must_use]
    pub fn new(workspace_id: VortexStagedWorkspaceId, target_uri: DatasetUri) -> Self {
        Self {
            workspace_id,
            workspace_path: None,
            target_uri,
            signals: Vec::new(),
            write_intent_summary: None,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn with_workspace_path(mut self, workspace_path: VortexStagedWorkspacePath) -> Self {
        self.workspace_path = Some(workspace_path);
        self.add_signal(VortexStagedOutputSignal::WorkspacePathKnown, true);
        self
    }
    #[must_use]
    pub fn with_write_intent_summary(mut self, summary: impl Into<String>) -> Self {
        self.write_intent_summary = Some(summary.into());
        self
    }
    pub fn add_signal(&mut self, signal: VortexStagedOutputSignal, value: bool) {
        if value {
            if !self.signals.contains(&signal) {
                self.signals.push(signal);
            }
        } else {
            self.signals.retain(|s| *s != signal);
        }
    }
    #[must_use]
    pub fn write_intent_planned(mut self, value: bool) -> Self {
        self.add_signal(VortexStagedOutputSignal::WriteIntentPlanned, value);
        self
    }
    #[must_use]
    pub fn write_intent_blocked(mut self, value: bool) -> Self {
        self.add_signal(VortexStagedOutputSignal::WriteIntentBlocked, value);
        self
    }
    #[must_use]
    pub fn workspace_required(mut self, value: bool) -> Self {
        self.add_signal(VortexStagedOutputSignal::WorkspaceRequired, value);
        self
    }
    #[must_use]
    pub fn workspace_path_known(mut self, value: bool) -> Self {
        self.add_signal(VortexStagedOutputSignal::WorkspacePathKnown, value);
        self
    }
    #[must_use]
    pub fn local_workspace(mut self, value: bool) -> Self {
        self.add_signal(VortexStagedOutputSignal::LocalWorkspace, value);
        self
    }
    #[must_use]
    pub fn object_store_workspace(mut self, value: bool) -> Self {
        self.add_signal(VortexStagedOutputSignal::ObjectStoreWorkspace, value);
        self
    }
    #[must_use]
    pub fn commit_protocol_available(mut self, value: bool) -> Self {
        self.add_signal(VortexStagedOutputSignal::CommitProtocolAvailable, value);
        self
    }
    #[must_use]
    pub fn feature_gate_enabled(mut self, value: bool) -> Self {
        self.add_signal(VortexStagedOutputSignal::FeatureGateEnabled, value);
        self
    }
    #[must_use]
    pub fn has_signal(&self, signal: VortexStagedOutputSignal) -> bool {
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
            "workspace_id={} target_uri={} signals={}",
            self.workspace_id.as_str(),
            self.target_uri.as_str(),
            self.signals.len()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexStagedOutputReport {
    pub status: VortexStagedOutputStatus,
    pub mode: VortexStagedOutputMode,
    pub request: VortexStagedOutputRequest,
    pub effects_performed: Vec<VortexStagedOutputEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexStagedOutputReport {
    /// # Errors
    /// Returns an error only if diagnostic rendering fails unexpectedly.
    pub fn from_request(request: VortexStagedOutputRequest) -> Result<Self> {
        let mut report = Self {
            status: VortexStagedOutputStatus::Planned,
            mode: VortexStagedOutputMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        };
        if report.object_store_workspace() {
            report.status = VortexStagedOutputStatus::BlockedByObjectStoreTarget;
        } else if report
            .request
            .has_signal(VortexStagedOutputSignal::WriteIntentBlocked)
        {
            report.status = VortexStagedOutputStatus::BlockedByWriteIntent;
        } else if report.workspace_required() && !report.workspace_path_known() {
            report.status = VortexStagedOutputStatus::BlockedByMissingWorkspace;
        } else if !report.commit_protocol_available() {
            report.status = VortexStagedOutputStatus::BlockedByCommitProtocol;
        } else if report.workspace_path_known() {
            report.status = VortexStagedOutputStatus::WorkspaceKnown;
            report.mode = VortexStagedOutputMode::WorkspacePlanning;
        } else if report.workspace_required() {
            report.status = VortexStagedOutputStatus::WorkspaceRequired;
            report.mode = VortexStagedOutputMode::WorkspacePlanning;
        }
        Ok(report)
    }
    #[must_use]
    pub fn unsupported(
        request: VortexStagedOutputRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self {
            status: VortexStagedOutputStatus::Unsupported,
            mode: VortexStagedOutputMode::Unsupported,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        };
        report.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::UnsupportedOutputFormat,
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
    pub fn workspace_required(&self) -> bool {
        self.request
            .has_signal(VortexStagedOutputSignal::WorkspaceRequired)
    }
    #[must_use]
    pub fn workspace_path_known(&self) -> bool {
        self.request.workspace_path.is_some()
            || self
                .request
                .has_signal(VortexStagedOutputSignal::WorkspacePathKnown)
    }
    #[must_use]
    pub fn local_workspace(&self) -> bool {
        self.request
            .has_signal(VortexStagedOutputSignal::LocalWorkspace)
    }
    #[must_use]
    pub fn object_store_workspace(&self) -> bool {
        self.request
            .has_signal(VortexStagedOutputSignal::ObjectStoreWorkspace)
    }
    #[must_use]
    pub fn commit_protocol_available(&self) -> bool {
        self.request
            .has_signal(VortexStagedOutputSignal::CommitProtocolAvailable)
    }
    #[must_use]
    pub const fn workspace_created(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn marker_written(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn output_data_written(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn manifest_written(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn object_store_io(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn allows_workspace_creation(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn allows_output_write(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty() && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut t = String::new();
        let _ = writeln!(t, "Vortex staged output plan");
        let _ = writeln!(t, "status: {}", self.status.as_str());
        let _ = writeln!(t, "mode: {}", self.mode.as_str());
        let _ = writeln!(t, "workspace id: {}", self.request.workspace_id.as_str());
        let _ = writeln!(t, "workspace path known: {}", self.workspace_path_known());
        let _ = writeln!(t, "workspace required: {}", self.workspace_required());
        let _ = writeln!(t, "local workspace: {}", self.local_workspace());
        let _ = writeln!(
            t,
            "object-store workspace: {}",
            self.object_store_workspace()
        );
        let _ = writeln!(
            t,
            "commit protocol available: {}",
            self.commit_protocol_available()
        );
        let _ = writeln!(t, "workspace created: false");
        let _ = writeln!(t, "marker written: false");
        let _ = writeln!(t, "output data written: false");
        let _ = writeln!(t, "manifest written: false");
        let _ = writeln!(t, "object-store IO: false");
        let _ = write!(t, "fallback execution: disabled");
        if self.request.diagnostics.is_empty() && self.diagnostics.is_empty() {
            let _ = write!(t, "\ndiagnostics: none");
        } else {
            let _ = write!(t, "\ndiagnostics:");
            for d in self
                .request
                .diagnostics
                .iter()
                .chain(self.diagnostics.iter())
            {
                let _ = write!(t, "\n- {}", d.to_human_text());
            }
        }
        t
    }
}

/// # Errors
/// Propagates errors from `VortexStagedOutputReport::from_request`.
pub fn plan_vortex_staged_output(
    request: VortexStagedOutputRequest,
) -> Result<VortexStagedOutputReport> {
    VortexStagedOutputReport::from_request(request)
}

#[must_use]
pub fn vortex_staged_output_is_side_effect_free(report: &VortexStagedOutputReport) -> bool {
    report.is_side_effect_free()
}

#[must_use]
pub fn staged_output_request_from_write_intent(
    workspace_id: VortexStagedWorkspaceId,
    target_uri: DatasetUri,
    write_intent: &VortexWriteIntentReport,
) -> VortexStagedOutputRequest {
    let mut request = VortexStagedOutputRequest::new(workspace_id, target_uri)
        .write_intent_planned(true)
        .with_write_intent_summary(write_intent.to_human_text());
    if write_intent.has_errors() {
        request = request.write_intent_blocked(true);
    }
    if matches!(
        write_intent.status,
        VortexWriteIntentStatus::StagedOutputRequired
    ) {
        request = request.workspace_required(true);
    }
    if write_intent.commit_protocol_available() {
        request = request.commit_protocol_available(true);
    }
    if write_intent.object_store_target() {
        request = request.object_store_workspace(true);
    }
    if matches!(
        request.target_uri.scheme(),
        UriScheme::File | UriScheme::LocalPath
    ) {
        request = request.local_workspace(true);
    }
    request
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{DiagnosticCategory, FallbackStatus};
    fn base() -> VortexStagedOutputRequest {
        VortexStagedOutputRequest::new(
            VortexStagedWorkspaceId::new("ws1").unwrap(),
            DatasetUri::new("file://tmp/out.vortex").unwrap(),
        )
        .workspace_required(true)
    }
    #[test]
    fn status_planned_workspace_creation_false() {
        assert!(!VortexStagedOutputStatus::Planned.allows_workspace_creation());
    }
    #[test]
    fn status_missing_workspace_is_error() {
        assert!(VortexStagedOutputStatus::BlockedByMissingWorkspace.is_error());
    }
    #[test]
    fn mode_report_only_no_workspace_or_write() {
        assert!(!VortexStagedOutputMode::ReportOnly.creates_workspace());
        assert!(!VortexStagedOutputMode::ReportOnly.writes_output_data());
    }
    #[test]
    fn workspace_id_rejects_empty() {
        assert!(VortexStagedWorkspaceId::new("   ").is_err());
    }
    #[test]
    fn workspace_id_rejects_slash() {
        assert!(VortexStagedWorkspaceId::new("a/b").is_err());
    }
    #[test]
    fn workspace_id_rejects_dotdot() {
        assert!(VortexStagedWorkspaceId::new("a..b").is_err());
    }
    #[test]
    fn workspace_path_rejects_empty() {
        assert!(VortexStagedWorkspacePath::new("   ").is_err());
    }
    #[test]
    fn workspace_path_rejects_nul() {
        assert!(VortexStagedWorkspacePath::new("a\0b").is_err());
    }
    #[test]
    fn with_workspace_path_sets_signal() {
        let req = base().with_workspace_path(VortexStagedWorkspacePath::new("tmp/ws").unwrap());
        assert!(req.has_signal(VortexStagedOutputSignal::WorkspacePathKnown));
    }
    #[test]
    fn report_priority_object_store() {
        let rep =
            VortexStagedOutputReport::from_request(base().object_store_workspace(true)).unwrap();
        assert_eq!(
            rep.status,
            VortexStagedOutputStatus::BlockedByObjectStoreTarget
        );
    }
    #[test]
    fn report_priority_write_intent_blocked() {
        let rep =
            VortexStagedOutputReport::from_request(base().write_intent_blocked(true)).unwrap();
        assert_eq!(rep.status, VortexStagedOutputStatus::BlockedByWriteIntent);
    }
    #[test]
    fn report_missing_workspace() {
        let rep = VortexStagedOutputReport::from_request(base()).unwrap();
        assert_eq!(
            rep.status,
            VortexStagedOutputStatus::BlockedByMissingWorkspace
        );
    }
    #[test]
    fn report_missing_commit_protocol() {
        let rep = VortexStagedOutputReport::from_request(
            base().with_workspace_path(VortexStagedWorkspacePath::new("tmp/ws").unwrap()),
        )
        .unwrap();
        assert_eq!(
            rep.status,
            VortexStagedOutputStatus::BlockedByCommitProtocol
        );
    }
    #[test]
    fn report_workspace_known() {
        let rep = VortexStagedOutputReport::from_request(
            base()
                .with_workspace_path(VortexStagedWorkspacePath::new("tmp/ws").unwrap())
                .commit_protocol_available(true),
        )
        .unwrap();
        assert_eq!(rep.status, VortexStagedOutputStatus::WorkspaceKnown);
    }
    #[test]
    fn report_effects_false_and_side_effect_free() {
        let rep = VortexStagedOutputReport::from_request(
            base()
                .with_workspace_path(VortexStagedWorkspacePath::new("tmp/ws").unwrap())
                .commit_protocol_available(true),
        )
        .unwrap();
        assert!(!rep.workspace_created());
        assert!(!rep.marker_written());
        assert!(!rep.output_data_written());
        assert!(!rep.manifest_written());
        assert!(!rep.object_store_io());
        assert!(!rep.fallback_execution_allowed());
        assert!(rep.is_side_effect_free());
    }
    #[test]
    fn human_text_includes_flags_and_diagnostics() {
        let mut req = base()
            .with_workspace_path(VortexStagedWorkspacePath::new("tmp/ws").unwrap())
            .commit_protocol_available(true);
        req.add_diagnostic(Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            "bad",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        ));
        let txt = VortexStagedOutputReport::from_request(req)
            .unwrap()
            .to_human_text();
        assert!(txt.contains("fallback execution: disabled"));
        assert!(txt.contains("output data written: false"));
        assert!(txt.contains("[error]"));
    }
    #[test]
    fn plan_helper_no_io() {
        let rep = plan_vortex_staged_output(
            base()
                .with_workspace_path(VortexStagedWorkspacePath::new("tmp/ws").unwrap())
                .commit_protocol_available(true),
        )
        .unwrap();
        assert!(rep.is_side_effect_free());
    }
    #[test]
    fn from_write_intent_maps_workspace_required() {
        let intent_request =
            crate::VortexWriteIntentRequest::new(DatasetUri::new("file://tmp/out.vortex").unwrap())
                .target_is_native_vortex(true)
                .schema_known(true)
                .schema_compatible(true)
                .delete_semantics_known(true)
                .tombstone_semantics_known(true)
                .staged_output_required(true)
                .commit_protocol_available(true);
        let intent_report = crate::VortexWriteIntentReport::from_request(intent_request).unwrap();
        let req = staged_output_request_from_write_intent(
            VortexStagedWorkspaceId::new("ws").unwrap(),
            DatasetUri::new("file://tmp/out.vortex").unwrap(),
            &intent_report,
        );
        assert!(req.has_signal(VortexStagedOutputSignal::WorkspaceRequired));
    }

    #[test]
    fn from_write_intent_preserves_blocked_state() {
        let intent_request =
            crate::VortexWriteIntentRequest::new(DatasetUri::new("file://tmp/out.vortex").unwrap())
                .target_is_native_vortex(true)
                .schema_known(true)
                .schema_compatible(true)
                .delete_semantics_known(true)
                .tombstone_semantics_known(true)
                .staged_output_required(true);
        let intent_report = crate::VortexWriteIntentReport::from_request(intent_request).unwrap();
        let req = staged_output_request_from_write_intent(
            VortexStagedWorkspaceId::new("ws").unwrap(),
            DatasetUri::new("file://tmp/out.vortex").unwrap(),
            &intent_report,
        );
        assert!(req.has_signal(VortexStagedOutputSignal::WriteIntentBlocked));
    }
}
