use std::fmt::Write as _;

use shardloom_core::{
    Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity, FallbackStatus, Result,
    ShardLoomError,
};

fn invalid_operation(message: impl Into<String>) -> ShardLoomError {
    ShardLoomError::InvalidOperation(message.into())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillLifecycleStatus {
    Planned,
    WorkspaceReady,
    CleanupPlanned,
    CleanupRequired,
    CleanupCompleted,
    FeatureDisabled,
    BlockedByPolicy,
    BlockedByMissingPath,
    Unsupported,
}
impl SpillLifecycleStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::WorkspaceReady => "workspace_ready",
            Self::CleanupPlanned => "cleanup_planned",
            Self::CleanupRequired => "cleanup_required",
            Self::CleanupCompleted => "cleanup_completed",
            Self::FeatureDisabled => "feature_disabled",
            Self::BlockedByPolicy => "blocked_by_policy",
            Self::BlockedByMissingPath => "blocked_by_missing_path",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::BlockedByPolicy | Self::BlockedByMissingPath | Self::Unsupported
        )
    }
    #[must_use]
    pub const fn requires_cleanup(&self) -> bool {
        matches!(self, Self::CleanupRequired)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillLifecycleMode {
    ReportOnly,
    LocalWorkspace,
    CleanupOnly,
    Unsupported,
}
impl SpillLifecycleMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::LocalWorkspace => "local_workspace",
            Self::CleanupOnly => "cleanup_only",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn writes_spill_data(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn reads_spill_data(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn touches_filesystem(&self) -> bool {
        matches!(self, Self::LocalWorkspace | Self::CleanupOnly)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillIoState {
    No,
    Yes,
}
impl SpillIoState {
    #[must_use]
    pub const fn as_bool(self) -> bool {
        matches!(self, Self::Yes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SpillWorkspaceId(String);
impl SpillWorkspaceId {
    /// # Errors
    /// Returns an error when the id is empty or whitespace.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(invalid_operation("spill workspace id must not be empty"));
        }
        Ok(Self(value))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SpillWorkspacePath(String);
impl SpillWorkspacePath {
    /// # Errors
    /// Returns an error when the path is empty or whitespace.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(invalid_operation("spill workspace path must not be empty"));
        }
        Ok(Self(value))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillLifecycleRequest {
    pub workspace_id: SpillWorkspaceId,
    pub workspace_path: SpillWorkspacePath,
    pub allow_filesystem_side_effects: bool,
    pub allow_cleanup: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl SpillLifecycleRequest {
    #[must_use]
    pub fn report_only(workspace_id: SpillWorkspaceId, workspace_path: SpillWorkspacePath) -> Self {
        Self {
            workspace_id,
            workspace_path,
            allow_filesystem_side_effects: false,
            allow_cleanup: false,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn local_workspace(
        workspace_id: SpillWorkspaceId,
        workspace_path: SpillWorkspacePath,
    ) -> Self {
        Self {
            workspace_id,
            workspace_path,
            allow_filesystem_side_effects: true,
            allow_cleanup: false,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn cleanup_only(
        workspace_id: SpillWorkspaceId,
        workspace_path: SpillWorkspacePath,
    ) -> Self {
        Self {
            workspace_id,
            workspace_path,
            allow_filesystem_side_effects: true,
            allow_cleanup: true,
            diagnostics: vec![],
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
        format!(
            "workspace_id={}, workspace_path={}, fs_side_effects={}, cleanup={}",
            self.workspace_id.as_str(),
            self.workspace_path.as_str(),
            self.allow_filesystem_side_effects,
            self.allow_cleanup
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillCleanupActionKind {
    RemoveEmptyWorkspace,
    RemoveMarkerFile,
    VerifyNoPayloadFiles,
    ReportOnly,
    Unsupported,
}
impl SpillCleanupActionKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::RemoveEmptyWorkspace => "remove_empty_workspace",
            Self::RemoveMarkerFile => "remove_marker_file",
            Self::VerifyNoPayloadFiles => "verify_no_payload_files",
            Self::ReportOnly => "report_only",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn touches_filesystem(&self) -> bool {
        matches!(
            self,
            Self::RemoveEmptyWorkspace | Self::RemoveMarkerFile | Self::VerifyNoPayloadFiles
        )
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillCleanupAction {
    pub kind: SpillCleanupActionKind,
    pub target: String,
    pub diagnostics: Vec<Diagnostic>,
}
impl SpillCleanupAction {
    #[must_use]
    pub fn report_only(target: impl Into<String>) -> Self {
        Self {
            kind: SpillCleanupActionKind::ReportOnly,
            target: target.into(),
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn verify_no_payload_files(target: impl Into<String>) -> Self {
        Self {
            kind: SpillCleanupActionKind::VerifyNoPayloadFiles,
            target: target.into(),
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn remove_empty_workspace(target: impl Into<String>) -> Self {
        Self {
            kind: SpillCleanupActionKind::RemoveEmptyWorkspace,
            target: target.into(),
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn remove_marker_file(target: impl Into<String>) -> Self {
        Self {
            kind: SpillCleanupActionKind::RemoveMarkerFile,
            target: target.into(),
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn unsupported(
        target: impl Into<String>,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self {
            kind: SpillCleanupActionKind::Unsupported,
            target: target.into(),
            diagnostics: vec![],
        };
        s.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("No fallback execution is allowed.".to_string()),
        ));
        s
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
        format!("kind={}, target={}", self.kind.as_str(), self.target)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillCleanupPlan {
    pub actions: Vec<SpillCleanupAction>,
    pub diagnostics: Vec<Diagnostic>,
}
impl SpillCleanupPlan {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            actions: vec![],
            diagnostics: vec![],
        }
    }
    pub fn add_action(&mut self, action: SpillCleanupAction) {
        self.actions.push(action);
    }
    #[must_use]
    pub fn action_count(&self) -> usize {
        self.actions.len()
    }
    #[must_use]
    pub fn requires_filesystem(&self) -> bool {
        self.actions.iter().any(|a| a.kind.touches_filesystem())
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .chain(self.actions.iter().flat_map(|a| a.diagnostics.iter()))
            .any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(&mut out, "cleanup actions: {}", self.actions.len());
        for action in &self.actions {
            let _ = writeln!(&mut out, "- {}", action.summary());
        }
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpillLifecycleReport {
    pub status: SpillLifecycleStatus,
    pub mode: SpillLifecycleMode,
    pub request: SpillLifecycleRequest,
    pub cleanup_plan: SpillCleanupPlan,
    pub workspace_created: SpillIoState,
    pub marker_created: SpillIoState,
    pub cleanup_performed: SpillIoState,
    pub spill_data_written: SpillIoState,
    pub spill_data_read: SpillIoState,
    pub object_store_io: SpillIoState,
    pub fallback_execution_allowed: SpillIoState,
    pub diagnostics: Vec<Diagnostic>,
}
impl SpillLifecycleReport {
    #[must_use]
    pub fn planned(request: SpillLifecycleRequest) -> Self {
        Self {
            status: SpillLifecycleStatus::Planned,
            mode: SpillLifecycleMode::ReportOnly,
            request,
            cleanup_plan: SpillCleanupPlan::empty(),
            workspace_created: SpillIoState::No,
            marker_created: SpillIoState::No,
            cleanup_performed: SpillIoState::No,
            spill_data_written: SpillIoState::No,
            spill_data_read: SpillIoState::No,
            object_store_io: SpillIoState::No,
            fallback_execution_allowed: SpillIoState::No,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn feature_disabled(request: SpillLifecycleRequest) -> Self {
        Self {
            status: SpillLifecycleStatus::FeatureDisabled,
            mode: if request.allow_cleanup {
                SpillLifecycleMode::CleanupOnly
            } else {
                SpillLifecycleMode::LocalWorkspace
            },
            request,
            cleanup_plan: SpillCleanupPlan::empty(),
            workspace_created: SpillIoState::No,
            marker_created: SpillIoState::No,
            cleanup_performed: SpillIoState::No,
            spill_data_written: SpillIoState::No,
            spill_data_read: SpillIoState::No,
            object_store_io: SpillIoState::No,
            fallback_execution_allowed: SpillIoState::No,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn workspace_ready(request: SpillLifecycleRequest, cleanup_plan: SpillCleanupPlan) -> Self {
        let mut s = Self::planned(request);
        s.status = SpillLifecycleStatus::WorkspaceReady;
        s.mode = SpillLifecycleMode::LocalWorkspace;
        s.cleanup_plan = cleanup_plan;
        s.workspace_created = SpillIoState::Yes;
        s
    }
    #[must_use]
    pub fn cleanup_planned(request: SpillLifecycleRequest, cleanup_plan: SpillCleanupPlan) -> Self {
        let mut s = Self::planned(request);
        s.status = SpillLifecycleStatus::CleanupPlanned;
        s.mode = SpillLifecycleMode::CleanupOnly;
        s.cleanup_plan = cleanup_plan;
        s
    }
    #[must_use]
    pub fn unsupported(
        request: SpillLifecycleRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::planned(request);
        s.status = SpillLifecycleStatus::Unsupported;
        s.mode = SpillLifecycleMode::Unsupported;
        s.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("No fallback execution is allowed.".to_string()),
        ));
        s
    }
    /// # Errors
    /// Returns an error when request validation fails.
    pub fn from_request(request: SpillLifecycleRequest) -> Result<Self> {
        if !request.allow_filesystem_side_effects {
            return Ok(Self::planned(request));
        }
        if !spill_lifecycle_feature_enabled() {
            let mut r = Self::feature_disabled(request);
            r.add_diagnostic(Diagnostic::new(DiagnosticCode::ConfigurationError,DiagnosticSeverity::Warning,DiagnosticCategory::Configuration,"spill lifecycle filesystem side effects disabled by feature gate",Some("spill-lifecycle-fs".to_string()),Some("Feature disabled in current build.".to_string()),Some("Rebuild with --features spill-lifecycle-fs to enable local lifecycle setup/cleanup.".to_string()),FallbackStatus::disabled_by_policy()));
            return Ok(r);
        }
        #[cfg(feature = "spill-lifecycle-fs")]
        {
            return Ok(execute_request(request));
        }
        #[allow(unreachable_code)]
        Ok(Self::feature_disabled(request))
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
    pub const fn is_side_effect_free(&self) -> bool {
        !self.workspace_created.as_bool()
            && !self.marker_created.as_bool()
            && !self.cleanup_performed.as_bool()
            && !self.spill_data_written.as_bool()
            && !self.spill_data_read.as_bool()
            && !self.object_store_io.as_bool()
            && !self.fallback_execution_allowed.as_bool()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(&mut out, "lifecycle status: {}", self.status.as_str());
        let _ = writeln!(&mut out, "mode: {}", self.mode.as_str());
        let _ = writeln!(
            &mut out,
            "workspace id: {}",
            self.request.workspace_id.as_str()
        );
        let _ = writeln!(
            &mut out,
            "workspace path: {}",
            self.request.workspace_path.as_str()
        );
        let _ = writeln!(
            &mut out,
            "cleanup action count: {}",
            self.cleanup_plan.action_count()
        );
        let _ = writeln!(
            &mut out,
            "workspace created: {}",
            self.workspace_created.as_bool()
        );
        let _ = writeln!(
            &mut out,
            "marker created: {}",
            self.marker_created.as_bool()
        );
        let _ = writeln!(
            &mut out,
            "cleanup performed: {}",
            self.cleanup_performed.as_bool()
        );
        let _ = writeln!(&mut out, "spill data written: false");
        let _ = writeln!(&mut out, "spill data read: false");
        let _ = writeln!(&mut out, "object-store IO: false");
        let _ = writeln!(&mut out, "fallback execution disabled: true");
        for d in &self.diagnostics {
            let _ = writeln!(&mut out, "diagnostic: {}", d.message);
        }
        out
    }
}

#[cfg(feature = "spill-lifecycle-fs")]
fn execute_request(request: SpillLifecycleRequest) -> SpillLifecycleReport {
    let workspace = request.workspace_path.as_str();
    let marker = format!("{workspace}/.shardloom-spill-lifecycle");
    let mut plan = SpillCleanupPlan::empty();
    plan.add_action(SpillCleanupAction::verify_no_payload_files(
        workspace.to_string(),
    ));
    plan.add_action(SpillCleanupAction::remove_marker_file(marker.clone()));
    plan.add_action(SpillCleanupAction::remove_empty_workspace(
        workspace.to_string(),
    ));
    if request.allow_cleanup {
        let mut report = SpillLifecycleReport::cleanup_planned(request, plan);
        if std::path::Path::new(&marker).exists() {
            let _ = std::fs::remove_file(&marker);
            report.cleanup_performed = SpillIoState::Yes;
        }
        if std::path::Path::new(workspace).exists()
            && std::fs::read_dir(workspace).is_ok_and(|mut it| it.next().is_none())
        {
            let _ = std::fs::remove_dir(workspace);
            report.cleanup_performed = SpillIoState::Yes;
        }
        report
    } else {
        let mut report = SpillLifecycleReport::workspace_ready(request, plan);
        let _ = std::fs::create_dir_all(workspace);
        report.workspace_created = SpillIoState::Yes;
        let _ = std::fs::write(marker, b"lifecycle-marker");
        report.marker_created = SpillIoState::Yes;
        report
    }
}

/// # Errors
pub fn plan_spill_lifecycle(request: SpillLifecycleRequest) -> Result<SpillLifecycleReport> {
    SpillLifecycleReport::from_request(request)
}
#[must_use]
pub const fn spill_lifecycle_feature_enabled() -> bool {
    cfg!(feature = "spill-lifecycle-fs")
}

#[cfg(test)]
mod tests {
    use super::*;
    fn req() -> SpillLifecycleRequest {
        SpillLifecycleRequest::report_only(
            SpillWorkspaceId::new("id").expect("id"),
            SpillWorkspacePath::new("/tmp/x").expect("path"),
        )
    }
    #[test]
    fn workspace_id_rejects_empty() {
        assert!(SpillWorkspaceId::new(" ").is_err());
    }
    #[test]
    fn workspace_path_rejects_empty() {
        assert!(SpillWorkspacePath::new(" ").is_err());
    }
    #[test]
    fn status_cleanup_required_requires_cleanup() {
        assert!(SpillLifecycleStatus::CleanupRequired.requires_cleanup());
    }
    #[test]
    fn status_unsupported_is_error() {
        assert!(SpillLifecycleStatus::Unsupported.is_error());
    }
    #[test]
    fn mode_report_only_no_fs() {
        assert!(!SpillLifecycleMode::ReportOnly.touches_filesystem());
    }
    #[test]
    fn mode_local_workspace_no_data_rw() {
        assert!(
            !SpillLifecycleMode::LocalWorkspace.writes_spill_data()
                && !SpillLifecycleMode::LocalWorkspace.reads_spill_data()
        );
    }
    #[test]
    fn request_report_only_disallows_fs() {
        assert!(!req().allow_filesystem_side_effects);
    }
    #[test]
    fn request_local_workspace_allows_fs() {
        let r = SpillLifecycleRequest::local_workspace(
            SpillWorkspaceId::new("id").expect("id"),
            SpillWorkspacePath::new("/tmp/x").expect("path"),
        );
        assert!(r.allow_filesystem_side_effects);
    }
    #[test]
    fn unsupported_cleanup_action_has_errors_and_no_fallback() {
        let a = SpillCleanupAction::unsupported("t", "feat", "reason");
        assert!(a.has_errors());
        assert!(!a.diagnostics[0].fallback.attempted);
    }
    #[test]
    fn cleanup_plan_empty_zero() {
        assert_eq!(SpillCleanupPlan::empty().action_count(), 0);
    }
    #[test]
    fn cleanup_plan_counts() {
        let mut p = SpillCleanupPlan::empty();
        p.add_action(SpillCleanupAction::report_only("x"));
        assert_eq!(p.action_count(), 1);
    }
    #[test]
    fn planned_report_flags_false() {
        let r = SpillLifecycleReport::planned(req());
        assert!(
            !r.spill_data_written.as_bool()
                && !r.spill_data_read.as_bool()
                && !r.object_store_io.as_bool()
                && !r.fallback_execution_allowed.as_bool()
        );
    }
    #[test]
    fn feature_disabled_no_io() {
        let r = SpillLifecycleReport::feature_disabled(SpillLifecycleRequest::local_workspace(
            SpillWorkspaceId::new("id").expect("id"),
            SpillWorkspacePath::new("/tmp/x").expect("path"),
        ));
        assert!(!r.spill_data_written.as_bool() && !r.spill_data_read.as_bool());
    }
    #[test]
    fn unsupported_has_errors_no_fallback() {
        let r = SpillLifecycleReport::unsupported(req(), "x", "y");
        assert!(r.has_errors());
        assert!(!r.fallback_execution_allowed.as_bool());
    }
    #[test]
    fn plan_report_only_side_effect_free() {
        let r = plan_spill_lifecycle(req()).expect("ok");
        assert!(r.is_side_effect_free());
    }
    #[test]
    fn human_text_includes_expected() {
        let mut r = SpillLifecycleReport::planned(req());
        r.add_diagnostic(Diagnostic::no_fallback_execution("disabled"));
        let txt = r.to_human_text();
        assert!(txt.contains("fallback execution disabled"));
        assert!(txt.contains("spill data written: false"));
        assert!(txt.contains("diagnostic:"));
    }
}
