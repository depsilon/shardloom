use std::fmt::Write as _;
#[cfg(feature = "vortex-staged-output-fs")]
use std::path::{Path, PathBuf};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStagedWorkspaceSetupStatus {
    FeatureDisabled,
    Planned,
    WorkspaceCreated,
    BlockedByNonEmptyWorkspace,
    BlockedByMissingWorkspace,
    BlockedByObjectStoreTarget,
    BlockedByExistingNonDirectory,
    Unsupported,
}
impl VortexStagedWorkspaceSetupStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::Planned => "planned",
            Self::WorkspaceCreated => "workspace_created",
            Self::BlockedByNonEmptyWorkspace => "blocked_by_non_empty_workspace",
            Self::BlockedByMissingWorkspace => "blocked_by_missing_workspace",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByExistingNonDirectory => "blocked_by_existing_non_directory",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(self) -> bool {
        matches!(
            self,
            Self::BlockedByMissingWorkspace
                | Self::BlockedByObjectStoreTarget
                | Self::BlockedByExistingNonDirectory
                | Self::BlockedByNonEmptyWorkspace
                | Self::Unsupported
        )
    }
    #[must_use]
    pub const fn workspace_created(self) -> bool {
        matches!(self, Self::WorkspaceCreated)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStagedWorkspaceSetupMode {
    ReportOnly,
    LocalWorkspaceSetup,
    Unsupported,
}
impl VortexStagedWorkspaceSetupMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::LocalWorkspaceSetup => "local_workspace_setup",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn creates_workspace(self) -> bool {
        matches!(self, Self::LocalWorkspaceSetup)
    }
    #[must_use]
    pub const fn writes_marker(self) -> bool {
        false
    }
    #[must_use]
    pub const fn writes_output_data(self) -> bool {
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
pub enum VortexStagedWorkspaceSetupOption {
    CreateIfMissing,
    RequireEmpty,
}
impl VortexStagedWorkspaceSetupOption {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CreateIfMissing => "create_if_missing",
            Self::RequireEmpty => "require_empty",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexStagedWorkspaceSetupRequest {
    pub workspace_id: VortexStagedWorkspaceId,
    pub workspace_path: VortexStagedWorkspacePath,
    pub options: Vec<VortexStagedWorkspaceSetupOption>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexStagedWorkspaceSetupRequest {
    #[must_use]
    pub fn new(
        workspace_id: VortexStagedWorkspaceId,
        workspace_path: VortexStagedWorkspacePath,
    ) -> Self {
        Self {
            workspace_id,
            workspace_path,
            options: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn create_if_missing(mut self, value: bool) -> Self {
        self.set_option(VortexStagedWorkspaceSetupOption::CreateIfMissing, value);
        self
    }
    #[must_use]
    pub fn require_empty(mut self, value: bool) -> Self {
        self.set_option(VortexStagedWorkspaceSetupOption::RequireEmpty, value);
        self
    }
    fn set_option(&mut self, option: VortexStagedWorkspaceSetupOption, value: bool) {
        if value && !self.options.contains(&option) {
            self.options.push(option);
        }
        if !value {
            self.options.retain(|o| *o != option);
        }
    }
    #[must_use]
    pub fn has_option(&self, option: VortexStagedWorkspaceSetupOption) -> bool {
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
            "workspace_id={} workspace_path={} options={}",
            self.workspace_id.as_str(),
            self.workspace_path.as_str(),
            self.options.len()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexStagedWorkspaceSetupReport {
    pub status: VortexStagedWorkspaceSetupStatus,
    pub mode: VortexStagedWorkspaceSetupMode,
    pub request: VortexStagedWorkspaceSetupRequest,
    pub effects_performed: Vec<VortexStagedOutputEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexStagedWorkspaceSetupReport {
    /// Builds a setup report from a `VortexStagedWorkspaceSetupRequest`.
    ///
    /// # Errors
    /// Returns an error only when local workspace creation is explicitly requested
    /// and local filesystem directory creation fails.
    pub fn from_request(request: VortexStagedWorkspaceSetupRequest) -> Result<Self> {
        #[cfg(not(feature = "vortex-staged-output-fs"))]
        {
            Ok(Self::feature_disabled(request))
        }
        #[cfg(feature = "vortex-staged-output-fs")]
        {
            let Ok(path_ref) = staged_workspace_local_path(&request.workspace_path) else {
                return Ok(Self::blocked(
                    request,
                    VortexStagedWorkspaceSetupStatus::BlockedByObjectStoreTarget,
                    "workspace path looks like object-store target",
                ));
            };
            if path_ref.exists() && !path_ref.is_dir() {
                return Ok(Self::blocked(
                    request,
                    VortexStagedWorkspaceSetupStatus::BlockedByExistingNonDirectory,
                    "workspace path exists and is not a directory",
                ));
            }
            if !path_ref.exists()
                && !request.has_option(VortexStagedWorkspaceSetupOption::CreateIfMissing)
            {
                return Ok(Self::blocked(
                    request,
                    VortexStagedWorkspaceSetupStatus::BlockedByMissingWorkspace,
                    "workspace path does not exist",
                ));
            }
            if !path_ref.exists()
                && request.has_option(VortexStagedWorkspaceSetupOption::CreateIfMissing)
            {
                std::fs::create_dir_all(&path_ref).map_err(|error| {
                    ShardLoomError::InvalidOperation(format!(
                        "failed to create staged workspace directory: {error}"
                    ))
                })?;
                return Ok(Self::workspace_created_report(request));
            }
            if request.has_option(VortexStagedWorkspaceSetupOption::RequireEmpty)
                && std::fs::read_dir(&path_ref)
                    .map_err(|error| {
                        ShardLoomError::InvalidOperation(format!(
                            "failed to read staged workspace directory: {error}"
                        ))
                    })?
                    .next()
                    .is_some()
            {
                return Ok(Self::blocked(
                    request,
                    VortexStagedWorkspaceSetupStatus::BlockedByNonEmptyWorkspace,
                    "workspace path must be empty",
                ));
            }
            Ok(Self::planned(request))
        }
    }
    #[must_use]
    pub fn feature_disabled(request: VortexStagedWorkspaceSetupRequest) -> Self {
        Self {
            status: VortexStagedWorkspaceSetupStatus::FeatureDisabled,
            mode: VortexStagedWorkspaceSetupMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn planned(request: VortexStagedWorkspaceSetupRequest) -> Self {
        Self {
            status: VortexStagedWorkspaceSetupStatus::Planned,
            mode: VortexStagedWorkspaceSetupMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn workspace_created_report(request: VortexStagedWorkspaceSetupRequest) -> Self {
        Self {
            status: VortexStagedWorkspaceSetupStatus::WorkspaceCreated,
            mode: VortexStagedWorkspaceSetupMode::LocalWorkspaceSetup,
            request,
            effects_performed: vec![VortexStagedOutputEffect::WorkspaceCreated],
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn blocked(
        request: VortexStagedWorkspaceSetupRequest,
        status: VortexStagedWorkspaceSetupStatus,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self {
            status,
            mode: VortexStagedWorkspaceSetupMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        };
        report.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::UnsupportedOutputFormat,
            "vortex_staged_workspace_setup",
            reason,
            None,
        ));
        report
    }
    #[must_use]
    pub fn unsupported(
        request: VortexStagedWorkspaceSetupRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self {
            status: VortexStagedWorkspaceSetupStatus::Unsupported,
            mode: VortexStagedWorkspaceSetupMode::Unsupported,
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
    pub fn workspace_created(&self) -> bool {
        self.effects_performed
            .contains(&VortexStagedOutputEffect::WorkspaceCreated)
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
    pub fn is_side_effect_free(&self) -> bool {
        !self.workspace_created() && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut t = String::new();
        let _ = writeln!(t, "Vortex staged workspace setup");
        let _ = writeln!(t, "status: {}", self.status.as_str());
        let _ = writeln!(t, "mode: {}", self.mode.as_str());
        let _ = writeln!(t, "workspace id: {}", self.request.workspace_id.as_str());
        let _ = writeln!(
            t,
            "workspace path: {}",
            self.request.workspace_path.as_str()
        );
        let _ = writeln!(t, "workspace created: {}", self.workspace_created());
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

#[cfg(feature = "vortex-staged-output-fs")]
fn staged_workspace_local_path(path: &VortexStagedWorkspacePath) -> Result<PathBuf> {
    let raw_path = path.as_str();
    let uri = DatasetUri::new(raw_path.to_string())?;
    match uri.scheme() {
        UriScheme::LocalPath => Ok(PathBuf::from(raw_path)),
        UriScheme::File => {
            if let Some(local_path) = raw_path.strip_prefix("file:///") {
                Ok(Path::new("/").join(local_path))
            } else {
                Err(ShardLoomError::InvalidOperation(
                    "workspace file URI must use file:/// absolute local path".to_string(),
                ))
            }
        }
        _ => Err(ShardLoomError::InvalidOperation(
            "workspace path looks like object-store target".to_string(),
        )),
    }
}

/// Sets up local staged workspace behavior for `VortexStagedWorkspaceSetupRequest`.
///
/// # Errors
/// Returns an error only when explicit local workspace creation is requested
/// and filesystem directory creation fails.
pub fn setup_vortex_staged_workspace(
    request: VortexStagedWorkspaceSetupRequest,
) -> Result<VortexStagedWorkspaceSetupReport> {
    VortexStagedWorkspaceSetupReport::from_request(request)
}

#[must_use]
pub fn vortex_staged_workspace_setup_is_side_effect_free(
    report: &VortexStagedWorkspaceSetupReport,
) -> bool {
    report.is_side_effect_free()
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
            report.status = VortexStagedOutputStatus::WorkspaceRequired;
            report.mode = VortexStagedOutputMode::WorkspacePlanning;
        } else if !report.commit_protocol_available() {
            report.status = VortexStagedOutputStatus::BlockedByCommitProtocol;
        } else if report.workspace_path_known() {
            report.status = VortexStagedOutputStatus::WorkspaceKnown;
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
    use std::time::{SystemTime, UNIX_EPOCH};
    fn base() -> VortexStagedOutputRequest {
        VortexStagedOutputRequest::new(
            VortexStagedWorkspaceId::new("ws1").unwrap(),
            DatasetUri::new("file://tmp/out.vortex").unwrap(),
        )
        .workspace_required(true)
    }
    fn setup_request(path: &str) -> VortexStagedWorkspaceSetupRequest {
        VortexStagedWorkspaceSetupRequest::new(
            VortexStagedWorkspaceId::new("ws_setup").unwrap(),
            VortexStagedWorkspacePath::new(path).unwrap(),
        )
    }
    fn unique_temp_path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("shardloom-{name}-{nanos}"))
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
    fn report_workspace_required_reachable() {
        let rep = VortexStagedOutputReport::from_request(base()).unwrap();
        assert_eq!(rep.status, VortexStagedOutputStatus::WorkspaceRequired);
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

    #[test]
    fn setup_request_options_work() {
        let req = setup_request("/tmp/ws");
        assert!(!req.has_option(VortexStagedWorkspaceSetupOption::CreateIfMissing));
        assert!(!req.has_option(VortexStagedWorkspaceSetupOption::RequireEmpty));
        let req = req.create_if_missing(true).require_empty(true);
        assert!(req.has_option(VortexStagedWorkspaceSetupOption::CreateIfMissing));
        assert!(req.has_option(VortexStagedWorkspaceSetupOption::RequireEmpty));
    }

    #[cfg(not(feature = "vortex-staged-output-fs"))]
    #[test]
    fn setup_default_build_feature_disabled() {
        let path = unique_temp_path("feature-disabled");
        let report = setup_vortex_staged_workspace(setup_request(&path.to_string_lossy())).unwrap();
        assert_eq!(
            report.status,
            VortexStagedWorkspaceSetupStatus::FeatureDisabled
        );
        assert!(!report.workspace_created());
        assert!(!report.marker_written());
        assert!(!report.output_data_written());
        assert!(!report.manifest_written());
        assert!(!report.object_store_io());
        assert!(!report.fallback_execution_allowed());
        assert!(report.is_side_effect_free());
        let text = report.to_human_text();
        assert!(text.contains("fallback execution: disabled"));
        assert!(text.contains("marker written: false"));
    }

    #[cfg(not(feature = "vortex-staged-output-fs"))]
    #[test]
    fn setup_default_build_file_uri_still_feature_disabled() {
        let report =
            setup_vortex_staged_workspace(setup_request("file:///tmp/shardloom-ws")).unwrap();
        assert_eq!(
            report.status,
            VortexStagedWorkspaceSetupStatus::FeatureDisabled
        );
        assert!(report.is_side_effect_free());
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    #[test]
    fn setup_feature_missing_workspace_blocked() {
        let path = unique_temp_path("missing");
        let report = setup_vortex_staged_workspace(setup_request(&path.to_string_lossy())).unwrap();
        assert_eq!(
            report.status,
            VortexStagedWorkspaceSetupStatus::BlockedByMissingWorkspace
        );
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    #[test]
    fn setup_feature_create_if_missing_creates_directory() {
        let path = unique_temp_path("create");
        let report = setup_vortex_staged_workspace(
            setup_request(&path.to_string_lossy()).create_if_missing(true),
        )
        .unwrap();
        assert_eq!(
            report.status,
            VortexStagedWorkspaceSetupStatus::WorkspaceCreated
        );
        assert!(report.workspace_created());
        assert!(!report.marker_written());
        assert!(!report.output_data_written());
        assert!(!report.manifest_written());
        assert!(!report.object_store_io());
        assert!(!report.fallback_execution_allowed());
        assert!(!path.join("_MARKER").exists());
        assert!(!path.join("manifest.json").exists());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    #[test]
    fn setup_feature_file_uri_is_normalized() {
        let path = unique_temp_path("file-uri");
        let file_uri = format!("file://{}", path.to_string_lossy());
        let report =
            setup_vortex_staged_workspace(setup_request(&file_uri).create_if_missing(true))
                .unwrap();
        assert_eq!(
            report.status,
            VortexStagedWorkspaceSetupStatus::WorkspaceCreated
        );
        assert!(path.exists());
        assert!(!Path::new("file:").exists());
        assert!(!report.marker_written());
        assert!(!report.output_data_written());
        assert!(!report.manifest_written());
        assert!(!report.object_store_io());
        assert!(!report.fallback_execution_allowed());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    #[test]
    fn setup_feature_existing_workspace_not_reported_as_created() {
        let path = unique_temp_path("exists");
        std::fs::create_dir_all(&path).unwrap();
        let report = setup_vortex_staged_workspace(setup_request(&path.to_string_lossy())).unwrap();
        assert_eq!(report.status, VortexStagedWorkspaceSetupStatus::Planned);
        assert!(!report.workspace_created());
        assert!(report.is_side_effect_free());
        assert!(!report.marker_written());
        assert!(!report.output_data_written());
        assert!(!report.manifest_written());
        assert!(!report.fallback_execution_allowed());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    #[test]
    fn setup_feature_existing_non_directory_blocked() {
        let path = unique_temp_path("nondir");
        std::fs::write(&path, "x").unwrap();
        let report = setup_vortex_staged_workspace(setup_request(&path.to_string_lossy())).unwrap();
        assert_eq!(
            report.status,
            VortexStagedWorkspaceSetupStatus::BlockedByExistingNonDirectory
        );
        std::fs::remove_file(path).unwrap();
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    #[test]
    fn setup_feature_require_empty_non_empty_blocked() {
        let path = unique_temp_path("nonempty");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("keep.txt"), "x").unwrap();
        let report = setup_vortex_staged_workspace(
            setup_request(&path.to_string_lossy()).require_empty(true),
        )
        .unwrap();
        assert_eq!(
            report.status,
            VortexStagedWorkspaceSetupStatus::BlockedByNonEmptyWorkspace
        );
        std::fs::remove_file(path.join("keep.txt")).unwrap();
        std::fs::remove_dir(path).unwrap();
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    #[test]
    fn setup_feature_object_store_path_blocked() {
        let path = unique_temp_path("s3-blocked");
        let report = setup_vortex_staged_workspace(setup_request("s3://bucket/ws")).unwrap();
        assert_eq!(
            report.status,
            VortexStagedWorkspaceSetupStatus::BlockedByObjectStoreTarget
        );
        assert!(!path.exists());
    }
}
