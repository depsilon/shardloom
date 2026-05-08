use std::fmt::Write as _;
#[cfg(feature = "vortex-staged-output-fs")]
use std::{
    fs::{self, OpenOptions},
    io::Write as _,
    path::{Path, PathBuf},
};

#[cfg(feature = "vortex-staged-output-fs")]
use shardloom_core::UriScheme;
use shardloom_core::{
    DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, FallbackStatus, Result,
    ShardLoomError,
};

use crate::VortexStagedWorkspacePath;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCommitProtocolState {
    NotStarted,
    IntentValidated,
    DraftManifestReady,
    AwaitingManifestFinalization,
    AwaitingCommitMarker,
    CommitReady,
    CommitBlocked,
    CommitAborted,
    Unsupported,
}
impl VortexCommitProtocolState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::IntentValidated => "intent_validated",
            Self::DraftManifestReady => "draft_manifest_ready",
            Self::AwaitingManifestFinalization => "awaiting_manifest_finalization",
            Self::AwaitingCommitMarker => "awaiting_commit_marker",
            Self::CommitReady => "commit_ready",
            Self::CommitBlocked => "commit_blocked",
            Self::CommitAborted => "commit_aborted",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::CommitReady | Self::CommitBlocked | Self::CommitAborted | Self::Unsupported
        )
    }
    #[must_use]
    pub const fn is_blocked(self) -> bool {
        matches!(self, Self::CommitBlocked | Self::Unsupported)
    }
    #[must_use]
    pub const fn allows_commit_execution(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCommitProtocolTransition {
    ValidateIntent,
    PrepareManifestFinalization,
    PrepareCommitMarker,
    MarkCommitReady,
    Abort,
    Unsupported,
}
impl VortexCommitProtocolTransition {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ValidateIntent => "validate_intent",
            Self::PrepareManifestFinalization => "prepare_manifest_finalization",
            Self::PrepareCommitMarker => "prepare_commit_marker",
            Self::MarkCommitReady => "mark_commit_ready",
            Self::Abort => "abort",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn requires_manifest_finalization(self) -> bool {
        matches!(
            self,
            Self::PrepareManifestFinalization | Self::PrepareCommitMarker | Self::MarkCommitReady
        )
    }
    #[must_use]
    pub const fn requires_commit_marker(self) -> bool {
        matches!(self, Self::PrepareCommitMarker | Self::MarkCommitReady)
    }
    #[must_use]
    pub const fn executes_commit(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCommitProtocolStatus {
    Planned,
    TransitionAllowed,
    TransitionBlocked,
    BlockedByCommitIntent,
    BlockedByManifestFinalization,
    BlockedByCommitMarker,
    BlockedByObjectStoreTarget,
    BlockedByRecovery,
    Unsupported,
}
impl VortexCommitProtocolStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::TransitionAllowed => "transition_allowed",
            Self::TransitionBlocked => "transition_blocked",
            Self::BlockedByCommitIntent => "blocked_by_commit_intent",
            Self::BlockedByManifestFinalization => "blocked_by_manifest_finalization",
            Self::BlockedByCommitMarker => "blocked_by_commit_marker",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByRecovery => "blocked_by_recovery",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::Planned | Self::TransitionAllowed)
    }
    #[must_use]
    pub const fn allows_transition_execution(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCommitProtocolMode {
    ReportOnly,
    StateTransitionPlanning,
    Unsupported,
}
impl VortexCommitProtocolMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::StateTransitionPlanning => "state_transition_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn executes_commit(self) -> bool {
        false
    }
    #[must_use]
    pub const fn finalizes_manifest(self) -> bool {
        false
    }
    #[must_use]
    pub const fn writes_commit_marker(self) -> bool {
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
pub enum VortexCommitProtocolSignal {
    CommitIntentReady,
    CommitIntentBlocked,
    DraftManifestReady,
    DraftManifestMissing,
    ManifestFinalizationAvailable,
    ManifestFinalizationMissing,
    CommitMarkerAvailable,
    CommitMarkerMissing,
    ObjectStoreTarget,
    RecoveryReady,
    RecoveryBlocked,
    FeatureGateEnabled,
}
impl VortexCommitProtocolSignal {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CommitIntentReady => "commit_intent_ready",
            Self::CommitIntentBlocked => "commit_intent_blocked",
            Self::DraftManifestReady => "draft_manifest_ready",
            Self::DraftManifestMissing => "draft_manifest_missing",
            Self::ManifestFinalizationAvailable => "manifest_finalization_available",
            Self::ManifestFinalizationMissing => "manifest_finalization_missing",
            Self::CommitMarkerAvailable => "commit_marker_available",
            Self::CommitMarkerMissing => "commit_marker_missing",
            Self::ObjectStoreTarget => "object_store_target",
            Self::RecoveryReady => "recovery_ready",
            Self::RecoveryBlocked => "recovery_blocked",
            Self::FeatureGateEnabled => "feature_gate_enabled",
        }
    }
    #[must_use]
    pub const fn is_blocking(self) -> bool {
        matches!(
            self,
            Self::CommitIntentBlocked
                | Self::DraftManifestMissing
                | Self::ManifestFinalizationMissing
                | Self::CommitMarkerMissing
                | Self::ObjectStoreTarget
                | Self::RecoveryBlocked
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCommitProtocolEffect {
    ManifestFinalized,
    CommitMarkerWritten,
    ManifestCommitted,
    OutputDataWritten,
    ObjectStoreIo,
    UpstreamVortexWriteCalled,
    RecoveryActionExecuted,
    FallbackExecution,
}
impl VortexCommitProtocolEffect {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ManifestFinalized => "manifest_finalized",
            Self::CommitMarkerWritten => "commit_marker_written",
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
pub enum VortexLocalCommitExecutionStatus {
    FeatureDisabled,
    Planned,
    CommitExecuted,
    AlreadyCommitted,
    BlockedByCommitProtocol,
    BlockedByFinalizedManifest,
    BlockedByCommitMarker,
    BlockedByOutputPayload,
    BlockedByObjectStoreTarget,
    BlockedByMissingWorkspace,
    BlockedByMissingFinalizedManifest,
    BlockedByMissingCommitMarker,
    BlockedByMissingOutputPayload,
    BlockedByExistingCommittedManifest,
    BlockedByExistingNonDirectory,
    BlockedByFeatureGate,
    AmbiguousCommit,
    Unsupported,
}
impl VortexLocalCommitExecutionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::Planned => "planned",
            Self::CommitExecuted => "commit_executed",
            Self::AlreadyCommitted => "already_committed",
            Self::BlockedByCommitProtocol => "blocked_by_commit_protocol",
            Self::BlockedByFinalizedManifest => "blocked_by_finalized_manifest",
            Self::BlockedByCommitMarker => "blocked_by_commit_marker",
            Self::BlockedByOutputPayload => "blocked_by_output_payload",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByMissingWorkspace => "blocked_by_missing_workspace",
            Self::BlockedByMissingFinalizedManifest => "blocked_by_missing_finalized_manifest",
            Self::BlockedByMissingCommitMarker => "blocked_by_missing_commit_marker",
            Self::BlockedByMissingOutputPayload => "blocked_by_missing_output_payload",
            Self::BlockedByExistingCommittedManifest => "blocked_by_existing_committed_manifest",
            Self::BlockedByExistingNonDirectory => "blocked_by_existing_non_directory",
            Self::BlockedByFeatureGate => "blocked_by_feature_gate",
            Self::AmbiguousCommit => "ambiguous_commit",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(
            self,
            Self::FeatureDisabled | Self::Planned | Self::CommitExecuted | Self::AlreadyCommitted
        )
    }
    #[must_use]
    pub const fn commit_executed(self) -> bool {
        matches!(self, Self::CommitExecuted | Self::AlreadyCommitted)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLocalCommitExecutionMode {
    ReportOnly,
    LocalManifestCommit,
    Unsupported,
}
impl VortexLocalCommitExecutionMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::LocalManifestCommit => "local_manifest_commit",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn executes_commit(self) -> bool {
        matches!(self, Self::LocalManifestCommit)
    }
    #[must_use]
    pub const fn writes_manifest(self) -> bool {
        matches!(self, Self::LocalManifestCommit)
    }
    #[must_use]
    pub const fn writes_object_store(self) -> bool {
        false
    }
    #[must_use]
    pub const fn calls_upstream_vortex_write(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLocalCommitExecutionSignal {
    CommitProtocolReady,
    CommitProtocolBlocked,
    FinalizedManifestWritten,
    FinalizedManifestMissing,
    CommitMarkerWritten,
    CommitMarkerMissing,
    OutputPayloadWritten,
    OutputPayloadMissing,
    LocalWorkspace,
    ObjectStoreTarget,
    FeatureGateEnabled,
}
impl VortexLocalCommitExecutionSignal {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CommitProtocolReady => "commit_protocol_ready",
            Self::CommitProtocolBlocked => "commit_protocol_blocked",
            Self::FinalizedManifestWritten => "finalized_manifest_written",
            Self::FinalizedManifestMissing => "finalized_manifest_missing",
            Self::CommitMarkerWritten => "commit_marker_written",
            Self::CommitMarkerMissing => "commit_marker_missing",
            Self::OutputPayloadWritten => "output_payload_written",
            Self::OutputPayloadMissing => "output_payload_missing",
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
                | Self::FinalizedManifestMissing
                | Self::CommitMarkerMissing
                | Self::OutputPayloadMissing
                | Self::ObjectStoreTarget
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexCommittedManifestFileName(String);
impl VortexCommittedManifestFileName {
    /// # Errors
    /// Returns an error when the committed manifest file name is empty or unsafe.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let trimmed = value.trim();
        if trimmed.is_empty()
            || trimmed.contains('/')
            || trimmed.contains('\\')
            || trimmed.contains("..")
        {
            return Err(ShardLoomError::InvalidOperation(
                "invalid committed manifest file name".to_string(),
            ));
        }
        Ok(Self(trimmed.to_string()))
    }
    #[must_use]
    pub fn default_committed() -> Self {
        Self("_shardloom_committed_manifest.json".to_string())
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexCommittedManifestFileRef {
    pub workspace_path: VortexStagedWorkspacePath,
    pub file_name: VortexCommittedManifestFileName,
}
impl VortexCommittedManifestFileRef {
    #[must_use]
    pub fn new(
        workspace_path: VortexStagedWorkspacePath,
        file_name: VortexCommittedManifestFileName,
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
            VortexCommittedManifestFileName::default_committed(),
        )
    }
    #[must_use]
    pub fn path_string(&self) -> String {
        format!(
            "{}/{}",
            self.workspace_path.as_str().trim_end_matches('/'),
            self.file_name.as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexCommitProtocolRequest {
    pub target_uri: DatasetUri,
    pub current_state: VortexCommitProtocolState,
    pub transition: VortexCommitProtocolTransition,
    pub signals: Vec<VortexCommitProtocolSignal>,
    pub commit_intent_summary: Option<String>,
    pub recovery_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
macro_rules! signal_builder {
    ($name:ident, $sig:expr) => {
        #[must_use]
        pub fn $name(mut self, value: bool) -> Self {
            self.add_signal($sig, value);
            self
        }
    };
}
impl VortexCommitProtocolRequest {
    #[must_use]
    pub fn new(
        target_uri: DatasetUri,
        current_state: VortexCommitProtocolState,
        transition: VortexCommitProtocolTransition,
    ) -> Self {
        Self {
            target_uri,
            current_state,
            transition,
            signals: Vec::new(),
            commit_intent_summary: None,
            recovery_summary: None,
            diagnostics: Vec::new(),
        }
    }
    pub fn add_signal(&mut self, signal: VortexCommitProtocolSignal, value: bool) {
        if value {
            if !self.signals.contains(&signal) {
                self.signals.push(signal);
            }
        } else {
            self.signals.retain(|s| *s != signal);
        }
    }
    signal_builder!(
        commit_intent_ready,
        VortexCommitProtocolSignal::CommitIntentReady
    );
    signal_builder!(
        commit_intent_blocked,
        VortexCommitProtocolSignal::CommitIntentBlocked
    );
    signal_builder!(
        draft_manifest_ready,
        VortexCommitProtocolSignal::DraftManifestReady
    );
    signal_builder!(
        draft_manifest_missing,
        VortexCommitProtocolSignal::DraftManifestMissing
    );
    signal_builder!(
        manifest_finalization_available,
        VortexCommitProtocolSignal::ManifestFinalizationAvailable
    );
    signal_builder!(
        manifest_finalization_missing,
        VortexCommitProtocolSignal::ManifestFinalizationMissing
    );
    signal_builder!(
        commit_marker_available,
        VortexCommitProtocolSignal::CommitMarkerAvailable
    );
    signal_builder!(
        commit_marker_missing,
        VortexCommitProtocolSignal::CommitMarkerMissing
    );
    signal_builder!(
        object_store_target,
        VortexCommitProtocolSignal::ObjectStoreTarget
    );
    signal_builder!(recovery_ready, VortexCommitProtocolSignal::RecoveryReady);
    signal_builder!(
        recovery_blocked,
        VortexCommitProtocolSignal::RecoveryBlocked
    );
    signal_builder!(
        feature_gate_enabled,
        VortexCommitProtocolSignal::FeatureGateEnabled
    );
    #[must_use]
    pub fn with_commit_intent_summary(mut self, summary: impl Into<String>) -> Self {
        self.commit_intent_summary = Some(summary.into());
        self
    }
    #[must_use]
    pub fn with_recovery_summary(mut self, summary: impl Into<String>) -> Self {
        self.recovery_summary = Some(summary.into());
        self
    }
    #[must_use]
    pub fn has_signal(&self, signal: VortexCommitProtocolSignal) -> bool {
        self.signals.contains(&signal)
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .any(|d| matches!(d.severity, DiagnosticSeverity::Error))
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "target_uri={} current_state={} transition={} signals={}",
            self.target_uri.as_str(),
            self.current_state.as_str(),
            self.transition.as_str(),
            self.signals.len()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexCommitProtocolReport {
    pub status: VortexCommitProtocolStatus,
    pub mode: VortexCommitProtocolMode,
    pub request: VortexCommitProtocolRequest,
    pub next_state: VortexCommitProtocolState,
    pub effects_performed: Vec<VortexCommitProtocolEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexCommitProtocolReport {
    /// # Errors
    /// Propagates formatting failures when constructing human-facing summaries.
    pub fn from_request(request: VortexCommitProtocolRequest) -> Result<Self> {
        let status = derive_status(&request);
        let next_state = derive_next_state(request.current_state, request.transition, status);
        Ok(Self {
            status,
            mode: VortexCommitProtocolMode::ReportOnly,
            request,
            next_state,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        })
    }
    #[must_use]
    pub fn unsupported(
        request: VortexCommitProtocolRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let reason = reason.into();
        let feature = feature.into();
        let diagnostic = Diagnostic::new(
            DiagnosticCode::UnsupportedEffect,
            DiagnosticSeverity::Error,
            shardloom_core::DiagnosticCategory::UnsupportedFeature,
            format!("unsupported commit protocol feature: {feature}"),
            Some(feature),
            Some(reason),
            Some(
                "Use report-only protocol planning signals until a later commit execution phase."
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        );
        Self {
            status: VortexCommitProtocolStatus::Unsupported,
            mode: VortexCommitProtocolMode::Unsupported,
            request,
            next_state: VortexCommitProtocolState::Unsupported,
            effects_performed: Vec::new(),
            diagnostics: vec![diagnostic],
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
    pub const fn transition_allowed(&self) -> bool {
        matches!(self.status, VortexCommitProtocolStatus::TransitionAllowed)
    }
    #[must_use]
    pub const fn current_state(&self) -> VortexCommitProtocolState {
        self.request.current_state
    }
    #[must_use]
    pub const fn next_state(&self) -> VortexCommitProtocolState {
        self.next_state
    }
    #[must_use]
    pub fn commit_intent_ready(&self) -> bool {
        self.request
            .has_signal(VortexCommitProtocolSignal::CommitIntentReady)
    }
    #[must_use]
    pub fn draft_manifest_ready(&self) -> bool {
        self.request
            .has_signal(VortexCommitProtocolSignal::DraftManifestReady)
    }
    #[must_use]
    pub fn manifest_finalization_available(&self) -> bool {
        self.request
            .has_signal(VortexCommitProtocolSignal::ManifestFinalizationAvailable)
    }
    #[must_use]
    pub fn commit_marker_available(&self) -> bool {
        self.request
            .has_signal(VortexCommitProtocolSignal::CommitMarkerAvailable)
    }
    #[must_use]
    pub fn object_store_target(&self) -> bool {
        self.request
            .has_signal(VortexCommitProtocolSignal::ObjectStoreTarget)
    }
    #[must_use]
    pub fn recovery_ready(&self) -> bool {
        self.request
            .has_signal(VortexCommitProtocolSignal::RecoveryReady)
    }
    #[must_use]
    pub fn manifest_finalized(&self) -> bool {
        self.effects_performed
            .contains(&VortexCommitProtocolEffect::ManifestFinalized)
    }
    #[must_use]
    pub fn commit_marker_written(&self) -> bool {
        self.effects_performed
            .contains(&VortexCommitProtocolEffect::CommitMarkerWritten)
    }
    #[must_use]
    pub fn manifest_committed(&self) -> bool {
        self.effects_performed
            .contains(&VortexCommitProtocolEffect::ManifestCommitted)
    }
    #[must_use]
    pub fn output_data_written(&self) -> bool {
        self.effects_performed
            .contains(&VortexCommitProtocolEffect::OutputDataWritten)
    }
    #[must_use]
    pub fn object_store_io(&self) -> bool {
        self.effects_performed
            .contains(&VortexCommitProtocolEffect::ObjectStoreIo)
    }
    #[must_use]
    pub fn upstream_vortex_write_called(&self) -> bool {
        self.effects_performed
            .contains(&VortexCommitProtocolEffect::UpstreamVortexWriteCalled)
    }
    #[must_use]
    pub fn recovery_action_executed(&self) -> bool {
        self.effects_performed
            .contains(&VortexCommitProtocolEffect::RecoveryActionExecuted)
    }
    #[must_use]
    pub fn fallback_execution_allowed(&self) -> bool {
        self.effects_performed
            .contains(&VortexCommitProtocolEffect::FallbackExecution)
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
        let _ = writeln!(out, "commit protocol status: {}", self.status.as_str());
        let _ = writeln!(out, "mode: {}", self.mode.as_str());
        let _ = writeln!(out, "current state: {}", self.current_state().as_str());
        let _ = writeln!(
            out,
            "requested transition: {}",
            self.request.transition.as_str()
        );
        let _ = writeln!(out, "next state: {}", self.next_state().as_str());
        let _ = writeln!(out, "target URI: {}", self.request.target_uri.as_str());
        let _ = writeln!(out, "commit intent ready: {}", self.commit_intent_ready());
        let _ = writeln!(out, "draft manifest ready: {}", self.draft_manifest_ready());
        let _ = writeln!(
            out,
            "manifest finalization available: {}",
            self.manifest_finalization_available()
        );
        let _ = writeln!(
            out,
            "commit marker available: {}",
            self.commit_marker_available()
        );
        let _ = writeln!(out, "recovery ready: {}", self.recovery_ready());
        let _ = writeln!(out, "object-store target: {}", self.object_store_target());
        let _ = writeln!(out, "manifest finalized: {}", self.manifest_finalized());
        let _ = writeln!(
            out,
            "commit marker written: {}",
            self.commit_marker_written()
        );
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
            for diagnostic in self
                .request
                .diagnostics
                .iter()
                .chain(self.diagnostics.iter())
            {
                let _ = writeln!(
                    out,
                    "- [{}] {}",
                    diagnostic.code.as_str(),
                    diagnostic.message
                );
            }
        }
        out
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexLocalCommitExecutionRequest {
    pub target_uri: DatasetUri,
    pub workspace_path: VortexStagedWorkspacePath,
    pub committed_manifest_ref: VortexCommittedManifestFileRef,
    pub signals: Vec<VortexLocalCommitExecutionSignal>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexLocalCommitExecutionRequest {
    #[must_use]
    pub fn new(target_uri: DatasetUri, workspace_path: VortexStagedWorkspacePath) -> Self {
        let committed_manifest_ref =
            VortexCommittedManifestFileRef::default_for_workspace(workspace_path.clone());
        Self {
            target_uri,
            workspace_path,
            committed_manifest_ref,
            signals: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    pub fn add_signal(&mut self, signal: VortexLocalCommitExecutionSignal, value: bool) {
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
        self.add_signal(VortexLocalCommitExecutionSignal::CommitProtocolReady, value);
        self
    }
    #[must_use]
    pub fn finalized_manifest_written(mut self, value: bool) -> Self {
        self.add_signal(
            VortexLocalCommitExecutionSignal::FinalizedManifestWritten,
            value,
        );
        self
    }
    #[must_use]
    pub fn commit_marker_written(mut self, value: bool) -> Self {
        self.add_signal(VortexLocalCommitExecutionSignal::CommitMarkerWritten, value);
        self
    }
    #[must_use]
    pub fn output_payload_written(mut self, value: bool) -> Self {
        self.add_signal(
            VortexLocalCommitExecutionSignal::OutputPayloadWritten,
            value,
        );
        self
    }
    #[must_use]
    pub fn local_workspace(mut self, value: bool) -> Self {
        self.add_signal(VortexLocalCommitExecutionSignal::LocalWorkspace, value);
        self
    }
    #[must_use]
    pub fn object_store_target(mut self, value: bool) -> Self {
        self.add_signal(VortexLocalCommitExecutionSignal::ObjectStoreTarget, value);
        self
    }
    #[must_use]
    pub fn feature_gate_enabled(mut self, value: bool) -> Self {
        self.add_signal(VortexLocalCommitExecutionSignal::FeatureGateEnabled, value);
        self
    }
    #[must_use]
    pub fn has_signal(&self, signal: VortexLocalCommitExecutionSignal) -> bool {
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
            "target_uri={} workspace_path={} committed_manifest_path={}",
            self.target_uri.as_str(),
            self.workspace_path.as_str(),
            self.committed_manifest_ref.path_string()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexLocalCommitExecutionReport {
    pub status: VortexLocalCommitExecutionStatus,
    pub mode: VortexLocalCommitExecutionMode,
    pub request: VortexLocalCommitExecutionRequest,
    pub effects_performed: Vec<VortexCommitProtocolEffect>,
    pub bytes_written: usize,
    pub checksum: Option<u64>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexLocalCommitExecutionReport {
    /// # Errors
    /// Returns errors when local commit artifact IO fails.
    pub fn from_request(request: VortexLocalCommitExecutionRequest) -> Result<Self> {
        execute_vortex_local_commit(request)
    }
    #[must_use]
    pub fn feature_disabled(request: VortexLocalCommitExecutionRequest) -> Self {
        Self {
            status: VortexLocalCommitExecutionStatus::FeatureDisabled,
            mode: VortexLocalCommitExecutionMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            bytes_written: 0,
            checksum: None,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn planned(request: VortexLocalCommitExecutionRequest) -> Self {
        Self {
            status: VortexLocalCommitExecutionStatus::Planned,
            mode: VortexLocalCommitExecutionMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            bytes_written: 0,
            checksum: None,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn committed(
        request: VortexLocalCommitExecutionRequest,
        status: VortexLocalCommitExecutionStatus,
        bytes_written: usize,
        checksum: u64,
    ) -> Self {
        let effects = if matches!(status, VortexLocalCommitExecutionStatus::CommitExecuted) {
            vec![VortexCommitProtocolEffect::ManifestCommitted]
        } else {
            Vec::new()
        };
        Self {
            status,
            mode: VortexLocalCommitExecutionMode::LocalManifestCommit,
            request,
            effects_performed: effects,
            bytes_written,
            checksum: Some(checksum),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn blocked(
        request: VortexLocalCommitExecutionRequest,
        status: VortexLocalCommitExecutionStatus,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self::planned(request);
        report.status = status;
        report.add_diagnostic(Diagnostic::invalid_input(
            "local_commit_execution",
            reason.into(),
            "provide ready local commit protocol, marker, payload, and finalized-manifest evidence",
        ));
        report
    }
    #[must_use]
    pub fn unsupported(
        request: VortexLocalCommitExecutionRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self {
            status: VortexLocalCommitExecutionStatus::Unsupported,
            mode: VortexLocalCommitExecutionMode::Unsupported,
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
    pub fn commit_executed(&self) -> bool {
        self.status.commit_executed()
    }
    #[must_use]
    pub fn manifest_committed(&self) -> bool {
        self.commit_executed()
    }
    #[must_use]
    pub fn manifest_written(&self) -> bool {
        self.effects_performed
            .contains(&VortexCommitProtocolEffect::ManifestCommitted)
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
    pub fn is_side_effect_free(&self) -> bool {
        !self.manifest_written()
            && !self.object_store_io()
            && !self.upstream_vortex_write_called()
            && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "local commit execution status: {}",
            self.status.as_str()
        );
        let _ = writeln!(out, "mode: {}", self.mode.as_str());
        let _ = writeln!(out, "target URI: {}", self.request.target_uri.as_str());
        let _ = writeln!(
            out,
            "workspace path: {}",
            self.request.workspace_path.as_str()
        );
        let _ = writeln!(
            out,
            "committed manifest path: {}",
            self.request.committed_manifest_ref.path_string()
        );
        let _ = writeln!(out, "commit executed: {}", self.commit_executed());
        let _ = writeln!(out, "manifest committed: {}", self.manifest_committed());
        let _ = writeln!(out, "manifest written: {}", self.manifest_written());
        let _ = writeln!(out, "output data written: false");
        let _ = writeln!(out, "object-store IO: false");
        let _ = writeln!(out, "upstream Vortex write called: false");
        let _ = writeln!(out, "recovery action executed: false");
        let _ = writeln!(out, "bytes written: {}", self.bytes_written);
        let _ = writeln!(
            out,
            "checksum: {}",
            self.checksum
                .map_or_else(|| "none".to_string(), |v| v.to_string())
        );
        let _ = writeln!(out, "fallback execution disabled");
        for diagnostic in self
            .request
            .diagnostics
            .iter()
            .chain(self.diagnostics.iter())
        {
            let _ = writeln!(
                out,
                "diagnostic [{}] {}",
                diagnostic.severity.as_str(),
                diagnostic.message
            );
        }
        out
    }
}

fn derive_status(request: &VortexCommitProtocolRequest) -> VortexCommitProtocolStatus {
    if matches!(request.transition, VortexCommitProtocolTransition::Abort) {
        return VortexCommitProtocolStatus::TransitionAllowed;
    }
    if request.has_signal(VortexCommitProtocolSignal::ObjectStoreTarget) {
        return VortexCommitProtocolStatus::BlockedByObjectStoreTarget;
    }
    if request.has_signal(VortexCommitProtocolSignal::CommitIntentBlocked)
        || !request.has_signal(VortexCommitProtocolSignal::CommitIntentReady)
    {
        return VortexCommitProtocolStatus::BlockedByCommitIntent;
    }
    if request.has_signal(VortexCommitProtocolSignal::RecoveryBlocked)
        || !request.has_signal(VortexCommitProtocolSignal::RecoveryReady)
    {
        return VortexCommitProtocolStatus::BlockedByRecovery;
    }
    let status = match request.transition {
        VortexCommitProtocolTransition::PrepareManifestFinalization => {
            if !request.has_signal(VortexCommitProtocolSignal::DraftManifestReady)
                || !request.has_signal(VortexCommitProtocolSignal::ManifestFinalizationAvailable)
            {
                VortexCommitProtocolStatus::BlockedByManifestFinalization
            } else {
                VortexCommitProtocolStatus::TransitionAllowed
            }
        }
        VortexCommitProtocolTransition::PrepareCommitMarker => {
            if !request.has_signal(VortexCommitProtocolSignal::ManifestFinalizationAvailable) {
                VortexCommitProtocolStatus::BlockedByManifestFinalization
            } else if !request.has_signal(VortexCommitProtocolSignal::CommitMarkerAvailable) {
                VortexCommitProtocolStatus::BlockedByCommitMarker
            } else {
                VortexCommitProtocolStatus::TransitionAllowed
            }
        }
        VortexCommitProtocolTransition::MarkCommitReady => {
            if !request.has_signal(VortexCommitProtocolSignal::DraftManifestReady)
                || !request.has_signal(VortexCommitProtocolSignal::ManifestFinalizationAvailable)
            {
                VortexCommitProtocolStatus::BlockedByManifestFinalization
            } else if !request.has_signal(VortexCommitProtocolSignal::CommitMarkerAvailable) {
                VortexCommitProtocolStatus::BlockedByCommitMarker
            } else {
                VortexCommitProtocolStatus::TransitionAllowed
            }
        }
        VortexCommitProtocolTransition::ValidateIntent => {
            VortexCommitProtocolStatus::TransitionAllowed
        }
        VortexCommitProtocolTransition::Abort => VortexCommitProtocolStatus::TransitionAllowed,
        VortexCommitProtocolTransition::Unsupported => VortexCommitProtocolStatus::Unsupported,
    };
    if matches!(status, VortexCommitProtocolStatus::TransitionAllowed)
        && !is_transition_allowed_from_state(request.current_state, request.transition)
    {
        VortexCommitProtocolStatus::TransitionBlocked
    } else {
        status
    }
}
fn derive_next_state(
    current_state: VortexCommitProtocolState,
    transition: VortexCommitProtocolTransition,
    status: VortexCommitProtocolStatus,
) -> VortexCommitProtocolState {
    if matches!(status, VortexCommitProtocolStatus::Unsupported) {
        return VortexCommitProtocolState::Unsupported;
    }
    if !matches!(status, VortexCommitProtocolStatus::TransitionAllowed) {
        return VortexCommitProtocolState::CommitBlocked;
    }
    if !is_transition_allowed_from_state(current_state, transition) {
        return VortexCommitProtocolState::CommitBlocked;
    }
    match transition {
        VortexCommitProtocolTransition::ValidateIntent => {
            VortexCommitProtocolState::IntentValidated
        }
        VortexCommitProtocolTransition::PrepareManifestFinalization => {
            VortexCommitProtocolState::AwaitingManifestFinalization
        }
        VortexCommitProtocolTransition::PrepareCommitMarker => {
            VortexCommitProtocolState::AwaitingCommitMarker
        }
        VortexCommitProtocolTransition::MarkCommitReady => VortexCommitProtocolState::CommitReady,
        VortexCommitProtocolTransition::Abort => VortexCommitProtocolState::CommitAborted,
        VortexCommitProtocolTransition::Unsupported => VortexCommitProtocolState::Unsupported,
    }
}
fn is_transition_allowed_from_state(
    current_state: VortexCommitProtocolState,
    transition: VortexCommitProtocolTransition,
) -> bool {
    match transition {
        VortexCommitProtocolTransition::ValidateIntent => {
            matches!(current_state, VortexCommitProtocolState::NotStarted)
        }
        VortexCommitProtocolTransition::PrepareManifestFinalization => matches!(
            current_state,
            VortexCommitProtocolState::IntentValidated
                | VortexCommitProtocolState::DraftManifestReady
                | VortexCommitProtocolState::AwaitingManifestFinalization
        ),
        VortexCommitProtocolTransition::PrepareCommitMarker => {
            matches!(
                current_state,
                VortexCommitProtocolState::AwaitingManifestFinalization
            )
        }
        VortexCommitProtocolTransition::MarkCommitReady => {
            matches!(
                current_state,
                VortexCommitProtocolState::AwaitingCommitMarker
            )
        }
        VortexCommitProtocolTransition::Abort => !matches!(
            current_state,
            VortexCommitProtocolState::CommitReady
                | VortexCommitProtocolState::CommitAborted
                | VortexCommitProtocolState::Unsupported
        ),
        VortexCommitProtocolTransition::Unsupported => false,
    }
}

/// Plans a `Vortex` commit protocol transition from explicit signals only.
/// # Errors
/// Propagates errors from [`VortexCommitProtocolReport::from_request`].
pub fn plan_vortex_commit_protocol(
    request: VortexCommitProtocolRequest,
) -> Result<VortexCommitProtocolReport> {
    VortexCommitProtocolReport::from_request(request)
}

/// Derives a [`VortexCommitProtocolRequest`] from a [`VortexCommitIntentReport`].
///
/// The derived request is report-only and preserves readiness/blocker signals without
/// attempting commit execution, manifest finalization, commit marker writes,
/// upstream `Vortex` writes, object-store IO, or fallback behavior.
#[must_use]
pub fn commit_protocol_request_from_commit_intent(
    target_uri: DatasetUri,
    current_state: VortexCommitProtocolState,
    transition: VortexCommitProtocolTransition,
    commit_intent: &crate::VortexCommitIntentReport,
) -> VortexCommitProtocolRequest {
    let mut request = VortexCommitProtocolRequest::new(target_uri, current_state, transition)
        .with_commit_intent_summary(commit_intent.to_human_text())
        .commit_intent_ready(matches!(
            commit_intent.status,
            crate::VortexCommitIntentStatus::CommitReady
        ))
        .commit_intent_blocked(
            commit_intent.has_errors()
                || !matches!(
                    commit_intent.status,
                    crate::VortexCommitIntentStatus::Planned
                        | crate::VortexCommitIntentStatus::CommitReady
                ),
        )
        .object_store_target(commit_intent.object_store_target())
        .recovery_ready(commit_intent.recovery_ready())
        .recovery_blocked(!commit_intent.recovery_ready())
        .draft_manifest_ready(commit_intent.staged_manifest_draft_written())
        .draft_manifest_missing(!commit_intent.staged_manifest_draft_written())
        .manifest_finalization_available(commit_intent.manifest_finalization_available())
        .manifest_finalization_missing(!commit_intent.manifest_finalization_available());

    for diagnostic in &commit_intent.request.diagnostics {
        request.add_diagnostic(diagnostic.clone());
    }
    for diagnostic in &commit_intent.diagnostics {
        request.add_diagnostic(diagnostic.clone());
    }
    request
}

/// Plans a report-only [`VortexCommitProtocolReport`] from a [`VortexCommitIntentReport`].
///
/// # Errors
/// Propagates errors from [`plan_vortex_commit_protocol`].
pub fn plan_vortex_commit_protocol_from_commit_intent(
    target_uri: DatasetUri,
    current_state: VortexCommitProtocolState,
    transition: VortexCommitProtocolTransition,
    commit_intent: &crate::VortexCommitIntentReport,
) -> Result<VortexCommitProtocolReport> {
    let request = commit_protocol_request_from_commit_intent(
        target_uri,
        current_state,
        transition,
        commit_intent,
    );
    plan_vortex_commit_protocol(request)
}

/// Executes the first local-only commit step by copying a finalized-manifest
/// candidate into the committed-manifest artifact.
///
/// This does not write output data, perform object-store IO, call upstream
/// `Vortex` write/commit APIs, execute recovery, or enable fallback execution.
///
/// # Errors
/// Returns an error only when local artifact reads/writes fail after all
/// feature gates and explicit readiness signals have passed.
pub fn execute_vortex_local_commit(
    request: VortexLocalCommitExecutionRequest,
) -> Result<VortexLocalCommitExecutionReport> {
    #[cfg(not(feature = "vortex-staged-output-fs"))]
    {
        Ok(VortexLocalCommitExecutionReport::feature_disabled(request))
    }
    #[cfg(feature = "vortex-staged-output-fs")]
    {
        execute_vortex_local_commit_fs(request)
    }
}

#[cfg(feature = "vortex-staged-output-fs")]
#[derive(Debug, Clone)]
struct LocalCommitArtifactPaths {
    finalized_manifest: PathBuf,
    commit_marker: PathBuf,
    output_payload: PathBuf,
    committed_manifest: PathBuf,
}

#[cfg(feature = "vortex-staged-output-fs")]
fn execute_vortex_local_commit_fs(
    request: VortexLocalCommitExecutionRequest,
) -> Result<VortexLocalCommitExecutionReport> {
    if let Some((status, reason)) = local_commit_readiness_blocker(&request) {
        return Ok(VortexLocalCommitExecutionReport::blocked(
            request, status, reason,
        ));
    }
    let workspace_path = match local_commit_ready_workspace(&request)? {
        Ok(path) => path,
        Err((status, reason)) => {
            return Ok(VortexLocalCommitExecutionReport::blocked(
                request, status, reason,
            ));
        }
    };
    let paths = local_commit_artifact_paths(&request, &workspace_path);
    if let Some((status, reason)) = local_commit_artifact_blocker(&paths) {
        return Ok(VortexLocalCommitExecutionReport::blocked(
            request, status, reason,
        ));
    }
    let finalized_bytes = fs::read(&paths.finalized_manifest).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read finalized-manifest candidate for local commit: {error}"
        ))
    })?;
    write_or_verify_committed_manifest(request, &paths.committed_manifest, &finalized_bytes)
}

#[cfg(feature = "vortex-staged-output-fs")]
fn local_commit_readiness_blocker(
    request: &VortexLocalCommitExecutionRequest,
) -> Option<(VortexLocalCommitExecutionStatus, &'static str)> {
    if request.has_signal(VortexLocalCommitExecutionSignal::ObjectStoreTarget) {
        return Some((
            VortexLocalCommitExecutionStatus::BlockedByObjectStoreTarget,
            "local commit execution does not support object-store targets",
        ));
    }
    if request.has_signal(VortexLocalCommitExecutionSignal::CommitProtocolBlocked)
        || !request.has_signal(VortexLocalCommitExecutionSignal::CommitProtocolReady)
    {
        return Some((
            VortexLocalCommitExecutionStatus::BlockedByCommitProtocol,
            "commit protocol is not ready",
        ));
    }
    if request.has_signal(VortexLocalCommitExecutionSignal::FinalizedManifestMissing)
        || !request.has_signal(VortexLocalCommitExecutionSignal::FinalizedManifestWritten)
    {
        return Some((
            VortexLocalCommitExecutionStatus::BlockedByFinalizedManifest,
            "finalized-manifest candidate readiness is missing",
        ));
    }
    if request.has_signal(VortexLocalCommitExecutionSignal::CommitMarkerMissing)
        || !request.has_signal(VortexLocalCommitExecutionSignal::CommitMarkerWritten)
    {
        return Some((
            VortexLocalCommitExecutionStatus::BlockedByCommitMarker,
            "commit marker readiness is missing",
        ));
    }
    if request.has_signal(VortexLocalCommitExecutionSignal::OutputPayloadMissing)
        || !request.has_signal(VortexLocalCommitExecutionSignal::OutputPayloadWritten)
    {
        return Some((
            VortexLocalCommitExecutionStatus::BlockedByOutputPayload,
            "output payload readiness is missing",
        ));
    }
    if !request.has_signal(VortexLocalCommitExecutionSignal::LocalWorkspace) {
        return Some((
            VortexLocalCommitExecutionStatus::BlockedByMissingWorkspace,
            "local workspace readiness is missing",
        ));
    }
    if !request.has_signal(VortexLocalCommitExecutionSignal::FeatureGateEnabled) {
        return Some((
            VortexLocalCommitExecutionStatus::BlockedByFeatureGate,
            "local commit execution feature gate readiness is missing",
        ));
    }
    None
}

#[cfg(feature = "vortex-staged-output-fs")]
fn local_commit_ready_workspace(
    request: &VortexLocalCommitExecutionRequest,
) -> Result<std::result::Result<PathBuf, (VortexLocalCommitExecutionStatus, &'static str)>> {
    let workspace_uri = DatasetUri::new(request.workspace_path.as_str().to_string())?;
    if !matches!(
        workspace_uri.scheme(),
        UriScheme::LocalPath | UriScheme::File
    ) {
        return Ok(Err((
            VortexLocalCommitExecutionStatus::BlockedByObjectStoreTarget,
            "workspace path looks like object-store storage",
        )));
    }
    let workspace_path = local_commit_workspace_path(&request.workspace_path)?;
    if !workspace_path.exists() {
        return Ok(Err((
            VortexLocalCommitExecutionStatus::BlockedByMissingWorkspace,
            "local commit workspace does not exist",
        )));
    }
    if !workspace_path.is_dir() {
        return Ok(Err((
            VortexLocalCommitExecutionStatus::BlockedByExistingNonDirectory,
            "local commit workspace exists and is not a directory",
        )));
    }
    Ok(Ok(workspace_path))
}

#[cfg(feature = "vortex-staged-output-fs")]
fn local_commit_artifact_paths(
    request: &VortexLocalCommitExecutionRequest,
    workspace_path: &Path,
) -> LocalCommitArtifactPaths {
    LocalCommitArtifactPaths {
        finalized_manifest: workspace_path
            .join(crate::VortexFinalizedManifestFileName::default_finalized().as_str()),
        commit_marker: workspace_path
            .join(crate::VortexCommitMarkerFileName::default_marker().as_str()),
        output_payload: workspace_path
            .join(crate::VortexOutputPayloadFileName::default_payload().as_str()),
        committed_manifest: workspace_path.join(request.committed_manifest_ref.file_name.as_str()),
    }
}

#[cfg(feature = "vortex-staged-output-fs")]
fn local_commit_artifact_blocker(
    paths: &LocalCommitArtifactPaths,
) -> Option<(VortexLocalCommitExecutionStatus, &'static str)> {
    if !paths.finalized_manifest.is_file() {
        return Some((
            VortexLocalCommitExecutionStatus::BlockedByMissingFinalizedManifest,
            "finalized-manifest candidate artifact is missing",
        ));
    }
    if !paths.commit_marker.is_file() {
        return Some((
            VortexLocalCommitExecutionStatus::BlockedByMissingCommitMarker,
            "commit marker artifact is missing",
        ));
    }
    if !paths.output_payload.is_file() {
        return Some((
            VortexLocalCommitExecutionStatus::BlockedByMissingOutputPayload,
            "native output payload artifact is missing",
        ));
    }
    None
}

#[cfg(feature = "vortex-staged-output-fs")]
fn write_or_verify_committed_manifest(
    request: VortexLocalCommitExecutionRequest,
    committed_manifest_path: &Path,
    finalized_bytes: &[u8],
) -> Result<VortexLocalCommitExecutionReport> {
    let checksum = checksum_bytes(finalized_bytes);
    if committed_manifest_path.exists() {
        return verify_existing_committed_manifest(
            request,
            committed_manifest_path,
            finalized_bytes,
            checksum,
        );
    }
    let mut file = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(committed_manifest_path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Ok(VortexLocalCommitExecutionReport::blocked(
                request,
                VortexLocalCommitExecutionStatus::BlockedByExistingCommittedManifest,
                "committed-manifest artifact appeared before local commit could create it",
            ));
        }
        Err(error) => {
            return Err(ShardLoomError::InvalidOperation(format!(
                "failed to create committed-manifest artifact: {error}"
            )));
        }
    };
    file.write_all(finalized_bytes).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to write committed-manifest artifact: {error}"
        ))
    })?;
    Ok(VortexLocalCommitExecutionReport::committed(
        request,
        VortexLocalCommitExecutionStatus::CommitExecuted,
        finalized_bytes.len(),
        checksum,
    ))
}

#[cfg(feature = "vortex-staged-output-fs")]
fn verify_existing_committed_manifest(
    request: VortexLocalCommitExecutionRequest,
    committed_manifest_path: &Path,
    finalized_bytes: &[u8],
    checksum: u64,
) -> Result<VortexLocalCommitExecutionReport> {
    if !committed_manifest_path.is_file() {
        return Ok(VortexLocalCommitExecutionReport::blocked(
            request,
            VortexLocalCommitExecutionStatus::BlockedByExistingCommittedManifest,
            "existing committed-manifest path is not a regular file",
        ));
    }
    let committed_bytes = fs::read(committed_manifest_path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to read existing committed-manifest artifact: {error}"
        ))
    })?;
    if committed_bytes == finalized_bytes {
        return Ok(VortexLocalCommitExecutionReport::committed(
            request,
            VortexLocalCommitExecutionStatus::AlreadyCommitted,
            0,
            checksum,
        ));
    }
    Ok(VortexLocalCommitExecutionReport::blocked(
        request,
        VortexLocalCommitExecutionStatus::AmbiguousCommit,
        "existing committed-manifest artifact differs from finalized-manifest candidate",
    ))
}

#[cfg(feature = "vortex-staged-output-fs")]
fn local_commit_workspace_path(path: &VortexStagedWorkspacePath) -> Result<PathBuf> {
    let raw_path = path.as_str();
    match DatasetUri::new(raw_path.to_string())?.scheme() {
        UriScheme::LocalPath => Ok(PathBuf::from(raw_path)),
        UriScheme::File => {
            if let Some(local_path) = raw_path.strip_prefix("file:///") {
                if cfg!(windows) && local_path.as_bytes().get(1) == Some(&b':') {
                    Ok(PathBuf::from(local_path))
                } else {
                    Ok(Path::new("/").join(local_path))
                }
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

#[cfg(feature = "vortex-staged-output-fs")]
fn checksum_bytes(bytes: &[u8]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001b3;

    bytes.iter().fold(FNV_OFFSET_BASIS, |checksum, byte| {
        (checksum ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

#[must_use]
pub const fn vortex_local_commit_execution_feature_enabled() -> bool {
    cfg!(feature = "vortex-staged-output-fs")
}

#[must_use]
pub fn vortex_local_commit_execution_is_side_effect_free(
    report: &VortexLocalCommitExecutionReport,
) -> bool {
    report.is_side_effect_free()
}

#[must_use]
pub fn vortex_commit_protocol_is_side_effect_free(report: &VortexCommitProtocolReport) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{DatasetUri, DiagnosticCategory, FallbackStatus};
    fn uri() -> DatasetUri {
        DatasetUri::new("file:///tmp/dataset.vortex").expect("uri")
    }
    fn req(t: VortexCommitProtocolTransition) -> VortexCommitProtocolRequest {
        VortexCommitProtocolRequest::new(uri(), VortexCommitProtocolState::NotStarted, t)
            .commit_intent_ready(true)
            .recovery_ready(true)
    }
    fn ready_commit_intent() -> crate::VortexCommitIntentReport {
        crate::plan_vortex_commit_intent(
            crate::VortexCommitIntentRequest::new(uri())
                .commit_requested(true)
                .staged_manifest_draft_written(true)
                .manifest_finalization_available(true)
                .commit_protocol_available(true)
                .schema_known(true)
                .schema_compatible(true)
                .delete_semantics_known(true)
                .tombstone_semantics_known(true)
                .recovery_ready(true)
                .retry_gate_open(true)
                .cancellation_gate_open(true)
                .feature_gate_enabled(true),
        )
        .expect("ready commit intent")
    }
    #[test]
    fn required_behaviors() {
        assert!(!VortexCommitProtocolState::CommitReady.allows_commit_execution());
        assert!(VortexCommitProtocolState::CommitBlocked.is_blocked());
        assert!(!VortexCommitProtocolTransition::MarkCommitReady.executes_commit());
        assert!(!VortexCommitProtocolStatus::TransitionAllowed.allows_transition_execution());
        assert!(!VortexCommitProtocolMode::ReportOnly.finalizes_manifest());
        assert!(!VortexCommitProtocolMode::ReportOnly.writes_commit_marker());
    }
    #[test]
    fn signals_no_duplicates() {
        let r = req(VortexCommitProtocolTransition::ValidateIntent)
            .draft_manifest_ready(true)
            .draft_manifest_ready(true)
            .draft_manifest_ready(false);
        assert!(!r.has_signal(VortexCommitProtocolSignal::DraftManifestReady));
    }
    #[test]
    fn status_transitions() {
        assert_eq!(
            VortexCommitProtocolReport::from_request(
                req(VortexCommitProtocolTransition::ValidateIntent).object_store_target(true)
            )
            .expect("r")
            .status,
            VortexCommitProtocolStatus::BlockedByObjectStoreTarget
        );
        assert_eq!(
            VortexCommitProtocolReport::from_request(
                VortexCommitProtocolRequest::new(
                    uri(),
                    VortexCommitProtocolState::NotStarted,
                    VortexCommitProtocolTransition::ValidateIntent
                )
                .recovery_ready(true)
            )
            .expect("r")
            .status,
            VortexCommitProtocolStatus::BlockedByCommitIntent
        );
        assert_eq!(
            VortexCommitProtocolReport::from_request(
                VortexCommitProtocolRequest::new(
                    uri(),
                    VortexCommitProtocolState::NotStarted,
                    VortexCommitProtocolTransition::ValidateIntent
                )
                .commit_intent_ready(true)
            )
            .expect("r")
            .status,
            VortexCommitProtocolStatus::BlockedByRecovery
        );
        assert_eq!(
            VortexCommitProtocolReport::from_request(req(
                VortexCommitProtocolTransition::ValidateIntent
            ))
            .expect("r")
            .next_state,
            VortexCommitProtocolState::IntentValidated
        );
        assert_eq!(
            VortexCommitProtocolReport::from_request(req(
                VortexCommitProtocolTransition::PrepareManifestFinalization
            ))
            .expect("r")
            .status,
            VortexCommitProtocolStatus::BlockedByManifestFinalization
        );
    }
    #[test]
    fn status_transitions_manifest_and_marker_paths() {
        assert_eq!(
            VortexCommitProtocolReport::from_request(
                VortexCommitProtocolRequest::new(
                    uri(),
                    VortexCommitProtocolState::IntentValidated,
                    VortexCommitProtocolTransition::PrepareManifestFinalization,
                )
                .commit_intent_ready(true)
                .recovery_ready(true)
                .draft_manifest_ready(true)
                .manifest_finalization_available(true)
            )
            .expect("r")
            .next_state,
            VortexCommitProtocolState::AwaitingManifestFinalization
        );
        assert_eq!(
            VortexCommitProtocolReport::from_request(
                VortexCommitProtocolRequest::new(
                    uri(),
                    VortexCommitProtocolState::AwaitingManifestFinalization,
                    VortexCommitProtocolTransition::PrepareCommitMarker,
                )
                .commit_intent_ready(true)
                .recovery_ready(true)
                .manifest_finalization_available(true)
            )
            .expect("r")
            .status,
            VortexCommitProtocolStatus::BlockedByCommitMarker
        );
    }
    #[test]
    fn status_transitions_mark_ready_and_abort() {
        assert_eq!(
            VortexCommitProtocolReport::from_request(
                VortexCommitProtocolRequest::new(
                    uri(),
                    VortexCommitProtocolState::AwaitingCommitMarker,
                    VortexCommitProtocolTransition::MarkCommitReady,
                )
                .commit_intent_ready(true)
                .recovery_ready(true)
                .draft_manifest_ready(true)
                .manifest_finalization_available(true)
                .commit_marker_available(true)
            )
            .expect("r")
            .next_state,
            VortexCommitProtocolState::CommitReady
        );
        assert_eq!(
            VortexCommitProtocolReport::from_request(req(VortexCommitProtocolTransition::Abort))
                .expect("r")
                .next_state,
            VortexCommitProtocolState::CommitAborted
        );
    }
    #[test]
    fn abort_transition_allowed_when_readiness_missing() {
        let report = VortexCommitProtocolReport::from_request(VortexCommitProtocolRequest::new(
            uri(),
            VortexCommitProtocolState::CommitBlocked,
            VortexCommitProtocolTransition::Abort,
        ))
        .expect("abort report");
        assert_eq!(report.status, VortexCommitProtocolStatus::TransitionAllowed);
        assert_eq!(
            report.next_state(),
            VortexCommitProtocolState::CommitAborted
        );
    }
    #[test]
    fn invalid_state_hop_does_not_progress_to_commit_ready() {
        let report = VortexCommitProtocolReport::from_request(
            VortexCommitProtocolRequest::new(
                uri(),
                VortexCommitProtocolState::CommitAborted,
                VortexCommitProtocolTransition::MarkCommitReady,
            )
            .commit_intent_ready(true)
            .recovery_ready(true)
            .draft_manifest_ready(true)
            .manifest_finalization_available(true)
            .commit_marker_available(true),
        )
        .expect("report");
        assert_eq!(report.status, VortexCommitProtocolStatus::TransitionBlocked);
        assert!(report.has_errors());
        assert!(!report.transition_allowed());
        assert_eq!(
            report.next_state(),
            VortexCommitProtocolState::CommitBlocked
        );
    }
    #[test]
    fn report_side_effect_and_text() {
        let mut rep = VortexCommitProtocolReport::from_request(req(
            VortexCommitProtocolTransition::ValidateIntent,
        ))
        .expect("r");
        assert!(!rep.manifest_finalized());
        assert!(!rep.commit_marker_written());
        assert!(!rep.manifest_committed());
        assert!(!rep.output_data_written());
        assert!(!rep.object_store_io());
        assert!(!rep.upstream_vortex_write_called());
        assert!(!rep.recovery_action_executed());
        assert!(!rep.fallback_execution_allowed());
        assert!(!rep.allows_commit_execution());
        assert!(rep.is_side_effect_free());
        rep.add_diagnostic(Diagnostic::new(
            DiagnosticCode::UnsupportedEffect,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "details",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        ));
        let text = rep.to_human_text();
        assert!(text.contains("fallback execution disabled"));
        assert!(text.contains("manifest committed: false"));
        assert!(text.contains("details"));
    }
    #[test]
    fn has_errors_includes_blocked_status_and_fatal_diagnostics() {
        let blocked = VortexCommitProtocolReport::from_request(VortexCommitProtocolRequest::new(
            uri(),
            VortexCommitProtocolState::NotStarted,
            VortexCommitProtocolTransition::ValidateIntent,
        ))
        .expect("blocked report");
        assert!(blocked.has_errors());

        let mut fatal = VortexCommitProtocolReport::from_request(req(
            VortexCommitProtocolTransition::ValidateIntent,
        ))
        .expect("fatal report");
        fatal.add_diagnostic(Diagnostic::new(
            DiagnosticCode::UnsupportedEffect,
            DiagnosticSeverity::Fatal,
            DiagnosticCategory::UnsupportedFeature,
            "fatal details",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        ));
        assert!(fatal.has_errors());
    }
    #[test]
    fn helper_and_constructor() {
        let request = VortexCommitProtocolRequest::new(
            uri(),
            VortexCommitProtocolState::NotStarted,
            VortexCommitProtocolTransition::ValidateIntent,
        );
        assert!(request.signals.is_empty());
        let report =
            plan_vortex_commit_protocol(request.commit_intent_ready(true).recovery_ready(true))
                .expect("report");
        assert_eq!(report.status, VortexCommitProtocolStatus::TransitionAllowed);
    }
    #[test]
    fn request_from_commit_intent_maps_signals() {
        let ready = ready_commit_intent();
        let request = commit_protocol_request_from_commit_intent(
            uri(),
            VortexCommitProtocolState::NotStarted,
            VortexCommitProtocolTransition::ValidateIntent,
            &ready,
        );
        assert!(request.has_signal(VortexCommitProtocolSignal::CommitIntentReady));
        assert!(!request.has_signal(VortexCommitProtocolSignal::CommitIntentBlocked));
        assert!(request.has_signal(VortexCommitProtocolSignal::RecoveryReady));
        assert!(!request.has_signal(VortexCommitProtocolSignal::RecoveryBlocked));
        assert!(request.has_signal(VortexCommitProtocolSignal::DraftManifestReady));
        assert!(!request.has_signal(VortexCommitProtocolSignal::DraftManifestMissing));
        assert!(request.has_signal(VortexCommitProtocolSignal::ManifestFinalizationAvailable));
        assert!(!request.has_signal(VortexCommitProtocolSignal::ManifestFinalizationMissing));
        assert!(!request.has_signal(VortexCommitProtocolSignal::CommitMarkerAvailable));
    }
    #[test]
    fn request_from_commit_intent_maps_blockers_and_diagnostics() {
        let mut blocked = crate::plan_vortex_commit_intent(
            crate::VortexCommitIntentRequest::new(uri())
                .commit_requested(true)
                .object_store_target(true),
        )
        .expect("blocked");
        blocked.add_diagnostic(Diagnostic::new(
            DiagnosticCode::UnsupportedEffect,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "blocked details",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        ));
        let request = commit_protocol_request_from_commit_intent(
            uri(),
            VortexCommitProtocolState::NotStarted,
            VortexCommitProtocolTransition::ValidateIntent,
            &blocked,
        );
        assert!(request.has_signal(VortexCommitProtocolSignal::CommitIntentBlocked));
        assert!(request.has_signal(VortexCommitProtocolSignal::ObjectStoreTarget));
        assert!(request.has_signal(VortexCommitProtocolSignal::RecoveryBlocked));
        assert!(request.has_signal(VortexCommitProtocolSignal::DraftManifestMissing));
        assert!(request.has_signal(VortexCommitProtocolSignal::ManifestFinalizationMissing));
        assert_eq!(request.diagnostics.len(), 1);
        let report = plan_vortex_commit_protocol(request).expect("report");
        assert!(report.to_human_text().contains("blocked details"));
    }
    #[test]
    fn plan_from_commit_intent_is_report_only_and_blocks_missing_marker() {
        let ready = ready_commit_intent();
        let validated = plan_vortex_commit_protocol_from_commit_intent(
            uri(),
            VortexCommitProtocolState::NotStarted,
            VortexCommitProtocolTransition::ValidateIntent,
            &ready,
        )
        .expect("validate report");
        assert_eq!(
            validated.next_state(),
            VortexCommitProtocolState::IntentValidated
        );
        let marker_blocked = plan_vortex_commit_protocol_from_commit_intent(
            uri(),
            VortexCommitProtocolState::IntentValidated,
            VortexCommitProtocolTransition::MarkCommitReady,
            &ready,
        )
        .expect("marker blocked report");
        assert_eq!(
            marker_blocked.status,
            VortexCommitProtocolStatus::BlockedByCommitMarker
        );
        assert!(!marker_blocked.manifest_finalized());
        assert!(!marker_blocked.commit_marker_written());
        assert!(!marker_blocked.manifest_committed());
        assert!(!marker_blocked.output_data_written());
        assert!(!marker_blocked.object_store_io());
        assert!(!marker_blocked.upstream_vortex_write_called());
        assert!(!marker_blocked.recovery_action_executed());
        assert!(!marker_blocked.fallback_execution_allowed());
        assert!(!marker_blocked.allows_commit_execution());
        assert!(marker_blocked.is_side_effect_free());
        let text = marker_blocked.to_human_text();
        assert!(text.contains("fallback execution disabled"));
    }

    fn local_commit_request(path: &str) -> VortexLocalCommitExecutionRequest {
        VortexLocalCommitExecutionRequest::new(
            uri(),
            VortexStagedWorkspacePath::new(path).expect("workspace path"),
        )
        .commit_protocol_ready(true)
        .finalized_manifest_written(true)
        .commit_marker_written(true)
        .output_payload_written(true)
        .local_workspace(true)
        .feature_gate_enabled(true)
    }

    #[test]
    fn local_commit_file_name_and_signal_helpers() {
        assert!(VortexCommittedManifestFileName::new("committed.json").is_ok());
        assert!(VortexCommittedManifestFileName::new("../committed.json").is_err());
        let request = local_commit_request("/tmp/stage")
            .commit_protocol_ready(true)
            .commit_protocol_ready(false);
        assert!(!request.has_signal(VortexLocalCommitExecutionSignal::CommitProtocolReady));
        assert!(VortexLocalCommitExecutionSignal::CommitProtocolBlocked.is_blocking());
    }

    #[cfg(not(feature = "vortex-staged-output-fs"))]
    #[test]
    fn local_commit_execution_default_build_is_report_only() {
        let report = execute_vortex_local_commit(local_commit_request("/tmp/stage"))
            .expect("feature-disabled report");
        assert_eq!(
            report.status,
            VortexLocalCommitExecutionStatus::FeatureDisabled
        );
        assert!(report.is_side_effect_free());
        assert!(!report.commit_executed());
        assert!(!report.object_store_io());
        assert!(!report.upstream_vortex_write_called());
        assert!(!report.fallback_execution_allowed());
        assert!(vortex_local_commit_execution_is_side_effect_free(&report));
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    fn unique_workspace(label: &str) -> std::path::PathBuf {
        let unique = format!(
            "shardloom-vortex-local-commit-{label}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        std::env::temp_dir().join(unique)
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    fn write_required_local_commit_artifacts(workspace: &std::path::Path, finalized: &[u8]) {
        std::fs::create_dir_all(workspace).expect("workspace");
        std::fs::write(
            workspace.join(crate::VortexFinalizedManifestFileName::default_finalized().as_str()),
            finalized,
        )
        .expect("finalized manifest");
        std::fs::write(
            workspace.join(crate::VortexCommitMarkerFileName::default_marker().as_str()),
            b"marker=true\n",
        )
        .expect("commit marker");
        std::fs::write(
            workspace.join(crate::VortexOutputPayloadFileName::default_payload().as_str()),
            b"payload",
        )
        .expect("output payload");
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    #[test]
    fn local_commit_execution_writes_manifest_and_is_idempotent() {
        let workspace = unique_workspace("success");
        let finalized = br#"{"finalized":true}"#;
        write_required_local_commit_artifacts(&workspace, finalized);
        let request = local_commit_request(&workspace.to_string_lossy());

        let report = execute_vortex_local_commit(request.clone()).expect("commit report");
        let committed_path =
            workspace.join(VortexCommittedManifestFileName::default_committed().as_str());
        assert_eq!(
            report.status,
            VortexLocalCommitExecutionStatus::CommitExecuted
        );
        assert_eq!(
            std::fs::read(&committed_path).expect("committed"),
            finalized
        );
        assert!(report.commit_executed());
        assert!(report.manifest_committed());
        assert!(report.manifest_written());
        assert!(!report.output_data_written());
        assert!(!report.object_store_io());
        assert!(!report.upstream_vortex_write_called());
        assert!(!report.recovery_action_executed());
        assert!(!report.fallback_execution_allowed());

        let idempotent = execute_vortex_local_commit(request).expect("idempotent report");
        assert_eq!(
            idempotent.status,
            VortexLocalCommitExecutionStatus::AlreadyCommitted
        );
        assert_eq!(idempotent.bytes_written, 0);
        assert_eq!(idempotent.checksum, report.checksum);
        assert!(idempotent.commit_executed());
        assert!(idempotent.manifest_committed());
        assert!(!idempotent.manifest_written());
        assert!(idempotent.is_side_effect_free());

        std::fs::remove_dir_all(workspace).expect("cleanup");
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    #[test]
    fn local_commit_execution_blocks_missing_payload_and_object_store_workspace() {
        let workspace = unique_workspace("missing-payload");
        std::fs::create_dir_all(&workspace).expect("workspace");
        std::fs::write(
            workspace.join(crate::VortexFinalizedManifestFileName::default_finalized().as_str()),
            b"finalized",
        )
        .expect("finalized manifest");
        std::fs::write(
            workspace.join(crate::VortexCommitMarkerFileName::default_marker().as_str()),
            b"marker=true\n",
        )
        .expect("commit marker");
        let missing_payload =
            execute_vortex_local_commit(local_commit_request(&workspace.to_string_lossy()))
                .expect("missing payload report");
        assert_eq!(
            missing_payload.status,
            VortexLocalCommitExecutionStatus::BlockedByMissingOutputPayload
        );
        assert!(missing_payload.has_errors());

        let object_store = execute_vortex_local_commit(local_commit_request("s3://bucket/stage"))
            .expect("object-store report");
        assert_eq!(
            object_store.status,
            VortexLocalCommitExecutionStatus::BlockedByObjectStoreTarget
        );
        assert!(object_store.has_errors());
        assert!(!object_store.object_store_io());

        std::fs::remove_dir_all(workspace).expect("cleanup");
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    #[test]
    fn local_commit_execution_reports_ambiguous_existing_manifest() {
        let workspace = unique_workspace("ambiguous");
        write_required_local_commit_artifacts(&workspace, b"finalized");
        std::fs::write(
            workspace.join(VortexCommittedManifestFileName::default_committed().as_str()),
            b"different",
        )
        .expect("existing committed manifest");

        let report =
            execute_vortex_local_commit(local_commit_request(&workspace.to_string_lossy()))
                .expect("ambiguous report");
        assert_eq!(
            report.status,
            VortexLocalCommitExecutionStatus::AmbiguousCommit
        );
        assert!(report.has_errors());
        assert!(!report.manifest_written());
        assert!(!report.fallback_execution_allowed());

        std::fs::remove_dir_all(workspace).expect("cleanup");
    }
}
