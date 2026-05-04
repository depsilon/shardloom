use std::fmt::Write as _;

use shardloom_core::{
    DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, FallbackStatus, Result,
};

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
        let next_state = derive_next_state(request.transition, status);
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
        self.request.has_errors()
            || self
                .diagnostics
                .iter()
                .any(|d| matches!(d.severity, DiagnosticSeverity::Error))
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

fn derive_status(request: &VortexCommitProtocolRequest) -> VortexCommitProtocolStatus {
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
    match request.transition {
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
    }
}
fn derive_next_state(
    transition: VortexCommitProtocolTransition,
    status: VortexCommitProtocolStatus,
) -> VortexCommitProtocolState {
    if matches!(status, VortexCommitProtocolStatus::Unsupported) {
        return VortexCommitProtocolState::Unsupported;
    }
    if !matches!(status, VortexCommitProtocolStatus::TransitionAllowed) {
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

/// Plans a `Vortex` commit protocol transition from explicit signals only.
/// # Errors
/// Propagates errors from [`VortexCommitProtocolReport::from_request`].
pub fn plan_vortex_commit_protocol(
    request: VortexCommitProtocolRequest,
) -> Result<VortexCommitProtocolReport> {
    VortexCommitProtocolReport::from_request(request)
}
#[must_use]
pub fn vortex_commit_protocol_is_side_effect_free(report: &VortexCommitProtocolReport) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{DatasetUri, DiagnosticCategory};
    fn uri() -> DatasetUri {
        DatasetUri::new("file:///tmp/dataset.vortex").expect("uri")
    }
    fn req(t: VortexCommitProtocolTransition) -> VortexCommitProtocolRequest {
        VortexCommitProtocolRequest::new(uri(), VortexCommitProtocolState::NotStarted, t)
            .commit_intent_ready(true)
            .recovery_ready(true)
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
        assert_eq!(
            VortexCommitProtocolReport::from_request(
                req(VortexCommitProtocolTransition::PrepareManifestFinalization)
                    .draft_manifest_ready(true)
                    .manifest_finalization_available(true)
            )
            .expect("r")
            .next_state,
            VortexCommitProtocolState::AwaitingManifestFinalization
        );
        assert_eq!(
            VortexCommitProtocolReport::from_request(
                req(VortexCommitProtocolTransition::PrepareCommitMarker)
                    .manifest_finalization_available(true)
            )
            .expect("r")
            .status,
            VortexCommitProtocolStatus::BlockedByCommitMarker
        );
        assert_eq!(
            VortexCommitProtocolReport::from_request(
                req(VortexCommitProtocolTransition::MarkCommitReady)
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
}
