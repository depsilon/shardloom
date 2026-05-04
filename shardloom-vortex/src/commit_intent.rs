use std::fmt::Write as _;

use shardloom_core::{DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, Result};
#[cfg(test)]
use shardloom_core::{DiagnosticCategory, FallbackStatus};

use crate::VortexStagedManifestFileWriteReport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCommitIntentStatus {
    Planned,
    CommitReady,
    BlockedByMissingCommitIntent,
    BlockedByStagedManifestDraft,
    BlockedByManifestFinalization,
    BlockedByCommitProtocol,
    BlockedBySchema,
    BlockedByDeleteSemantics,
    BlockedByTombstoneSemantics,
    BlockedByRecovery,
    BlockedByRetryGate,
    BlockedByCancellationGate,
    BlockedByObjectStoreTarget,
    BlockedByFeatureGate,
    Unsupported,
}
impl VortexCommitIntentStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::CommitReady => "commit_ready",
            Self::BlockedByMissingCommitIntent => "blocked_by_missing_commit_intent",
            Self::BlockedByStagedManifestDraft => "blocked_by_staged_manifest_draft",
            Self::BlockedByManifestFinalization => "blocked_by_manifest_finalization",
            Self::BlockedByCommitProtocol => "blocked_by_commit_protocol",
            Self::BlockedBySchema => "blocked_by_schema",
            Self::BlockedByDeleteSemantics => "blocked_by_delete_semantics",
            Self::BlockedByTombstoneSemantics => "blocked_by_tombstone_semantics",
            Self::BlockedByRecovery => "blocked_by_recovery",
            Self::BlockedByRetryGate => "blocked_by_retry_gate",
            Self::BlockedByCancellationGate => "blocked_by_cancellation_gate",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByFeatureGate => "blocked_by_feature_gate",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::Planned | Self::CommitReady)
    }
    #[must_use]
    pub const fn allows_commit_execution(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCommitIntentMode {
    ReportOnly,
    CommitIntentPlanning,
    Unsupported,
}
impl VortexCommitIntentMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::CommitIntentPlanning => "commit_intent_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn commits_manifest(self) -> bool {
        false
    }
    #[must_use]
    pub const fn finalizes_manifest(self) -> bool {
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
pub enum VortexCommitIntentSignal {
    CommitRequested,
    StagedManifestDraftWritten,
    StagedManifestDraftMissing,
    ManifestFinalizationAvailable,
    ManifestFinalizationMissing,
    CommitProtocolAvailable,
    SchemaKnown,
    SchemaCompatible,
    DeleteSemanticsKnown,
    TombstoneSemanticsKnown,
    RecoveryReady,
    RecoveryBlocked,
    RetryGateOpen,
    RetryGateClosed,
    CancellationGateOpen,
    CancellationGateClosed,
    ObjectStoreTarget,
    FeatureGateEnabled,
}
impl VortexCommitIntentSignal {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CommitRequested => "commit_requested",
            Self::StagedManifestDraftWritten => "staged_manifest_draft_written",
            Self::StagedManifestDraftMissing => "staged_manifest_draft_missing",
            Self::ManifestFinalizationAvailable => "manifest_finalization_available",
            Self::ManifestFinalizationMissing => "manifest_finalization_missing",
            Self::CommitProtocolAvailable => "commit_protocol_available",
            Self::SchemaKnown => "schema_known",
            Self::SchemaCompatible => "schema_compatible",
            Self::DeleteSemanticsKnown => "delete_semantics_known",
            Self::TombstoneSemanticsKnown => "tombstone_semantics_known",
            Self::RecoveryReady => "recovery_ready",
            Self::RecoveryBlocked => "recovery_blocked",
            Self::RetryGateOpen => "retry_gate_open",
            Self::RetryGateClosed => "retry_gate_closed",
            Self::CancellationGateOpen => "cancellation_gate_open",
            Self::CancellationGateClosed => "cancellation_gate_closed",
            Self::ObjectStoreTarget => "object_store_target",
            Self::FeatureGateEnabled => "feature_gate_enabled",
        }
    }
    #[must_use]
    pub const fn is_blocking(self) -> bool {
        matches!(
            self,
            Self::StagedManifestDraftMissing
                | Self::ManifestFinalizationMissing
                | Self::RecoveryBlocked
                | Self::RetryGateClosed
                | Self::CancellationGateClosed
                | Self::ObjectStoreTarget
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCommitIntentEffect {
    ManifestCommitted,
    ManifestFinalized,
    OutputDataWritten,
    ObjectStoreIo,
    UpstreamVortexWriteCalled,
    RecoveryActionExecuted,
    FallbackExecution,
}
impl VortexCommitIntentEffect {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ManifestCommitted => "manifest_committed",
            Self::ManifestFinalized => "manifest_finalized",
            Self::OutputDataWritten => "output_data_written",
            Self::ObjectStoreIo => "object_store_io",
            Self::UpstreamVortexWriteCalled => "upstream_vortex_write_called",
            Self::RecoveryActionExecuted => "recovery_action_executed",
            Self::FallbackExecution => "fallback_execution",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexCommitIntentRequest {
    pub target_uri: DatasetUri,
    pub signals: Vec<VortexCommitIntentSignal>,
    pub staged_manifest_summary: Option<String>,
    pub recovery_summary: Option<String>,
    pub retry_gate_summary: Option<String>,
    pub cancellation_gate_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexCommitIntentRequest {
    #[must_use]
    pub fn new(target_uri: DatasetUri) -> Self {
        Self {
            target_uri,
            signals: Vec::new(),
            staged_manifest_summary: None,
            recovery_summary: None,
            retry_gate_summary: None,
            cancellation_gate_summary: None,
            diagnostics: Vec::new(),
        }
    }
    pub fn add_signal(&mut self, signal: VortexCommitIntentSignal, value: bool) {
        if value {
            if !self.signals.contains(&signal) {
                self.signals.push(signal);
            }
        } else {
            self.signals.retain(|s| *s != signal);
        }
    }
    #[must_use]
    pub fn commit_requested(mut self, v: bool) -> Self {
        self.add_signal(VortexCommitIntentSignal::CommitRequested, v);
        self
    }
    #[must_use]
    pub fn staged_manifest_draft_written(mut self, v: bool) -> Self {
        self.add_signal(VortexCommitIntentSignal::StagedManifestDraftWritten, v);
        self
    }
    #[must_use]
    pub fn staged_manifest_draft_missing(mut self, v: bool) -> Self {
        self.add_signal(VortexCommitIntentSignal::StagedManifestDraftMissing, v);
        self
    }
    #[must_use]
    pub fn manifest_finalization_available(mut self, v: bool) -> Self {
        self.add_signal(VortexCommitIntentSignal::ManifestFinalizationAvailable, v);
        self
    }
    #[must_use]
    pub fn manifest_finalization_missing(mut self, v: bool) -> Self {
        self.add_signal(VortexCommitIntentSignal::ManifestFinalizationMissing, v);
        self
    }
    #[must_use]
    pub fn commit_protocol_available(mut self, v: bool) -> Self {
        self.add_signal(VortexCommitIntentSignal::CommitProtocolAvailable, v);
        self
    }
    #[must_use]
    pub fn schema_known(mut self, v: bool) -> Self {
        self.add_signal(VortexCommitIntentSignal::SchemaKnown, v);
        self
    }
    #[must_use]
    pub fn schema_compatible(mut self, v: bool) -> Self {
        self.add_signal(VortexCommitIntentSignal::SchemaCompatible, v);
        self
    }
    #[must_use]
    pub fn delete_semantics_known(mut self, v: bool) -> Self {
        self.add_signal(VortexCommitIntentSignal::DeleteSemanticsKnown, v);
        self
    }
    #[must_use]
    pub fn tombstone_semantics_known(mut self, v: bool) -> Self {
        self.add_signal(VortexCommitIntentSignal::TombstoneSemanticsKnown, v);
        self
    }
    #[must_use]
    pub fn recovery_ready(mut self, v: bool) -> Self {
        self.add_signal(VortexCommitIntentSignal::RecoveryReady, v);
        self
    }
    #[must_use]
    pub fn recovery_blocked(mut self, v: bool) -> Self {
        self.add_signal(VortexCommitIntentSignal::RecoveryBlocked, v);
        self
    }
    #[must_use]
    pub fn retry_gate_open(mut self, v: bool) -> Self {
        self.add_signal(VortexCommitIntentSignal::RetryGateOpen, v);
        self
    }
    #[must_use]
    pub fn retry_gate_closed(mut self, v: bool) -> Self {
        self.add_signal(VortexCommitIntentSignal::RetryGateClosed, v);
        self
    }
    #[must_use]
    pub fn cancellation_gate_open(mut self, v: bool) -> Self {
        self.add_signal(VortexCommitIntentSignal::CancellationGateOpen, v);
        self
    }
    #[must_use]
    pub fn cancellation_gate_closed(mut self, v: bool) -> Self {
        self.add_signal(VortexCommitIntentSignal::CancellationGateClosed, v);
        self
    }
    #[must_use]
    pub fn object_store_target(mut self, v: bool) -> Self {
        self.add_signal(VortexCommitIntentSignal::ObjectStoreTarget, v);
        self
    }
    #[must_use]
    pub fn feature_gate_enabled(mut self, v: bool) -> Self {
        self.add_signal(VortexCommitIntentSignal::FeatureGateEnabled, v);
        self
    }
    #[must_use]
    pub fn with_staged_manifest_summary(mut self, v: impl Into<String>) -> Self {
        self.staged_manifest_summary = Some(v.into());
        self
    }
    #[must_use]
    pub fn with_recovery_summary(mut self, v: impl Into<String>) -> Self {
        self.recovery_summary = Some(v.into());
        self
    }
    #[must_use]
    pub fn with_retry_gate_summary(mut self, v: impl Into<String>) -> Self {
        self.retry_gate_summary = Some(v.into());
        self
    }
    #[must_use]
    pub fn with_cancellation_gate_summary(mut self, v: impl Into<String>) -> Self {
        self.cancellation_gate_summary = Some(v.into());
        self
    }
    #[must_use]
    pub fn has_signal(&self, s: VortexCommitIntentSignal) -> bool {
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
pub struct VortexCommitIntentReport {
    pub status: VortexCommitIntentStatus,
    pub mode: VortexCommitIntentMode,
    pub request: VortexCommitIntentRequest,
    pub effects_performed: Vec<VortexCommitIntentEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexCommitIntentReport {
    /// # Errors
    /// Returns an error only if rendering deterministic diagnostics fails unexpectedly.
    pub fn from_request(request: VortexCommitIntentRequest) -> Result<Self> {
        let mut report = Self {
            status: VortexCommitIntentStatus::Planned,
            mode: VortexCommitIntentMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        };
        report.status = if report.object_store_target() {
            VortexCommitIntentStatus::BlockedByObjectStoreTarget
        } else if !report.commit_requested() {
            VortexCommitIntentStatus::BlockedByMissingCommitIntent
        } else if report
            .request
            .has_signal(VortexCommitIntentSignal::StagedManifestDraftMissing)
            || !report.staged_manifest_draft_written()
        {
            VortexCommitIntentStatus::BlockedByStagedManifestDraft
        } else if report
            .request
            .has_signal(VortexCommitIntentSignal::ManifestFinalizationMissing)
            || !report.manifest_finalization_available()
        {
            VortexCommitIntentStatus::BlockedByManifestFinalization
        } else if !report.commit_protocol_available() {
            VortexCommitIntentStatus::BlockedByCommitProtocol
        } else if !report.schema_known() || !report.schema_compatible() {
            VortexCommitIntentStatus::BlockedBySchema
        } else if !report.delete_semantics_known() {
            VortexCommitIntentStatus::BlockedByDeleteSemantics
        } else if !report.tombstone_semantics_known() {
            VortexCommitIntentStatus::BlockedByTombstoneSemantics
        } else if report
            .request
            .has_signal(VortexCommitIntentSignal::RecoveryBlocked)
            || !report.recovery_ready()
        {
            VortexCommitIntentStatus::BlockedByRecovery
        } else if report
            .request
            .has_signal(VortexCommitIntentSignal::RetryGateClosed)
            || !report.retry_gate_open()
        {
            VortexCommitIntentStatus::BlockedByRetryGate
        } else if report
            .request
            .has_signal(VortexCommitIntentSignal::CancellationGateClosed)
            || !report.cancellation_gate_open()
        {
            VortexCommitIntentStatus::BlockedByCancellationGate
        } else if !report
            .request
            .has_signal(VortexCommitIntentSignal::FeatureGateEnabled)
        {
            VortexCommitIntentStatus::BlockedByFeatureGate
        } else {
            VortexCommitIntentStatus::CommitReady
        };
        Ok(report)
    }
    #[must_use]
    pub fn unsupported(
        request: VortexCommitIntentRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self {
            status: VortexCommitIntentStatus::Unsupported,
            mode: VortexCommitIntentMode::Unsupported,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        };
        report.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Commit protocol remains report-only in this phase.".to_string()),
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
    pub fn commit_requested(&self) -> bool {
        self.request
            .has_signal(VortexCommitIntentSignal::CommitRequested)
    }
    #[must_use]
    pub fn staged_manifest_draft_written(&self) -> bool {
        self.request
            .has_signal(VortexCommitIntentSignal::StagedManifestDraftWritten)
    }
    #[must_use]
    pub fn manifest_finalization_available(&self) -> bool {
        self.request
            .has_signal(VortexCommitIntentSignal::ManifestFinalizationAvailable)
    }
    #[must_use]
    pub fn commit_protocol_available(&self) -> bool {
        self.request
            .has_signal(VortexCommitIntentSignal::CommitProtocolAvailable)
    }
    #[must_use]
    pub fn schema_known(&self) -> bool {
        self.request
            .has_signal(VortexCommitIntentSignal::SchemaKnown)
    }
    #[must_use]
    pub fn schema_compatible(&self) -> bool {
        self.request
            .has_signal(VortexCommitIntentSignal::SchemaCompatible)
    }
    #[must_use]
    pub fn delete_semantics_known(&self) -> bool {
        self.request
            .has_signal(VortexCommitIntentSignal::DeleteSemanticsKnown)
    }
    #[must_use]
    pub fn tombstone_semantics_known(&self) -> bool {
        self.request
            .has_signal(VortexCommitIntentSignal::TombstoneSemanticsKnown)
    }
    #[must_use]
    pub fn recovery_ready(&self) -> bool {
        self.request
            .has_signal(VortexCommitIntentSignal::RecoveryReady)
    }
    #[must_use]
    pub fn retry_gate_open(&self) -> bool {
        self.request
            .has_signal(VortexCommitIntentSignal::RetryGateOpen)
    }
    #[must_use]
    pub fn cancellation_gate_open(&self) -> bool {
        self.request
            .has_signal(VortexCommitIntentSignal::CancellationGateOpen)
    }
    #[must_use]
    pub fn object_store_target(&self) -> bool {
        self.request
            .has_signal(VortexCommitIntentSignal::ObjectStoreTarget)
    }
    #[must_use]
    pub const fn manifest_committed(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn manifest_finalized(&self) -> bool {
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
    pub const fn allows_commit_execution(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty() && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut t = String::new();
        let _ = writeln!(t, "Vortex commit intent plan");
        let _ = writeln!(t, "status: {}", self.status.as_str());
        let _ = writeln!(t, "mode: {}", self.mode.as_str());
        let _ = writeln!(t, "target URI: {}", self.request.target_uri.as_str());
        let _ = writeln!(t, "commit requested: {}", self.commit_requested());
        let _ = writeln!(
            t,
            "staged manifest draft written: {}",
            self.staged_manifest_draft_written()
        );
        let _ = writeln!(
            t,
            "manifest finalization available: {}",
            self.manifest_finalization_available()
        );
        let _ = writeln!(
            t,
            "commit protocol available: {}",
            self.commit_protocol_available()
        );
        let _ = writeln!(t, "schema known: {}", self.schema_known());
        let _ = writeln!(t, "schema compatible: {}", self.schema_compatible());
        let _ = writeln!(
            t,
            "delete semantics known: {}",
            self.delete_semantics_known()
        );
        let _ = writeln!(
            t,
            "tombstone semantics known: {}",
            self.tombstone_semantics_known()
        );
        let _ = writeln!(t, "recovery ready: {}", self.recovery_ready());
        let _ = writeln!(t, "retry gate open: {}", self.retry_gate_open());
        let _ = writeln!(
            t,
            "cancellation gate open: {}",
            self.cancellation_gate_open()
        );
        let _ = writeln!(t, "object-store target: {}", self.object_store_target());
        let _ = writeln!(t, "manifest committed: false");
        let _ = writeln!(t, "manifest finalized: false");
        let _ = writeln!(t, "output data written: false");
        let _ = writeln!(t, "object-store IO: false");
        let _ = writeln!(t, "upstream Vortex write called: false");
        let _ = writeln!(t, "recovery action executed: false");
        let _ = writeln!(t, "fallback execution: disabled");
        if !(self.request.diagnostics.is_empty() && self.diagnostics.is_empty()) {
            let _ = writeln!(t, "diagnostics:");
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
/// Propagates errors from [`VortexCommitIntentReport::from_request`].
pub fn plan_vortex_commit_intent(
    request: VortexCommitIntentRequest,
) -> Result<VortexCommitIntentReport> {
    VortexCommitIntentReport::from_request(request)
}

#[must_use]
pub fn vortex_commit_intent_is_side_effect_free(report: &VortexCommitIntentReport) -> bool {
    report.is_side_effect_free()
}

#[must_use]
pub fn commit_intent_request_from_reports(
    target_uri: DatasetUri,
    staged_manifest_write: &VortexStagedManifestFileWriteReport,
) -> VortexCommitIntentRequest {
    let mut request = VortexCommitIntentRequest::new(target_uri)
        .with_staged_manifest_summary(staged_manifest_write.request.summary());
    request.add_signal(
        VortexCommitIntentSignal::StagedManifestDraftWritten,
        staged_manifest_write.draft_file_written(),
    );
    request.add_signal(
        VortexCommitIntentSignal::StagedManifestDraftMissing,
        !staged_manifest_write.draft_file_written(),
    );
    request.add_signal(
        VortexCommitIntentSignal::ObjectStoreTarget,
        staged_manifest_write.object_store_target(),
    );
    request.add_signal(
        VortexCommitIntentSignal::FeatureGateEnabled,
        staged_manifest_write
            .request
            .has_signal(crate::VortexStagedManifestFileWriteSignal::FeatureGateEnabled),
    );
    request
        .diagnostics
        .extend(staged_manifest_write.diagnostics.clone());
    request
}

#[must_use]
pub fn commit_intent_request_with_recovery_report(
    mut request: VortexCommitIntentRequest,
    recovery: &shardloom_exec::recovery::ShardLoomRecoveryIntegrationReport,
) -> VortexCommitIntentRequest {
    let recovery_ready = recovery.is_side_effect_free()
        && !recovery.has_errors()
        && recovery.unknown_artifact_count == 0;
    request = request
        .recovery_ready(recovery_ready)
        .recovery_blocked(!recovery_ready)
        .with_recovery_summary(recovery.to_human_text());
    request.diagnostics.extend(recovery.diagnostics.clone());
    request
}

#[must_use]
pub fn commit_intent_request_with_retry_gate_report(
    mut request: VortexCommitIntentRequest,
    retry_gate: &shardloom_exec::ShardLoomRetryExecutionGateReport,
) -> VortexCommitIntentRequest {
    let retry_gate_open = retry_gate.retry_gate_open()
        && retry_gate.is_side_effect_free()
        && !retry_gate.has_errors();
    request = request
        .retry_gate_open(retry_gate_open)
        .retry_gate_closed(!retry_gate_open)
        .with_retry_gate_summary(retry_gate.to_human_text());
    request.diagnostics.extend(retry_gate.diagnostics.clone());
    request
}

#[must_use]
pub fn commit_intent_request_with_cancellation_gate_report(
    mut request: VortexCommitIntentRequest,
    cancellation_gate: &shardloom_exec::ShardLoomCancellationExecutionGateReport,
) -> VortexCommitIntentRequest {
    let cancellation_gate_open = cancellation_gate.is_side_effect_free()
        && !cancellation_gate.has_errors()
        && (cancellation_gate.cancellation_gate_open()
            || !cancellation_gate.cancellation_requested());
    request = request
        .cancellation_gate_open(cancellation_gate_open)
        .cancellation_gate_closed(!cancellation_gate_open)
        .with_cancellation_gate_summary(cancellation_gate.to_human_text());
    request
        .diagnostics
        .extend(cancellation_gate.diagnostics.clone());
    request
}

#[must_use]
pub fn commit_intent_request_from_readiness_reports(
    target_uri: DatasetUri,
    staged_manifest_write: &crate::VortexStagedManifestFileWriteReport,
    recovery: Option<&shardloom_exec::recovery::ShardLoomRecoveryIntegrationReport>,
    retry_gate: Option<&shardloom_exec::ShardLoomRetryExecutionGateReport>,
    cancellation_gate: Option<&shardloom_exec::ShardLoomCancellationExecutionGateReport>,
) -> VortexCommitIntentRequest {
    let mut request = commit_intent_request_from_reports(target_uri, staged_manifest_write);
    request = match recovery {
        Some(report) => commit_intent_request_with_recovery_report(request, report),
        None => request.recovery_blocked(true),
    };
    request = match retry_gate {
        Some(report) => commit_intent_request_with_retry_gate_report(request, report),
        None => request.retry_gate_closed(true),
    };
    match cancellation_gate {
        Some(report) => commit_intent_request_with_cancellation_gate_report(request, report),
        None => request.cancellation_gate_closed(true),
    }
}

/// # Errors
/// Propagates errors from [`plan_vortex_commit_intent`].
pub fn plan_vortex_commit_intent_from_readiness_reports(
    target_uri: DatasetUri,
    staged_manifest_write: &crate::VortexStagedManifestFileWriteReport,
    recovery: Option<&shardloom_exec::recovery::ShardLoomRecoveryIntegrationReport>,
    retry_gate: Option<&shardloom_exec::ShardLoomRetryExecutionGateReport>,
    cancellation_gate: Option<&shardloom_exec::ShardLoomCancellationExecutionGateReport>,
) -> Result<VortexCommitIntentReport> {
    let request = commit_intent_request_from_readiness_reports(
        target_uri,
        staged_manifest_write,
        recovery,
        retry_gate,
        cancellation_gate,
    );
    plan_vortex_commit_intent(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_exec::{
        ShardLoomCancellationExecutionGateRequest, ShardLoomRetryExecutionGateRequest,
        plan_cancellation_execution_gate, plan_retry_execution_gate,
        recovery::{
            RecoveryArtifactRef, ShardLoomRecoveryIntegrationReport,
            ShardLoomRecoveryIntegrationRequest,
        },
    };
    fn uri() -> DatasetUri {
        DatasetUri::new("file:///tmp/t.vortex").expect("uri")
    }
    fn ready() -> VortexCommitIntentRequest {
        VortexCommitIntentRequest::new(uri())
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
            .feature_gate_enabled(true)
    }
    #[test]
    fn status_ready_disallows_exec() {
        assert!(!VortexCommitIntentStatus::CommitReady.allows_commit_execution());
    }
    #[test]
    fn status_blocked_is_error() {
        assert!(VortexCommitIntentStatus::BlockedByCommitProtocol.is_error());
    }
    #[test]
    fn mode_report_only_no_commit_write() {
        assert!(!VortexCommitIntentMode::ReportOnly.commits_manifest());
        assert!(!VortexCommitIntentMode::ReportOnly.writes_output_data());
    }
    #[test]
    fn request_and_signal_dedupe() {
        let mut r = VortexCommitIntentRequest::new(uri());
        r.add_signal(VortexCommitIntentSignal::CommitRequested, true);
        r.add_signal(VortexCommitIntentSignal::CommitRequested, true);
        assert_eq!(r.signals.len(), 1);
        r.add_signal(VortexCommitIntentSignal::CommitRequested, false);
        assert!(!r.has_signal(VortexCommitIntentSignal::CommitRequested));
    }
    #[test]
    fn blockers_object_store_commit_and_staged() {
        assert_eq!(
            VortexCommitIntentReport::from_request(ready().object_store_target(true))
                .expect("r")
                .status,
            VortexCommitIntentStatus::BlockedByObjectStoreTarget
        );
        assert_eq!(
            VortexCommitIntentReport::from_request(VortexCommitIntentRequest::new(uri()))
                .expect("r")
                .status,
            VortexCommitIntentStatus::BlockedByMissingCommitIntent
        );
        assert_eq!(
            VortexCommitIntentReport::from_request(
                VortexCommitIntentRequest::new(uri()).commit_requested(true)
            )
            .expect("r")
            .status,
            VortexCommitIntentStatus::BlockedByStagedManifestDraft
        );
        assert_eq!(
            VortexCommitIntentReport::from_request(
                VortexCommitIntentRequest::new(uri())
                    .commit_requested(true)
                    .staged_manifest_draft_written(true)
            )
            .expect("r")
            .status,
            VortexCommitIntentStatus::BlockedByManifestFinalization
        );
    }

    #[test]
    fn blockers_manifest_commit_protocol_and_schema() {
        assert_eq!(
            VortexCommitIntentReport::from_request(
                VortexCommitIntentRequest::new(uri())
                    .commit_requested(true)
                    .staged_manifest_draft_written(true)
                    .manifest_finalization_available(true)
            )
            .expect("r")
            .status,
            VortexCommitIntentStatus::BlockedByCommitProtocol
        );
        assert_eq!(
            VortexCommitIntentReport::from_request(
                VortexCommitIntentRequest::new(uri())
                    .commit_requested(true)
                    .staged_manifest_draft_written(true)
                    .manifest_finalization_available(true)
                    .commit_protocol_available(true)
            )
            .expect("r")
            .status,
            VortexCommitIntentStatus::BlockedBySchema
        );
        assert_eq!(
            VortexCommitIntentReport::from_request(
                VortexCommitIntentRequest::new(uri())
                    .commit_requested(true)
                    .staged_manifest_draft_written(true)
                    .manifest_finalization_available(true)
                    .commit_protocol_available(true)
                    .schema_known(true)
            )
            .expect("r")
            .status,
            VortexCommitIntentStatus::BlockedBySchema
        );
        assert_eq!(
            VortexCommitIntentReport::from_request(
                VortexCommitIntentRequest::new(uri())
                    .commit_requested(true)
                    .staged_manifest_draft_written(true)
                    .manifest_finalization_available(true)
                    .commit_protocol_available(true)
                    .schema_known(true)
                    .schema_compatible(true)
            )
            .expect("r")
            .status,
            VortexCommitIntentStatus::BlockedByDeleteSemantics
        );
        assert_eq!(
            VortexCommitIntentReport::from_request(
                VortexCommitIntentRequest::new(uri())
                    .commit_requested(true)
                    .staged_manifest_draft_written(true)
                    .manifest_finalization_available(true)
                    .commit_protocol_available(true)
                    .schema_known(true)
                    .schema_compatible(true)
                    .delete_semantics_known(true)
            )
            .expect("r")
            .status,
            VortexCommitIntentStatus::BlockedByTombstoneSemantics
        );
    }

    #[test]
    fn blockers_delete_tombstone_recovery_retry_cancel() {
        assert_eq!(
            VortexCommitIntentReport::from_request(
                VortexCommitIntentRequest::new(uri())
                    .commit_requested(true)
                    .staged_manifest_draft_written(true)
                    .manifest_finalization_available(true)
                    .commit_protocol_available(true)
                    .schema_known(true)
                    .schema_compatible(true)
                    .delete_semantics_known(true)
                    .tombstone_semantics_known(true)
                    .recovery_blocked(true)
            )
            .expect("r")
            .status,
            VortexCommitIntentStatus::BlockedByRecovery
        );
        assert_eq!(
            VortexCommitIntentReport::from_request(
                VortexCommitIntentRequest::new(uri())
                    .commit_requested(true)
                    .staged_manifest_draft_written(true)
                    .manifest_finalization_available(true)
                    .commit_protocol_available(true)
                    .schema_known(true)
                    .schema_compatible(true)
                    .delete_semantics_known(true)
                    .tombstone_semantics_known(true)
                    .recovery_ready(true)
                    .retry_gate_closed(true)
            )
            .expect("r")
            .status,
            VortexCommitIntentStatus::BlockedByRetryGate
        );
        assert_eq!(
            VortexCommitIntentReport::from_request(
                VortexCommitIntentRequest::new(uri())
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
                    .cancellation_gate_closed(true)
                    .feature_gate_enabled(true)
            )
            .expect("r")
            .status,
            VortexCommitIntentStatus::BlockedByCancellationGate
        );
        assert_eq!(
            VortexCommitIntentReport::from_request(
                VortexCommitIntentRequest::new(uri())
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
            )
            .expect("r")
            .status,
            VortexCommitIntentStatus::BlockedByFeatureGate
        );
    }

    #[test]
    fn ready_report_has_no_effects_and_human_text() {
        let rep = VortexCommitIntentReport::from_request(ready()).expect("r");
        assert_eq!(rep.status, VortexCommitIntentStatus::CommitReady);
        assert!(!rep.allows_commit_execution());
        assert!(!rep.manifest_committed());
        assert!(!rep.manifest_finalized());
        assert!(!rep.output_data_written());
        assert!(!rep.object_store_io());
        assert!(!rep.upstream_vortex_write_called());
        assert!(!rep.recovery_action_executed());
        assert!(!rep.fallback_execution_allowed());
        assert!(rep.is_side_effect_free());
        let txt = rep.to_human_text();
        assert!(txt.contains("fallback execution: disabled"));
        assert!(txt.contains("manifest committed: false"));
    }
    #[test]
    fn diagnostics_rendered_and_plan_helper() {
        let mut req = ready();
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
        let rep = plan_vortex_commit_intent(req).expect("ok");
        assert!(rep.has_errors());
        assert!(rep.to_human_text().contains("diagnostics:"));
    }
    #[test]
    fn request_from_stage_report_maps_draft_only() {
        let file_req = crate::VortexStagedManifestFileWriteRequest::new(
            crate::VortexStagedManifestFileRef::new(
                crate::VortexStagedWorkspacePath::new("/tmp/w").expect("p"),
                crate::VortexStagedManifestFileName::new("manifest.draft").expect("n"),
            ),
            crate::VortexStagedManifestDraftContent::new("d").expect("c"),
        )
        .file_plan_ready(true)
        .workspace_known(true)
        .feature_gate_enabled(true);
        let stage =
            crate::VortexStagedManifestFileWriteReport::from_request(file_req).expect("stage");
        let mut stage = stage;
        stage
            .effects_performed
            .push(crate::VortexStagedManifestFileWriteEffect::DraftFileWritten);
        let req = commit_intent_request_from_reports(uri(), &stage);
        assert!(req.has_signal(VortexCommitIntentSignal::StagedManifestDraftWritten));
        assert!(req.has_signal(VortexCommitIntentSignal::FeatureGateEnabled));
        let rep = VortexCommitIntentReport::from_request(req.commit_requested(true)).expect("rep");
        assert!(!rep.manifest_committed());
    }

    #[test]
    fn readiness_helpers_map_recovery_retry_and_cancellation() {
        let recovery = ShardLoomRecoveryIntegrationReport::from_request(
            ShardLoomRecoveryIntegrationRequest::new(),
        )
        .expect("recovery");
        let req = commit_intent_request_with_recovery_report(
            VortexCommitIntentRequest::new(uri()),
            &recovery,
        );
        assert!(req.has_signal(VortexCommitIntentSignal::RecoveryReady));

        let mut recovery_unknown_req = ShardLoomRecoveryIntegrationRequest::new();
        recovery_unknown_req.add_artifact(RecoveryArtifactRef::unknown(
            "artifact-unknown",
            "unknown artifact",
        ));
        let recovery_unknown =
            ShardLoomRecoveryIntegrationReport::from_request(recovery_unknown_req).expect("report");
        let req = commit_intent_request_with_recovery_report(
            VortexCommitIntentRequest::new(uri()),
            &recovery_unknown,
        );
        assert!(req.has_signal(VortexCommitIntentSignal::RecoveryBlocked));

        let retry_open = plan_retry_execution_gate(
            ShardLoomRetryExecutionGateRequest::new()
                .retry_requested(true)
                .retry_allowed_by_plan(true),
        )
        .expect("retry open");
        let req = commit_intent_request_with_retry_gate_report(
            VortexCommitIntentRequest::new(uri()),
            &retry_open,
        );
        assert!(req.has_signal(VortexCommitIntentSignal::RetryGateOpen));

        let retry_closed = plan_retry_execution_gate(ShardLoomRetryExecutionGateRequest::new())
            .expect("retry closed");
        let req = commit_intent_request_with_retry_gate_report(
            VortexCommitIntentRequest::new(uri()),
            &retry_closed,
        );
        assert!(req.has_signal(VortexCommitIntentSignal::RetryGateClosed));

        let cancellation_open = plan_cancellation_execution_gate(
            ShardLoomCancellationExecutionGateRequest::new().cancellation_requested(true),
        )
        .expect("cancel open");
        let req = commit_intent_request_with_cancellation_gate_report(
            VortexCommitIntentRequest::new(uri()),
            &cancellation_open,
        );
        assert!(req.has_signal(VortexCommitIntentSignal::CancellationGateOpen));

        let cancellation_not_requested =
            plan_cancellation_execution_gate(ShardLoomCancellationExecutionGateRequest::new())
                .expect("cancel not requested");
        let req = commit_intent_request_with_cancellation_gate_report(
            VortexCommitIntentRequest::new(uri()),
            &cancellation_not_requested,
        );
        assert!(req.has_signal(VortexCommitIntentSignal::CancellationGateOpen));

        let cancellation_blocked = plan_cancellation_execution_gate(
            ShardLoomCancellationExecutionGateRequest::new()
                .cancellation_requested(true)
                .cleanup_required(true),
        )
        .expect("cancel blocked");
        let req = commit_intent_request_with_cancellation_gate_report(
            VortexCommitIntentRequest::new(uri()),
            &cancellation_blocked,
        );
        assert!(req.has_signal(VortexCommitIntentSignal::CancellationGateClosed));
    }

    #[test]
    fn readiness_combined_helpers_keep_staged_manifest_and_block_on_missing_inputs() {
        let file_req = crate::VortexStagedManifestFileWriteRequest::new(
            crate::VortexStagedManifestFileRef::new(
                crate::VortexStagedWorkspacePath::new("/tmp/w").expect("p"),
                crate::VortexStagedManifestFileName::new("manifest.draft").expect("n"),
            ),
            crate::VortexStagedManifestDraftContent::new("d").expect("c"),
        )
        .file_plan_ready(true)
        .workspace_known(true);
        let mut staged =
            crate::VortexStagedManifestFileWriteReport::from_request(file_req).expect("stage");
        staged
            .effects_performed
            .push(crate::VortexStagedManifestFileWriteEffect::DraftFileWritten);

        let request =
            commit_intent_request_from_readiness_reports(uri(), &staged, None, None, None);
        assert!(request.has_signal(VortexCommitIntentSignal::StagedManifestDraftWritten));
        let report =
            plan_vortex_commit_intent_from_readiness_reports(uri(), &staged, None, None, None)
                .expect("report");
        assert_ne!(report.status, VortexCommitIntentStatus::CommitReady);
        assert!(report.is_side_effect_free());
        assert!(!report.manifest_committed());
        assert!(!report.manifest_finalized());
        assert!(!report.output_data_written());
        assert!(!report.object_store_io());
        assert!(!report.upstream_vortex_write_called());
        assert!(!report.recovery_action_executed());
        assert!(!report.fallback_execution_allowed());
    }
}
