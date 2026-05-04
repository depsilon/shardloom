use std::fmt::Write as _;

use shardloom_core::{DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, Result};

use crate::{
    VortexStagedMarkerReport, VortexStagedOutputReport, VortexStagedWorkspaceSetupReport,
    VortexWriteIntentReport,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStagedManifestDraftStatus {
    Planned,
    DraftReady,
    BlockedByWriteIntent,
    BlockedByStagedOutput,
    BlockedByWorkspace,
    BlockedByMarker,
    BlockedBySchema,
    BlockedByDeleteSemantics,
    BlockedByTombstoneSemantics,
    BlockedByCommitProtocol,
    BlockedByObjectStoreTarget,
    BlockedByFeatureGate,
    Unsupported,
}
impl VortexStagedManifestDraftStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::DraftReady => "draft_ready",
            Self::BlockedByWriteIntent => "blocked_by_write_intent",
            Self::BlockedByStagedOutput => "blocked_by_staged_output",
            Self::BlockedByWorkspace => "blocked_by_workspace",
            Self::BlockedByMarker => "blocked_by_marker",
            Self::BlockedBySchema => "blocked_by_schema",
            Self::BlockedByDeleteSemantics => "blocked_by_delete_semantics",
            Self::BlockedByTombstoneSemantics => "blocked_by_tombstone_semantics",
            Self::BlockedByCommitProtocol => "blocked_by_commit_protocol",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByFeatureGate => "blocked_by_feature_gate",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::Planned | Self::DraftReady)
    }
    #[must_use]
    pub const fn allows_manifest_write(self) -> bool {
        false
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStagedManifestDraftMode {
    ReportOnly,
    ManifestDraftPlanning,
    Unsupported,
}
impl VortexStagedManifestDraftMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::ManifestDraftPlanning => "manifest_draft_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn writes_manifest(self) -> bool {
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
    pub const fn commits_manifest(self) -> bool {
        false
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStagedManifestDraftSignal {
    WriteIntentPlanned,
    WriteIntentBlocked,
    StagedOutputPlanned,
    StagedOutputBlocked,
    WorkspaceKnown,
    WorkspaceMissing,
    MarkerWritten,
    MarkerMissing,
    SchemaKnown,
    SchemaCompatible,
    DeleteSemanticsKnown,
    TombstoneSemanticsKnown,
    CommitProtocolAvailable,
    ObjectStoreTarget,
    FeatureGateEnabled,
}
impl VortexStagedManifestDraftSignal {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WriteIntentPlanned => "write_intent_planned",
            Self::WriteIntentBlocked => "write_intent_blocked",
            Self::StagedOutputPlanned => "staged_output_planned",
            Self::StagedOutputBlocked => "staged_output_blocked",
            Self::WorkspaceKnown => "workspace_known",
            Self::WorkspaceMissing => "workspace_missing",
            Self::MarkerWritten => "marker_written",
            Self::MarkerMissing => "marker_missing",
            Self::SchemaKnown => "schema_known",
            Self::SchemaCompatible => "schema_compatible",
            Self::DeleteSemanticsKnown => "delete_semantics_known",
            Self::TombstoneSemanticsKnown => "tombstone_semantics_known",
            Self::CommitProtocolAvailable => "commit_protocol_available",
            Self::ObjectStoreTarget => "object_store_target",
            Self::FeatureGateEnabled => "feature_gate_enabled",
        }
    }
    #[must_use]
    pub const fn is_blocking(self) -> bool {
        matches!(
            self,
            Self::WriteIntentBlocked
                | Self::StagedOutputBlocked
                | Self::WorkspaceMissing
                | Self::MarkerMissing
                | Self::ObjectStoreTarget
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStagedManifestDraftEffect {
    ManifestWritten,
    OutputDataWritten,
    ObjectStoreIo,
    UpstreamVortexWriteCalled,
    CommitPerformed,
    FallbackExecution,
}
impl VortexStagedManifestDraftEffect {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ManifestWritten => "manifest_written",
            Self::OutputDataWritten => "output_data_written",
            Self::ObjectStoreIo => "object_store_io",
            Self::UpstreamVortexWriteCalled => "upstream_vortex_write_called",
            Self::CommitPerformed => "commit_performed",
            Self::FallbackExecution => "fallback_execution",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexStagedManifestDraftRequest {
    pub target_uri: DatasetUri,
    pub signals: Vec<VortexStagedManifestDraftSignal>,
    pub write_intent_summary: Option<String>,
    pub staged_output_summary: Option<String>,
    pub workspace_summary: Option<String>,
    pub marker_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
macro_rules! signal_builder {
    ($name:ident,$sig:expr) => {
        #[must_use]
        pub fn $name(mut self, v: bool) -> Self {
            self.add_signal($sig, v);
            self
        }
    };
}
impl VortexStagedManifestDraftRequest {
    #[must_use]
    pub fn new(target_uri: DatasetUri) -> Self {
        Self {
            target_uri,
            signals: Vec::new(),
            write_intent_summary: None,
            staged_output_summary: None,
            workspace_summary: None,
            marker_summary: None,
            diagnostics: Vec::new(),
        }
    }
    pub fn add_signal(&mut self, signal: VortexStagedManifestDraftSignal, value: bool) {
        if value {
            if !self.signals.contains(&signal) {
                self.signals.push(signal);
            }
        } else {
            self.signals.retain(|s| *s != signal);
        }
    }
    signal_builder!(
        write_intent_planned,
        VortexStagedManifestDraftSignal::WriteIntentPlanned
    );
    signal_builder!(
        write_intent_blocked,
        VortexStagedManifestDraftSignal::WriteIntentBlocked
    );
    signal_builder!(
        staged_output_planned,
        VortexStagedManifestDraftSignal::StagedOutputPlanned
    );
    signal_builder!(
        staged_output_blocked,
        VortexStagedManifestDraftSignal::StagedOutputBlocked
    );
    signal_builder!(
        workspace_known,
        VortexStagedManifestDraftSignal::WorkspaceKnown
    );
    signal_builder!(
        workspace_missing,
        VortexStagedManifestDraftSignal::WorkspaceMissing
    );
    signal_builder!(
        marker_written,
        VortexStagedManifestDraftSignal::MarkerWritten
    );
    signal_builder!(
        marker_missing,
        VortexStagedManifestDraftSignal::MarkerMissing
    );
    signal_builder!(schema_known, VortexStagedManifestDraftSignal::SchemaKnown);
    signal_builder!(
        schema_compatible,
        VortexStagedManifestDraftSignal::SchemaCompatible
    );
    signal_builder!(
        delete_semantics_known,
        VortexStagedManifestDraftSignal::DeleteSemanticsKnown
    );
    signal_builder!(
        tombstone_semantics_known,
        VortexStagedManifestDraftSignal::TombstoneSemanticsKnown
    );
    signal_builder!(
        commit_protocol_available,
        VortexStagedManifestDraftSignal::CommitProtocolAvailable
    );
    signal_builder!(
        object_store_target,
        VortexStagedManifestDraftSignal::ObjectStoreTarget
    );
    signal_builder!(
        feature_gate_enabled,
        VortexStagedManifestDraftSignal::FeatureGateEnabled
    );
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
    pub fn with_workspace_summary(mut self, v: impl Into<String>) -> Self {
        self.workspace_summary = Some(v.into());
        self
    }
    #[must_use]
    pub fn with_marker_summary(mut self, v: impl Into<String>) -> Self {
        self.marker_summary = Some(v.into());
        self
    }
    #[must_use]
    pub fn has_signal(&self, s: VortexStagedManifestDraftSignal) -> bool {
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
pub struct VortexStagedManifestDraftReport {
    pub status: VortexStagedManifestDraftStatus,
    pub mode: VortexStagedManifestDraftMode,
    pub request: VortexStagedManifestDraftRequest,
    pub effects_performed: Vec<VortexStagedManifestDraftEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexStagedManifestDraftReport {
    /// # Errors
    /// Returns an error only if diagnostic rendering fails unexpectedly.
    pub fn from_request(request: VortexStagedManifestDraftRequest) -> Result<Self> {
        let mut r = Self {
            status: VortexStagedManifestDraftStatus::Planned,
            mode: VortexStagedManifestDraftMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        };
        r.status = if r.object_store_target() {
            VortexStagedManifestDraftStatus::BlockedByObjectStoreTarget
        } else if r
            .request
            .has_signal(VortexStagedManifestDraftSignal::WriteIntentBlocked)
        {
            VortexStagedManifestDraftStatus::BlockedByWriteIntent
        } else if r
            .request
            .has_signal(VortexStagedManifestDraftSignal::StagedOutputBlocked)
        {
            VortexStagedManifestDraftStatus::BlockedByStagedOutput
        } else if r
            .request
            .has_signal(VortexStagedManifestDraftSignal::WorkspaceMissing)
            || !r.workspace_known()
        {
            VortexStagedManifestDraftStatus::BlockedByWorkspace
        } else if r
            .request
            .has_signal(VortexStagedManifestDraftSignal::MarkerMissing)
            || !r.marker_written()
        {
            VortexStagedManifestDraftStatus::BlockedByMarker
        } else if !r.schema_known() || !r.schema_compatible() {
            VortexStagedManifestDraftStatus::BlockedBySchema
        } else if !r.delete_semantics_known() {
            VortexStagedManifestDraftStatus::BlockedByDeleteSemantics
        } else if !r.tombstone_semantics_known() {
            VortexStagedManifestDraftStatus::BlockedByTombstoneSemantics
        } else if !r.commit_protocol_available() {
            VortexStagedManifestDraftStatus::BlockedByCommitProtocol
        } else {
            VortexStagedManifestDraftStatus::DraftReady
        };
        Ok(r)
    }
    #[must_use]
    pub fn unsupported(
        request: VortexStagedManifestDraftRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut r = Self {
            status: VortexStagedManifestDraftStatus::Unsupported,
            mode: VortexStagedManifestDraftMode::Unsupported,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        };
        r.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::UnsupportedOutputFormat,
            feature,
            reason,
            None,
        ));
        r
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
    pub fn write_intent_planned(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestDraftSignal::WriteIntentPlanned)
    }
    #[must_use]
    pub fn staged_output_planned(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestDraftSignal::StagedOutputPlanned)
    }
    #[must_use]
    pub fn workspace_known(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestDraftSignal::WorkspaceKnown)
    }
    #[must_use]
    pub fn marker_written(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestDraftSignal::MarkerWritten)
    }
    #[must_use]
    pub fn schema_known(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestDraftSignal::SchemaKnown)
    }
    #[must_use]
    pub fn schema_compatible(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestDraftSignal::SchemaCompatible)
    }
    #[must_use]
    pub fn delete_semantics_known(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestDraftSignal::DeleteSemanticsKnown)
    }
    #[must_use]
    pub fn tombstone_semantics_known(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestDraftSignal::TombstoneSemanticsKnown)
    }
    #[must_use]
    pub fn commit_protocol_available(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestDraftSignal::CommitProtocolAvailable)
    }
    #[must_use]
    pub fn object_store_target(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestDraftSignal::ObjectStoreTarget)
    }
    #[must_use]
    pub const fn manifest_written(&self) -> bool {
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
    pub const fn commit_performed(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn allows_manifest_write(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty() && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut t = String::new();
        let _ = writeln!(t, "Vortex staged manifest draft plan");
        let _ = writeln!(t, "status: {}", self.status.as_str());
        let _ = writeln!(t, "mode: {}", self.mode.as_str());
        let _ = writeln!(t, "target URI: {}", self.request.target_uri.as_str());
        let _ = writeln!(t, "write intent planned: {}", self.write_intent_planned());
        let _ = writeln!(t, "staged output planned: {}", self.staged_output_planned());
        let _ = writeln!(t, "workspace known: {}", self.workspace_known());
        let _ = writeln!(t, "marker written: {}", self.marker_written());
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
        let _ = writeln!(
            t,
            "commit protocol available: {}",
            self.commit_protocol_available()
        );
        let _ = writeln!(t, "object-store target: {}", self.object_store_target());
        let _ = writeln!(t, "manifest written: false");
        let _ = writeln!(t, "output data written: false");
        let _ = writeln!(t, "object-store IO: false");
        let _ = writeln!(t, "upstream Vortex write called: false");
        let _ = writeln!(t, "commit performed: false");
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
/// Propagates errors from `VortexStagedManifestDraftReport::from_request`.
pub fn plan_vortex_staged_manifest_draft(
    request: VortexStagedManifestDraftRequest,
) -> Result<VortexStagedManifestDraftReport> {
    VortexStagedManifestDraftReport::from_request(request)
}
#[must_use]
pub fn vortex_staged_manifest_draft_is_side_effect_free(
    report: &VortexStagedManifestDraftReport,
) -> bool {
    report.is_side_effect_free()
}

#[must_use]
pub fn staged_manifest_request_from_reports(
    target_uri: DatasetUri,
    write_intent: &VortexWriteIntentReport,
    staged_output: &VortexStagedOutputReport,
    workspace: Option<&VortexStagedWorkspaceSetupReport>,
    marker: Option<&VortexStagedMarkerReport>,
) -> VortexStagedManifestDraftRequest {
    let mut req = VortexStagedManifestDraftRequest::new(target_uri)
        .with_write_intent_summary(write_intent.request.summary())
        .with_staged_output_summary(staged_output.request.summary());
    req.add_signal(
        VortexStagedManifestDraftSignal::WriteIntentBlocked,
        write_intent.has_errors(),
    );
    req.add_signal(
        VortexStagedManifestDraftSignal::WriteIntentPlanned,
        !write_intent.has_errors()
            && (matches!(
                write_intent.status,
                crate::VortexWriteIntentStatus::Planned
                    | crate::VortexWriteIntentStatus::StagedOutputRequired
            ) || write_intent.staged_output_required()),
    );
    req.add_signal(
        VortexStagedManifestDraftSignal::StagedOutputBlocked,
        staged_output.has_errors(),
    );
    req.add_signal(
        VortexStagedManifestDraftSignal::StagedOutputPlanned,
        !staged_output.has_errors(),
    );
    if let Some(w) = workspace {
        req.workspace_summary = Some(w.request.summary());
        req.add_signal(
            VortexStagedManifestDraftSignal::WorkspaceKnown,
            w.workspace_created() || !w.has_errors(),
        );
        req.add_signal(
            VortexStagedManifestDraftSignal::WorkspaceMissing,
            w.has_errors(),
        );
    } else {
        req.add_signal(VortexStagedManifestDraftSignal::WorkspaceMissing, true);
    }
    if let Some(m) = marker {
        req.marker_summary = Some(m.request.summary());
        req.add_signal(
            VortexStagedManifestDraftSignal::MarkerWritten,
            m.marker_written(),
        );
        req.add_signal(
            VortexStagedManifestDraftSignal::MarkerMissing,
            !m.marker_written(),
        );
    } else {
        req.add_signal(VortexStagedManifestDraftSignal::MarkerMissing, true);
    }
    req.add_signal(
        VortexStagedManifestDraftSignal::SchemaKnown,
        write_intent.schema_known(),
    );
    req.add_signal(
        VortexStagedManifestDraftSignal::SchemaCompatible,
        write_intent.schema_compatible(),
    );
    req.add_signal(
        VortexStagedManifestDraftSignal::DeleteSemanticsKnown,
        write_intent.delete_semantics_known(),
    );
    req.add_signal(
        VortexStagedManifestDraftSignal::TombstoneSemanticsKnown,
        write_intent.tombstone_semantics_known(),
    );
    req.add_signal(
        VortexStagedManifestDraftSignal::CommitProtocolAvailable,
        write_intent.commit_protocol_available(),
    );
    req.add_signal(
        VortexStagedManifestDraftSignal::ObjectStoreTarget,
        write_intent.object_store_target(),
    );
    req
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        VortexStagedMarkerMode, VortexStagedMarkerRequest, VortexStagedMarkerStatus,
        VortexStagedOutputRequest, VortexWriteIntentRequest,
    };
    fn uri() -> DatasetUri {
        DatasetUri::new("file://tmp/out.vortex").unwrap()
    }
    fn base_req() -> VortexStagedManifestDraftRequest {
        VortexStagedManifestDraftRequest::new(uri())
            .write_intent_planned(true)
            .staged_output_planned(true)
            .workspace_known(true)
            .marker_written(true)
            .schema_known(true)
            .schema_compatible(true)
            .delete_semantics_known(true)
            .tombstone_semantics_known(true)
            .commit_protocol_available(true)
    }
    #[test]
    fn basics() {
        assert!(!VortexStagedManifestDraftStatus::DraftReady.allows_manifest_write());
        assert!(VortexStagedManifestDraftStatus::BlockedBySchema.is_error());
        assert!(!VortexStagedManifestDraftMode::ReportOnly.writes_manifest());
        assert!(!VortexStagedManifestDraftMode::ReportOnly.writes_output_data());
    }
    #[test]
    fn signal_dedupe() {
        let mut r = VortexStagedManifestDraftRequest::new(uri());
        r.add_signal(VortexStagedManifestDraftSignal::SchemaKnown, true);
        r.add_signal(VortexStagedManifestDraftSignal::SchemaKnown, true);
        assert_eq!(r.signals.len(), 1);
        r.add_signal(VortexStagedManifestDraftSignal::SchemaKnown, false);
        assert!(r.signals.is_empty());
    }
    #[test]
    fn status_priority_and_side_effects() {
        assert_eq!(
            VortexStagedManifestDraftReport::from_request(base_req().object_store_target(true))
                .unwrap()
                .status,
            VortexStagedManifestDraftStatus::BlockedByObjectStoreTarget
        );
        assert_eq!(
            VortexStagedManifestDraftReport::from_request(base_req().write_intent_blocked(true))
                .unwrap()
                .status,
            VortexStagedManifestDraftStatus::BlockedByWriteIntent
        );
        assert_eq!(
            VortexStagedManifestDraftReport::from_request(base_req().staged_output_blocked(true))
                .unwrap()
                .status,
            VortexStagedManifestDraftStatus::BlockedByStagedOutput
        );
        assert_eq!(
            VortexStagedManifestDraftReport::from_request(base_req().workspace_known(false))
                .unwrap()
                .status,
            VortexStagedManifestDraftStatus::BlockedByWorkspace
        );
        assert_eq!(
            VortexStagedManifestDraftReport::from_request(base_req().marker_written(false))
                .unwrap()
                .status,
            VortexStagedManifestDraftStatus::BlockedByMarker
        );
        assert_eq!(
            VortexStagedManifestDraftReport::from_request(base_req().schema_known(false))
                .unwrap()
                .status,
            VortexStagedManifestDraftStatus::BlockedBySchema
        );
        assert_eq!(
            VortexStagedManifestDraftReport::from_request(base_req().schema_compatible(false))
                .unwrap()
                .status,
            VortexStagedManifestDraftStatus::BlockedBySchema
        );
        assert_eq!(
            VortexStagedManifestDraftReport::from_request(base_req().delete_semantics_known(false))
                .unwrap()
                .status,
            VortexStagedManifestDraftStatus::BlockedByDeleteSemantics
        );
        assert_eq!(
            VortexStagedManifestDraftReport::from_request(
                base_req().tombstone_semantics_known(false)
            )
            .unwrap()
            .status,
            VortexStagedManifestDraftStatus::BlockedByTombstoneSemantics
        );
        assert_eq!(
            VortexStagedManifestDraftReport::from_request(
                base_req().commit_protocol_available(false)
            )
            .unwrap()
            .status,
            VortexStagedManifestDraftStatus::BlockedByCommitProtocol
        );
        let rep = VortexStagedManifestDraftReport::from_request(base_req()).unwrap();
        assert_eq!(rep.status, VortexStagedManifestDraftStatus::DraftReady);
        assert!(!rep.allows_manifest_write());
        assert!(!rep.manifest_written());
        assert!(!rep.output_data_written());
        assert!(!rep.object_store_io());
        assert!(!rep.upstream_vortex_write_called());
        assert!(!rep.commit_performed());
        assert!(!rep.fallback_execution_allowed());
        assert!(rep.is_side_effect_free());
        let text = rep.to_human_text();
        assert!(text.contains("fallback execution: disabled"));
        assert!(text.contains("manifest written: false"));
    }
    #[test]
    fn diagnostics_rendered() {
        let mut req = base_req();
        req.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::UnsupportedOutputFormat,
            "staged_manifest",
            "x",
            None,
        ));
        let text = VortexStagedManifestDraftReport::from_request(req)
            .unwrap()
            .to_human_text();
        assert!(text.contains("diagnostics:"));
    }
    #[test]
    fn plan_no_io() {
        let _ = plan_vortex_staged_manifest_draft(base_req()).unwrap();
    }
    #[test]
    fn from_reports_marker_mapping() {
        let wi = crate::plan_vortex_write_intent(
            VortexWriteIntentRequest::new(uri())
                .target_is_native_vortex(true)
                .schema_known(true)
                .schema_compatible(true)
                .delete_semantics_known(true)
                .tombstone_semantics_known(true)
                .commit_protocol_available(true)
                .staged_output_required(true),
        )
        .unwrap();
        let so = crate::plan_vortex_staged_output(
            VortexStagedOutputRequest::new(
                crate::VortexStagedWorkspaceId::new("ws").unwrap(),
                uri(),
            )
            .write_intent_planned(true)
            .workspace_required(true)
            .workspace_path_known(true)
            .local_workspace(true)
            .commit_protocol_available(true),
        )
        .unwrap();
        let req = staged_manifest_request_from_reports(uri(), &wi, &so, None, None);
        assert!(req.has_signal(VortexStagedManifestDraftSignal::MarkerMissing));
        let mut marker = crate::VortexStagedMarkerReport::planned(VortexStagedMarkerRequest::new(
            crate::VortexStagedWorkspaceId::new("ws").unwrap(),
            crate::VortexStagedWorkspacePath::new("/tmp/ws").unwrap(),
        ));
        marker.status = VortexStagedMarkerStatus::MarkerWritten;
        marker.mode = VortexStagedMarkerMode::LocalMarkerWrite;
        marker
            .effects_performed
            .push(crate::VortexStagedOutputEffect::MarkerWritten);
        let req2 = staged_manifest_request_from_reports(uri(), &wi, &so, None, Some(&marker));
        assert!(req2.has_signal(VortexStagedManifestDraftSignal::MarkerWritten));
        let blocked_wi = crate::VortexWriteIntentReport::unsupported(
            VortexWriteIntentRequest::new(uri()),
            "f",
            "r",
        );
        let req3 = staged_manifest_request_from_reports(uri(), &blocked_wi, &so, None, None);
        assert!(req3.has_signal(VortexStagedManifestDraftSignal::WriteIntentBlocked));
    }
}
