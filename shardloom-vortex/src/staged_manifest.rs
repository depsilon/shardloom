use std::fmt::Write as _;
#[cfg(feature = "vortex-staged-output-fs")]
use std::{fs, path::PathBuf};

#[cfg(feature = "vortex-staged-output-fs")]
use shardloom_core::UriScheme;
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
        let workspace_usable = matches!(
            w.status,
            crate::VortexStagedWorkspaceSetupStatus::Planned
                | crate::VortexStagedWorkspaceSetupStatus::WorkspaceCreated
        );
        req.add_signal(
            VortexStagedManifestDraftSignal::WorkspaceKnown,
            workspace_usable && !w.has_errors(),
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

const STAGED_MANIFEST_DRAFT_MAX_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStagedManifestFileStatus {
    Planned,
    FileReady,
    BlockedByDraft,
    BlockedByWorkspace,
    BlockedByMarker,
    BlockedByObjectStoreTarget,
    Unsupported,
}
impl VortexStagedManifestFileStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::FileReady => "file_ready",
            Self::BlockedByDraft => "blocked_by_draft",
            Self::BlockedByWorkspace => "blocked_by_workspace",
            Self::BlockedByMarker => "blocked_by_marker",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::Planned | Self::FileReady)
    }
    #[must_use]
    pub const fn allows_file_write(self) -> bool {
        false
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStagedManifestFileMode {
    ReportOnly,
    FilePlanning,
    Unsupported,
}
impl VortexStagedManifestFileMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::FilePlanning => "file_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn writes_manifest_file(self) -> bool {
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
pub enum VortexStagedManifestFileSignal {
    DraftReady,
    DraftBlocked,
    WorkspaceKnown,
    WorkspaceMissing,
    MarkerWritten,
    MarkerMissing,
    LocalWorkspace,
    ObjectStoreWorkspace,
}
impl VortexStagedManifestFileSignal {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DraftReady => "draft_ready",
            Self::DraftBlocked => "draft_blocked",
            Self::WorkspaceKnown => "workspace_known",
            Self::WorkspaceMissing => "workspace_missing",
            Self::MarkerWritten => "marker_written",
            Self::MarkerMissing => "marker_missing",
            Self::LocalWorkspace => "local_workspace",
            Self::ObjectStoreWorkspace => "object_store_workspace",
        }
    }
    #[must_use]
    pub const fn is_blocking(self) -> bool {
        matches!(
            self,
            Self::DraftBlocked
                | Self::WorkspaceMissing
                | Self::MarkerMissing
                | Self::ObjectStoreWorkspace
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStagedManifestFileEffect {
    ManifestFileWritten,
    OutputDataWritten,
    ObjectStoreIo,
    UpstreamVortexWriteCalled,
    CommitPerformed,
    FallbackExecution,
}
impl VortexStagedManifestFileEffect {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ManifestFileWritten => "manifest_file_written",
            Self::OutputDataWritten => "output_data_written",
            Self::ObjectStoreIo => "object_store_io",
            Self::UpstreamVortexWriteCalled => "upstream_vortex_write_called",
            Self::CommitPerformed => "commit_performed",
            Self::FallbackExecution => "fallback_execution",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexStagedManifestFileName(String);
impl VortexStagedManifestFileName {
    /// # Errors
    /// Returns an error when the file name is empty or contains invalid traversal/path separator content.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let t = value.trim();
        if t.is_empty() || t.contains('/') || t.contains('\\') || t.contains("..") {
            return Err(shardloom_core::ShardLoomError::InvalidOperation(
                "invalid staged manifest file name".to_string(),
            ));
        }
        Ok(Self(t.to_string()))
    }
    #[must_use]
    pub fn default_draft() -> Self {
        Self("_shardloom_staged_manifest_draft.json".to_string())
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!("file_name={}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexStagedManifestFileRef {
    workspace_path: crate::VortexStagedWorkspacePath,
    file_name: VortexStagedManifestFileName,
}
impl VortexStagedManifestFileRef {
    #[must_use]
    pub fn new(
        workspace_path: crate::VortexStagedWorkspacePath,
        file_name: VortexStagedManifestFileName,
    ) -> Self {
        Self {
            workspace_path,
            file_name,
        }
    }
    #[must_use]
    pub fn default_for_workspace(workspace_path: crate::VortexStagedWorkspacePath) -> Self {
        Self::new(
            workspace_path,
            VortexStagedManifestFileName::default_draft(),
        )
    }
    #[must_use]
    pub fn workspace_path(&self) -> &crate::VortexStagedWorkspacePath {
        &self.workspace_path
    }
    #[must_use]
    pub fn file_name(&self) -> &VortexStagedManifestFileName {
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
        format!("manifest_file_path={}", self.path_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexStagedManifestDraftContent {
    content: String,
}
impl VortexStagedManifestDraftContent {
    /// # Errors
    /// Returns an error when content is empty/whitespace or exceeds the phase size limit.
    pub fn new(content: impl Into<String>) -> Result<Self> {
        let content = content.into();
        if content.trim().is_empty() || content.len() > STAGED_MANIFEST_DRAFT_MAX_BYTES {
            return Err(shardloom_core::ShardLoomError::InvalidOperation(
                "invalid staged manifest draft content".to_string(),
            ));
        }
        Ok(Self { content })
    }
    /// # Errors
    /// Propagates validation errors from [`Self::new`].
    pub fn from_report_summary(report: &VortexStagedManifestDraftReport) -> Result<Self> {
        let mut content = String::new();
        let _ = writeln!(content, "shardloom_staged_manifest_draft=true");
        let _ = writeln!(content, "draft_status={}", report.status.as_str());
        let _ = writeln!(content, "manifest_written=false");
        let _ = writeln!(content, "output_data_written=false");
        let _ = writeln!(content, "commit_performed=false");
        let _ = writeln!(content, "fallback_execution_allowed=false");
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
        self.content
            .as_bytes()
            .iter()
            .fold(1_469_598_103_934_665_603_u64, |acc, b| {
                acc.wrapping_mul(1_099_511_628_211) ^ u64::from(*b)
            })
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "content_len={} checksum_u64={}",
            self.len(),
            self.checksum_u64()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexStagedManifestFileRequest {
    pub file_ref: VortexStagedManifestFileRef,
    pub draft_content: VortexStagedManifestDraftContent,
    pub signals: Vec<VortexStagedManifestFileSignal>,
    pub draft_summary: Option<String>,
    pub workspace_summary: Option<String>,
    pub marker_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexStagedManifestFileRequest {
    #[must_use]
    pub fn new(
        file_ref: VortexStagedManifestFileRef,
        draft_content: VortexStagedManifestDraftContent,
    ) -> Self {
        Self {
            file_ref,
            draft_content,
            signals: Vec::new(),
            draft_summary: None,
            workspace_summary: None,
            marker_summary: None,
            diagnostics: Vec::new(),
        }
    }
    pub fn add_signal(&mut self, signal: VortexStagedManifestFileSignal, value: bool) {
        if value {
            if !self.signals.contains(&signal) {
                self.signals.push(signal);
            }
        } else {
            self.signals.retain(|s| *s != signal);
        }
    }
    signal_builder!(draft_ready, VortexStagedManifestFileSignal::DraftReady);
    signal_builder!(draft_blocked, VortexStagedManifestFileSignal::DraftBlocked);
    signal_builder!(
        workspace_known,
        VortexStagedManifestFileSignal::WorkspaceKnown
    );
    signal_builder!(
        workspace_missing,
        VortexStagedManifestFileSignal::WorkspaceMissing
    );
    signal_builder!(
        marker_written,
        VortexStagedManifestFileSignal::MarkerWritten
    );
    signal_builder!(
        marker_missing,
        VortexStagedManifestFileSignal::MarkerMissing
    );
    signal_builder!(
        local_workspace,
        VortexStagedManifestFileSignal::LocalWorkspace
    );
    signal_builder!(
        object_store_workspace,
        VortexStagedManifestFileSignal::ObjectStoreWorkspace
    );
    #[must_use]
    pub fn with_draft_summary(mut self, v: impl Into<String>) -> Self {
        self.draft_summary = Some(v.into());
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
    pub fn has_signal(&self, s: VortexStagedManifestFileSignal) -> bool {
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
            "{} {}",
            self.file_ref.summary(),
            self.draft_content.summary()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexStagedManifestFileReport {
    pub status: VortexStagedManifestFileStatus,
    pub mode: VortexStagedManifestFileMode,
    pub request: VortexStagedManifestFileRequest,
    pub effects_performed: Vec<VortexStagedManifestFileEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexStagedManifestFileReport {
    /// # Errors
    /// Returns an error only if text rendering fails unexpectedly.
    pub fn from_request(request: VortexStagedManifestFileRequest) -> Result<Self> {
        let mut r = Self {
            status: VortexStagedManifestFileStatus::Planned,
            mode: VortexStagedManifestFileMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        };
        r.status = if r.object_store_workspace() {
            VortexStagedManifestFileStatus::BlockedByObjectStoreTarget
        } else if r
            .request
            .has_signal(VortexStagedManifestFileSignal::DraftBlocked)
        {
            VortexStagedManifestFileStatus::BlockedByDraft
        } else if r
            .request
            .has_signal(VortexStagedManifestFileSignal::WorkspaceMissing)
            || !r.workspace_known()
        {
            VortexStagedManifestFileStatus::BlockedByWorkspace
        } else if r
            .request
            .has_signal(VortexStagedManifestFileSignal::MarkerMissing)
            || !r.marker_written()
        {
            VortexStagedManifestFileStatus::BlockedByMarker
        } else {
            VortexStagedManifestFileStatus::FileReady
        };
        Ok(r)
    }
    #[must_use]
    pub fn unsupported(
        request: VortexStagedManifestFileRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut r = Self {
            status: VortexStagedManifestFileStatus::Unsupported,
            mode: VortexStagedManifestFileMode::Unsupported,
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
    pub fn draft_ready(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestFileSignal::DraftReady)
    }
    #[must_use]
    pub fn workspace_known(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestFileSignal::WorkspaceKnown)
    }
    #[must_use]
    pub fn marker_written(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestFileSignal::MarkerWritten)
    }
    #[must_use]
    pub fn local_workspace(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestFileSignal::LocalWorkspace)
    }
    #[must_use]
    pub fn object_store_workspace(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestFileSignal::ObjectStoreWorkspace)
    }
    #[must_use]
    pub const fn manifest_file_written(&self) -> bool {
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
    pub const fn allows_file_write(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty() && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut t = String::new();
        let _ = writeln!(t, "Vortex staged manifest file plan");
        let _ = writeln!(t, "status: {}", self.status.as_str());
        let _ = writeln!(t, "mode: {}", self.mode.as_str());
        let _ = writeln!(t, "file path: {}", self.request.file_ref.path_string());
        let _ = writeln!(t, "content length: {}", self.request.draft_content.len());
        let _ = writeln!(
            t,
            "content checksum: {}",
            self.request.draft_content.checksum_u64()
        );
        let _ = writeln!(t, "draft ready: {}", self.draft_ready());
        let _ = writeln!(t, "workspace known: {}", self.workspace_known());
        let _ = writeln!(t, "marker written: {}", self.marker_written());
        let _ = writeln!(t, "local workspace: {}", self.local_workspace());
        let _ = writeln!(
            t,
            "object-store workspace: {}",
            self.object_store_workspace()
        );
        let _ = writeln!(t, "manifest file written: false");
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
/// Propagates errors from [`VortexStagedManifestFileReport::from_request`].
pub fn plan_vortex_staged_manifest_file(
    request: VortexStagedManifestFileRequest,
) -> Result<VortexStagedManifestFileReport> {
    VortexStagedManifestFileReport::from_request(request)
}
#[must_use]
pub fn vortex_staged_manifest_file_is_side_effect_free(
    report: &VortexStagedManifestFileReport,
) -> bool {
    report.is_side_effect_free()
}

/// # Errors
/// Propagates errors from draft-content and file-name validation.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStagedManifestFileWriteStatus {
    Planned,
    WriteWouldExecute,
    DraftFileWritten,
    BlockedByFilePlan,
    BlockedByObjectStoreTarget,
    BlockedByMissingWorkspace,
    BlockedByExistingNonDirectory,
    BlockedByExistingDraftFile,
    BlockedByFeatureGate,
    Unsupported,
}
impl VortexStagedManifestFileWriteStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::WriteWouldExecute => "write_would_execute",
            Self::DraftFileWritten => "draft_file_written",
            Self::BlockedByFilePlan => "blocked_by_file_plan",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByMissingWorkspace => "blocked_by_missing_workspace",
            Self::BlockedByExistingNonDirectory => "blocked_by_existing_non_directory",
            Self::BlockedByExistingDraftFile => "blocked_by_existing_draft_file",
            Self::BlockedByFeatureGate => "blocked_by_feature_gate",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(
            self,
            Self::Planned | Self::WriteWouldExecute | Self::DraftFileWritten
        )
    }
    #[must_use]
    pub const fn allows_file_write(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStagedManifestFileWriteMode {
    ReportOnly,
    WritePlanning,
    LocalDraftFileWrite,
    Unsupported,
}
impl VortexStagedManifestFileWriteMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::WritePlanning => "write_planning",
            Self::LocalDraftFileWrite => "local_draft_file_write",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn writes_manifest_file(self) -> bool {
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
pub enum VortexStagedManifestFileWriteSignal {
    FilePlanReady,
    FilePlanBlocked,
    WorkspaceKnown,
    WorkspaceMissing,
    ObjectStoreTarget,
    ExistingDraftFile,
    FeatureGateEnabled,
    OverwriteAllowed,
}
impl VortexStagedManifestFileWriteSignal {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FilePlanReady => "file_plan_ready",
            Self::FilePlanBlocked => "file_plan_blocked",
            Self::WorkspaceKnown => "workspace_known",
            Self::WorkspaceMissing => "workspace_missing",
            Self::ObjectStoreTarget => "object_store_target",
            Self::ExistingDraftFile => "existing_draft_file",
            Self::FeatureGateEnabled => "feature_gate_enabled",
            Self::OverwriteAllowed => "overwrite_allowed",
        }
    }
    #[must_use]
    pub const fn is_blocking(self) -> bool {
        matches!(
            self,
            Self::FilePlanBlocked
                | Self::WorkspaceMissing
                | Self::ObjectStoreTarget
                | Self::ExistingDraftFile
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStagedManifestFileWriteEffect {
    DraftFileWritten,
    OutputDataWritten,
    ObjectStoreIo,
    UpstreamVortexWriteCalled,
    CommitPerformed,
    FallbackExecution,
}
impl VortexStagedManifestFileWriteEffect {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DraftFileWritten => "draft_file_written",
            Self::OutputDataWritten => "output_data_written",
            Self::ObjectStoreIo => "object_store_io",
            Self::UpstreamVortexWriteCalled => "upstream_vortex_write_called",
            Self::CommitPerformed => "commit_performed",
            Self::FallbackExecution => "fallback_execution",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexStagedManifestFileWriteOption {
    AllowOverwrite,
}
impl VortexStagedManifestFileWriteOption {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AllowOverwrite => "allow_overwrite",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexStagedManifestFileWriteRequest {
    pub file_ref: VortexStagedManifestFileRef,
    pub draft_content: VortexStagedManifestDraftContent,
    pub options: Vec<VortexStagedManifestFileWriteOption>,
    pub signals: Vec<VortexStagedManifestFileWriteSignal>,
    pub file_plan_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexStagedManifestFileWriteRequest {
    #[must_use]
    pub fn new(
        file_ref: VortexStagedManifestFileRef,
        draft_content: VortexStagedManifestDraftContent,
    ) -> Self {
        Self {
            file_ref,
            draft_content,
            options: Vec::new(),
            signals: Vec::new(),
            file_plan_summary: None,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn from_file_request(request: &VortexStagedManifestFileRequest) -> Self {
        Self::new(request.file_ref.clone(), request.draft_content.clone())
    }
    #[must_use]
    pub fn allow_overwrite(mut self, value: bool) -> Self {
        if value {
            if !self
                .options
                .contains(&VortexStagedManifestFileWriteOption::AllowOverwrite)
            {
                self.options
                    .push(VortexStagedManifestFileWriteOption::AllowOverwrite);
            }
            self.add_signal(VortexStagedManifestFileWriteSignal::OverwriteAllowed, true);
        } else {
            self.options
                .retain(|o| *o != VortexStagedManifestFileWriteOption::AllowOverwrite);
            self.add_signal(VortexStagedManifestFileWriteSignal::OverwriteAllowed, false);
        }
        self
    }
    pub fn add_signal(&mut self, signal: VortexStagedManifestFileWriteSignal, value: bool) {
        if value {
            if !self.signals.contains(&signal) {
                self.signals.push(signal);
            }
        } else {
            self.signals.retain(|s| *s != signal);
        }
    }
    signal_builder!(
        file_plan_ready,
        VortexStagedManifestFileWriteSignal::FilePlanReady
    );
    signal_builder!(
        file_plan_blocked,
        VortexStagedManifestFileWriteSignal::FilePlanBlocked
    );
    signal_builder!(
        workspace_known,
        VortexStagedManifestFileWriteSignal::WorkspaceKnown
    );
    signal_builder!(
        workspace_missing,
        VortexStagedManifestFileWriteSignal::WorkspaceMissing
    );
    signal_builder!(
        object_store_target,
        VortexStagedManifestFileWriteSignal::ObjectStoreTarget
    );
    signal_builder!(
        existing_draft_file,
        VortexStagedManifestFileWriteSignal::ExistingDraftFile
    );
    signal_builder!(
        feature_gate_enabled,
        VortexStagedManifestFileWriteSignal::FeatureGateEnabled
    );
    #[must_use]
    pub fn with_file_plan_summary(mut self, v: impl Into<String>) -> Self {
        self.file_plan_summary = Some(v.into());
        self
    }
    #[must_use]
    pub fn has_option(&self, option: VortexStagedManifestFileWriteOption) -> bool {
        self.options.contains(&option)
    }
    #[must_use]
    pub fn has_signal(&self, signal: VortexStagedManifestFileWriteSignal) -> bool {
        self.signals.contains(&signal)
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
            "{} {}",
            self.file_ref.summary(),
            self.draft_content.summary()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexStagedManifestFileWriteReport {
    pub status: VortexStagedManifestFileWriteStatus,
    pub mode: VortexStagedManifestFileWriteMode,
    pub request: VortexStagedManifestFileWriteRequest,
    pub effects_performed: Vec<VortexStagedManifestFileWriteEffect>,
    pub bytes_written: usize,
    pub checksum: Option<u64>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexStagedManifestFileWriteReport {
    /// # Errors
    /// Returns an error only if rendering diagnostics text fails unexpectedly.
    pub fn from_request(request: VortexStagedManifestFileWriteRequest) -> Result<Self> {
        let mut r = Self::planned(request);
        r.status = if r.object_store_target() {
            VortexStagedManifestFileWriteStatus::BlockedByObjectStoreTarget
        } else if r
            .request
            .has_signal(VortexStagedManifestFileWriteSignal::FilePlanBlocked)
        {
            VortexStagedManifestFileWriteStatus::BlockedByFilePlan
        } else if r
            .request
            .has_signal(VortexStagedManifestFileWriteSignal::WorkspaceMissing)
            || !r.workspace_known()
        {
            VortexStagedManifestFileWriteStatus::BlockedByMissingWorkspace
        } else if !r.file_plan_ready() {
            VortexStagedManifestFileWriteStatus::BlockedByFilePlan
        } else if r.existing_draft_file() && !r.overwrite_allowed() {
            VortexStagedManifestFileWriteStatus::BlockedByExistingDraftFile
        } else if !r
            .request
            .has_signal(VortexStagedManifestFileWriteSignal::FeatureGateEnabled)
        {
            VortexStagedManifestFileWriteStatus::Planned
        } else {
            VortexStagedManifestFileWriteStatus::WriteWouldExecute
        };
        Ok(r)
    }
    #[must_use]
    pub fn planned(request: VortexStagedManifestFileWriteRequest) -> Self {
        Self {
            status: VortexStagedManifestFileWriteStatus::Planned,
            mode: VortexStagedManifestFileWriteMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            bytes_written: 0,
            checksum: None,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn write_would_execute(request: VortexStagedManifestFileWriteRequest) -> Self {
        let mut r = Self::planned(request);
        r.status = VortexStagedManifestFileWriteStatus::WriteWouldExecute;
        r
    }
    #[must_use]
    pub fn blocked(
        request: VortexStagedManifestFileWriteRequest,
        status: VortexStagedManifestFileWriteStatus,
        reason: impl Into<String>,
    ) -> Self {
        let mut r = Self::planned(request);
        r.status = status;
        r.add_diagnostic(Diagnostic::invalid_input(
            "staged_manifest_file_write_plan",
            reason,
            "adjust staged manifest file write planning signals",
        ));
        r
    }
    #[must_use]
    pub fn unsupported(
        request: VortexStagedManifestFileWriteRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut r = Self {
            status: VortexStagedManifestFileWriteStatus::Unsupported,
            mode: VortexStagedManifestFileWriteMode::Unsupported,
            request,
            effects_performed: Vec::new(),
            bytes_written: 0,
            checksum: None,
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
    pub fn file_plan_ready(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestFileWriteSignal::FilePlanReady)
    }
    #[must_use]
    pub fn workspace_known(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestFileWriteSignal::WorkspaceKnown)
    }
    #[must_use]
    pub fn object_store_target(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestFileWriteSignal::ObjectStoreTarget)
    }
    #[must_use]
    pub fn existing_draft_file(&self) -> bool {
        self.request
            .has_signal(VortexStagedManifestFileWriteSignal::ExistingDraftFile)
    }
    #[must_use]
    pub fn overwrite_allowed(&self) -> bool {
        self.request
            .has_option(VortexStagedManifestFileWriteOption::AllowOverwrite)
            || self
                .request
                .has_signal(VortexStagedManifestFileWriteSignal::OverwriteAllowed)
    }
    #[must_use]
    pub fn draft_file_written(&self) -> bool {
        self.effects_performed
            .contains(&VortexStagedManifestFileWriteEffect::DraftFileWritten)
    }
    #[must_use]
    pub const fn manifest_file_written(&self) -> bool {
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
    pub const fn allows_file_write(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        !self.draft_file_written() && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut t = String::new();
        let _ = writeln!(t, "Vortex staged manifest file write plan");
        let _ = writeln!(t, "status: {}", self.status.as_str());
        let _ = writeln!(t, "mode: {}", self.mode.as_str());
        let _ = writeln!(t, "file path: {}", self.request.file_ref.path_string());
        let _ = writeln!(t, "content length: {}", self.request.draft_content.len());
        let _ = writeln!(
            t,
            "content checksum: {}",
            self.request.draft_content.checksum_u64()
        );
        let _ = writeln!(t, "file plan ready: {}", self.file_plan_ready());
        let _ = writeln!(t, "workspace known: {}", self.workspace_known());
        let _ = writeln!(t, "object-store target: {}", self.object_store_target());
        let _ = writeln!(t, "existing draft file: {}", self.existing_draft_file());
        let _ = writeln!(t, "overwrite allowed: {}", self.overwrite_allowed());
        let _ = writeln!(t, "draft file written: {}", self.draft_file_written());
        let _ = writeln!(t, "manifest file written: false");
        let _ = writeln!(t, "output data written: false");
        let _ = writeln!(t, "object-store IO: false");
        let _ = writeln!(t, "upstream Vortex write called: false");
        let _ = writeln!(t, "commit performed: false");
        let _ = writeln!(t, "bytes written: {}", self.bytes_written);
        let _ = writeln!(
            t,
            "checksum: {}",
            self.checksum
                .map_or_else(|| "none".to_string(), |c| c.to_string())
        );
        let _ = write!(t, "fallback execution: disabled");
        if self.request.diagnostics.is_empty() && self.diagnostics.is_empty() {
            let _ = write!(
                t,
                "
diagnostics: none"
            );
        } else {
            let _ = write!(
                t,
                "
diagnostics:"
            );
            for d in self
                .request
                .diagnostics
                .iter()
                .chain(self.diagnostics.iter())
            {
                let _ = write!(
                    t,
                    "
- {}",
                    d.to_human_text()
                );
            }
        }
        t
    }
}

/// # Errors
/// Propagates errors from [`VortexStagedManifestFileWriteReport::from_request`].
pub fn plan_vortex_staged_manifest_file_write(
    request: VortexStagedManifestFileWriteRequest,
) -> Result<VortexStagedManifestFileWriteReport> {
    VortexStagedManifestFileWriteReport::from_request(request)
}

/// Writes only the staged manifest draft file addressed by `request.file_ref` when
/// `vortex-staged-output-fs` is enabled.
///
/// # Errors
/// Propagates report-construction and local path normalization errors.
pub fn write_vortex_staged_manifest_file(
    request: VortexStagedManifestFileWriteRequest,
) -> Result<VortexStagedManifestFileWriteReport> {
    #[cfg(not(feature = "vortex-staged-output-fs"))]
    {
        VortexStagedManifestFileWriteReport::from_request(request)
    }
    #[cfg(feature = "vortex-staged-output-fs")]
    {
        let mut report = VortexStagedManifestFileWriteReport::from_request(request)?;
        if !matches!(
            report.status,
            VortexStagedManifestFileWriteStatus::WriteWouldExecute
        ) {
            return Ok(report);
        }
        let draft_path = staged_manifest_draft_local_path(&report.request.file_ref)?;
        let workspace_path = draft_path.parent().ok_or_else(|| {
            shardloom_core::ShardLoomError::InvalidOperation("missing parent path".to_string())
        })?;
        if !workspace_path.exists() {
            report.status = VortexStagedManifestFileWriteStatus::BlockedByMissingWorkspace;
            return Ok(report);
        }
        if !workspace_path.is_dir() {
            report.status = VortexStagedManifestFileWriteStatus::BlockedByExistingNonDirectory;
            return Ok(report);
        }
        if draft_path.exists() && !report.overwrite_allowed() {
            report.status = VortexStagedManifestFileWriteStatus::BlockedByExistingDraftFile;
            return Ok(report);
        }
        fs::write(
            &draft_path,
            report.request.draft_content.as_str().as_bytes(),
        )
        .map_err(|err| {
            shardloom_core::ShardLoomError::InvalidOperation(format!(
                "failed to write staged manifest draft file: {err}"
            ))
        })?;
        report.mode = VortexStagedManifestFileWriteMode::LocalDraftFileWrite;
        report.status = VortexStagedManifestFileWriteStatus::DraftFileWritten;
        report.bytes_written = report.request.draft_content.len();
        report.checksum = Some(report.request.draft_content.checksum_u64());
        report
            .effects_performed
            .push(VortexStagedManifestFileWriteEffect::DraftFileWritten);
        Ok(report)
    }
}

#[cfg(feature = "vortex-staged-output-fs")]
fn staged_manifest_draft_local_path(file_ref: &VortexStagedManifestFileRef) -> Result<PathBuf> {
    let workspace_raw = file_ref.workspace_path().as_str();
    let workspace = match DatasetUri::new(workspace_raw.to_string())?.scheme() {
        UriScheme::LocalPath => PathBuf::from(workspace_raw),
        UriScheme::File => {
            if let Some(local) = workspace_raw.strip_prefix("file:///") {
                PathBuf::from(format!("/{local}"))
            } else {
                return Err(shardloom_core::ShardLoomError::InvalidOperation(
                    "workspace file URI must use file:/// absolute local path".to_string(),
                ));
            }
        }
        _ => {
            return Err(shardloom_core::ShardLoomError::InvalidOperation(
                "workspace path looks like object-store target".to_string(),
            ));
        }
    };
    Ok(workspace.join(file_ref.file_name().as_str()))
}
#[must_use]
pub fn vortex_staged_manifest_file_write_is_side_effect_free(
    report: &VortexStagedManifestFileWriteReport,
) -> bool {
    report.is_side_effect_free()
}

/// # Errors
/// Propagates validation errors from `VortexStagedManifestDraftContent` conversion.
pub fn staged_manifest_file_write_request_from_plan(
    plan: &VortexStagedManifestFileReport,
) -> Result<VortexStagedManifestFileWriteRequest> {
    let mut request = VortexStagedManifestFileWriteRequest::from_file_request(&plan.request)
        .with_file_plan_summary(plan.request.summary());
    if plan.has_errors() {
        request.add_signal(VortexStagedManifestFileWriteSignal::FilePlanBlocked, true);
        for d in plan
            .request
            .diagnostics
            .iter()
            .chain(plan.diagnostics.iter())
        {
            request.add_diagnostic(d.clone());
        }
    }
    if matches!(plan.status, VortexStagedManifestFileStatus::FileReady) {
        request.add_signal(VortexStagedManifestFileWriteSignal::FilePlanReady, true);
    }
    if plan.workspace_known() {
        request.add_signal(VortexStagedManifestFileWriteSignal::WorkspaceKnown, true);
    }
    if plan.object_store_workspace() {
        request.add_signal(VortexStagedManifestFileWriteSignal::ObjectStoreTarget, true);
    }
    Ok(request)
}
/// # Errors
/// Propagates errors from draft-content and file-name validation.
pub fn staged_manifest_file_request_from_reports(
    workspace_path: crate::VortexStagedWorkspacePath,
    draft: &VortexStagedManifestDraftReport,
    workspace: Option<&crate::VortexStagedWorkspaceSetupReport>,
    marker: Option<&crate::VortexStagedMarkerReport>,
) -> Result<VortexStagedManifestFileRequest> {
    let file_ref = VortexStagedManifestFileRef::default_for_workspace(workspace_path);
    let draft_content = VortexStagedManifestDraftContent::from_report_summary(draft)?;
    let mut request = VortexStagedManifestFileRequest::new(file_ref, draft_content)
        .with_draft_summary(draft.request.summary());
    request.add_signal(
        VortexStagedManifestFileSignal::DraftBlocked,
        draft.has_errors(),
    );
    request.add_signal(
        VortexStagedManifestFileSignal::DraftReady,
        !draft.has_errors() && matches!(draft.status, VortexStagedManifestDraftStatus::DraftReady),
    );
    if let Some(w) = workspace {
        request.workspace_summary = Some(w.request.summary());
        request.add_signal(
            VortexStagedManifestFileSignal::WorkspaceKnown,
            w.workspace_created() || !w.has_errors(),
        );
        request.add_signal(
            VortexStagedManifestFileSignal::WorkspaceMissing,
            w.has_errors(),
        );
    } else {
        request.add_signal(VortexStagedManifestFileSignal::WorkspaceMissing, true);
    }
    if let Some(m) = marker {
        request.marker_summary = Some(m.request.summary());
        request.add_signal(
            VortexStagedManifestFileSignal::MarkerWritten,
            m.marker_written(),
        );
        request.add_signal(
            VortexStagedManifestFileSignal::MarkerMissing,
            !m.marker_written(),
        );
    } else {
        request.add_signal(VortexStagedManifestFileSignal::MarkerMissing, true);
    }
    request.add_signal(
        VortexStagedManifestFileSignal::ObjectStoreWorkspace,
        draft.object_store_target(),
    );
    request.add_signal(
        VortexStagedManifestFileSignal::LocalWorkspace,
        !draft.object_store_target(),
    );
    Ok(request)
}

#[cfg(test)]
mod staged_manifest_file_tests {
    use super::*;

    fn ws() -> crate::VortexStagedWorkspacePath {
        crate::VortexStagedWorkspacePath::new("/tmp/ws").unwrap()
    }

    fn draft_report_ready() -> VortexStagedManifestDraftReport {
        VortexStagedManifestDraftReport::from_request(
            VortexStagedManifestDraftRequest::new(
                DatasetUri::new("file://tmp/out.vortex").unwrap(),
            )
            .write_intent_planned(true)
            .staged_output_planned(true)
            .workspace_known(true)
            .marker_written(true)
            .schema_known(true)
            .schema_compatible(true)
            .delete_semantics_known(true)
            .tombstone_semantics_known(true)
            .commit_protocol_available(true),
        )
        .unwrap()
    }

    #[test]
    fn file_contract_basics() {
        assert!(!VortexStagedManifestFileStatus::FileReady.allows_file_write());
        assert!(VortexStagedManifestFileStatus::BlockedByDraft.is_error());
        assert!(!VortexStagedManifestFileMode::ReportOnly.writes_manifest_file());
        assert!(VortexStagedManifestFileName::new(" ").is_err());
        assert!(VortexStagedManifestFileName::new("a/b").is_err());
        assert!(VortexStagedManifestFileName::new("a..b").is_err());
        assert_eq!(
            VortexStagedManifestFileName::default_draft().as_str(),
            "_shardloom_staged_manifest_draft.json"
        );
        let rf = VortexStagedManifestFileRef::default_for_workspace(ws());
        assert_eq!(
            rf.path_string(),
            "/tmp/ws/_shardloom_staged_manifest_draft.json"
        );
    }

    #[test]
    fn draft_content_checks() {
        assert!(VortexStagedManifestDraftContent::new(" ").is_err());
        assert!(VortexStagedManifestDraftContent::new("x".repeat(70000)).is_err());
        let c = VortexStagedManifestDraftContent::new("hello").unwrap();
        assert_eq!(
            c.checksum_u64(),
            VortexStagedManifestDraftContent::new("hello")
                .unwrap()
                .checksum_u64()
        );
    }

    #[test]
    fn request_and_report_no_io_behaviors() {
        let mut req = VortexStagedManifestFileRequest::new(
            VortexStagedManifestFileRef::default_for_workspace(ws()),
            VortexStagedManifestDraftContent::new("ok").unwrap(),
        );
        req.add_signal(VortexStagedManifestFileSignal::DraftReady, true);
        req.add_signal(VortexStagedManifestFileSignal::DraftReady, true);
        req.add_signal(VortexStagedManifestFileSignal::DraftReady, false);
        assert!(!req.has_signal(VortexStagedManifestFileSignal::DraftReady));

        let report = VortexStagedManifestFileReport::from_request(
            VortexStagedManifestFileRequest::new(
                VortexStagedManifestFileRef::default_for_workspace(ws()),
                VortexStagedManifestDraftContent::new("ok").unwrap(),
            )
            .draft_ready(true)
            .workspace_known(true)
            .marker_written(true),
        )
        .unwrap();
        assert_eq!(report.status, VortexStagedManifestFileStatus::FileReady);
        assert!(!report.allows_file_write());
        assert!(!report.manifest_file_written());
        assert!(!report.output_data_written());
        assert!(!report.object_store_io());
        assert!(!report.upstream_vortex_write_called());
        assert!(!report.commit_performed());
        assert!(!report.fallback_execution_allowed());
        assert!(report.is_side_effect_free());
        assert!(
            report
                .to_human_text()
                .contains("fallback execution: disabled")
        );
        assert!(
            report
                .to_human_text()
                .contains("manifest file written: false")
        );
    }

    #[test]
    fn report_status_priority() {
        let base = VortexStagedManifestFileRequest::new(
            VortexStagedManifestFileRef::default_for_workspace(ws()),
            VortexStagedManifestDraftContent::new("ok").unwrap(),
        )
        .draft_ready(true)
        .workspace_known(true)
        .marker_written(true);
        assert_eq!(
            VortexStagedManifestFileReport::from_request(base.clone().object_store_workspace(true))
                .unwrap()
                .status,
            VortexStagedManifestFileStatus::BlockedByObjectStoreTarget
        );
        assert_eq!(
            VortexStagedManifestFileReport::from_request(base.clone().draft_blocked(true))
                .unwrap()
                .status,
            VortexStagedManifestFileStatus::BlockedByDraft
        );
        assert_eq!(
            VortexStagedManifestFileReport::from_request(base.clone().workspace_known(false))
                .unwrap()
                .status,
            VortexStagedManifestFileStatus::BlockedByWorkspace
        );
        assert_eq!(
            VortexStagedManifestFileReport::from_request(base.clone().marker_written(false))
                .unwrap()
                .status,
            VortexStagedManifestFileStatus::BlockedByMarker
        );
    }

    #[test]
    fn helper_from_reports_mapping() {
        let draft = draft_report_ready();
        let req = staged_manifest_file_request_from_reports(ws(), &draft, None, None).unwrap();
        assert!(req.has_signal(VortexStagedManifestFileSignal::MarkerMissing));
        let mut marker =
            crate::VortexStagedMarkerReport::planned(crate::VortexStagedMarkerRequest::new(
                crate::VortexStagedWorkspaceId::new("ws").unwrap(),
                ws(),
            ));
        marker.status = crate::VortexStagedMarkerStatus::MarkerWritten;
        marker
            .effects_performed
            .push(crate::VortexStagedOutputEffect::MarkerWritten);
        let req2 =
            staged_manifest_file_request_from_reports(ws(), &draft, None, Some(&marker)).unwrap();
        assert!(req2.has_signal(VortexStagedManifestFileSignal::MarkerWritten));

        let mut blocked = draft.clone();
        blocked.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::UnsupportedOutputFormat,
            "staged_manifest_file",
            "e",
            None,
        ));
        let req3 = staged_manifest_file_request_from_reports(ws(), &blocked, None, None).unwrap();
        assert!(req3.has_signal(VortexStagedManifestFileSignal::DraftBlocked));

        let rep = plan_vortex_staged_manifest_file(
            VortexStagedManifestFileRequest::new(
                VortexStagedManifestFileRef::default_for_workspace(ws()),
                VortexStagedManifestDraftContent::new("ok").unwrap(),
            )
            .draft_ready(true)
            .workspace_known(true)
            .marker_written(true),
        )
        .unwrap();
        assert!(vortex_staged_manifest_file_is_side_effect_free(&rep));
    }

    #[test]
    fn write_contract_behaviors() {
        assert!(!VortexStagedManifestFileWriteStatus::WriteWouldExecute.allows_file_write());
        assert!(VortexStagedManifestFileWriteStatus::BlockedByFilePlan.is_error());
        assert!(!VortexStagedManifestFileWriteMode::ReportOnly.writes_manifest_file());

        let req = VortexStagedManifestFileWriteRequest::new(
            VortexStagedManifestFileRef::default_for_workspace(ws()),
            VortexStagedManifestDraftContent::new("ok").expect("draft content"),
        )
        .file_plan_ready(true)
        .workspace_known(true)
        .existing_draft_file(true)
        .allow_overwrite(true)
        .feature_gate_enabled(true);
        assert!(req.has_option(VortexStagedManifestFileWriteOption::AllowOverwrite));

        let rep = VortexStagedManifestFileWriteReport::from_request(req).expect("report");
        assert!(matches!(
            rep.status,
            VortexStagedManifestFileWriteStatus::WriteWouldExecute
        ));
        assert!(!rep.draft_file_written());
        assert!(!rep.manifest_file_written());
        assert!(!rep.output_data_written());
        assert!(!rep.object_store_io());
        assert!(!rep.upstream_vortex_write_called());
        assert!(!rep.commit_performed());
        assert!(!rep.fallback_execution_allowed());
        assert!(!rep.allows_file_write());
        assert!(rep.is_side_effect_free());
        assert!(rep.to_human_text().contains("fallback execution: disabled"));
        assert!(rep.to_human_text().contains("draft file written: false"));

        let blocked = VortexStagedManifestFileWriteReport::from_request(
            VortexStagedManifestFileWriteRequest::new(
                VortexStagedManifestFileRef::default_for_workspace(ws()),
                VortexStagedManifestDraftContent::new("ok").expect("draft content"),
            )
            .object_store_target(true),
        )
        .expect("blocked report");
        assert!(matches!(
            blocked.status,
            VortexStagedManifestFileWriteStatus::BlockedByObjectStoreTarget
        ));

        let plan = VortexStagedManifestFileReport::from_request(
            VortexStagedManifestFileRequest::new(
                VortexStagedManifestFileRef::default_for_workspace(ws()),
                VortexStagedManifestDraftContent::new("ok").expect("draft"),
            )
            .draft_ready(true)
            .workspace_known(true)
            .marker_written(true),
        )
        .expect("plan");
        let from_plan =
            staged_manifest_file_write_request_from_plan(&plan).expect("write request from plan");
        assert!(from_plan.has_signal(VortexStagedManifestFileWriteSignal::FilePlanReady));

        let missing_file_plan = VortexStagedManifestFileWriteReport::from_request(
            VortexStagedManifestFileWriteRequest::new(
                VortexStagedManifestFileRef::default_for_workspace(ws()),
                VortexStagedManifestDraftContent::new("ok").expect("draft"),
            )
            .workspace_known(true)
            .feature_gate_enabled(true),
        )
        .expect("missing file plan");
        assert!(matches!(
            missing_file_plan.status,
            VortexStagedManifestFileWriteStatus::BlockedByFilePlan
        ));
    }

    #[test]
    fn write_helper_is_report_only_without_feature() {
        let req = VortexStagedManifestFileWriteRequest::new(
            VortexStagedManifestFileRef::default_for_workspace(ws()),
            VortexStagedManifestDraftContent::new("ok").expect("draft"),
        )
        .file_plan_ready(true)
        .workspace_known(true)
        .feature_gate_enabled(true);
        let report = write_vortex_staged_manifest_file(req).expect("write report");
        #[cfg(not(feature = "vortex-staged-output-fs"))]
        assert!(matches!(
            report.status,
            VortexStagedManifestFileWriteStatus::WriteWouldExecute
        ));
        assert!(!report.draft_file_written());
        assert!(!report.output_data_written());
        assert!(!report.object_store_io());
        assert!(!report.upstream_vortex_write_called());
        assert!(!report.commit_performed());
        assert!(!report.fallback_execution_allowed());
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    #[test]
    fn write_helper_writes_exact_draft_file() {
        let base = std::env::temp_dir().join(format!(
            "shardloom-staged-manifest-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        std::fs::create_dir_all(&base).expect("create workspace");
        let workspace = crate::VortexStagedWorkspacePath::new(base.to_string_lossy().to_string())
            .expect("workspace");
        let content = VortexStagedManifestDraftContent::new("deterministic").expect("content");
        let req = VortexStagedManifestFileWriteRequest::new(
            VortexStagedManifestFileRef::default_for_workspace(workspace.clone()),
            content.clone(),
        )
        .file_plan_ready(true)
        .workspace_known(true)
        .feature_gate_enabled(true);
        let report = write_vortex_staged_manifest_file(req).expect("write");
        assert!(matches!(
            report.status,
            VortexStagedManifestFileWriteStatus::DraftFileWritten
        ));
        assert!(report.draft_file_written());
        assert_eq!(report.bytes_written, content.len());
        assert_eq!(report.checksum, Some(content.checksum_u64()));
        let draft_path = base.join("_shardloom_staged_manifest_draft.json");
        assert_eq!(
            std::fs::read_to_string(draft_path).expect("read"),
            content.as_str()
        );
        std::fs::remove_dir_all(&base).expect("cleanup");
    }
}
