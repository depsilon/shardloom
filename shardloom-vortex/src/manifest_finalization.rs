use std::{
    collections::hash_map::DefaultHasher,
    fmt::Write as _,
    hash::{Hash, Hasher},
};

use shardloom_core::{
    DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, FallbackStatus, Result,
    ShardLoomError,
};

use crate::{
    VortexCommitMarkerWriteReport, VortexCommitProtocolReport, VortexCommitProtocolState,
    VortexStagedManifestFileWriteReport, VortexStagedWorkspacePath,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexManifestFinalizationStatus {
    Planned,
    FinalizationReady,
    BlockedByDraftManifest,
    BlockedByCommitMarker,
    BlockedByCommitProtocol,
    BlockedBySchema,
    BlockedByDeleteSemantics,
    BlockedByTombstoneSemantics,
    BlockedByObjectStoreTarget,
    BlockedByFeatureGate,
    Unsupported,
}
impl VortexManifestFinalizationStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::FinalizationReady => "finalization_ready",
            Self::BlockedByDraftManifest => "blocked_by_draft_manifest",
            Self::BlockedByCommitMarker => "blocked_by_commit_marker",
            Self::BlockedByCommitProtocol => "blocked_by_commit_protocol",
            Self::BlockedBySchema => "blocked_by_schema",
            Self::BlockedByDeleteSemantics => "blocked_by_delete_semantics",
            Self::BlockedByTombstoneSemantics => "blocked_by_tombstone_semantics",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByFeatureGate => "blocked_by_feature_gate",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::Planned | Self::FinalizationReady)
    }
    #[must_use]
    pub const fn allows_finalization_execution(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexManifestFinalizationMode {
    ReportOnly,
    FinalizationPlanning,
    Unsupported,
}
impl VortexManifestFinalizationMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::FinalizationPlanning => "finalization_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn writes_finalized_manifest(self) -> bool {
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
pub enum VortexManifestFinalizationSignal {
    DraftManifestWritten,
    DraftManifestMissing,
    CommitMarkerWritten,
    CommitMarkerMissing,
    CommitProtocolReady,
    CommitProtocolBlocked,
    SchemaKnown,
    SchemaCompatible,
    DeleteSemanticsKnown,
    TombstoneSemanticsKnown,
    LocalWorkspace,
    ObjectStoreTarget,
    FeatureGateEnabled,
}
impl VortexManifestFinalizationSignal {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DraftManifestWritten => "draft_manifest_written",
            Self::DraftManifestMissing => "draft_manifest_missing",
            Self::CommitMarkerWritten => "commit_marker_written",
            Self::CommitMarkerMissing => "commit_marker_missing",
            Self::CommitProtocolReady => "commit_protocol_ready",
            Self::CommitProtocolBlocked => "commit_protocol_blocked",
            Self::SchemaKnown => "schema_known",
            Self::SchemaCompatible => "schema_compatible",
            Self::DeleteSemanticsKnown => "delete_semantics_known",
            Self::TombstoneSemanticsKnown => "tombstone_semantics_known",
            Self::LocalWorkspace => "local_workspace",
            Self::ObjectStoreTarget => "object_store_target",
            Self::FeatureGateEnabled => "feature_gate_enabled",
        }
    }
    #[must_use]
    pub const fn is_blocking(self) -> bool {
        matches!(
            self,
            Self::DraftManifestMissing
                | Self::CommitMarkerMissing
                | Self::CommitProtocolBlocked
                | Self::ObjectStoreTarget
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexManifestFinalizationEffect {
    FinalizedManifestWritten,
    ManifestCommitted,
    OutputDataWritten,
    ObjectStoreIo,
    UpstreamVortexWriteCalled,
    RecoveryActionExecuted,
    FallbackExecution,
}
impl VortexManifestFinalizationEffect {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FinalizedManifestWritten => "finalized_manifest_written",
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
pub struct VortexFinalizedManifestFileName(String);
impl VortexFinalizedManifestFileName {
    /// # Errors
    /// Returns an error when the finalized manifest file name is empty or unsafe.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let t = value.trim();
        if t.is_empty() || t.contains('/') || t.contains('\\') || t.contains("..") {
            return Err(ShardLoomError::InvalidOperation(
                "invalid finalized manifest file name".to_string(),
            ));
        }
        Ok(Self(t.to_string()))
    }
    #[must_use]
    pub fn default_finalized() -> Self {
        Self("_shardloom_finalized_manifest.json".to_string())
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!("finalized_manifest_file_name={}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexFinalizedManifestFileRef {
    pub workspace_path: VortexStagedWorkspacePath,
    pub file_name: VortexFinalizedManifestFileName,
}
impl VortexFinalizedManifestFileRef {
    #[must_use]
    pub fn new(
        workspace_path: VortexStagedWorkspacePath,
        file_name: VortexFinalizedManifestFileName,
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
            VortexFinalizedManifestFileName::default_finalized(),
        )
    }
    #[must_use]
    pub fn workspace_path(&self) -> &VortexStagedWorkspacePath {
        &self.workspace_path
    }
    #[must_use]
    pub fn file_name(&self) -> &VortexFinalizedManifestFileName {
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
        format!("finalized_manifest_path={}", self.path_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexFinalizedManifestContent {
    content: String,
}
impl VortexFinalizedManifestContent {
    /// # Errors
    /// Returns an error when content is empty or larger than 64 KiB.
    pub fn new(content: impl Into<String>) -> Result<Self> {
        let content = content.into();
        if content.trim().is_empty() || content.len() > 64 * 1024 {
            return Err(ShardLoomError::InvalidOperation(
                "invalid finalized manifest content".to_string(),
            ));
        }
        Ok(Self { content })
    }
    /// # Errors
    /// Propagates validation failures for generated finalized-manifest candidate content.
    pub fn from_inputs(
        draft_summary: impl Into<String>,
        commit_marker_summary: impl Into<String>,
    ) -> Result<Self> {
        let mut content = String::new();
        let _ = writeln!(content, "shardloom_finalized_manifest_candidate=true");
        let _ = writeln!(content, "manifest_finalized=false");
        let _ = writeln!(content, "manifest_committed=false");
        let _ = writeln!(content, "output_data_written=false");
        let _ = writeln!(content, "fallback_execution_allowed=false");
        let _ = writeln!(content, "draft_summary={}", draft_summary.into());
        let _ = writeln!(
            content,
            "commit_marker_summary={}",
            commit_marker_summary.into()
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
            "finalized_manifest_content_len={} checksum_u64={}",
            self.len(),
            self.checksum_u64()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexManifestFinalizationRequest {
    pub target_uri: DatasetUri,
    pub finalized_manifest_ref: VortexFinalizedManifestFileRef,
    pub finalized_manifest_content: VortexFinalizedManifestContent,
    pub signals: Vec<VortexManifestFinalizationSignal>,
    pub draft_manifest_summary: Option<String>,
    pub commit_marker_summary: Option<String>,
    pub protocol_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
macro_rules! sb {
    ($n:ident,$s:expr) => {
        #[must_use]
        pub fn $n(mut self, v: bool) -> Self {
            self.add_signal($s, v);
            self
        }
    };
}
impl VortexManifestFinalizationRequest {
    #[must_use]
    pub fn new(
        target_uri: DatasetUri,
        finalized_manifest_ref: VortexFinalizedManifestFileRef,
        finalized_manifest_content: VortexFinalizedManifestContent,
    ) -> Self {
        Self {
            target_uri,
            finalized_manifest_ref,
            finalized_manifest_content,
            signals: Vec::new(),
            draft_manifest_summary: None,
            commit_marker_summary: None,
            protocol_summary: None,
            diagnostics: Vec::new(),
        }
    }
    pub fn add_signal(&mut self, s: VortexManifestFinalizationSignal, v: bool) {
        if v {
            if !self.signals.contains(&s) {
                self.signals.push(s);
            }
        } else {
            self.signals.retain(|x| *x != s);
        }
    }
    sb!(
        draft_manifest_written,
        VortexManifestFinalizationSignal::DraftManifestWritten
    );
    sb!(
        draft_manifest_missing,
        VortexManifestFinalizationSignal::DraftManifestMissing
    );
    sb!(
        commit_marker_written,
        VortexManifestFinalizationSignal::CommitMarkerWritten
    );
    sb!(
        commit_marker_missing,
        VortexManifestFinalizationSignal::CommitMarkerMissing
    );
    sb!(
        commit_protocol_ready,
        VortexManifestFinalizationSignal::CommitProtocolReady
    );
    sb!(
        commit_protocol_blocked,
        VortexManifestFinalizationSignal::CommitProtocolBlocked
    );
    sb!(schema_known, VortexManifestFinalizationSignal::SchemaKnown);
    sb!(
        schema_compatible,
        VortexManifestFinalizationSignal::SchemaCompatible
    );
    sb!(
        delete_semantics_known,
        VortexManifestFinalizationSignal::DeleteSemanticsKnown
    );
    sb!(
        tombstone_semantics_known,
        VortexManifestFinalizationSignal::TombstoneSemanticsKnown
    );
    sb!(
        local_workspace,
        VortexManifestFinalizationSignal::LocalWorkspace
    );
    sb!(
        object_store_target,
        VortexManifestFinalizationSignal::ObjectStoreTarget
    );
    sb!(
        feature_gate_enabled,
        VortexManifestFinalizationSignal::FeatureGateEnabled
    );
    #[must_use]
    pub fn with_draft_manifest_summary(mut self, s: impl Into<String>) -> Self {
        self.draft_manifest_summary = Some(s.into());
        self
    }
    #[must_use]
    pub fn with_commit_marker_summary(mut self, s: impl Into<String>) -> Self {
        self.commit_marker_summary = Some(s.into());
        self
    }
    #[must_use]
    pub fn with_protocol_summary(mut self, s: impl Into<String>) -> Self {
        self.protocol_summary = Some(s.into());
        self
    }
    #[must_use]
    pub fn has_signal(&self, s: VortexManifestFinalizationSignal) -> bool {
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
pub struct VortexManifestFinalizationReport {
    pub status: VortexManifestFinalizationStatus,
    pub mode: VortexManifestFinalizationMode,
    pub request: VortexManifestFinalizationRequest,
    pub effects_performed: Vec<VortexManifestFinalizationEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexManifestFinalizationReport {
    /// # Errors
    /// Returns an error if report text assembly fails unexpectedly.
    pub fn from_request(request: VortexManifestFinalizationRequest) -> Result<Self> {
        Ok(Self {
            status: derive_status(&request),
            mode: VortexManifestFinalizationMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        })
    }
    #[must_use]
    pub fn unsupported(
        request: VortexManifestFinalizationRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        Self {
            status: VortexManifestFinalizationStatus::Unsupported,
            mode: VortexManifestFinalizationMode::Unsupported,
            request,
            effects_performed: Vec::new(),
            diagnostics: vec![Diagnostic::new(
                DiagnosticCode::UnsupportedEffect,
                DiagnosticSeverity::Error,
                shardloom_core::DiagnosticCategory::UnsupportedFeature,
                format!("unsupported manifest finalization feature: {feature}"),
                Some(feature),
                Some(reason),
                None,
                FallbackStatus::disabled_by_policy(),
            )],
        }
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
    pub fn draft_manifest_written(&self) -> bool {
        self.request
            .has_signal(VortexManifestFinalizationSignal::DraftManifestWritten)
    }
    #[must_use]
    pub fn commit_marker_written(&self) -> bool {
        self.request
            .has_signal(VortexManifestFinalizationSignal::CommitMarkerWritten)
    }
    #[must_use]
    pub fn commit_protocol_ready(&self) -> bool {
        self.request
            .has_signal(VortexManifestFinalizationSignal::CommitProtocolReady)
    }
    #[must_use]
    pub fn schema_known(&self) -> bool {
        self.request
            .has_signal(VortexManifestFinalizationSignal::SchemaKnown)
    }
    #[must_use]
    pub fn schema_compatible(&self) -> bool {
        self.request
            .has_signal(VortexManifestFinalizationSignal::SchemaCompatible)
    }
    #[must_use]
    pub fn delete_semantics_known(&self) -> bool {
        self.request
            .has_signal(VortexManifestFinalizationSignal::DeleteSemanticsKnown)
    }
    #[must_use]
    pub fn tombstone_semantics_known(&self) -> bool {
        self.request
            .has_signal(VortexManifestFinalizationSignal::TombstoneSemanticsKnown)
    }
    #[must_use]
    pub fn local_workspace(&self) -> bool {
        self.request
            .has_signal(VortexManifestFinalizationSignal::LocalWorkspace)
    }
    #[must_use]
    pub fn object_store_target(&self) -> bool {
        self.request
            .has_signal(VortexManifestFinalizationSignal::ObjectStoreTarget)
    }
    #[must_use]
    pub fn finalized_manifest_written(&self) -> bool {
        self.effects_performed
            .contains(&VortexManifestFinalizationEffect::FinalizedManifestWritten)
    }
    #[must_use]
    pub fn manifest_committed(&self) -> bool {
        self.effects_performed
            .contains(&VortexManifestFinalizationEffect::ManifestCommitted)
    }
    #[must_use]
    pub fn output_data_written(&self) -> bool {
        self.effects_performed
            .contains(&VortexManifestFinalizationEffect::OutputDataWritten)
    }
    #[must_use]
    pub fn object_store_io(&self) -> bool {
        self.effects_performed
            .contains(&VortexManifestFinalizationEffect::ObjectStoreIo)
    }
    #[must_use]
    pub fn upstream_vortex_write_called(&self) -> bool {
        self.effects_performed
            .contains(&VortexManifestFinalizationEffect::UpstreamVortexWriteCalled)
    }
    #[must_use]
    pub fn recovery_action_executed(&self) -> bool {
        self.effects_performed
            .contains(&VortexManifestFinalizationEffect::RecoveryActionExecuted)
    }
    #[must_use]
    pub fn fallback_execution_allowed(&self) -> bool {
        self.effects_performed
            .contains(&VortexManifestFinalizationEffect::FallbackExecution)
    }
    #[must_use]
    pub const fn allows_finalization_execution(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty() && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut o = String::new();
        let _ = writeln!(o, "manifest finalization status: {}", self.status.as_str());
        let _ = writeln!(o, "mode: {}", self.mode.as_str());
        let _ = writeln!(o, "target URI: {}", self.request.target_uri.as_str());
        let _ = writeln!(
            o,
            "finalized manifest path: {}",
            self.request.finalized_manifest_ref.path_string()
        );
        let _ = writeln!(
            o,
            "content length/checksum: {}/{}",
            self.request.finalized_manifest_content.len(),
            self.request.finalized_manifest_content.checksum_u64()
        );
        let _ = writeln!(
            o,
            "draft manifest written: {}",
            self.draft_manifest_written()
        );
        let _ = writeln!(o, "commit marker written: {}", self.commit_marker_written());
        let _ = writeln!(o, "commit protocol ready: {}", self.commit_protocol_ready());
        let _ = writeln!(o, "schema known: {}", self.schema_known());
        let _ = writeln!(o, "schema compatible: {}", self.schema_compatible());
        let _ = writeln!(
            o,
            "delete semantics known: {}",
            self.delete_semantics_known()
        );
        let _ = writeln!(
            o,
            "tombstone semantics known: {}",
            self.tombstone_semantics_known()
        );
        let _ = writeln!(o, "local workspace: {}", self.local_workspace());
        let _ = writeln!(o, "object-store target: {}", self.object_store_target());
        let _ = writeln!(
            o,
            "finalized manifest written: {}",
            self.finalized_manifest_written()
        );
        let _ = writeln!(o, "manifest committed: {}", self.manifest_committed());
        let _ = writeln!(o, "output data written: {}", self.output_data_written());
        let _ = writeln!(o, "object-store IO: {}", self.object_store_io());
        let _ = writeln!(
            o,
            "upstream Vortex write called: {}",
            self.upstream_vortex_write_called()
        );
        let _ = writeln!(
            o,
            "recovery action executed: {}",
            self.recovery_action_executed()
        );
        let _ = writeln!(o, "fallback execution disabled");
        for d in self
            .request
            .diagnostics
            .iter()
            .chain(self.diagnostics.iter())
        {
            let _ = writeln!(o, "diagnostic [{}] {}", d.severity.as_str(), d.message);
        }
        o
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexFinalizedManifestArtifactWriteStatus {
    FeatureDisabled,
    Planned,
    FinalizedManifestArtifactWritten,
    BlockedByFinalizationPlan,
    BlockedByObjectStoreTarget,
    BlockedByMissingWorkspace,
    BlockedByExistingFinalizedManifest,
    BlockedByExistingNonDirectory,
    BlockedByFeatureGate,
    Unsupported,
}
impl VortexFinalizedManifestArtifactWriteStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::Planned => "planned",
            Self::FinalizedManifestArtifactWritten => "finalized_manifest_artifact_written",
            Self::BlockedByFinalizationPlan => "blocked_by_finalization_plan",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByMissingWorkspace => "blocked_by_missing_workspace",
            Self::BlockedByExistingFinalizedManifest => "blocked_by_existing_finalized_manifest",
            Self::BlockedByExistingNonDirectory => "blocked_by_existing_non_directory",
            Self::BlockedByFeatureGate => "blocked_by_feature_gate",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(
            self,
            Self::FeatureDisabled | Self::Planned | Self::FinalizedManifestArtifactWritten
        )
    }
    #[must_use]
    pub const fn finalized_manifest_artifact_written(self) -> bool {
        matches!(self, Self::FinalizedManifestArtifactWritten)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexFinalizedManifestArtifactWriteMode {
    ReportOnly,
    LocalFinalizedManifestArtifactWrite,
    Unsupported,
}
impl VortexFinalizedManifestArtifactWriteMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::LocalFinalizedManifestArtifactWrite => "local_finalized_manifest_artifact_write",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn writes_finalized_manifest(self) -> bool {
        matches!(self, Self::LocalFinalizedManifestArtifactWrite)
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
pub enum VortexFinalizedManifestArtifactWriteOption {
    AllowOverwrite,
}
impl VortexFinalizedManifestArtifactWriteOption {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        "allow_overwrite"
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct VortexFinalizedManifestArtifactWriteRequest {
    pub finalized_manifest_ref: VortexFinalizedManifestFileRef,
    pub finalized_manifest_content: VortexFinalizedManifestContent,
    pub options: Vec<VortexFinalizedManifestArtifactWriteOption>,
    pub finalization_plan_summary: Option<String>,
    pub finalization_plan_feature_gate_ready: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexFinalizedManifestArtifactWriteRequest {
    #[must_use]
    pub fn new(
        finalized_manifest_ref: VortexFinalizedManifestFileRef,
        finalized_manifest_content: VortexFinalizedManifestContent,
    ) -> Self {
        Self {
            finalized_manifest_ref,
            finalized_manifest_content,
            options: Vec::new(),
            finalization_plan_summary: None,
            finalization_plan_feature_gate_ready: false,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn from_finalization_request(request: &VortexManifestFinalizationRequest) -> Self {
        Self::new(
            request.finalized_manifest_ref.clone(),
            request.finalized_manifest_content.clone(),
        )
        .feature_gate_ready(
            request.has_signal(VortexManifestFinalizationSignal::FeatureGateEnabled),
        )
    }
    #[must_use]
    pub fn allow_overwrite(mut self, value: bool) -> Self {
        if value {
            if !self
                .options
                .contains(&VortexFinalizedManifestArtifactWriteOption::AllowOverwrite)
            {
                self.options
                    .push(VortexFinalizedManifestArtifactWriteOption::AllowOverwrite);
            }
        } else {
            self.options
                .retain(|o| *o != VortexFinalizedManifestArtifactWriteOption::AllowOverwrite);
        }
        self
    }
    #[must_use]
    pub fn with_finalization_plan_summary(mut self, s: impl Into<String>) -> Self {
        self.finalization_plan_summary = Some(s.into());
        self
    }
    #[must_use]
    pub fn feature_gate_ready(mut self, v: bool) -> Self {
        self.finalization_plan_feature_gate_ready = v;
        self
    }
    #[must_use]
    pub fn has_option(&self, o: VortexFinalizedManifestArtifactWriteOption) -> bool {
        self.options.contains(&o)
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
            "finalized_manifest_path={} feature_gate_ready={}",
            self.finalized_manifest_ref.path_string(),
            self.finalization_plan_feature_gate_ready
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexFinalizedManifestArtifactWriteReport {
    pub status: VortexFinalizedManifestArtifactWriteStatus,
    pub mode: VortexFinalizedManifestArtifactWriteMode,
    pub request: VortexFinalizedManifestArtifactWriteRequest,
    pub effects_performed: Vec<VortexManifestFinalizationEffect>,
    pub bytes_written: usize,
    pub checksum: Option<u64>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexFinalizedManifestArtifactWriteReport {
    /// # Errors
    /// Propagates finalized-manifest candidate artifact write planning/write errors.
    pub fn from_request(request: VortexFinalizedManifestArtifactWriteRequest) -> Result<Self> {
        write_vortex_finalized_manifest_artifact(request)
    }
    #[must_use]
    pub fn feature_disabled(request: VortexFinalizedManifestArtifactWriteRequest) -> Self {
        Self {
            status: VortexFinalizedManifestArtifactWriteStatus::FeatureDisabled,
            mode: VortexFinalizedManifestArtifactWriteMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            bytes_written: 0,
            checksum: None,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn planned(request: VortexFinalizedManifestArtifactWriteRequest) -> Self {
        Self {
            status: VortexFinalizedManifestArtifactWriteStatus::Planned,
            mode: VortexFinalizedManifestArtifactWriteMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            bytes_written: 0,
            checksum: None,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn wrote_finalized_manifest_artifact(
        request: VortexFinalizedManifestArtifactWriteRequest,
        bytes_written: usize,
        checksum: u64,
    ) -> Self {
        Self {
            status: VortexFinalizedManifestArtifactWriteStatus::FinalizedManifestArtifactWritten,
            mode: VortexFinalizedManifestArtifactWriteMode::LocalFinalizedManifestArtifactWrite,
            request,
            effects_performed: vec![VortexManifestFinalizationEffect::FinalizedManifestWritten],
            bytes_written,
            checksum: Some(checksum),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn blocked(
        request: VortexFinalizedManifestArtifactWriteRequest,
        status: VortexFinalizedManifestArtifactWriteStatus,
        reason: impl Into<String>,
    ) -> Self {
        let reason = reason.into();
        Self {
            status,
            mode: VortexFinalizedManifestArtifactWriteMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            bytes_written: 0,
            checksum: None,
            diagnostics: vec![Diagnostic::invalid_input(
                "finalized_manifest_artifact_write",
                reason.clone(),
                reason,
            )],
        }
    }
    #[must_use]
    pub fn unsupported(
        request: VortexFinalizedManifestArtifactWriteRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let feature = feature.into();
        let reason = reason.into();
        Self {
            status: VortexFinalizedManifestArtifactWriteStatus::Unsupported,
            mode: VortexFinalizedManifestArtifactWriteMode::Unsupported,
            request,
            effects_performed: Vec::new(),
            bytes_written: 0,
            checksum: None,
            diagnostics: vec![Diagnostic::new(
                DiagnosticCode::UnsupportedEffect,
                DiagnosticSeverity::Error,
                shardloom_core::DiagnosticCategory::UnsupportedFeature,
                format!("unsupported finalized manifest artifact write feature: {feature}"),
                Some(feature),
                Some(reason),
                None,
                FallbackStatus::disabled_by_policy(),
            )],
        }
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
    pub fn finalized_manifest_artifact_written(&self) -> bool {
        self.status.finalized_manifest_artifact_written()
    }
    #[must_use]
    pub fn finalized_manifest_written(&self) -> bool {
        self.effects_performed
            .contains(&VortexManifestFinalizationEffect::FinalizedManifestWritten)
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
    pub fn is_side_effect_free(&self) -> bool {
        !self.finalized_manifest_artifact_written()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut o = String::new();
        let _ = writeln!(
            o,
            "finalized manifest artifact write status: {}",
            self.status.as_str()
        );
        let _ = writeln!(o, "mode: {}", self.mode.as_str());
        let _ = writeln!(
            o,
            "finalized manifest candidate path: {}",
            self.request.finalized_manifest_ref.path_string()
        );
        let _ = writeln!(o, "bytes written: {}", self.bytes_written);
        let _ = writeln!(
            o,
            "checksum: {}",
            self.checksum
                .map_or_else(|| "unknown".to_string(), |c| c.to_string())
        );
        let _ = writeln!(
            o,
            "feature gate ready: {}",
            self.request.finalization_plan_feature_gate_ready
        );
        let _ = writeln!(
            o,
            "finalized manifest artifact written: {}",
            self.finalized_manifest_artifact_written()
        );
        let _ = writeln!(
            o,
            "finalized manifest written: {}",
            self.finalized_manifest_written()
        );
        let _ = writeln!(o, "manifest committed: false");
        let _ = writeln!(o, "output data written: false");
        let _ = writeln!(o, "object-store IO: false");
        let _ = writeln!(o, "upstream Vortex write called: false");
        let _ = writeln!(o, "recovery action executed: false");
        let _ = writeln!(o, "fallback execution disabled");
        for d in self
            .request
            .diagnostics
            .iter()
            .chain(self.diagnostics.iter())
        {
            let _ = writeln!(o, "diagnostic [{}] {}", d.severity.as_str(), d.message);
        }
        o
    }
}

/// # Errors
/// Returns an error when finalization-plan to artifact-write request conversion fails.
pub fn finalized_manifest_artifact_write_request_from_plan(
    plan: &VortexManifestFinalizationReport,
) -> Result<VortexFinalizedManifestArtifactWriteRequest> {
    let mut req =
        VortexFinalizedManifestArtifactWriteRequest::from_finalization_request(&plan.request);
    if plan.status == VortexManifestFinalizationStatus::FinalizationReady {
        req = req.with_finalization_plan_summary(plan.request.summary());
    }
    if plan.has_errors() {
        for d in plan
            .request
            .diagnostics
            .iter()
            .chain(plan.diagnostics.iter())
        {
            req.add_diagnostic(d.clone());
        }
    }
    Ok(req)
}

/// # Errors
/// Returns an error when writing the local finalized-manifest candidate artifact fails.
pub fn write_vortex_finalized_manifest_artifact(
    request: VortexFinalizedManifestArtifactWriteRequest,
) -> Result<VortexFinalizedManifestArtifactWriteReport> {
    #[cfg(not(feature = "vortex-staged-output-fs"))]
    {
        Ok(VortexFinalizedManifestArtifactWriteReport::feature_disabled(request))
    }
    #[cfg(feature = "vortex-staged-output-fs")]
    {
        use std::{fs, path::PathBuf};
        if !request.finalization_plan_feature_gate_ready {
            return Ok(VortexFinalizedManifestArtifactWriteReport::blocked(
                request,
                VortexFinalizedManifestArtifactWriteStatus::BlockedByFeatureGate,
                "feature gate readiness is required",
            ));
        }
        let path = PathBuf::from(request.finalized_manifest_ref.path_string());
        let pstr = path.to_string_lossy();
        if pstr.contains("://") {
            return Ok(VortexFinalizedManifestArtifactWriteReport::blocked(
                request,
                VortexFinalizedManifestArtifactWriteStatus::BlockedByObjectStoreTarget,
                "object-store-like finalized manifest target is blocked",
            ));
        }
        let Some(parent) = path.parent() else {
            return Ok(VortexFinalizedManifestArtifactWriteReport::blocked(
                request,
                VortexFinalizedManifestArtifactWriteStatus::BlockedByMissingWorkspace,
                "workspace parent path is missing",
            ));
        };
        if !parent.exists() {
            return Ok(VortexFinalizedManifestArtifactWriteReport::blocked(
                request,
                VortexFinalizedManifestArtifactWriteStatus::BlockedByMissingWorkspace,
                "workspace directory is missing",
            ));
        }
        if !parent.is_dir() {
            return Ok(VortexFinalizedManifestArtifactWriteReport::blocked(
                request,
                VortexFinalizedManifestArtifactWriteStatus::BlockedByExistingNonDirectory,
                "workspace parent exists but is not a directory",
            ));
        }
        if path.exists()
            && !request.has_option(VortexFinalizedManifestArtifactWriteOption::AllowOverwrite)
        {
            return Ok(VortexFinalizedManifestArtifactWriteReport::blocked(
                request,
                VortexFinalizedManifestArtifactWriteStatus::BlockedByExistingFinalizedManifest,
                "finalized-manifest candidate already exists",
            ));
        }
        fs::write(&path, request.finalized_manifest_content.as_str()).map_err(|e| {
            ShardLoomError::new(format!(
                "failed to write finalized-manifest candidate artifact: {e}"
            ))
        })?;
        let bytes = request.finalized_manifest_content.len();
        let checksum = request.finalized_manifest_content.checksum_u64();
        Ok(
            VortexFinalizedManifestArtifactWriteReport::wrote_finalized_manifest_artifact(
                request, bytes, checksum,
            ),
        )
    }
}

#[must_use]
pub fn vortex_finalized_manifest_artifact_write_is_side_effect_free(
    report: &VortexFinalizedManifestArtifactWriteReport,
) -> bool {
    report.is_side_effect_free()
}

fn derive_status(req: &VortexManifestFinalizationRequest) -> VortexManifestFinalizationStatus {
    if req.has_signal(VortexManifestFinalizationSignal::ObjectStoreTarget) {
        return VortexManifestFinalizationStatus::BlockedByObjectStoreTarget;
    }
    if req.has_signal(VortexManifestFinalizationSignal::DraftManifestMissing)
        || !req.has_signal(VortexManifestFinalizationSignal::DraftManifestWritten)
    {
        return VortexManifestFinalizationStatus::BlockedByDraftManifest;
    }
    if req.has_signal(VortexManifestFinalizationSignal::CommitMarkerMissing)
        || !req.has_signal(VortexManifestFinalizationSignal::CommitMarkerWritten)
    {
        return VortexManifestFinalizationStatus::BlockedByCommitMarker;
    }
    if req.has_signal(VortexManifestFinalizationSignal::CommitProtocolBlocked)
        || !req.has_signal(VortexManifestFinalizationSignal::CommitProtocolReady)
    {
        return VortexManifestFinalizationStatus::BlockedByCommitProtocol;
    }
    if !req.has_signal(VortexManifestFinalizationSignal::SchemaKnown)
        || !req.has_signal(VortexManifestFinalizationSignal::SchemaCompatible)
    {
        return VortexManifestFinalizationStatus::BlockedBySchema;
    }
    if !req.has_signal(VortexManifestFinalizationSignal::DeleteSemanticsKnown) {
        return VortexManifestFinalizationStatus::BlockedByDeleteSemantics;
    }
    if !req.has_signal(VortexManifestFinalizationSignal::TombstoneSemanticsKnown) {
        return VortexManifestFinalizationStatus::BlockedByTombstoneSemantics;
    }
    if !req.has_signal(VortexManifestFinalizationSignal::FeatureGateEnabled) {
        return VortexManifestFinalizationStatus::BlockedByFeatureGate;
    }
    VortexManifestFinalizationStatus::FinalizationReady
}

/// # Errors
/// Propagates report construction errors.
pub fn plan_vortex_manifest_finalization(
    request: VortexManifestFinalizationRequest,
) -> Result<VortexManifestFinalizationReport> {
    VortexManifestFinalizationReport::from_request(request)
}
#[must_use]
pub fn vortex_manifest_finalization_is_side_effect_free(
    report: &VortexManifestFinalizationReport,
) -> bool {
    report.is_side_effect_free()
}

/// # Errors
/// Returns an error if finalized-manifest candidate inputs are invalid.
pub fn manifest_finalization_request_from_reports(
    target_uri: DatasetUri,
    workspace_path: VortexStagedWorkspacePath,
    staged_manifest_write: &VortexStagedManifestFileWriteReport,
    commit_marker_write: &VortexCommitMarkerWriteReport,
    commit_protocol: &VortexCommitProtocolReport,
) -> Result<VortexManifestFinalizationRequest> {
    let draft_summary = staged_manifest_write.request.file_ref.summary();
    let marker_summary = commit_marker_write.request.marker_ref.summary();
    let content =
        VortexFinalizedManifestContent::from_inputs(draft_summary.clone(), marker_summary.clone())?;
    let mut req = VortexManifestFinalizationRequest::new(
        target_uri,
        VortexFinalizedManifestFileRef::default_for_workspace(workspace_path),
        content,
    )
    .with_draft_manifest_summary(draft_summary)
    .with_commit_marker_summary(marker_summary)
    .with_protocol_summary(commit_protocol.request.summary())
    .draft_manifest_written(staged_manifest_write.draft_file_written())
    .draft_manifest_missing(!staged_manifest_write.draft_file_written())
    .commit_marker_written(commit_marker_write.commit_marker_written())
    .commit_marker_missing(!commit_marker_write.commit_marker_written())
    .commit_protocol_ready(matches!(
        commit_protocol.next_state(),
        VortexCommitProtocolState::CommitReady
    ))
    .commit_protocol_blocked(commit_protocol.has_errors())
    .object_store_target(
        staged_manifest_write.object_store_target()
            || commit_marker_write
                .request
                .has_signal(crate::VortexCommitMarkerWriteSignal::ObjectStoreTarget)
            || commit_protocol.object_store_target()
            || staged_manifest_write.object_store_io()
            || commit_marker_write.object_store_io()
            || commit_protocol.object_store_io(),
    )
    .local_workspace(true);
    for d in staged_manifest_write
        .request
        .diagnostics
        .iter()
        .chain(staged_manifest_write.diagnostics.iter())
        .chain(commit_marker_write.request.diagnostics.iter())
        .chain(commit_marker_write.diagnostics.iter())
        .chain(commit_protocol.request.diagnostics.iter())
        .chain(commit_protocol.diagnostics.iter())
    {
        req.add_diagnostic(d.clone());
    }
    Ok(req)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        VortexCommitMarkerContent, VortexCommitMarkerFileRef, VortexCommitMarkerWriteRequest,
        VortexCommitProtocolRequest, VortexCommitProtocolState, VortexCommitProtocolTransition,
        VortexStagedManifestDraftContent, VortexStagedManifestFileRef,
        VortexStagedManifestFileWriteRequest,
    };
    fn base_req() -> VortexManifestFinalizationRequest {
        let uri = DatasetUri::new("file://tmp/a.vortex").unwrap();
        let ws = VortexStagedWorkspacePath::new("/tmp/w").unwrap();
        let rf = VortexFinalizedManifestFileRef::default_for_workspace(ws);
        let c = VortexFinalizedManifestContent::new("x").unwrap();
        VortexManifestFinalizationRequest::new(uri, rf, c)
    }
    #[test]
    fn manifest_finalization_status_and_mode_helpers() {
        assert!(
            !VortexManifestFinalizationStatus::FinalizationReady.allows_finalization_execution()
        );
        assert!(VortexManifestFinalizationStatus::BlockedByDraftManifest.is_error());
        assert!(!VortexManifestFinalizationMode::ReportOnly.writes_finalized_manifest());
        assert!(!VortexManifestFinalizationMode::ReportOnly.commits_manifest());
    }

    #[test]
    fn finalized_manifest_file_name_validation() {
        assert!(VortexFinalizedManifestFileName::new(" ").is_err());
        assert!(VortexFinalizedManifestFileName::new("a/b").is_err());
        assert!(VortexFinalizedManifestFileName::new("..a").is_err());
        assert_eq!(
            VortexFinalizedManifestFileName::default_finalized().as_str(),
            "_shardloom_finalized_manifest.json"
        );
    }

    #[test]
    fn finalized_manifest_file_ref_path_is_deterministic() {
        assert_eq!(
            VortexFinalizedManifestFileRef::default_for_workspace(
                VortexStagedWorkspacePath::new("/x").unwrap()
            )
            .path_string(),
            "/x/_shardloom_finalized_manifest.json"
        );
    }

    #[test]
    fn finalized_manifest_content_validation_and_checksum() {
        assert!(VortexFinalizedManifestContent::new(" ").is_err());
        assert!(VortexFinalizedManifestContent::new("x".repeat(65537)).is_err());
        assert_eq!(
            VortexFinalizedManifestContent::new("abc")
                .unwrap()
                .checksum_u64(),
            VortexFinalizedManifestContent::new("abc")
                .unwrap()
                .checksum_u64()
        );
    }

    #[test]
    fn manifest_finalization_request_signal_helpers() {
        let mut r = base_req();
        r.add_signal(VortexManifestFinalizationSignal::SchemaKnown, true);
        r.add_signal(VortexManifestFinalizationSignal::SchemaKnown, true);
        r.add_signal(VortexManifestFinalizationSignal::SchemaKnown, false);
        assert!(!r.has_signal(VortexManifestFinalizationSignal::SchemaKnown));
    }

    #[test]
    fn manifest_finalization_blocks_missing_inputs() {
        let rep =
            VortexManifestFinalizationReport::from_request(base_req().object_store_target(true))
                .unwrap();
        assert_eq!(
            rep.status,
            VortexManifestFinalizationStatus::BlockedByObjectStoreTarget
        );
        assert_eq!(
            VortexManifestFinalizationReport::from_request(base_req())
                .unwrap()
                .status,
            VortexManifestFinalizationStatus::BlockedByDraftManifest
        );
        assert_eq!(
            VortexManifestFinalizationReport::from_request(base_req().draft_manifest_written(true))
                .unwrap()
                .status,
            VortexManifestFinalizationStatus::BlockedByCommitMarker
        );
        assert_eq!(
            VortexManifestFinalizationReport::from_request(
                base_req()
                    .draft_manifest_written(true)
                    .commit_marker_written(true)
            )
            .unwrap()
            .status,
            VortexManifestFinalizationStatus::BlockedByCommitProtocol
        );
        assert_eq!(
            VortexManifestFinalizationReport::from_request(
                base_req()
                    .draft_manifest_written(true)
                    .commit_marker_written(true)
                    .commit_protocol_ready(true)
            )
            .unwrap()
            .status,
            VortexManifestFinalizationStatus::BlockedBySchema
        );
        assert_eq!(
            VortexManifestFinalizationReport::from_request(
                base_req()
                    .draft_manifest_written(true)
                    .commit_marker_written(true)
                    .commit_protocol_ready(true)
                    .schema_known(true)
            )
            .unwrap()
            .status,
            VortexManifestFinalizationStatus::BlockedBySchema
        );
        assert_eq!(
            VortexManifestFinalizationReport::from_request(
                base_req()
                    .draft_manifest_written(true)
                    .commit_marker_written(true)
                    .commit_protocol_ready(true)
                    .schema_known(true)
                    .schema_compatible(true)
            )
            .unwrap()
            .status,
            VortexManifestFinalizationStatus::BlockedByDeleteSemantics
        );
        assert_eq!(
            VortexManifestFinalizationReport::from_request(
                base_req()
                    .draft_manifest_written(true)
                    .commit_marker_written(true)
                    .commit_protocol_ready(true)
                    .schema_known(true)
                    .schema_compatible(true)
                    .delete_semantics_known(true)
            )
            .unwrap()
            .status,
            VortexManifestFinalizationStatus::BlockedByTombstoneSemantics
        );
    }

    #[test]
    fn manifest_finalization_ready_remains_report_only() {
        let ready = VortexManifestFinalizationReport::from_request(
            base_req()
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
        assert_eq!(
            ready.status,
            VortexManifestFinalizationStatus::FinalizationReady
        );
        assert!(!ready.allows_finalization_execution());
    }

    #[test]
    fn manifest_finalization_report_side_effect_flags() {
        let ready = VortexManifestFinalizationReport::from_request(
            base_req()
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
        assert!(
            !ready.finalized_manifest_written()
                && !ready.manifest_committed()
                && !ready.output_data_written()
                && !ready.object_store_io()
                && !ready.upstream_vortex_write_called()
                && !ready.recovery_action_executed()
                && !ready.fallback_execution_allowed()
        );
        assert!(ready.is_side_effect_free());
        assert!(plan_vortex_manifest_finalization(base_req()).is_ok());
    }

    #[test]
    fn manifest_finalization_human_text_renders_diagnostics() {
        let ready = VortexManifestFinalizationReport::from_request(
            base_req()
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
        assert!(
            ready
                .to_human_text()
                .contains("fallback execution disabled")
        );
        assert!(
            ready
                .to_human_text()
                .contains("finalized manifest written: false")
        );
        let mut with_diag = ready.clone();
        with_diag.add_diagnostic(Diagnostic::invalid_input("x", "y", "z"));
        assert!(with_diag.to_human_text().contains("diagnostic"));
    }

    #[test]
    fn manifest_finalization_request_from_reports_maps_inputs() {
        let ws = VortexStagedWorkspacePath::new("/tmp/w").unwrap();
        let staged_req = VortexStagedManifestFileWriteRequest::new(
            VortexStagedManifestFileRef::default_for_workspace(ws.clone()),
            VortexStagedManifestDraftContent::new("draft=true").unwrap(),
        )
        .file_plan_ready(true)
        .workspace_known(true)
        .feature_gate_enabled(true);
        let mut staged = VortexStagedManifestFileWriteReport::from_request(staged_req).unwrap();
        staged
            .effects_performed
            .push(crate::VortexStagedManifestFileWriteEffect::DraftFileWritten);
        let marker_req = VortexCommitMarkerWriteRequest::new(
            VortexCommitMarkerFileRef::default_for_workspace(ws.clone()),
            VortexCommitMarkerContent::new("marker=true").unwrap(),
        )
        .marker_plan_ready(true)
        .feature_gate_ready(true);
        let mut marker = VortexCommitMarkerWriteReport::from_request(marker_req).unwrap();
        marker
            .effects_performed
            .push(crate::VortexCommitMarkerEffect::CommitMarkerWritten);
        let protocol_req = VortexCommitProtocolRequest::new(
            DatasetUri::new("file://tmp/a.vtx").unwrap(),
            VortexCommitProtocolState::AwaitingCommitMarker,
            VortexCommitProtocolTransition::MarkCommitReady,
        )
        .commit_intent_ready(true)
        .draft_manifest_ready(true)
        .manifest_finalization_available(true)
        .commit_marker_available(true)
        .recovery_ready(true);
        let protocol = VortexCommitProtocolReport::from_request(protocol_req).unwrap();
        let req = manifest_finalization_request_from_reports(
            DatasetUri::new("file://tmp/a.vtx").unwrap(),
            ws,
            &staged,
            &marker,
            &protocol,
        )
        .unwrap();
        assert!(req.has_signal(VortexManifestFinalizationSignal::DraftManifestWritten));
        assert!(req.has_signal(VortexManifestFinalizationSignal::CommitMarkerWritten));
        let rep = VortexManifestFinalizationReport::from_request(req).unwrap();
        assert!(!rep.manifest_committed());
    }

    #[test]
    fn finalized_manifest_artifact_write_request_options_and_feature_disabled() {
        let req = VortexFinalizedManifestArtifactWriteRequest::new(
            VortexFinalizedManifestFileRef::default_for_workspace(
                VortexStagedWorkspacePath::new("/tmp/w").unwrap(),
            ),
            VortexFinalizedManifestContent::new("abc").unwrap(),
        );
        assert!(!req.has_option(VortexFinalizedManifestArtifactWriteOption::AllowOverwrite));
        let req = req.allow_overwrite(true);
        assert!(req.has_option(VortexFinalizedManifestArtifactWriteOption::AllowOverwrite));
        let rep = write_vortex_finalized_manifest_artifact(req).unwrap();
        #[cfg(not(feature = "vortex-staged-output-fs"))]
        {
            assert_eq!(
                rep.status,
                VortexFinalizedManifestArtifactWriteStatus::FeatureDisabled
            );
        }
        assert!(!rep.finalized_manifest_artifact_written());
        assert!(!rep.manifest_committed());
        assert!(!rep.output_data_written());
        assert!(!rep.object_store_io());
        assert!(!rep.upstream_vortex_write_called());
        assert!(!rep.recovery_action_executed());
        assert!(!rep.fallback_execution_allowed());
        assert!(rep.to_human_text().contains("fallback execution disabled"));
        assert!(rep.to_human_text().contains("output data written: false"));
        assert!(rep.to_human_text().contains("candidate path"));
        assert!(rep.to_human_text().contains("manifest committed: false"));
    }

    #[test]
    fn finalized_manifest_artifact_request_from_plan_preserves_readiness() {
        let plan = VortexManifestFinalizationReport::from_request(
            base_req()
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
        let req = finalized_manifest_artifact_write_request_from_plan(&plan).unwrap();
        assert!(req.finalization_plan_feature_gate_ready);
    }

    #[cfg(feature = "vortex-staged-output-fs")]
    #[test]
    fn finalized_manifest_artifact_write_feature_paths() {
        use std::fs;
        use std::time::{SystemTime, UNIX_EPOCH};
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("shardloom-finalized-artifact-{nanos}"));
        let workspace = root.join("ws");
        let file_path = workspace.join("_shardloom_finalized_manifest.json");
        let ws = VortexStagedWorkspacePath::new(workspace.to_str().unwrap()).unwrap();
        let base = VortexFinalizedManifestArtifactWriteRequest::new(
            VortexFinalizedManifestFileRef::default_for_workspace(ws.clone()),
            VortexFinalizedManifestContent::new("manifest=1").unwrap(),
        );
        let rep = write_vortex_finalized_manifest_artifact(base.clone()).unwrap();
        assert_eq!(
            rep.status,
            VortexFinalizedManifestArtifactWriteStatus::BlockedByFeatureGate
        );
        let rep = write_vortex_finalized_manifest_artifact(base.clone().feature_gate_ready(true))
            .unwrap();
        assert_eq!(
            rep.status,
            VortexFinalizedManifestArtifactWriteStatus::BlockedByMissingWorkspace
        );
        fs::create_dir_all(&workspace).unwrap();
        let rep = write_vortex_finalized_manifest_artifact(base.clone().feature_gate_ready(true))
            .unwrap();
        assert!(rep.finalized_manifest_artifact_written());
        assert!(file_path.exists());
        assert_eq!(fs::read_to_string(&file_path).unwrap(), "manifest=1");
        let rep = write_vortex_finalized_manifest_artifact(base.clone().feature_gate_ready(true))
            .unwrap();
        assert_eq!(
            rep.status,
            VortexFinalizedManifestArtifactWriteStatus::BlockedByExistingFinalizedManifest
        );
        let rep = write_vortex_finalized_manifest_artifact(
            base.clone().feature_gate_ready(true).allow_overwrite(true),
        )
        .unwrap();
        assert!(rep.finalized_manifest_artifact_written());
        let obj_ws = VortexStagedWorkspacePath::new("s3://bucket/prefix").unwrap();
        let obj_req = VortexFinalizedManifestArtifactWriteRequest::new(
            VortexFinalizedManifestFileRef::default_for_workspace(obj_ws),
            VortexFinalizedManifestContent::new("x").unwrap(),
        )
        .feature_gate_ready(true);
        let object_store_report = write_vortex_finalized_manifest_artifact(obj_req).unwrap();
        assert_eq!(
            object_store_report.status,
            VortexFinalizedManifestArtifactWriteStatus::BlockedByObjectStoreTarget
        );
        fs::remove_file(&file_path).unwrap();
        fs::remove_dir(&workspace).unwrap();
        fs::remove_dir(&root).unwrap();
    }
}
