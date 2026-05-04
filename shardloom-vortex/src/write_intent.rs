use std::fmt::Write as _;

use shardloom_core::{
    DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, Result, UriScheme,
};
#[cfg(test)]
use shardloom_core::{DiagnosticCategory, FallbackStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexWriteIntentStatus {
    Planned,
    StagedOutputRequired,
    BlockedBySchema,
    BlockedByDeleteSemantics,
    BlockedByTombstoneSemantics,
    BlockedByCommitProtocol,
    BlockedByObjectStoreWrite,
    BlockedByFeatureGate,
    Unsupported,
}
impl VortexWriteIntentStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::StagedOutputRequired => "staged_output_required",
            Self::BlockedBySchema => "blocked_by_schema",
            Self::BlockedByDeleteSemantics => "blocked_by_delete_semantics",
            Self::BlockedByTombstoneSemantics => "blocked_by_tombstone_semantics",
            Self::BlockedByCommitProtocol => "blocked_by_commit_protocol",
            Self::BlockedByObjectStoreWrite => "blocked_by_object_store_write",
            Self::BlockedByFeatureGate => "blocked_by_feature_gate",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::Planned | Self::StagedOutputRequired)
    }
    #[must_use]
    pub const fn allows_write_execution(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexWriteIntentMode {
    ReportOnly,
    NativeVortexIntent,
    StagedOutputPlanning,
    Unsupported,
}
impl VortexWriteIntentMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::NativeVortexIntent => "native_vortex_intent",
            Self::StagedOutputPlanning => "staged_output_planning",
            Self::Unsupported => "unsupported",
        }
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
    #[must_use]
    pub const fn calls_upstream_vortex_write(self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexWriteIntentSignal {
    TargetIsNativeVortex,
    StagedOutputRequired,
    SchemaKnown,
    SchemaCompatible,
    DeleteSemanticsKnown,
    TombstoneSemanticsKnown,
    CommitProtocolAvailable,
    ObjectStoreTarget,
    UpstreamVortexWriteFeatureEnabled,
}
impl VortexWriteIntentSignal {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TargetIsNativeVortex => "target_is_native_vortex",
            Self::StagedOutputRequired => "staged_output_required",
            Self::SchemaKnown => "schema_known",
            Self::SchemaCompatible => "schema_compatible",
            Self::DeleteSemanticsKnown => "delete_semantics_known",
            Self::TombstoneSemanticsKnown => "tombstone_semantics_known",
            Self::CommitProtocolAvailable => "commit_protocol_available",
            Self::ObjectStoreTarget => "object_store_target",
            Self::UpstreamVortexWriteFeatureEnabled => "upstream_vortex_write_feature_enabled",
        }
    }
    #[must_use]
    pub const fn is_blocking_absence(self) -> bool {
        matches!(
            self,
            Self::TargetIsNativeVortex
                | Self::SchemaKnown
                | Self::SchemaCompatible
                | Self::DeleteSemanticsKnown
                | Self::TombstoneSemanticsKnown
                | Self::CommitProtocolAvailable
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexWriteIntentEffect {
    OutputDataWritten,
    ManifestWritten,
    ObjectStoreIo,
    UpstreamVortexWriteCalled,
    FallbackExecution,
}
impl VortexWriteIntentEffect {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OutputDataWritten => "output_data_written",
            Self::ManifestWritten => "manifest_written",
            Self::ObjectStoreIo => "object_store_io",
            Self::UpstreamVortexWriteCalled => "upstream_vortex_write_called",
            Self::FallbackExecution => "fallback_execution",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexWriteIntentRequest {
    pub target_uri: DatasetUri,
    pub signals: Vec<VortexWriteIntentSignal>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexWriteIntentRequest {
    #[must_use]
    pub fn new(target_uri: DatasetUri) -> Self {
        Self {
            target_uri,
            signals: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    pub fn add_signal(&mut self, signal: VortexWriteIntentSignal, value: bool) {
        if value {
            if !self.signals.contains(&signal) {
                self.signals.push(signal);
            }
        } else {
            self.signals.retain(|s| *s != signal);
        }
    }
    #[must_use]
    pub fn target_is_native_vortex(mut self, v: bool) -> Self {
        self.add_signal(VortexWriteIntentSignal::TargetIsNativeVortex, v);
        self
    }
    #[must_use]
    pub fn staged_output_required(mut self, v: bool) -> Self {
        self.add_signal(VortexWriteIntentSignal::StagedOutputRequired, v);
        self
    }
    #[must_use]
    pub fn schema_known(mut self, v: bool) -> Self {
        self.add_signal(VortexWriteIntentSignal::SchemaKnown, v);
        self
    }
    #[must_use]
    pub fn schema_compatible(mut self, v: bool) -> Self {
        self.add_signal(VortexWriteIntentSignal::SchemaCompatible, v);
        self
    }
    #[must_use]
    pub fn delete_semantics_known(mut self, v: bool) -> Self {
        self.add_signal(VortexWriteIntentSignal::DeleteSemanticsKnown, v);
        self
    }
    #[must_use]
    pub fn tombstone_semantics_known(mut self, v: bool) -> Self {
        self.add_signal(VortexWriteIntentSignal::TombstoneSemanticsKnown, v);
        self
    }
    #[must_use]
    pub fn commit_protocol_available(mut self, v: bool) -> Self {
        self.add_signal(VortexWriteIntentSignal::CommitProtocolAvailable, v);
        self
    }
    #[must_use]
    pub fn object_store_target(mut self, v: bool) -> Self {
        self.add_signal(VortexWriteIntentSignal::ObjectStoreTarget, v);
        self
    }
    #[must_use]
    pub fn upstream_vortex_write_feature_enabled(mut self, v: bool) -> Self {
        self.add_signal(
            VortexWriteIntentSignal::UpstreamVortexWriteFeatureEnabled,
            v,
        );
        self
    }
    #[must_use]
    pub fn has_signal(&self, s: VortexWriteIntentSignal) -> bool {
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
pub struct VortexWriteIntentReport {
    pub status: VortexWriteIntentStatus,
    pub mode: VortexWriteIntentMode,
    pub request: VortexWriteIntentRequest,
    pub effects_performed: Vec<VortexWriteIntentEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexWriteIntentReport {
    /// # Errors
    /// Returns an error only if diagnostic rendering fails unexpectedly.
    pub fn from_request(request: VortexWriteIntentRequest) -> Result<Self> {
        let mut report = Self {
            status: VortexWriteIntentStatus::Planned,
            mode: VortexWriteIntentMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        };
        if report.request.has_errors() {
            report.status = VortexWriteIntentStatus::Unsupported;
            report.mode = VortexWriteIntentMode::Unsupported;
        } else if report.object_store_target() {
            report.status = VortexWriteIntentStatus::BlockedByObjectStoreWrite;
        } else if !report.target_is_native_vortex() {
            report.status = VortexWriteIntentStatus::Unsupported;
            report.add_diagnostic(Diagnostic::unsupported(
                DiagnosticCode::UnsupportedOutputFormat,
                "vortex_write_intent_target",
                "Native Vortex target signal is required. Fallback execution was not attempted.",
                None,
            ));
            report.mode = VortexWriteIntentMode::Unsupported;
        } else if !report.schema_known() || !report.schema_compatible() {
            report.status = VortexWriteIntentStatus::BlockedBySchema;
        } else if !report.delete_semantics_known() {
            report.status = VortexWriteIntentStatus::BlockedByDeleteSemantics;
        } else if !report.tombstone_semantics_known() {
            report.status = VortexWriteIntentStatus::BlockedByTombstoneSemantics;
        } else if report
            .request
            .has_signal(VortexWriteIntentSignal::StagedOutputRequired)
        {
            report.status = VortexWriteIntentStatus::StagedOutputRequired;
            report.mode = VortexWriteIntentMode::StagedOutputPlanning;
        } else if !report.commit_protocol_available() {
            report.status = VortexWriteIntentStatus::BlockedByCommitProtocol;
            report.mode = VortexWriteIntentMode::ReportOnly;
        } else if !report
            .request
            .has_signal(VortexWriteIntentSignal::UpstreamVortexWriteFeatureEnabled)
        {
            report.status = VortexWriteIntentStatus::BlockedByFeatureGate;
            report.mode = VortexWriteIntentMode::ReportOnly;
        } else {
            report.status = VortexWriteIntentStatus::Planned;
            report.mode = VortexWriteIntentMode::NativeVortexIntent;
        }
        Ok(report)
    }
    #[must_use]
    pub fn unsupported(
        request: VortexWriteIntentRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut report = Self {
            status: VortexWriteIntentStatus::Unsupported,
            mode: VortexWriteIntentMode::Unsupported,
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
    pub fn target_is_native_vortex(&self) -> bool {
        self.request
            .has_signal(VortexWriteIntentSignal::TargetIsNativeVortex)
    }
    #[must_use]
    pub fn staged_output_required(&self) -> bool {
        self.request
            .has_signal(VortexWriteIntentSignal::StagedOutputRequired)
            || matches!(self.status, VortexWriteIntentStatus::StagedOutputRequired)
    }
    #[must_use]
    pub fn schema_known(&self) -> bool {
        self.request
            .has_signal(VortexWriteIntentSignal::SchemaKnown)
    }
    #[must_use]
    pub fn schema_compatible(&self) -> bool {
        self.request
            .has_signal(VortexWriteIntentSignal::SchemaCompatible)
    }
    #[must_use]
    pub fn delete_semantics_known(&self) -> bool {
        self.request
            .has_signal(VortexWriteIntentSignal::DeleteSemanticsKnown)
    }
    #[must_use]
    pub fn tombstone_semantics_known(&self) -> bool {
        self.request
            .has_signal(VortexWriteIntentSignal::TombstoneSemanticsKnown)
    }
    #[must_use]
    pub fn commit_protocol_available(&self) -> bool {
        self.request
            .has_signal(VortexWriteIntentSignal::CommitProtocolAvailable)
    }
    #[must_use]
    pub fn object_store_target(&self) -> bool {
        self.request
            .has_signal(VortexWriteIntentSignal::ObjectStoreTarget)
            || matches!(
                self.request.target_uri.scheme(),
                UriScheme::S3 | UriScheme::Gcs | UriScheme::Adls | UriScheme::Other
            )
    }
    #[must_use]
    pub fn output_data_written(&self) -> bool {
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
    pub const fn upstream_vortex_write_called(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn allows_write_execution(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty() && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut t = String::new();
        let _ = writeln!(t, "Vortex write intent plan");
        let _ = writeln!(t, "status: {}", self.status.as_str());
        let _ = writeln!(t, "mode: {}", self.mode.as_str());
        let _ = writeln!(t, "target URI: {}", self.request.target_uri.as_str());
        let _ = writeln!(
            t,
            "target is native Vortex: {}",
            self.target_is_native_vortex()
        );
        let _ = writeln!(
            t,
            "staged output required: {}",
            self.staged_output_required()
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
        let _ = writeln!(
            t,
            "commit protocol available: {}",
            self.commit_protocol_available()
        );
        let _ = writeln!(t, "object-store target: {}", self.object_store_target());
        let _ = writeln!(t, "output data written: false");
        let _ = writeln!(t, "manifest written: false");
        let _ = writeln!(t, "object-store IO: false");
        let _ = writeln!(t, "upstream Vortex write called: false");
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
/// Propagates errors from `VortexWriteIntentReport::from_request`.
pub fn plan_vortex_write_intent(
    request: VortexWriteIntentRequest,
) -> Result<VortexWriteIntentReport> {
    VortexWriteIntentReport::from_request(request)
}
#[must_use]
pub fn vortex_write_intent_is_side_effect_free(report: &VortexWriteIntentReport) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn status_planned_disallows_write() {
        assert!(!VortexWriteIntentStatus::Planned.allows_write_execution());
    }
    #[test]
    fn status_blocked_schema_is_error() {
        assert!(VortexWriteIntentStatus::BlockedBySchema.is_error());
    }
    #[test]
    fn mode_report_only_no_writes() {
        assert!(!VortexWriteIntentMode::ReportOnly.writes_output_data());
        assert!(!VortexWriteIntentMode::ReportOnly.writes_manifest());
    }
    #[test]
    fn signal_add_remove_no_duplicates() {
        let uri = DatasetUri::new("file://tmp/out.vortex").unwrap();
        let mut r = VortexWriteIntentRequest::new(uri);
        r.add_signal(VortexWriteIntentSignal::SchemaKnown, true);
        r.add_signal(VortexWriteIntentSignal::SchemaKnown, true);
        assert_eq!(r.signals.len(), 1);
        r.add_signal(VortexWriteIntentSignal::SchemaKnown, false);
        assert!(r.signals.is_empty());
    }
    fn base() -> VortexWriteIntentRequest {
        VortexWriteIntentRequest::new(DatasetUri::new("file://tmp/out.vortex").unwrap())
            .target_is_native_vortex(true)
            .schema_known(true)
            .schema_compatible(true)
            .delete_semantics_known(true)
            .tombstone_semantics_known(true)
    }

    #[test]
    fn object_store_uri_blocks_without_signal() {
        let req = VortexWriteIntentRequest::new(DatasetUri::new("s3://bucket/out.vortex").unwrap())
            .target_is_native_vortex(true)
            .schema_known(true)
            .schema_compatible(true)
            .delete_semantics_known(true)
            .tombstone_semantics_known(true)
            .commit_protocol_available(true)
            .upstream_vortex_write_feature_enabled(true);
        let rep = VortexWriteIntentReport::from_request(req).unwrap();
        assert_eq!(
            rep.status,
            VortexWriteIntentStatus::BlockedByObjectStoreWrite
        );
    }
    #[test]
    fn object_store_blocks() {
        let rep = VortexWriteIntentReport::from_request(base().object_store_target(true)).unwrap();
        assert_eq!(
            rep.status,
            VortexWriteIntentStatus::BlockedByObjectStoreWrite
        );
    }
    #[test]
    fn missing_native_unsupported() {
        let rep = VortexWriteIntentReport::from_request(VortexWriteIntentRequest::new(
            DatasetUri::new("file://tmp/out.vortex").unwrap(),
        ))
        .unwrap();
        assert_eq!(rep.status, VortexWriteIntentStatus::Unsupported);
    }
    #[test]
    fn missing_schema_known_blocks() {
        let rep = VortexWriteIntentReport::from_request(
            VortexWriteIntentRequest::new(DatasetUri::new("file://tmp/out.vortex").unwrap())
                .target_is_native_vortex(true),
        )
        .unwrap();
        assert_eq!(rep.status, VortexWriteIntentStatus::BlockedBySchema);
    }
    #[test]
    fn missing_schema_compatible_blocks() {
        let rep = VortexWriteIntentReport::from_request(
            VortexWriteIntentRequest::new(DatasetUri::new("file://tmp/out.vortex").unwrap())
                .target_is_native_vortex(true)
                .schema_known(true),
        )
        .unwrap();
        assert_eq!(rep.status, VortexWriteIntentStatus::BlockedBySchema);
    }
    #[test]
    fn missing_delete_blocks() {
        let rep = VortexWriteIntentReport::from_request(
            VortexWriteIntentRequest::new(DatasetUri::new("file://tmp/out.vortex").unwrap())
                .target_is_native_vortex(true)
                .schema_known(true)
                .schema_compatible(true),
        )
        .unwrap();
        assert_eq!(
            rep.status,
            VortexWriteIntentStatus::BlockedByDeleteSemantics
        );
    }
    #[test]
    fn missing_tombstone_blocks() {
        let rep = VortexWriteIntentReport::from_request(
            VortexWriteIntentRequest::new(DatasetUri::new("file://tmp/out.vortex").unwrap())
                .target_is_native_vortex(true)
                .schema_known(true)
                .schema_compatible(true)
                .delete_semantics_known(true),
        )
        .unwrap();
        assert_eq!(
            rep.status,
            VortexWriteIntentStatus::BlockedByTombstoneSemantics
        );
    }
    #[test]
    fn missing_commit_blocked_by_commit_protocol() {
        let rep = VortexWriteIntentReport::from_request(base()).unwrap();
        assert_eq!(rep.status, VortexWriteIntentStatus::BlockedByCommitProtocol);
    }

    #[test]
    fn staged_output_signal_overrides_commit_available_mode() {
        let rep = VortexWriteIntentReport::from_request(
            base()
                .commit_protocol_available(true)
                .staged_output_required(true),
        )
        .unwrap();
        assert_eq!(rep.status, VortexWriteIntentStatus::StagedOutputRequired);
        assert_eq!(rep.mode, VortexWriteIntentMode::StagedOutputPlanning);
    }

    #[test]
    fn missing_upstream_feature_gate_blocks() {
        let rep =
            VortexWriteIntentReport::from_request(base().commit_protocol_available(true)).unwrap();
        assert_eq!(rep.status, VortexWriteIntentStatus::BlockedByFeatureGate);
        assert_eq!(rep.mode, VortexWriteIntentMode::ReportOnly);
    }
    #[test]
    fn all_signals_planned_but_disabled() {
        let rep = VortexWriteIntentReport::from_request(
            base()
                .commit_protocol_available(true)
                .upstream_vortex_write_feature_enabled(true),
        )
        .unwrap();
        assert_eq!(rep.status, VortexWriteIntentStatus::Planned);
        assert!(!rep.allows_write_execution());
        assert!(rep.is_side_effect_free());
        assert!(!rep.output_data_written());
        assert!(!rep.manifest_written());
        assert!(!rep.object_store_io());
        assert!(!rep.upstream_vortex_write_called());
        assert!(!rep.fallback_execution_allowed());
    }

    #[test]
    fn request_errors_force_unsupported_status() {
        let mut req = base()
            .commit_protocol_available(true)
            .upstream_vortex_write_feature_enabled(true);
        req.add_diagnostic(Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            "request prevalidation failed",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        ));
        let rep = VortexWriteIntentReport::from_request(req).unwrap();
        assert_eq!(rep.status, VortexWriteIntentStatus::Unsupported);
        assert_eq!(rep.mode, VortexWriteIntentMode::Unsupported);
        assert!(rep.has_errors());
    }
    #[test]
    fn human_text_includes_required() {
        let mut req = base();
        req.add_diagnostic(Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            "x",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        ));
        let rep = VortexWriteIntentReport::from_request(req).unwrap();
        let text = rep.to_human_text();
        assert!(text.contains("fallback execution: disabled"));
        assert!(text.contains("upstream Vortex write called: false"));
        assert!(text.contains("SL_INVALID_INPUT"));
    }
    #[test]
    fn helpers_no_io() {
        let rep = plan_vortex_write_intent(base()).unwrap();
        assert!(vortex_write_intent_is_side_effect_free(&rep));
    }
}
