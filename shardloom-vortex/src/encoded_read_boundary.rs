use std::fmt::Write as _;

use shardloom_core::{DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadBoundaryStatus {
    FeatureDisabled,
    Planned,
    BoundaryReady,
    BlockedByMissingApiSurface,
    BlockedByScanExecutionRisk,
    BlockedByDecodeRisk,
    BlockedByMaterializationRisk,
    BlockedByObjectStoreTarget,
    BlockedByWriteRisk,
    BlockedByArrowDefaultRisk,
    Unsupported,
}

impl VortexEncodedReadBoundaryStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::Planned => "planned",
            Self::BoundaryReady => "boundary_ready",
            Self::BlockedByMissingApiSurface => "blocked_by_missing_api_surface",
            Self::BlockedByScanExecutionRisk => "blocked_by_scan_execution_risk",
            Self::BlockedByDecodeRisk => "blocked_by_decode_risk",
            Self::BlockedByMaterializationRisk => "blocked_by_materialization_risk",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByWriteRisk => "blocked_by_write_risk",
            Self::BlockedByArrowDefaultRisk => "blocked_by_arrow_default_risk",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::BlockedByMissingApiSurface
                | Self::BlockedByScanExecutionRisk
                | Self::BlockedByDecodeRisk
                | Self::BlockedByMaterializationRisk
                | Self::BlockedByObjectStoreTarget
                | Self::BlockedByWriteRisk
                | Self::BlockedByArrowDefaultRisk
                | Self::Unsupported
        )
    }
    #[must_use]
    pub const fn allows_read_execution(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadBoundaryMode {
    ReportOnly,
    BoundaryPlanning,
    MetadataOnlyProbe,
    Unsupported,
}
impl VortexEncodedReadBoundaryMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::BoundaryPlanning => "boundary_planning",
            Self::MetadataOnlyProbe => "metadata_only_probe",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn reads_data(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn decodes_data(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn materializes_data(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn converts_to_arrow(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn writes_data(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn performs_object_store_io(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn calls_upstream_scan(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadBoundarySignal {
    UpstreamOpenOptionsAvailable,
    UpstreamFooterAvailable,
    UpstreamMetadataSurfaceAvailable,
    UpstreamScanSurfaceDeferred,
    LocalPathOnly,
    ObjectStoreTarget,
    DecodeRisk,
    MaterializationRisk,
    ArrowDefaultRisk,
    WriteRisk,
    FeatureGateEnabled,
}
impl VortexEncodedReadBoundarySignal {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::UpstreamOpenOptionsAvailable => "upstream_open_options_available",
            Self::UpstreamFooterAvailable => "upstream_footer_available",
            Self::UpstreamMetadataSurfaceAvailable => "upstream_metadata_surface_available",
            Self::UpstreamScanSurfaceDeferred => "upstream_scan_surface_deferred",
            Self::LocalPathOnly => "local_path_only",
            Self::ObjectStoreTarget => "object_store_target",
            Self::DecodeRisk => "decode_risk",
            Self::MaterializationRisk => "materialization_risk",
            Self::ArrowDefaultRisk => "arrow_default_risk",
            Self::WriteRisk => "write_risk",
            Self::FeatureGateEnabled => "feature_gate_enabled",
        }
    }
    #[must_use]
    pub const fn is_blocking(&self) -> bool {
        matches!(
            self,
            Self::ObjectStoreTarget
                | Self::DecodeRisk
                | Self::MaterializationRisk
                | Self::ArrowDefaultRisk
                | Self::WriteRisk
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadBoundaryEffect {
    DataRead,
    ArrayDecoded,
    ValuesMaterialized,
    ArrowConverted,
    ObjectStoreIo,
    DataWritten,
    UpstreamScanCalled,
    FallbackExecution,
}
impl VortexEncodedReadBoundaryEffect {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::DataRead => "data_read",
            Self::ArrayDecoded => "array_decoded",
            Self::ValuesMaterialized => "values_materialized",
            Self::ArrowConverted => "arrow_converted",
            Self::ObjectStoreIo => "object_store_io",
            Self::DataWritten => "data_written",
            Self::UpstreamScanCalled => "upstream_scan_called",
            Self::FallbackExecution => "fallback_execution",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexEncodedReadBoundaryRequest {
    pub target_uri: DatasetUri,
    pub signals: Vec<VortexEncodedReadBoundarySignal>,
    pub api_surface_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodedReadBoundaryRequest {
    #[must_use]
    pub fn new(target_uri: DatasetUri) -> Self {
        Self {
            target_uri,
            signals: Vec::new(),
            api_surface_summary: None,
            diagnostics: Vec::new(),
        }
    }
    pub fn add_signal(&mut self, signal: VortexEncodedReadBoundarySignal) {
        if !self.signals.contains(&signal) {
            self.signals.push(signal);
        }
    }
    fn set_signal(mut self, signal: VortexEncodedReadBoundarySignal, value: bool) -> Self {
        if value {
            self.add_signal(signal);
        } else {
            self.signals.retain(|s| *s != signal);
        }
        self
    }
    #[must_use]
    pub fn upstream_open_options_available(self, value: bool) -> Self {
        self.set_signal(
            VortexEncodedReadBoundarySignal::UpstreamOpenOptionsAvailable,
            value,
        )
    }
    #[must_use]
    pub fn upstream_footer_available(self, value: bool) -> Self {
        self.set_signal(
            VortexEncodedReadBoundarySignal::UpstreamFooterAvailable,
            value,
        )
    }
    #[must_use]
    pub fn upstream_metadata_surface_available(self, value: bool) -> Self {
        self.set_signal(
            VortexEncodedReadBoundarySignal::UpstreamMetadataSurfaceAvailable,
            value,
        )
    }
    #[must_use]
    pub fn upstream_scan_surface_deferred(self, value: bool) -> Self {
        self.set_signal(
            VortexEncodedReadBoundarySignal::UpstreamScanSurfaceDeferred,
            value,
        )
    }
    #[must_use]
    pub fn local_path_only(self, value: bool) -> Self {
        self.set_signal(VortexEncodedReadBoundarySignal::LocalPathOnly, value)
    }
    #[must_use]
    pub fn object_store_target(self, value: bool) -> Self {
        self.set_signal(VortexEncodedReadBoundarySignal::ObjectStoreTarget, value)
    }
    #[must_use]
    pub fn decode_risk(self, value: bool) -> Self {
        self.set_signal(VortexEncodedReadBoundarySignal::DecodeRisk, value)
    }
    #[must_use]
    pub fn materialization_risk(self, value: bool) -> Self {
        self.set_signal(VortexEncodedReadBoundarySignal::MaterializationRisk, value)
    }
    #[must_use]
    pub fn arrow_default_risk(self, value: bool) -> Self {
        self.set_signal(VortexEncodedReadBoundarySignal::ArrowDefaultRisk, value)
    }
    #[must_use]
    pub fn write_risk(self, value: bool) -> Self {
        self.set_signal(VortexEncodedReadBoundarySignal::WriteRisk, value)
    }
    #[must_use]
    pub fn feature_gate_enabled(self, value: bool) -> Self {
        self.set_signal(VortexEncodedReadBoundarySignal::FeatureGateEnabled, value)
    }
    #[must_use]
    pub fn with_api_surface_summary(mut self, summary: impl Into<String>) -> Self {
        self.api_surface_summary = Some(summary.into());
        self
    }
    #[must_use]
    pub fn has_signal(&self, signal: VortexEncodedReadBoundarySignal) -> bool {
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
        self.api_surface_summary
            .clone()
            .unwrap_or_else(|| "encoded read boundary request".to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexEncodedReadBoundaryReport {
    pub status: VortexEncodedReadBoundaryStatus,
    pub mode: VortexEncodedReadBoundaryMode,
    pub request: VortexEncodedReadBoundaryRequest,
    pub effects_performed: Vec<VortexEncodedReadBoundaryEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodedReadBoundaryReport {
    /// Builds a `VortexEncodedReadBoundaryReport` from a `VortexEncodedReadBoundaryRequest`.
    ///
    /// # Errors
    /// Returns an error only if `ShardLoom` cannot build a deterministic report.
    pub fn from_request(request: VortexEncodedReadBoundaryRequest) -> Result<Self> {
        let status = if request.has_signal(VortexEncodedReadBoundarySignal::ObjectStoreTarget) {
            VortexEncodedReadBoundaryStatus::BlockedByObjectStoreTarget
        } else if request.has_signal(VortexEncodedReadBoundarySignal::DecodeRisk) {
            VortexEncodedReadBoundaryStatus::BlockedByDecodeRisk
        } else if request.has_signal(VortexEncodedReadBoundarySignal::MaterializationRisk) {
            VortexEncodedReadBoundaryStatus::BlockedByMaterializationRisk
        } else if request.has_signal(VortexEncodedReadBoundarySignal::ArrowDefaultRisk) {
            VortexEncodedReadBoundaryStatus::BlockedByArrowDefaultRisk
        } else if request.has_signal(VortexEncodedReadBoundarySignal::WriteRisk) {
            VortexEncodedReadBoundaryStatus::BlockedByWriteRisk
        } else if !request.has_signal(VortexEncodedReadBoundarySignal::UpstreamOpenOptionsAvailable)
            || !request.has_signal(VortexEncodedReadBoundarySignal::UpstreamFooterAvailable)
        {
            VortexEncodedReadBoundaryStatus::BlockedByMissingApiSurface
        } else {
            VortexEncodedReadBoundaryStatus::BoundaryReady
        };
        Ok(Self {
            status,
            mode: VortexEncodedReadBoundaryMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        })
    }
    #[must_use]
    pub fn feature_disabled(request: VortexEncodedReadBoundaryRequest) -> Self {
        Self {
            status: VortexEncodedReadBoundaryStatus::FeatureDisabled,
            mode: VortexEncodedReadBoundaryMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn unsupported(
        request: VortexEncodedReadBoundaryRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let reason = reason.into();
        Self {
            status: VortexEncodedReadBoundaryStatus::Unsupported,
            mode: VortexEncodedReadBoundaryMode::Unsupported,
            request,
            effects_performed: Vec::new(),
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                feature,
                reason,
                Some("Keep encoded-read boundary in report-only contract mode.".to_string()),
            )],
        }
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
    pub fn upstream_open_options_available(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadBoundarySignal::UpstreamOpenOptionsAvailable)
    }
    #[must_use]
    pub fn upstream_footer_available(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadBoundarySignal::UpstreamFooterAvailable)
    }
    #[must_use]
    pub fn upstream_metadata_surface_available(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadBoundarySignal::UpstreamMetadataSurfaceAvailable)
    }
    #[must_use]
    pub fn upstream_scan_surface_deferred(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadBoundarySignal::UpstreamScanSurfaceDeferred)
    }
    #[must_use]
    pub fn local_path_only(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadBoundarySignal::LocalPathOnly)
    }
    #[must_use]
    pub fn object_store_target(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadBoundarySignal::ObjectStoreTarget)
    }
    #[must_use]
    pub fn decode_risk(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadBoundarySignal::DecodeRisk)
    }
    #[must_use]
    pub fn materialization_risk(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadBoundarySignal::MaterializationRisk)
    }
    #[must_use]
    pub fn arrow_default_risk(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadBoundarySignal::ArrowDefaultRisk)
    }
    #[must_use]
    pub fn write_risk(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadBoundarySignal::WriteRisk)
    }
    #[must_use]
    pub fn data_read(&self) -> bool {
        self.effects_performed
            .contains(&VortexEncodedReadBoundaryEffect::DataRead)
    }
    #[must_use]
    pub fn array_decoded(&self) -> bool {
        self.effects_performed
            .contains(&VortexEncodedReadBoundaryEffect::ArrayDecoded)
    }
    #[must_use]
    pub fn values_materialized(&self) -> bool {
        self.effects_performed
            .contains(&VortexEncodedReadBoundaryEffect::ValuesMaterialized)
    }
    #[must_use]
    pub fn arrow_converted(&self) -> bool {
        self.effects_performed
            .contains(&VortexEncodedReadBoundaryEffect::ArrowConverted)
    }
    #[must_use]
    pub fn object_store_io(&self) -> bool {
        self.effects_performed
            .contains(&VortexEncodedReadBoundaryEffect::ObjectStoreIo)
    }
    #[must_use]
    pub fn data_written(&self) -> bool {
        self.effects_performed
            .contains(&VortexEncodedReadBoundaryEffect::DataWritten)
    }
    #[must_use]
    pub fn upstream_scan_called(&self) -> bool {
        self.effects_performed
            .contains(&VortexEncodedReadBoundaryEffect::UpstreamScanCalled)
    }
    #[must_use]
    pub fn fallback_execution_allowed(&self) -> bool {
        self.effects_performed
            .contains(&VortexEncodedReadBoundaryEffect::FallbackExecution)
    }
    #[must_use]
    pub const fn allows_read_execution(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty() && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "encoded read boundary status: {}",
            self.status.as_str()
        );
        let _ = writeln!(out, "mode: {}", self.mode.as_str());
        let _ = writeln!(out, "target URI: {}", self.request.target_uri.as_str());
        let _ = writeln!(
            out,
            "open options surface available: {}",
            self.upstream_open_options_available()
        );
        let _ = writeln!(
            out,
            "footer surface available: {}",
            self.upstream_footer_available()
        );
        let _ = writeln!(
            out,
            "metadata surface available: {}",
            self.upstream_metadata_surface_available()
        );
        let _ = writeln!(
            out,
            "scan surface deferred: {}",
            self.upstream_scan_surface_deferred()
        );
        let _ = writeln!(out, "local path only: {}", self.local_path_only());
        let _ = writeln!(out, "object-store target: {}", self.object_store_target());
        let _ = writeln!(out, "decode risk: {}", self.decode_risk());
        let _ = writeln!(out, "materialization risk: {}", self.materialization_risk());
        let _ = writeln!(out, "Arrow default risk: {}", self.arrow_default_risk());
        let _ = writeln!(out, "write risk: {}", self.write_risk());
        let _ = writeln!(out, "data read: {}", self.data_read());
        let _ = writeln!(out, "array decoded: {}", self.array_decoded());
        let _ = writeln!(out, "values materialized: {}", self.values_materialized());
        let _ = writeln!(out, "Arrow converted: {}", self.arrow_converted());
        let _ = writeln!(out, "object-store IO: {}", self.object_store_io());
        let _ = writeln!(out, "data written: {}", self.data_written());
        let _ = writeln!(out, "upstream scan called: {}", self.upstream_scan_called());
        let _ = writeln!(out, "fallback execution disabled");
        if !self.diagnostics.is_empty() || !self.request.diagnostics.is_empty() {
            let _ = writeln!(out, "diagnostics:");
            for d in self
                .request
                .diagnostics
                .iter()
                .chain(self.diagnostics.iter())
            {
                let _ = writeln!(out, "- [{}] {}", d.severity.as_str(), d.message);
            }
        }
        out
    }
}

/// Plans a report-only encoded-read boundary contract.
///
/// # Errors
/// Returns an error if `VortexEncodedReadBoundaryReport` cannot be built.
pub fn plan_vortex_encoded_read_boundary(
    request: VortexEncodedReadBoundaryRequest,
) -> Result<VortexEncodedReadBoundaryReport> {
    VortexEncodedReadBoundaryReport::from_request(request)
}

#[must_use]
pub fn vortex_encoded_read_boundary_is_side_effect_free(
    report: &VortexEncodedReadBoundaryReport,
) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uri() -> DatasetUri {
        DatasetUri::new("file:///tmp/x.vortex").expect("valid")
    }

    #[test]
    fn status_ready_disallows_execution() {
        assert!(!VortexEncodedReadBoundaryStatus::BoundaryReady.allows_read_execution());
    }
    #[test]
    fn status_blocked_missing_api_is_error() {
        assert!(VortexEncodedReadBoundaryStatus::BlockedByMissingApiSurface.is_error());
    }
    #[test]
    fn mode_report_only_reads_false() {
        assert!(!VortexEncodedReadBoundaryMode::ReportOnly.reads_data());
    }
    #[test]
    fn mode_metadata_probe_reads_false_and_scan_false() {
        let m = VortexEncodedReadBoundaryMode::MetadataOnlyProbe;
        assert!(!m.reads_data());
        assert!(!m.calls_upstream_scan());
    }
    #[test]
    fn request_signal_add_remove_dedup() {
        let mut r = VortexEncodedReadBoundaryRequest::new(uri());
        r.add_signal(VortexEncodedReadBoundarySignal::LocalPathOnly);
        r.add_signal(VortexEncodedReadBoundarySignal::LocalPathOnly);
        assert_eq!(r.signals.len(), 1);
        let r = r.local_path_only(false);
        assert!(!r.has_signal(VortexEncodedReadBoundarySignal::LocalPathOnly));
    }
    #[test]
    fn from_request_object_store_blocks() {
        let r = VortexEncodedReadBoundaryRequest::new(uri())
            .object_store_target(true)
            .upstream_open_options_available(true)
            .upstream_footer_available(true);
        let out = VortexEncodedReadBoundaryReport::from_request(r).expect("ok");
        assert_eq!(
            out.status,
            VortexEncodedReadBoundaryStatus::BlockedByObjectStoreTarget
        );
    }
    #[test]
    fn from_request_decode_blocks() {
        let out = VortexEncodedReadBoundaryReport::from_request(
            VortexEncodedReadBoundaryRequest::new(uri())
                .decode_risk(true)
                .upstream_open_options_available(true)
                .upstream_footer_available(true),
        )
        .expect("ok");
        assert_eq!(
            out.status,
            VortexEncodedReadBoundaryStatus::BlockedByDecodeRisk
        );
    }
    #[test]
    fn from_request_materialization_blocks() {
        let out = VortexEncodedReadBoundaryReport::from_request(
            VortexEncodedReadBoundaryRequest::new(uri())
                .materialization_risk(true)
                .upstream_open_options_available(true)
                .upstream_footer_available(true),
        )
        .expect("ok");
        assert_eq!(
            out.status,
            VortexEncodedReadBoundaryStatus::BlockedByMaterializationRisk
        );
    }
    #[test]
    fn from_request_arrow_blocks() {
        let out = VortexEncodedReadBoundaryReport::from_request(
            VortexEncodedReadBoundaryRequest::new(uri())
                .arrow_default_risk(true)
                .upstream_open_options_available(true)
                .upstream_footer_available(true),
        )
        .expect("ok");
        assert_eq!(
            out.status,
            VortexEncodedReadBoundaryStatus::BlockedByArrowDefaultRisk
        );
    }
    #[test]
    fn from_request_write_blocks() {
        let out = VortexEncodedReadBoundaryReport::from_request(
            VortexEncodedReadBoundaryRequest::new(uri())
                .write_risk(true)
                .upstream_open_options_available(true)
                .upstream_footer_available(true),
        )
        .expect("ok");
        assert_eq!(
            out.status,
            VortexEncodedReadBoundaryStatus::BlockedByWriteRisk
        );
    }
    #[test]
    fn from_request_missing_open_blocks() {
        let out = VortexEncodedReadBoundaryReport::from_request(
            VortexEncodedReadBoundaryRequest::new(uri()).upstream_footer_available(true),
        )
        .expect("ok");
        assert_eq!(
            out.status,
            VortexEncodedReadBoundaryStatus::BlockedByMissingApiSurface
        );
    }
    #[test]
    fn from_request_missing_footer_blocks() {
        let out = VortexEncodedReadBoundaryReport::from_request(
            VortexEncodedReadBoundaryRequest::new(uri()).upstream_open_options_available(true),
        )
        .expect("ok");
        assert_eq!(
            out.status,
            VortexEncodedReadBoundaryStatus::BlockedByMissingApiSurface
        );
    }
    #[test]
    fn from_request_ready_still_disallows_execution() {
        let out = VortexEncodedReadBoundaryReport::from_request(
            VortexEncodedReadBoundaryRequest::new(uri())
                .upstream_open_options_available(true)
                .upstream_footer_available(true)
                .upstream_metadata_surface_available(true)
                .upstream_scan_surface_deferred(true),
        )
        .expect("ok");
        assert_eq!(out.status, VortexEncodedReadBoundaryStatus::BoundaryReady);
        assert!(!out.allows_read_execution());
    }
    #[test]
    fn report_effect_flags_false_and_side_effect_free() {
        let out = VortexEncodedReadBoundaryReport::from_request(
            VortexEncodedReadBoundaryRequest::new(uri())
                .upstream_open_options_available(true)
                .upstream_footer_available(true),
        )
        .expect("ok");
        assert!(!out.data_read());
        assert!(!out.array_decoded());
        assert!(!out.values_materialized());
        assert!(!out.arrow_converted());
        assert!(!out.object_store_io());
        assert!(!out.data_written());
        assert!(!out.upstream_scan_called());
        assert!(!out.fallback_execution_allowed());
        assert!(out.is_side_effect_free());
    }
    #[test]
    fn text_contains_required_lines_and_diags() {
        let mut req = VortexEncodedReadBoundaryRequest::new(uri())
            .upstream_open_options_available(true)
            .upstream_footer_available(true);
        req.add_diagnostic(Diagnostic::no_fallback_execution("no fallback"));
        let mut out = VortexEncodedReadBoundaryReport::from_request(req).expect("ok");
        out.add_diagnostic(Diagnostic::not_implemented(
            "encoded_read_boundary",
            "deferred",
            "stay report-only",
        ));
        let text = out.to_human_text();
        assert!(text.contains("fallback execution disabled"));
        assert!(text.contains("data read: false"));
        assert!(text.contains("diagnostics:"));
    }
    #[test]
    fn plan_helper_no_io() {
        let out = plan_vortex_encoded_read_boundary(
            VortexEncodedReadBoundaryRequest::new(uri())
                .upstream_open_options_available(true)
                .upstream_footer_available(true),
        )
        .expect("ok");
        assert_eq!(out.mode, VortexEncodedReadBoundaryMode::ReportOnly);
    }
}
