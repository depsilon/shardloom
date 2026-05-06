use std::fmt::Write as _;

use shardloom_core::{DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, Result};

use crate::{VortexEncodedReadFixtureRef, VortexEncodedReadFixtureReport};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadMetadataProbeStatus {
    FeatureDisabled,
    Planned,
    MetadataProbeReady,
    MetadataProbeCompleted,
    BlockedByFixture,
    BlockedByMissingFixtureRef,
    BlockedByObjectStoreTarget,
    BlockedByMissingLocalFile,
    BlockedByUnsupportedApiSurface,
    BlockedByScanExecutionRisk,
    BlockedByDecodeRisk,
    BlockedByMaterializationRisk,
    BlockedByArrowDefaultRisk,
    BlockedByWriteRisk,
    Unsupported,
}
impl VortexEncodedReadMetadataProbeStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::Planned => "planned",
            Self::MetadataProbeReady => "metadata_probe_ready",
            Self::MetadataProbeCompleted => "metadata_probe_completed",
            Self::BlockedByFixture => "blocked_by_fixture",
            Self::BlockedByMissingFixtureRef => "blocked_by_missing_fixture_ref",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByMissingLocalFile => "blocked_by_missing_local_file",
            Self::BlockedByUnsupportedApiSurface => "blocked_by_unsupported_api_surface",
            Self::BlockedByScanExecutionRisk => "blocked_by_scan_execution_risk",
            Self::BlockedByDecodeRisk => "blocked_by_decode_risk",
            Self::BlockedByMaterializationRisk => "blocked_by_materialization_risk",
            Self::BlockedByArrowDefaultRisk => "blocked_by_arrow_default_risk",
            Self::BlockedByWriteRisk => "blocked_by_write_risk",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        !matches!(
            self,
            Self::FeatureDisabled
                | Self::Planned
                | Self::MetadataProbeReady
                | Self::MetadataProbeCompleted
        )
    }
    #[must_use]
    pub const fn metadata_probe_completed(&self) -> bool {
        matches!(self, Self::MetadataProbeCompleted)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadMetadataProbeMode {
    ReportOnly,
    LocalMetadataFooterProbe,
    Unsupported,
}
impl VortexEncodedReadMetadataProbeMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::LocalMetadataFooterProbe => "local_metadata_footer_probe",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn opens_metadata(&self) -> bool {
        matches!(self, Self::LocalMetadataFooterProbe)
    }
    #[must_use]
    pub const fn inspects_footer(&self) -> bool {
        matches!(self, Self::LocalMetadataFooterProbe)
    }
    #[must_use]
    pub const fn reads_encoded_data(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn reads_rows(&self) -> bool {
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
    pub const fn performs_object_store_io(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn calls_upstream_scan(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn writes_data(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadMetadataProbeSignal {
    FixtureReady,
    FixtureBlocked,
    FixtureRefProvided,
    LocalPathOnly,
    ObjectStoreTarget,
    ScanExecutionRisk,
    DecodeRisk,
    MaterializationRisk,
    ArrowDefaultRisk,
    WriteRisk,
    FeatureGateEnabled,
}
impl VortexEncodedReadMetadataProbeSignal {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FixtureReady => "fixture_ready",
            Self::FixtureBlocked => "fixture_blocked",
            Self::FixtureRefProvided => "fixture_ref_provided",
            Self::LocalPathOnly => "local_path_only",
            Self::ObjectStoreTarget => "object_store_target",
            Self::ScanExecutionRisk => "scan_execution_risk",
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
            Self::FixtureBlocked
                | Self::ObjectStoreTarget
                | Self::ScanExecutionRisk
                | Self::DecodeRisk
                | Self::MaterializationRisk
                | Self::ArrowDefaultRisk
                | Self::WriteRisk
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadMetadataProbeEffect {
    MetadataOpened,
    FooterInspected,
    EncodedDataRead,
    RowRead,
    ArrayDecoded,
    ValuesMaterialized,
    ArrowConverted,
    ObjectStoreIo,
    DataWritten,
    UpstreamScanCalled,
    FallbackExecution,
}
impl VortexEncodedReadMetadataProbeEffect {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOpened => "metadata_opened",
            Self::FooterInspected => "footer_inspected",
            Self::EncodedDataRead => "encoded_data_read",
            Self::RowRead => "row_read",
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
pub struct VortexEncodedReadMetadataProbeRequest {
    pub target_uri: DatasetUri,
    pub fixture_ref: VortexEncodedReadFixtureRef,
    pub signals: Vec<VortexEncodedReadMetadataProbeSignal>,
    pub fixture_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodedReadMetadataProbeRequest {
    #[must_use]
    pub fn new(target_uri: DatasetUri, fixture_ref: VortexEncodedReadFixtureRef) -> Self {
        Self {
            target_uri,
            fixture_ref,
            signals: Vec::new(),
            fixture_summary: None,
            diagnostics: Vec::new(),
        }
    }
    pub fn add_signal(&mut self, s: VortexEncodedReadMetadataProbeSignal) {
        if !self.signals.contains(&s) {
            self.signals.push(s);
        }
    }
    fn set_signal(mut self, s: VortexEncodedReadMetadataProbeSignal, v: bool) -> Self {
        if v {
            self.add_signal(s);
        } else {
            self.signals.retain(|x| *x != s);
        }
        self
    }
    #[must_use]
    pub fn fixture_ready(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadMetadataProbeSignal::FixtureReady, v)
    }
    #[must_use]
    pub fn fixture_blocked(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadMetadataProbeSignal::FixtureBlocked, v)
    }
    #[must_use]
    pub fn fixture_ref_provided(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadMetadataProbeSignal::FixtureRefProvided, v)
    }
    #[must_use]
    pub fn local_path_only(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadMetadataProbeSignal::LocalPathOnly, v)
    }
    #[must_use]
    pub fn object_store_target(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadMetadataProbeSignal::ObjectStoreTarget, v)
    }
    #[must_use]
    pub fn scan_execution_risk(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadMetadataProbeSignal::ScanExecutionRisk, v)
    }
    #[must_use]
    pub fn decode_risk(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadMetadataProbeSignal::DecodeRisk, v)
    }
    #[must_use]
    pub fn materialization_risk(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadMetadataProbeSignal::MaterializationRisk, v)
    }
    #[must_use]
    pub fn arrow_default_risk(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadMetadataProbeSignal::ArrowDefaultRisk, v)
    }
    #[must_use]
    pub fn write_risk(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadMetadataProbeSignal::WriteRisk, v)
    }
    #[must_use]
    pub fn feature_gate_enabled(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadMetadataProbeSignal::FeatureGateEnabled, v)
    }
    #[must_use]
    pub fn with_fixture_summary(mut self, s: impl Into<String>) -> Self {
        self.fixture_summary = Some(s.into());
        self
    }
    #[must_use]
    pub fn has_signal(&self, s: VortexEncodedReadMetadataProbeSignal) -> bool {
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
        self.fixture_summary
            .clone()
            .unwrap_or_else(|| self.fixture_ref.summary())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexEncodedReadMetadataProbeReport {
    pub status: VortexEncodedReadMetadataProbeStatus,
    pub mode: VortexEncodedReadMetadataProbeMode,
    pub request: VortexEncodedReadMetadataProbeRequest,
    pub effects_performed: Vec<VortexEncodedReadMetadataProbeEffect>,
    pub metadata_summary: Option<String>,
    pub footer_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodedReadMetadataProbeReport {
    /// # Errors
    /// Returns an error if report construction fails.
    pub fn from_request(request: VortexEncodedReadMetadataProbeRequest) -> Result<Self> {
        let mut r = Self::feature_disabled(request);
        if r.object_store_target() || r.request.fixture_ref.is_object_store_like() {
            r.status = VortexEncodedReadMetadataProbeStatus::BlockedByObjectStoreTarget;
            return Ok(r);
        }
        if r.request
            .has_signal(VortexEncodedReadMetadataProbeSignal::FixtureBlocked)
            || !r.fixture_ready()
        {
            r.status = VortexEncodedReadMetadataProbeStatus::BlockedByFixture;
            return Ok(r);
        }
        if !r.fixture_ref_provided() {
            r.status = VortexEncodedReadMetadataProbeStatus::BlockedByMissingFixtureRef;
            return Ok(r);
        }
        if r.scan_execution_risk() {
            r.status = VortexEncodedReadMetadataProbeStatus::BlockedByScanExecutionRisk;
            return Ok(r);
        }
        if r.decode_risk() {
            r.status = VortexEncodedReadMetadataProbeStatus::BlockedByDecodeRisk;
            return Ok(r);
        }
        if r.materialization_risk() {
            r.status = VortexEncodedReadMetadataProbeStatus::BlockedByMaterializationRisk;
            return Ok(r);
        }
        if r.arrow_default_risk() {
            r.status = VortexEncodedReadMetadataProbeStatus::BlockedByArrowDefaultRisk;
            return Ok(r);
        }
        if r.write_risk() {
            r.status = VortexEncodedReadMetadataProbeStatus::BlockedByWriteRisk;
            return Ok(r);
        }
        if !r
            .request
            .has_signal(VortexEncodedReadMetadataProbeSignal::FeatureGateEnabled)
        {
            return Ok(r);
        }
        #[cfg(feature = "vortex-file-io")]
        {
            let local_path = r.request.fixture_ref.as_str();
            let path = std::path::Path::new(local_path);
            if !path.exists() {
                r.status = VortexEncodedReadMetadataProbeStatus::BlockedByMissingLocalFile;
                r.add_diagnostic(Diagnostic::invalid_input(
                    "vortex_encoded_read_metadata_probe",
                    format!("local fixture path does not exist: {local_path}"),
                    "provide an existing local fixture path for metadata/footer probe",
                ));
                return Ok(r);
            }
            r.status = VortexEncodedReadMetadataProbeStatus::BlockedByUnsupportedApiSurface;
            r.add_diagnostic(Diagnostic::not_implemented(
                "vortex_encoded_read_metadata_probe",
                "validated metadata/footer-only upstream `Vortex` API invocation is not wired yet; probe remains blocked",
                "wire `VortexOpenOptions` + `VortexFile::footer` metadata-only open in a follow-up while preserving no-scan/no-decode invariants",
            ));
            Ok(r)
        }
        #[cfg(not(feature = "vortex-file-io"))]
        {
            r.status = VortexEncodedReadMetadataProbeStatus::BlockedByUnsupportedApiSurface;
            r.add_diagnostic(Diagnostic::not_implemented(
                "vortex_encoded_read_metadata_probe",
                "safe metadata/footer probe API is not enabled or not confirmed; default report path performs no local filesystem inspection",
                "enable a validated metadata-only feature gate in a future phase; local file checks remain deferred",
            ));
            Ok(r)
        }
    }
    #[must_use]
    pub fn feature_disabled(request: VortexEncodedReadMetadataProbeRequest) -> Self {
        Self {
            status: VortexEncodedReadMetadataProbeStatus::FeatureDisabled,
            mode: VortexEncodedReadMetadataProbeMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            metadata_summary: None,
            footer_summary: None,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn unsupported(
        request: VortexEncodedReadMetadataProbeRequest,
        feature: &str,
        reason: &str,
    ) -> Self {
        let mut r = Self {
            status: VortexEncodedReadMetadataProbeStatus::Unsupported,
            mode: VortexEncodedReadMetadataProbeMode::Unsupported,
            request,
            effects_performed: Vec::new(),
            metadata_summary: None,
            footer_summary: None,
            diagnostics: Vec::new(),
        };
        r.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            format!("Unsupported metadata probe: {reason}"),
            Some("Keep metadata probe in report-only mode with fallback disabled.".to_string()),
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
    pub fn fixture_ready(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadMetadataProbeSignal::FixtureReady)
    }
    #[must_use]
    pub fn fixture_ref_provided(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadMetadataProbeSignal::FixtureRefProvided)
    }
    #[must_use]
    pub fn local_path_only(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadMetadataProbeSignal::LocalPathOnly)
    }
    #[must_use]
    pub fn object_store_target(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadMetadataProbeSignal::ObjectStoreTarget)
            || self.request.fixture_ref.is_object_store_like()
    }
    #[must_use]
    pub fn scan_execution_risk(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadMetadataProbeSignal::ScanExecutionRisk)
    }
    #[must_use]
    pub fn decode_risk(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadMetadataProbeSignal::DecodeRisk)
    }
    #[must_use]
    pub fn materialization_risk(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadMetadataProbeSignal::MaterializationRisk)
    }
    #[must_use]
    pub fn arrow_default_risk(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadMetadataProbeSignal::ArrowDefaultRisk)
    }
    #[must_use]
    pub fn write_risk(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadMetadataProbeSignal::WriteRisk)
    }
    #[must_use]
    pub fn metadata_opened(&self) -> bool {
        self.effects_performed
            .contains(&VortexEncodedReadMetadataProbeEffect::MetadataOpened)
    }
    #[must_use]
    pub fn footer_inspected(&self) -> bool {
        self.effects_performed
            .contains(&VortexEncodedReadMetadataProbeEffect::FooterInspected)
    }
    #[must_use]
    pub const fn encoded_data_read(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn row_read(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn array_decoded(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn values_materialized(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn arrow_converted(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn object_store_io(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn data_written(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn upstream_scan_called(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn metadata_probe_completed(&self) -> bool {
        matches!(
            self.status,
            VortexEncodedReadMetadataProbeStatus::MetadataProbeCompleted
        )
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut o = String::new();
        let _ = writeln!(o, "status: {}", self.status.as_str());
        let _ = writeln!(o, "mode: {}", self.mode.as_str());
        let _ = writeln!(o, "object-store target: {}", self.object_store_target());
        let _ = writeln!(o, "metadata opened: {}", self.metadata_opened());
        let _ = writeln!(o, "footer inspected: {}", self.footer_inspected());
        let _ = writeln!(
            o,
            "encoded data read: false\nrow read: false\narray decoded: false\nvalues materialized: false\narrow converted: false\nobject-store IO: false\nupstream scan called: false\ndata written: false\nfallback execution allowed: false"
        );
        o
    }
}
/// # Errors
/// Returns an error if `VortexEncodedReadMetadataProbeReport` cannot be built.
pub fn probe_vortex_encoded_read_metadata(
    request: VortexEncodedReadMetadataProbeRequest,
) -> Result<VortexEncodedReadMetadataProbeReport> {
    VortexEncodedReadMetadataProbeReport::from_request(request)
}
#[must_use]
pub fn vortex_encoded_read_metadata_probe_is_side_effect_free(
    report: &VortexEncodedReadMetadataProbeReport,
) -> bool {
    report.is_side_effect_free()
}
#[must_use]
pub fn encoded_read_metadata_probe_request_from_fixture_report(
    target_uri: DatasetUri,
    fixture_ref: VortexEncodedReadFixtureRef,
    fixture: &VortexEncodedReadFixtureReport,
) -> VortexEncodedReadMetadataProbeRequest {
    let mut r = VortexEncodedReadMetadataProbeRequest::new(target_uri, fixture_ref)
        .with_fixture_summary(fixture.to_human_text());
    if matches!(
        fixture.status,
        crate::VortexEncodedReadFixtureStatus::FixtureReady
    ) && !fixture.has_errors()
    {
        r.add_signal(VortexEncodedReadMetadataProbeSignal::FixtureReady);
    }
    if fixture.has_errors() {
        r.add_signal(VortexEncodedReadMetadataProbeSignal::FixtureBlocked);
    }
    r.add_signal(VortexEncodedReadMetadataProbeSignal::FixtureRefProvided);
    if fixture.local_path_only() {
        r.add_signal(VortexEncodedReadMetadataProbeSignal::LocalPathOnly);
    }
    if fixture.object_store_target() {
        r.add_signal(VortexEncodedReadMetadataProbeSignal::ObjectStoreTarget);
    }
    if fixture.scan_execution_risk() {
        r.add_signal(VortexEncodedReadMetadataProbeSignal::ScanExecutionRisk);
    }
    if fixture.decode_risk() {
        r.add_signal(VortexEncodedReadMetadataProbeSignal::DecodeRisk);
    }
    if fixture.materialization_risk() {
        r.add_signal(VortexEncodedReadMetadataProbeSignal::MaterializationRisk);
    }
    if fixture.arrow_default_risk() {
        r.add_signal(VortexEncodedReadMetadataProbeSignal::ArrowDefaultRisk);
    }
    if fixture.write_risk() {
        r.add_signal(VortexEncodedReadMetadataProbeSignal::WriteRisk);
    }
    if fixture
        .request
        .has_signal(crate::VortexEncodedReadFixtureSignal::FeatureGateEnabled)
    {
        r.add_signal(VortexEncodedReadMetadataProbeSignal::FeatureGateEnabled);
    }
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local_ready_request(path: &str) -> VortexEncodedReadMetadataProbeRequest {
        VortexEncodedReadMetadataProbeRequest::new(
            DatasetUri::new("file:///tmp/data.vortex").expect("valid dataset uri"),
            VortexEncodedReadFixtureRef::new(path).expect("valid fixture ref"),
        )
        .fixture_ready(true)
        .fixture_ref_provided(true)
        .local_path_only(true)
        .feature_gate_enabled(true)
    }

    #[cfg(not(feature = "vortex-file-io"))]
    #[test]
    fn local_fixture_with_feature_gate_is_deferred_without_filesystem_probe() {
        let report = VortexEncodedReadMetadataProbeReport::from_request(local_ready_request(
            "/definitely/not/a/real/path.vortex",
        ))
        .expect("report builds");
        assert_eq!(
            report.status,
            VortexEncodedReadMetadataProbeStatus::BlockedByUnsupportedApiSurface
        );
        assert!(report.local_path_only());
        assert!(report.fixture_ref_provided());
        assert!(!report.metadata_opened());
        assert!(!report.footer_inspected());
        assert!(!report.encoded_data_read());
        assert!(!report.row_read());
        assert!(!report.array_decoded());
        assert!(!report.values_materialized());
        assert!(!report.arrow_converted());
        assert!(!report.object_store_io());
        assert!(!report.upstream_scan_called());
        assert!(!report.fallback_execution_allowed());
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn object_store_fixture_ref_without_signal_is_consistent() {
        let request = VortexEncodedReadMetadataProbeRequest::new(
            DatasetUri::new("file:///tmp/data.vortex").expect("valid dataset uri"),
            VortexEncodedReadFixtureRef::new("s3://bucket/data.vortex").expect("valid fixture ref"),
        )
        .fixture_ready(true)
        .fixture_ref_provided(true)
        .feature_gate_enabled(true);
        let report =
            VortexEncodedReadMetadataProbeReport::from_request(request).expect("report builds");
        assert_eq!(
            report.status,
            VortexEncodedReadMetadataProbeStatus::BlockedByObjectStoreTarget
        );
        assert!(report.object_store_target());
        assert!(report.to_human_text().contains("object-store target: true"));
        assert!(!report.metadata_opened());
        assert!(!report.footer_inspected());
        assert!(!report.encoded_data_read());
        assert!(!report.row_read());
        assert!(!report.array_decoded());
        assert!(!report.values_materialized());
        assert!(!report.arrow_converted());
        assert!(!report.object_store_io());
        assert!(!report.data_written());
        assert!(!report.upstream_scan_called());
        assert!(!report.fallback_execution_allowed());
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn object_store_fixture_ref_remains_blocked_with_object_store_target_signal() {
        let request = VortexEncodedReadMetadataProbeRequest::new(
            DatasetUri::new("file:///tmp/data.vortex").expect("valid dataset uri"),
            VortexEncodedReadFixtureRef::new("s3://bucket/data.vortex").expect("valid fixture ref"),
        )
        .fixture_ready(true)
        .fixture_ref_provided(true)
        .object_store_target(true)
        .feature_gate_enabled(true);
        let report =
            VortexEncodedReadMetadataProbeReport::from_request(request).expect("report builds");
        assert_eq!(
            report.status,
            VortexEncodedReadMetadataProbeStatus::BlockedByObjectStoreTarget
        );
        assert!(report.object_store_target());
        assert!(report.is_side_effect_free());
    }

    #[cfg(feature = "vortex-file-io")]
    #[test]
    fn missing_local_fixture_path_blocks_with_missing_local_file() {
        let report = VortexEncodedReadMetadataProbeReport::from_request(local_ready_request(
            "/definitely/not/a/real/path.vortex",
        ))
        .expect("report builds");
        assert_eq!(
            report.status,
            VortexEncodedReadMetadataProbeStatus::BlockedByMissingLocalFile
        );
        assert!(!report.metadata_opened());
        assert!(!report.footer_inspected());
        assert!(!report.encoded_data_read());
        assert!(!report.row_read());
        assert!(!report.array_decoded());
        assert!(!report.values_materialized());
        assert!(!report.arrow_converted());
        assert!(!report.object_store_io());
        assert!(!report.data_written());
        assert!(!report.upstream_scan_called());
        assert!(!report.fallback_execution_allowed());
    }
}
