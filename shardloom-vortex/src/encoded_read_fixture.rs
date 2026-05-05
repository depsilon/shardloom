use std::fmt::Write as _;

use shardloom_core::{DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadFixtureStatus {
    Planned,
    FixtureReady,
    BlockedByBoundary,
    BlockedByMissingFixtureRef,
    BlockedByObjectStoreTarget,
    BlockedByScanExecutionRisk,
    BlockedByDecodeRisk,
    BlockedByMaterializationRisk,
    BlockedByArrowDefaultRisk,
    BlockedByWriteRisk,
    BlockedByFeatureGate,
    Unsupported,
}
impl VortexEncodedReadFixtureStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::FixtureReady => "fixture_ready",
            Self::BlockedByBoundary => "blocked_by_boundary",
            Self::BlockedByMissingFixtureRef => "blocked_by_missing_fixture_ref",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByScanExecutionRisk => "blocked_by_scan_execution_risk",
            Self::BlockedByDecodeRisk => "blocked_by_decode_risk",
            Self::BlockedByMaterializationRisk => "blocked_by_materialization_risk",
            Self::BlockedByArrowDefaultRisk => "blocked_by_arrow_default_risk",
            Self::BlockedByWriteRisk => "blocked_by_write_risk",
            Self::BlockedByFeatureGate => "blocked_by_feature_gate",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        !matches!(self, Self::Planned | Self::FixtureReady)
    }
    #[must_use]
    pub const fn allows_fixture_probe(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedReadFixtureMode {
    ReportOnly,
    FixturePlanning,
    Unsupported,
}
impl VortexEncodedReadFixtureMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::FixturePlanning => "fixture_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn reads_data(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn opens_metadata(&self) -> bool {
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
pub enum VortexEncodedReadFixtureSignal {
    BoundaryReady,
    BoundaryBlocked,
    FixtureRefProvided,
    FixtureRefMissing,
    LocalPathOnly,
    ObjectStoreTarget,
    ScanExecutionRisk,
    DecodeRisk,
    MaterializationRisk,
    ArrowDefaultRisk,
    WriteRisk,
    FeatureGateEnabled,
}
impl VortexEncodedReadFixtureSignal {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::BoundaryReady => "boundary_ready",
            Self::BoundaryBlocked => "boundary_blocked",
            Self::FixtureRefProvided => "fixture_ref_provided",
            Self::FixtureRefMissing => "fixture_ref_missing",
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
            Self::BoundaryBlocked
                | Self::FixtureRefMissing
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
pub enum VortexEncodedReadFixtureEffect {
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
impl VortexEncodedReadFixtureEffect {
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
pub struct VortexEncodedReadFixtureRef(String);
impl VortexEncodedReadFixtureRef {
    /// # Errors
    /// Returns an error if the `Vortex` fixture reference is empty or whitespace.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(shardloom_core::ShardLoomError::InvalidOperation(
                "Fixture reference cannot be empty.".to_string(),
            ));
        }
        Ok(Self(value))
    }
    /// # Errors
    /// Returns an error if the `DatasetUri` cannot be converted into a non-empty fixture reference.
    pub fn from_dataset_uri(uri: &DatasetUri) -> Result<Self> {
        Self::new(uri.as_str())
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
    #[must_use]
    pub fn is_object_store_like(&self) -> bool {
        let s = self.0.to_ascii_lowercase();
        s.starts_with("s3://")
            || s.starts_with("gs://")
            || s.starts_with("abfs://")
            || s.starts_with("azure://")
            || s.starts_with("http://")
            || s.starts_with("https://")
    }
    #[must_use]
    pub fn summary(&self) -> String {
        if self.is_object_store_like() {
            format!("{} (object-store-like)", self.0)
        } else {
            self.0.clone()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexEncodedReadFixtureRequest {
    pub target_uri: DatasetUri,
    pub fixture_ref: Option<VortexEncodedReadFixtureRef>,
    pub signals: Vec<VortexEncodedReadFixtureSignal>,
    pub boundary_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodedReadFixtureRequest {
    #[must_use]
    pub fn new(target_uri: DatasetUri) -> Self {
        Self {
            target_uri,
            fixture_ref: None,
            signals: Vec::new(),
            boundary_summary: None,
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn with_fixture_ref(
        target_uri: DatasetUri,
        fixture_ref: VortexEncodedReadFixtureRef,
    ) -> Self {
        Self {
            target_uri,
            fixture_ref: Some(fixture_ref),
            signals: Vec::new(),
            boundary_summary: None,
            diagnostics: Vec::new(),
        }
    }
    pub fn add_signal(&mut self, s: VortexEncodedReadFixtureSignal) {
        if !self.signals.contains(&s) {
            self.signals.push(s);
        }
    }
    fn set_signal(mut self, s: VortexEncodedReadFixtureSignal, v: bool) -> Self {
        if v {
            self.add_signal(s);
        } else {
            self.signals.retain(|x| x != &s);
        }
        self
    }
    #[must_use]
    pub fn boundary_ready(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadFixtureSignal::BoundaryReady, v)
    }
    #[must_use]
    pub fn boundary_blocked(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadFixtureSignal::BoundaryBlocked, v)
    }
    #[must_use]
    pub fn fixture_ref_provided(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadFixtureSignal::FixtureRefProvided, v)
    }
    #[must_use]
    pub fn fixture_ref_missing(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadFixtureSignal::FixtureRefMissing, v)
    }
    #[must_use]
    pub fn local_path_only(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadFixtureSignal::LocalPathOnly, v)
    }
    #[must_use]
    pub fn object_store_target(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadFixtureSignal::ObjectStoreTarget, v)
    }
    #[must_use]
    pub fn scan_execution_risk(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadFixtureSignal::ScanExecutionRisk, v)
    }
    #[must_use]
    pub fn decode_risk(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadFixtureSignal::DecodeRisk, v)
    }
    #[must_use]
    pub fn materialization_risk(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadFixtureSignal::MaterializationRisk, v)
    }
    #[must_use]
    pub fn arrow_default_risk(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadFixtureSignal::ArrowDefaultRisk, v)
    }
    #[must_use]
    pub fn write_risk(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadFixtureSignal::WriteRisk, v)
    }
    #[must_use]
    pub fn feature_gate_enabled(self, v: bool) -> Self {
        self.set_signal(VortexEncodedReadFixtureSignal::FeatureGateEnabled, v)
    }
    #[must_use]
    pub fn with_boundary_summary(mut self, s: impl Into<String>) -> Self {
        self.boundary_summary = Some(s.into());
        self
    }
    #[must_use]
    pub fn has_signal(&self, s: VortexEncodedReadFixtureSignal) -> bool {
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
        self.boundary_summary
            .clone()
            .unwrap_or_else(|| "encoded read fixture request".to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexEncodedReadFixtureReport {
    pub status: VortexEncodedReadFixtureStatus,
    pub mode: VortexEncodedReadFixtureMode,
    pub request: VortexEncodedReadFixtureRequest,
    pub effects_performed: Vec<VortexEncodedReadFixtureEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexEncodedReadFixtureReport {
    /// Builds a `VortexEncodedReadFixtureReport` from a `VortexEncodedReadFixtureRequest`.
    ///
    /// # Errors
    /// Returns an error only if `ShardLoom` cannot build a deterministic report.
    pub fn from_request(request: VortexEncodedReadFixtureRequest) -> Result<Self> {
        let status = if request.has_signal(VortexEncodedReadFixtureSignal::ObjectStoreTarget) {
            VortexEncodedReadFixtureStatus::BlockedByObjectStoreTarget
        } else if request.has_signal(VortexEncodedReadFixtureSignal::BoundaryBlocked)
            || !request.has_signal(VortexEncodedReadFixtureSignal::BoundaryReady)
        {
            VortexEncodedReadFixtureStatus::BlockedByBoundary
        } else if request.has_signal(VortexEncodedReadFixtureSignal::FixtureRefMissing)
            || !request.has_signal(VortexEncodedReadFixtureSignal::FixtureRefProvided)
            || request.fixture_ref.is_none()
        {
            VortexEncodedReadFixtureStatus::BlockedByMissingFixtureRef
        } else if request.has_signal(VortexEncodedReadFixtureSignal::ScanExecutionRisk) {
            VortexEncodedReadFixtureStatus::BlockedByScanExecutionRisk
        } else if request.has_signal(VortexEncodedReadFixtureSignal::DecodeRisk) {
            VortexEncodedReadFixtureStatus::BlockedByDecodeRisk
        } else if request.has_signal(VortexEncodedReadFixtureSignal::MaterializationRisk) {
            VortexEncodedReadFixtureStatus::BlockedByMaterializationRisk
        } else if request.has_signal(VortexEncodedReadFixtureSignal::ArrowDefaultRisk) {
            VortexEncodedReadFixtureStatus::BlockedByArrowDefaultRisk
        } else if request.has_signal(VortexEncodedReadFixtureSignal::WriteRisk) {
            VortexEncodedReadFixtureStatus::BlockedByWriteRisk
        } else if !request.has_signal(VortexEncodedReadFixtureSignal::FeatureGateEnabled) {
            VortexEncodedReadFixtureStatus::BlockedByFeatureGate
        } else {
            VortexEncodedReadFixtureStatus::FixtureReady
        };
        Ok(Self {
            status,
            mode: VortexEncodedReadFixtureMode::ReportOnly,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        })
    }
    #[must_use]
    pub fn unsupported(
        request: VortexEncodedReadFixtureRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self{status:VortexEncodedReadFixtureStatus::Unsupported,mode:VortexEncodedReadFixtureMode::Unsupported,request,effects_performed:Vec::new(),diagnostics:vec![Diagnostic::unsupported(DiagnosticCode::NotImplemented,feature,reason,Some("Keep encoded-read fixture in report-only contract mode with fallback attempted false.".to_string()))]}
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
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
    pub fn boundary_ready(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadFixtureSignal::BoundaryReady)
    }
    #[must_use]
    pub fn fixture_ref_provided(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadFixtureSignal::FixtureRefProvided)
    }
    #[must_use]
    pub fn local_path_only(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadFixtureSignal::LocalPathOnly)
    }
    #[must_use]
    pub fn object_store_target(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadFixtureSignal::ObjectStoreTarget)
    }
    #[must_use]
    pub fn scan_execution_risk(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadFixtureSignal::ScanExecutionRisk)
    }
    #[must_use]
    pub fn decode_risk(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadFixtureSignal::DecodeRisk)
    }
    #[must_use]
    pub fn materialization_risk(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadFixtureSignal::MaterializationRisk)
    }
    #[must_use]
    pub fn arrow_default_risk(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadFixtureSignal::ArrowDefaultRisk)
    }
    #[must_use]
    pub fn write_risk(&self) -> bool {
        self.request
            .has_signal(VortexEncodedReadFixtureSignal::WriteRisk)
    }
    #[must_use]
    pub fn metadata_opened(&self) -> bool {
        false
    }
    #[must_use]
    pub fn footer_inspected(&self) -> bool {
        false
    }
    #[must_use]
    pub fn encoded_data_read(&self) -> bool {
        false
    }
    #[must_use]
    pub fn row_read(&self) -> bool {
        false
    }
    #[must_use]
    pub fn array_decoded(&self) -> bool {
        false
    }
    #[must_use]
    pub fn values_materialized(&self) -> bool {
        false
    }
    #[must_use]
    pub fn arrow_converted(&self) -> bool {
        false
    }
    #[must_use]
    pub fn object_store_io(&self) -> bool {
        false
    }
    #[must_use]
    pub fn data_written(&self) -> bool {
        false
    }
    #[must_use]
    pub fn upstream_scan_called(&self) -> bool {
        false
    }
    #[must_use]
    pub fn fallback_execution_allowed(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn allows_fixture_probe(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty() && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "encoded read fixture status: {}", self.status.as_str());
        let _ = writeln!(out, "mode: {}", self.mode.as_str());
        let _ = writeln!(out, "target URI: {}", self.request.target_uri.as_str());
        if let Some(f) = &self.request.fixture_ref {
            let _ = writeln!(out, "fixture ref: {}", f.summary());
        }
        let _ = writeln!(out, "boundary ready: {}", self.boundary_ready());
        let _ = writeln!(out, "fixture ref provided: {}", self.fixture_ref_provided());
        let _ = writeln!(out, "local path only: {}", self.local_path_only());
        let _ = writeln!(out, "object-store target: {}", self.object_store_target());
        let _ = writeln!(out, "scan execution risk: {}", self.scan_execution_risk());
        let _ = writeln!(out, "decode risk: {}", self.decode_risk());
        let _ = writeln!(out, "materialization risk: {}", self.materialization_risk());
        let _ = writeln!(out, "Arrow default risk: {}", self.arrow_default_risk());
        let _ = writeln!(out, "write risk: {}", self.write_risk());
        let _ = writeln!(out, "metadata opened: {}", self.metadata_opened());
        let _ = writeln!(out, "footer inspected: {}", self.footer_inspected());
        let _ = writeln!(out, "encoded data read: {}", self.encoded_data_read());
        let _ = writeln!(out, "row read: {}", self.row_read());
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

/// Plans a report-only encoded-read fixture contract.
///
/// # Errors
/// Returns an error if `VortexEncodedReadFixtureReport` cannot be built.
pub fn plan_vortex_encoded_read_fixture(
    request: VortexEncodedReadFixtureRequest,
) -> Result<VortexEncodedReadFixtureReport> {
    VortexEncodedReadFixtureReport::from_request(request)
}
#[must_use]
pub fn vortex_encoded_read_fixture_is_side_effect_free(
    report: &VortexEncodedReadFixtureReport,
) -> bool {
    report.is_side_effect_free()
}

#[must_use]
pub fn encoded_read_fixture_request_from_boundary_report(
    target_uri: DatasetUri,
    fixture_ref: VortexEncodedReadFixtureRef,
    boundary: &crate::VortexEncodedReadBoundaryReport,
) -> VortexEncodedReadFixtureRequest {
    let mut request = VortexEncodedReadFixtureRequest::with_fixture_ref(target_uri, fixture_ref)
        .fixture_ref_provided(true)
        .with_boundary_summary(boundary.to_human_text());
    if boundary.status == crate::VortexEncodedReadBoundaryStatus::BoundaryReady
        && !boundary.has_errors()
    {
        request.add_signal(VortexEncodedReadFixtureSignal::BoundaryReady);
    }
    if boundary.has_errors() {
        request.add_signal(VortexEncodedReadFixtureSignal::BoundaryBlocked);
    }
    if boundary.local_path_only() {
        request.add_signal(VortexEncodedReadFixtureSignal::LocalPathOnly);
    }
    if boundary.object_store_target() {
        request.add_signal(VortexEncodedReadFixtureSignal::ObjectStoreTarget);
    }
    if boundary.decode_risk() {
        request.add_signal(VortexEncodedReadFixtureSignal::DecodeRisk);
    }
    if boundary.materialization_risk() {
        request.add_signal(VortexEncodedReadFixtureSignal::MaterializationRisk);
    }
    if boundary.arrow_default_risk() {
        request.add_signal(VortexEncodedReadFixtureSignal::ArrowDefaultRisk);
    }
    if boundary.write_risk() {
        request.add_signal(VortexEncodedReadFixtureSignal::WriteRisk);
    }
    request
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uri() -> DatasetUri {
        DatasetUri::new("file:///tmp/f.vortex").expect("valid")
    }

    #[test]
    fn status_and_mode_basics() {
        assert!(!VortexEncodedReadFixtureStatus::FixtureReady.allows_fixture_probe());
        assert!(VortexEncodedReadFixtureStatus::BlockedByBoundary.is_error());
        assert!(!VortexEncodedReadFixtureMode::ReportOnly.reads_data());
        assert!(!VortexEncodedReadFixtureMode::ReportOnly.opens_metadata());
    }
    #[test]
    fn fixture_ref_validation_and_detection() {
        assert!(VortexEncodedReadFixtureRef::new("   ").is_err());
        assert!(VortexEncodedReadFixtureRef::from_dataset_uri(&uri()).is_ok());
        assert!(
            VortexEncodedReadFixtureRef::new("s3://bucket/x.vortex")
                .expect("ok")
                .is_object_store_like()
        );
    }
    #[test]
    fn request_signal_dedup() {
        let mut r = VortexEncodedReadFixtureRequest::new(uri());
        r.add_signal(VortexEncodedReadFixtureSignal::LocalPathOnly);
        r.add_signal(VortexEncodedReadFixtureSignal::LocalPathOnly);
        assert_eq!(r.signals.len(), 1);
        let r = r.local_path_only(false);
        assert!(!r.has_signal(VortexEncodedReadFixtureSignal::LocalPathOnly));
    }
    fn assert_blocked(
        req: VortexEncodedReadFixtureRequest,
        status: VortexEncodedReadFixtureStatus,
    ) {
        let out = VortexEncodedReadFixtureReport::from_request(req).expect("ok");
        assert_eq!(out.status, status);
    }

    #[test]
    fn status_priority_checks() {
        let ref_local = VortexEncodedReadFixtureRef::new("/tmp/x.vortex").expect("ok");
        assert_blocked(
            VortexEncodedReadFixtureRequest::with_fixture_ref(uri(), ref_local.clone())
                .object_store_target(true)
                .boundary_ready(true)
                .fixture_ref_provided(true)
                .feature_gate_enabled(true),
            VortexEncodedReadFixtureStatus::BlockedByObjectStoreTarget,
        );
        assert_blocked(
            VortexEncodedReadFixtureRequest::with_fixture_ref(uri(), ref_local.clone())
                .fixture_ref_provided(true)
                .feature_gate_enabled(true),
            VortexEncodedReadFixtureStatus::BlockedByBoundary,
        );
        assert_blocked(
            VortexEncodedReadFixtureRequest::new(uri())
                .boundary_ready(true)
                .feature_gate_enabled(true),
            VortexEncodedReadFixtureStatus::BlockedByMissingFixtureRef,
        );
        assert_blocked(
            VortexEncodedReadFixtureRequest::with_fixture_ref(uri(), ref_local.clone())
                .boundary_ready(true)
                .fixture_ref_provided(true)
                .scan_execution_risk(true)
                .feature_gate_enabled(true),
            VortexEncodedReadFixtureStatus::BlockedByScanExecutionRisk,
        );
        assert_blocked(
            VortexEncodedReadFixtureRequest::with_fixture_ref(uri(), ref_local.clone())
                .boundary_ready(true)
                .fixture_ref_provided(true)
                .decode_risk(true)
                .feature_gate_enabled(true),
            VortexEncodedReadFixtureStatus::BlockedByDecodeRisk,
        );
    }

    #[test]
    fn status_priority_checks_2() {
        let ref_local = VortexEncodedReadFixtureRef::new("/tmp/x.vortex").expect("ok");
        assert_blocked(
            VortexEncodedReadFixtureRequest::with_fixture_ref(uri(), ref_local.clone())
                .boundary_ready(true)
                .fixture_ref_provided(true)
                .materialization_risk(true)
                .feature_gate_enabled(true),
            VortexEncodedReadFixtureStatus::BlockedByMaterializationRisk,
        );
        assert_blocked(
            VortexEncodedReadFixtureRequest::with_fixture_ref(uri(), ref_local.clone())
                .boundary_ready(true)
                .fixture_ref_provided(true)
                .arrow_default_risk(true)
                .feature_gate_enabled(true),
            VortexEncodedReadFixtureStatus::BlockedByArrowDefaultRisk,
        );
        assert_blocked(
            VortexEncodedReadFixtureRequest::with_fixture_ref(uri(), ref_local.clone())
                .boundary_ready(true)
                .fixture_ref_provided(true)
                .write_risk(true)
                .feature_gate_enabled(true),
            VortexEncodedReadFixtureStatus::BlockedByWriteRisk,
        );
        assert_blocked(
            VortexEncodedReadFixtureRequest::with_fixture_ref(uri(), ref_local.clone())
                .boundary_ready(true)
                .fixture_ref_provided(true),
            VortexEncodedReadFixtureStatus::BlockedByFeatureGate,
        );

        let ready = VortexEncodedReadFixtureReport::from_request(
            VortexEncodedReadFixtureRequest::with_fixture_ref(uri(), ref_local)
                .boundary_ready(true)
                .fixture_ref_provided(true)
                .local_path_only(true)
                .feature_gate_enabled(true),
        )
        .expect("ok");
        assert_eq!(ready.status, VortexEncodedReadFixtureStatus::FixtureReady);
        assert!(!ready.allows_fixture_probe());
        assert!(ready.is_side_effect_free());
        let text = ready.to_human_text();
        assert!(text.contains("fallback execution disabled"));
        assert!(text.contains("encoded data read: false"));
    }

    #[test]
    fn report_effect_flags_false() {
        let ref_local = VortexEncodedReadFixtureRef::new("/tmp/x.vortex").expect("ok");
        let ready = VortexEncodedReadFixtureReport::from_request(
            VortexEncodedReadFixtureRequest::with_fixture_ref(uri(), ref_local)
                .boundary_ready(true)
                .fixture_ref_provided(true)
                .local_path_only(true)
                .feature_gate_enabled(true),
        )
        .expect("ok");
        assert!(!ready.metadata_opened());
        assert!(!ready.footer_inspected());
        assert!(!ready.encoded_data_read());
        assert!(!ready.row_read());
        assert!(!ready.array_decoded());
        assert!(!ready.values_materialized());
        assert!(!ready.arrow_converted());
        assert!(!ready.object_store_io());
        assert!(!ready.data_written());
        assert!(!ready.upstream_scan_called());
        assert!(!ready.fallback_execution_allowed());
    }
    #[test]
    fn diagnostics_rendered_and_plan_helper_no_io() {
        let mut r = VortexEncodedReadFixtureRequest::new(uri());
        r.add_diagnostic(Diagnostic::invalid_input(
            "encoded_read_fixture",
            "diag",
            "provide valid request signals",
        ));
        let out = plan_vortex_encoded_read_fixture(r).expect("ok");
        assert!(out.has_errors());
        assert!(out.to_human_text().contains("diag"));
    }
    #[test]
    fn request_from_boundary_maps_states() {
        let b_ready = crate::VortexEncodedReadBoundaryReport::from_request(
            crate::VortexEncodedReadBoundaryRequest::new(uri())
                .upstream_open_options_available(true)
                .upstream_footer_available(true)
                .upstream_scan_surface_deferred(true)
                .feature_gate_enabled(true)
                .local_path_only(true),
        )
        .expect("ok");
        let req = encoded_read_fixture_request_from_boundary_report(
            uri(),
            VortexEncodedReadFixtureRef::new("/tmp/x.vortex").expect("ok"),
            &b_ready,
        );
        assert!(req.has_signal(VortexEncodedReadFixtureSignal::BoundaryReady));
        let b_block = crate::VortexEncodedReadBoundaryReport::from_request(
            crate::VortexEncodedReadBoundaryRequest::new(uri()).decode_risk(true),
        )
        .expect("ok");
        let req2 = encoded_read_fixture_request_from_boundary_report(
            uri(),
            VortexEncodedReadFixtureRef::new("/tmp/x.vortex").expect("ok"),
            &b_block,
        );
        assert!(req2.has_signal(VortexEncodedReadFixtureSignal::BoundaryBlocked));
    }
}
