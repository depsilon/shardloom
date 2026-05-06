use std::fmt::Write as _;

use shardloom_core::{
    DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, Result, ShardLoomError,
};

use crate::{
    VortexEncodedReadFixtureRef, VortexEncodedReadMetadataProbeReport,
    VortexEncodedReadMetadataProbeSignal, VortexEncodedReadMetadataProbeStatus,
};

#[cfg(feature = "vortex-file-io")]
type VortexOpenOptionsCompileProbe = vortex::file::VortexOpenOptions;
#[cfg(feature = "vortex-file-io")]
type VortexFileCompileProbe = vortex::file::VortexFile;
#[cfg(feature = "vortex-file-io")]
type VortexSessionCompileProbe = vortex::session::VortexSession;

#[cfg(feature = "vortex-file-io")]
fn assert_open_options_session_ext_symbol<S: vortex::file::OpenOptionsSessionExt>() {}

#[cfg(feature = "vortex-file-io")]
#[must_use]
pub fn vortex_metadata_async_public_api_compile_probe_summary() -> &'static str {
    let _ = core::any::type_name::<VortexOpenOptionsCompileProbe>();
    let _ = core::any::type_name::<VortexFileCompileProbe>();
    let _ = core::any::type_name::<VortexSessionCompileProbe>();
    assert_open_options_session_ext_symbol::<VortexSessionCompileProbe>();
    "confirmed public symbols: `vortex::file::VortexOpenOptions`, `vortex::file::OpenOptionsSessionExt`, `vortex::file::VortexFile`, `vortex::session::VortexSession`; unresolved for this phase: compile-safe no-IO `VortexOpenOptions` construction and metadata/footer invocation path remains deferred"
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataAsyncBoundaryStatus {
    Planned,
    BoundaryReady,
    BlockedByMissingFeatureGate,
    BlockedByMissingLocalFixture,
    BlockedByObjectStoreTarget,
    BlockedByRuntimeNotApproved,
    BlockedByAsyncSessionPolicy,
    BlockedByScanRisk,
    BlockedByDecodeRisk,
    BlockedByMaterializationRisk,
    BlockedByArrowDefaultRisk,
    BlockedByWriteRisk,
    Unsupported,
}
impl VortexMetadataAsyncBoundaryStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::BoundaryReady => "boundary_ready",
            Self::BlockedByMissingFeatureGate => "blocked_by_missing_feature_gate",
            Self::BlockedByMissingLocalFixture => "blocked_by_missing_local_fixture",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByRuntimeNotApproved => "blocked_by_runtime_not_approved",
            Self::BlockedByAsyncSessionPolicy => "blocked_by_async_session_policy",
            Self::BlockedByScanRisk => "blocked_by_scan_risk",
            Self::BlockedByDecodeRisk => "blocked_by_decode_risk",
            Self::BlockedByMaterializationRisk => "blocked_by_materialization_risk",
            Self::BlockedByArrowDefaultRisk => "blocked_by_arrow_default_risk",
            Self::BlockedByWriteRisk => "blocked_by_write_risk",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        !matches!(self, Self::Planned | Self::BoundaryReady)
    }
    #[must_use]
    pub const fn boundary_ready(&self) -> bool {
        matches!(self, Self::BoundaryReady)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataAsyncBoundaryMode {
    ReportOnly,
    AsyncSessionBoundaryPlanning,
    Unsupported,
}
impl VortexMetadataAsyncBoundaryMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::AsyncSessionBoundaryPlanning => "async_session_boundary_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn performs_async_runtime(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn opens_metadata(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn inspects_footer(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn calls_scan(&self) -> bool {
        false
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
    pub const fn writes_data(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataAsyncBoundarySignal {
    FeatureGateEnabled,
    LocalFixtureReady,
    FixtureMissing,
    ObjectStoreTarget,
    RuntimeBoundaryApproved,
    AsyncSessionAllowed,
    MetadataFooterOnlyIntent,
    ScanExecutionRisk,
    DecodeRisk,
    MaterializationRisk,
    ArrowDefaultRisk,
    WriteRisk,
}
impl VortexMetadataAsyncBoundarySignal {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FeatureGateEnabled => "feature_gate_enabled",
            Self::LocalFixtureReady => "local_fixture_ready",
            Self::FixtureMissing => "fixture_missing",
            Self::ObjectStoreTarget => "object_store_target",
            Self::RuntimeBoundaryApproved => "runtime_boundary_approved",
            Self::AsyncSessionAllowed => "async_session_allowed",
            Self::MetadataFooterOnlyIntent => "metadata_footer_only_intent",
            Self::ScanExecutionRisk => "scan_execution_risk",
            Self::DecodeRisk => "decode_risk",
            Self::MaterializationRisk => "materialization_risk",
            Self::ArrowDefaultRisk => "arrow_default_risk",
            Self::WriteRisk => "write_risk",
        }
    }
    #[must_use]
    pub const fn is_blocking(&self) -> bool {
        matches!(
            self,
            Self::FixtureMissing
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
pub enum VortexMetadataAsyncBoundaryEffect {
    AsyncRuntimeStarted,
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
impl VortexMetadataAsyncBoundaryEffect {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::AsyncRuntimeStarted => "async_runtime_started",
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
pub struct VortexMetadataAsyncBoundaryRequest {
    pub target_uri: DatasetUri,
    pub fixture_ref: VortexEncodedReadFixtureRef,
    pub signals: Vec<VortexMetadataAsyncBoundarySignal>,
    pub metadata_probe_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexMetadataAsyncBoundaryRequest {
    #[must_use]
    pub fn new(target_uri: DatasetUri, fixture_ref: VortexEncodedReadFixtureRef) -> Self {
        Self {
            target_uri,
            fixture_ref,
            signals: Vec::new(),
            metadata_probe_summary: None,
            diagnostics: Vec::new(),
        }
    }
    pub fn add_signal(&mut self, s: VortexMetadataAsyncBoundarySignal) {
        if !self.signals.contains(&s) {
            self.signals.push(s);
        }
    }
    fn set_signal(mut self, s: VortexMetadataAsyncBoundarySignal, v: bool) -> Self {
        if v {
            self.add_signal(s);
        } else {
            self.signals.retain(|x| *x != s);
        }
        self
    }
    #[must_use]
    pub fn feature_gate_enabled(self, v: bool) -> Self {
        self.set_signal(VortexMetadataAsyncBoundarySignal::FeatureGateEnabled, v)
    }
    #[must_use]
    pub fn local_fixture_ready(self, v: bool) -> Self {
        self.set_signal(VortexMetadataAsyncBoundarySignal::LocalFixtureReady, v)
    }
    #[must_use]
    pub fn fixture_missing(self, v: bool) -> Self {
        self.set_signal(VortexMetadataAsyncBoundarySignal::FixtureMissing, v)
    }
    #[must_use]
    pub fn object_store_target(self, v: bool) -> Self {
        self.set_signal(VortexMetadataAsyncBoundarySignal::ObjectStoreTarget, v)
    }
    #[must_use]
    pub fn runtime_boundary_approved(self, v: bool) -> Self {
        self.set_signal(
            VortexMetadataAsyncBoundarySignal::RuntimeBoundaryApproved,
            v,
        )
    }
    #[must_use]
    pub fn async_session_allowed(self, v: bool) -> Self {
        self.set_signal(VortexMetadataAsyncBoundarySignal::AsyncSessionAllowed, v)
    }
    #[must_use]
    pub fn metadata_footer_only_intent(self, v: bool) -> Self {
        self.set_signal(
            VortexMetadataAsyncBoundarySignal::MetadataFooterOnlyIntent,
            v,
        )
    }
    #[must_use]
    pub fn scan_execution_risk(self, v: bool) -> Self {
        self.set_signal(VortexMetadataAsyncBoundarySignal::ScanExecutionRisk, v)
    }
    #[must_use]
    pub fn decode_risk(self, v: bool) -> Self {
        self.set_signal(VortexMetadataAsyncBoundarySignal::DecodeRisk, v)
    }
    #[must_use]
    pub fn materialization_risk(self, v: bool) -> Self {
        self.set_signal(VortexMetadataAsyncBoundarySignal::MaterializationRisk, v)
    }
    #[must_use]
    pub fn arrow_default_risk(self, v: bool) -> Self {
        self.set_signal(VortexMetadataAsyncBoundarySignal::ArrowDefaultRisk, v)
    }
    #[must_use]
    pub fn write_risk(self, v: bool) -> Self {
        self.set_signal(VortexMetadataAsyncBoundarySignal::WriteRisk, v)
    }
    #[must_use]
    pub fn with_metadata_probe_summary(mut self, s: impl Into<String>) -> Self {
        self.metadata_probe_summary = Some(s.into());
        self
    }
    #[must_use]
    pub fn has_signal(&self, s: VortexMetadataAsyncBoundarySignal) -> bool {
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
            "{}: {}",
            self.target_uri.as_str(),
            self.fixture_ref.summary()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexMetadataAsyncBoundaryReport {
    pub status: VortexMetadataAsyncBoundaryStatus,
    pub mode: VortexMetadataAsyncBoundaryMode,
    pub request: VortexMetadataAsyncBoundaryRequest,
    pub effects_performed: Vec<VortexMetadataAsyncBoundaryEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexMetadataAsyncBoundaryReport {
    /// # Errors
    /// Returns an error if the request carries no deterministic target URI.
    pub fn from_request(request: VortexMetadataAsyncBoundaryRequest) -> Result<Self> {
        if request.target_uri.as_str().trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "`DatasetUri` must not be empty".to_string(),
            ));
        }
        let status = derive_status(&request);
        Ok(Self {
            status,
            mode: VortexMetadataAsyncBoundaryMode::AsyncSessionBoundaryPlanning,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        })
    }
    #[must_use]
    pub fn unsupported(
        request: VortexMetadataAsyncBoundaryRequest,
        feature: &str,
        reason: &str,
    ) -> Self {
        let mut report = Self {
            status: VortexMetadataAsyncBoundaryStatus::Unsupported,
            mode: VortexMetadataAsyncBoundaryMode::Unsupported,
            request,
            effects_performed: Vec::new(),
            diagnostics: Vec::new(),
        };
        report.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            format!("unsupported `{feature}`"),
            reason.to_string(),
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
    pub const fn boundary_ready(&self) -> bool {
        self.status.boundary_ready()
    }
    #[must_use]
    pub fn feature_gate_enabled(&self) -> bool {
        self.request
            .has_signal(VortexMetadataAsyncBoundarySignal::FeatureGateEnabled)
    }
    #[must_use]
    pub fn local_fixture_ready(&self) -> bool {
        self.request
            .has_signal(VortexMetadataAsyncBoundarySignal::LocalFixtureReady)
    }
    #[must_use]
    pub fn object_store_target(&self) -> bool {
        self.request
            .has_signal(VortexMetadataAsyncBoundarySignal::ObjectStoreTarget)
            || self.request.fixture_ref.is_object_store_like()
    }
    #[must_use]
    pub fn runtime_boundary_approved(&self) -> bool {
        self.request
            .has_signal(VortexMetadataAsyncBoundarySignal::RuntimeBoundaryApproved)
    }
    #[must_use]
    pub fn async_session_allowed(&self) -> bool {
        self.request
            .has_signal(VortexMetadataAsyncBoundarySignal::AsyncSessionAllowed)
    }
    #[must_use]
    pub fn metadata_footer_only_intent(&self) -> bool {
        self.request
            .has_signal(VortexMetadataAsyncBoundarySignal::MetadataFooterOnlyIntent)
    }
    #[must_use]
    pub fn scan_execution_risk(&self) -> bool {
        self.request
            .has_signal(VortexMetadataAsyncBoundarySignal::ScanExecutionRisk)
    }
    #[must_use]
    pub fn decode_risk(&self) -> bool {
        self.request
            .has_signal(VortexMetadataAsyncBoundarySignal::DecodeRisk)
    }
    #[must_use]
    pub fn materialization_risk(&self) -> bool {
        self.request
            .has_signal(VortexMetadataAsyncBoundarySignal::MaterializationRisk)
    }
    #[must_use]
    pub fn arrow_default_risk(&self) -> bool {
        self.request
            .has_signal(VortexMetadataAsyncBoundarySignal::ArrowDefaultRisk)
    }
    #[must_use]
    pub fn write_risk(&self) -> bool {
        self.request
            .has_signal(VortexMetadataAsyncBoundarySignal::WriteRisk)
    }
    #[must_use]
    pub const fn async_runtime_started(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn metadata_opened(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn footer_inspected(&self) -> bool {
        false
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
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty()
            && !self.async_runtime_started()
            && !self.metadata_opened()
            && !self.footer_inspected()
            && !self.encoded_data_read()
            && !self.row_read()
            && !self.array_decoded()
            && !self.values_materialized()
            && !self.arrow_converted()
            && !self.object_store_io()
            && !self.data_written()
            && !self.upstream_scan_called()
            && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut s = String::new();
        let _ = writeln!(s, "status: {}", self.status.as_str());
        let _ = writeln!(s, "mode: {}", self.mode.as_str());
        let _ = writeln!(s, "async runtime started: {}", self.async_runtime_started());
        let _ = writeln!(s, "metadata opened: {}", self.metadata_opened());
        let _ = writeln!(s, "footer inspected: {}", self.footer_inspected());
        let _ = writeln!(s, "upstream scan called: {}", self.upstream_scan_called());
        let _ = writeln!(
            s,
            "fallback execution allowed: {}",
            self.fallback_execution_allowed()
        );
        s
    }
}

fn derive_status(r: &VortexMetadataAsyncBoundaryRequest) -> VortexMetadataAsyncBoundaryStatus {
    if r.has_signal(VortexMetadataAsyncBoundarySignal::ObjectStoreTarget)
        || r.fixture_ref.is_object_store_like()
    {
        return VortexMetadataAsyncBoundaryStatus::BlockedByObjectStoreTarget;
    }
    if r.has_signal(VortexMetadataAsyncBoundarySignal::FixtureMissing)
        || !r.has_signal(VortexMetadataAsyncBoundarySignal::LocalFixtureReady)
    {
        return VortexMetadataAsyncBoundaryStatus::BlockedByMissingLocalFixture;
    }
    if r.has_signal(VortexMetadataAsyncBoundarySignal::ScanExecutionRisk) {
        return VortexMetadataAsyncBoundaryStatus::BlockedByScanRisk;
    }
    if r.has_signal(VortexMetadataAsyncBoundarySignal::DecodeRisk) {
        return VortexMetadataAsyncBoundaryStatus::BlockedByDecodeRisk;
    }
    if r.has_signal(VortexMetadataAsyncBoundarySignal::MaterializationRisk) {
        return VortexMetadataAsyncBoundaryStatus::BlockedByMaterializationRisk;
    }
    if r.has_signal(VortexMetadataAsyncBoundarySignal::ArrowDefaultRisk) {
        return VortexMetadataAsyncBoundaryStatus::BlockedByArrowDefaultRisk;
    }
    if r.has_signal(VortexMetadataAsyncBoundarySignal::WriteRisk) {
        return VortexMetadataAsyncBoundaryStatus::BlockedByWriteRisk;
    }
    if !r.has_signal(VortexMetadataAsyncBoundarySignal::FeatureGateEnabled) {
        return VortexMetadataAsyncBoundaryStatus::BlockedByMissingFeatureGate;
    }
    if !r.has_signal(VortexMetadataAsyncBoundarySignal::MetadataFooterOnlyIntent) {
        return VortexMetadataAsyncBoundaryStatus::BlockedByAsyncSessionPolicy;
    }
    if !r.has_signal(VortexMetadataAsyncBoundarySignal::RuntimeBoundaryApproved) {
        return VortexMetadataAsyncBoundaryStatus::BlockedByRuntimeNotApproved;
    }
    if !r.has_signal(VortexMetadataAsyncBoundarySignal::AsyncSessionAllowed) {
        return VortexMetadataAsyncBoundaryStatus::BlockedByAsyncSessionPolicy;
    }
    VortexMetadataAsyncBoundaryStatus::BoundaryReady
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataAsyncInvocationStatus {
    BoundaryReady,
    MetadataFooterOpened,
    BlockedByBoundary,
    BlockedByUnsupportedApiSurface,
    Unsupported,
}
impl VortexMetadataAsyncInvocationStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::BoundaryReady => "boundary_ready",
            Self::MetadataFooterOpened => "metadata_footer_opened",
            Self::BlockedByBoundary => "blocked_by_boundary",
            Self::BlockedByUnsupportedApiSurface => "blocked_by_unsupported_api_surface",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        !matches!(self, Self::BoundaryReady | Self::MetadataFooterOpened)
    }
    #[must_use]
    pub const fn metadata_footer_opened(&self) -> bool {
        matches!(self, Self::MetadataFooterOpened)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataAsyncInvocationEffect {
    AsyncRuntimeStarted,
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
impl VortexMetadataAsyncInvocationEffect {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::AsyncRuntimeStarted => "async_runtime_started",
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
pub struct VortexMetadataAsyncInvocationReport {
    pub status: VortexMetadataAsyncInvocationStatus,
    pub boundary_report: VortexMetadataAsyncBoundaryReport,
    pub effects_performed: Vec<VortexMetadataAsyncInvocationEffect>,
    pub metadata_summary: Option<String>,
    pub footer_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexMetadataAsyncInvocationReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub fn metadata_opened(&self) -> bool {
        self.effects_performed
            .contains(&VortexMetadataAsyncInvocationEffect::MetadataOpened)
    }
    #[must_use]
    pub fn footer_inspected(&self) -> bool {
        self.effects_performed
            .contains(&VortexMetadataAsyncInvocationEffect::FooterInspected)
    }
    #[must_use]
    pub fn encoded_data_read(&self) -> bool {
        self.effects_performed
            .contains(&VortexMetadataAsyncInvocationEffect::EncodedDataRead)
    }
    #[must_use]
    pub fn row_read(&self) -> bool {
        self.effects_performed
            .contains(&VortexMetadataAsyncInvocationEffect::RowRead)
    }
    #[must_use]
    pub fn array_decoded(&self) -> bool {
        self.effects_performed
            .contains(&VortexMetadataAsyncInvocationEffect::ArrayDecoded)
    }
    #[must_use]
    pub fn values_materialized(&self) -> bool {
        self.effects_performed
            .contains(&VortexMetadataAsyncInvocationEffect::ValuesMaterialized)
    }
    #[must_use]
    pub fn arrow_converted(&self) -> bool {
        self.effects_performed
            .contains(&VortexMetadataAsyncInvocationEffect::ArrowConverted)
    }
    #[must_use]
    pub fn object_store_io(&self) -> bool {
        self.effects_performed
            .contains(&VortexMetadataAsyncInvocationEffect::ObjectStoreIo)
    }
    #[must_use]
    pub fn data_written(&self) -> bool {
        self.effects_performed
            .contains(&VortexMetadataAsyncInvocationEffect::DataWritten)
    }
    #[must_use]
    pub fn upstream_scan_called(&self) -> bool {
        self.effects_performed
            .contains(&VortexMetadataAsyncInvocationEffect::UpstreamScanCalled)
    }
    #[must_use]
    pub fn async_runtime_started(&self) -> bool {
        self.effects_performed
            .contains(&VortexMetadataAsyncInvocationEffect::AsyncRuntimeStarted)
    }
    #[must_use]
    pub fn fallback_execution_allowed(&self) -> bool {
        self.effects_performed
            .contains(&VortexMetadataAsyncInvocationEffect::FallbackExecution)
    }
    #[must_use]
    pub const fn metadata_footer_opened(&self) -> bool {
        self.status.metadata_footer_opened()
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty()
            && !self.async_runtime_started()
            && !self.metadata_opened()
            && !self.footer_inspected()
            && !self.encoded_data_read()
            && !self.row_read()
            && !self.array_decoded()
            && !self.values_materialized()
            && !self.arrow_converted()
            && !self.object_store_io()
            && !self.data_written()
            && !self.upstream_scan_called()
            && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut o = String::new();
        let _ = writeln!(o, "status: {}", self.status.as_str());
        let _ = writeln!(
            o,
            "metadata footer opened: {}",
            self.metadata_footer_opened()
        );
        let _ = writeln!(o, "async runtime started: {}", self.async_runtime_started());
        let _ = writeln!(
            o,
            "fallback execution allowed: {}",
            self.fallback_execution_allowed()
        );
        o
    }
}

#[cfg(feature = "vortex-file-io")]
/// # Errors
/// Returns an error if deterministic async invocation report construction fails.
pub async fn invoke_vortex_metadata_footer_probe_async(
    boundary: VortexMetadataAsyncBoundaryReport,
) -> Result<VortexMetadataAsyncInvocationReport> {
    if !boundary.boundary_ready() {
        return Ok(VortexMetadataAsyncInvocationReport {
            status: VortexMetadataAsyncInvocationStatus::BlockedByBoundary,
            boundary_report: boundary,
            effects_performed: Vec::new(),
            metadata_summary: None,
            footer_summary: None,
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "metadata/footer async boundary blocked",
                "`VortexMetadataAsyncBoundaryReport` is not `BoundaryReady`",
                None,
            )],
        });
    }

    Ok(VortexMetadataAsyncInvocationReport {
        status: VortexMetadataAsyncInvocationStatus::BlockedByUnsupportedApiSurface,
        boundary_report: boundary,
        effects_performed: Vec::new(),
        metadata_summary: None,
        footer_summary: None,
        diagnostics: vec![Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            "metadata/footer async invocation blocked",
            vortex_metadata_async_public_api_compile_probe_summary(),
            None,
        )],
    })
}

#[cfg(not(feature = "vortex-file-io"))]
/// # Errors
/// Returns an error if deterministic report construction fails.
pub async fn invoke_vortex_metadata_footer_probe_async(
    boundary: VortexMetadataAsyncBoundaryReport,
) -> Result<VortexMetadataAsyncInvocationReport> {
    if !boundary.boundary_ready() {
        return Ok(VortexMetadataAsyncInvocationReport {
            status: VortexMetadataAsyncInvocationStatus::BlockedByBoundary,
            boundary_report: boundary,
            effects_performed: Vec::new(),
            metadata_summary: None,
            footer_summary: None,
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "metadata/footer async boundary blocked",
                "`VortexMetadataAsyncBoundaryReport` is not `BoundaryReady`",
                None,
            )],
        });
    }

    Ok(VortexMetadataAsyncInvocationReport {
        status: VortexMetadataAsyncInvocationStatus::BlockedByUnsupportedApiSurface,
        boundary_report: boundary,
        effects_performed: Vec::new(),
        metadata_summary: None,
        footer_summary: None,
        diagnostics: vec![Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            "metadata/footer async invocation blocked",
            "`vortex-file-io` feature not enabled",
            None,
        )],
    })
}

#[must_use]
pub fn metadata_async_boundary_request_from_metadata_probe_report(
    target_uri: DatasetUri,
    fixture_ref: VortexEncodedReadFixtureRef,
    metadata_probe: &VortexEncodedReadMetadataProbeReport,
) -> VortexMetadataAsyncBoundaryRequest {
    let mut req = VortexMetadataAsyncBoundaryRequest::new(target_uri, fixture_ref)
        .with_metadata_probe_summary(metadata_probe.to_human_text());
    if metadata_probe
        .request
        .has_signal(VortexEncodedReadMetadataProbeSignal::FeatureGateEnabled)
    {
        req.add_signal(VortexMetadataAsyncBoundarySignal::FeatureGateEnabled);
    }
    if metadata_probe.object_store_target() {
        req.add_signal(VortexMetadataAsyncBoundarySignal::ObjectStoreTarget);
    }
    match metadata_probe.status {
        VortexEncodedReadMetadataProbeStatus::BlockedByMissingLocalFile
        | VortexEncodedReadMetadataProbeStatus::BlockedByMissingFixtureRef => {
            req.add_signal(VortexMetadataAsyncBoundarySignal::FixtureMissing);
        }
        _ => {}
    }
    if !matches!(
        metadata_probe.status,
        VortexEncodedReadMetadataProbeStatus::BlockedByMissingLocalFile
            | VortexEncodedReadMetadataProbeStatus::BlockedByMissingFixtureRef
            | VortexEncodedReadMetadataProbeStatus::BlockedByFixture
            | VortexEncodedReadMetadataProbeStatus::BlockedByObjectStoreTarget
            | VortexEncodedReadMetadataProbeStatus::BlockedByScanExecutionRisk
            | VortexEncodedReadMetadataProbeStatus::BlockedByDecodeRisk
            | VortexEncodedReadMetadataProbeStatus::BlockedByMaterializationRisk
            | VortexEncodedReadMetadataProbeStatus::BlockedByArrowDefaultRisk
            | VortexEncodedReadMetadataProbeStatus::BlockedByWriteRisk
    ) {
        req.add_signal(VortexMetadataAsyncBoundarySignal::LocalFixtureReady);
    }
    if metadata_probe.scan_execution_risk() {
        req.add_signal(VortexMetadataAsyncBoundarySignal::ScanExecutionRisk);
    }
    if metadata_probe.decode_risk() {
        req.add_signal(VortexMetadataAsyncBoundarySignal::DecodeRisk);
    }
    if metadata_probe.materialization_risk() {
        req.add_signal(VortexMetadataAsyncBoundarySignal::MaterializationRisk);
    }
    if metadata_probe.arrow_default_risk() {
        req.add_signal(VortexMetadataAsyncBoundarySignal::ArrowDefaultRisk);
    }
    if metadata_probe.write_risk() {
        req.add_signal(VortexMetadataAsyncBoundarySignal::WriteRisk);
    }
    if matches!(
        metadata_probe.status,
        VortexEncodedReadMetadataProbeStatus::BlockedByUnsupportedApiSurface
    ) {
        req.add_signal(VortexMetadataAsyncBoundarySignal::MetadataFooterOnlyIntent);
    }
    req
}

/// # Errors
/// Returns an error if [`VortexMetadataAsyncBoundaryReport::from_request`] cannot build a deterministic report.
pub fn plan_vortex_metadata_async_boundary(
    request: VortexMetadataAsyncBoundaryRequest,
) -> Result<VortexMetadataAsyncBoundaryReport> {
    VortexMetadataAsyncBoundaryReport::from_request(request)
}

#[must_use]
pub fn vortex_metadata_async_boundary_is_side_effect_free(
    report: &VortexMetadataAsyncBoundaryReport,
) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        VortexEncodedReadMetadataProbeMode, VortexEncodedReadMetadataProbeRequest,
        probe_vortex_encoded_read_metadata,
    };
    fn mk_req() -> VortexMetadataAsyncBoundaryRequest {
        VortexMetadataAsyncBoundaryRequest::new(
            DatasetUri::new("file:///tmp/a.vortex").unwrap(),
            VortexEncodedReadFixtureRef::new("/tmp/a.vortex").unwrap(),
        )
    }
    #[test]
    fn boundary_ready_requires_all() {
        let req = mk_req()
            .feature_gate_enabled(true)
            .local_fixture_ready(true)
            .runtime_boundary_approved(true)
            .async_session_allowed(true)
            .metadata_footer_only_intent(true);
        let r = plan_vortex_metadata_async_boundary(req).unwrap();
        assert!(r.boundary_ready());
        assert!(r.is_side_effect_free());
    }
    #[test]
    fn missing_runtime_blocks() {
        let r = plan_vortex_metadata_async_boundary(
            mk_req()
                .feature_gate_enabled(true)
                .local_fixture_ready(true)
                .async_session_allowed(true)
                .metadata_footer_only_intent(true),
        )
        .unwrap();
        assert_eq!(
            r.status,
            VortexMetadataAsyncBoundaryStatus::BlockedByRuntimeNotApproved
        );
    }
    #[test]
    fn async_invoke_future_construction_non_ready_is_lazy() {
        let report = plan_vortex_metadata_async_boundary(mk_req()).unwrap();
        let fut = invoke_vortex_metadata_footer_probe_async(report);
        drop(fut);
    }

    #[test]
    fn async_invoke_future_construction_ready_is_lazy() {
        let report = plan_vortex_metadata_async_boundary(
            mk_req()
                .feature_gate_enabled(true)
                .local_fixture_ready(true)
                .runtime_boundary_approved(true)
                .async_session_allowed(true)
                .metadata_footer_only_intent(true),
        )
        .unwrap();
        let fut = invoke_vortex_metadata_footer_probe_async(report);
        drop(fut);
    }

    #[test]
    fn invocation_helpers_stable() {
        assert_eq!(
            VortexMetadataAsyncInvocationStatus::BoundaryReady.as_str(),
            "boundary_ready"
        );
        assert_eq!(
            VortexMetadataAsyncInvocationEffect::FallbackExecution.as_str(),
            "fallback_execution"
        );
        let report = VortexMetadataAsyncInvocationReport {
            status: VortexMetadataAsyncInvocationStatus::BlockedByUnsupportedApiSurface,
            boundary_report: plan_vortex_metadata_async_boundary(mk_req()).unwrap(),
            effects_performed: Vec::new(),
            metadata_summary: None,
            footer_summary: None,
            diagnostics: Vec::new(),
        };
        assert!(!report.fallback_execution_allowed());
        assert!(!report.async_runtime_started());
        assert!(
            report
                .to_human_text()
                .contains("fallback execution allowed: false")
        );
    }

    #[test]
    fn helper_no_auto_approve() {
        let mp = probe_vortex_encoded_read_metadata(
            VortexEncodedReadMetadataProbeRequest::new(
                DatasetUri::new("file:///tmp/a.vortex").unwrap(),
                VortexEncodedReadFixtureRef::new("/tmp/a.vortex").unwrap(),
            )
            .feature_gate_enabled(true)
            .fixture_ref_provided(true)
            .local_path_only(true),
        )
        .unwrap();
        let req = metadata_async_boundary_request_from_metadata_probe_report(
            DatasetUri::new("file:///tmp/a.vortex").unwrap(),
            VortexEncodedReadFixtureRef::new("/tmp/a.vortex").unwrap(),
            &mp,
        );
        assert!(!req.has_signal(VortexMetadataAsyncBoundarySignal::RuntimeBoundaryApproved));
        assert!(!req.has_signal(VortexMetadataAsyncBoundarySignal::AsyncSessionAllowed));
    }

    #[test]
    fn helper_preserves_fixture_blocked_status() {
        let mp = VortexEncodedReadMetadataProbeReport {
            status: VortexEncodedReadMetadataProbeStatus::BlockedByFixture,
            mode: VortexEncodedReadMetadataProbeMode::ReportOnly,
            request: VortexEncodedReadMetadataProbeRequest::new(
                DatasetUri::new("file:///tmp/a.vortex").unwrap(),
                VortexEncodedReadFixtureRef::new("/tmp/a.vortex").unwrap(),
            ),
            effects_performed: Vec::new(),
            metadata_summary: None,
            footer_summary: None,
            diagnostics: Vec::new(),
        };
        let req = metadata_async_boundary_request_from_metadata_probe_report(
            DatasetUri::new("file:///tmp/a.vortex").unwrap(),
            VortexEncodedReadFixtureRef::new("/tmp/a.vortex").unwrap(),
            &mp,
        );
        assert!(!req.has_signal(VortexMetadataAsyncBoundarySignal::LocalFixtureReady));
    }

    #[cfg(feature = "vortex-file-io")]
    #[test]
    fn compile_probe_helper_reports_confirmed_symbols() {
        let summary = vortex_metadata_async_public_api_compile_probe_summary();
        assert!(summary.contains("VortexOpenOptions"));
        assert!(summary.contains("OpenOptionsSessionExt"));
        assert!(summary.contains("VortexSession"));
    }
}
