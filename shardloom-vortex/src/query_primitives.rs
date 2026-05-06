use std::fmt::Write as _;

use shardloom_core::{
    DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, Result, ShardLoomError,
};

use crate::{VortexMetadataAsyncBoundaryReport, VortexMetadataAsyncInvocationReport};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexQueryPrimitiveKind {
    Count,
    FilteredCount,
    Projection,
    PredicateFilter,
}
impl VortexQueryPrimitiveKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::FilteredCount => "filtered_count",
            Self::Projection => "projection",
            Self::PredicateFilter => "predicate_filter",
        }
    }
    #[must_use]
    pub const fn requires_predicate(&self) -> bool {
        matches!(self, Self::FilteredCount | Self::PredicateFilter)
    }
    #[must_use]
    pub const fn requires_projection(&self) -> bool {
        matches!(self, Self::Projection)
    }
    #[must_use]
    pub const fn can_be_metadata_only_candidate(&self) -> bool {
        matches!(self, Self::Count)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexQueryPrimitiveStatus {
    FeatureDisabled,
    Planned,
    PrimitiveReady,
    BlockedByMissingMetadataFooter,
    BlockedByMissingEncodedDataPath,
    BlockedByMissingPredicate,
    BlockedByMissingProjection,
    BlockedByUnsupportedPredicate,
    BlockedByDecodeRisk,
    BlockedByMaterializationRisk,
    BlockedByArrowDefaultRisk,
    BlockedByObjectStoreTarget,
    BlockedByWriteRisk,
    BlockedByScanExecutionRisk,
    BlockedByFallbackPolicy,
    Unsupported,
}
impl VortexQueryPrimitiveStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::Planned => "planned",
            Self::PrimitiveReady => "primitive_ready",
            Self::BlockedByMissingMetadataFooter => "blocked_by_missing_metadata_footer",
            Self::BlockedByMissingEncodedDataPath => "blocked_by_missing_encoded_data_path",
            Self::BlockedByMissingPredicate => "blocked_by_missing_predicate",
            Self::BlockedByMissingProjection => "blocked_by_missing_projection",
            Self::BlockedByUnsupportedPredicate => "blocked_by_unsupported_predicate",
            Self::BlockedByDecodeRisk => "blocked_by_decode_risk",
            Self::BlockedByMaterializationRisk => "blocked_by_materialization_risk",
            Self::BlockedByArrowDefaultRisk => "blocked_by_arrow_default_risk",
            Self::BlockedByObjectStoreTarget => "blocked_by_object_store_target",
            Self::BlockedByWriteRisk => "blocked_by_write_risk",
            Self::BlockedByScanExecutionRisk => "blocked_by_scan_execution_risk",
            Self::BlockedByFallbackPolicy => "blocked_by_fallback_policy",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        !matches!(
            self,
            Self::FeatureDisabled | Self::Planned | Self::PrimitiveReady
        )
    }
    #[must_use]
    pub const fn primitive_ready(&self) -> bool {
        matches!(self, Self::PrimitiveReady)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexQueryPrimitiveMode {
    ReportOnly,
    ReadinessPlanning,
    Unsupported,
}
impl VortexQueryPrimitiveMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::ReadinessPlanning => "readiness_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn executes_query(&self) -> bool {
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
    #[must_use]
    pub const fn calls_upstream_scan(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexQueryPrimitiveSignal {
    FeatureGateEnabled,
    MetadataFooterReady,
    EncodedDataPathReady,
    LocalOnly,
    ObjectStoreTarget,
    PredicateProvided,
    ProjectionProvided,
    PredicateUnsupported,
    DecodeRisk,
    MaterializationRisk,
    ArrowDefaultRisk,
    WriteRisk,
    ScanExecutionRisk,
    FallbackPolicyBlocked,
}
impl VortexQueryPrimitiveSignal {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FeatureGateEnabled => "feature_gate_enabled",
            Self::MetadataFooterReady => "metadata_footer_ready",
            Self::EncodedDataPathReady => "encoded_data_path_ready",
            Self::LocalOnly => "local_only",
            Self::ObjectStoreTarget => "object_store_target",
            Self::PredicateProvided => "predicate_provided",
            Self::ProjectionProvided => "projection_provided",
            Self::PredicateUnsupported => "predicate_unsupported",
            Self::DecodeRisk => "decode_risk",
            Self::MaterializationRisk => "materialization_risk",
            Self::ArrowDefaultRisk => "arrow_default_risk",
            Self::WriteRisk => "write_risk",
            Self::ScanExecutionRisk => "scan_execution_risk",
            Self::FallbackPolicyBlocked => "fallback_policy_blocked",
        }
    }
    #[must_use]
    pub const fn is_blocking(&self) -> bool {
        matches!(
            self,
            Self::ObjectStoreTarget
                | Self::PredicateUnsupported
                | Self::DecodeRisk
                | Self::MaterializationRisk
                | Self::ArrowDefaultRisk
                | Self::WriteRisk
                | Self::ScanExecutionRisk
                | Self::FallbackPolicyBlocked
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexQueryPrimitiveEffect {
    QueryExecuted,
    EncodedDataRead,
    RowRead,
    PredicateEvaluated,
    ProjectionApplied,
    ArrayDecoded,
    ValuesMaterialized,
    ArrowConverted,
    ObjectStoreIo,
    DataWritten,
    UpstreamScanCalled,
    FallbackExecution,
}
impl VortexQueryPrimitiveEffect {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::QueryExecuted => "query_executed",
            Self::EncodedDataRead => "encoded_data_read",
            Self::RowRead => "row_read",
            Self::PredicateEvaluated => "predicate_evaluated",
            Self::ProjectionApplied => "projection_applied",
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
pub struct VortexQueryPrimitiveRequest {
    pub target_uri: DatasetUri,
    pub primitive: VortexQueryPrimitiveKind,
    pub signals: Vec<VortexQueryPrimitiveSignal>,
    pub predicate_summary: Option<String>,
    pub projection_summary: Option<String>,
    pub upstream_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexQueryPrimitiveRequest {
    #[must_use]
    pub fn new(target_uri: DatasetUri, primitive: VortexQueryPrimitiveKind) -> Self {
        Self {
            target_uri,
            primitive,
            signals: vec![],
            predicate_summary: None,
            projection_summary: None,
            upstream_summary: None,
            diagnostics: vec![],
        }
    }
    fn set_signal(mut self, s: VortexQueryPrimitiveSignal, v: bool) -> Self {
        if v {
            self.add_signal(s);
        } else {
            self.signals.retain(|x| *x != s);
        }
        self
    }
    pub fn add_signal(&mut self, s: VortexQueryPrimitiveSignal) {
        if !self.signals.contains(&s) {
            self.signals.push(s);
        }
    }
    #[must_use]
    pub fn has_signal(&self, s: VortexQueryPrimitiveSignal) -> bool {
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
        format!("{}:{}", self.primitive.as_str(), self.target_uri.as_str())
    }
}
macro_rules! bld {
    ($n:ident,$s:ident) => {
        #[must_use]
        pub fn $n(self, v: bool) -> Self {
            self.set_signal(VortexQueryPrimitiveSignal::$s, v)
        }
    };
}
impl VortexQueryPrimitiveRequest {
    bld!(feature_gate_enabled, FeatureGateEnabled);
    bld!(metadata_footer_ready, MetadataFooterReady);
    bld!(encoded_data_path_ready, EncodedDataPathReady);
    bld!(local_only, LocalOnly);
    bld!(object_store_target, ObjectStoreTarget);
    bld!(predicate_provided, PredicateProvided);
    bld!(projection_provided, ProjectionProvided);
    bld!(predicate_unsupported, PredicateUnsupported);
    bld!(decode_risk, DecodeRisk);
    bld!(materialization_risk, MaterializationRisk);
    bld!(arrow_default_risk, ArrowDefaultRisk);
    bld!(write_risk, WriteRisk);
    bld!(scan_execution_risk, ScanExecutionRisk);
    bld!(fallback_policy_blocked, FallbackPolicyBlocked);
    #[must_use]
    pub fn with_predicate_summary(mut self, s: impl Into<String>) -> Self {
        self.predicate_summary = Some(s.into());
        self
    }
    #[must_use]
    pub fn with_projection_summary(mut self, s: impl Into<String>) -> Self {
        self.projection_summary = Some(s.into());
        self
    }
    #[must_use]
    pub fn with_upstream_summary(mut self, s: impl Into<String>) -> Self {
        self.upstream_summary = Some(s.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexQueryPrimitiveReport {
    pub status: VortexQueryPrimitiveStatus,
    pub mode: VortexQueryPrimitiveMode,
    pub request: VortexQueryPrimitiveRequest,
    pub effects_performed: Vec<VortexQueryPrimitiveEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexQueryPrimitiveReport {
    /// # Errors
    /// Returns an error if the `DatasetUri` is empty.
    pub fn from_request(request: VortexQueryPrimitiveRequest) -> Result<Self> {
        if request.target_uri.as_str().trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "`DatasetUri` must not be empty".to_string(),
            ));
        }
        Ok(Self {
            status: derive_status(&request),
            mode: VortexQueryPrimitiveMode::ReadinessPlanning,
            request,
            effects_performed: vec![],
            diagnostics: vec![],
        })
    }
    #[must_use]
    pub fn unsupported(request: VortexQueryPrimitiveRequest, feature: &str, reason: &str) -> Self {
        Self {
            status: VortexQueryPrimitiveStatus::Unsupported,
            mode: VortexQueryPrimitiveMode::Unsupported,
            request,
            effects_performed: vec![],
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                format!("unsupported `{feature}`"),
                reason.to_string(),
                None,
            )],
        }
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error() || self.request.has_errors()
    }
    #[must_use]
    pub const fn primitive_ready(&self) -> bool {
        self.status.primitive_ready()
    }
    #[must_use]
    pub fn feature_gate_enabled(&self) -> bool {
        self.request
            .has_signal(VortexQueryPrimitiveSignal::FeatureGateEnabled)
    }
    #[must_use]
    pub fn metadata_footer_ready(&self) -> bool {
        self.request
            .has_signal(VortexQueryPrimitiveSignal::MetadataFooterReady)
    }
    #[must_use]
    pub fn encoded_data_path_ready(&self) -> bool {
        self.request
            .has_signal(VortexQueryPrimitiveSignal::EncodedDataPathReady)
    }
    #[must_use]
    pub fn object_store_target(&self) -> bool {
        self.request
            .has_signal(VortexQueryPrimitiveSignal::ObjectStoreTarget)
    }
    #[must_use]
    pub fn decode_risk(&self) -> bool {
        self.request
            .has_signal(VortexQueryPrimitiveSignal::DecodeRisk)
    }
    #[must_use]
    pub fn materialization_risk(&self) -> bool {
        self.request
            .has_signal(VortexQueryPrimitiveSignal::MaterializationRisk)
    }
    #[must_use]
    pub fn arrow_default_risk(&self) -> bool {
        self.request
            .has_signal(VortexQueryPrimitiveSignal::ArrowDefaultRisk)
    }
    #[must_use]
    pub fn write_risk(&self) -> bool {
        self.request
            .has_signal(VortexQueryPrimitiveSignal::WriteRisk)
    }
    #[must_use]
    pub fn scan_execution_risk(&self) -> bool {
        self.request
            .has_signal(VortexQueryPrimitiveSignal::ScanExecutionRisk)
    }
}
macro_rules! eff {
    ($n:ident,$e:ident) => {
        #[must_use]
        pub fn $n(&self) -> bool {
            self.effects_performed
                .contains(&VortexQueryPrimitiveEffect::$e)
        }
    };
}
impl VortexQueryPrimitiveReport {
    eff!(query_executed, QueryExecuted);
    eff!(encoded_data_read, EncodedDataRead);
    eff!(row_read, RowRead);
    eff!(predicate_evaluated, PredicateEvaluated);
    eff!(projection_applied, ProjectionApplied);
    eff!(array_decoded, ArrayDecoded);
    eff!(values_materialized, ValuesMaterialized);
    eff!(arrow_converted, ArrowConverted);
    eff!(object_store_io, ObjectStoreIo);
    eff!(data_written, DataWritten);
    eff!(upstream_scan_called, UpstreamScanCalled);
    #[must_use]
    pub fn fallback_execution_allowed(&self) -> bool {
        self.effects_performed
            .contains(&VortexQueryPrimitiveEffect::FallbackExecution)
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        self.effects_performed.is_empty() && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut s = String::new();
        let _ = writeln!(s, "status: {}", self.status.as_str());
        let _ = writeln!(s, "mode: {}", self.mode.as_str());
        let _ = writeln!(s, "query executed: {}", self.query_executed());
        let _ = writeln!(
            s,
            "fallback execution allowed: {}",
            self.fallback_execution_allowed()
        );
        let _ = writeln!(s, "no execution performed: true");
        s
    }
}

fn derive_status(r: &VortexQueryPrimitiveRequest) -> VortexQueryPrimitiveStatus {
    if r.has_signal(VortexQueryPrimitiveSignal::ObjectStoreTarget) {
        return VortexQueryPrimitiveStatus::BlockedByObjectStoreTarget;
    }
    if r.has_signal(VortexQueryPrimitiveSignal::ScanExecutionRisk) {
        return VortexQueryPrimitiveStatus::BlockedByScanExecutionRisk;
    }
    if r.has_signal(VortexQueryPrimitiveSignal::DecodeRisk) {
        return VortexQueryPrimitiveStatus::BlockedByDecodeRisk;
    }
    if r.has_signal(VortexQueryPrimitiveSignal::MaterializationRisk) {
        return VortexQueryPrimitiveStatus::BlockedByMaterializationRisk;
    }
    if r.has_signal(VortexQueryPrimitiveSignal::ArrowDefaultRisk) {
        return VortexQueryPrimitiveStatus::BlockedByArrowDefaultRisk;
    }
    if r.has_signal(VortexQueryPrimitiveSignal::WriteRisk) {
        return VortexQueryPrimitiveStatus::BlockedByWriteRisk;
    }
    if r.has_signal(VortexQueryPrimitiveSignal::FallbackPolicyBlocked) {
        return VortexQueryPrimitiveStatus::BlockedByFallbackPolicy;
    }
    if !r.has_signal(VortexQueryPrimitiveSignal::FeatureGateEnabled) {
        return VortexQueryPrimitiveStatus::FeatureDisabled;
    }
    if !r.has_signal(VortexQueryPrimitiveSignal::MetadataFooterReady) {
        return VortexQueryPrimitiveStatus::BlockedByMissingMetadataFooter;
    }
    if !r.has_signal(VortexQueryPrimitiveSignal::EncodedDataPathReady) {
        return VortexQueryPrimitiveStatus::BlockedByMissingEncodedDataPath;
    }
    if r.has_signal(VortexQueryPrimitiveSignal::PredicateUnsupported) {
        return VortexQueryPrimitiveStatus::BlockedByUnsupportedPredicate;
    }
    if r.primitive.requires_predicate()
        && !r.has_signal(VortexQueryPrimitiveSignal::PredicateProvided)
    {
        return VortexQueryPrimitiveStatus::BlockedByMissingPredicate;
    }
    if r.primitive.requires_projection()
        && !r.has_signal(VortexQueryPrimitiveSignal::ProjectionProvided)
    {
        return VortexQueryPrimitiveStatus::BlockedByMissingProjection;
    }
    VortexQueryPrimitiveStatus::PrimitiveReady
}

#[must_use]
pub fn query_primitive_request_from_metadata_async_boundary(
    target_uri: DatasetUri,
    primitive: VortexQueryPrimitiveKind,
    boundary: &VortexMetadataAsyncBoundaryReport,
) -> VortexQueryPrimitiveRequest {
    let mut req = VortexQueryPrimitiveRequest::new(target_uri, primitive)
        .with_upstream_summary(boundary.to_human_text());
    if boundary.feature_gate_enabled() {
        req.add_signal(VortexQueryPrimitiveSignal::FeatureGateEnabled);
    }
    if boundary.object_store_target() {
        req.add_signal(VortexQueryPrimitiveSignal::ObjectStoreTarget);
    }
    if boundary.scan_execution_risk() {
        req.add_signal(VortexQueryPrimitiveSignal::ScanExecutionRisk);
    }
    if boundary.decode_risk() {
        req.add_signal(VortexQueryPrimitiveSignal::DecodeRisk);
    }
    if boundary.materialization_risk() {
        req.add_signal(VortexQueryPrimitiveSignal::MaterializationRisk);
    }
    if boundary.arrow_default_risk() {
        req.add_signal(VortexQueryPrimitiveSignal::ArrowDefaultRisk);
    }
    if boundary.write_risk() {
        req.add_signal(VortexQueryPrimitiveSignal::WriteRisk);
    }
    req
}

#[must_use]
pub fn query_primitive_request_from_metadata_async_invocation(
    target_uri: DatasetUri,
    primitive: VortexQueryPrimitiveKind,
    invocation: &VortexMetadataAsyncInvocationReport,
) -> VortexQueryPrimitiveRequest {
    let mut req = VortexQueryPrimitiveRequest::new(target_uri, primitive)
        .with_upstream_summary(invocation.to_human_text());
    if invocation.boundary_report.feature_gate_enabled() {
        req.add_signal(VortexQueryPrimitiveSignal::FeatureGateEnabled);
    }
    if invocation.metadata_footer_opened() {
        req.add_signal(VortexQueryPrimitiveSignal::MetadataFooterReady);
    }
    if invocation.boundary_report.object_store_target() || invocation.object_store_io() {
        req.add_signal(VortexQueryPrimitiveSignal::ObjectStoreTarget);
    }
    if invocation.boundary_report.scan_execution_risk() {
        req.add_signal(VortexQueryPrimitiveSignal::ScanExecutionRisk);
    }
    if invocation.boundary_report.decode_risk() {
        req.add_signal(VortexQueryPrimitiveSignal::DecodeRisk);
    }
    if invocation.boundary_report.materialization_risk() {
        req.add_signal(VortexQueryPrimitiveSignal::MaterializationRisk);
    }
    if invocation.boundary_report.arrow_default_risk() {
        req.add_signal(VortexQueryPrimitiveSignal::ArrowDefaultRisk);
    }
    if invocation.boundary_report.write_risk() {
        req.add_signal(VortexQueryPrimitiveSignal::WriteRisk);
    }
    if !invocation.boundary_report.boundary_ready() {
        req.add_signal(VortexQueryPrimitiveSignal::FallbackPolicyBlocked);
    }
    req
}

/// # Errors
/// # Errors
/// Returns an error if `VortexQueryPrimitiveReport::from_request` fails.
pub fn plan_vortex_query_primitive(
    request: VortexQueryPrimitiveRequest,
) -> Result<VortexQueryPrimitiveReport> {
    VortexQueryPrimitiveReport::from_request(request)
}

#[must_use]
pub fn vortex_query_primitive_is_side_effect_free(report: &VortexQueryPrimitiveReport) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn uri() -> DatasetUri {
        DatasetUri::new("file:///tmp/a.vortex").unwrap()
    }
    #[test]
    fn basics() {
        assert_eq!(VortexQueryPrimitiveKind::Count.as_str(), "count");
        assert!(!VortexQueryPrimitiveKind::Count.requires_predicate());
        assert!(VortexQueryPrimitiveKind::FilteredCount.requires_predicate());
        assert!(VortexQueryPrimitiveKind::Projection.requires_projection());
    }
    #[test]
    fn statuses() {
        let r = plan_vortex_query_primitive(VortexQueryPrimitiveRequest::new(
            uri(),
            VortexQueryPrimitiveKind::Count,
        ))
        .unwrap();
        assert_eq!(r.status, VortexQueryPrimitiveStatus::FeatureDisabled);
        let r = plan_vortex_query_primitive(
            VortexQueryPrimitiveRequest::new(uri(), VortexQueryPrimitiveKind::Count)
                .feature_gate_enabled(true),
        )
        .unwrap();
        assert_eq!(
            r.status,
            VortexQueryPrimitiveStatus::BlockedByMissingMetadataFooter
        );
        let r = plan_vortex_query_primitive(
            VortexQueryPrimitiveRequest::new(uri(), VortexQueryPrimitiveKind::Count)
                .feature_gate_enabled(true)
                .metadata_footer_ready(true),
        )
        .unwrap();
        assert_eq!(
            r.status,
            VortexQueryPrimitiveStatus::BlockedByMissingEncodedDataPath
        );
    }
    #[test]
    fn predicate_projection_blocks() {
        let r = plan_vortex_query_primitive(
            VortexQueryPrimitiveRequest::new(uri(), VortexQueryPrimitiveKind::FilteredCount)
                .feature_gate_enabled(true)
                .metadata_footer_ready(true)
                .encoded_data_path_ready(true),
        )
        .unwrap();
        assert_eq!(
            r.status,
            VortexQueryPrimitiveStatus::BlockedByMissingPredicate
        );
        let r = plan_vortex_query_primitive(
            VortexQueryPrimitiveRequest::new(uri(), VortexQueryPrimitiveKind::Projection)
                .feature_gate_enabled(true)
                .metadata_footer_ready(true)
                .encoded_data_path_ready(true),
        )
        .unwrap();
        assert_eq!(
            r.status,
            VortexQueryPrimitiveStatus::BlockedByMissingProjection
        );
    }
    #[test]
    fn ready_and_effects() {
        let r = plan_vortex_query_primitive(
            VortexQueryPrimitiveRequest::new(uri(), VortexQueryPrimitiveKind::Count)
                .feature_gate_enabled(true)
                .metadata_footer_ready(true)
                .encoded_data_path_ready(true),
        )
        .unwrap();
        assert!(r.primitive_ready());
        assert!(!r.query_executed());
        assert!(!r.fallback_execution_allowed());
        assert!(r.is_side_effect_free());
        assert!(
            r.to_human_text()
                .contains("fallback execution allowed: false")
        );
        assert!(r.to_human_text().contains("no execution"));
    }

    #[test]
    fn invocation_request_propagates_boundary_signals() {
        let boundary = VortexMetadataAsyncBoundaryReport::from_request(
            crate::VortexMetadataAsyncBoundaryRequest::new(
                uri(),
                crate::VortexEncodedReadFixtureRef::new("/tmp/a.vortex").unwrap(),
            )
            .feature_gate_enabled(true)
            .local_fixture_ready(true)
            .runtime_boundary_approved(true)
            .async_session_allowed(true)
            .metadata_footer_only_intent(true)
            .scan_execution_risk(true)
            .decode_risk(true)
            .materialization_risk(true)
            .arrow_default_risk(true)
            .write_risk(true),
        )
        .unwrap();
        let invocation = VortexMetadataAsyncInvocationReport {
            status: crate::VortexMetadataAsyncInvocationStatus::BlockedByUnsupportedApiSurface,
            boundary_report: boundary,
            effects_performed: Vec::new(),
            metadata_summary: None,
            footer_summary: None,
            diagnostics: Vec::new(),
        };
        let req = query_primitive_request_from_metadata_async_invocation(
            uri(),
            VortexQueryPrimitiveKind::Count,
            &invocation,
        );
        assert!(req.has_signal(VortexQueryPrimitiveSignal::FeatureGateEnabled));
        assert!(req.has_signal(VortexQueryPrimitiveSignal::ScanExecutionRisk));
        assert!(req.has_signal(VortexQueryPrimitiveSignal::DecodeRisk));
        assert!(req.has_signal(VortexQueryPrimitiveSignal::MaterializationRisk));
        assert!(req.has_signal(VortexQueryPrimitiveSignal::ArrowDefaultRisk));
        assert!(req.has_signal(VortexQueryPrimitiveSignal::WriteRisk));
        assert!(req.has_signal(VortexQueryPrimitiveSignal::FallbackPolicyBlocked));
    }
}
