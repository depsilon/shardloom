use std::fmt::Write as _;

use shardloom_core::{
    DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, Result, ShardLoomError,
};

use crate::{
    VortexQueryPrimitiveBoundaryKind, VortexQueryPrimitiveBoundaryStatus,
    VortexQueryPrimitiveReport, VortexQueryPrimitiveSignal,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCountCandidateSource {
    MetadataFooter,
    EncodedDataPath,
    Unknown,
}
impl VortexCountCandidateSource {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataFooter => "metadata_footer",
            Self::EncodedDataPath => "encoded_data_path",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn requires_metadata_footer(&self) -> bool {
        matches!(self, Self::MetadataFooter)
    }
    #[must_use]
    pub const fn requires_encoded_data_path(&self) -> bool {
        matches!(self, Self::EncodedDataPath)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCountReadinessStatus {
    FeatureDisabled,
    Planned,
    CountReady,
    BlockedByMissingMetadataFooter,
    BlockedByMissingEncodedDataPath,
    BlockedByUnsupportedPrimitive,
    BlockedByFilteredCount,
    BlockedByDecodeRisk,
    BlockedByMaterializationRisk,
    BlockedByArrowDefaultRisk,
    BlockedByObjectStoreTarget,
    BlockedByWriteRisk,
    BlockedByScanExecutionRisk,
    BlockedByFallbackPolicy,
    Unsupported,
}
impl VortexCountReadinessStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::Planned => "planned",
            Self::CountReady => "count_ready",
            Self::BlockedByMissingMetadataFooter => "blocked_by_missing_metadata_footer",
            Self::BlockedByMissingEncodedDataPath => "blocked_by_missing_encoded_data_path",
            Self::BlockedByUnsupportedPrimitive => "blocked_by_unsupported_primitive",
            Self::BlockedByFilteredCount => "blocked_by_filtered_count",
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
            Self::FeatureDisabled | Self::Planned | Self::CountReady
        )
    }
    #[must_use]
    pub const fn count_ready(&self) -> bool {
        matches!(self, Self::CountReady)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexCountReadinessMode {
    ReportOnly,
    MetadataCountPlanning,
    EncodedCountPlanning,
    Unsupported,
}
impl VortexCountReadinessMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::MetadataCountPlanning => "metadata_count_planning",
            Self::EncodedCountPlanning => "encoded_count_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn executes_count(&self) -> bool {
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
pub enum VortexCountReadinessSignal {
    FeatureGateEnabled,
    QueryPrimitiveReady,
    MetadataFooterReady,
    EncodedDataPathReady,
    CountPrimitive,
    FilteredCountRequested,
    PredicateProvided,
    ObjectStoreTarget,
    DecodeRisk,
    MaterializationRisk,
    ArrowDefaultRisk,
    WriteRisk,
    ScanExecutionRisk,
    FallbackPolicyBlocked,
}
impl VortexCountReadinessSignal {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FeatureGateEnabled => "feature_gate_enabled",
            Self::QueryPrimitiveReady => "query_primitive_ready",
            Self::MetadataFooterReady => "metadata_footer_ready",
            Self::EncodedDataPathReady => "encoded_data_path_ready",
            Self::CountPrimitive => "count_primitive",
            Self::FilteredCountRequested => "filtered_count_requested",
            Self::PredicateProvided => "predicate_provided",
            Self::ObjectStoreTarget => "object_store_target",
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
            Self::FilteredCountRequested
                | Self::ObjectStoreTarget
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
pub enum VortexCountReadinessEffect {
    CountExecuted,
    MetadataRead,
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
impl VortexCountReadinessEffect {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::CountExecuted => "count_executed",
            Self::MetadataRead => "metadata_read",
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
pub struct VortexCountReadinessRequest {
    pub target_uri: DatasetUri,
    pub candidate_source: VortexCountCandidateSource,
    pub signals: Vec<VortexCountReadinessSignal>,
    pub expected_count_summary: Option<String>,
    pub upstream_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexCountReadinessRequest {
    #[must_use]
    pub fn new(target_uri: DatasetUri, candidate_source: VortexCountCandidateSource) -> Self {
        Self {
            target_uri,
            candidate_source,
            signals: vec![],
            expected_count_summary: None,
            upstream_summary: None,
            diagnostics: vec![],
        }
    }
    fn set_signal(mut self, s: VortexCountReadinessSignal, v: bool) -> Self {
        if v {
            self.add_signal(s);
        } else {
            self.signals.retain(|x| *x != s);
        }
        self
    }
    pub fn add_signal(&mut self, s: VortexCountReadinessSignal) {
        if !self.signals.contains(&s) {
            self.signals.push(s);
        }
    }
    #[must_use]
    pub fn has_signal(&self, s: VortexCountReadinessSignal) -> bool {
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
            "{}:{}",
            self.candidate_source.as_str(),
            self.target_uri.as_str()
        )
    }
}
macro_rules! b {
    ($n:ident,$s:ident) => {
        #[must_use]
        pub fn $n(self, v: bool) -> Self {
            self.set_signal(VortexCountReadinessSignal::$s, v)
        }
    };
}
impl VortexCountReadinessRequest {
    b!(feature_gate_enabled, FeatureGateEnabled);
    b!(query_primitive_ready, QueryPrimitiveReady);
    b!(metadata_footer_ready, MetadataFooterReady);
    b!(encoded_data_path_ready, EncodedDataPathReady);
    b!(count_primitive, CountPrimitive);
    b!(filtered_count_requested, FilteredCountRequested);
    b!(predicate_provided, PredicateProvided);
    b!(object_store_target, ObjectStoreTarget);
    b!(decode_risk, DecodeRisk);
    b!(materialization_risk, MaterializationRisk);
    b!(arrow_default_risk, ArrowDefaultRisk);
    b!(write_risk, WriteRisk);
    b!(scan_execution_risk, ScanExecutionRisk);
    b!(fallback_policy_blocked, FallbackPolicyBlocked);
    #[must_use]
    pub fn with_expected_count_summary(mut self, s: impl Into<String>) -> Self {
        self.expected_count_summary = Some(s.into());
        self
    }
    #[must_use]
    pub fn with_upstream_summary(mut self, s: impl Into<String>) -> Self {
        self.upstream_summary = Some(s.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexCountReadinessReport {
    pub status: VortexCountReadinessStatus,
    pub mode: VortexCountReadinessMode,
    pub request: VortexCountReadinessRequest,
    pub effects_performed: Vec<VortexCountReadinessEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexCountReadinessReport {
    /// # Errors
    /// Returns an error if report generation cannot construct diagnostics.
    pub fn from_request(request: VortexCountReadinessRequest) -> Result<Self> {
        let status = derive_status(&request);
        let mode = derive_mode(&request, status);
        Ok(Self {
            status,
            mode,
            request,
            effects_performed: vec![],
            diagnostics: vec![],
        })
    }
    #[must_use]
    pub fn unsupported(request: VortexCountReadinessRequest, feature: &str, reason: &str) -> Self {
        let mut diagnostics = request.diagnostics.clone();
        diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            format!("unsupported {feature}"),
            reason.to_string(),
            Some("count readiness remains report-only".to_string()),
        ));
        Self {
            status: VortexCountReadinessStatus::Unsupported,
            mode: VortexCountReadinessMode::Unsupported,
            request,
            effects_performed: vec![],
            diagnostics,
        }
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
    pub const fn count_ready(&self) -> bool {
        self.status.count_ready()
    }
    #[must_use]
    pub fn feature_gate_enabled(&self) -> bool {
        self.request
            .has_signal(VortexCountReadinessSignal::FeatureGateEnabled)
    }
    #[must_use]
    pub fn query_primitive_ready(&self) -> bool {
        self.request
            .has_signal(VortexCountReadinessSignal::QueryPrimitiveReady)
    }
    #[must_use]
    pub fn metadata_footer_ready(&self) -> bool {
        self.request
            .has_signal(VortexCountReadinessSignal::MetadataFooterReady)
    }
    #[must_use]
    pub fn encoded_data_path_ready(&self) -> bool {
        self.request
            .has_signal(VortexCountReadinessSignal::EncodedDataPathReady)
    }
    #[must_use]
    pub fn object_store_target(&self) -> bool {
        self.request
            .has_signal(VortexCountReadinessSignal::ObjectStoreTarget)
    }
    #[must_use]
    pub fn decode_risk(&self) -> bool {
        self.request
            .has_signal(VortexCountReadinessSignal::DecodeRisk)
    }
    #[must_use]
    pub fn materialization_risk(&self) -> bool {
        self.request
            .has_signal(VortexCountReadinessSignal::MaterializationRisk)
    }
    #[must_use]
    pub fn arrow_default_risk(&self) -> bool {
        self.request
            .has_signal(VortexCountReadinessSignal::ArrowDefaultRisk)
    }
    #[must_use]
    pub fn write_risk(&self) -> bool {
        self.request
            .has_signal(VortexCountReadinessSignal::WriteRisk)
    }
    #[must_use]
    pub fn scan_execution_risk(&self) -> bool {
        self.request
            .has_signal(VortexCountReadinessSignal::ScanExecutionRisk)
    }
    #[must_use]
    pub fn count_executed(&self) -> bool {
        self.effects_performed
            .contains(&VortexCountReadinessEffect::CountExecuted)
    }
    #[must_use]
    pub fn metadata_read(&self) -> bool {
        self.effects_performed
            .contains(&VortexCountReadinessEffect::MetadataRead)
    }
    #[must_use]
    pub fn encoded_data_read(&self) -> bool {
        self.effects_performed
            .contains(&VortexCountReadinessEffect::EncodedDataRead)
    }
    #[must_use]
    pub fn row_read(&self) -> bool {
        self.effects_performed
            .contains(&VortexCountReadinessEffect::RowRead)
    }
    #[must_use]
    pub fn array_decoded(&self) -> bool {
        self.effects_performed
            .contains(&VortexCountReadinessEffect::ArrayDecoded)
    }
    #[must_use]
    pub fn values_materialized(&self) -> bool {
        self.effects_performed
            .contains(&VortexCountReadinessEffect::ValuesMaterialized)
    }
    #[must_use]
    pub fn arrow_converted(&self) -> bool {
        self.effects_performed
            .contains(&VortexCountReadinessEffect::ArrowConverted)
    }
    #[must_use]
    pub fn object_store_io(&self) -> bool {
        self.effects_performed
            .contains(&VortexCountReadinessEffect::ObjectStoreIo)
    }
    #[must_use]
    pub fn data_written(&self) -> bool {
        self.effects_performed
            .contains(&VortexCountReadinessEffect::DataWritten)
    }
    #[must_use]
    pub fn upstream_scan_called(&self) -> bool {
        self.effects_performed
            .contains(&VortexCountReadinessEffect::UpstreamScanCalled)
    }
    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        !self.count_executed()
            && !self.metadata_read()
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
        let mut t = String::new();
        let _ = writeln!(&mut t, "status={}", self.status.as_str());
        let _ = writeln!(&mut t, "mode={}", self.mode.as_str());
        let _ = writeln!(&mut t, "count_executed={}", self.count_executed());
        let _ = writeln!(
            &mut t,
            "fallback_execution_allowed={}",
            self.fallback_execution_allowed()
        );
        t
    }
}
fn derive_status(r: &VortexCountReadinessRequest) -> VortexCountReadinessStatus {
    use VortexCountReadinessSignal as S;
    if r.has_signal(S::ObjectStoreTarget) {
        return VortexCountReadinessStatus::BlockedByObjectStoreTarget;
    }
    if r.has_signal(S::ScanExecutionRisk) {
        return VortexCountReadinessStatus::BlockedByScanExecutionRisk;
    }
    if r.has_signal(S::DecodeRisk) {
        return VortexCountReadinessStatus::BlockedByDecodeRisk;
    }
    if r.has_signal(S::MaterializationRisk) {
        return VortexCountReadinessStatus::BlockedByMaterializationRisk;
    }
    if r.has_signal(S::ArrowDefaultRisk) {
        return VortexCountReadinessStatus::BlockedByArrowDefaultRisk;
    }
    if r.has_signal(S::WriteRisk) {
        return VortexCountReadinessStatus::BlockedByWriteRisk;
    }
    if r.has_signal(S::FallbackPolicyBlocked) {
        return VortexCountReadinessStatus::BlockedByFallbackPolicy;
    }
    if !r.has_signal(S::FeatureGateEnabled) {
        return VortexCountReadinessStatus::FeatureDisabled;
    }
    if r.has_signal(S::FilteredCountRequested) {
        return VortexCountReadinessStatus::BlockedByFilteredCount;
    }
    if !r.has_signal(S::CountPrimitive) || !r.has_signal(S::QueryPrimitiveReady) {
        return VortexCountReadinessStatus::BlockedByUnsupportedPrimitive;
    }
    if r.candidate_source.requires_metadata_footer() && !r.has_signal(S::MetadataFooterReady) {
        return VortexCountReadinessStatus::BlockedByMissingMetadataFooter;
    }
    if r.candidate_source.requires_encoded_data_path() && !r.has_signal(S::EncodedDataPathReady) {
        return VortexCountReadinessStatus::BlockedByMissingEncodedDataPath;
    }
    if matches!(r.candidate_source, VortexCountCandidateSource::Unknown) {
        return VortexCountReadinessStatus::BlockedByUnsupportedPrimitive;
    }
    VortexCountReadinessStatus::CountReady
}
fn derive_mode(
    r: &VortexCountReadinessRequest,
    s: VortexCountReadinessStatus,
) -> VortexCountReadinessMode {
    if matches!(s, VortexCountReadinessStatus::Unsupported) {
        return VortexCountReadinessMode::Unsupported;
    }
    match r.candidate_source {
        VortexCountCandidateSource::MetadataFooter => {
            VortexCountReadinessMode::MetadataCountPlanning
        }
        VortexCountCandidateSource::EncodedDataPath => {
            VortexCountReadinessMode::EncodedCountPlanning
        }
        VortexCountCandidateSource::Unknown => VortexCountReadinessMode::ReportOnly,
    }
}

#[must_use]
pub fn count_readiness_request_from_query_primitive_report(
    target_uri: DatasetUri,
    query_report: &VortexQueryPrimitiveReport,
) -> VortexCountReadinessRequest {
    let candidate_source = if query_report
        .request
        .primitive
        .can_be_metadata_only_candidate()
        && query_report.metadata_footer_ready()
    {
        VortexCountCandidateSource::MetadataFooter
    } else if query_report.encoded_data_path_ready() {
        VortexCountCandidateSource::EncodedDataPath
    } else {
        VortexCountCandidateSource::Unknown
    };
    let mut req = VortexCountReadinessRequest::new(target_uri, candidate_source)
        .with_upstream_summary(query_report.to_human_text());
    req = req
        .feature_gate_enabled(query_report.feature_gate_enabled())
        .query_primitive_ready(query_report.primitive_ready())
        .metadata_footer_ready(query_report.metadata_footer_ready())
        .encoded_data_path_ready(query_report.encoded_data_path_ready())
        .object_store_target(query_report.object_store_target())
        .decode_risk(query_report.decode_risk())
        .materialization_risk(query_report.materialization_risk())
        .arrow_default_risk(query_report.arrow_default_risk())
        .write_risk(query_report.write_risk())
        .scan_execution_risk(query_report.scan_execution_risk());
    req = req
        .count_primitive(query_report.request.primitive == VortexQueryPrimitiveBoundaryKind::Count);
    req = req.filtered_count_requested(
        query_report.request.primitive == VortexQueryPrimitiveBoundaryKind::FilteredCount,
    );
    req = req.predicate_provided(
        query_report
            .request
            .has_signal(VortexQueryPrimitiveSignal::PredicateProvided),
    );
    req.fallback_policy_blocked(
        query_report.fallback_execution_allowed()
            || query_report.status == VortexQueryPrimitiveBoundaryStatus::BlockedByFallbackPolicy,
    )
}

/// # Errors
/// Returns any deterministic planning failure while building `VortexCountReadinessReport`.
pub fn plan_vortex_count_readiness(
    request: VortexCountReadinessRequest,
) -> Result<VortexCountReadinessReport> {
    VortexCountReadinessReport::from_request(request).map_err(|e| {
        ShardLoomError::NotImplemented(format!("count readiness planning failed: {e}"))
    })
}
#[must_use]
pub fn vortex_count_readiness_is_side_effect_free(report: &VortexCountReadinessReport) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        VortexQueryPrimitiveBoundaryKind, VortexQueryPrimitiveBoundaryRequest,
        plan_vortex_query_primitive,
    };
    fn uri() -> DatasetUri {
        DatasetUri::new("file://tmp/c.vortex").expect("uri")
    }
    #[test]
    fn source_labels_and_requirements_are_stable() {
        assert_eq!(
            VortexCountCandidateSource::MetadataFooter.as_str(),
            "metadata_footer"
        );
        assert!(VortexCountCandidateSource::MetadataFooter.requires_metadata_footer());
        assert!(VortexCountCandidateSource::EncodedDataPath.requires_encoded_data_path());
    }
    #[test]
    fn readiness_paths_statuses() {
        let r = plan_vortex_count_readiness(VortexCountReadinessRequest::new(
            uri(),
            VortexCountCandidateSource::Unknown,
        ))
        .expect("ok");
        assert_eq!(r.status, VortexCountReadinessStatus::FeatureDisabled);
        let r = plan_vortex_count_readiness(
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::MetadataFooter)
                .feature_gate_enabled(true)
                .query_primitive_ready(true),
        )
        .expect("ok");
        assert_eq!(
            r.status,
            VortexCountReadinessStatus::BlockedByUnsupportedPrimitive
        );
        let r = plan_vortex_count_readiness(
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::Unknown)
                .feature_gate_enabled(true)
                .query_primitive_ready(true)
                .filtered_count_requested(true),
        )
        .expect("ok");
        assert_eq!(r.status, VortexCountReadinessStatus::BlockedByFilteredCount);
        let r = plan_vortex_count_readiness(
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::MetadataFooter)
                .feature_gate_enabled(true)
                .query_primitive_ready(true)
                .count_primitive(true),
        )
        .expect("ok");
        assert_eq!(
            r.status,
            VortexCountReadinessStatus::BlockedByMissingMetadataFooter
        );
        let r = plan_vortex_count_readiness(
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::EncodedDataPath)
                .feature_gate_enabled(true)
                .query_primitive_ready(true)
                .count_primitive(true),
        )
        .expect("ok");
        assert_eq!(
            r.status,
            VortexCountReadinessStatus::BlockedByMissingEncodedDataPath
        );
        for blocked in [
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::Unknown)
                .object_store_target(true),
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::Unknown)
                .decode_risk(true),
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::Unknown)
                .materialization_risk(true),
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::Unknown)
                .arrow_default_risk(true),
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::Unknown)
                .write_risk(true),
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::Unknown)
                .scan_execution_risk(true),
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::Unknown)
                .fallback_policy_blocked(true),
        ] {
            assert!(
                plan_vortex_count_readiness(blocked)
                    .expect("ok")
                    .status
                    .is_error()
            );
        }
        let ready_meta = plan_vortex_count_readiness(
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::MetadataFooter)
                .feature_gate_enabled(true)
                .query_primitive_ready(true)
                .count_primitive(true)
                .metadata_footer_ready(true),
        )
        .expect("ok");
        assert_eq!(ready_meta.status, VortexCountReadinessStatus::CountReady);
        let ready_enc = plan_vortex_count_readiness(
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::EncodedDataPath)
                .feature_gate_enabled(true)
                .query_primitive_ready(true)
                .count_primitive(true)
                .encoded_data_path_ready(true),
        )
        .expect("ok");
        assert_eq!(ready_enc.status, VortexCountReadinessStatus::CountReady);
        assert!(!ready_enc.count_executed());
        assert!(!ready_enc.encoded_data_read());
        assert!(!ready_enc.row_read());
        assert!(!ready_enc.array_decoded());
        assert!(!ready_enc.values_materialized());
        assert!(!ready_enc.arrow_converted());
        assert!(!ready_enc.object_store_io());
        assert!(!ready_enc.data_written());
        assert!(!ready_enc.upstream_scan_called());
        assert_eq!(ready_enc.status, VortexCountReadinessStatus::CountReady);
    }

    #[test]
    fn effects_and_text_remain_report_only() {
        let ready_enc = plan_vortex_count_readiness(
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::EncodedDataPath)
                .feature_gate_enabled(true)
                .query_primitive_ready(true)
                .count_primitive(true)
                .encoded_data_path_ready(true),
        )
        .expect("ok");
        assert!(!ready_enc.count_executed());
        assert!(!ready_enc.encoded_data_read());
        assert!(!ready_enc.row_read());
        assert!(!ready_enc.array_decoded());
        assert!(!ready_enc.values_materialized());
        assert!(!ready_enc.arrow_converted());
        assert!(!ready_enc.object_store_io());
        assert!(!ready_enc.data_written());
        assert!(!ready_enc.upstream_scan_called());
        assert!(!ready_enc.fallback_execution_allowed());
        assert!(ready_enc.is_side_effect_free());
        assert!(ready_enc.to_human_text().contains("count_executed=false"));
        assert!(
            ready_enc
                .to_human_text()
                .contains("fallback_execution_allowed=false")
        );
    }
    #[test]
    fn helper_from_query_report_preserves_blockers() {
        let q = plan_vortex_query_primitive(
            VortexQueryPrimitiveBoundaryRequest::new(
                uri(),
                VortexQueryPrimitiveBoundaryKind::Count,
            )
            .feature_gate_enabled(true)
            .object_store_target(true),
        )
        .expect("q");
        let req = count_readiness_request_from_query_primitive_report(uri(), &q);
        let rep = plan_vortex_count_readiness(req).expect("rep");
        assert_eq!(
            rep.status,
            VortexCountReadinessStatus::BlockedByObjectStoreTarget
        );
    }
    #[test]
    fn report_has_errors_includes_request_and_report_diagnostics() {
        let req =
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::MetadataFooter)
                .feature_gate_enabled(true)
                .query_primitive_ready(true)
                .count_primitive(true)
                .metadata_footer_ready(true);
        let mut report = plan_vortex_count_readiness(req).expect("report");
        assert_eq!(report.status, VortexCountReadinessStatus::CountReady);
        assert!(!report.has_errors());

        report.request.add_diagnostic(Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Warning,
            shardloom_core::DiagnosticCategory::UnsupportedFeature,
            "warning diagnostic",
            None,
            None,
            None,
            shardloom_core::FallbackStatus::disabled_by_policy(),
        ));
        assert!(!report.has_errors());

        report
            .request
            .add_diagnostic(Diagnostic::no_fallback_execution("error diagnostic"));
        assert!(report.has_errors());

        let req =
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::MetadataFooter)
                .feature_gate_enabled(true)
                .query_primitive_ready(true)
                .count_primitive(true)
                .metadata_footer_ready(true);
        let mut report = plan_vortex_count_readiness(req).expect("report");
        report
            .diagnostics
            .push(Diagnostic::no_fallback_execution("fatal diagnostic"));
        assert!(report.has_errors());
    }

    #[test]
    fn unknown_candidate_source_is_never_count_ready() {
        let report = plan_vortex_count_readiness(
            VortexCountReadinessRequest::new(uri(), VortexCountCandidateSource::Unknown)
                .feature_gate_enabled(true)
                .query_primitive_ready(true)
                .count_primitive(true),
        )
        .expect("report");
        assert_eq!(
            report.status,
            VortexCountReadinessStatus::BlockedByUnsupportedPrimitive
        );
    }

    #[test]
    fn helper_unknown_candidate_from_non_ready_report_blocks_deterministically() {
        let query_report = plan_vortex_query_primitive(VortexQueryPrimitiveBoundaryRequest::new(
            uri(),
            VortexQueryPrimitiveBoundaryKind::Projection,
        ))
        .expect("query report");
        let request = count_readiness_request_from_query_primitive_report(uri(), &query_report);
        assert_eq!(
            request.candidate_source,
            VortexCountCandidateSource::Unknown
        );
        let readiness = plan_vortex_count_readiness(request).expect("count report");
        assert_eq!(
            readiness.status,
            VortexCountReadinessStatus::FeatureDisabled
        );

        let query_report = plan_vortex_query_primitive(
            VortexQueryPrimitiveBoundaryRequest::new(
                uri(),
                VortexQueryPrimitiveBoundaryKind::Projection,
            )
            .feature_gate_enabled(true),
        )
        .expect("query report");
        let request = count_readiness_request_from_query_primitive_report(uri(), &query_report);
        assert_eq!(
            request.candidate_source,
            VortexCountCandidateSource::Unknown
        );
        let readiness = plan_vortex_count_readiness(request).expect("count report");
        assert_eq!(
            readiness.status,
            VortexCountReadinessStatus::BlockedByUnsupportedPrimitive
        );
    }

    #[test]
    fn helper_from_ready_count_sets_signals() {
        let q = plan_vortex_query_primitive(
            VortexQueryPrimitiveBoundaryRequest::new(
                uri(),
                VortexQueryPrimitiveBoundaryKind::Count,
            )
            .feature_gate_enabled(true)
            .metadata_footer_ready(true)
            .encoded_data_path_ready(true),
        )
        .expect("q");
        let req = count_readiness_request_from_query_primitive_report(uri(), &q);
        assert!(req.has_signal(VortexCountReadinessSignal::QueryPrimitiveReady));
        assert!(req.has_signal(VortexCountReadinessSignal::CountPrimitive));
        assert!(req.expected_count_summary.is_none());
    }
}
