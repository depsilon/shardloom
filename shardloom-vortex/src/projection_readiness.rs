use std::fmt::Write as _;

use shardloom_core::{DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, Result};

use crate::{
    VortexQueryPrimitiveBoundaryKind, VortexQueryPrimitiveBoundaryStatus,
    VortexQueryPrimitiveReport, VortexQueryPrimitiveSignal,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexProjectionCandidateSource {
    MetadataSchemaProjection,
    EncodedColumnPath,
    Unknown,
}
impl VortexProjectionCandidateSource {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataSchemaProjection => "metadata_schema_projection",
            Self::EncodedColumnPath => "encoded_column_path",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn requires_metadata_footer(&self) -> bool {
        matches!(self, Self::MetadataSchemaProjection)
    }
    #[must_use]
    pub const fn requires_encoded_data_path(&self) -> bool {
        matches!(self, Self::EncodedColumnPath)
    }
    #[must_use]
    pub const fn requires_projection(&self) -> bool {
        !matches!(self, Self::Unknown)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexProjectionReadinessStatus {
    FeatureDisabled,
    Planned,
    ProjectionReady,
    BlockedByMissingMetadataFooter,
    BlockedByMissingEncodedDataPath,
    BlockedByMissingProjection,
    BlockedByUnsupportedProjection,
    BlockedByUnsupportedPrimitive,
    BlockedByDecodeRisk,
    BlockedByMaterializationRisk,
    BlockedByArrowDefaultRisk,
    BlockedByObjectStoreTarget,
    BlockedByWriteRisk,
    BlockedByScanExecutionRisk,
    BlockedByFallbackPolicy,
    Unsupported,
}
impl VortexProjectionReadinessStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::Planned => "planned",
            Self::ProjectionReady => "projection_ready",
            Self::BlockedByMissingMetadataFooter => "blocked_by_missing_metadata_footer",
            Self::BlockedByMissingEncodedDataPath => "blocked_by_missing_encoded_data_path",
            Self::BlockedByMissingProjection => "blocked_by_missing_projection",
            Self::BlockedByUnsupportedProjection => "blocked_by_unsupported_projection",
            Self::BlockedByUnsupportedPrimitive => "blocked_by_unsupported_primitive",
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
            Self::FeatureDisabled | Self::Planned | Self::ProjectionReady
        )
    }
    #[must_use]
    pub const fn projection_ready(&self) -> bool {
        matches!(self, Self::ProjectionReady)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexProjectionReadinessMode {
    ReportOnly,
    MetadataSchemaProjectionPlanning,
    EncodedColumnProjectionPlanning,
    Unsupported,
}
impl VortexProjectionReadinessMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::MetadataSchemaProjectionPlanning => "metadata_schema_projection_planning",
            Self::EncodedColumnProjectionPlanning => "encoded_column_projection_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn executes_projection(&self) -> bool {
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
pub enum VortexProjectionReadinessSignal {
    FeatureGateEnabled,
    QueryPrimitiveReady,
    MetadataFooterReady,
    EncodedDataPathReady,
    ProjectionPrimitive,
    ProjectionProvided,
    ProjectionSupported,
    ProjectionUnsupported,
    ObjectStoreTarget,
    DecodeRisk,
    MaterializationRisk,
    ArrowDefaultRisk,
    WriteRisk,
    ScanExecutionRisk,
    FallbackPolicyBlocked,
}
impl VortexProjectionReadinessSignal {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FeatureGateEnabled => "feature_gate_enabled",
            Self::QueryPrimitiveReady => "query_primitive_ready",
            Self::MetadataFooterReady => "metadata_footer_ready",
            Self::EncodedDataPathReady => "encoded_data_path_ready",
            Self::ProjectionPrimitive => "projection_primitive",
            Self::ProjectionProvided => "projection_provided",
            Self::ProjectionSupported => "projection_supported",
            Self::ProjectionUnsupported => "projection_unsupported",
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
            Self::ProjectionUnsupported
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
pub enum VortexProjectionReadinessEffect {
    ProjectionExecuted,
    ProjectionApplied,
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
impl VortexProjectionReadinessEffect {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        ""
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexProjectionReadinessRequest {
    pub target_uri: DatasetUri,
    pub candidate_source: VortexProjectionCandidateSource,
    pub signals: Vec<VortexProjectionReadinessSignal>,
    pub projection_summary: Option<String>,
    pub upstream_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexProjectionReadinessRequest {
    #[must_use]
    pub fn new(target_uri: DatasetUri, candidate_source: VortexProjectionCandidateSource) -> Self {
        Self {
            target_uri,
            candidate_source,
            signals: vec![],
            projection_summary: None,
            upstream_summary: None,
            diagnostics: vec![],
        }
    }
    fn set_signal(mut self, signal: VortexProjectionReadinessSignal, value: bool) -> Self {
        if value {
            self.add_signal(signal);
        } else {
            self.signals.retain(|x| *x != signal);
        }
        self
    }
    #[must_use]
    pub fn feature_gate_enabled(self, v: bool) -> Self {
        self.set_signal(VortexProjectionReadinessSignal::FeatureGateEnabled, v)
    }
    #[must_use]
    pub fn query_primitive_ready(self, v: bool) -> Self {
        self.set_signal(VortexProjectionReadinessSignal::QueryPrimitiveReady, v)
    }
    #[must_use]
    pub fn metadata_footer_ready(self, v: bool) -> Self {
        self.set_signal(VortexProjectionReadinessSignal::MetadataFooterReady, v)
    }
    #[must_use]
    pub fn encoded_data_path_ready(self, v: bool) -> Self {
        self.set_signal(VortexProjectionReadinessSignal::EncodedDataPathReady, v)
    }
    #[must_use]
    pub fn projection_primitive(self, v: bool) -> Self {
        self.set_signal(VortexProjectionReadinessSignal::ProjectionPrimitive, v)
    }
    #[must_use]
    pub fn projection_provided(self, v: bool) -> Self {
        self.set_signal(VortexProjectionReadinessSignal::ProjectionProvided, v)
    }
    #[must_use]
    pub fn projection_supported(self, v: bool) -> Self {
        self.set_signal(VortexProjectionReadinessSignal::ProjectionSupported, v)
    }
    #[must_use]
    pub fn projection_unsupported(self, v: bool) -> Self {
        self.set_signal(VortexProjectionReadinessSignal::ProjectionUnsupported, v)
    }
    #[must_use]
    pub fn object_store_target(self, v: bool) -> Self {
        self.set_signal(VortexProjectionReadinessSignal::ObjectStoreTarget, v)
    }
    #[must_use]
    pub fn decode_risk(self, v: bool) -> Self {
        self.set_signal(VortexProjectionReadinessSignal::DecodeRisk, v)
    }
    #[must_use]
    pub fn materialization_risk(self, v: bool) -> Self {
        self.set_signal(VortexProjectionReadinessSignal::MaterializationRisk, v)
    }
    #[must_use]
    pub fn arrow_default_risk(self, v: bool) -> Self {
        self.set_signal(VortexProjectionReadinessSignal::ArrowDefaultRisk, v)
    }
    #[must_use]
    pub fn write_risk(self, v: bool) -> Self {
        self.set_signal(VortexProjectionReadinessSignal::WriteRisk, v)
    }
    #[must_use]
    pub fn scan_execution_risk(self, v: bool) -> Self {
        self.set_signal(VortexProjectionReadinessSignal::ScanExecutionRisk, v)
    }
    #[must_use]
    pub fn fallback_policy_blocked(self, v: bool) -> Self {
        self.set_signal(VortexProjectionReadinessSignal::FallbackPolicyBlocked, v)
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
    pub fn add_signal(&mut self, s: VortexProjectionReadinessSignal) {
        if !self.signals.contains(&s) {
            self.signals.push(s);
        }
    }
    #[must_use]
    pub fn has_signal(&self, s: VortexProjectionReadinessSignal) -> bool {
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
            "projection_readiness_request(candidate_source={})",
            self.candidate_source.as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexProjectionReadinessReport {
    pub status: VortexProjectionReadinessStatus,
    pub mode: VortexProjectionReadinessMode,
    pub request: VortexProjectionReadinessRequest,
    pub effects_performed: Vec<VortexProjectionReadinessEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexProjectionReadinessReport {
    /// # Errors
    /// Returns an error when request has fatal diagnostics.
    pub fn from_request(request: VortexProjectionReadinessRequest) -> Result<Self> {
        if request
            .diagnostics
            .iter()
            .any(|d| matches!(d.severity, DiagnosticSeverity::Fatal))
        {
            return Err(shardloom_core::ShardLoomError::new(
                "fatal diagnostic present in projection readiness request",
            ));
        }
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
    pub fn unsupported(
        request: VortexProjectionReadinessRequest,
        feature: &str,
        reason: &str,
    ) -> Self {
        let mut diagnostics = request.diagnostics.clone();
        diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            format!("unsupported {feature}"),
            reason.to_string(),
            Some("projection readiness remains report-only".to_string()),
        ));
        Self {
            status: VortexProjectionReadinessStatus::Unsupported,
            mode: VortexProjectionReadinessMode::Unsupported,
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
    pub const fn projection_ready(&self) -> bool {
        self.status.projection_ready()
    }
    fn effect(&self, e: VortexProjectionReadinessEffect) -> bool {
        self.effects_performed.contains(&e)
    }
    #[must_use]
    pub fn projection_executed(&self) -> bool {
        self.effect(VortexProjectionReadinessEffect::ProjectionExecuted)
    }
    #[must_use]
    pub fn projection_applied(&self) -> bool {
        self.effect(VortexProjectionReadinessEffect::ProjectionApplied)
    }
    #[must_use]
    pub fn fallback_execution_allowed(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        !self.projection_executed()
            && !self.projection_applied()
            && !self.fallback_execution_allowed()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(&mut out, "status: {}", self.status.as_str());
        let _ = writeln!(
            &mut out,
            "projection_executed: {}",
            self.projection_executed()
        );
        let _ = writeln!(
            &mut out,
            "fallback_execution_allowed: {}",
            self.fallback_execution_allowed()
        );
        out
    }
}

fn derive_status(r: &VortexProjectionReadinessRequest) -> VortexProjectionReadinessStatus {
    use VortexProjectionReadinessSignal as S;
    if r.has_signal(S::ObjectStoreTarget) {
        return VortexProjectionReadinessStatus::BlockedByObjectStoreTarget;
    }
    if r.has_signal(S::ScanExecutionRisk) {
        return VortexProjectionReadinessStatus::BlockedByScanExecutionRisk;
    }
    if r.has_signal(S::DecodeRisk) {
        return VortexProjectionReadinessStatus::BlockedByDecodeRisk;
    }
    if r.has_signal(S::MaterializationRisk) {
        return VortexProjectionReadinessStatus::BlockedByMaterializationRisk;
    }
    if r.has_signal(S::ArrowDefaultRisk) {
        return VortexProjectionReadinessStatus::BlockedByArrowDefaultRisk;
    }
    if r.has_signal(S::WriteRisk) {
        return VortexProjectionReadinessStatus::BlockedByWriteRisk;
    }
    if r.has_signal(S::FallbackPolicyBlocked) {
        return VortexProjectionReadinessStatus::BlockedByFallbackPolicy;
    }
    if !r.has_signal(S::FeatureGateEnabled) {
        return VortexProjectionReadinessStatus::FeatureDisabled;
    }
    if !r.has_signal(S::ProjectionPrimitive) {
        return VortexProjectionReadinessStatus::BlockedByUnsupportedPrimitive;
    }
    if r.has_signal(S::ProjectionUnsupported) {
        return VortexProjectionReadinessStatus::BlockedByUnsupportedProjection;
    }
    if !r.has_signal(S::ProjectionProvided) {
        return VortexProjectionReadinessStatus::BlockedByMissingProjection;
    }
    match r.candidate_source {
        VortexProjectionCandidateSource::MetadataSchemaProjection => {
            if !r.has_signal(S::MetadataFooterReady) {
                return VortexProjectionReadinessStatus::BlockedByMissingMetadataFooter;
            }
            if !r.has_signal(S::ProjectionSupported) {
                return VortexProjectionReadinessStatus::BlockedByUnsupportedProjection;
            }
        }
        VortexProjectionCandidateSource::EncodedColumnPath => {
            if !r.has_signal(S::EncodedDataPathReady) {
                return VortexProjectionReadinessStatus::BlockedByMissingEncodedDataPath;
            }
        }
        VortexProjectionCandidateSource::Unknown => {
            return VortexProjectionReadinessStatus::BlockedByUnsupportedPrimitive;
        }
    }
    if !r.has_signal(S::QueryPrimitiveReady) {
        return VortexProjectionReadinessStatus::BlockedByUnsupportedPrimitive;
    }
    VortexProjectionReadinessStatus::ProjectionReady
}
fn derive_mode(
    r: &VortexProjectionReadinessRequest,
    s: VortexProjectionReadinessStatus,
) -> VortexProjectionReadinessMode {
    match r.candidate_source {
        VortexProjectionCandidateSource::MetadataSchemaProjection => {
            VortexProjectionReadinessMode::MetadataSchemaProjectionPlanning
        }
        VortexProjectionCandidateSource::EncodedColumnPath => {
            VortexProjectionReadinessMode::EncodedColumnProjectionPlanning
        }
        VortexProjectionCandidateSource::Unknown => {
            if matches!(s, VortexProjectionReadinessStatus::Unsupported) {
                VortexProjectionReadinessMode::Unsupported
            } else {
                VortexProjectionReadinessMode::ReportOnly
            }
        }
    }
}

#[must_use]
pub fn projection_readiness_request_from_query_primitive_report(
    target_uri: DatasetUri,
    query_report: &VortexQueryPrimitiveReport,
) -> VortexProjectionReadinessRequest {
    let projection_primitive =
        query_report.request.primitive == VortexQueryPrimitiveBoundaryKind::Projection;
    let projection_provided = query_report
        .request
        .has_signal(VortexQueryPrimitiveSignal::ProjectionProvided);
    let candidate_source = if projection_primitive && projection_provided {
        VortexProjectionCandidateSource::EncodedColumnPath
    } else {
        VortexProjectionCandidateSource::Unknown
    };
    VortexProjectionReadinessRequest::new(target_uri, candidate_source)
        .feature_gate_enabled(query_report.feature_gate_enabled())
        .query_primitive_ready(query_report.primitive_ready())
        .projection_primitive(projection_primitive)
        .projection_provided(projection_provided)
        .metadata_footer_ready(query_report.metadata_footer_ready())
        .encoded_data_path_ready(query_report.encoded_data_path_ready())
        .object_store_target(query_report.object_store_target())
        .decode_risk(query_report.decode_risk())
        .materialization_risk(query_report.materialization_risk())
        .arrow_default_risk(query_report.arrow_default_risk())
        .write_risk(query_report.write_risk())
        .scan_execution_risk(query_report.scan_execution_risk())
        .fallback_policy_blocked(
            query_report.fallback_execution_allowed()
                || query_report.status
                    == VortexQueryPrimitiveBoundaryStatus::BlockedByFallbackPolicy,
        )
        .with_upstream_summary(query_report.to_human_text())
}

/// # Errors
/// Returns an error when request has fatal diagnostics.
pub fn plan_vortex_projection_readiness(
    request: VortexProjectionReadinessRequest,
) -> Result<VortexProjectionReadinessReport> {
    VortexProjectionReadinessReport::from_request(request)
}
#[must_use]
pub fn vortex_projection_readiness_is_side_effect_free(
    report: &VortexProjectionReadinessReport,
) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uri() -> DatasetUri {
        DatasetUri::new("file:///tmp/a.vortex").expect("uri")
    }

    #[test]
    fn compiles_projection_readiness_path() {
        let report = plan_vortex_projection_readiness(
            VortexProjectionReadinessRequest::new(
                uri(),
                VortexProjectionCandidateSource::EncodedColumnPath,
            )
            .feature_gate_enabled(true)
            .query_primitive_ready(true)
            .projection_primitive(true)
            .projection_provided(true)
            .encoded_data_path_ready(true),
        )
        .expect("report");
        assert!(report.projection_ready());
        assert!(!report.projection_executed());
        assert!(!report.projection_applied());
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn helper_maps_missing_encoded_path() {
        let mut req = crate::VortexQueryPrimitiveBoundaryRequest::new(
            uri(),
            crate::VortexQueryPrimitiveBoundaryKind::Projection,
        )
        .feature_gate_enabled(true)
        .with_projection_summary("x");
        req.add_signal(crate::VortexQueryPrimitiveSignal::ProjectionProvided);
        let report = crate::plan_vortex_query_primitive(req).expect("query report");
        let p = projection_readiness_request_from_query_primitive_report(uri(), &report);
        let out = plan_vortex_projection_readiness(p).expect("projection report");
        assert_eq!(
            out.status,
            VortexProjectionReadinessStatus::BlockedByMissingEncodedDataPath
        );
    }
}
