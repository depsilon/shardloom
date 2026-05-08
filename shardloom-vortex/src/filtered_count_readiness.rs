use std::fmt::Write as _;

use shardloom_core::{
    DatasetUri, Diagnostic, DiagnosticCode, DiagnosticSeverity, Result, ShardLoomError,
};

use crate::query_primitive::evaluate_vortex_count_where_from_summary;
use crate::{
    VortexMetadataSummaryReport, VortexQueryPrimitiveBoundaryKind,
    VortexQueryPrimitiveBoundaryStatus, VortexQueryPrimitiveKind, VortexQueryPrimitiveReport,
    VortexQueryPrimitiveRequest, VortexQueryPrimitiveSignal, VortexQueryPrimitiveStatus,
    VortexQueryPrimitiveValue,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexFilteredCountCandidateSource {
    MetadataPredicateProof,
    EncodedPredicatePath,
    Unknown,
}
impl VortexFilteredCountCandidateSource {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataPredicateProof => "metadata_predicate_proof",
            Self::EncodedPredicatePath => "encoded_predicate_path",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn requires_metadata_footer(&self) -> bool {
        matches!(self, Self::MetadataPredicateProof)
    }
    #[must_use]
    pub const fn requires_encoded_data_path(&self) -> bool {
        matches!(self, Self::EncodedPredicatePath)
    }
    #[must_use]
    pub const fn requires_predicate(&self) -> bool {
        !matches!(self, Self::Unknown)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexFilteredCountReadinessStatus {
    FeatureDisabled,
    Planned,
    FilteredCountReady,
    BlockedByMissingMetadataFooter,
    BlockedByMissingEncodedDataPath,
    BlockedByMissingPredicate,
    BlockedByMissingPredicateProof,
    BlockedByUnsupportedPredicate,
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
impl VortexFilteredCountReadinessStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::Planned => "planned",
            Self::FilteredCountReady => "filtered_count_ready",
            Self::BlockedByMissingMetadataFooter => "blocked_by_missing_metadata_footer",
            Self::BlockedByMissingEncodedDataPath => "blocked_by_missing_encoded_data_path",
            Self::BlockedByMissingPredicate => "blocked_by_missing_predicate",
            Self::BlockedByMissingPredicateProof => "blocked_by_missing_predicate_proof",
            Self::BlockedByUnsupportedPredicate => "blocked_by_unsupported_predicate",
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
            Self::FeatureDisabled | Self::Planned | Self::FilteredCountReady
        )
    }
    #[must_use]
    pub const fn filtered_count_ready(&self) -> bool {
        matches!(self, Self::FilteredCountReady)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexFilteredCountReadinessMode {
    ReportOnly,
    MetadataPredicateCountPlanning,
    EncodedPredicateCountPlanning,
    Unsupported,
}
impl VortexFilteredCountReadinessMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::MetadataPredicateCountPlanning => "metadata_predicate_count_planning",
            Self::EncodedPredicateCountPlanning => "encoded_predicate_count_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn executes_filtered_count(&self) -> bool {
        false
    }
    #[must_use]
    pub const fn evaluates_predicate(&self) -> bool {
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
pub enum VortexFilteredCountReadinessSignal {
    FeatureGateEnabled,
    QueryPrimitiveReady,
    MetadataFooterReady,
    EncodedDataPathReady,
    FilteredCountPrimitive,
    PredicateProvided,
    PredicateMetadataProofReady,
    PredicateUnsupported,
    ObjectStoreTarget,
    DecodeRisk,
    MaterializationRisk,
    ArrowDefaultRisk,
    WriteRisk,
    ScanExecutionRisk,
    FallbackPolicyBlocked,
}
impl VortexFilteredCountReadinessSignal {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FeatureGateEnabled => "feature_gate_enabled",
            Self::QueryPrimitiveReady => "query_primitive_ready",
            Self::MetadataFooterReady => "metadata_footer_ready",
            Self::EncodedDataPathReady => "encoded_data_path_ready",
            Self::FilteredCountPrimitive => "filtered_count_primitive",
            Self::PredicateProvided => "predicate_provided",
            Self::PredicateMetadataProofReady => "predicate_metadata_proof_ready",
            Self::PredicateUnsupported => "predicate_unsupported",
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
            Self::PredicateUnsupported
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
pub enum VortexFilteredCountReadinessEffect {
    FilteredCountExecuted,
    PredicateEvaluated,
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
impl VortexFilteredCountReadinessEffect {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FilteredCountExecuted => "filtered_count_executed",
            Self::PredicateEvaluated => "predicate_evaluated",
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
pub struct VortexFilteredCountReadinessRequest {
    pub target_uri: DatasetUri,
    pub candidate_source: VortexFilteredCountCandidateSource,
    pub signals: Vec<VortexFilteredCountReadinessSignal>,
    pub predicate_summary: Option<String>,
    pub expected_count_summary: Option<String>,
    pub upstream_summary: Option<String>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexFilteredCountReadinessRequest {
    #[must_use]
    pub fn new(
        target_uri: DatasetUri,
        candidate_source: VortexFilteredCountCandidateSource,
    ) -> Self {
        Self {
            target_uri,
            candidate_source,
            signals: vec![],
            predicate_summary: None,
            expected_count_summary: None,
            upstream_summary: None,
            diagnostics: vec![],
        }
    }
    fn set_signal(mut self, s: VortexFilteredCountReadinessSignal, v: bool) -> Self {
        if v {
            self.add_signal(s);
        } else {
            self.signals.retain(|x| *x != s);
        }
        self
    }
    pub fn add_signal(&mut self, s: VortexFilteredCountReadinessSignal) {
        if !self.signals.contains(&s) {
            self.signals.push(s);
        }
    }
    #[must_use]
    pub fn has_signal(&self, s: VortexFilteredCountReadinessSignal) -> bool {
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
            self.set_signal(VortexFilteredCountReadinessSignal::$s, v)
        }
    };
}
impl VortexFilteredCountReadinessRequest {
    b!(feature_gate_enabled, FeatureGateEnabled);
    b!(query_primitive_ready, QueryPrimitiveReady);
    b!(metadata_footer_ready, MetadataFooterReady);
    b!(encoded_data_path_ready, EncodedDataPathReady);
    b!(filtered_count_primitive, FilteredCountPrimitive);
    b!(predicate_provided, PredicateProvided);
    b!(predicate_metadata_proof_ready, PredicateMetadataProofReady);
    b!(predicate_unsupported, PredicateUnsupported);
    b!(object_store_target, ObjectStoreTarget);
    b!(decode_risk, DecodeRisk);
    b!(materialization_risk, MaterializationRisk);
    b!(arrow_default_risk, ArrowDefaultRisk);
    b!(write_risk, WriteRisk);
    b!(scan_execution_risk, ScanExecutionRisk);
    b!(fallback_policy_blocked, FallbackPolicyBlocked);
    #[must_use]
    pub fn with_predicate_summary(mut self, s: impl Into<String>) -> Self {
        self.predicate_summary = Some(s.into());
        self
    }
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
pub struct VortexFilteredCountReadinessReport {
    pub status: VortexFilteredCountReadinessStatus,
    pub mode: VortexFilteredCountReadinessMode,
    pub request: VortexFilteredCountReadinessRequest,
    pub effects_performed: Vec<VortexFilteredCountReadinessEffect>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexFilteredCountReadinessReport {
    /// # Errors
    pub fn from_request(request: VortexFilteredCountReadinessRequest) -> Result<Self> {
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
        request: VortexFilteredCountReadinessRequest,
        feature: &str,
        reason: &str,
    ) -> Self {
        let mut diagnostics = request.diagnostics.clone();
        diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            format!("unsupported {feature}"),
            reason.to_string(),
            Some("filtered count readiness remains report-only".to_string()),
        ));
        Self {
            status: VortexFilteredCountReadinessStatus::Unsupported,
            mode: VortexFilteredCountReadinessMode::Unsupported,
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
    pub const fn filtered_count_ready(&self) -> bool {
        self.status.filtered_count_ready()
    }
    #[must_use]
    pub fn feature_gate_enabled(&self) -> bool {
        self.request
            .has_signal(VortexFilteredCountReadinessSignal::FeatureGateEnabled)
    }
    #[must_use]
    pub fn query_primitive_ready(&self) -> bool {
        self.request
            .has_signal(VortexFilteredCountReadinessSignal::QueryPrimitiveReady)
    }
    #[must_use]
    pub fn metadata_footer_ready(&self) -> bool {
        self.request
            .has_signal(VortexFilteredCountReadinessSignal::MetadataFooterReady)
    }
    #[must_use]
    pub fn encoded_data_path_ready(&self) -> bool {
        self.request
            .has_signal(VortexFilteredCountReadinessSignal::EncodedDataPathReady)
    }
    #[must_use]
    pub fn filtered_count_primitive(&self) -> bool {
        self.request
            .has_signal(VortexFilteredCountReadinessSignal::FilteredCountPrimitive)
    }
    #[must_use]
    pub fn predicate_provided(&self) -> bool {
        self.request
            .has_signal(VortexFilteredCountReadinessSignal::PredicateProvided)
    }
    #[must_use]
    pub fn predicate_metadata_proof_ready(&self) -> bool {
        self.request
            .has_signal(VortexFilteredCountReadinessSignal::PredicateMetadataProofReady)
    }
    #[must_use]
    pub fn object_store_target(&self) -> bool {
        self.request
            .has_signal(VortexFilteredCountReadinessSignal::ObjectStoreTarget)
    }
    #[must_use]
    pub fn decode_risk(&self) -> bool {
        self.request
            .has_signal(VortexFilteredCountReadinessSignal::DecodeRisk)
    }
    #[must_use]
    pub fn materialization_risk(&self) -> bool {
        self.request
            .has_signal(VortexFilteredCountReadinessSignal::MaterializationRisk)
    }
    #[must_use]
    pub fn arrow_default_risk(&self) -> bool {
        self.request
            .has_signal(VortexFilteredCountReadinessSignal::ArrowDefaultRisk)
    }
    #[must_use]
    pub fn write_risk(&self) -> bool {
        self.request
            .has_signal(VortexFilteredCountReadinessSignal::WriteRisk)
    }
    #[must_use]
    pub fn scan_execution_risk(&self) -> bool {
        self.request
            .has_signal(VortexFilteredCountReadinessSignal::ScanExecutionRisk)
    }
    fn has_effect(&self, e: VortexFilteredCountReadinessEffect) -> bool {
        self.effects_performed.contains(&e)
    }
    #[must_use]
    pub fn filtered_count_executed(&self) -> bool {
        self.has_effect(VortexFilteredCountReadinessEffect::FilteredCountExecuted)
    }
    #[must_use]
    pub fn predicate_evaluated(&self) -> bool {
        self.has_effect(VortexFilteredCountReadinessEffect::PredicateEvaluated)
    }
    #[must_use]
    pub fn metadata_read(&self) -> bool {
        self.has_effect(VortexFilteredCountReadinessEffect::MetadataRead)
    }
    #[must_use]
    pub fn encoded_data_read(&self) -> bool {
        self.has_effect(VortexFilteredCountReadinessEffect::EncodedDataRead)
    }
    #[must_use]
    pub fn row_read(&self) -> bool {
        self.has_effect(VortexFilteredCountReadinessEffect::RowRead)
    }
    #[must_use]
    pub fn array_decoded(&self) -> bool {
        self.has_effect(VortexFilteredCountReadinessEffect::ArrayDecoded)
    }
    #[must_use]
    pub fn values_materialized(&self) -> bool {
        self.has_effect(VortexFilteredCountReadinessEffect::ValuesMaterialized)
    }
    #[must_use]
    pub fn arrow_converted(&self) -> bool {
        self.has_effect(VortexFilteredCountReadinessEffect::ArrowConverted)
    }
    #[must_use]
    pub fn object_store_io(&self) -> bool {
        self.has_effect(VortexFilteredCountReadinessEffect::ObjectStoreIo)
    }
    #[must_use]
    pub fn data_written(&self) -> bool {
        self.has_effect(VortexFilteredCountReadinessEffect::DataWritten)
    }
    #[must_use]
    pub fn upstream_scan_called(&self) -> bool {
        self.has_effect(VortexFilteredCountReadinessEffect::UpstreamScanCalled)
    }
    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        !self.filtered_count_executed()
            && !self.predicate_evaluated()
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
        let mut o = String::new();
        let _ = writeln!(
            o,
            "filtered_count_readiness_status={}",
            self.status.as_str()
        );
        let _ = writeln!(o, "filtered_count_ready={}", self.filtered_count_ready());
        let _ = writeln!(
            o,
            "filtered_count_executed={}",
            self.filtered_count_executed()
        );
        let _ = writeln!(o, "predicate_evaluated={}", self.predicate_evaluated());
        let _ = writeln!(
            o,
            "fallback_execution_allowed={}",
            self.fallback_execution_allowed()
        );
        o
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexFilteredCountMetadataProofStatus {
    ProofReady,
    NeedsEncodedPredicate,
    MissingMetadata,
    Unsupported,
}
impl VortexFilteredCountMetadataProofStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ProofReady => "proof_ready",
            Self::NeedsEncodedPredicate => "needs_encoded_predicate",
            Self::MissingMetadata => "missing_metadata",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
    #[must_use]
    pub const fn proof_ready(&self) -> bool {
        matches!(self, Self::ProofReady)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexFilteredCountMetadataProofReport {
    pub schema_version: &'static str,
    pub status: VortexFilteredCountMetadataProofStatus,
    pub request: VortexQueryPrimitiveRequest,
    pub count: Option<u64>,
    pub result_known: bool,
    pub metadata_summary_supplied: bool,
    pub needs_encoded_predicate: bool,
    pub data_read: bool,
    pub row_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexFilteredCountMetadataProofReport {
    /// # Errors
    /// Returns an error only if metadata predicate proof evaluation over supplied summary fails internally.
    pub fn from_summary(
        request: VortexQueryPrimitiveRequest,
        summary: &VortexMetadataSummaryReport,
    ) -> Result<Self> {
        let primitive_result = evaluate_vortex_count_where_from_summary(request.clone(), summary)?;
        let (status, count, needs_encoded_predicate) = match primitive_result.status {
            VortexQueryPrimitiveStatus::MetadataAnswered => match primitive_result.value {
                VortexQueryPrimitiveValue::Count(v) => (
                    VortexFilteredCountMetadataProofStatus::ProofReady,
                    Some(v),
                    false,
                ),
                _ => (
                    VortexFilteredCountMetadataProofStatus::Unsupported,
                    None,
                    false,
                ),
            },
            VortexQueryPrimitiveStatus::NeedsEncodedPredicate
            | VortexQueryPrimitiveStatus::NeedsEncodedRead => (
                VortexFilteredCountMetadataProofStatus::NeedsEncodedPredicate,
                None,
                true,
            ),
            VortexQueryPrimitiveStatus::MissingMetadata => (
                VortexFilteredCountMetadataProofStatus::MissingMetadata,
                None,
                false,
            ),
            _ => (
                VortexFilteredCountMetadataProofStatus::Unsupported,
                None,
                false,
            ),
        };
        Ok(Self {
            schema_version: "shardloom.vortex_filtered_count_metadata_proof.v1",
            status,
            request,
            count,
            result_known: count.is_some(),
            metadata_summary_supplied: true,
            needs_encoded_predicate,
            data_read: false,
            row_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            fallback_execution_allowed: false,
            diagnostics: primitive_result.diagnostics,
        })
    }
    #[must_use]
    pub const fn proof_ready(&self) -> bool {
        self.status.proof_ready()
    }
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
    pub const fn is_side_effect_free(&self) -> bool {
        !self.data_read
            && !self.row_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.object_store_io
            && !self.write_io
            && !self.fallback_execution_allowed
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "filtered_count_metadata_proof_status={}\nproof_ready={}\nresult_known={}\nneeds_encoded_predicate={}\nmetadata_summary_supplied={}\ndata_read={}\nrow_read={}\ndata_decoded={}\ndata_materialized={}\nobject_store_io={}\nwrite_io={}\nfallback_execution_allowed={}",
            self.status.as_str(),
            self.proof_ready(),
            self.result_known,
            self.needs_encoded_predicate,
            self.metadata_summary_supplied,
            self.data_read,
            self.row_read,
            self.data_decoded,
            self.data_materialized,
            self.object_store_io,
            self.write_io,
            self.fallback_execution_allowed
        )
    }
}

fn derive_status(r: &VortexFilteredCountReadinessRequest) -> VortexFilteredCountReadinessStatus {
    use VortexFilteredCountReadinessSignal as S;
    if r.has_signal(S::ObjectStoreTarget) {
        return VortexFilteredCountReadinessStatus::BlockedByObjectStoreTarget;
    }
    if r.has_signal(S::ScanExecutionRisk) {
        return VortexFilteredCountReadinessStatus::BlockedByScanExecutionRisk;
    }
    if r.has_signal(S::DecodeRisk) {
        return VortexFilteredCountReadinessStatus::BlockedByDecodeRisk;
    }
    if r.has_signal(S::MaterializationRisk) {
        return VortexFilteredCountReadinessStatus::BlockedByMaterializationRisk;
    }
    if r.has_signal(S::ArrowDefaultRisk) {
        return VortexFilteredCountReadinessStatus::BlockedByArrowDefaultRisk;
    }
    if r.has_signal(S::WriteRisk) {
        return VortexFilteredCountReadinessStatus::BlockedByWriteRisk;
    }
    if r.has_signal(S::FallbackPolicyBlocked) {
        return VortexFilteredCountReadinessStatus::BlockedByFallbackPolicy;
    }
    if !r.has_signal(S::FeatureGateEnabled) {
        return VortexFilteredCountReadinessStatus::FeatureDisabled;
    }
    if !r.has_signal(S::FilteredCountPrimitive) {
        return VortexFilteredCountReadinessStatus::BlockedByUnsupportedPrimitive;
    }
    if r.has_signal(S::PredicateUnsupported) {
        return VortexFilteredCountReadinessStatus::BlockedByUnsupportedPredicate;
    }
    if !r.has_signal(S::PredicateProvided) {
        return VortexFilteredCountReadinessStatus::BlockedByMissingPredicate;
    }
    match r.candidate_source {
        VortexFilteredCountCandidateSource::MetadataPredicateProof => {
            if !r.has_signal(S::MetadataFooterReady) {
                return VortexFilteredCountReadinessStatus::BlockedByMissingMetadataFooter;
            }
            if !r.has_signal(S::PredicateMetadataProofReady) {
                return VortexFilteredCountReadinessStatus::BlockedByMissingPredicateProof;
            }
        }
        VortexFilteredCountCandidateSource::EncodedPredicatePath => {
            if !r.has_signal(S::EncodedDataPathReady) {
                return VortexFilteredCountReadinessStatus::BlockedByMissingEncodedDataPath;
            }
        }
        VortexFilteredCountCandidateSource::Unknown => {
            return VortexFilteredCountReadinessStatus::BlockedByUnsupportedPrimitive;
        }
    }
    if !r.has_signal(S::QueryPrimitiveReady) {
        return VortexFilteredCountReadinessStatus::BlockedByUnsupportedPrimitive;
    }
    VortexFilteredCountReadinessStatus::FilteredCountReady
}

fn derive_mode(
    r: &VortexFilteredCountReadinessRequest,
    s: VortexFilteredCountReadinessStatus,
) -> VortexFilteredCountReadinessMode {
    if matches!(s, VortexFilteredCountReadinessStatus::Unsupported) {
        return VortexFilteredCountReadinessMode::Unsupported;
    }
    match r.candidate_source {
        VortexFilteredCountCandidateSource::MetadataPredicateProof => {
            VortexFilteredCountReadinessMode::MetadataPredicateCountPlanning
        }
        VortexFilteredCountCandidateSource::EncodedPredicatePath => {
            VortexFilteredCountReadinessMode::EncodedPredicateCountPlanning
        }
        VortexFilteredCountCandidateSource::Unknown => VortexFilteredCountReadinessMode::ReportOnly,
    }
}

#[must_use]
pub fn filtered_count_readiness_request_from_query_primitive_report(
    target_uri: DatasetUri,
    query_report: &VortexQueryPrimitiveReport,
) -> VortexFilteredCountReadinessRequest {
    let is_filtered_count = matches!(
        query_report.request.primitive,
        VortexQueryPrimitiveBoundaryKind::FilteredCount
    );
    let has_predicate = query_report
        .request
        .has_signal(VortexQueryPrimitiveSignal::PredicateProvided);
    let candidate_source = if is_filtered_count && has_predicate {
        VortexFilteredCountCandidateSource::EncodedPredicatePath
    } else {
        VortexFilteredCountCandidateSource::Unknown
    };
    let mut req = VortexFilteredCountReadinessRequest::new(target_uri, candidate_source)
        .with_upstream_summary(query_report.to_human_text());
    req = req
        .feature_gate_enabled(query_report.feature_gate_enabled())
        .query_primitive_ready(query_report.primitive_ready())
        .metadata_footer_ready(query_report.metadata_footer_ready())
        .encoded_data_path_ready(query_report.encoded_data_path_ready())
        .predicate_provided(
            query_report
                .request
                .has_signal(VortexQueryPrimitiveSignal::PredicateProvided),
        )
        .predicate_unsupported(
            query_report
                .request
                .has_signal(VortexQueryPrimitiveSignal::PredicateUnsupported),
        )
        .object_store_target(query_report.object_store_target())
        .decode_risk(query_report.decode_risk())
        .materialization_risk(query_report.materialization_risk())
        .arrow_default_risk(query_report.arrow_default_risk())
        .write_risk(query_report.write_risk())
        .scan_execution_risk(query_report.scan_execution_risk());
    if is_filtered_count {
        req = req.filtered_count_primitive(true);
    }
    if query_report.fallback_execution_allowed()
        || matches!(
            query_report.status,
            VortexQueryPrimitiveBoundaryStatus::BlockedByFallbackPolicy
        )
    {
        req = req.fallback_policy_blocked(true);
    }
    req
}

/// # Errors
pub fn plan_vortex_filtered_count_readiness(
    request: VortexFilteredCountReadinessRequest,
) -> Result<VortexFilteredCountReadinessReport> {
    VortexFilteredCountReadinessReport::from_request(request).map_err(|e| {
        ShardLoomError::new(format!(
            "failed to build `VortexFilteredCountReadinessReport`: {e}"
        ))
    })
}

/// # Errors
pub fn plan_vortex_filtered_count_metadata_proof(
    request: VortexQueryPrimitiveRequest,
    summary: &VortexMetadataSummaryReport,
) -> Result<VortexFilteredCountMetadataProofReport> {
    if request.kind != VortexQueryPrimitiveKind::CountWhere {
        return Ok(VortexFilteredCountMetadataProofReport {
            schema_version: "shardloom.vortex_filtered_count_metadata_proof.v1",
            status: VortexFilteredCountMetadataProofStatus::Unsupported,
            request,
            count: None,
            result_known: false,
            metadata_summary_supplied: true,
            needs_encoded_predicate: false,
            data_read: false,
            row_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            fallback_execution_allowed: false,
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "vortex_filtered_count_metadata_proof",
                "Only CountWhere requests can produce filtered-count metadata proof.",
                Some("Use a CountWhere request with a predicate and metadata summary.".to_string()),
            )],
        });
    }
    VortexFilteredCountMetadataProofReport::from_summary(request, summary)
}
#[must_use]
pub fn vortex_filtered_count_readiness_is_side_effect_free(
    report: &VortexFilteredCountReadinessReport,
) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;
    use shardloom_core::{ColumnRef, PredicateExpr};
    fn uri() -> DatasetUri {
        DatasetUri::new("file://tmp/a.vortex").expect("uri")
    }
    fn metadata_summary_with_segment_rows(rows: u64) -> VortexMetadataSummaryReport {
        let mut summary = VortexFileMetadataSummary::empty();
        summary.row_count = Some(rows);
        summary.add_segment(VortexSegmentMetadataSummary::unknown().with_row_count(rows));
        VortexMetadataSummaryReport {
            status: VortexMetadataSummaryStatus::Summarized,
            summary,
            diagnostics: vec![],
        }
    }
    #[test]
    fn basic_statuses_and_helpers() {
        let r = plan_vortex_filtered_count_readiness(VortexFilteredCountReadinessRequest::new(
            uri(),
            VortexFilteredCountCandidateSource::Unknown,
        ))
        .expect("r");
        assert_eq!(
            r.status,
            VortexFilteredCountReadinessStatus::FeatureDisabled
        );
        let ready = plan_vortex_filtered_count_readiness(
            VortexFilteredCountReadinessRequest::new(
                uri(),
                VortexFilteredCountCandidateSource::EncodedPredicatePath,
            )
            .feature_gate_enabled(true)
            .query_primitive_ready(true)
            .filtered_count_primitive(true)
            .predicate_provided(true)
            .encoded_data_path_ready(true),
        )
        .expect("ready");
        assert!(ready.filtered_count_ready());
        assert!(!ready.filtered_count_executed());
        assert!(!ready.predicate_evaluated());
        assert!(ready.is_side_effect_free());
        assert!(
            ready
                .to_human_text()
                .contains("filtered_count_executed=false")
        );
        assert!(
            ready
                .to_human_text()
                .contains("fallback_execution_allowed=false")
        );
    }
    #[test]
    fn blockers_and_unknown() {
        let r = plan_vortex_filtered_count_readiness(
            VortexFilteredCountReadinessRequest::new(
                uri(),
                VortexFilteredCountCandidateSource::Unknown,
            )
            .feature_gate_enabled(true)
            .query_primitive_ready(true)
            .filtered_count_primitive(true)
            .predicate_provided(true),
        )
        .expect("r");
        assert_eq!(
            r.status,
            VortexFilteredCountReadinessStatus::BlockedByUnsupportedPrimitive
        );
        let m = plan_vortex_filtered_count_readiness(
            VortexFilteredCountReadinessRequest::new(
                uri(),
                VortexFilteredCountCandidateSource::MetadataPredicateProof,
            )
            .feature_gate_enabled(true)
            .query_primitive_ready(true)
            .filtered_count_primitive(true)
            .predicate_provided(true),
        )
        .expect("m");
        assert_eq!(
            m.status,
            VortexFilteredCountReadinessStatus::BlockedByMissingMetadataFooter
        );
        let e = plan_vortex_filtered_count_readiness(
            VortexFilteredCountReadinessRequest::new(
                uri(),
                VortexFilteredCountCandidateSource::EncodedPredicatePath,
            )
            .feature_gate_enabled(true)
            .query_primitive_ready(true)
            .filtered_count_primitive(true)
            .predicate_provided(true),
        )
        .expect("e");
        assert_eq!(
            e.status,
            VortexFilteredCountReadinessStatus::BlockedByMissingEncodedDataPath
        );
    }
    #[test]
    fn helper_mapping() {
        let q = plan_vortex_query_primitive(
            VortexQueryPrimitiveBoundaryRequest::new(
                uri(),
                VortexQueryPrimitiveBoundaryKind::FilteredCount,
            )
            .feature_gate_enabled(true)
            .metadata_footer_ready(true)
            .encoded_data_path_ready(true)
            .predicate_provided(true),
        )
        .expect("q");
        let req = filtered_count_readiness_request_from_query_primitive_report(uri(), &q);
        assert!(req.has_signal(VortexFilteredCountReadinessSignal::FilteredCountPrimitive));
        assert!(req.has_signal(VortexFilteredCountReadinessSignal::PredicateProvided));
        assert!(!req.has_signal(VortexFilteredCountReadinessSignal::PredicateMetadataProofReady));
        assert_eq!(
            req.candidate_source,
            VortexFilteredCountCandidateSource::EncodedPredicatePath
        );
    }

    #[test]
    fn metadata_proof_report_marks_proven_count_where_ready() {
        let report = plan_vortex_filtered_count_metadata_proof(
            VortexQueryPrimitiveRequest::count_where(uri(), PredicateExpr::AlwaysTrue),
            &metadata_summary_with_segment_rows(9),
        )
        .expect("report");

        assert_eq!(
            report.schema_version,
            "shardloom.vortex_filtered_count_metadata_proof.v1"
        );
        assert_eq!(
            report.status,
            VortexFilteredCountMetadataProofStatus::ProofReady
        );
        assert!(report.proof_ready());
        assert_eq!(report.count, Some(9));
        assert!(report.result_known);
        assert!(!report.needs_encoded_predicate);
        assert!(report.is_side_effect_free());
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn metadata_proof_report_defers_inconclusive_predicate() {
        let report = plan_vortex_filtered_count_metadata_proof(
            VortexQueryPrimitiveRequest::count_where(
                uri(),
                PredicateExpr::IsNull {
                    column: ColumnRef::new("missing_stats").expect("column"),
                },
            ),
            &metadata_summary_with_segment_rows(9),
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexFilteredCountMetadataProofStatus::NeedsEncodedPredicate
        );
        assert!(!report.proof_ready());
        assert_eq!(report.count, None);
        assert!(report.needs_encoded_predicate);
        assert!(!report.has_errors());
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn metadata_proof_report_rejects_non_count_where_requests() {
        let report = plan_vortex_filtered_count_metadata_proof(
            VortexQueryPrimitiveRequest::count_all(uri()),
            &metadata_summary_with_segment_rows(9),
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexFilteredCountMetadataProofStatus::Unsupported
        );
        assert!(!report.proof_ready());
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn helper_preserves_missing_encoded_path_blocker_for_filtered_count() {
        let q = plan_vortex_query_primitive(
            VortexQueryPrimitiveBoundaryRequest::new(
                uri(),
                VortexQueryPrimitiveBoundaryKind::FilteredCount,
            )
            .feature_gate_enabled(true)
            .metadata_footer_ready(true)
            .predicate_provided(true),
        )
        .expect("q");
        assert!(!q.encoded_data_path_ready());

        let req = filtered_count_readiness_request_from_query_primitive_report(uri(), &q);
        assert_eq!(
            req.candidate_source,
            VortexFilteredCountCandidateSource::EncodedPredicatePath
        );
        assert!(!req.has_signal(VortexFilteredCountReadinessSignal::PredicateMetadataProofReady));

        let report = plan_vortex_filtered_count_readiness(req).expect("report");
        assert_eq!(
            report.status,
            VortexFilteredCountReadinessStatus::BlockedByMissingEncodedDataPath
        );
        assert_ne!(
            report.status,
            VortexFilteredCountReadinessStatus::BlockedByUnsupportedPrimitive
        );
    }

    #[test]
    fn helper_keeps_non_filtered_count_as_unknown_candidate() {
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
        let req = filtered_count_readiness_request_from_query_primitive_report(uri(), &q);
        assert_eq!(
            req.candidate_source,
            VortexFilteredCountCandidateSource::Unknown
        );
        assert!(!req.has_signal(VortexFilteredCountReadinessSignal::FilteredCountPrimitive));
        let report = plan_vortex_filtered_count_readiness(req).expect("report");
        assert_eq!(
            report.status,
            VortexFilteredCountReadinessStatus::BlockedByUnsupportedPrimitive
        );
        assert!(!report.filtered_count_ready());
        assert!(!report.predicate_evaluated());
        assert!(!report.fallback_execution_allowed());
    }
}
