use std::fmt::Write as _;

use shardloom_core::{
    BenchmarkEvidenceState, BenchmarkFallbackState, DatasetUri, Diagnostic, DiagnosticCode,
    DiagnosticSeverity, KernelKind, OperatorMemoryCertification, PhysicalKernelAdmissionReport,
    PhysicalKernelAdmissionStatus, PhysicalKernelRequirement, PhysicalKernelSlot,
    PhysicalOperatorContract, PhysicalOperatorExecutionLevel, PhysicalOperatorKind, Result,
};

use crate::{
    VortexQueryPrimitiveBoundaryKind, VortexQueryPrimitiveBoundaryStatus,
    VortexQueryPrimitiveReport, VortexQueryPrimitiveSignal,
};

const METADATA_PROJECTION_ADMISSION_SCHEMA_VERSION: &str =
    "shardloom.vortex_metadata_projection_kernel_admission.v1";
const METADATA_PROJECTION_OPERATOR_ID: &str =
    "vortex.query_primitive.project_columns.metadata_project";

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
        match self {
            Self::ProjectionExecuted => "projection_executed",
            Self::ProjectionApplied => "projection_applied",
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
    #[must_use]
    pub fn feature_gate_enabled(&self) -> bool {
        self.request
            .has_signal(VortexProjectionReadinessSignal::FeatureGateEnabled)
    }
    #[must_use]
    pub fn query_primitive_ready(&self) -> bool {
        self.request
            .has_signal(VortexProjectionReadinessSignal::QueryPrimitiveReady)
    }
    #[must_use]
    pub fn metadata_footer_ready(&self) -> bool {
        self.request
            .has_signal(VortexProjectionReadinessSignal::MetadataFooterReady)
    }
    #[must_use]
    pub fn encoded_data_path_ready(&self) -> bool {
        self.request
            .has_signal(VortexProjectionReadinessSignal::EncodedDataPathReady)
    }
    #[must_use]
    pub fn projection_primitive(&self) -> bool {
        self.request
            .has_signal(VortexProjectionReadinessSignal::ProjectionPrimitive)
    }
    #[must_use]
    pub fn projection_provided(&self) -> bool {
        self.request
            .has_signal(VortexProjectionReadinessSignal::ProjectionProvided)
    }
    #[must_use]
    pub fn projection_supported(&self) -> bool {
        self.request
            .has_signal(VortexProjectionReadinessSignal::ProjectionSupported)
    }
    #[must_use]
    pub fn object_store_target(&self) -> bool {
        self.request
            .has_signal(VortexProjectionReadinessSignal::ObjectStoreTarget)
    }
    #[must_use]
    pub fn decode_risk(&self) -> bool {
        self.request
            .has_signal(VortexProjectionReadinessSignal::DecodeRisk)
    }
    #[must_use]
    pub fn materialization_risk(&self) -> bool {
        self.request
            .has_signal(VortexProjectionReadinessSignal::MaterializationRisk)
    }
    #[must_use]
    pub fn arrow_default_risk(&self) -> bool {
        self.request
            .has_signal(VortexProjectionReadinessSignal::ArrowDefaultRisk)
    }
    #[must_use]
    pub fn write_risk(&self) -> bool {
        self.request
            .has_signal(VortexProjectionReadinessSignal::WriteRisk)
    }
    #[must_use]
    pub fn scan_execution_risk(&self) -> bool {
        self.request
            .has_signal(VortexProjectionReadinessSignal::ScanExecutionRisk)
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
    pub fn metadata_read(&self) -> bool {
        self.effect(VortexProjectionReadinessEffect::MetadataRead)
    }
    #[must_use]
    pub fn encoded_data_read(&self) -> bool {
        self.effect(VortexProjectionReadinessEffect::EncodedDataRead)
    }
    #[must_use]
    pub fn row_read(&self) -> bool {
        self.effect(VortexProjectionReadinessEffect::RowRead)
    }
    #[must_use]
    pub fn array_decoded(&self) -> bool {
        self.effect(VortexProjectionReadinessEffect::ArrayDecoded)
    }
    #[must_use]
    pub fn values_materialized(&self) -> bool {
        self.effect(VortexProjectionReadinessEffect::ValuesMaterialized)
    }
    #[must_use]
    pub fn arrow_converted(&self) -> bool {
        self.effect(VortexProjectionReadinessEffect::ArrowConverted)
    }
    #[must_use]
    pub fn object_store_io(&self) -> bool {
        self.effect(VortexProjectionReadinessEffect::ObjectStoreIo)
    }
    #[must_use]
    pub fn data_written(&self) -> bool {
        self.effect(VortexProjectionReadinessEffect::DataWritten)
    }
    #[must_use]
    pub fn upstream_scan_called(&self) -> bool {
        self.effect(VortexProjectionReadinessEffect::UpstreamScanCalled)
    }
    #[must_use]
    pub fn fallback_execution_allowed(&self) -> bool {
        false
    }
    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        !self.projection_executed()
            && !self.projection_applied()
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
    pub fn is_safe_metadata_projection_kernel_evidence(&self) -> bool {
        self.status == VortexProjectionReadinessStatus::ProjectionReady
            && self.mode == VortexProjectionReadinessMode::MetadataSchemaProjectionPlanning
            && self.request.candidate_source
                == VortexProjectionCandidateSource::MetadataSchemaProjection
            && self.feature_gate_enabled()
            && self.query_primitive_ready()
            && self.metadata_footer_ready()
            && self.projection_primitive()
            && self.projection_provided()
            && self.projection_supported()
            && self.is_side_effect_free()
            && !self.has_errors()
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(&mut out, "status: {}", self.status.as_str());
        let _ = writeln!(&mut out, "mode: {}", self.mode.as_str());
        let _ = writeln!(&mut out, "projection_ready: {}", self.projection_ready());
        let _ = writeln!(
            &mut out,
            "projection_executed: {}",
            self.projection_executed()
        );
        let _ = writeln!(
            &mut out,
            "projection_applied: {}",
            self.projection_applied()
        );
        let _ = writeln!(&mut out, "encoded_data_read: {}", self.encoded_data_read());
        let _ = writeln!(&mut out, "row_read: {}", self.row_read());
        let _ = writeln!(
            &mut out,
            "values_materialized: {}",
            self.values_materialized()
        );
        let _ = writeln!(&mut out, "arrow_converted: {}", self.arrow_converted());
        let _ = writeln!(
            &mut out,
            "fallback_execution_allowed: {}",
            self.fallback_execution_allowed()
        );
        out
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexMetadataProjectionKernelAdmissionReport {
    pub schema_version: &'static str,
    pub admission_id: String,
    pub projection_readiness_status: VortexProjectionReadinessStatus,
    pub projection_readiness_mode: VortexProjectionReadinessMode,
    pub slot_id: String,
    pub operator_kind: PhysicalOperatorKind,
    pub required_kernel_kind: KernelKind,
    pub candidate_kernel_kind: KernelKind,
    pub correctness_evidence: BenchmarkEvidenceState,
    pub benchmark_evidence: BenchmarkEvidenceState,
    pub memory: OperatorMemoryCertification,
    pub fallback: BenchmarkFallbackState,
    pub status: PhysicalKernelAdmissionStatus,
    pub slot_marked_present: bool,
    pub production_claim_allowed: bool,
    pub runtime_execution_allowed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexMetadataProjectionKernelAdmissionReport {
    #[must_use]
    pub fn from_admission(
        readiness: &VortexProjectionReadinessReport,
        admission: PhysicalKernelAdmissionReport,
    ) -> Self {
        let mut diagnostics = readiness.diagnostics.clone();
        diagnostics.extend(admission.diagnostics.clone());
        let slot_marked_present = admission.can_mark_kernel_present();
        let production_claim_allowed = admission.can_satisfy_production_claim();
        Self {
            schema_version: METADATA_PROJECTION_ADMISSION_SCHEMA_VERSION,
            admission_id: "vortex.query-primitive.project_columns.metadata-projection-admission"
                .to_string(),
            projection_readiness_status: readiness.status,
            projection_readiness_mode: readiness.mode,
            slot_id: admission.slot_id,
            operator_kind: admission.operator_kind,
            required_kernel_kind: admission.required_kernel_kind,
            candidate_kernel_kind: admission.candidate_kernel_kind,
            correctness_evidence: admission.correctness_evidence,
            benchmark_evidence: admission.benchmark_evidence,
            memory: admission.memory,
            fallback: admission.fallback,
            status: admission.status,
            slot_marked_present,
            production_claim_allowed,
            runtime_execution_allowed: false,
            fallback_execution_allowed: false,
            diagnostics,
        }
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.status.can_enter_registry()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.runtime_execution_allowed && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "metadata projection kernel admission\nschema_version: {}\nadmission: {}\nslot: {}\noperator: {}\nrequired kernel: {}\ncandidate kernel: {}\nstatus: {}\nslot marked present: {}\nproduction claim allowed: {}\nruntime execution: disabled\nfallback execution: disabled",
            self.schema_version,
            self.admission_id,
            self.slot_id,
            self.operator_kind.as_str(),
            self.required_kernel_kind.as_str(),
            self.candidate_kernel_kind.as_str(),
            self.status.as_str(),
            self.slot_marked_present,
            self.production_claim_allowed
        )
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

/// Admits metadata-schema projection readiness into the CG-7 project metadata
/// kernel slot.
///
/// This is a contextual evidence bridge. It does not execute projection, read
/// encoded columns, decode arrays, materialize values, register a global runtime
/// project kernel, claim production readiness, or close broader projection-kernel
/// work.
///
/// # Errors
/// Returns an error only if the static metadata-project operator contract cannot
/// be built.
pub fn admit_vortex_metadata_projection_kernel(
    readiness: &VortexProjectionReadinessReport,
    correctness_evidence: BenchmarkEvidenceState,
    benchmark_evidence: BenchmarkEvidenceState,
    memory: OperatorMemoryCertification,
    fallback: BenchmarkFallbackState,
) -> Result<VortexMetadataProjectionKernelAdmissionReport> {
    let slot = metadata_projection_kernel_slot()?;
    let safe_readiness = readiness.is_safe_metadata_projection_kernel_evidence();
    let admission = PhysicalKernelAdmissionReport::evaluate(
        &slot,
        KernelKind::Metadata,
        if safe_readiness {
            correctness_evidence
        } else {
            BenchmarkEvidenceState::Missing
        },
        if safe_readiness {
            benchmark_evidence
        } else {
            BenchmarkEvidenceState::Missing
        },
        if safe_readiness {
            memory
        } else {
            OperatorMemoryCertification::unsupported()
        },
        fallback,
    );
    Ok(VortexMetadataProjectionKernelAdmissionReport::from_admission(readiness, admission))
}

fn metadata_projection_kernel_slot() -> Result<PhysicalKernelSlot> {
    let operator = PhysicalOperatorContract::new(
        METADATA_PROJECTION_OPERATOR_ID,
        PhysicalOperatorKind::Project,
        PhysicalOperatorExecutionLevel::MetadataOnly,
        vec![PhysicalKernelRequirement::missing(KernelKind::Metadata)],
    )?;
    Ok(PhysicalKernelSlot::from_requirement(
        &operator,
        PhysicalKernelRequirement::missing(KernelKind::Metadata),
    ))
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

    fn safe_streaming_memory() -> OperatorMemoryCertification {
        OperatorMemoryCertification {
            streaming: true,
            bounded_memory: true,
            spillable: false,
            requires_full_materialization: false,
            requires_shuffle: false,
            oom_safe: true,
        }
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

    #[test]
    fn effect_labels_are_stable() {
        assert_eq!(
            VortexProjectionReadinessEffect::ProjectionExecuted.as_str(),
            "projection_executed"
        );
        assert_eq!(
            VortexProjectionReadinessEffect::ProjectionApplied.as_str(),
            "projection_applied"
        );
        assert_eq!(
            VortexProjectionReadinessEffect::MetadataRead.as_str(),
            "metadata_read"
        );
        assert_eq!(
            VortexProjectionReadinessEffect::EncodedDataRead.as_str(),
            "encoded_data_read"
        );
        assert_eq!(
            VortexProjectionReadinessEffect::RowRead.as_str(),
            "row_read"
        );
        assert_eq!(
            VortexProjectionReadinessEffect::ArrayDecoded.as_str(),
            "array_decoded"
        );
        assert_eq!(
            VortexProjectionReadinessEffect::ValuesMaterialized.as_str(),
            "values_materialized"
        );
        assert_eq!(
            VortexProjectionReadinessEffect::ArrowConverted.as_str(),
            "arrow_converted"
        );
        assert_eq!(
            VortexProjectionReadinessEffect::ObjectStoreIo.as_str(),
            "object_store_io"
        );
        assert_eq!(
            VortexProjectionReadinessEffect::DataWritten.as_str(),
            "data_written"
        );
        assert_eq!(
            VortexProjectionReadinessEffect::UpstreamScanCalled.as_str(),
            "upstream_scan_called"
        );
        assert_eq!(
            VortexProjectionReadinessEffect::FallbackExecution.as_str(),
            "fallback_execution"
        );
    }

    #[test]
    fn report_signal_helpers_reflect_request_signals() {
        let report = plan_vortex_projection_readiness(
            VortexProjectionReadinessRequest::new(
                uri(),
                VortexProjectionCandidateSource::MetadataSchemaProjection,
            )
            .feature_gate_enabled(true)
            .query_primitive_ready(true)
            .metadata_footer_ready(true)
            .projection_primitive(true)
            .projection_provided(true)
            .projection_supported(true)
            .object_store_target(true)
            .decode_risk(true)
            .materialization_risk(true)
            .arrow_default_risk(true)
            .write_risk(true)
            .scan_execution_risk(true),
        )
        .expect("report");
        assert!(report.feature_gate_enabled());
        assert!(report.query_primitive_ready());
        assert!(report.metadata_footer_ready());
        assert!(!report.encoded_data_path_ready());
        assert!(report.projection_primitive());
        assert!(report.projection_provided());
        assert!(report.projection_supported());
        assert!(report.object_store_target());
        assert!(report.decode_risk());
        assert!(report.materialization_risk());
        assert!(report.arrow_default_risk());
        assert!(report.write_risk());
        assert!(report.scan_execution_risk());
    }

    #[test]
    fn effect_helpers_are_false_for_ready_projection() {
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
        assert!(!report.projection_executed());
        assert!(!report.projection_applied());
        assert!(!report.metadata_read());
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

    #[test]
    fn is_side_effect_free_checks_full_effect_set() {
        let mut report = plan_vortex_projection_readiness(
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
        assert!(report.is_side_effect_free());
        report
            .effects_performed
            .push(VortexProjectionReadinessEffect::EncodedDataRead);
        assert!(!report.is_side_effect_free());
    }

    #[test]
    fn metadata_schema_projection_ready_requires_support_and_metadata() {
        let ready = plan_vortex_projection_readiness(
            VortexProjectionReadinessRequest::new(
                uri(),
                VortexProjectionCandidateSource::MetadataSchemaProjection,
            )
            .feature_gate_enabled(true)
            .query_primitive_ready(true)
            .projection_primitive(true)
            .projection_provided(true)
            .metadata_footer_ready(true)
            .projection_supported(true),
        )
        .expect("report");
        assert!(ready.projection_ready());
    }

    #[test]
    fn metadata_projection_kernel_admits_metadata_slot_without_production_claim() {
        let readiness = plan_vortex_projection_readiness(
            VortexProjectionReadinessRequest::new(
                uri(),
                VortexProjectionCandidateSource::MetadataSchemaProjection,
            )
            .feature_gate_enabled(true)
            .query_primitive_ready(true)
            .projection_primitive(true)
            .projection_provided(true)
            .metadata_footer_ready(true)
            .projection_supported(true),
        )
        .expect("projection readiness");

        let admission = admit_vortex_metadata_projection_kernel(
            &readiness,
            BenchmarkEvidenceState::Present,
            BenchmarkEvidenceState::Missing,
            safe_streaming_memory(),
            BenchmarkFallbackState::NotAttempted,
        )
        .expect("projection admission");

        assert_eq!(
            admission.schema_version,
            METADATA_PROJECTION_ADMISSION_SCHEMA_VERSION
        );
        assert_eq!(
            admission.status,
            PhysicalKernelAdmissionStatus::RegistryReady
        );
        assert_eq!(admission.operator_kind, PhysicalOperatorKind::Project);
        assert_eq!(admission.required_kernel_kind, KernelKind::Metadata);
        assert_eq!(admission.candidate_kernel_kind, KernelKind::Metadata);
        assert_eq!(
            admission.correctness_evidence,
            BenchmarkEvidenceState::Present
        );
        assert_eq!(
            admission.benchmark_evidence,
            BenchmarkEvidenceState::Missing
        );
        assert!(admission.slot_marked_present);
        assert!(!admission.production_claim_allowed);
        assert!(admission.memory.streaming);
        assert!(admission.memory.bounded_memory);
        assert!(admission.memory.oom_safe);
        assert!(admission.is_side_effect_free());
        assert!(!admission.has_errors());
    }

    #[test]
    fn metadata_projection_kernel_missing_correctness_blocks_admission() {
        let readiness = plan_vortex_projection_readiness(
            VortexProjectionReadinessRequest::new(
                uri(),
                VortexProjectionCandidateSource::MetadataSchemaProjection,
            )
            .feature_gate_enabled(true)
            .query_primitive_ready(true)
            .projection_primitive(true)
            .projection_provided(true)
            .metadata_footer_ready(true)
            .projection_supported(true),
        )
        .expect("projection readiness");

        let admission = admit_vortex_metadata_projection_kernel(
            &readiness,
            BenchmarkEvidenceState::Missing,
            BenchmarkEvidenceState::Missing,
            safe_streaming_memory(),
            BenchmarkFallbackState::NotAttempted,
        )
        .expect("projection admission");

        assert_eq!(
            admission.status,
            PhysicalKernelAdmissionStatus::BlockedMissingCorrectness
        );
        assert!(!admission.slot_marked_present);
        assert!(!admission.production_claim_allowed);
        assert!(admission.has_errors());
        assert!(admission.is_side_effect_free());
    }

    #[test]
    fn encoded_projection_readiness_cannot_admit_metadata_projection_slot() {
        let readiness = plan_vortex_projection_readiness(
            VortexProjectionReadinessRequest::new(
                uri(),
                VortexProjectionCandidateSource::EncodedColumnPath,
            )
            .feature_gate_enabled(true)
            .query_primitive_ready(true)
            .projection_primitive(true)
            .projection_provided(true)
            .projection_supported(true)
            .encoded_data_path_ready(true),
        )
        .expect("projection readiness");

        let admission = admit_vortex_metadata_projection_kernel(
            &readiness,
            BenchmarkEvidenceState::Present,
            BenchmarkEvidenceState::Missing,
            safe_streaming_memory(),
            BenchmarkFallbackState::NotAttempted,
        )
        .expect("projection admission");

        assert_eq!(
            admission.status,
            PhysicalKernelAdmissionStatus::BlockedMissingCorrectness
        );
        assert!(!admission.slot_marked_present);
        assert!(admission.has_errors());
    }

    #[test]
    fn helper_does_not_auto_mark_projection_supported() {
        let mut req = crate::VortexQueryPrimitiveBoundaryRequest::new(
            uri(),
            crate::VortexQueryPrimitiveBoundaryKind::Projection,
        )
        .feature_gate_enabled(true)
        .with_projection_summary("x");
        req.add_signal(crate::VortexQueryPrimitiveSignal::ProjectionProvided);
        req.add_signal(crate::VortexQueryPrimitiveSignal::EncodedDataPathReady);
        let query_report = crate::plan_vortex_query_primitive(req).expect("query report");
        let projection_request =
            projection_readiness_request_from_query_primitive_report(uri(), &query_report);
        assert!(
            !projection_request.has_signal(VortexProjectionReadinessSignal::ProjectionSupported)
        );
    }
}
