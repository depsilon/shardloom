use std::fmt::Write as _;

use shardloom_core::{
    BenchmarkEvidenceState, BenchmarkFallbackState, Diagnostic, DiagnosticCode, KernelKind,
    OperatorMemoryCertification, PhysicalKernelAdmissionReport, PhysicalKernelAdmissionStatus,
    PhysicalKernelRequirement, PhysicalKernelSlot, PhysicalOperatorContract,
    PhysicalOperatorExecutionLevel, PhysicalOperatorKind, Result,
};

use crate::VortexQueryPrimitiveKind;

pub const VORTEX_SPECIALIZED_KERNEL_REGISTRY_SCHEMA_VERSION: &str =
    "shardloom.vortex_specialized_kernel_registry.v1";
pub const VORTEX_SPECIALIZED_KERNEL_REGISTRY_ID: &str =
    "shardloom.vortex.specialized-kernel-registry.local-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexKernelMaterializationLevel {
    MetadataOnly,
    EncodedNoMaterialization,
    ColumnarState,
    RowRefsOnly,
    SinkBoundaryOnly,
    Unsupported,
}

impl VortexKernelMaterializationLevel {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::EncodedNoMaterialization => "encoded_no_materialization",
            Self::ColumnarState => "columnar_state",
            Self::RowRefsOnly => "row_refs_only",
            Self::SinkBoundaryOnly => "sink_boundary_only",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexSpecializedKernelRuntimeLane {
    ExistingNativeExecutor,
    ExistingBenchmarkOnly,
    PlanningOnly,
}

impl VortexSpecializedKernelRuntimeLane {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExistingNativeExecutor => "existing_native_executor",
            Self::ExistingBenchmarkOnly => "existing_benchmark_only",
            Self::PlanningOnly => "planning_only",
        }
    }

    #[must_use]
    pub const fn execution_admitted(self) -> bool {
        matches!(self, Self::ExistingNativeExecutor)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexSpecializedKernelAdmissionStatus {
    Admitted,
    BlockedUnsupportedRoute,
    BlockedOperatorMismatch,
    BlockedPrimitiveMismatch,
    BlockedLogicalDtype,
    BlockedEncodingLayout,
    BlockedNullSemantics,
    BlockedMaterializationBoundary,
    BlockedUnsafeEffects,
    BlockedPhysicalAdmission,
}

impl VortexSpecializedKernelAdmissionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::BlockedUnsupportedRoute => "blocked_unsupported_route",
            Self::BlockedOperatorMismatch => "blocked_operator_mismatch",
            Self::BlockedPrimitiveMismatch => "blocked_primitive_mismatch",
            Self::BlockedLogicalDtype => "blocked_logical_dtype",
            Self::BlockedEncodingLayout => "blocked_encoding_layout",
            Self::BlockedNullSemantics => "blocked_null_semantics",
            Self::BlockedMaterializationBoundary => "blocked_materialization_boundary",
            Self::BlockedUnsafeEffects => "blocked_unsafe_effects",
            Self::BlockedPhysicalAdmission => "blocked_physical_admission",
        }
    }

    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::Admitted)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexSpecializedKernelContract {
    pub kernel_id: &'static str,
    pub route_families: &'static [&'static str],
    pub query_primitive_kinds: &'static [VortexQueryPrimitiveKind],
    pub operator_kind: PhysicalOperatorKind,
    pub kernel_kind: KernelKind,
    pub execution_level: PhysicalOperatorExecutionLevel,
    pub logical_dtypes: &'static [&'static str],
    pub encoding_layouts: &'static [&'static str],
    pub null_semantics: &'static [&'static str],
    pub materialization_level: VortexKernelMaterializationLevel,
    pub determinism_contract: &'static str,
    pub memory: OperatorMemoryCertification,
    pub vortex_provider_surface: &'static str,
    pub input_contract: &'static str,
    pub output_contract: &'static str,
    pub correctness_contract: &'static str,
    pub runtime_lane: VortexSpecializedKernelRuntimeLane,
    pub benchmark_required_for_production: bool,
    pub fallback_execution_allowed: bool,
}

impl VortexSpecializedKernelContract {
    #[must_use]
    pub fn supports_route(&self, route_family: &str) -> bool {
        token_matches(self.route_families, route_family)
    }

    #[must_use]
    pub fn supports_primitive(&self, primitive_kind: Option<VortexQueryPrimitiveKind>) -> bool {
        self.query_primitive_kinds.is_empty()
            || primitive_kind
                .is_some_and(|primitive| self.query_primitive_kinds.contains(&primitive))
    }

    #[must_use]
    pub fn supports_dtype(&self, logical_dtype: &str) -> bool {
        token_matches(self.logical_dtypes, logical_dtype)
    }

    #[must_use]
    pub fn supports_encoding(&self, encoding_layout: &str) -> bool {
        token_matches(self.encoding_layouts, encoding_layout)
    }

    #[must_use]
    pub fn supports_null_semantics(&self, null_semantics: &str) -> bool {
        token_matches(self.null_semantics, null_semantics)
    }

    #[must_use]
    pub const fn runtime_execution_admitted(&self) -> bool {
        self.runtime_lane.execution_admitted()
    }

    #[must_use]
    pub const fn production_claim_allowed_by_contract(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexSpecializedKernelRequest {
    pub candidate_id: String,
    pub route_family: String,
    pub primitive_kind: Option<VortexQueryPrimitiveKind>,
    pub operator_kind: PhysicalOperatorKind,
    pub logical_dtype: String,
    pub encoding_layout: String,
    pub null_semantics: String,
    pub materialization_level: VortexKernelMaterializationLevel,
    pub correctness_evidence: BenchmarkEvidenceState,
    pub benchmark_evidence: BenchmarkEvidenceState,
    pub decoded_reference_compared: bool,
    pub data_decoded: bool,
    pub rows_materialized: u64,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub external_engine_invoked: bool,
}

impl VortexSpecializedKernelRequest {
    #[must_use]
    pub fn new(
        route_family: impl Into<String>,
        operator_kind: PhysicalOperatorKind,
        logical_dtype: impl Into<String>,
        encoding_layout: impl Into<String>,
        null_semantics: impl Into<String>,
    ) -> Self {
        let route_family = route_family.into();
        Self {
            candidate_id: format!("vortex.specialized-kernel.candidate.{route_family}"),
            route_family,
            primitive_kind: None,
            operator_kind,
            logical_dtype: logical_dtype.into(),
            encoding_layout: encoding_layout.into(),
            null_semantics: null_semantics.into(),
            materialization_level: VortexKernelMaterializationLevel::EncodedNoMaterialization,
            correctness_evidence: BenchmarkEvidenceState::Missing,
            benchmark_evidence: BenchmarkEvidenceState::Missing,
            decoded_reference_compared: false,
            data_decoded: false,
            rows_materialized: 0,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub fn with_candidate_id(mut self, candidate_id: impl Into<String>) -> Self {
        self.candidate_id = candidate_id.into();
        self
    }

    #[must_use]
    pub const fn with_primitive_kind(mut self, primitive_kind: VortexQueryPrimitiveKind) -> Self {
        self.primitive_kind = Some(primitive_kind);
        self
    }

    #[must_use]
    pub const fn with_materialization_level(
        mut self,
        materialization_level: VortexKernelMaterializationLevel,
    ) -> Self {
        self.materialization_level = materialization_level;
        self
    }

    #[must_use]
    pub const fn with_correctness_evidence(mut self, evidence: BenchmarkEvidenceState) -> Self {
        self.correctness_evidence = evidence;
        self
    }

    #[must_use]
    pub const fn with_benchmark_evidence(mut self, evidence: BenchmarkEvidenceState) -> Self {
        self.benchmark_evidence = evidence;
        self
    }

    #[must_use]
    pub const fn with_decoded_reference_compared(mut self, value: bool) -> Self {
        self.decoded_reference_compared = value;
        self
    }

    #[must_use]
    pub const fn with_data_decoded(mut self, value: bool) -> Self {
        self.data_decoded = value;
        self
    }

    #[must_use]
    pub const fn with_rows_materialized(mut self, value: u64) -> Self {
        self.rows_materialized = value;
        self
    }

    #[must_use]
    pub const fn with_fallback_attempted(mut self, value: bool) -> Self {
        self.fallback_attempted = value;
        self
    }

    #[must_use]
    pub const fn with_fallback_execution_allowed(mut self, value: bool) -> Self {
        self.fallback_execution_allowed = value;
        self
    }

    #[must_use]
    pub const fn with_external_engine_invoked(mut self, value: bool) -> Self {
        self.external_engine_invoked = value;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexSpecializedKernelRegistryReport {
    pub schema_version: &'static str,
    pub registry_id: &'static str,
    pub contracts: Vec<VortexSpecializedKernelContract>,
}

impl VortexSpecializedKernelRegistryReport {
    #[must_use]
    pub fn local_v1() -> Self {
        Self {
            schema_version: VORTEX_SPECIALIZED_KERNEL_REGISTRY_SCHEMA_VERSION,
            registry_id: VORTEX_SPECIALIZED_KERNEL_REGISTRY_ID,
            contracts: local_kernel_contracts(),
        }
    }

    #[must_use]
    pub fn contract_for_kernel_id(
        &self,
        kernel_id: &str,
    ) -> Option<&VortexSpecializedKernelContract> {
        self.contracts
            .iter()
            .find(|contract| contract.kernel_id == kernel_id)
    }

    #[must_use]
    pub fn candidate_contracts(
        &self,
        request: &VortexSpecializedKernelRequest,
    ) -> Vec<&VortexSpecializedKernelContract> {
        self.contracts
            .iter()
            .filter(|contract| contract.supports_route(&request.route_family))
            .collect()
    }

    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn evidence_fields(&self) -> Vec<(String, String)> {
        vec![
            (
                "specialized_kernel_registry_schema_version".to_string(),
                self.schema_version.to_string(),
            ),
            (
                "specialized_kernel_registry_id".to_string(),
                self.registry_id.to_string(),
            ),
            (
                "specialized_kernel_registry_kernel_count".to_string(),
                self.contracts.len().to_string(),
            ),
            (
                "specialized_kernel_registry_kernel_ids".to_string(),
                registry_contract_value_rows(&self.contracts, |contract| {
                    contract.kernel_id.to_string()
                }),
            ),
            (
                "specialized_kernel_registry_operator_kinds".to_string(),
                registry_contract_value_rows(&self.contracts, |contract| {
                    contract.operator_kind.as_str().to_string()
                }),
            ),
            (
                "specialized_kernel_registry_kernel_kinds".to_string(),
                registry_contract_value_rows(&self.contracts, |contract| {
                    contract.kernel_kind.as_str().to_string()
                }),
            ),
            (
                "specialized_kernel_registry_execution_levels".to_string(),
                registry_contract_value_rows(&self.contracts, |contract| {
                    contract.execution_level.as_str().to_string()
                }),
            ),
            (
                "specialized_kernel_registry_materialization_levels".to_string(),
                registry_contract_value_rows(&self.contracts, |contract| {
                    contract.materialization_level.as_str().to_string()
                }),
            ),
            (
                "specialized_kernel_registry_runtime_lanes".to_string(),
                registry_contract_value_rows(&self.contracts, |contract| {
                    contract.runtime_lane.as_str().to_string()
                }),
            ),
            (
                "specialized_kernel_registry_route_families".to_string(),
                registry_contract_value_rows(&self.contracts, |contract| {
                    contract.route_families.join(",")
                }),
            ),
            (
                "specialized_kernel_registry_fallback_execution_allowed".to_string(),
                self.contracts
                    .iter()
                    .any(|contract| contract.fallback_execution_allowed)
                    .to_string(),
            ),
            (
                "specialized_kernel_registry_external_engine_invoked".to_string(),
                "false".to_string(),
            ),
            (
                "specialized_kernel_registry_production_claim_allowed".to_string(),
                self.contracts
                    .iter()
                    .any(VortexSpecializedKernelContract::production_claim_allowed_by_contract)
                    .to_string(),
            ),
            (
                "specialized_kernel_registry_claim_boundary".to_string(),
                "registry admission and local native route selection only; production benchmark superiority remains blocked until retained CG-5/CG-6 evidence"
                    .to_string(),
            ),
        ]
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "registry: {}", self.registry_id);
        let _ = writeln!(out, "kernels: {}", self.contracts.len());
        let _ = writeln!(
            out,
            "runtime lanes: {}",
            registry_contract_value_rows(&self.contracts, |contract| {
                contract.runtime_lane.as_str().to_string()
            })
        );
        let _ = writeln!(out, "fallback execution: disabled");
        let _ = writeln!(out, "production claim: blocked pending CG-5/CG-6");
        out
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexSpecializedKernelAdmissionReport {
    pub schema_version: &'static str,
    pub registry_id: &'static str,
    pub candidate_id: String,
    pub requested_route_family: String,
    pub candidate_kernel_ids: Vec<String>,
    pub selected_kernel_id: Option<String>,
    pub status: VortexSpecializedKernelAdmissionStatus,
    pub physical_admission_status: Option<PhysicalKernelAdmissionStatus>,
    pub operator_kind: PhysicalOperatorKind,
    pub kernel_kind: Option<KernelKind>,
    pub execution_level: Option<PhysicalOperatorExecutionLevel>,
    pub materialization_level: VortexKernelMaterializationLevel,
    pub runtime_lane: Option<VortexSpecializedKernelRuntimeLane>,
    pub admission_reason: String,
    pub input_contract: String,
    pub output_contract: String,
    pub correctness_contract: String,
    pub vortex_provider_surface: String,
    pub decoded_reference_compared: bool,
    pub correctness_evidence: BenchmarkEvidenceState,
    pub benchmark_evidence: BenchmarkEvidenceState,
    pub runtime_execution_admitted: bool,
    pub production_claim_allowed: bool,
    pub data_decoded: bool,
    pub rows_materialized: u64,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexSpecializedKernelAdmissionReport {
    #[must_use]
    pub fn is_admitted(&self) -> bool {
        self.status == VortexSpecializedKernelAdmissionStatus::Admitted
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.fallback_attempted
            || self.fallback_execution_allowed
            || self.external_engine_invoked
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    shardloom_core::DiagnosticSeverity::Error
                        | shardloom_core::DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn evidence_fields(&self) -> Vec<(String, String)> {
        vec![
            (
                "specialized_kernel_admission_schema_version".to_string(),
                self.schema_version.to_string(),
            ),
            (
                "specialized_kernel_admission_registry_id".to_string(),
                self.registry_id.to_string(),
            ),
            (
                "specialized_kernel_admission_candidate_id".to_string(),
                self.candidate_id.clone(),
            ),
            (
                "specialized_kernel_admission_route_family".to_string(),
                self.requested_route_family.clone(),
            ),
            (
                "specialized_kernel_admission_candidate_kernel_ids".to_string(),
                self.candidate_kernel_ids.join("|"),
            ),
            (
                "specialized_kernel_admission_selected_kernel_id".to_string(),
                self.selected_kernel_id
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
            ),
            (
                "specialized_kernel_admission_status".to_string(),
                self.status.as_str().to_string(),
            ),
            (
                "specialized_kernel_admission_physical_status".to_string(),
                self.physical_admission_status.map_or_else(
                    || "not_evaluated".to_string(),
                    |status| status.as_str().to_string(),
                ),
            ),
            (
                "specialized_kernel_admission_operator_kind".to_string(),
                self.operator_kind.as_str().to_string(),
            ),
            (
                "specialized_kernel_admission_kernel_kind".to_string(),
                self.kernel_kind.map_or_else(
                    || "none".to_string(),
                    |kernel_kind| kernel_kind.as_str().to_string(),
                ),
            ),
            (
                "specialized_kernel_admission_execution_level".to_string(),
                self.execution_level
                    .map_or_else(|| "none".to_string(), |level| level.as_str().to_string()),
            ),
            (
                "specialized_kernel_admission_materialization_level".to_string(),
                self.materialization_level.as_str().to_string(),
            ),
            (
                "specialized_kernel_admission_runtime_lane".to_string(),
                self.runtime_lane
                    .map_or_else(|| "none".to_string(), |lane| lane.as_str().to_string()),
            ),
            (
                "specialized_kernel_admission_reason".to_string(),
                self.admission_reason.clone(),
            ),
            (
                "specialized_kernel_admission_input_contract".to_string(),
                self.input_contract.clone(),
            ),
            (
                "specialized_kernel_admission_output_contract".to_string(),
                self.output_contract.clone(),
            ),
            (
                "specialized_kernel_admission_correctness_contract".to_string(),
                self.correctness_contract.clone(),
            ),
            (
                "specialized_kernel_admission_vortex_provider_surface".to_string(),
                self.vortex_provider_surface.clone(),
            ),
            (
                "specialized_kernel_admission_decoded_reference_compared".to_string(),
                self.decoded_reference_compared.to_string(),
            ),
            (
                "specialized_kernel_admission_correctness_evidence".to_string(),
                self.correctness_evidence.as_str().to_string(),
            ),
            (
                "specialized_kernel_admission_benchmark_evidence".to_string(),
                self.benchmark_evidence.as_str().to_string(),
            ),
            (
                "specialized_kernel_admission_runtime_execution_admitted".to_string(),
                self.runtime_execution_admitted.to_string(),
            ),
            (
                "specialized_kernel_admission_production_claim_allowed".to_string(),
                self.production_claim_allowed.to_string(),
            ),
            (
                "specialized_kernel_admission_data_decoded".to_string(),
                self.data_decoded.to_string(),
            ),
            (
                "specialized_kernel_admission_rows_materialized".to_string(),
                self.rows_materialized.to_string(),
            ),
            (
                "specialized_kernel_admission_fallback_attempted".to_string(),
                self.fallback_attempted.to_string(),
            ),
            (
                "specialized_kernel_admission_external_engine_invoked".to_string(),
                self.external_engine_invoked.to_string(),
            ),
            (
                "specialized_kernel_admission_fallback_execution_allowed".to_string(),
                self.fallback_execution_allowed.to_string(),
            ),
        ]
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "schema_version: {}", self.schema_version);
        let _ = writeln!(out, "registry: {}", self.registry_id);
        let _ = writeln!(out, "candidate: {}", self.candidate_id);
        let _ = writeln!(out, "route family: {}", self.requested_route_family);
        let _ = writeln!(out, "status: {}", self.status.as_str());
        let _ = writeln!(
            out,
            "selected kernel: {}",
            self.selected_kernel_id.as_deref().unwrap_or("none")
        );
        let _ = writeln!(out, "reason: {}", self.admission_reason);
        let _ = writeln!(
            out,
            "runtime execution admitted: {}",
            self.runtime_execution_admitted
        );
        let _ = writeln!(out, "production claim allowed: false");
        let _ = writeln!(out, "fallback execution: disabled");
        out
    }
}

/// Builds the local specialized-kernel registry contract set.
#[must_use]
pub fn plan_vortex_specialized_kernel_registry() -> VortexSpecializedKernelRegistryReport {
    VortexSpecializedKernelRegistryReport::local_v1()
}

/// Selects and admits a specialized kernel from the local registry.
///
/// The function performs no IO and does not execute a kernel. It validates a
/// caller-supplied candidate against typed preconditions and the shared
/// `PhysicalKernelAdmissionReport` contract.
///
/// # Errors
/// Returns an error only if a static registry operator id fails contract construction.
#[allow(clippy::too_many_lines)]
pub fn admit_vortex_specialized_kernel(
    request: &VortexSpecializedKernelRequest,
) -> Result<VortexSpecializedKernelAdmissionReport> {
    let registry = plan_vortex_specialized_kernel_registry();
    let candidates = registry.candidate_contracts(request);
    if candidates.is_empty() {
        return Ok(blocked_admission(
            request,
            &registry,
            Vec::new(),
            None,
            VortexSpecializedKernelAdmissionStatus::BlockedUnsupportedRoute,
            "no specialized kernel contract matched the requested route family",
            Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "vortex_specialized_kernel_registry",
                format!(
                    "no specialized kernel contract matched route family {}",
                    request.route_family
                ),
                Some(
                    "Use an admitted ShardLoom-native route family or keep the route on its existing lower-tier native path."
                        .to_string(),
                ),
            ),
        ));
    }

    let contract = candidates[0];
    let candidate_kernel_ids = candidates
        .iter()
        .map(|candidate| candidate.kernel_id.to_string())
        .collect::<Vec<_>>();

    if request.fallback_attempted
        || request.fallback_execution_allowed
        || request.external_engine_invoked
    {
        return Ok(blocked_admission(
            request,
            &registry,
            candidate_kernel_ids,
            Some(contract),
            VortexSpecializedKernelAdmissionStatus::BlockedUnsafeEffects,
            "fallback or external execution evidence blocks specialized kernel admission",
            no_fallback_diagnostic(),
        ));
    }
    if request.operator_kind != contract.operator_kind {
        return Ok(blocked_admission(
            request,
            &registry,
            candidate_kernel_ids,
            Some(contract),
            VortexSpecializedKernelAdmissionStatus::BlockedOperatorMismatch,
            "candidate operator kind does not match the selected specialized kernel",
            Diagnostic::invalid_input(
                "vortex_specialized_kernel_registry",
                format!(
                    "operator kind {} does not match specialized kernel operator {}",
                    request.operator_kind.as_str(),
                    contract.operator_kind.as_str()
                ),
                "Select a kernel route whose operator family matches the physical operator.",
            ),
        ));
    }
    if !contract.supports_primitive(request.primitive_kind) {
        return Ok(blocked_admission(
            request,
            &registry,
            candidate_kernel_ids,
            Some(contract),
            VortexSpecializedKernelAdmissionStatus::BlockedPrimitiveMismatch,
            "candidate query primitive does not match the selected specialized kernel",
            Diagnostic::invalid_input(
                "vortex_specialized_kernel_registry",
                "query primitive kind is not admitted by the specialized kernel contract",
                "Use a matching Vortex query primitive or leave the route on its existing native plan.",
            ),
        ));
    }
    if !contract.supports_dtype(&request.logical_dtype) {
        return Ok(blocked_admission(
            request,
            &registry,
            candidate_kernel_ids,
            Some(contract),
            VortexSpecializedKernelAdmissionStatus::BlockedLogicalDtype,
            "candidate logical dtype does not satisfy the specialized kernel contract",
            Diagnostic::invalid_input(
                "vortex_specialized_kernel_registry",
                format!(
                    "logical dtype {} is not admitted for kernel {}",
                    request.logical_dtype, contract.kernel_id
                ),
                "Use an admitted logical dtype or implement a separate kernel contract.",
            ),
        ));
    }
    if !contract.supports_encoding(&request.encoding_layout) {
        return Ok(blocked_admission(
            request,
            &registry,
            candidate_kernel_ids,
            Some(contract),
            VortexSpecializedKernelAdmissionStatus::BlockedEncodingLayout,
            "candidate encoding/layout does not satisfy the specialized kernel contract",
            Diagnostic::invalid_input(
                "vortex_specialized_kernel_registry",
                format!(
                    "encoding/layout {} is not admitted for kernel {}",
                    request.encoding_layout, contract.kernel_id
                ),
                "Use an admitted encoded layout or keep the route on a lower-tier native path.",
            ),
        ));
    }
    if !contract.supports_null_semantics(&request.null_semantics) {
        return Ok(blocked_admission(
            request,
            &registry,
            candidate_kernel_ids,
            Some(contract),
            VortexSpecializedKernelAdmissionStatus::BlockedNullSemantics,
            "candidate null semantics do not satisfy the specialized kernel contract",
            Diagnostic::invalid_input(
                "vortex_specialized_kernel_registry",
                format!(
                    "null semantics {} are not admitted for kernel {}",
                    request.null_semantics, contract.kernel_id
                ),
                "Add a null-handling contract and decoded-reference tests before admitting this shape.",
            ),
        ));
    }
    if request.materialization_level != contract.materialization_level
        || request.data_decoded
        || materialized_before_boundary(request, contract)
    {
        return Ok(blocked_admission(
            request,
            &registry,
            candidate_kernel_ids,
            Some(contract),
            VortexSpecializedKernelAdmissionStatus::BlockedMaterializationBoundary,
            "candidate materialization/decode posture violates the specialized kernel contract",
            Diagnostic::invalid_input(
                "vortex_specialized_kernel_registry",
                "specialized kernel admission requires the declared materialization boundary to match the selected contract",
                "Keep rows encoded, columnar, or row-ref retained until the declared sink boundary.",
            ),
        ));
    }

    let physical_admission = physical_admission_for_contract(contract, request)?;
    if !physical_admission.status.can_enter_registry() || !request.decoded_reference_compared {
        let mut diagnostic = Diagnostic::not_implemented(
            "vortex_specialized_kernel_registry",
            format!(
                "specialized kernel physical admission is blocked with status {}",
                physical_admission.status.as_str()
            ),
            "Provide decoded-reference correctness evidence, memory safety, and no-fallback proof before selecting this kernel.",
        );
        if !request.decoded_reference_compared {
            diagnostic = Diagnostic::not_implemented(
                "vortex_specialized_kernel_registry",
                "specialized kernel admission requires decoded-reference comparison evidence",
                "Run the focused kernel parity fixture before admitting this kernel.",
            );
        }
        return Ok(blocked_admission_with_physical_status(
            request,
            &registry,
            candidate_kernel_ids,
            contract,
            VortexSpecializedKernelAdmissionStatus::BlockedPhysicalAdmission,
            "physical admission or decoded-reference evidence is missing",
            physical_admission.status,
            diagnostic,
        ));
    }

    Ok(VortexSpecializedKernelAdmissionReport {
        schema_version: VORTEX_SPECIALIZED_KERNEL_REGISTRY_SCHEMA_VERSION,
        registry_id: registry.registry_id,
        candidate_id: request.candidate_id.clone(),
        requested_route_family: request.route_family.clone(),
        candidate_kernel_ids,
        selected_kernel_id: Some(contract.kernel_id.to_string()),
        status: VortexSpecializedKernelAdmissionStatus::Admitted,
        physical_admission_status: Some(physical_admission.status),
        operator_kind: request.operator_kind,
        kernel_kind: Some(contract.kernel_kind),
        execution_level: Some(contract.execution_level),
        materialization_level: contract.materialization_level,
        runtime_lane: Some(contract.runtime_lane),
        admission_reason: if physical_admission.status
            == PhysicalKernelAdmissionStatus::ProductionReady
        {
            "kernel admitted with retained benchmark evidence".to_string()
        } else {
            "kernel admitted for native registry selection; production benchmark claim remains blocked"
                .to_string()
        },
        input_contract: contract.input_contract.to_string(),
        output_contract: contract.output_contract.to_string(),
        correctness_contract: contract.correctness_contract.to_string(),
        vortex_provider_surface: contract.vortex_provider_surface.to_string(),
        decoded_reference_compared: request.decoded_reference_compared,
        correctness_evidence: request.correctness_evidence,
        benchmark_evidence: request.benchmark_evidence,
        runtime_execution_admitted: contract.runtime_execution_admitted(),
        production_claim_allowed: physical_admission.can_satisfy_production_claim()
            && contract.production_claim_allowed_by_contract(),
        data_decoded: request.data_decoded,
        rows_materialized: request.rows_materialized,
        fallback_attempted: false,
        fallback_execution_allowed: false,
        external_engine_invoked: false,
        diagnostics: Vec::new(),
    })
}

fn blocked_admission(
    request: &VortexSpecializedKernelRequest,
    registry: &VortexSpecializedKernelRegistryReport,
    candidate_kernel_ids: Vec<String>,
    contract: Option<&VortexSpecializedKernelContract>,
    status: VortexSpecializedKernelAdmissionStatus,
    reason: impl Into<String>,
    diagnostic: Diagnostic,
) -> VortexSpecializedKernelAdmissionReport {
    blocked_admission_base(
        request,
        registry,
        candidate_kernel_ids,
        contract,
        status,
        reason,
        None,
        diagnostic,
    )
}

#[allow(clippy::too_many_arguments)]
fn blocked_admission_with_physical_status(
    request: &VortexSpecializedKernelRequest,
    registry: &VortexSpecializedKernelRegistryReport,
    candidate_kernel_ids: Vec<String>,
    contract: &VortexSpecializedKernelContract,
    status: VortexSpecializedKernelAdmissionStatus,
    reason: impl Into<String>,
    physical_admission_status: PhysicalKernelAdmissionStatus,
    diagnostic: Diagnostic,
) -> VortexSpecializedKernelAdmissionReport {
    blocked_admission_base(
        request,
        registry,
        candidate_kernel_ids,
        Some(contract),
        status,
        reason,
        Some(physical_admission_status),
        diagnostic,
    )
}

#[allow(clippy::too_many_arguments)]
fn blocked_admission_base(
    request: &VortexSpecializedKernelRequest,
    registry: &VortexSpecializedKernelRegistryReport,
    candidate_kernel_ids: Vec<String>,
    contract: Option<&VortexSpecializedKernelContract>,
    status: VortexSpecializedKernelAdmissionStatus,
    reason: impl Into<String>,
    physical_admission_status: Option<PhysicalKernelAdmissionStatus>,
    diagnostic: Diagnostic,
) -> VortexSpecializedKernelAdmissionReport {
    VortexSpecializedKernelAdmissionReport {
        schema_version: VORTEX_SPECIALIZED_KERNEL_REGISTRY_SCHEMA_VERSION,
        registry_id: registry.registry_id,
        candidate_id: request.candidate_id.clone(),
        requested_route_family: request.route_family.clone(),
        candidate_kernel_ids,
        selected_kernel_id: contract.map(|contract| contract.kernel_id.to_string()),
        status,
        physical_admission_status,
        operator_kind: request.operator_kind,
        kernel_kind: contract.map(|contract| contract.kernel_kind),
        execution_level: contract.map(|contract| contract.execution_level),
        materialization_level: contract.map_or(request.materialization_level, |contract| {
            contract.materialization_level
        }),
        runtime_lane: contract.map(|contract| contract.runtime_lane),
        admission_reason: reason.into(),
        input_contract: contract
            .map_or_else(String::new, |contract| contract.input_contract.to_string()),
        output_contract: contract
            .map_or_else(String::new, |contract| contract.output_contract.to_string()),
        correctness_contract: contract.map_or_else(String::new, |contract| {
            contract.correctness_contract.to_string()
        }),
        vortex_provider_surface: contract.map_or_else(String::new, |contract| {
            contract.vortex_provider_surface.to_string()
        }),
        decoded_reference_compared: request.decoded_reference_compared,
        correctness_evidence: request.correctness_evidence,
        benchmark_evidence: request.benchmark_evidence,
        runtime_execution_admitted: false,
        production_claim_allowed: false,
        data_decoded: request.data_decoded,
        rows_materialized: request.rows_materialized,
        fallback_attempted: request.fallback_attempted,
        fallback_execution_allowed: request.fallback_execution_allowed,
        external_engine_invoked: request.external_engine_invoked,
        diagnostics: vec![diagnostic],
    }
}

fn physical_admission_for_contract(
    contract: &VortexSpecializedKernelContract,
    request: &VortexSpecializedKernelRequest,
) -> Result<PhysicalKernelAdmissionReport> {
    let slot = contract_kernel_slot(contract)?;
    Ok(PhysicalKernelAdmissionReport::evaluate(
        &slot,
        contract.kernel_kind,
        request.correctness_evidence,
        request.benchmark_evidence,
        contract.memory,
        if request.fallback_attempted || request.fallback_execution_allowed {
            BenchmarkFallbackState::Attempted
        } else {
            BenchmarkFallbackState::NotAttempted
        },
    ))
}

fn contract_kernel_slot(contract: &VortexSpecializedKernelContract) -> Result<PhysicalKernelSlot> {
    let operator = PhysicalOperatorContract::new(
        format!(
            "{}.{}",
            VORTEX_SPECIALIZED_KERNEL_REGISTRY_ID, contract.kernel_id
        ),
        contract.operator_kind,
        contract.execution_level,
        vec![PhysicalKernelRequirement::missing(contract.kernel_kind)],
    )?;
    Ok(PhysicalKernelSlot::from_requirement(
        &operator,
        PhysicalKernelRequirement::missing(contract.kernel_kind),
    ))
}

fn no_fallback_diagnostic() -> Diagnostic {
    Diagnostic::unsupported(
        DiagnosticCode::NoFallbackExecution,
        "vortex_specialized_kernel_registry",
        "specialized kernel admission rejects fallback or external-engine execution evidence",
        Some(
            "Fallback attempted: false and external engine invoked: false are required."
                .to_string(),
        ),
    )
}

fn materialized_before_boundary(
    request: &VortexSpecializedKernelRequest,
    contract: &VortexSpecializedKernelContract,
) -> bool {
    request.rows_materialized > 0
        && !matches!(
            contract.materialization_level,
            VortexKernelMaterializationLevel::SinkBoundaryOnly
        )
}

fn token_matches(allowed: &[&str], observed: &str) -> bool {
    allowed.is_empty()
        || allowed.contains(&"any")
        || allowed.iter().any(|token| {
            observed == *token
                || observed
                    .split(['+', '|', ',', ';', ':'])
                    .any(|part| part == *token)
        })
}

fn registry_contract_value_rows(
    contracts: &[VortexSpecializedKernelContract],
    value: impl Fn(&VortexSpecializedKernelContract) -> String,
) -> String {
    contracts.iter().map(value).collect::<Vec<_>>().join("|")
}

const fn safe_metadata_memory() -> OperatorMemoryCertification {
    OperatorMemoryCertification {
        streaming: true,
        bounded_memory: true,
        spillable: false,
        requires_full_materialization: false,
        requires_shuffle: false,
        oom_safe: true,
    }
}

const fn safe_encoded_memory() -> OperatorMemoryCertification {
    OperatorMemoryCertification {
        streaming: true,
        bounded_memory: true,
        spillable: false,
        requires_full_materialization: false,
        requires_shuffle: false,
        oom_safe: true,
    }
}

#[allow(clippy::too_many_lines)]
fn local_kernel_contracts() -> Vec<VortexSpecializedKernelContract> {
    vec![
        VortexSpecializedKernelContract {
            kernel_id: "metadata_count_primitive",
            route_families: &["metadata_count", "count_all", "count_where"],
            query_primitive_kinds: &[
                VortexQueryPrimitiveKind::CountAll,
                VortexQueryPrimitiveKind::CountWhere,
            ],
            operator_kind: PhysicalOperatorKind::CountAggregate,
            kernel_kind: KernelKind::Metadata,
            execution_level: PhysicalOperatorExecutionLevel::MetadataOnly,
            logical_dtypes: &["row_count_metadata"],
            encoding_layouts: &["vortex_footer_statistics", "segment_metadata_primitive"],
            null_semantics: &["not_applicable"],
            materialization_level: VortexKernelMaterializationLevel::MetadataOnly,
            determinism_contract: "metadata row-count provenance must be exact and segment-covered",
            memory: safe_metadata_memory(),
            vortex_provider_surface: "Vortex footer/statistics metadata through ShardLoom segment metadata primitive",
            input_contract: "proven row-count metadata with no sidecar query answer",
            output_contract: "scalar count result",
            correctness_contract: "row count provenance and decoded-reference parity fixture required before admission",
            runtime_lane: VortexSpecializedKernelRuntimeLane::ExistingNativeExecutor,
            benchmark_required_for_production: true,
            fallback_execution_allowed: false,
        },
        VortexSpecializedKernelContract {
            kernel_id: "exact_predicate_count",
            route_families: &[
                "string_predicate_count",
                "exact_predicate_count",
                "selective_filter",
            ],
            query_primitive_kinds: &[
                VortexQueryPrimitiveKind::CountWhere,
                VortexQueryPrimitiveKind::FilterPredicate,
            ],
            operator_kind: PhysicalOperatorKind::Filter,
            kernel_kind: KernelKind::Encoded,
            execution_level: PhysicalOperatorExecutionLevel::EncodedNative,
            logical_dtypes: &["bool", "integer", "utf8", "any_scalar"],
            encoding_layouts: &[
                "fastlanes.bitpacked",
                "vortex.sequence",
                "vortex.constant",
                "dictionary",
                "fsst_or_dictionary_string",
            ],
            null_semantics: &[
                "non_null",
                "nullable_three_valued_logic",
                "constant_null",
                "metadata_proven_all_none",
            ],
            materialization_level: VortexKernelMaterializationLevel::EncodedNoMaterialization,
            determinism_contract: "selection vectors must be emitted in segment/source order",
            memory: safe_encoded_memory(),
            vortex_provider_surface: "reader-generated encoded kernel inputs plus ShardLoom selection-vector filter kernel",
            input_contract: "encoded predicate batch with dtype, encoding, values, and row-count mapping evidence",
            output_contract: "selection vectors and exact selected-row count",
            correctness_contract: "decoded predicate parity over empty, all-null, mixed-null, dense, sparse, and dictionary fixtures",
            runtime_lane: VortexSpecializedKernelRuntimeLane::ExistingNativeExecutor,
            benchmark_required_for_production: true,
            fallback_execution_allowed: false,
        },
        VortexSpecializedKernelContract {
            kernel_id: "string_heavy_hitter_topk",
            route_families: &[
                "string_heavy_hitter_topk",
                "source_state_generated_string_domain_grouping",
            ],
            query_primitive_kinds: &[VortexQueryPrimitiveKind::SimpleAggregate],
            operator_kind: PhysicalOperatorKind::TopK,
            kernel_kind: KernelKind::Encoded,
            execution_level: PhysicalOperatorExecutionLevel::EncodedNative,
            logical_dtypes: &["utf8", "dictionary_utf8"],
            encoding_layouts: &["dictionary", "vortex.dict", "utf8_chunk_dictionary"],
            null_semantics: &["non_null", "nullable_group_key"],
            materialization_level: VortexKernelMaterializationLevel::ColumnarState,
            determinism_contract: "top-K ties use aggregate order terms then stable key order",
            memory: safe_encoded_memory(),
            vortex_provider_surface: "ShardLoom grouped aggregate compact dictionary state",
            input_contract: "dictionary or direct UTF-8 group key batches with count/sum partials",
            output_contract: "columnar aggregate state with retained top-K candidates",
            correctness_contract: "decoded group-by/top-K parity across low/high cardinality and null-key fixtures",
            runtime_lane: VortexSpecializedKernelRuntimeLane::ExistingNativeExecutor,
            benchmark_required_for_production: true,
            fallback_execution_allowed: false,
        },
        VortexSpecializedKernelContract {
            kernel_id: "transformed_dictionary_url_domain_grouping",
            route_families: &["transformed_url_domain_grouping", "url_domain_grouping"],
            query_primitive_kinds: &[VortexQueryPrimitiveKind::SimpleAggregate],
            operator_kind: PhysicalOperatorKind::Aggregate,
            kernel_kind: KernelKind::Encoded,
            execution_level: PhysicalOperatorExecutionLevel::EncodedNative,
            logical_dtypes: &["utf8", "dictionary_utf8"],
            encoding_layouts: &["dictionary", "vortex.dict", "utf8_chunk_dictionary"],
            null_semantics: &["non_null", "nullable_group_key"],
            materialization_level: VortexKernelMaterializationLevel::ColumnarState,
            determinism_contract: "domain transform is deterministic and keyed before aggregate merge",
            memory: safe_encoded_memory(),
            vortex_provider_surface: "ShardLoom URL/domain transform over dictionary/direct UTF-8 accessors",
            input_contract: "URL UTF-8 accessor plus declared deterministic domain transform",
            output_contract: "columnar grouped aggregate state keyed by transformed domain",
            correctness_contract: "decoded transform/group parity including empty, missing-host, mixed-case, and null URL fixtures",
            runtime_lane: VortexSpecializedKernelRuntimeLane::ExistingNativeExecutor,
            benchmark_required_for_production: true,
            fallback_execution_allowed: false,
        },
        VortexSpecializedKernelContract {
            kernel_id: "numeric_pair_aggregate",
            route_families: &["numeric_pair_aggregate", "compact_numeric_measures"],
            query_primitive_kinds: &[VortexQueryPrimitiveKind::SimpleAggregate],
            operator_kind: PhysicalOperatorKind::Aggregate,
            kernel_kind: KernelKind::Encoded,
            execution_level: PhysicalOperatorExecutionLevel::EncodedNative,
            logical_dtypes: &["integer", "float", "numeric_pair"],
            encoding_layouts: &["primitive", "vortex.primitive", "direct_primitive"],
            null_semantics: &["non_null", "nullable_measure_skip_null"],
            materialization_level: VortexKernelMaterializationLevel::ColumnarState,
            determinism_contract: "integer sums are exact; floating aggregation order is fixed by source segment then row ordinal",
            memory: safe_encoded_memory(),
            vortex_provider_surface: "ShardLoom compact numeric aggregate measure accessors",
            input_contract: "direct numeric columns with declared aggregate functions and null policy",
            output_contract: "columnar scalar or grouped aggregate partial state",
            correctness_contract: "decoded aggregate parity for empty, all-null, mixed-null, integer, and floating fixtures",
            runtime_lane: VortexSpecializedKernelRuntimeLane::ExistingNativeExecutor,
            benchmark_required_for_production: true,
            fallback_execution_allowed: false,
        },
        VortexSpecializedKernelContract {
            kernel_id: "numeric_utf8_grouped_topk",
            route_families: &[
                "numeric_utf8_grouped_topk",
                "high_cardinality_grouped_topk",
                "source_order_numeric_utf8_dictionary_slots",
            ],
            query_primitive_kinds: &[VortexQueryPrimitiveKind::SimpleAggregate],
            operator_kind: PhysicalOperatorKind::TopK,
            kernel_kind: KernelKind::Encoded,
            execution_level: PhysicalOperatorExecutionLevel::EncodedNative,
            logical_dtypes: &["numeric_utf8", "integer", "utf8"],
            encoding_layouts: &[
                "primitive+dictionary",
                "vortex.primitive+vortex.dict",
                "direct_primitive+utf8_chunk_dictionary",
            ],
            null_semantics: &[
                "non_null",
                "nullable_group_key",
                "nullable_measure_skip_null",
            ],
            materialization_level: VortexKernelMaterializationLevel::ColumnarState,
            determinism_contract: "grouped top-K ties use aggregate order terms, source order, and stable typed key ordering",
            memory: safe_encoded_memory(),
            vortex_provider_surface: "ShardLoom source-order numeric plus UTF-8 dictionary grouped aggregate slots",
            input_contract: "numeric direct key/measure plus UTF-8 dictionary key accessors",
            output_contract: "columnar grouped top-K candidate state",
            correctness_contract: "decoded grouped top-K parity across ties, nulls, high-cardinality keys, and offset/limit fixtures",
            runtime_lane: VortexSpecializedKernelRuntimeLane::ExistingNativeExecutor,
            benchmark_required_for_production: true,
            fallback_execution_allowed: false,
        },
        VortexSpecializedKernelContract {
            kernel_id: "dense_exact_distinct",
            route_families: &["dense_exact_distinct", "exact_distinct"],
            query_primitive_kinds: &[
                VortexQueryPrimitiveKind::DistinctRows,
                VortexQueryPrimitiveKind::SimpleAggregate,
            ],
            operator_kind: PhysicalOperatorKind::Aggregate,
            kernel_kind: KernelKind::Encoded,
            execution_level: PhysicalOperatorExecutionLevel::EncodedNative,
            logical_dtypes: &["integer", "utf8", "dictionary_utf8", "any_scalar"],
            encoding_layouts: &[
                "dense_integer",
                "dictionary",
                "direct_primitive",
                "vortex.dict",
            ],
            null_semantics: &["non_null", "nullable_distinct_single_null"],
            materialization_level: VortexKernelMaterializationLevel::ColumnarState,
            determinism_contract: "exact distinct emits stable typed-key order after deterministic set merge",
            memory: safe_encoded_memory(),
            vortex_provider_surface: "ShardLoom exact distinct dense preunion and dictionary/direct accessors",
            input_contract: "typed scalar or dictionary values with exact key equality semantics",
            output_contract: "columnar exact distinct state or scalar cardinality",
            correctness_contract: "decoded exact-distinct parity including empty, all-null, mixed-null, dense, sparse, and dictionary fixtures",
            runtime_lane: VortexSpecializedKernelRuntimeLane::ExistingNativeExecutor,
            benchmark_required_for_production: true,
            fallback_execution_allowed: false,
        },
        VortexSpecializedKernelContract {
            kernel_id: "row_ref_topk",
            route_families: &["row_ref_topk", "bounded_wide_row_topk", "sort_and_topk"],
            query_primitive_kinds: &[VortexQueryPrimitiveKind::SortRows],
            operator_kind: PhysicalOperatorKind::TopK,
            kernel_kind: KernelKind::Encoded,
            execution_level: PhysicalOperatorExecutionLevel::HybridNative,
            logical_dtypes: &["ordered_scalar", "integer", "float", "timestamp"],
            encoding_layouts: &[
                "primitive",
                "direct_primitive",
                "vortex.sequence",
                "vortex.primitive",
            ],
            null_semantics: &["non_null", "nullable_sort_nulls_declared"],
            materialization_level: VortexKernelMaterializationLevel::RowRefsOnly,
            determinism_contract: "row-ref top-K ties use order terms, null ordering, and source ordinal",
            memory: safe_encoded_memory(),
            vortex_provider_surface: "ShardLoom row-ref top-K candidate state with late selected-row materialization",
            input_contract: "order-key encoded batches plus source row ordinal mapping",
            output_contract: "retained row refs and order-key columns until sink materialization",
            correctness_contract: "decoded sort/top-K parity for ties, null ordering, limit/offset, and wide projections",
            runtime_lane: VortexSpecializedKernelRuntimeLane::ExistingNativeExecutor,
            benchmark_required_for_production: true,
            fallback_execution_allowed: false,
        },
        VortexSpecializedKernelContract {
            kernel_id: "direct_primitive_aggregate",
            route_families: &[
                "direct_primitive_aggregate",
                "count_star_direct",
                "direct_accessor_count_distinct_group_update",
            ],
            query_primitive_kinds: &[VortexQueryPrimitiveKind::SimpleAggregate],
            operator_kind: PhysicalOperatorKind::Aggregate,
            kernel_kind: KernelKind::Encoded,
            execution_level: PhysicalOperatorExecutionLevel::EncodedNative,
            logical_dtypes: &["integer", "float", "bool", "timestamp", "any_scalar"],
            encoding_layouts: &[
                "primitive",
                "direct_primitive",
                "vortex.primitive",
                "vortex.constant",
            ],
            null_semantics: &["non_null", "nullable_measure_skip_null", "constant_null"],
            materialization_level: VortexKernelMaterializationLevel::ColumnarState,
            determinism_contract: "primitive aggregate state merges in stable segment and ordinal order",
            memory: safe_encoded_memory(),
            vortex_provider_surface: "ShardLoom direct primitive aggregate accessors",
            input_contract: "direct primitive aggregate measure/key columns",
            output_contract: "columnar scalar or grouped aggregate state",
            correctness_contract: "decoded aggregate parity over empty, all-null, mixed-null, min/max/sum/count/count-distinct fixtures",
            runtime_lane: VortexSpecializedKernelRuntimeLane::ExistingNativeExecutor,
            benchmark_required_for_production: true,
            fallback_execution_allowed: false,
        },
        VortexSpecializedKernelContract {
            kernel_id: "selection_vector_filter",
            route_families: &["selection_vector_filter", "filter_project_selection_vector"],
            query_primitive_kinds: &[
                VortexQueryPrimitiveKind::FilterPredicate,
                VortexQueryPrimitiveKind::FilterAndProject,
            ],
            operator_kind: PhysicalOperatorKind::Filter,
            kernel_kind: KernelKind::Encoded,
            execution_level: PhysicalOperatorExecutionLevel::EncodedNative,
            logical_dtypes: &["bool", "integer", "utf8", "any_scalar"],
            encoding_layouts: &[
                "selection_vector",
                "fastlanes.bitpacked",
                "vortex.sequence",
                "dictionary",
            ],
            null_semantics: &[
                "non_null",
                "nullable_three_valued_logic",
                "metadata_proven_all_none",
            ],
            materialization_level: VortexKernelMaterializationLevel::EncodedNoMaterialization,
            determinism_contract: "selection vector output preserves segment order and row ordinal ordering",
            memory: safe_encoded_memory(),
            vortex_provider_surface: "ShardLoom encoded predicate evaluation and selection-vector filter kernel",
            input_contract: "predicate evaluation report with one selection vector per segment",
            output_contract: "selection vectors consumed by downstream project/count kernels",
            correctness_contract: "decoded filter parity for all/none/mixed selections and unsupported layouts",
            runtime_lane: VortexSpecializedKernelRuntimeLane::ExistingNativeExecutor,
            benchmark_required_for_production: true,
            fallback_execution_allowed: false,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admitted_request(
        route: &str,
        operator: PhysicalOperatorKind,
    ) -> VortexSpecializedKernelRequest {
        VortexSpecializedKernelRequest::new(
            route,
            operator,
            "integer",
            "direct_primitive",
            "non_null",
        )
        .with_primitive_kind(VortexQueryPrimitiveKind::SimpleAggregate)
        .with_materialization_level(VortexKernelMaterializationLevel::ColumnarState)
        .with_correctness_evidence(BenchmarkEvidenceState::Present)
        .with_decoded_reference_compared(true)
    }

    #[test]
    fn registry_contains_first_hot_lane_contracts_without_fallback() {
        let registry = plan_vortex_specialized_kernel_registry();

        assert_eq!(
            registry.schema_version,
            VORTEX_SPECIALIZED_KERNEL_REGISTRY_SCHEMA_VERSION
        );
        for kernel_id in [
            "string_heavy_hitter_topk",
            "transformed_dictionary_url_domain_grouping",
            "numeric_pair_aggregate",
            "numeric_utf8_grouped_topk",
            "dense_exact_distinct",
            "row_ref_topk",
            "exact_predicate_count",
            "direct_primitive_aggregate",
        ] {
            let contract = registry
                .contract_for_kernel_id(kernel_id)
                .unwrap_or_else(|| panic!("missing contract {kernel_id}"));
            assert!(!contract.fallback_execution_allowed);
            assert!(contract.memory.oom_safe);
            assert!(!contract.memory.requires_full_materialization);
            assert!(contract.benchmark_required_for_production);
            assert!(!contract.production_claim_allowed_by_contract());
        }
    }

    #[test]
    fn direct_primitive_aggregate_admits_with_correctness_and_no_fallback() {
        let request = admitted_request(
            "direct_primitive_aggregate",
            PhysicalOperatorKind::Aggregate,
        );

        let admission = admit_vortex_specialized_kernel(&request).expect("admission");

        assert!(admission.is_admitted());
        assert_eq!(
            admission.selected_kernel_id.as_deref(),
            Some("direct_primitive_aggregate")
        );
        assert_eq!(
            admission.physical_admission_status,
            Some(PhysicalKernelAdmissionStatus::RegistryReady)
        );
        assert!(admission.runtime_execution_admitted);
        assert!(!admission.production_claim_allowed);
        assert!(!admission.has_errors());
        assert_eq!(admission.rows_materialized, 0);
    }

    #[test]
    fn row_ref_topk_blocks_early_materialization() {
        let request = VortexSpecializedKernelRequest::new(
            "row_ref_topk",
            PhysicalOperatorKind::TopK,
            "ordered_scalar",
            "direct_primitive",
            "nullable_sort_nulls_declared",
        )
        .with_primitive_kind(VortexQueryPrimitiveKind::SortRows)
        .with_materialization_level(VortexKernelMaterializationLevel::SinkBoundaryOnly)
        .with_correctness_evidence(BenchmarkEvidenceState::Present)
        .with_decoded_reference_compared(true)
        .with_rows_materialized(5);

        let admission = admit_vortex_specialized_kernel(&request).expect("admission");

        assert_eq!(
            admission.status,
            VortexSpecializedKernelAdmissionStatus::BlockedMaterializationBoundary
        );
        assert_eq!(
            admission.selected_kernel_id.as_deref(),
            Some("row_ref_topk")
        );
        assert!(admission.has_errors());
        assert!(!admission.fallback_attempted);
    }

    #[test]
    fn unsupported_route_blocks_with_deterministic_diagnostic() {
        let request = VortexSpecializedKernelRequest::new(
            "unregistered_hot_lane",
            PhysicalOperatorKind::Aggregate,
            "integer",
            "direct_primitive",
            "non_null",
        )
        .with_correctness_evidence(BenchmarkEvidenceState::Present)
        .with_decoded_reference_compared(true);

        let admission = admit_vortex_specialized_kernel(&request).expect("admission");

        assert_eq!(
            admission.status,
            VortexSpecializedKernelAdmissionStatus::BlockedUnsupportedRoute
        );
        assert_eq!(admission.selected_kernel_id, None);
        assert!(!admission.diagnostics.is_empty());
        assert!(
            admission
                .diagnostics
                .iter()
                .all(|diagnostic| !diagnostic.fallback.attempted)
        );
    }

    #[test]
    fn fallback_evidence_blocks_kernel_admission() {
        let request = admitted_request(
            "direct_primitive_aggregate",
            PhysicalOperatorKind::Aggregate,
        )
        .with_fallback_attempted(true);

        let admission = admit_vortex_specialized_kernel(&request).expect("admission");

        assert_eq!(
            admission.status,
            VortexSpecializedKernelAdmissionStatus::BlockedUnsafeEffects
        );
        assert_eq!(
            admission.selected_kernel_id.as_deref(),
            Some("direct_primitive_aggregate")
        );
        assert!(admission.fallback_attempted);
        assert!(admission.has_errors());
    }

    #[test]
    fn missing_decoded_reference_blocks_physical_admission() {
        let request = VortexSpecializedKernelRequest::new(
            "direct_primitive_aggregate",
            PhysicalOperatorKind::Aggregate,
            "integer",
            "direct_primitive",
            "non_null",
        )
        .with_primitive_kind(VortexQueryPrimitiveKind::SimpleAggregate)
        .with_materialization_level(VortexKernelMaterializationLevel::ColumnarState)
        .with_correctness_evidence(BenchmarkEvidenceState::Present);

        let admission = admit_vortex_specialized_kernel(&request).expect("admission");

        assert_eq!(
            admission.status,
            VortexSpecializedKernelAdmissionStatus::BlockedPhysicalAdmission
        );
        assert_eq!(
            admission.physical_admission_status,
            Some(PhysicalKernelAdmissionStatus::RegistryReady)
        );
        assert!(admission.has_errors());
    }
}
