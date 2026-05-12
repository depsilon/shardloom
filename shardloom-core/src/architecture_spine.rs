//! Modular compute-engine architecture spine.
//!
//! This module is report-only. It names the engine layers, provider taxonomy,
//! registry requirements, shared data-model primitives, runtime graph
//! prerequisites, and evidence outputs that implementation work must preserve
//! before broad execution or support claims are allowed.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComputeEngineLayerKind {
    FrontendAdapter,
    LogicalIr,
    SemanticProfileBinding,
    CapabilityAdmission,
    OptimizerRewrite,
    PhysicalPlanning,
    ExecutionProviderSelection,
    SchedulerRuntime,
    SinkDelivery,
    EvidenceArtifactEmission,
}

impl ComputeEngineLayerKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FrontendAdapter => "frontend_adapter",
            Self::LogicalIr => "logical_ir",
            Self::SemanticProfileBinding => "semantic_profile_binding",
            Self::CapabilityAdmission => "capability_admission",
            Self::OptimizerRewrite => "optimizer_rewrite",
            Self::PhysicalPlanning => "physical_planning",
            Self::ExecutionProviderSelection => "execution_provider_selection",
            Self::SchedulerRuntime => "scheduler_runtime",
            Self::SinkDelivery => "sink_delivery",
            Self::EvidenceArtifactEmission => "evidence_artifact_emission",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputeEngineLayerContract {
    pub layer: ComputeEngineLayerKind,
    pub owner_crates: Vec<&'static str>,
    pub input_contracts: Vec<&'static str>,
    pub output_contracts: Vec<&'static str>,
    pub runtime_execution_allowed: bool,
    pub fallback_execution_allowed: bool,
}

impl ComputeEngineLayerContract {
    #[must_use]
    pub fn new(
        layer: ComputeEngineLayerKind,
        owner_crates: Vec<&'static str>,
        input_contracts: Vec<&'static str>,
        output_contracts: Vec<&'static str>,
    ) -> Self {
        Self {
            layer,
            owner_crates,
            input_contracts,
            output_contracts,
            runtime_execution_allowed: false,
            fallback_execution_allowed: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionProviderKind {
    ShardLoomKernel,
    ShardLoomMetadata,
    VortexArrayKernel,
    VortexComputeFunction,
    VortexScan,
    VortexSource,
    VortexSink,
    CompatibilityImport,
    CompatibilityExport,
    ExternalBaseline,
    ProhibitedExternalFallback,
}

impl ExecutionProviderKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ShardLoomKernel => "shardloom_kernel",
            Self::ShardLoomMetadata => "shardloom_metadata",
            Self::VortexArrayKernel => "vortex_array_kernel",
            Self::VortexComputeFunction => "vortex_compute_function",
            Self::VortexScan => "vortex_scan",
            Self::VortexSource => "vortex_source",
            Self::VortexSink => "vortex_sink",
            Self::CompatibilityImport => "compatibility_import",
            Self::CompatibilityExport => "compatibility_export",
            Self::ExternalBaseline => "external_baseline",
            Self::ProhibitedExternalFallback => "prohibited_external_fallback",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionProviderContract {
    pub kind: ExecutionProviderKind,
    pub role: ExecutionProviderRole,
    pub fallback_policy: FallbackExecutionPolicy,
    pub certificate_policy: CertificatePolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionProviderRole {
    NativeProvider,
    CompatibilityBoundary,
    ExternalBaseline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackExecutionPolicy {
    Prohibited,
    Allowed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertificatePolicy {
    Required,
    NotRequired,
}

impl ExecutionProviderContract {
    #[must_use]
    pub const fn native(kind: ExecutionProviderKind) -> Self {
        Self {
            kind,
            role: ExecutionProviderRole::NativeProvider,
            fallback_policy: FallbackExecutionPolicy::Prohibited,
            certificate_policy: CertificatePolicy::Required,
        }
    }

    #[must_use]
    pub const fn compatibility_boundary(kind: ExecutionProviderKind) -> Self {
        Self {
            kind,
            role: ExecutionProviderRole::CompatibilityBoundary,
            fallback_policy: FallbackExecutionPolicy::Prohibited,
            certificate_policy: CertificatePolicy::Required,
        }
    }

    #[must_use]
    pub const fn external_baseline(kind: ExecutionProviderKind) -> Self {
        Self {
            kind,
            role: ExecutionProviderRole::ExternalBaseline,
            fallback_policy: FallbackExecutionPolicy::Prohibited,
            certificate_policy: CertificatePolicy::NotRequired,
        }
    }

    #[must_use]
    pub const fn native_provider(&self) -> bool {
        matches!(self.role, ExecutionProviderRole::NativeProvider)
    }

    #[must_use]
    pub const fn external_baseline_provider(&self) -> bool {
        matches!(self.role, ExecutionProviderRole::ExternalBaseline)
    }

    #[must_use]
    pub const fn fallback_prohibited(&self) -> bool {
        matches!(self.fallback_policy, FallbackExecutionPolicy::Prohibited)
    }

    #[must_use]
    pub const fn certificate_required(&self) -> bool {
        matches!(self.certificate_policy, CertificatePolicy::Required)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComputeRegistryKind {
    Operator,
    Function,
    Aggregate,
    Sketch,
    Window,
    Join,
    SortTopN,
    Sink,
}

impl ComputeRegistryKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Operator => "operator",
            Self::Function => "function",
            Self::Aggregate => "aggregate",
            Self::Sketch => "sketch",
            Self::Window => "window",
            Self::Join => "join",
            Self::SortTopN => "sort_top_n",
            Self::Sink => "sink",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputeRegistryContract {
    pub kind: ComputeRegistryKind,
    pub requirements: Vec<ComputeRegistryRequirement>,
    pub runtime_policy: RuntimeExecutionPolicy,
    pub fallback_policy: FallbackExecutionPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComputeRegistryRequirement {
    TypedCapabilityDescriptor,
    SemanticProfile,
    StateContract,
    MemoryDeclaration,
    MaterializationRequirement,
    CertificateRequirement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeExecutionPolicy {
    ReportOnlyUntilCertified,
    ExecutionAllowed,
}

impl ComputeRegistryContract {
    #[must_use]
    pub fn required(kind: ComputeRegistryKind, state_contract_required: bool) -> Self {
        let mut requirements = vec![
            ComputeRegistryRequirement::TypedCapabilityDescriptor,
            ComputeRegistryRequirement::SemanticProfile,
            ComputeRegistryRequirement::MemoryDeclaration,
            ComputeRegistryRequirement::MaterializationRequirement,
            ComputeRegistryRequirement::CertificateRequirement,
        ];
        if state_contract_required {
            requirements.push(ComputeRegistryRequirement::StateContract);
        }

        Self {
            kind,
            requirements,
            runtime_policy: RuntimeExecutionPolicy::ReportOnlyUntilCertified,
            fallback_policy: FallbackExecutionPolicy::Prohibited,
        }
    }

    #[must_use]
    pub fn is_claim_gated(&self) -> bool {
        self.has_requirement(ComputeRegistryRequirement::TypedCapabilityDescriptor)
            && self.has_requirement(ComputeRegistryRequirement::SemanticProfile)
            && self.has_requirement(ComputeRegistryRequirement::MemoryDeclaration)
            && self.has_requirement(ComputeRegistryRequirement::MaterializationRequirement)
            && self.has_requirement(ComputeRegistryRequirement::CertificateRequirement)
            && matches!(
                self.runtime_policy,
                RuntimeExecutionPolicy::ReportOnlyUntilCertified
            )
            && matches!(self.fallback_policy, FallbackExecutionPolicy::Prohibited)
    }

    #[must_use]
    pub fn has_requirement(&self, requirement: ComputeRegistryRequirement) -> bool {
        self.requirements.contains(&requirement)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedDataModelPrimitiveKind {
    LogicalDType,
    EncodedRepresentationState,
    SelectionVector,
    SegmentStatistics,
    NullSemantics,
    DictionaryEncoding,
    RunLengthEncoding,
    SparseEncoding,
    MaterializationBoundary,
    DecodeBoundary,
    NativeIoEnvelope,
}

impl SharedDataModelPrimitiveKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LogicalDType => "logical_dtype",
            Self::EncodedRepresentationState => "encoded_representation_state",
            Self::SelectionVector => "selection_vector",
            Self::SegmentStatistics => "segment_statistics",
            Self::NullSemantics => "null_semantics",
            Self::DictionaryEncoding => "dictionary_encoding",
            Self::RunLengthEncoding => "run_length_encoding",
            Self::SparseEncoding => "sparse_encoding",
            Self::MaterializationBoundary => "materialization_boundary",
            Self::DecodeBoundary => "decode_boundary",
            Self::NativeIoEnvelope => "native_io_envelope",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct RuntimeTaskGraphContract {
    pub task_split_graph_required: bool,
    pub dynamic_sizing_required: bool,
    pub target_task_policy_required: bool,
    pub bounded_queues_required: bool,
    pub cancellation_required: bool,
    pub retry_required: bool,
    pub backpressure_required: bool,
    pub memory_spill_reservation_required: bool,
    pub object_store_request_budget_required: bool,
    pub cost_fairness_accounting_required: bool,
    pub runtime_execution_allowed: bool,
    pub fallback_execution_allowed: bool,
}

impl Default for RuntimeTaskGraphContract {
    fn default() -> Self {
        Self {
            task_split_graph_required: true,
            dynamic_sizing_required: true,
            target_task_policy_required: true,
            bounded_queues_required: true,
            cancellation_required: true,
            retry_required: true,
            backpressure_required: true,
            memory_spill_reservation_required: true,
            object_store_request_budget_required: true,
            cost_fairness_accounting_required: true,
            runtime_execution_allowed: false,
            fallback_execution_allowed: false,
        }
    }
}

impl RuntimeTaskGraphContract {
    #[must_use]
    pub const fn complete_before_large_workload_claims(&self) -> bool {
        self.task_split_graph_required
            && self.dynamic_sizing_required
            && self.target_task_policy_required
            && self.bounded_queues_required
            && self.cancellation_required
            && self.retry_required
            && self.backpressure_required
            && self.memory_spill_reservation_required
            && self.object_store_request_budget_required
            && self.cost_fairness_accounting_required
            && !self.runtime_execution_allowed
            && !self.fallback_execution_allowed
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceOutputKind {
    EvidenceArtifact,
    Diagnostic,
    LineageFacet,
    Profile,
    BenchmarkRow,
    ExecutionCertificate,
    NativeIoCertificate,
}

impl EvidenceOutputKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EvidenceArtifact => "evidence_artifact",
            Self::Diagnostic => "diagnostic",
            Self::LineageFacet => "lineage_facet",
            Self::Profile => "profile",
            Self::BenchmarkRow => "benchmark_row",
            Self::ExecutionCertificate => "execution_certificate",
            Self::NativeIoCertificate => "native_io_certificate",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct ComputeEngineArchitectureSpineReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub layers: Vec<ComputeEngineLayerContract>,
    pub execution_providers: Vec<ExecutionProviderContract>,
    pub registries: Vec<ComputeRegistryContract>,
    pub shared_primitives: Vec<SharedDataModelPrimitiveKind>,
    pub runtime_task_graph: RuntimeTaskGraphContract,
    pub evidence_outputs: Vec<EvidenceOutputKind>,
    pub deterministic_unsupported_behavior_required: bool,
    pub runtime_execution_allowed: bool,
    pub fallback_execution_allowed: bool,
}

impl ComputeEngineArchitectureSpineReport {
    #[must_use]
    pub fn default_modular_spine() -> Self {
        Self {
            schema_version: "shardloom.compute_engine_architecture_spine.v1",
            report_id: "priority-1.6.compute-engine-architecture-spine".to_string(),
            layers: default_layers(),
            execution_providers: default_execution_providers(),
            registries: default_registries(),
            shared_primitives: default_shared_primitives(),
            runtime_task_graph: RuntimeTaskGraphContract::default(),
            evidence_outputs: vec![
                EvidenceOutputKind::EvidenceArtifact,
                EvidenceOutputKind::Diagnostic,
                EvidenceOutputKind::LineageFacet,
                EvidenceOutputKind::Profile,
                EvidenceOutputKind::BenchmarkRow,
                EvidenceOutputKind::ExecutionCertificate,
                EvidenceOutputKind::NativeIoCertificate,
            ],
            deterministic_unsupported_behavior_required: true,
            runtime_execution_allowed: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub fn has_layer(&self, layer: ComputeEngineLayerKind) -> bool {
        self.layers.iter().any(|contract| contract.layer == layer)
    }

    #[must_use]
    pub fn all_layers_no_fallback(&self) -> bool {
        self.layers
            .iter()
            .all(|layer| !layer.fallback_execution_allowed)
    }

    #[must_use]
    pub fn providers_distinguish_native_from_external(&self) -> bool {
        self.execution_providers
            .iter()
            .any(ExecutionProviderContract::native_provider)
            && self
                .execution_providers
                .iter()
                .any(ExecutionProviderContract::external_baseline_provider)
            && self
                .execution_providers
                .iter()
                .all(ExecutionProviderContract::fallback_prohibited)
    }

    #[must_use]
    pub fn all_registries_claim_gated(&self) -> bool {
        self.registries
            .iter()
            .all(ComputeRegistryContract::is_claim_gated)
    }

    #[must_use]
    pub fn has_shared_primitive(&self, primitive: SharedDataModelPrimitiveKind) -> bool {
        self.shared_primitives.contains(&primitive)
    }

    #[must_use]
    pub fn evidence_is_first_class_output(&self) -> bool {
        [
            EvidenceOutputKind::EvidenceArtifact,
            EvidenceOutputKind::Diagnostic,
            EvidenceOutputKind::LineageFacet,
            EvidenceOutputKind::Profile,
            EvidenceOutputKind::BenchmarkRow,
            EvidenceOutputKind::ExecutionCertificate,
            EvidenceOutputKind::NativeIoCertificate,
        ]
        .into_iter()
        .all(|kind| self.evidence_outputs.contains(&kind))
    }

    #[must_use]
    pub const fn deterministic_unsupported_behavior(&self) -> bool {
        self.deterministic_unsupported_behavior_required
            && !self.runtime_execution_allowed
            && !self.fallback_execution_allowed
    }
}

#[must_use]
pub fn plan_compute_engine_architecture_spine() -> ComputeEngineArchitectureSpineReport {
    ComputeEngineArchitectureSpineReport::default_modular_spine()
}

fn default_layers() -> Vec<ComputeEngineLayerContract> {
    vec![
        ComputeEngineLayerContract::new(
            ComputeEngineLayerKind::FrontendAdapter,
            vec!["shardloom-cli", "python"],
            vec!["user_request", "wrapper_request"],
            vec!["logical_plan_request"],
        ),
        ComputeEngineLayerContract::new(
            ComputeEngineLayerKind::LogicalIr,
            vec!["shardloom-plan"],
            vec!["logical_plan_request"],
            vec!["logical_ir"],
        ),
        ComputeEngineLayerContract::new(
            ComputeEngineLayerKind::SemanticProfileBinding,
            vec!["shardloom-core", "shardloom-plan"],
            vec!["logical_ir"],
            vec!["semantic_profile_bound_plan"],
        ),
        ComputeEngineLayerContract::new(
            ComputeEngineLayerKind::CapabilityAdmission,
            vec!["shardloom-core"],
            vec!["semantic_profile_bound_plan"],
            vec!["admitted_capability_plan", "unsupported_diagnostics"],
        ),
        ComputeEngineLayerContract::new(
            ComputeEngineLayerKind::OptimizerRewrite,
            vec!["shardloom-plan"],
            vec!["admitted_capability_plan"],
            vec!["rewritten_plan", "optimizer_evidence"],
        ),
        ComputeEngineLayerContract::new(
            ComputeEngineLayerKind::PhysicalPlanning,
            vec!["shardloom-core", "shardloom-vortex"],
            vec!["rewritten_plan"],
            vec!["physical_plan", "operator_certificate"],
        ),
        ComputeEngineLayerContract::new(
            ComputeEngineLayerKind::ExecutionProviderSelection,
            vec!["shardloom-core", "shardloom-vortex"],
            vec!["physical_plan"],
            vec!["provider_plan", "residual_boundary_report"],
        ),
        ComputeEngineLayerContract::new(
            ComputeEngineLayerKind::SchedulerRuntime,
            vec!["shardloom-exec", "shardloom-vortex"],
            vec!["provider_plan"],
            vec!["task_graph", "runtime_profile"],
        ),
        ComputeEngineLayerContract::new(
            ComputeEngineLayerKind::SinkDelivery,
            vec!["shardloom-core", "shardloom-vortex"],
            vec!["task_graph_result"],
            vec!["native_result_stream", "sink_report"],
        ),
        ComputeEngineLayerContract::new(
            ComputeEngineLayerKind::EvidenceArtifactEmission,
            vec!["shardloom-core"],
            vec!["native_result_stream", "sink_report", "runtime_profile"],
            vec!["evidence_artifact_envelope", "diagnostics", "certificates"],
        ),
    ]
}

fn default_execution_providers() -> Vec<ExecutionProviderContract> {
    vec![
        ExecutionProviderContract::native(ExecutionProviderKind::ShardLoomKernel),
        ExecutionProviderContract::native(ExecutionProviderKind::ShardLoomMetadata),
        ExecutionProviderContract::native(ExecutionProviderKind::VortexArrayKernel),
        ExecutionProviderContract::native(ExecutionProviderKind::VortexComputeFunction),
        ExecutionProviderContract::native(ExecutionProviderKind::VortexScan),
        ExecutionProviderContract::native(ExecutionProviderKind::VortexSource),
        ExecutionProviderContract::native(ExecutionProviderKind::VortexSink),
        ExecutionProviderContract::compatibility_boundary(
            ExecutionProviderKind::CompatibilityImport,
        ),
        ExecutionProviderContract::compatibility_boundary(
            ExecutionProviderKind::CompatibilityExport,
        ),
        ExecutionProviderContract::external_baseline(ExecutionProviderKind::ExternalBaseline),
        ExecutionProviderContract::external_baseline(
            ExecutionProviderKind::ProhibitedExternalFallback,
        ),
    ]
}

fn default_registries() -> Vec<ComputeRegistryContract> {
    vec![
        ComputeRegistryContract::required(ComputeRegistryKind::Operator, false),
        ComputeRegistryContract::required(ComputeRegistryKind::Function, false),
        ComputeRegistryContract::required(ComputeRegistryKind::Aggregate, true),
        ComputeRegistryContract::required(ComputeRegistryKind::Sketch, true),
        ComputeRegistryContract::required(ComputeRegistryKind::Window, true),
        ComputeRegistryContract::required(ComputeRegistryKind::Join, true),
        ComputeRegistryContract::required(ComputeRegistryKind::SortTopN, false),
        ComputeRegistryContract::required(ComputeRegistryKind::Sink, true),
    ]
}

fn default_shared_primitives() -> Vec<SharedDataModelPrimitiveKind> {
    vec![
        SharedDataModelPrimitiveKind::LogicalDType,
        SharedDataModelPrimitiveKind::EncodedRepresentationState,
        SharedDataModelPrimitiveKind::SelectionVector,
        SharedDataModelPrimitiveKind::SegmentStatistics,
        SharedDataModelPrimitiveKind::NullSemantics,
        SharedDataModelPrimitiveKind::DictionaryEncoding,
        SharedDataModelPrimitiveKind::RunLengthEncoding,
        SharedDataModelPrimitiveKind::SparseEncoding,
        SharedDataModelPrimitiveKind::MaterializationBoundary,
        SharedDataModelPrimitiveKind::DecodeBoundary,
        SharedDataModelPrimitiveKind::NativeIoEnvelope,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn architecture_spine_has_ordered_layer_contracts() {
        let report = plan_compute_engine_architecture_spine();

        assert_eq!(report.layers.len(), 10);
        assert_eq!(
            report.layers.first().map(|layer| layer.layer),
            Some(ComputeEngineLayerKind::FrontendAdapter)
        );
        assert_eq!(
            report.layers.last().map(|layer| layer.layer),
            Some(ComputeEngineLayerKind::EvidenceArtifactEmission)
        );
        assert!(report.has_layer(ComputeEngineLayerKind::LogicalIr));
        assert!(report.has_layer(ComputeEngineLayerKind::ExecutionProviderSelection));
        assert!(report.all_layers_no_fallback());
    }

    #[test]
    fn architecture_spine_distinguishes_providers_and_baselines() {
        let report = plan_compute_engine_architecture_spine();

        assert!(report.providers_distinguish_native_from_external());
        assert!(
            report
                .execution_providers
                .iter()
                .any(
                    |provider| provider.kind == ExecutionProviderKind::VortexScan
                        && provider.native_provider()
                        && provider.certificate_required()
                )
        );
        assert!(
            report
                .execution_providers
                .iter()
                .any(
                    |provider| provider.kind == ExecutionProviderKind::ExternalBaseline
                        && provider.external_baseline_provider()
                        && provider.fallback_prohibited()
                )
        );
    }

    #[test]
    fn architecture_spine_claim_gates_registries_and_shared_primitives() {
        let report = plan_compute_engine_architecture_spine();

        assert_eq!(report.registries.len(), 8);
        assert!(report.all_registries_claim_gated());
        assert!(report.has_shared_primitive(SharedDataModelPrimitiveKind::LogicalDType));
        assert!(report.has_shared_primitive(SharedDataModelPrimitiveKind::SelectionVector));
        assert!(report.has_shared_primitive(SharedDataModelPrimitiveKind::NativeIoEnvelope));
    }

    #[test]
    fn architecture_spine_blocks_runtime_and_fallback_claims() {
        let report = plan_compute_engine_architecture_spine();

        assert!(
            report
                .runtime_task_graph
                .complete_before_large_workload_claims()
        );
        assert!(report.evidence_is_first_class_output());
        assert!(report.deterministic_unsupported_behavior());
    }
}
