//! Core types and traits shared across `ShardLoom` crates.
//!
//! This crate owns provider-neutral contracts: identifiers, diagnostics,
//! encoded-data vocabulary, evidence artifacts, Native I/O certificates,
//! benchmark/correctness contracts, release gates, policy reports, and
//! user-facing capability surfaces.
//!
//! Public exports are contract surfaces. They do not perform dataset reads,
//! object-store I/O, external engine execution, package publication, model
//! calls, or fallback execution by themselves. Executable work lives in
//! provider crates such as `shardloom-vortex` and must attach the relevant
//! evidence before support or performance claims.

pub mod agent_contract;
pub mod approx_sketch;
pub mod architecture_gate;
pub mod architecture_spine;
pub mod benchmark;
pub mod benchmark_suite;
pub mod capabilities;
pub mod certification;
pub mod correctness;
pub mod cpu_specialization;
pub mod dataset;
pub mod diagnostics;
pub mod effect_budget;
pub mod encoded;
pub mod engine_modes;
pub mod execution_certificate;
pub mod expression;
pub mod extension;
pub mod feature_footprint;
pub mod hybrid_engine;
pub mod input;
pub mod live_engine;
pub mod manifest;
pub mod native_io;
pub mod observability;
pub mod operational_contracts;
pub mod operator;
pub mod output;
pub mod release;
pub mod remote_api;
pub mod rfc_coverage;
pub mod schema;
pub mod security;
pub mod session;
pub mod stateful_reuse;
pub mod table_intelligence;
pub mod translation;
pub mod universal_harness;
pub mod unstructured_workflow;
pub mod wrapper_architecture;

pub use agent_contract::{
    AgentContractPack, AgentContractSurface, AgentContractSurfaceKind, AgentContractSurfaceStatus,
};
pub use approx_sketch::{
    ApproxSketchFunctionGateEntry, ApproxSketchFunctionGateReport, ApproxSketchFunctionStatus,
    ApproxSketchFunctionSurface, plan_approx_sketch_function_gate,
};
pub use architecture_gate::{
    ArchitectureRuntimeClaimGateReport, ArchitectureRuntimeClaimGateRow,
    ArchitectureRuntimeClaimSupportStatus, ArchitectureRuntimeClaimSurface,
    plan_global_architecture_runtime_claim_gate,
};
pub use architecture_spine::{
    ComputeEngineArchitectureSpineReport, ComputeEngineLayerContract, ComputeEngineLayerKind,
    ComputeRegistryContract, ComputeRegistryKind, EvidenceOutputKind, ExecutionProviderContract,
    ExecutionProviderKind, RuntimeTaskGraphContract, SharedDataModelPrimitiveKind,
    plan_compute_engine_architecture_spine,
};
pub use benchmark::{
    BaselineEngine, BenchmarkCacheState, BenchmarkClaimEvidenceReport,
    BenchmarkClaimEvidenceStatus, BenchmarkClaimGate, BenchmarkClaimStatus,
    BenchmarkComparisonReport, BenchmarkComparisonStatus, BenchmarkDatasetProfile,
    BenchmarkEngineVersion, BenchmarkEvidenceBundle, BenchmarkEvidenceState,
    BenchmarkFallbackState, BenchmarkMetric, BenchmarkMetricGap, BenchmarkPlan,
    BenchmarkReproducibilityStatus, BenchmarkResult, BenchmarkResultGap, BenchmarkRunManifest,
    BenchmarkScenario, CorrectnessValidationMode, MetricValue, WorkloadClass,
    plan_benchmark_claim_evidence,
};
pub use benchmark_suite::{
    BenchmarkConstitutionRequirementReport, BenchmarkCoverageStatus, BenchmarkCoverageTableRow,
    BenchmarkEnginePluginContract, BenchmarkEngineRole, BenchmarkResultSchemaV2Report,
    BenchmarkScenarioCategory, BenchmarkSuiteCatalogReport, BenchmarkSuiteDatasetProfileKind,
    BenchmarkSuiteKind, plan_benchmark_suite_catalog,
};
pub use capabilities::{Capability, CapabilityStatus, EngineCapabilities};
pub use certification::{
    AdapterCertificationEntry, AdapterCertificationMatrix, AdapterMaturityLevel,
    BestChoiceScorecard, BestChoiceScorecardEntry, CapabilityCertificationEntry,
    CapabilityCertificationReport, CapabilityCertificationStatus, CapabilityCertificationSurface,
    FunctionCoverageEntry, FunctionCoverageGroup, FunctionCoverageMatrix,
    MigrationCompatibilityEntry, MigrationReportKind, OperatorCertificationStatus,
    OperatorCoverageEntry, OperatorCoverageMatrix, OperatorFamily, OperatorMemoryCertification,
    PlannerReadinessSupportStatus, ScorecardDimension, SemanticProfileEntry, SemanticProfileName,
    SourcePushdownExactness, SqlCoverageEntry, SqlCoverageMatrix, SqlCoverageTier,
    SqlDataFramePlannerReadinessMatrix, SqlDataFramePlannerReadinessRow,
    SqlDataFramePlannerReadinessSurface, SqlFeatureGroup, UserCapabilityPromotionGateEntry,
    UserCapabilityPromotionGateReport, UserCapabilityPromotionStatus,
    UserCapabilityPromotionSurface, WorldClassSufficiencyDecision, WorldClassSufficiencyDimension,
    WorldClassSufficiencyDimensionKind, WorldClassSufficiencyReport, WorldClassSufficiencyStatus,
    plan_user_capability_promotion_gate, plan_world_class_sufficiency,
};
pub use correctness::{
    CorrectnessDifferentialHarnessReport, CorrectnessDifferentialHarnessStatus, CorrectnessFixture,
    CorrectnessPlanStatus, CorrectnessValidationPlan, CorrectnessValidationReport,
    DiagnosticExpectation, DifferentialBaseline, EdgeCase, ExpectedOutcome,
    ExternalOracleArtifactStatus, ExternalOracleResultArtifact, FixtureFormat, FixtureId, FuzzSeed,
    ReferenceArtifact, ReferenceRole, SemanticArea, ValidationResultStatus,
    plan_correctness_differential_harness,
};
pub use cpu_specialization::{
    CpuInstructionClass, CpuOperatorSpecializationEntry, CpuOperatorSpecializationReport,
    CpuSpecializationStatus, plan_cpu_operator_specialization,
};
pub use dataset::{
    DatasetFormat, DatasetId, DatasetRef, DatasetUri, ManifestId, SnapshotId, UriScheme,
};
pub use diagnostics::{
    Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity, FallbackStatus,
};
pub use effect_budget::{
    EffectBudgetEntry, EffectBudgetReport, EffectBudgetScope, EffectBudgetStatus,
};
pub use encoded::{
    ByteRange, ColumnRef, ComparisonOp, EncodedEvalCapability, EncodedPredicateEvaluationReport,
    EncodedPredicateEvaluationStatus, EncodedSegment, EncodedValueBatch, EncodedValueRun,
    EncodingKind, ExecutionState, LayoutKind, LogicalDType, MaterializationPolicy, Nullability,
    PredicateExpr, PredicateProof, PruningDecision, SegmentId, SegmentLayout, SegmentStats,
    SelectionVector, SortOrder, StatValue, StatisticsExactness,
    evaluate_predicate_on_encoded_segment, evaluate_predicate_on_encoded_values,
    prove_predicate_from_stats,
};
pub use engine_modes::{
    Boundedness, EngineCapabilityMatrixReport, EngineCapabilityRow, EngineMode,
    EngineSelectionReport, EngineSelectionRequest, EngineSelectionStatus, EngineSupportStatus,
    OutputMode, UpdateMode, boundedness_vocabulary, engine_mode_vocabulary, output_mode_vocabulary,
    update_mode_vocabulary,
};
pub use execution_certificate::{
    ExecutionCertificate, ExecutionCertificateEvidenceSurfaceReport,
    ExecutionCertificateEvidenceSurfaceStatus, ExecutionCertificateInput,
    ExecutionCertificateStatus, ExecutionEvidenceArtifactKind,
    ExecutionEvidenceArtifactRequirement, ExecutionEvidenceArtifactStatus,
    plan_execution_certificate_evidence_surface,
};
pub use extension::{
    ExtensionCapability, ExtensionCapabilityStatus, ExtensionCategory, ExtensionEffectDeclaration,
    ExtensionId, ExtensionInspectionReport, ExtensionInspectionStatus, ExtensionLicenseKind,
    ExtensionLifecycleState, ExtensionManifest, ExtensionPermission, ExtensionProvenance,
    ExtensionRegistrySnapshot, ExtensionVersion, PluginAbiRequirement, PluginAbiStatus,
    SandboxPolicy, SandboxPolicyKind, UdfRuntimeKind,
};

pub use feature_footprint::{
    ExternalBaselineAvailability, FeatureFootprintGate, FeatureFootprintGateStatus,
    FeatureFootprintReport,
};

pub use hybrid_engine::{
    DeltaOverlayCertificate, HotColdContributionReport, HybridBaseRow, HybridFixtureRunInput,
    HybridFixtureRunReport, HybridFixtureSegmentTier, HybridLayoutHealthBundle,
    MicroSegmentFlushEvidence, run_hybrid_fixture,
};

pub use expression::{
    BinaryOp, Determinism, EffectLevel, ExprId, Expression, ExpressionKind, FunctionCategory,
    FunctionSignature, KernelCapability, KernelDescriptor, KernelEvalMode, KernelId, KernelKind,
    KernelRegistrySnapshot, KernelSelectionRequest, KernelSelectionResult, KernelSelectionStatus,
    NullBehavior, ScalarValue, UnaryOp,
};

pub use input::{
    InputAdapterKind, InputAdapterRegistrySnapshot, InputAdapterReport, InputCapability,
    InputCapabilityStatus, InputEffectLevel, InputFidelityLevel, InputMaterializationRisk,
    InputMetadataAvailability, InputSourceId, InputSourceKind, UniversalInputSource,
    input_source_to_dataset_ref,
};

pub use live_engine::{
    ChangeOperation, ChangeRecord, CheckpointPolicy, ContinuousViewCertificate,
    FreshnessCertificate, LateDataPolicy, LiveCertificateStatus, LiveChangeContractReport,
    LiveFixtureOperator, LiveFixtureRunInput, LiveFixtureRunReport, LiveOutputRow,
    OutputChangelogEntry, OutputChangelogMode, StateCertificate, StateTtlPolicy, WatermarkPolicy,
    plan_live_change_contract, run_live_fixture,
};

pub use manifest::{
    CdcEventKind, CdcEventSummary, CdcIncrementalPlanningReport, CdcIncrementalPlanningStatus,
    ChangeSet, CommitRecord, CommitStatus, CompactionPlanningAction, CompactionPlanningActionKind,
    CompactionPlanningPolicy, CompactionPlanningReport, CompactionPlanningStatus, DatasetManifest,
    FileDescriptor, FileRole, IncrementalPlanSkeleton, IncrementalPlanningDecision,
    LayoutHealthIssue, LayoutHealthIssueKind, LayoutHealthPolicy, LayoutHealthReport,
    LayoutHealthStatus, ManifestSegment, ManifestVersion, SegmentChange, SegmentChangeKind,
    SnapshotRef, WriteIntent, WriteIntentStatus, evaluate_cdc_incremental_planning,
    evaluate_compaction_planning, evaluate_layout_health,
};
pub use native_io::{
    NativeIoAdapterFidelityReport, NativeIoCertificate, NativeIoCertificatePathRequirement,
    NativeIoContractKind, NativeIoContractSurface, NativeIoCoverageDirection,
    NativeIoEnvelopeReport, NativeIoEnvelopeStatus, NativeIoMaterializationBoundaryReport,
    NativeIoRepresentationTransition, NativeIoSideEffectReport, NativeIoSinkRequirementReport,
    NativeIoSourceCapabilityReport, NativeIoSourcePushdownReport, NativeIoSourceSinkCoverageRow,
    NativeIoTransitionExample, RepresentationState, RepresentationStateContract,
    plan_native_io_envelope,
};
pub use observability::{
    KernelProfile, MetricCategory, MetricKind, MetricSample, MetricUnit, ObservabilityMetricValue,
    ObservabilityPlan, ObservabilityPlanStatus, ObservabilitySchemaArea,
    ObservabilitySchemaCoverageEntry, ObservabilitySchemaCoverageReport, ObservabilitySchemaStatus,
    ObservabilitySurface, ObservedField, OperatorProfile, RedactionStatus,
    RuntimeObservabilityReport, SensitivityLevel, StructuredEvent, StructuredEventKind,
    TraceSpanCategory, TraceSpanId, TraceSpanSkeleton, TraceSpanStatus,
    plan_observability_schema_coverage,
};
pub use operational_contracts::{
    BenchmarkConstitution, CostSimulationReport, EvidenceArtifactEnvelope, EvidenceArtifactSafety,
    OperationalContractsReport, ProtocolSurfaceParityReport, ProtocolSurfaceParityRow,
    QueryLifecycleContract, QueryLifecycleState, RustPerformanceProfileEvidence,
    ShardLoomExecutionPolicy, ShardLoomNativeSemanticDimension, ShardLoomNativeSemanticProfile,
    StandardsDecisionStatus, StandardsDependencyDecision, StandardsDependencyDecisionReport,
    WorkloadConstitutionCatalog, WorkloadConstitutionEntry, plan_operational_contracts,
};

pub use operator::{
    PhysicalKernelAdmissionReport, PhysicalKernelAdmissionStatus, PhysicalKernelRegistryPlan,
    PhysicalKernelRequirement, PhysicalKernelRequirementStatus, PhysicalKernelSelectionReport,
    PhysicalKernelSelectionStatus, PhysicalKernelSlot, PhysicalOperatorContract,
    PhysicalOperatorExecutionLevel, PhysicalOperatorExecutionProfile,
    PhysicalOperatorExecutionProfileMatrix, PhysicalOperatorKind, PhysicalOperatorPlan,
    PhysicalOperatorPlanningCertificate, PhysicalOperatorPlanningCertificateStatus,
    PhysicalOperatorReadinessStatus,
};

pub use output::{
    CliApiJsonProtocolReport, CommandStatus, OutputEnvelope, OutputFormat, OutputTypedArtifact,
    OutputTypedPayload, OutputTypedRef, PythonWrapperFoundationReport, ShardLoomExecutionMode,
    ShardLoomExecutionModeFamily, ShardLoomExecutionModeSelectionReport,
    ShardLoomExecutionModeSelectionRequest,
};

pub use release::{
    ApiStabilityTier, ChecklistStatus, CondaBuildInstallCertificationReport,
    CondaPackageBuildInstallEntry, CondaPackageKind, DependencyLicenseClass, DependencyReview,
    DependencyReviewStatus, MachineReadableSchemaKind, NoFallbackReleaseCheck, PackageTarget,
    PackageTargetKind, ProjectVersion, PublicSurface, PublicSurfaceKind, ReleaseArtifactKind,
    ReleaseArtifactPlan, ReleaseChannel, ReleaseChecklistItem, ReleaseChecklistItemKind,
    ReleaseEvidenceRequirement, ReleaseEvidenceRequirementKind, ReleaseEvidenceRequirementStatus,
    ReleasePlan, ReleasePublicationBoundary, ReleasePublicationBoundaryKind,
    ReleasePublicationBoundaryReport, ReleasePublicationBoundaryStatus,
    ReleaseReadinessEvidenceReport, ReleaseReadinessStatus, ReleaseReport, SchemaCompatibilityPlan,
    SchemaStability, WorkspaceFeatureBuildMatrixFeatureSet, WorkspaceFeatureBuildMatrixReport,
    WorkspaceFeatureBuildMatrixRow, WorkspaceFeatureBuildMatrixRowStatus,
    plan_workspace_feature_build_matrix,
};
pub use remote_api::{
    RestApiAuditPolicyContract, RestApiAuthPostureContract, RestApiContractReport,
    RestApiDataPlaneReport, RestApiDataPlaneScenario, RestApiDataPlaneStatus,
    RestApiDataPlaneTransferContract, RestApiDiscoveryModeReport, RestApiEndpointContract,
    RestApiEventStreamEventContract, RestApiEventStreamReport, RestApiEventStreamScenario,
    RestApiEventStreamStatus, RestApiEvidenceModelSignal, RestApiLifecycleEvent,
    RestApiLocalLifecycleReport, RestApiLocalLifecycleScenario, RestApiLocalLifecycleStatus,
    RestApiMaturityStage, RestApiMaturityStatus, RestApiMcpContract, RestApiPlanPreviewReport,
    RestApiPlanPreviewScenario, RestApiPlanPreviewStage, RestApiPlanPreviewStatus,
    RestApiPlanStageStatus, RestApiProblemDetailsPreview, RestApiResultPolicyContract,
    RestApiScopeContract, RestApiSecurityGovernanceReport, RestApiSecurityGovernanceScenario,
    RestApiSecurityGovernanceStatus, RestApiStandardsBoundaryContract,
};
pub use rfc_coverage::{
    RfcCoverageFollowThroughArea, RfcCoverageFollowThroughEntry, RfcCoverageFollowThroughReport,
    RfcCoverageFollowThroughStatus, plan_rfc_coverage_followthrough,
};
pub use schema::{
    CatalogKind, CatalogRef, DeleteModel, DeleteTombstoneCompatibilityLevel,
    DeleteTombstoneCompatibilityReport, FieldId, FieldName, FieldPath, PartitionEvolutionChange,
    PartitionEvolutionChangeKind, PartitionEvolutionCompatibilityLevel,
    PartitionEvolutionCompatibilityReport, PartitionField, PartitionSpec, PartitionTransform,
    SchemaChange, SchemaChangeKind, SchemaCompatibilityLevel, SchemaCompatibilityReport,
    SchemaDefinition, SchemaEvolutionCompatibilityReport, SchemaEvolutionPolicy,
    SchemaEvolutionPolicyKind, SchemaField, SchemaId, SchemaVersion, TableCompatibilityPlan,
    TableCompatibilityReport, TableCompatibilityStatus, TableFeature, TableFeatureKind,
    TableFeatureStatus, TableFormatKind, evaluate_delete_tombstone_compatibility,
    evaluate_partition_evolution_compatibility, evaluate_schema_evolution_compatibility,
};

pub use security::{
    AgentSafetyMode, ApprovalRequirement, AuditActionKind, AuditRecord, CredentialScope,
    CredentialScopeKind, DataSensitivity, DryRunSafety, EvidenceArtifactSafetyReport,
    ExternalEffectKind, ExternalEffectPolicy, PermissionKind, PermissionRequirement,
    PermissionStatus, RedactionPolicy, RedactionPolicyKind, RuntimeInputSafetyReport,
    SecretProviderKind, SecretRef, SecretRefId, SecurityGovernanceEvidenceArea,
    SecurityGovernanceEvidenceEntry, SecurityGovernanceEvidenceGateReport,
    SecurityGovernanceEvidenceStatus, SecurityPlan, SecurityPolicyStatus, SecurityReport,
    SensitiveField, WorkspacePathSafetyReport, plan_security_governance_evidence_gate,
    redact_credential_like_values,
};

pub use session::{
    ShardLoomSessionModelReport, ShardLoomSessionRegistryEntry, ShardLoomSessionRegistryKind,
    ShardLoomSessionRegistryStatus, plan_shardloom_session_model,
};

pub use stateful_reuse::{
    InvalidationProofRequirement, InvalidationSignalKind, ReuseBoundaryStatus, ReuseCacheKind,
    StatefulReuseBoundary, StatefulReusePromotionGateEntry, StatefulReusePromotionGateReport,
    StatefulReusePromotionStatus, StatefulReusePromotionSurface, StatefulReuseReport,
    StatefulReuseStatus, plan_stateful_reuse, plan_stateful_reuse_promotion_gate,
};

pub use table_intelligence::{
    CatalogMetadataIntegrationGateEntry, CatalogMetadataIntegrationGateReport,
    CatalogMetadataIntegrationStatus, CatalogMetadataIntegrationSurface, TableIntelligenceReport,
    TableIntelligenceSurface, TableIntelligenceSurfaceKind, TableIntelligenceSurfaceStatus,
    plan_catalog_metadata_integration_gate,
};

pub use translation::{
    CommitMode, FidelityLevel, MaterializationRequirement, MetadataKind, MetadataPreservation,
    MetadataPreservationStatus, OutputTarget, OutputTargetKind, TranslationPlan,
    TranslationPlanningStatus, TranslationReport,
};

pub use universal_harness::{
    ExternalBaselineHarnessRequirement, UniversalHarnessEnvironmentKind,
    UniversalHarnessEnvironmentRequirement, UniversalHarnessEnvironmentStatus,
    UniversalHarnessReport, UniversalHarnessStatus, UniversalHarnessSurface,
    UniversalHarnessSurfaceKind, UniversalHarnessSurfaceStatus, plan_universal_harness,
};
pub use unstructured_workflow::{
    BoundaryExecutor, DeterminismLevel, EmbeddingBoundaryReport, EmbeddingTable,
    ExtractionBoundaryReport, FoundryAipLogicBoundaryReport, FoundryMediaBoundaryPosture,
    MediaKind, MediaLocationKind, MediaManifest, MediaRef, ModelCallBoundaryReport, TextChunkTable,
    UnstructuredMaturity, UnstructuredWorkflowBoundaryReport, UnstructuredWorkflowCertificate,
    WorkflowBoundaryKind, plan_unstructured_workflow_boundaries,
};
pub use wrapper_architecture::{
    ClientCoreOperation, ClientWrapperArchitectureReport, ProtocolSchemaArtifact,
    WrapperCapabilityReport, WrapperFamily, WrapperGoldenContractFixture,
    WrapperGoldenContractFixtureCatalog, WrapperMaturityLevel, WrapperRegistryEntry,
    WrapperTransportKind, plan_client_wrapper_architecture,
};

/// Canonical crate-level result type for `ShardLoom`.
pub type Result<T> = std::result::Result<T, ShardLoomError>;

/// Minimal error type for explicit failures in unsupported skeleton paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardLoomError {
    InvalidOperation(String),
    NotImplemented(String),
    Message(String),
}

impl ShardLoomError {
    /// Construct a new error with a human-readable message.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }

    /// View the error message.
    #[must_use]
    pub fn message(&self) -> &str {
        match self {
            Self::InvalidOperation(message)
            | Self::NotImplemented(message)
            | Self::Message(message) => message,
        }
    }

    /// Converts plain errors into stable structured diagnostics for user-facing output.
    ///
    /// This keeps machine-readable diagnostics deterministic for agents and preserves
    /// explicit no-fallback policy visibility.
    #[must_use]
    pub fn to_diagnostic(&self) -> Diagnostic {
        match self {
            Self::InvalidOperation(message) => Diagnostic::invalid_input(
                "operation",
                message.clone(),
                "Correct the input and retry with a supported value.",
            ),
            Self::NotImplemented(feature) => Diagnostic::not_implemented(
                feature.clone(),
                "This behavior is not implemented for native ShardLoom execution.",
                "Use supported planning/introspection commands or wait for native support.",
            ),
            Self::Message(message) => Diagnostic::configuration_error(
                "runtime",
                message.clone(),
                "Review command arguments and configuration before retrying.",
            ),
        }
    }
}

impl std::fmt::Display for ShardLoomError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl std::error::Error for ShardLoomError {}

#[cfg(test)]
mod tests {
    use super::{DiagnosticCode, ShardLoomError};

    #[test]
    fn error_message_roundtrip() {
        let error = ShardLoomError::new("boom");
        assert_eq!(error.message(), "boom");
        assert_eq!(error.to_string(), "boom");
    }

    #[test]
    fn invalid_operation_maps_to_invalid_input_diagnostic() {
        let diag = ShardLoomError::InvalidOperation("bad arg".to_string()).to_diagnostic();
        assert_eq!(diag.code, DiagnosticCode::InvalidInput);
    }

    #[test]
    fn message_maps_to_configuration_error_diagnostic() {
        let diag = ShardLoomError::Message("missing config".to_string()).to_diagnostic();
        assert_eq!(diag.code, DiagnosticCode::ConfigurationError);
    }

    #[test]
    fn not_implemented_maps_to_not_implemented_diagnostic() {
        let diag = ShardLoomError::NotImplemented("sql".to_string()).to_diagnostic();
        assert_eq!(diag.code, DiagnosticCode::NotImplemented);
    }
}
