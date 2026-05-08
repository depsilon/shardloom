//! Core types and traits shared across `ShardLoom` crates.
//!
//! This crate defines minimal cross-cutting contracts for the initial workspace:
//! identifiers, errors, diagnostics, and capability metadata for native
//! `Vortex`-first execution.

pub mod benchmark;
pub mod capabilities;
pub mod certification;
pub mod correctness;
pub mod cpu_specialization;
pub mod dataset;
pub mod diagnostics;
pub mod encoded;
pub mod execution_certificate;
pub mod expression;
pub mod extension;
pub mod feature_footprint;
pub mod input;
pub mod manifest;
pub mod native_io;
pub mod observability;
pub mod operator;
pub mod output;
pub mod release;
pub mod schema;
pub mod security;
pub mod stateful_reuse;
pub mod translation;
pub mod universal_harness;

pub use benchmark::{
    BaselineEngine, BenchmarkCacheState, BenchmarkClaimGate, BenchmarkClaimStatus,
    BenchmarkComparisonReport, BenchmarkComparisonStatus, BenchmarkDatasetProfile,
    BenchmarkEngineVersion, BenchmarkEvidenceBundle, BenchmarkEvidenceState,
    BenchmarkFallbackState, BenchmarkMetric, BenchmarkMetricGap, BenchmarkPlan,
    BenchmarkReproducibilityStatus, BenchmarkResult, BenchmarkResultGap, BenchmarkRunManifest,
    BenchmarkScenario, CorrectnessValidationMode, MetricValue, WorkloadClass,
};
pub use capabilities::{Capability, CapabilityStatus, EngineCapabilities};
pub use certification::{
    AdapterCertificationEntry, AdapterCertificationMatrix, AdapterMaturityLevel,
    BestChoiceScorecard, BestChoiceScorecardEntry, CapabilityCertificationEntry,
    CapabilityCertificationReport, CapabilityCertificationStatus, CapabilityCertificationSurface,
    FunctionCoverageEntry, FunctionCoverageGroup, FunctionCoverageMatrix,
    MigrationCompatibilityEntry, MigrationReportKind, OperatorCertificationStatus,
    OperatorCoverageEntry, OperatorCoverageMatrix, OperatorFamily, OperatorMemoryCertification,
    ScorecardDimension, SemanticProfileEntry, SemanticProfileName, SourcePushdownExactness,
    SqlCoverageEntry, SqlCoverageMatrix, SqlCoverageTier, SqlFeatureGroup,
    WorldClassSufficiencyDecision, WorldClassSufficiencyDimension,
    WorldClassSufficiencyDimensionKind, WorldClassSufficiencyReport, WorldClassSufficiencyStatus,
    plan_world_class_sufficiency,
};
pub use correctness::{
    CorrectnessFixture, CorrectnessPlanStatus, CorrectnessValidationPlan,
    CorrectnessValidationReport, DiagnosticExpectation, DifferentialBaseline, EdgeCase,
    ExpectedOutcome, FixtureFormat, FixtureId, FuzzSeed, ReferenceRole, SemanticArea,
    ValidationResultStatus,
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
pub use encoded::{
    ByteRange, ColumnRef, ComparisonOp, EncodedEvalCapability, EncodedPredicateEvaluationReport,
    EncodedPredicateEvaluationStatus, EncodedSegment, EncodingKind, ExecutionState, LayoutKind,
    LogicalDType, MaterializationPolicy, Nullability, PredicateExpr, PredicateProof,
    PruningDecision, SegmentId, SegmentLayout, SegmentStats, SelectionVector, SortOrder, StatValue,
    StatisticsExactness, evaluate_predicate_on_encoded_segment, prove_predicate_from_stats,
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
    NativeIoCertificatePathRequirement, NativeIoContractKind, NativeIoContractSurface,
    NativeIoEnvelopeReport, NativeIoEnvelopeStatus, NativeIoTransitionExample, RepresentationState,
    RepresentationStateContract, plan_native_io_envelope,
};
pub use observability::{
    KernelProfile, MetricCategory, MetricKind, MetricSample, MetricUnit, ObservabilityMetricValue,
    ObservabilityPlan, ObservabilityPlanStatus, ObservabilitySurface, ObservedField,
    OperatorProfile, RedactionStatus, RuntimeObservabilityReport, SensitivityLevel,
    StructuredEvent, StructuredEventKind, TraceSpanCategory, TraceSpanId, TraceSpanSkeleton,
    TraceSpanStatus,
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
    CliApiJsonProtocolReport, CommandStatus, OutputEnvelope, OutputFormat,
    PythonWrapperFoundationReport,
};

pub use release::{
    ApiStabilityTier, ChecklistStatus, DependencyLicenseClass, DependencyReview,
    DependencyReviewStatus, MachineReadableSchemaKind, NoFallbackReleaseCheck, PackageTarget,
    PackageTargetKind, ProjectVersion, PublicSurface, PublicSurfaceKind, ReleaseArtifactKind,
    ReleaseArtifactPlan, ReleaseChannel, ReleaseChecklistItem, ReleaseChecklistItemKind,
    ReleasePlan, ReleaseReadinessStatus, ReleaseReport, SchemaCompatibilityPlan, SchemaStability,
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
    CredentialScopeKind, DataSensitivity, DryRunSafety, ExternalEffectKind, ExternalEffectPolicy,
    PermissionKind, PermissionRequirement, PermissionStatus, RedactionPolicy, RedactionPolicyKind,
    SecretProviderKind, SecretRef, SecretRefId, SecurityPlan, SecurityPolicyStatus, SecurityReport,
    SensitiveField,
};

pub use stateful_reuse::{
    InvalidationProofRequirement, InvalidationSignalKind, ReuseBoundaryStatus, ReuseCacheKind,
    StatefulReuseBoundary, StatefulReuseReport, StatefulReuseStatus, plan_stateful_reuse,
};

pub use translation::{
    CommitMode, FidelityLevel, MaterializationRequirement, MetadataKind, MetadataPreservation,
    MetadataPreservationStatus, OutputTarget, OutputTargetKind, TranslationPlan,
    TranslationPlanningStatus, TranslationReport,
};

pub use universal_harness::{
    ExternalBaselineHarnessRequirement, UniversalHarnessReport, UniversalHarnessStatus,
    UniversalHarnessSurface, UniversalHarnessSurfaceKind, UniversalHarnessSurfaceStatus,
    plan_universal_harness,
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
