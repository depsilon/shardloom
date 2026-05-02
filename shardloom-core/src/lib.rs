//! Core types and traits shared across `ShardLoom` crates.
//!
//! This crate defines minimal cross-cutting contracts for the initial workspace:
//! identifiers, errors, diagnostics, and capability metadata for native
//! `Vortex`-first execution.

pub mod benchmark;
pub mod capabilities;
pub mod correctness;
pub mod dataset;
pub mod diagnostics;
pub mod encoded;
pub mod expression;
pub mod extension;
pub mod manifest;
pub mod observability;
pub mod output;
pub mod schema;
pub mod security;
pub mod translation;

pub use benchmark::{
    BaselineEngine, BenchmarkMetric, BenchmarkPlan, BenchmarkResult, BenchmarkScenario,
    CorrectnessValidationMode, MetricValue, WorkloadClass,
};
pub use capabilities::{Capability, CapabilityStatus, EngineCapabilities};
pub use correctness::{
    CorrectnessFixture, CorrectnessPlanStatus, CorrectnessValidationPlan,
    CorrectnessValidationReport, DiagnosticExpectation, DifferentialBaseline, EdgeCase,
    ExpectedOutcome, FixtureFormat, FixtureId, FuzzSeed, ReferenceRole, SemanticArea,
    ValidationResultStatus,
};
pub use dataset::{
    DatasetFormat, DatasetId, DatasetRef, DatasetUri, ManifestId, SnapshotId, UriScheme,
};
pub use diagnostics::{
    Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity, FallbackStatus,
};
pub use encoded::{
    ByteRange, ColumnRef, ComparisonOp, EncodedEvalCapability, EncodedSegment, EncodingKind,
    ExecutionState, LayoutKind, LogicalDType, MaterializationPolicy, Nullability, PredicateExpr,
    PredicateProof, PruningDecision, SegmentId, SegmentLayout, SegmentStats, SelectionVector,
    SortOrder, StatValue, StatisticsExactness,
};
pub use extension::{
    ExtensionCapability, ExtensionCapabilityStatus, ExtensionCategory, ExtensionEffectDeclaration,
    ExtensionId, ExtensionInspectionReport, ExtensionInspectionStatus, ExtensionLicenseKind,
    ExtensionLifecycleState, ExtensionManifest, ExtensionPermission, ExtensionProvenance,
    ExtensionRegistrySnapshot, ExtensionVersion, PluginAbiRequirement, PluginAbiStatus,
    SandboxPolicy, SandboxPolicyKind, UdfRuntimeKind,
};

pub use expression::{
    BinaryOp, Determinism, EffectLevel, ExprId, Expression, ExpressionKind, FunctionCategory,
    FunctionSignature, KernelCapability, KernelDescriptor, KernelEvalMode, KernelId, KernelKind,
    KernelRegistrySnapshot, KernelSelectionRequest, KernelSelectionResult, KernelSelectionStatus,
    NullBehavior, ScalarValue, UnaryOp,
};

pub use manifest::{
    ChangeSet, CommitRecord, CommitStatus, DatasetManifest, FileDescriptor, FileRole,
    IncrementalPlanSkeleton, IncrementalPlanningDecision, ManifestSegment, ManifestVersion,
    SegmentChange, SegmentChangeKind, SnapshotRef, WriteIntent, WriteIntentStatus,
};
pub use observability::{
    KernelProfile, MetricCategory, MetricKind, MetricSample, MetricUnit, ObservabilityMetricValue,
    ObservabilityPlan, ObservabilityPlanStatus, ObservabilitySurface, ObservedField,
    OperatorProfile, RedactionStatus, RuntimeObservabilityReport, SensitivityLevel,
    StructuredEvent, StructuredEventKind, TraceSpanCategory, TraceSpanId, TraceSpanSkeleton,
    TraceSpanStatus,
};

pub use output::{CommandStatus, OutputEnvelope, OutputFormat};

pub use schema::{
    CatalogKind, CatalogRef, DeleteModel, FieldId, FieldName, FieldPath, PartitionField,
    PartitionSpec, PartitionTransform, SchemaChange, SchemaChangeKind, SchemaCompatibilityLevel,
    SchemaCompatibilityReport, SchemaDefinition, SchemaEvolutionPolicy, SchemaEvolutionPolicyKind,
    SchemaField, SchemaId, SchemaVersion, TableCompatibilityPlan, TableCompatibilityReport,
    TableCompatibilityStatus, TableFeature, TableFeatureKind, TableFeatureStatus, TableFormatKind,
};

pub use security::{
    AgentSafetyMode, ApprovalRequirement, AuditActionKind, AuditRecord, CredentialScope,
    CredentialScopeKind, DataSensitivity, DryRunSafety, ExternalEffectKind, ExternalEffectPolicy,
    PermissionKind, PermissionRequirement, PermissionStatus, RedactionPolicy, RedactionPolicyKind,
    SecretProviderKind, SecretRef, SecretRefId, SecurityPlan, SecurityPolicyStatus, SecurityReport,
    SensitiveField,
};

pub use translation::{
    CommitMode, FidelityLevel, MaterializationRequirement, MetadataKind, MetadataPreservation,
    MetadataPreservationStatus, OutputTarget, OutputTargetKind, TranslationPlan,
    TranslationPlanningStatus, TranslationReport,
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
