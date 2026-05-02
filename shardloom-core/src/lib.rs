//! Core types and traits shared across `ShardLoom` crates.
//!
//! This crate defines minimal cross-cutting contracts for the initial workspace:
//! identifiers, errors, diagnostics, and capability metadata for native
//! `Vortex`-first execution.

pub mod benchmark;
pub mod capabilities;
pub mod dataset;
pub mod diagnostics;
pub mod encoded;

pub use benchmark::{
    BaselineEngine, BenchmarkMetric, BenchmarkPlan, BenchmarkResult, BenchmarkScenario,
    CorrectnessValidationMode, MetricValue, WorkloadClass,
};
pub use capabilities::{Capability, CapabilityStatus, EngineCapabilities};
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

/// Canonical crate-level result type for `ShardLoom`.
pub type Result<T> = std::result::Result<T, ShardLoomError>;

/// Minimal error type for explicit failures in unsupported skeleton paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardLoomError {
    InvalidOperation(String),
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
            Self::InvalidOperation(message) | Self::Message(message) => message,
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
    use super::ShardLoomError;

    #[test]
    fn error_message_roundtrip() {
        let error = ShardLoomError::new("boom");
        assert_eq!(error.message(), "boom");
        assert_eq!(error.to_string(), "boom");
    }
}
