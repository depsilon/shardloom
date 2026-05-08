//! Streaming, zero-copy boundary, and sink-driven planning skeleton.
//!
//! This module defines planning-only domain types. It does not execute streaming,
//! read object stores, or perform Vortex/Arrow IO.

use crate::ByteSize;
use shardloom_core::{
    DatasetRef, Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity, ExecutionState,
    FallbackStatus, FidelityLevel, MaterializationRequirement, OutputTarget, OutputTargetKind,
    Result, ShardLoomError, UriScheme,
};
use std::collections::HashSet;

/// Requested streaming mode for planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingMode {
    Disabled,
    PlanOnly,
    Preferred,
    Required,
}
impl StreamingMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::PlanOnly => "plan_only",
            Self::Preferred => "preferred",
            Self::Required => "required",
        }
    }
    #[must_use]
    pub const fn requires_streaming(&self) -> bool {
        matches!(self, Self::Required)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingCapability {
    Streaming,
    RequiresState,
    RequiresMaterialization,
    NotSupported,
    Unknown,
}
impl StreamingCapability {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Streaming => "streaming",
            Self::RequiresState => "requires_state",
            Self::RequiresMaterialization => "requires_materialization",
            Self::NotSupported => "not_supported",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn can_stream(&self) -> bool {
        matches!(self, Self::Streaming)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataWorkLevel {
    MetadataOnly,
    Pruned,
    ZeroDecode,
    ZeroCopyBoundary,
    PartialDecode,
    LateMaterialization,
    FullMaterialization,
    Shuffle,
    Distributed,
    Unsupported,
}
impl DataWorkLevel {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::Pruned => "pruned",
            Self::ZeroDecode => "zero_decode",
            Self::ZeroCopyBoundary => "zero_copy_boundary",
            Self::PartialDecode => "partial_decode",
            Self::LateMaterialization => "late_materialization",
            Self::FullMaterialization => "full_materialization",
            Self::Shuffle => "shuffle",
            Self::Distributed => "distributed",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn rank(&self) -> u8 {
        match self {
            Self::MetadataOnly => 0,
            Self::Pruned => 1,
            Self::ZeroDecode => 2,
            Self::ZeroCopyBoundary => 3,
            Self::PartialDecode => 4,
            Self::LateMaterialization => 5,
            Self::FullMaterialization => 6,
            Self::Shuffle => 7,
            Self::Distributed => 8,
            Self::Unsupported => 255,
        }
    }

    /// Returns the canonical terminology label used in stable agent/CLI/JSON output.
    ///
    /// This helper does not alter planning or execution semantics.
    #[must_use]
    pub const fn canonical_label(&self) -> &'static str {
        self.as_str()
    }

    /// Maps streaming work-level terminology into core execution-state terminology.
    ///
    /// This is a mapping helper only and preserves layer boundaries.
    #[must_use]
    pub const fn to_execution_state(&self) -> ExecutionState {
        match self {
            Self::MetadataOnly => ExecutionState::MetadataOnly,
            Self::Pruned => ExecutionState::Pruned,
            Self::ZeroDecode => ExecutionState::EncodedEvaluation,
            Self::ZeroCopyBoundary => ExecutionState::Translation,
            Self::PartialDecode | Self::LateMaterialization => ExecutionState::PartialDecode,
            Self::FullMaterialization => ExecutionState::FullMaterialization,
            Self::Shuffle | Self::Distributed | Self::Unsupported => ExecutionState::Unsupported,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryInteropKind {
    None,
    ArrowLikeCData,
    ArrowLikeCStream,
    ArrowIpc,
    FutureFlight,
    FutureFlightSql,
    PythonFfi,
    RustApi,
    CompatibilityExport,
}
impl BoundaryInteropKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ArrowLikeCData => "arrow_like_c_data",
            Self::ArrowLikeCStream => "arrow_like_c_stream",
            Self::ArrowIpc => "arrow_ipc",
            Self::FutureFlight => "future_flight",
            Self::FutureFlightSql => "future_flight_sql",
            Self::PythonFfi => "python_ffi",
            Self::RustApi => "rust_api",
            Self::CompatibilityExport => "compatibility_export",
        }
    }
    #[must_use]
    pub const fn is_future_boundary(&self) -> bool {
        matches!(self, Self::FutureFlight | Self::FutureFlightSql)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZeroCopyStatus {
    NotApplicable,
    Preserved,
    BoundaryOnly,
    RequiresCopy,
    Unsupported,
    Unknown,
}
impl ZeroCopyStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotApplicable => "not_applicable",
            Self::Preserved => "preserved",
            Self::BoundaryOnly => "boundary_only",
            Self::RequiresCopy => "requires_copy",
            Self::Unsupported => "unsupported",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZeroDecodeStatus {
    NotApplicable,
    Preserved,
    PartialDecodeRequired,
    FullDecodeRequired,
    Unsupported,
    Unknown,
}
impl ZeroDecodeStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotApplicable => "not_applicable",
            Self::Preserved => "preserved",
            Self::PartialDecodeRequired => "partial_decode_required",
            Self::FullDecodeRequired => "full_decode_required",
            Self::Unsupported => "unsupported",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackpressurePolicy {
    pub enabled: bool,
    pub max_in_flight_chunks: Option<usize>,
    pub max_buffered_bytes: Option<ByteSize>,
}
impl BackpressurePolicy {
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            max_in_flight_chunks: None,
            max_buffered_bytes: None,
        }
    }

    /// Builds a bounded backpressure policy for planning.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when `max_in_flight_chunks == 0`.
    pub fn bounded(max_in_flight_chunks: usize, max_buffered_bytes: ByteSize) -> Result<Self> {
        if max_in_flight_chunks == 0 {
            return Err(ShardLoomError::InvalidOperation(
                "max_in_flight_chunks must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            enabled: true,
            max_in_flight_chunks: Some(max_in_flight_chunks),
            max_buffered_bytes: Some(max_buffered_bytes),
        })
    }
    #[must_use]
    pub const fn is_bounded(&self) -> bool {
        self.enabled && self.max_in_flight_chunks.is_some() && self.max_buffered_bytes.is_some()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        if self.is_bounded() {
            format!(
                "enabled in_flight={} max_buffered_bytes={}",
                self.max_in_flight_chunks.unwrap_or_default(),
                self.max_buffered_bytes.map_or(0, |b| b.as_bytes())
            )
        } else {
            "disabled".to_string()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressurePlanStatus {
    Disabled,
    Planned,
    Bounded,
    BlockedByMissingBudget,
    Unsupported,
}
impl BackpressurePlanStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Planned => "planned",
            Self::Bounded => "bounded",
            Self::BlockedByMissingBudget => "blocked_by_missing_budget",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::BlockedByMissingBudget | Self::Unsupported)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressurePlanMode {
    PlanOnly,
    BoundedStreaming,
    Disabled,
    Unsupported,
}
impl BackpressurePlanMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PlanOnly => "plan_only",
            Self::BoundedStreaming => "bounded_streaming",
            Self::Disabled => "disabled",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn executes_streams(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackpressurePlanInput {
    pub memory: BoundedMemoryPolicy,
    pub max_parallelism: usize,
    pub estimated_chunk_bytes: Option<ByteSize>,
    pub diagnostics: Vec<Diagnostic>,
}
impl BackpressurePlanInput {
    /// Creates a backpressure planning input.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when `max_parallelism == 0`.
    pub fn new(memory: BoundedMemoryPolicy, max_parallelism: usize) -> Result<Self> {
        if max_parallelism == 0 {
            return Err(ShardLoomError::InvalidOperation(
                "max_parallelism must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            memory,
            max_parallelism,
            estimated_chunk_bytes: None,
            diagnostics: vec![],
        })
    }
    #[must_use]
    pub const fn with_estimated_chunk_bytes(mut self, value: ByteSize) -> Self {
        self.estimated_chunk_bytes = Some(value);
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct BackpressurePlanReport {
    pub status: BackpressurePlanStatus,
    pub mode: BackpressurePlanMode,
    pub input: BackpressurePlanInput,
    pub policy: BackpressurePolicy,
    pub bounded: bool,
    pub max_in_flight_chunks: Option<usize>,
    pub max_buffered_bytes: Option<ByteSize>,
    pub estimated_chunk_bytes: Option<ByteSize>,
    pub memory_required: bool,
    pub spill_allowed: bool,
    pub streams_executed: bool,
    pub tasks_executed: bool,
    pub data_read: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl BackpressurePlanReport {
    /// Builds a planning-only backpressure report.
    ///
    /// # Errors
    /// Returns errors from bounded policy validation.
    pub fn from_input(input: BackpressurePlanInput) -> Result<Self> {
        let mut out = Self {
            status: BackpressurePlanStatus::Planned,
            mode: BackpressurePlanMode::PlanOnly,
            policy: BackpressurePolicy::disabled(),
            bounded: false,
            max_in_flight_chunks: None,
            max_buffered_bytes: None,
            estimated_chunk_bytes: input.estimated_chunk_bytes,
            memory_required: input.memory.required,
            spill_allowed: input.memory.allow_spill,
            streams_executed: false,
            tasks_executed: false,
            data_read: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            fallback_execution_allowed: false,
            diagnostics: input.diagnostics.clone(),
            input,
        };
        if out.input.has_errors() {
            out.status = BackpressurePlanStatus::Unsupported;
            out.mode = BackpressurePlanMode::Unsupported;
            return Ok(out);
        }
        if !out.memory_required {
            out.status = BackpressurePlanStatus::Disabled;
            out.mode = BackpressurePlanMode::Disabled;
            return Ok(out);
        }
        let Some(max_buffered_bytes) = out.input.memory.max_memory_bytes else {
            out.status = BackpressurePlanStatus::BlockedByMissingBudget;
            out.mode = BackpressurePlanMode::Unsupported;
            out.diagnostics.push(Diagnostic::invalid_input(
                "backpressure.memory_budget",
                "bounded backpressure requires max_memory_bytes",
                "provide a bounded memory policy before streaming execution is enabled",
            ));
            return Ok(out);
        };
        if max_buffered_bytes.as_bytes() == 0 {
            out.status = BackpressurePlanStatus::Unsupported;
            out.mode = BackpressurePlanMode::Unsupported;
            out.diagnostics.push(Diagnostic::invalid_input(
                "backpressure.memory_budget",
                "bounded backpressure requires a non-zero memory budget",
                "provide memory_gb greater than zero",
            ));
            return Ok(out);
        }
        out.policy = BackpressurePolicy::bounded(out.input.max_parallelism, max_buffered_bytes)?;
        out.bounded = out.policy.is_bounded();
        out.max_in_flight_chunks = out.policy.max_in_flight_chunks;
        out.max_buffered_bytes = out.policy.max_buffered_bytes;
        out.status = BackpressurePlanStatus::Bounded;
        out.mode = BackpressurePlanMode::BoundedStreaming;
        Ok(out)
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.input.has_errors()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.streams_executed
            && !self.tasks_executed
            && !self.data_read
            && !self.data_materialized
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.fallback_execution_allowed
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "backpressure status: {}\nmode: {}\nbounded: {}\nmax in-flight chunks: {}\nmax buffered bytes: {}\nestimated chunk bytes: {}\nmemory required: {}\nspill allowed: {}\nstreams executed: false\ntasks executed: false\ndata read: false\ndata materialized: false\nobject-store IO: false\nwrite IO: false\nspill IO performed: false\nfallback execution: disabled",
            self.status.as_str(),
            self.mode.as_str(),
            self.bounded,
            self.max_in_flight_chunks
                .map_or("none".to_string(), |v| v.to_string()),
            self.max_buffered_bytes
                .map_or("none".to_string(), |v| v.as_bytes().to_string()),
            self.estimated_chunk_bytes
                .map_or("unknown".to_string(), |v| v.as_bytes().to_string()),
            self.memory_required,
            self.spill_allowed,
        )
    }
}

/// Plans bounded backpressure for streaming execution without executing streams.
///
/// # Errors
/// Returns errors from backpressure policy validation.
pub fn plan_backpressure(input: BackpressurePlanInput) -> Result<BackpressurePlanReport> {
    BackpressurePlanReport::from_input(input)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedMemoryPolicy {
    pub required: bool,
    pub max_memory_bytes: Option<ByteSize>,
    pub allow_spill: bool,
}
impl BoundedMemoryPolicy {
    #[must_use]
    pub const fn best_effort() -> Self {
        Self {
            required: false,
            max_memory_bytes: None,
            allow_spill: false,
        }
    }
    #[must_use]
    pub const fn required(max_memory_bytes: ByteSize) -> Self {
        Self {
            required: true,
            max_memory_bytes: Some(max_memory_bytes),
            allow_spill: false,
        }
    }
    #[must_use]
    pub const fn with_spill(mut self, allow_spill: bool) -> Self {
        self.allow_spill = allow_spill;
        self
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "required={} max_memory_bytes={} allow_spill={}",
            self.required,
            self.max_memory_bytes
                .map_or("none".to_string(), |b| b.as_bytes().to_string()),
            self.allow_spill
        )
    }

    /// Returns canonical bounded-memory policy terminology for diagnostics output.
    ///
    /// This helper is label-only and does not change memory behavior.
    #[must_use]
    pub const fn canonical_label(&self) -> &'static str {
        if self.required {
            "bounded_memory_required"
        } else {
            "bounded_memory_best_effort"
        }
    }
}

impl MaterializationBoundary {
    /// Returns canonical terminology for where materialization becomes required.
    ///
    /// This helper is for stable labeling and does not alter runtime behavior.
    #[must_use]
    pub const fn canonical_label(&self) -> &'static str {
        if self.required {
            match self.data_work_level {
                DataWorkLevel::PartialDecode => "partial_decode_boundary",
                DataWorkLevel::LateMaterialization => "late_materialization_boundary",
                DataWorkLevel::FullMaterialization => "full_materialization_boundary",
                _ => "materialization_boundary",
            }
        } else {
            "no_materialization_boundary"
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingSourceKind {
    VortexSegment,
    VortexSplit,
    ManifestSegmentGroup,
    ObjectStoreByteRange,
    MetadataOnlyPseudoChunk,
    ExternalRead,
    ArrowLikeBoundary,
    Unsupported,
}
impl StreamingSourceKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::VortexSegment => "vortex_segment",
            Self::VortexSplit => "vortex_split",
            Self::ManifestSegmentGroup => "manifest_segment_group",
            Self::ObjectStoreByteRange => "object_store_byte_range",
            Self::MetadataOnlyPseudoChunk => "metadata_only_pseudo_chunk",
            Self::ExternalRead => "external_read",
            Self::ArrowLikeBoundary => "arrow_like_boundary",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamingSource {
    pub kind: StreamingSourceKind,
    pub dataset: Option<DatasetRef>,
    pub capability: StreamingCapability,
    pub zero_decode: ZeroDecodeStatus,
    pub estimated_chunks: Option<u64>,
    pub diagnostics: Vec<Diagnostic>,
}
impl StreamingSource {
    #[must_use]
    pub fn vortex_dataset(dataset: DatasetRef) -> Self {
        Self {
            kind: StreamingSourceKind::VortexSegment,
            dataset: Some(dataset),
            capability: StreamingCapability::Streaming,
            zero_decode: ZeroDecodeStatus::Preserved,
            estimated_chunks: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn object_store_byte_range(dataset: DatasetRef) -> Self {
        Self {
            kind: StreamingSourceKind::ObjectStoreByteRange,
            dataset: Some(dataset),
            capability: StreamingCapability::Streaming,
            zero_decode: ZeroDecodeStatus::Preserved,
            estimated_chunks: None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn metadata_only(dataset: DatasetRef) -> Self {
        Self {
            kind: StreamingSourceKind::MetadataOnlyPseudoChunk,
            dataset: Some(dataset),
            capability: StreamingCapability::Streaming,
            zero_decode: ZeroDecodeStatus::NotApplicable,
            estimated_chunks: Some(1),
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            kind: StreamingSourceKind::Unsupported,
            dataset: None,
            capability: StreamingCapability::NotSupported,
            zero_decode: ZeroDecodeStatus::Unsupported,
            estimated_chunks: None,
            diagnostics: vec![unsupported_streaming_diagnostic(feature, reason)],
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
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
            "kind={} capability={} zero_decode={} estimated_chunks={}",
            self.kind.as_str(),
            self.capability.as_str(),
            self.zero_decode.as_str(),
            self.estimated_chunks
                .map_or("unknown".to_string(), |v| v.to_string())
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingOperatorKind {
    MetadataOnly,
    SegmentPruning,
    EncodedPredicate,
    Projection,
    PartialDecode,
    AggregatePartial,
    AggregateFinal,
    JoinBuild,
    JoinProbe,
    Translation,
    ExternalEffect,
    Unsupported,
}
impl StreamingOperatorKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::SegmentPruning => "segment_pruning",
            Self::EncodedPredicate => "encoded_predicate",
            Self::Projection => "projection",
            Self::PartialDecode => "partial_decode",
            Self::AggregatePartial => "aggregate_partial",
            Self::AggregateFinal => "aggregate_final",
            Self::JoinBuild => "join_build",
            Self::JoinProbe => "join_probe",
            Self::Translation => "translation",
            Self::ExternalEffect => "external_effect",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamingOperator {
    pub kind: StreamingOperatorKind,
    pub capability: StreamingCapability,
    pub data_work_level: DataWorkLevel,
    pub materialization: MaterializationRequirement,
    pub diagnostics: Vec<Diagnostic>,
}
impl StreamingOperator {
    #[must_use]
    pub fn new(
        kind: StreamingOperatorKind,
        capability: StreamingCapability,
        data_work_level: DataWorkLevel,
    ) -> Self {
        Self {
            kind,
            capability,
            data_work_level,
            materialization: MaterializationRequirement::None,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn metadata_only() -> Self {
        Self::new(
            StreamingOperatorKind::MetadataOnly,
            StreamingCapability::Streaming,
            DataWorkLevel::MetadataOnly,
        )
    }
    #[must_use]
    pub fn encoded_predicate() -> Self {
        Self::new(
            StreamingOperatorKind::EncodedPredicate,
            StreamingCapability::Streaming,
            DataWorkLevel::ZeroDecode,
        )
    }
    #[must_use]
    pub fn requires_materialization(
        kind: StreamingOperatorKind,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::new(
            kind,
            StreamingCapability::RequiresMaterialization,
            DataWorkLevel::FullMaterialization,
        );
        s.materialization = MaterializationRequirement::Full {
            reason: reason.into(),
        };
        s
    }
    #[must_use]
    pub fn unsupported(
        kind: StreamingOperatorKind,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::new(
            kind,
            StreamingCapability::NotSupported,
            DataWorkLevel::Unsupported,
        );
        s.diagnostics
            .push(unsupported_streaming_diagnostic(feature, reason));
        s
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub const fn can_stream(&self) -> bool {
        self.capability.can_stream()
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
            "kind={} capability={} data_work={} materialization={}",
            self.kind.as_str(),
            self.capability.as_str(),
            self.data_work_level.as_str(),
            self.materialization.to_human_text()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingSinkKind {
    VortexNative,
    ArrowIpcCompatibility,
    ParquetCompatibility,
    IcebergCompatible,
    DeltaCompatible,
    NullBenchmark,
    InMemoryDebug,
    ArrowLikeBoundary,
    FutureFlight,
    FutureFlightSql,
    Unsupported,
}
impl StreamingSinkKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::VortexNative => "vortex_native",
            Self::ArrowIpcCompatibility => "arrow_ipc_compatibility",
            Self::ParquetCompatibility => "parquet_compatibility",
            Self::IcebergCompatible => "iceberg_compatible",
            Self::DeltaCompatible => "delta_compatible",
            Self::NullBenchmark => "null_benchmark",
            Self::InMemoryDebug => "in_memory_debug",
            Self::ArrowLikeBoundary => "arrow_like_boundary",
            Self::FutureFlight => "future_flight",
            Self::FutureFlightSql => "future_flight_sql",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_native_vortex(&self) -> bool {
        matches!(self, Self::VortexNative)
    }
    #[must_use]
    pub const fn is_compatibility(&self) -> bool {
        matches!(
            self,
            Self::ArrowIpcCompatibility
                | Self::ParquetCompatibility
                | Self::IcebergCompatible
                | Self::DeltaCompatible
                | Self::ArrowLikeBoundary
                | Self::FutureFlight
                | Self::FutureFlightSql
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkRequirement {
    pub accepts_encoded: bool,
    pub requires_materialization: bool,
    pub preserves_metadata: bool,
    pub fidelity: FidelityLevel,
    pub boundary: BoundaryInteropKind,
}
impl SinkRequirement {
    #[must_use]
    pub const fn vortex_native() -> Self {
        Self {
            accepts_encoded: true,
            requires_materialization: false,
            preserves_metadata: true,
            fidelity: FidelityLevel::NativeFullFidelity,
            boundary: BoundaryInteropKind::None,
        }
    }
    #[must_use]
    pub const fn compatibility_materialized(boundary: BoundaryInteropKind) -> Self {
        Self {
            accepts_encoded: false,
            requires_materialization: true,
            preserves_metadata: false,
            fidelity: FidelityLevel::CompatibilityLossyPhysical,
            boundary,
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "accepts_encoded={} requires_materialization={} preserves_metadata={} fidelity={} boundary={}",
            self.accepts_encoded,
            self.requires_materialization,
            self.preserves_metadata,
            self.fidelity.as_str(),
            self.boundary.as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamingSink {
    pub kind: StreamingSinkKind,
    pub target: Option<OutputTarget>,
    pub requirement: SinkRequirement,
    pub capability: StreamingCapability,
    pub zero_copy: ZeroCopyStatus,
    pub diagnostics: Vec<Diagnostic>,
}
impl StreamingSink {
    #[must_use]
    pub fn vortex_native(target: OutputTarget) -> Self {
        Self {
            kind: StreamingSinkKind::VortexNative,
            target: Some(target),
            requirement: SinkRequirement::vortex_native(),
            capability: StreamingCapability::Streaming,
            zero_copy: ZeroCopyStatus::Preserved,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn compatibility(target: OutputTarget) -> Self {
        let (kind, boundary) = match target.kind {
            OutputTargetKind::ArrowIpc => (
                StreamingSinkKind::ArrowIpcCompatibility,
                BoundaryInteropKind::ArrowIpc,
            ),
            OutputTargetKind::Parquet => (
                StreamingSinkKind::ParquetCompatibility,
                BoundaryInteropKind::CompatibilityExport,
            ),
            OutputTargetKind::IcebergCompatible => (
                StreamingSinkKind::IcebergCompatible,
                BoundaryInteropKind::CompatibilityExport,
            ),
            OutputTargetKind::DeltaCompatible => (
                StreamingSinkKind::DeltaCompatible,
                BoundaryInteropKind::CompatibilityExport,
            ),
            _ => (
                StreamingSinkKind::Unsupported,
                BoundaryInteropKind::CompatibilityExport,
            ),
        };
        let mut s = if kind == StreamingSinkKind::Unsupported {
            Self::unsupported(
                "streaming sink target",
                format!(
                    "target kind {} is not supported for compatibility streaming sink",
                    target.kind.as_str()
                ),
            )
        } else {
            Self {
                kind,
                target: Some(target),
                requirement: SinkRequirement::compatibility_materialized(boundary),
                capability: StreamingCapability::RequiresMaterialization,
                zero_copy: ZeroCopyStatus::RequiresCopy,
                diagnostics: vec![],
            }
        };
        if kind != StreamingSinkKind::Unsupported {
            s.diagnostics.push(Diagnostic::new(DiagnosticCode::MetadataLoss, DiagnosticSeverity::Warning, DiagnosticCategory::MetadataLoss, "Compatibility sink may lose Vortex physical layout and encoding metadata.", Some("compatibility_sink".to_string()), Some("Compatibility outputs are boundary/export targets and may require materialization.".to_string()), Some("Use Vortex output for highest fidelity when metadata preservation is required.".to_string()), FallbackStatus::disabled_by_policy()));
        }
        s
    }
    #[must_use]
    pub fn null_benchmark() -> Self {
        Self {
            kind: StreamingSinkKind::NullBenchmark,
            target: None,
            requirement: SinkRequirement::vortex_native(),
            capability: StreamingCapability::Streaming,
            zero_copy: ZeroCopyStatus::NotApplicable,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            kind: StreamingSinkKind::Unsupported,
            target: None,
            requirement: SinkRequirement::compatibility_materialized(
                BoundaryInteropKind::CompatibilityExport,
            ),
            capability: StreamingCapability::NotSupported,
            zero_copy: ZeroCopyStatus::Unsupported,
            diagnostics: vec![unsupported_streaming_diagnostic(feature, reason)],
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
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
            "kind={} capability={} zero_copy={} requirement=[{}]",
            self.kind.as_str(),
            self.capability.as_str(),
            self.zero_copy.as_str(),
            self.requirement.summary()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializationBoundary {
    pub required: bool,
    pub reason: String,
    pub data_work_level: DataWorkLevel,
}
impl MaterializationBoundary {
    #[must_use]
    pub fn none() -> Self {
        Self {
            required: false,
            reason: "none".to_string(),
            data_work_level: DataWorkLevel::ZeroDecode,
        }
    }
    #[must_use]
    pub fn required(reason: impl Into<String>, data_work_level: DataWorkLevel) -> Self {
        Self {
            required: true,
            reason: reason.into(),
            data_work_level,
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "required={} reason={} data_work={}",
            self.required,
            self.reason,
            self.data_work_level.as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamingStage {
    pub name: String,
    pub source: Option<StreamingSourceKind>,
    pub operator: Option<StreamingOperatorKind>,
    pub sink: Option<StreamingSinkKind>,
    pub capability: StreamingCapability,
    pub data_work_level: DataWorkLevel,
    pub materialization_boundary: MaterializationBoundary,
    pub diagnostics: Vec<Diagnostic>,
}
impl StreamingStage {
    /// Creates a streaming stage skeleton entry.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when `name` is empty or whitespace-only.
    pub fn new(
        name: impl Into<String>,
        capability: StreamingCapability,
        data_work_level: DataWorkLevel,
    ) -> Result<Self> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "stage name must not be empty".to_string(),
            ));
        }
        Ok(Self {
            name,
            source: None,
            operator: None,
            sink: None,
            capability,
            data_work_level,
            materialization_boundary: MaterializationBoundary::none(),
            diagnostics: vec![],
        })
    }
    /// Creates a stage from a streaming source skeleton.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when `name` is empty or whitespace-only.
    pub fn from_source(name: impl Into<String>, source: &StreamingSource) -> Result<Self> {
        let mut s = Self::new(
            name,
            source.capability,
            if source.zero_decode == ZeroDecodeStatus::Preserved {
                DataWorkLevel::ZeroDecode
            } else {
                DataWorkLevel::MetadataOnly
            },
        )?;
        s.source = Some(source.kind);
        s.diagnostics.extend(source.diagnostics.clone());
        Ok(s)
    }
    /// Creates a stage from a streaming operator skeleton.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when `name` is empty or whitespace-only.
    pub fn from_operator(name: impl Into<String>, operator: &StreamingOperator) -> Result<Self> {
        let mut s = Self::new(name, operator.capability, operator.data_work_level)?;
        s.operator = Some(operator.kind);
        if operator.materialization.requires_materialization() {
            s.materialization_boundary = MaterializationBoundary::required(
                "operator requires materialization",
                operator.data_work_level,
            );
        }
        s.diagnostics.extend(operator.diagnostics.clone());
        Ok(s)
    }
    /// Creates a stage from a streaming sink skeleton.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when `name` is empty or whitespace-only.
    pub fn from_sink(name: impl Into<String>, sink: &StreamingSink) -> Result<Self> {
        let mut s = Self::new(
            name,
            sink.capability,
            if sink.requirement.requires_materialization {
                DataWorkLevel::FullMaterialization
            } else {
                DataWorkLevel::ZeroDecode
            },
        )?;
        s.sink = Some(sink.kind);
        if sink.requirement.requires_materialization {
            s.materialization_boundary = MaterializationBoundary::required(
                "sink requires materialization",
                DataWorkLevel::FullMaterialization,
            );
        }
        s.diagnostics.extend(sink.diagnostics.clone());
        Ok(s)
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub const fn can_stream(&self) -> bool {
        self.capability.can_stream()
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
            "name={} capability={} data_work={} materialization=[{}]",
            self.name,
            self.capability.as_str(),
            self.data_work_level.as_str(),
            self.materialization_boundary.summary()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamingPlanStatus {
    Planned,
    StreamingPreferred,
    StreamingRequired,
    RequiresMaterialization,
    StreamingNotImplemented,
    Unsupported,
}
impl StreamingPlanStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::StreamingPreferred => "streaming_preferred",
            Self::StreamingRequired => "streaming_required",
            Self::RequiresMaterialization => "requires_materialization",
            Self::StreamingNotImplemented => "streaming_not_implemented",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamingPlanSkeleton {
    pub mode: StreamingMode,
    pub source: StreamingSource,
    pub operators: Vec<StreamingOperator>,
    pub sink: StreamingSink,
    pub stages: Vec<StreamingStage>,
    pub backpressure: BackpressurePolicy,
    pub memory: BoundedMemoryPolicy,
    pub status: StreamingPlanStatus,
    pub diagnostics: Vec<Diagnostic>,
}
impl StreamingPlanSkeleton {
    #[must_use]
    pub fn new(mode: StreamingMode, source: StreamingSource, sink: StreamingSink) -> Self {
        Self {
            mode,
            source,
            operators: vec![],
            sink,
            stages: vec![],
            backpressure: BackpressurePolicy::disabled(),
            memory: BoundedMemoryPolicy::best_effort(),
            status: StreamingPlanStatus::Planned,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn for_vortex_to_target(dataset: DatasetRef, target: OutputTarget) -> Self {
        let source = StreamingSource::vortex_dataset(dataset);
        let sink = if target.is_native_vortex() {
            StreamingSink::vortex_native(target)
        } else {
            StreamingSink::compatibility(target)
        };
        let mut p = Self::new(StreamingMode::PlanOnly, source.clone(), sink.clone());
        p.status = if sink.kind == StreamingSinkKind::Unsupported {
            StreamingPlanStatus::Unsupported
        } else if sink.kind.is_native_vortex() {
            StreamingPlanStatus::Planned
        } else {
            StreamingPlanStatus::RequiresMaterialization
        };
        if let Ok(stage) = StreamingStage::from_source("source", &source) {
            p.add_stage(stage);
        }
        if let Ok(stage) = StreamingStage::from_sink("sink", &sink) {
            p.add_stage(stage);
        }
        p
    }
    #[must_use]
    pub fn streaming_not_implemented(source: StreamingSource, sink: StreamingSink) -> Self {
        let mut p = Self::new(StreamingMode::Preferred, source, sink);
        p.status = StreamingPlanStatus::StreamingNotImplemented;
        p.diagnostics.push(Diagnostic::new(DiagnosticCode::UnsupportedEffect, DiagnosticSeverity::Error, DiagnosticCategory::UnsupportedFeature, "Streaming execution is not implemented in this phase; fallback execution was not attempted.", Some("streaming_execution".to_string()), Some("Spark/DataFusion/DuckDB/Polars/Velox fallback execution is prohibited by policy.".to_string()), Some("Use planning outputs only until native streaming execution lands.".to_string()), FallbackStatus::disabled_by_policy()));
        p
    }
    #[must_use]
    pub fn unsupported(
        source: StreamingSource,
        sink: StreamingSink,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut p = Self::new(StreamingMode::Preferred, source, sink);
        p.status = StreamingPlanStatus::Unsupported;
        p.diagnostics
            .push(unsupported_streaming_diagnostic(feature, reason));
        p
    }
    pub fn add_operator(&mut self, operator: StreamingOperator) {
        self.operators.push(operator);
    }
    pub fn add_stage(&mut self, stage: StreamingStage) {
        self.stages.push(stage);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.source.has_errors()
            || self.sink.has_errors()
            || self.operators.iter().any(StreamingOperator::has_errors)
            || self.stages.iter().any(StreamingStage::has_errors)
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub fn requires_materialization(&self) -> bool {
        self.sink.requirement.requires_materialization
            || self
                .operators
                .iter()
                .any(|o| o.materialization.requires_materialization())
            || self
                .stages
                .iter()
                .any(|s| s.materialization_boundary.required)
    }
    #[must_use]
    pub fn best_data_work_level(&self) -> DataWorkLevel {
        let mut best = if self.source.zero_decode == ZeroDecodeStatus::Preserved {
            DataWorkLevel::ZeroDecode
        } else {
            DataWorkLevel::MetadataOnly
        };
        for op in &self.operators {
            if op.data_work_level.rank() > best.rank() {
                best = op.data_work_level;
            }
        }
        for st in &self.stages {
            if st.data_work_level.rank() > best.rank() {
                best = st.data_work_level;
            }
        }
        if self.sink.requirement.requires_materialization
            && DataWorkLevel::FullMaterialization.rank() > best.rank()
        {
            best = DataWorkLevel::FullMaterialization;
        }
        best
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = vec![
            format!("streaming mode: {}", self.mode.as_str()),
            format!("source: {}", self.source.summary()),
            format!("sink: {}", self.sink.summary()),
            format!("status: {}", self.status.as_str()),
            format!("backpressure: {}", self.backpressure.summary()),
            format!("memory policy: {}", self.memory.summary()),
            format!(
                "materialization required: {}",
                self.requires_materialization()
            ),
            "fallback execution: disabled".to_string(),
        ];
        if self.diagnostics.is_empty()
            && self.source.diagnostics.is_empty()
            && self.sink.diagnostics.is_empty()
            && self.operators.iter().all(|o| o.diagnostics.is_empty())
            && self.stages.iter().all(|s| s.diagnostics.is_empty())
        {
            out.push("diagnostics: none".to_string());
        } else {
            out.push("diagnostics:".to_string());
            let mut seen = HashSet::new();
            for d in self
                .source
                .diagnostics
                .iter()
                .chain(self.sink.diagnostics.iter())
                .chain(self.operators.iter().flat_map(|o| o.diagnostics.iter()))
                .chain(self.stages.iter().flat_map(|s| s.diagnostics.iter()))
                .chain(self.diagnostics.iter())
            {
                let line = format!(
                    "- {} | feature={} | reason={}",
                    d.to_human_text(),
                    d.feature.as_deref().unwrap_or("none"),
                    d.reason.as_deref().unwrap_or("none")
                );
                if seen.insert(line.clone()) {
                    out.push(line);
                }
            }
        }
        out.join("\n")
    }
}

/// Encoded representation planned for a streaming batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodedBatchRepresentation {
    MetadataOnly,
    VortexEncoded,
    ForeignEncoded,
    SelectionVectorEncoded,
    PartiallyDecoded,
    DecodedColumnar,
    MaterializedRows,
    Unsupported,
}
impl EncodedBatchRepresentation {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::VortexEncoded => "vortex_encoded",
            Self::ForeignEncoded => "foreign_encoded",
            Self::SelectionVectorEncoded => "selection_vector_encoded",
            Self::PartiallyDecoded => "partially_decoded",
            Self::DecodedColumnar => "decoded_columnar",
            Self::MaterializedRows => "materialized_rows",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn can_remain_encoded(&self) -> bool {
        matches!(
            self,
            Self::MetadataOnly
                | Self::VortexEncoded
                | Self::ForeignEncoded
                | Self::SelectionVectorEncoded
        )
    }
}

/// Planning status for encoded streaming batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodedStreamingBatchPlanStatus {
    Planned,
    RequiresMaterialization,
    BlockedByMissingMemoryBudget,
    BlockedByObjectStoreIo,
    Unsupported,
}
impl EncodedStreamingBatchPlanStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::RequiresMaterialization => "requires_materialization",
            Self::BlockedByMissingMemoryBudget => "blocked_by_missing_memory_budget",
            Self::BlockedByObjectStoreIo => "blocked_by_object_store_io",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::BlockedByMissingMemoryBudget | Self::BlockedByObjectStoreIo | Self::Unsupported
        )
    }
}

/// Input for encoded streaming-batch planning.
#[derive(Debug, Clone, PartialEq)]
pub struct EncodedStreamingBatchPlanInput {
    pub source: StreamingSource,
    pub operators: Vec<StreamingOperator>,
    pub sink: StreamingSink,
    pub memory: BoundedMemoryPolicy,
    pub max_parallelism: usize,
    pub estimated_batch_bytes: Option<ByteSize>,
    pub diagnostics: Vec<Diagnostic>,
}
impl EncodedStreamingBatchPlanInput {
    /// Creates an encoded streaming-batch planning input.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when `max_parallelism == 0`.
    pub fn new(
        source: StreamingSource,
        sink: StreamingSink,
        memory: BoundedMemoryPolicy,
        max_parallelism: usize,
    ) -> Result<Self> {
        if max_parallelism == 0 {
            return Err(ShardLoomError::InvalidOperation(
                "max_parallelism must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            source,
            operators: vec![],
            sink,
            memory,
            max_parallelism,
            estimated_batch_bytes: None,
            diagnostics: vec![],
        })
    }

    /// Builds a planning input for a Vortex dataset and output target.
    ///
    /// # Errors
    /// Returns errors from `Self::new`.
    pub fn for_vortex_to_target(
        dataset: DatasetRef,
        target: OutputTarget,
        memory: BoundedMemoryPolicy,
        max_parallelism: usize,
    ) -> Result<Self> {
        let source = if !dataset.is_native_vortex() {
            StreamingSource::unsupported(
                "encoded streaming batch source",
                format!(
                    "format {} is not a Vortex-native encoded source in this phase",
                    dataset.format.as_str()
                ),
            )
        } else if matches!(
            dataset.uri.scheme(),
            UriScheme::S3 | UriScheme::Gcs | UriScheme::Adls
        ) {
            StreamingSource::object_store_byte_range(dataset)
        } else {
            StreamingSource::vortex_dataset(dataset)
        };
        let sink = if target.is_native_vortex() {
            StreamingSink::vortex_native(target)
        } else {
            StreamingSink::compatibility(target)
        };
        Self::new(source, sink, memory, max_parallelism)
    }

    #[must_use]
    pub const fn with_estimated_batch_bytes(mut self, value: ByteSize) -> Self {
        self.estimated_batch_bytes = Some(value);
        self
    }

    pub fn add_operator(&mut self, operator: StreamingOperator) {
        self.operators.push(operator);
    }

    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.source.has_errors()
            || self.sink.has_errors()
            || self.operators.iter().any(StreamingOperator::has_errors)
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
}

/// Report for encoded streaming-batch planning.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct EncodedStreamingBatchPlanReport {
    pub status: EncodedStreamingBatchPlanStatus,
    pub mode: StreamingMode,
    pub input: EncodedStreamingBatchPlanInput,
    pub representation: EncodedBatchRepresentation,
    pub zero_decode: ZeroDecodeStatus,
    pub selection_vector_preserved: bool,
    pub encoded_representation_preserved: bool,
    pub estimated_batch_count: Option<u64>,
    pub estimated_batch_bytes: Option<ByteSize>,
    pub backpressure: BackpressurePolicy,
    pub bounded_parallelism: bool,
    pub bounded_memory: bool,
    pub backpressure_bounded: bool,
    pub materialization_boundary: MaterializationBoundary,
    pub streams_executed: bool,
    pub tasks_executed: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl EncodedStreamingBatchPlanReport {
    /// Builds a side-effect-free encoded streaming-batch planning report.
    ///
    /// # Errors
    /// Returns errors from bounded backpressure policy validation.
    pub fn from_input(input: EncodedStreamingBatchPlanInput) -> Result<Self> {
        let mut out = Self::new_plan_only(input);
        if out.input.has_errors() {
            out.mark_unsupported();
            out.refresh_preservation_flags();
            return Ok(out);
        }
        out.apply_memory_backpressure()?;
        if out.status.is_error() {
            out.refresh_preservation_flags();
            return Ok(out);
        }
        if out.block_object_store_if_needed() || out.apply_sink_materialization() {
            out.refresh_preservation_flags();
            return Ok(out);
        }
        out.apply_operator_boundaries();
        out.refresh_preservation_flags();
        Ok(out)
    }

    fn new_plan_only(input: EncodedStreamingBatchPlanInput) -> Self {
        Self {
            status: EncodedStreamingBatchPlanStatus::Planned,
            mode: StreamingMode::PlanOnly,
            representation: representation_for_source(&input.source),
            zero_decode: input.source.zero_decode,
            selection_vector_preserved: false,
            encoded_representation_preserved: false,
            estimated_batch_count: input.source.estimated_chunks,
            estimated_batch_bytes: input.estimated_batch_bytes,
            backpressure: BackpressurePolicy::disabled(),
            bounded_parallelism: input.max_parallelism > 0,
            bounded_memory: input.memory.required && input.memory.max_memory_bytes.is_some(),
            backpressure_bounded: false,
            materialization_boundary: MaterializationBoundary::none(),
            streams_executed: false,
            tasks_executed: false,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            fallback_execution_allowed: false,
            diagnostics: {
                let mut diagnostics = input.diagnostics.clone();
                diagnostics.extend(collect_streaming_input_diagnostics(
                    &input.source,
                    &input.operators,
                    &input.sink,
                ));
                diagnostics
            },
            input,
        }
    }

    fn mark_unsupported(&mut self) {
        self.status = EncodedStreamingBatchPlanStatus::Unsupported;
        self.representation = EncodedBatchRepresentation::Unsupported;
        self.zero_decode = ZeroDecodeStatus::Unsupported;
    }

    fn apply_memory_backpressure(&mut self) -> Result<()> {
        if !self.input.memory.required {
            return Ok(());
        }
        let Some(max_buffered_bytes) = self.input.memory.max_memory_bytes else {
            self.block_missing_memory_budget(
                "encoded streaming-batch planning requires max_memory_bytes",
                "provide a bounded memory policy before streaming batch execution is enabled",
            );
            return Ok(());
        };
        if max_buffered_bytes.as_bytes() == 0 {
            self.block_missing_memory_budget(
                "encoded streaming-batch planning requires a non-zero memory budget",
                "provide memory_gb greater than zero",
            );
            return Ok(());
        }
        self.backpressure =
            BackpressurePolicy::bounded(self.input.max_parallelism, max_buffered_bytes)?;
        self.backpressure_bounded = self.backpressure.is_bounded();
        Ok(())
    }

    fn block_missing_memory_budget(&mut self, message: &str, next_step: &str) {
        self.status = EncodedStreamingBatchPlanStatus::BlockedByMissingMemoryBudget;
        self.diagnostics.push(Diagnostic::invalid_input(
            "streaming_batch.memory_budget",
            message,
            next_step,
        ));
    }

    fn block_object_store_if_needed(&mut self) -> bool {
        if self.input.source.kind != StreamingSourceKind::ObjectStoreByteRange {
            return false;
        }
        self.status = EncodedStreamingBatchPlanStatus::BlockedByObjectStoreIo;
        self.diagnostics.push(unsupported_streaming_diagnostic(
            "encoded streaming batch object-store source",
            "object-store byte-range streaming is not implemented in this phase",
        ));
        true
    }

    fn apply_sink_materialization(&mut self) -> bool {
        if !self.input.sink.requirement.requires_materialization {
            return false;
        }
        self.status = EncodedStreamingBatchPlanStatus::RequiresMaterialization;
        self.representation = EncodedBatchRepresentation::MaterializedRows;
        self.zero_decode = ZeroDecodeStatus::FullDecodeRequired;
        self.materialization_boundary = MaterializationBoundary::required(
            "sink requires materialization",
            DataWorkLevel::FullMaterialization,
        );
        true
    }

    fn apply_operator_boundaries(&mut self) {
        for operator in &self.input.operators {
            if let Some((representation, zero_decode, data_work_level)) =
                materializing_operator_boundary(operator)
            {
                self.status = EncodedStreamingBatchPlanStatus::RequiresMaterialization;
                self.representation = representation;
                self.zero_decode = zero_decode;
                self.materialization_boundary = MaterializationBoundary::required(
                    "operator requires materialization",
                    data_work_level,
                );
                return;
            }
            if operator.kind == StreamingOperatorKind::EncodedPredicate {
                self.representation = EncodedBatchRepresentation::SelectionVectorEncoded;
                self.selection_vector_preserved = true;
            }
        }
    }

    fn refresh_preservation_flags(&mut self) {
        self.encoded_representation_preserved = self.representation.can_remain_encoded();
        self.selection_vector_preserved = self.representation
            == EncodedBatchRepresentation::SelectionVectorEncoded
            && !self.materialization_boundary.required;
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.input.has_errors()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.streams_executed
            && !self.tasks_executed
            && !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "encoded streaming-batch status: {}\nmode: {}\nsource kind: {}\nsink kind: {}\nrepresentation: {}\nzero decode: {}\nencoded representation preserved: {}\nselection vector preserved: {}\nbounded parallelism: {}\nbounded memory: {}\nbackpressure bounded: {}\nestimated batch count: {}\nestimated batch bytes: {}\nmaterialization boundary: {}\nstreams executed: false\ntasks executed: false\ndata read: false\ndata decoded: false\ndata materialized: false\nrow read: false\nArrow converted: false\nobject-store IO: false\nwrite IO: false\nspill IO performed: false\nfallback execution: disabled",
            self.status.as_str(),
            self.mode.as_str(),
            self.input.source.kind.as_str(),
            self.input.sink.kind.as_str(),
            self.representation.as_str(),
            self.zero_decode.as_str(),
            self.encoded_representation_preserved,
            self.selection_vector_preserved,
            self.bounded_parallelism,
            self.bounded_memory,
            self.backpressure_bounded,
            self.estimated_batch_count
                .map_or("unknown".to_string(), |value| value.to_string()),
            self.estimated_batch_bytes
                .map_or("unknown".to_string(), |value| value.as_bytes().to_string()),
            self.materialization_boundary.summary(),
        )
    }
}

/// Plans encoded streaming batches without executing streams or reading data.
///
/// # Errors
/// Returns errors from bounded backpressure policy validation.
pub fn plan_encoded_streaming_batches(
    input: EncodedStreamingBatchPlanInput,
) -> Result<EncodedStreamingBatchPlanReport> {
    EncodedStreamingBatchPlanReport::from_input(input)
}

fn representation_for_source(source: &StreamingSource) -> EncodedBatchRepresentation {
    match source.kind {
        StreamingSourceKind::MetadataOnlyPseudoChunk => EncodedBatchRepresentation::MetadataOnly,
        StreamingSourceKind::VortexSegment
        | StreamingSourceKind::VortexSplit
        | StreamingSourceKind::ManifestSegmentGroup
        | StreamingSourceKind::ObjectStoreByteRange => EncodedBatchRepresentation::VortexEncoded,
        StreamingSourceKind::ExternalRead | StreamingSourceKind::ArrowLikeBoundary => {
            EncodedBatchRepresentation::ForeignEncoded
        }
        StreamingSourceKind::Unsupported => EncodedBatchRepresentation::Unsupported,
    }
}

fn materializing_operator_boundary(
    operator: &StreamingOperator,
) -> Option<(EncodedBatchRepresentation, ZeroDecodeStatus, DataWorkLevel)> {
    if !operator.materialization.requires_materialization() {
        return None;
    }
    let representation = match operator.data_work_level {
        DataWorkLevel::PartialDecode | DataWorkLevel::LateMaterialization => {
            EncodedBatchRepresentation::PartiallyDecoded
        }
        DataWorkLevel::FullMaterialization => EncodedBatchRepresentation::MaterializedRows,
        _ => EncodedBatchRepresentation::DecodedColumnar,
    };
    let zero_decode = if matches!(representation, EncodedBatchRepresentation::PartiallyDecoded) {
        ZeroDecodeStatus::PartialDecodeRequired
    } else {
        ZeroDecodeStatus::FullDecodeRequired
    };
    Some((representation, zero_decode, operator.data_work_level))
}

fn collect_streaming_input_diagnostics(
    source: &StreamingSource,
    operators: &[StreamingOperator],
    sink: &StreamingSink,
) -> Vec<Diagnostic> {
    source
        .diagnostics
        .iter()
        .chain(
            operators
                .iter()
                .flat_map(|operator| operator.diagnostics.iter()),
        )
        .chain(sink.diagnostics.iter())
        .cloned()
        .collect()
}

fn unsupported_streaming_diagnostic(
    feature: impl Into<String>,
    reason: impl Into<String>,
) -> Diagnostic {
    let reason = reason.into();
    Diagnostic::new(DiagnosticCode::UnsupportedEffect, DiagnosticSeverity::Error, DiagnosticCategory::UnsupportedFeature, "Streaming behavior is unsupported in the current skeleton; fallback execution was not attempted.", Some(feature.into()), Some(format!("{reason}. Spark/DataFusion/DuckDB/Polars/Velox are not fallback engines in ShardLoom.")), Some("Adjust the request to supported planning-only behavior and retain Vortex-native execution/output where possible.".to_string()), FallbackStatus::disabled_by_policy())
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::DatasetUri;
    fn ds() -> DatasetRef {
        DatasetRef::from_uri(DatasetUri::new("data.vortex").unwrap()).unwrap()
    }
    fn vortex_target() -> OutputTarget {
        OutputTarget::from_uri(DatasetUri::new("out.vortex").unwrap())
    }
    fn parquet_target() -> OutputTarget {
        OutputTarget::from_uri(DatasetUri::new("out.parquet").unwrap())
    }
    #[test]
    fn streaming_mode_required_requires_streaming() {
        assert!(StreamingMode::Required.requires_streaming());
    }
    #[test]
    fn streaming_mode_preferred_not_required() {
        assert!(!StreamingMode::Preferred.requires_streaming());
    }
    #[test]
    fn streaming_capability_streaming_can_stream() {
        assert!(StreamingCapability::Streaming.can_stream());
    }
    #[test]
    fn streaming_capability_requires_materialization_cannot_stream() {
        assert!(!StreamingCapability::RequiresMaterialization.can_stream());
    }
    #[test]
    fn data_work_rank_order() {
        assert!(DataWorkLevel::MetadataOnly.rank() < DataWorkLevel::ZeroDecode.rank());
        assert!(DataWorkLevel::ZeroDecode.rank() < DataWorkLevel::FullMaterialization.rank());
    }
    #[test]
    fn data_work_zero_decode_maps_to_encoded_evaluation() {
        assert_eq!(
            DataWorkLevel::ZeroDecode.to_execution_state(),
            ExecutionState::EncodedEvaluation
        );
    }
    #[test]
    fn data_work_full_materialization_maps_to_full_materialization_state() {
        assert_eq!(
            DataWorkLevel::FullMaterialization.to_execution_state(),
            ExecutionState::FullMaterialization
        );
    }
    #[test]
    fn boundary_future_flight_is_future() {
        assert!(BoundaryInteropKind::FutureFlight.is_future_boundary());
    }
    #[test]
    fn backpressure_bounded_rejects_zero() {
        assert!(BackpressurePolicy::bounded(0, ByteSize::from_mib(1)).is_err());
    }
    #[test]
    fn backpressure_bounded_is_bounded() {
        assert!(
            BackpressurePolicy::bounded(1, ByteSize::from_mib(1))
                .unwrap()
                .is_bounded()
        );
    }
    #[test]
    fn backpressure_plan_bounds_in_flight_chunks_by_parallelism() {
        let input =
            BackpressurePlanInput::new(BoundedMemoryPolicy::required(ByteSize::from_mib(64)), 4)
                .unwrap()
                .with_estimated_chunk_bytes(ByteSize::from_mib(8));
        let report = plan_backpressure(input).expect("report");
        assert_eq!(report.status, BackpressurePlanStatus::Bounded);
        assert_eq!(report.mode, BackpressurePlanMode::BoundedStreaming);
        assert!(report.bounded);
        assert_eq!(report.max_in_flight_chunks, Some(4));
        assert_eq!(report.max_buffered_bytes, Some(ByteSize::from_mib(64)));
        assert!(report.is_side_effect_free());
    }
    #[test]
    fn backpressure_plan_disabled_when_memory_not_required() {
        let input = BackpressurePlanInput::new(BoundedMemoryPolicy::best_effort(), 2).unwrap();
        let report = plan_backpressure(input).expect("report");
        assert_eq!(report.status, BackpressurePlanStatus::Disabled);
        assert!(!report.bounded);
        assert!(report.is_side_effect_free());
    }
    #[test]
    fn backpressure_plan_blocks_required_memory_without_budget() {
        let mut memory = BoundedMemoryPolicy::required(ByteSize::from_mib(1));
        memory.max_memory_bytes = None;
        let input = BackpressurePlanInput::new(memory, 2).unwrap();
        let report = plan_backpressure(input).expect("report");
        assert_eq!(
            report.status,
            BackpressurePlanStatus::BlockedByMissingBudget
        );
        assert!(report.has_errors());
        assert!(!report.fallback_execution_allowed);
    }
    #[test]
    fn bounded_memory_required_sets_required() {
        assert!(BoundedMemoryPolicy::required(ByteSize::from_mib(1)).required);
    }
    #[test]
    fn bounded_memory_policy_canonical_label_distinguishes_modes() {
        assert_eq!(
            BoundedMemoryPolicy::required(ByteSize::from_mib(1)).canonical_label(),
            "bounded_memory_required"
        );
        assert_eq!(
            BoundedMemoryPolicy::best_effort().canonical_label(),
            "bounded_memory_best_effort"
        );
    }
    #[test]
    fn materialization_boundary_canonical_labels_work() {
        assert_eq!(
            MaterializationBoundary::none().canonical_label(),
            "no_materialization_boundary"
        );
        assert_eq!(
            MaterializationBoundary::required("x", DataWorkLevel::PartialDecode).canonical_label(),
            "partial_decode_boundary"
        );
    }
    #[test]
    fn encoded_batch_representation_labels_are_stable() {
        assert_eq!(
            EncodedBatchRepresentation::VortexEncoded.as_str(),
            "vortex_encoded"
        );
        assert!(EncodedBatchRepresentation::VortexEncoded.can_remain_encoded());
        assert!(!EncodedBatchRepresentation::MaterializedRows.can_remain_encoded());
    }
    #[test]
    fn source_vortex_preserves_zero_decode() {
        assert_eq!(
            StreamingSource::vortex_dataset(ds()).zero_decode,
            ZeroDecodeStatus::Preserved
        );
    }
    #[test]
    fn source_unsupported_has_errors() {
        assert!(StreamingSource::unsupported("x", "y").has_errors());
    }
    #[test]
    fn operator_metadata_only_work_level() {
        assert_eq!(
            StreamingOperator::metadata_only().data_work_level,
            DataWorkLevel::MetadataOnly
        );
    }
    #[test]
    fn operator_encoded_predicate_work_level() {
        assert_eq!(
            StreamingOperator::encoded_predicate().data_work_level,
            DataWorkLevel::ZeroDecode
        );
    }
    #[test]
    fn operator_requires_materialization() {
        assert!(
            StreamingOperator::requires_materialization(StreamingOperatorKind::Projection, "x")
                .materialization
                .requires_materialization()
        );
    }
    #[test]
    fn sink_kind_vortex_is_native() {
        assert!(StreamingSinkKind::VortexNative.is_native_vortex());
    }
    #[test]
    fn sink_kind_parquet_is_compatibility() {
        assert!(StreamingSinkKind::ParquetCompatibility.is_compatibility());
    }
    #[test]
    fn sink_requirement_vortex_native_preserves() {
        let s = SinkRequirement::vortex_native();
        assert!(s.accepts_encoded && s.preserves_metadata);
    }
    #[test]
    fn sink_requirement_compat_requires_materialization() {
        assert!(
            SinkRequirement::compatibility_materialized(BoundaryInteropKind::CompatibilityExport)
                .requires_materialization
        );
    }
    #[test]
    fn sink_vortex_native_fidelity() {
        assert_eq!(
            StreamingSink::vortex_native(vortex_target())
                .requirement
                .fidelity,
            FidelityLevel::NativeFullFidelity
        );
    }
    #[test]
    fn sink_parquet_has_metadata_loss_warning() {
        let s = StreamingSink::compatibility(parquet_target());
        assert!(
            s.diagnostics
                .iter()
                .any(|d| d.code == DiagnosticCode::MetadataLoss)
        );
    }
    #[test]
    fn materialization_none_not_required() {
        assert!(!MaterializationBoundary::none().required);
    }
    #[test]
    fn materialization_required_required() {
        assert!(
            MaterializationBoundary::required("x", DataWorkLevel::FullMaterialization).required
        );
    }
    #[test]
    fn encoded_streaming_batch_plan_preserves_vortex_encoded_representation() {
        let input = EncodedStreamingBatchPlanInput::for_vortex_to_target(
            ds(),
            vortex_target(),
            BoundedMemoryPolicy::required(ByteSize::from_mib(64)),
            2,
        )
        .unwrap()
        .with_estimated_batch_bytes(ByteSize::from_mib(8));
        let report = plan_encoded_streaming_batches(input).expect("report");
        assert_eq!(report.status, EncodedStreamingBatchPlanStatus::Planned);
        assert_eq!(
            report.representation,
            EncodedBatchRepresentation::VortexEncoded
        );
        assert_eq!(report.zero_decode, ZeroDecodeStatus::Preserved);
        assert!(report.encoded_representation_preserved);
        assert!(report.bounded_parallelism);
        assert!(report.bounded_memory);
        assert!(report.backpressure_bounded);
        assert_eq!(report.estimated_batch_bytes, Some(ByteSize::from_mib(8)));
        assert!(report.is_side_effect_free());
        assert!(!report.fallback_execution_allowed);
    }
    #[test]
    fn encoded_streaming_batch_plan_tracks_selection_vector_encoded_operator() {
        let mut input = EncodedStreamingBatchPlanInput::for_vortex_to_target(
            ds(),
            vortex_target(),
            BoundedMemoryPolicy::required(ByteSize::from_mib(64)),
            2,
        )
        .unwrap();
        input.add_operator(StreamingOperator::encoded_predicate());
        let report = plan_encoded_streaming_batches(input).expect("report");
        assert_eq!(
            report.representation,
            EncodedBatchRepresentation::SelectionVectorEncoded
        );
        assert!(report.selection_vector_preserved);
        assert!(report.encoded_representation_preserved);
        assert!(report.is_side_effect_free());
    }
    #[test]
    fn encoded_streaming_batch_plan_reports_compatibility_materialization() {
        let input = EncodedStreamingBatchPlanInput::for_vortex_to_target(
            ds(),
            parquet_target(),
            BoundedMemoryPolicy::required(ByteSize::from_mib(64)),
            2,
        )
        .unwrap();
        let report = plan_encoded_streaming_batches(input).expect("report");
        assert_eq!(
            report.status,
            EncodedStreamingBatchPlanStatus::RequiresMaterialization
        );
        assert_eq!(
            report.representation,
            EncodedBatchRepresentation::MaterializedRows
        );
        assert_eq!(report.zero_decode, ZeroDecodeStatus::FullDecodeRequired);
        assert!(report.materialization_boundary.required);
        assert!(!report.has_errors());
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn encoded_streaming_batch_plan_refreshes_preservation_for_sink_materialization() {
        let mut input = EncodedStreamingBatchPlanInput::for_vortex_to_target(
            ds(),
            parquet_target(),
            BoundedMemoryPolicy::required(ByteSize::from_mib(64)),
            2,
        )
        .unwrap();
        input.add_operator(StreamingOperator::encoded_predicate());

        let report = plan_encoded_streaming_batches(input).expect("report");

        assert_eq!(
            report.status,
            EncodedStreamingBatchPlanStatus::RequiresMaterialization
        );
        assert_eq!(
            report.representation,
            EncodedBatchRepresentation::MaterializedRows
        );
        assert!(!report.encoded_representation_preserved);
        assert!(!report.selection_vector_preserved);
        assert!(report.materialization_boundary.required);
    }

    #[test]
    fn encoded_streaming_batch_plan_refreshes_preservation_for_operator_materialization() {
        let mut input = EncodedStreamingBatchPlanInput::for_vortex_to_target(
            ds(),
            vortex_target(),
            BoundedMemoryPolicy::required(ByteSize::from_mib(64)),
            2,
        )
        .unwrap();
        input.add_operator(StreamingOperator::encoded_predicate());
        input.add_operator(StreamingOperator::requires_materialization(
            StreamingOperatorKind::AggregateFinal,
            "final aggregate materialization",
        ));

        let report = plan_encoded_streaming_batches(input).expect("report");

        assert_eq!(
            report.status,
            EncodedStreamingBatchPlanStatus::RequiresMaterialization
        );
        assert!(!report.encoded_representation_preserved);
        assert!(!report.selection_vector_preserved);
        assert!(report.materialization_boundary.required);
    }

    #[test]
    fn encoded_streaming_batch_plan_refreshes_preservation_for_object_store_blocker() {
        let dataset =
            DatasetRef::from_uri(DatasetUri::new("s3://bucket/input.vortex").unwrap()).unwrap();
        let input = EncodedStreamingBatchPlanInput::for_vortex_to_target(
            dataset,
            vortex_target(),
            BoundedMemoryPolicy::required(ByteSize::from_mib(64)),
            2,
        )
        .unwrap();

        let report = plan_encoded_streaming_batches(input).expect("report");

        assert_eq!(
            report.status,
            EncodedStreamingBatchPlanStatus::BlockedByObjectStoreIo
        );
        assert!(report.encoded_representation_preserved);
        assert!(!report.selection_vector_preserved);
    }

    #[test]
    fn encoded_streaming_batch_plan_blocks_object_store_source_without_io() {
        let dataset =
            DatasetRef::from_uri(DatasetUri::new("s3://bucket/input.vortex").unwrap()).unwrap();
        let input = EncodedStreamingBatchPlanInput::for_vortex_to_target(
            dataset,
            vortex_target(),
            BoundedMemoryPolicy::required(ByteSize::from_mib(64)),
            2,
        )
        .unwrap();
        let report = plan_encoded_streaming_batches(input).expect("report");
        assert_eq!(
            report.status,
            EncodedStreamingBatchPlanStatus::BlockedByObjectStoreIo
        );
        assert_eq!(
            report.input.source.kind,
            StreamingSourceKind::ObjectStoreByteRange
        );
        assert!(report.has_errors());
        assert!(!report.object_store_io);
        assert!(report.is_side_effect_free());
    }
    #[test]
    fn encoded_streaming_batch_plan_rejects_zero_parallelism() {
        assert!(
            EncodedStreamingBatchPlanInput::for_vortex_to_target(
                ds(),
                vortex_target(),
                BoundedMemoryPolicy::required(ByteSize::from_mib(64)),
                0
            )
            .is_err()
        );
    }
    #[test]
    fn stage_rejects_empty_name() {
        assert!(
            StreamingStage::new(
                "  ",
                StreamingCapability::Streaming,
                DataWorkLevel::MetadataOnly
            )
            .is_err()
        );
    }
    #[test]
    fn skeleton_vortex_target_no_materialization() {
        assert!(
            !StreamingPlanSkeleton::for_vortex_to_target(ds(), vortex_target())
                .requires_materialization()
        );
    }
    #[test]
    fn skeleton_parquet_target_requires_materialization() {
        assert!(
            StreamingPlanSkeleton::for_vortex_to_target(ds(), parquet_target())
                .requires_materialization()
        );
    }
    #[test]
    fn skeleton_human_text_has_fallback_disabled() {
        assert!(
            StreamingPlanSkeleton::for_vortex_to_target(ds(), parquet_target())
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
    #[test]
    fn skeleton_best_data_work_level_promotes_to_materialization_for_compat_sink() {
        assert_eq!(
            StreamingPlanSkeleton::for_vortex_to_target(ds(), parquet_target())
                .best_data_work_level(),
            DataWorkLevel::FullMaterialization
        );
    }
    #[test]
    fn skeleton_human_text_includes_operator_diagnostics() {
        let mut p = StreamingPlanSkeleton::new(
            StreamingMode::PlanOnly,
            StreamingSource::vortex_dataset(ds()),
            StreamingSink::vortex_native(vortex_target()),
        );
        p.add_operator(StreamingOperator::unsupported(
            StreamingOperatorKind::Projection,
            "projection",
            "projection streaming unsupported",
        ));
        let text = p.to_human_text();
        assert!(text.contains("diagnostics:"));
        assert!(!text.contains("diagnostics: none"));
    }
    #[test]
    fn skeleton_human_text_deduplicates_stage_and_sink_diagnostics() {
        let text =
            StreamingPlanSkeleton::for_vortex_to_target(ds(), parquet_target()).to_human_text();
        let matches = text
            .lines()
            .filter(|l| {
                l.contains(
                    "Compatibility sink may lose Vortex physical layout and encoding metadata.",
                )
            })
            .count();
        assert_eq!(matches, 1);
    }
    #[test]
    fn skeleton_unknown_target_is_marked_unsupported() {
        let p = StreamingPlanSkeleton::for_vortex_to_target(
            ds(),
            OutputTarget::new(
                DatasetUri::new("out.unknown").expect("valid uri"),
                OutputTargetKind::Unknown,
            ),
        );
        assert_eq!(p.status, StreamingPlanStatus::Unsupported);
    }
    #[test]
    fn skeleton_unsupported_has_errors() {
        assert!(
            StreamingPlanSkeleton::unsupported(
                StreamingSource::vortex_dataset(ds()),
                StreamingSink::vortex_native(vortex_target()),
                "streaming",
                "unsupported"
            )
            .has_errors()
        );
    }
    #[test]
    fn human_text_keeps_distinct_diagnostics_with_same_message() {
        let mut p = StreamingPlanSkeleton::for_vortex_to_target(ds(), vortex_target());
        p.diagnostics.push(unsupported_streaming_diagnostic(
            "source.scan",
            "operator requires materialization",
        ));
        p.diagnostics.push(unsupported_streaming_diagnostic(
            "sink.write",
            "sink requires ordered output",
        ));

        let text = p.to_human_text();
        assert!(text.contains("feature=source.scan"));
        assert!(text.contains("feature=sink.write"));
        assert!(text.contains("reason=operator requires materialization"));
        assert!(text.contains("reason=sink requires ordered output"));
    }
}
