//! Streaming, zero-copy boundary, and sink-driven planning skeleton.
//!
//! This module defines planning-only domain types. It does not execute streaming,
//! read object stores, or perform Vortex/Arrow IO.

use crate::ByteSize;
use shardloom_core::{
    DatasetRef, Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity, FallbackStatus,
    FidelityLevel, MaterializationRequirement, OutputTarget, OutputTargetKind, Result,
    ShardLoomError,
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
    fn bounded_memory_required_sets_required() {
        assert!(BoundedMemoryPolicy::required(ByteSize::from_mib(1)).required);
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
