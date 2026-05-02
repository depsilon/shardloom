//! Core encoded execution domain types.

#![allow(clippy::must_use_candidate, clippy::return_self_not_must_use)]

use crate::{Result, ShardLoomError};

/// Stable identifier for an encoded segment.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SegmentId(String);

impl SegmentId {
    /// Creates a validated segment identifier.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when the input is empty or whitespace only.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "segment id must not be empty".to_string(),
            ));
        }
        Ok(Self(value))
    }

    /// Returns the segment identifier string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stable column reference used by encoded segments and predicates.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef(String);

impl ColumnRef {
    /// Creates a validated column reference.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` when the input is empty or whitespace only.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "column name must not be empty".to_string(),
            ));
        }
        Ok(Self(value))
    }

    /// Returns the column reference string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub length: u64,
}
impl ByteRange {
    pub fn new(start: u64, length: u64) -> Self {
        Self { start, length }
    }
    #[must_use]
    pub fn end_exclusive(&self) -> u64 {
        self.start.saturating_add(self.length)
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalDType {
    Boolean,
    Int64,
    UInt64,
    Float64,
    Utf8,
    Binary,
    Date32,
    TimestampMicros,
    Struct,
    List,
    Unknown,
    Extension(String),
}
impl LogicalDType {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Boolean => "boolean",
            Self::Int64 => "int64",
            Self::UInt64 => "uint64",
            Self::Float64 => "float64",
            Self::Utf8 => "utf8",
            Self::Binary => "binary",
            Self::Date32 => "date32",
            Self::TimestampMicros => "timestamp_micros",
            Self::Struct => "struct",
            Self::List => "list",
            Self::Unknown => "unknown",
            Self::Extension(_) => "extension",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodingKind {
    Unknown,
    Plain,
    Constant,
    Dictionary,
    RunLength,
    Delta,
    BitPacked,
    FsstLike,
    FastLanesLike,
    AlpLike,
    VortexNative(String),
}
impl EncodingKind {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Unknown => "unknown",
            Self::Plain => "plain",
            Self::Constant => "constant",
            Self::Dictionary => "dictionary",
            Self::RunLength => "run_length",
            Self::Delta => "delta",
            Self::BitPacked => "bit_packed",
            Self::FsstLike => "fsst_like",
            Self::FastLanesLike => "fastlanes_like",
            Self::AlpLike => "alp_like",
            Self::VortexNative(_) => "vortex_native",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LayoutKind {
    Unknown,
    Flat,
    Chunked,
    Struct,
    List,
    Sparse,
    VortexNative(String),
}
impl LayoutKind {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Unknown => "unknown",
            Self::Flat => "flat",
            Self::Chunked => "chunked",
            Self::Struct => "struct",
            Self::List => "list",
            Self::Sparse => "sparse",
            Self::VortexNative(_) => "vortex_native",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Nullability {
    NonNullable,
    Nullable,
    Unknown,
}
impl Nullability {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NonNullable => "non_nullable",
            Self::Nullable => "nullable",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Ascending,
    Descending,
    Unsorted,
    Unknown,
}
impl SortOrder {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ascending => "ascending",
            Self::Descending => "descending",
            Self::Unsorted => "unsorted",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatisticsExactness {
    Exact,
    Approximate,
    Unknown,
}
impl StatisticsExactness {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Approximate => "approximate",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum StatValue {
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Utf8(String),
}
impl StatValue {
    #[must_use]
    pub fn dtype(&self) -> LogicalDType {
        match self {
            Self::Boolean(_) => LogicalDType::Boolean,
            Self::Int64(_) => LogicalDType::Int64,
            Self::UInt64(_) => LogicalDType::UInt64,
            Self::Float64(_) => LogicalDType::Float64,
            Self::Utf8(_) => LogicalDType::Utf8,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SegmentStats {
    pub row_count: Option<u64>,
    pub null_count: Option<u64>,
    pub min_value: Option<StatValue>,
    pub max_value: Option<StatValue>,
    pub true_count: Option<u64>,
    pub false_count: Option<u64>,
    pub run_count: Option<u64>,
    pub is_constant: Option<bool>,
    pub sort_order: SortOrder,
    pub exactness: StatisticsExactness,
}
impl SegmentStats {
    pub fn unknown() -> Self {
        Self {
            row_count: None,
            null_count: None,
            min_value: None,
            max_value: None,
            true_count: None,
            false_count: None,
            run_count: None,
            is_constant: None,
            sort_order: SortOrder::Unknown,
            exactness: StatisticsExactness::Unknown,
        }
    }
    pub fn with_row_count(row_count: u64) -> Self {
        Self {
            row_count: Some(row_count),
            ..Self::unknown()
        }
    }
    pub fn is_empty(&self) -> Option<bool> {
        self.row_count.map(|v| v == 0)
    }
    pub fn is_all_null(&self) -> Option<bool> {
        match (self.row_count, self.null_count) {
            (Some(r), Some(n)) if n <= r => Some(r == n),
            _ => None,
        }
    }
    #[allow(clippy::cast_precision_loss)]
    pub fn null_fraction(&self) -> Option<f64> {
        match (self.row_count, self.null_count) {
            (Some(0), _) => None,
            (Some(r), Some(n)) if n <= r => Some(n as f64 / r as f64),
            _ => None,
        }
    }
    pub fn has_min_max(&self) -> bool {
        self.min_value.is_some() && self.max_value.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentLayout {
    pub encoding: EncodingKind,
    pub layout: LayoutKind,
    pub byte_ranges: Vec<ByteRange>,
    pub physical_size_bytes: Option<u64>,
}
impl SegmentLayout {
    pub fn new(encoding: EncodingKind, layout: LayoutKind) -> Self {
        Self {
            encoding,
            layout,
            byte_ranges: Vec::new(),
            physical_size_bytes: None,
        }
    }
    pub fn with_byte_ranges(mut self, byte_ranges: Vec<ByteRange>) -> Self {
        self.byte_ranges = byte_ranges;
        self
    }
    pub fn has_byte_ranges(&self) -> bool {
        !self.byte_ranges.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EncodedSegment {
    pub id: SegmentId,
    pub column: ColumnRef,
    pub dtype: LogicalDType,
    pub nullability: Nullability,
    pub layout: SegmentLayout,
    pub stats: SegmentStats,
}
impl EncodedSegment {
    pub fn new(
        id: SegmentId,
        column: ColumnRef,
        dtype: LogicalDType,
        nullability: Nullability,
        layout: SegmentLayout,
        stats: SegmentStats,
    ) -> Self {
        Self {
            id,
            column,
            dtype,
            nullability,
            layout,
            stats,
        }
    }
    pub fn execution_summary(&self) -> String {
        format!(
            "segment={} column={} dtype={} encoding={} layout={} metadata_available={}",
            self.id.as_str(),
            self.column.as_str(),
            self.dtype.as_str(),
            self.layout.encoding.as_str(),
            self.layout.layout.as_str(),
            self.can_use_metadata()
        )
    }
    pub fn can_use_metadata(&self) -> bool {
        self.stats.row_count.is_some()
            || self.stats.has_min_max()
            || self.stats.null_count.is_some()
            || self.stats.true_count.is_some()
            || self.stats.false_count.is_some()
            || self.stats.run_count.is_some()
            || self.stats.is_constant.is_some()
    }
    pub fn has_byte_ranges(&self) -> bool {
        self.layout.has_byte_ranges()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}
impl ComparisonOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Eq => "eq",
            Self::NotEq => "not_eq",
            Self::Lt => "lt",
            Self::LtEq => "lt_eq",
            Self::Gt => "gt",
            Self::GtEq => "gt_eq",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PredicateExpr {
    AlwaysTrue,
    AlwaysFalse,
    IsNull {
        column: ColumnRef,
    },
    IsNotNull {
        column: ColumnRef,
    },
    Compare {
        column: ColumnRef,
        op: ComparisonOp,
        value: StatValue,
    },
}
impl PredicateExpr {
    pub fn summary(&self) -> String {
        match self {
            Self::AlwaysTrue => "always_true".to_string(),
            Self::AlwaysFalse => "always_false".to_string(),
            Self::IsNull { column } => format!("{} is null", column.as_str()),
            Self::IsNotNull { column } => format!("{} is not null", column.as_str()),
            Self::Compare { column, op, value } => {
                format!("{} {} {:?}", column.as_str(), op.as_str(), value)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateProof {
    AlwaysTrue { reason: String },
    AlwaysFalse { reason: String },
    MayMatch { reason: String },
    Unknown { reason: String },
    Unsupported { reason: String },
}
impl PredicateProof {
    pub fn is_prunable(&self) -> bool {
        matches!(self, Self::AlwaysFalse { .. })
    }
    pub fn reason(&self) -> &str {
        match self {
            Self::AlwaysTrue { reason }
            | Self::AlwaysFalse { reason }
            | Self::MayMatch { reason }
            | Self::Unknown { reason }
            | Self::Unsupported { reason } => reason,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PruningDecision {
    MetadataOnlyAnswer { reason: String },
    PruneSegment { reason: String },
    ReadEncoded { reason: String },
    NeedPartialDecode { reason: String },
    NeedMaterialization { reason: String },
    Unsupported { reason: String },
}
impl PruningDecision {
    pub fn from_proof(proof: PredicateProof) -> Self {
        match proof {
            PredicateProof::AlwaysFalse { reason } => Self::PruneSegment { reason },
            PredicateProof::AlwaysTrue { reason } | PredicateProof::MayMatch { reason } => {
                Self::ReadEncoded { reason }
            }
            PredicateProof::Unknown { reason } => Self::NeedPartialDecode { reason },
            PredicateProof::Unsupported { reason } => Self::Unsupported { reason },
        }
    }
    pub fn requires_read(&self) -> bool {
        matches!(
            self,
            Self::ReadEncoded { .. }
                | Self::NeedPartialDecode { .. }
                | Self::NeedMaterialization { .. }
        )
    }
    pub fn reason(&self) -> &str {
        match self {
            Self::MetadataOnlyAnswer { reason }
            | Self::PruneSegment { reason }
            | Self::ReadEncoded { reason }
            | Self::NeedPartialDecode { reason }
            | Self::NeedMaterialization { reason }
            | Self::Unsupported { reason } => reason,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionState {
    MetadataOnly,
    Pruned,
    EncodedEvaluation,
    PartialDecode,
    FullMaterialization,
    Translation,
    ExternalRead,
    ExternalWrite,
    ModelCall,
    Unsupported,
}
impl ExecutionState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::Pruned => "pruned",
            Self::EncodedEvaluation => "encoded_evaluation",
            Self::PartialDecode => "partial_decode",
            Self::FullMaterialization => "full_materialization",
            Self::Translation => "translation",
            Self::ExternalRead => "external_read",
            Self::ExternalWrite => "external_write",
            Self::ModelCall => "model_call",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaterializationPolicy {
    Never,
    Late,
    Partial { reason: String },
    Full { reason: String },
}
impl MaterializationPolicy {
    pub fn requires_materialization(&self) -> bool {
        matches!(self, Self::Partial { .. } | Self::Full { .. })
    }
    pub fn summary(&self) -> String {
        match self {
            Self::Never => "never".to_string(),
            Self::Late => "late".to_string(),
            Self::Partial { reason } => format!("partial: {reason}"),
            Self::Full { reason } => format!("full: {reason}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectionVector {
    All { row_count: u64 },
    None,
    Indices(Vec<u64>),
}
impl SelectionVector {
    pub fn all(row_count: u64) -> Self {
        Self::All { row_count }
    }
    pub fn none() -> Self {
        Self::None
    }
    pub fn from_indices(indices: Vec<u64>) -> Self {
        Self::Indices(indices)
    }
    pub fn selected_count(&self) -> u64 {
        match self {
            Self::All { row_count } => *row_count,
            Self::None => 0,
            Self::Indices(v) => v.len() as u64,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.selected_count() == 0
    }
    pub fn is_all(&self) -> bool {
        matches!(self, Self::All { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodedEvalCapability {
    MetadataOnly { reason: String },
    Encoded { reason: String },
    PartialDecodeRequired { reason: String },
    FullMaterializationRequired { reason: String },
    Unsupported { reason: String },
}
impl EncodedEvalCapability {
    pub fn execution_state(&self) -> ExecutionState {
        match self {
            Self::MetadataOnly { .. } => ExecutionState::MetadataOnly,
            Self::Encoded { .. } => ExecutionState::EncodedEvaluation,
            Self::PartialDecodeRequired { .. } => ExecutionState::PartialDecode,
            Self::FullMaterializationRequired { .. } => ExecutionState::FullMaterialization,
            Self::Unsupported { .. } => ExecutionState::Unsupported,
        }
    }
    pub fn reason(&self) -> &str {
        match self {
            Self::MetadataOnly { reason }
            | Self::Encoded { reason }
            | Self::PartialDecodeRequired { reason }
            | Self::FullMaterializationRequired { reason }
            | Self::Unsupported { reason } => reason,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn segment_id_rejects_empty_ids() {
        assert!(SegmentId::new("  ").is_err());
    }
    #[test]
    fn column_ref_rejects_empty_names() {
        assert!(ColumnRef::new("").is_err());
    }
    #[test]
    fn byte_range_end_exclusive_saturating() {
        let br = ByteRange::new(u64::MAX - 1, 10);
        assert_eq!(br.end_exclusive(), u64::MAX);
    }
    #[test]
    fn segment_stats_is_empty() {
        assert_eq!(SegmentStats::with_row_count(0).is_empty(), Some(true));
        assert_eq!(SegmentStats::with_row_count(5).is_empty(), Some(false));
    }
    #[test]
    fn segment_stats_is_all_null() {
        let mut s = SegmentStats::with_row_count(3);
        s.null_count = Some(3);
        assert_eq!(s.is_all_null(), Some(true));
    }

    #[test]
    fn segment_stats_is_all_null_rejects_impossible_counts() {
        let mut s = SegmentStats::with_row_count(2);
        s.null_count = Some(3);
        assert_eq!(s.is_all_null(), None);
    }

    #[test]
    fn segment_stats_null_fraction() {
        let mut s = SegmentStats::with_row_count(10);
        s.null_count = Some(2);
        assert_eq!(s.null_fraction(), Some(0.2));
    }

    #[test]
    fn segment_stats_null_fraction_rejects_impossible_counts() {
        let mut s = SegmentStats::with_row_count(2);
        s.null_count = Some(3);
        assert_eq!(s.null_fraction(), None);
    }

    #[test]
    fn encoded_segment_can_use_metadata_with_boolean_or_run_stats() {
        let mut stats = SegmentStats::unknown();
        stats.true_count = Some(10);
        let seg = EncodedSegment::new(
            SegmentId::new("s2").unwrap(),
            ColumnRef::new("flag").unwrap(),
            LogicalDType::Boolean,
            Nullability::Nullable,
            SegmentLayout::new(EncodingKind::RunLength, LayoutKind::Flat),
            stats,
        );
        assert!(seg.can_use_metadata());

        let mut stats = SegmentStats::unknown();
        stats.run_count = Some(4);
        let seg = EncodedSegment::new(
            SegmentId::new("s3").unwrap(),
            ColumnRef::new("rle").unwrap(),
            LogicalDType::Int64,
            Nullability::Nullable,
            SegmentLayout::new(EncodingKind::RunLength, LayoutKind::Flat),
            stats,
        );
        assert!(seg.can_use_metadata());
    }
    #[test]
    fn segment_stats_has_min_max() {
        let mut s = SegmentStats::unknown();
        s.min_value = Some(StatValue::Int64(1));
        s.max_value = Some(StatValue::Int64(2));
        assert!(s.has_min_max());
    }
    #[test]
    fn encoded_segment_can_use_metadata_with_row_count() {
        let seg = EncodedSegment::new(
            SegmentId::new("s1").unwrap(),
            ColumnRef::new("c1").unwrap(),
            LogicalDType::Int64,
            Nullability::Nullable,
            SegmentLayout::new(EncodingKind::Plain, LayoutKind::Flat),
            SegmentStats::with_row_count(1),
        );
        assert!(seg.can_use_metadata());
    }
    #[test]
    fn predicate_proof_is_prunable_only_always_false() {
        assert!(PredicateProof::AlwaysFalse { reason: "x".into() }.is_prunable());
        assert!(!PredicateProof::AlwaysTrue { reason: "x".into() }.is_prunable());
    }
    #[test]
    fn pruning_decision_maps_always_false() {
        let d = PruningDecision::from_proof(PredicateProof::AlwaysFalse { reason: "r".into() });
        assert!(matches!(d, PruningDecision::PruneSegment { .. }));
    }
    #[test]
    fn pruning_decision_maps_unknown() {
        let d = PruningDecision::from_proof(PredicateProof::Unknown { reason: "r".into() });
        assert!(matches!(d, PruningDecision::NeedPartialDecode { .. }));
    }
    #[test]
    fn selection_vector_counts() {
        assert_eq!(SelectionVector::all(9).selected_count(), 9);
        assert_eq!(SelectionVector::none().selected_count(), 0);
        assert_eq!(
            SelectionVector::from_indices(vec![1, 3, 7]).selected_count(),
            3
        );
    }
    #[test]
    fn materialization_policy_requires_materialization() {
        assert!(!MaterializationPolicy::Never.requires_materialization());
        assert!(!MaterializationPolicy::Late.requires_materialization());
        assert!(MaterializationPolicy::Partial { reason: "x".into() }.requires_materialization());
        assert!(MaterializationPolicy::Full { reason: "x".into() }.requires_materialization());
    }
    #[test]
    fn encoded_eval_capability_maps_execution_state() {
        assert_eq!(
            EncodedEvalCapability::MetadataOnly { reason: "x".into() }.execution_state(),
            ExecutionState::MetadataOnly
        );
        assert_eq!(
            EncodedEvalCapability::Encoded { reason: "x".into() }.execution_state(),
            ExecutionState::EncodedEvaluation
        );
        assert_eq!(
            EncodedEvalCapability::PartialDecodeRequired { reason: "x".into() }.execution_state(),
            ExecutionState::PartialDecode
        );
        assert_eq!(
            EncodedEvalCapability::FullMaterializationRequired { reason: "x".into() }
                .execution_state(),
            ExecutionState::FullMaterialization
        );
        assert_eq!(
            EncodedEvalCapability::Unsupported { reason: "x".into() }.execution_state(),
            ExecutionState::Unsupported
        );
    }
}
