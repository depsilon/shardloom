//! Core encoded execution domain types.

#![allow(clippy::must_use_candidate, clippy::return_self_not_must_use)]

use crate::{Diagnostic, DiagnosticCode, Result, ShardLoomError};

const ENCODED_PREDICATE_EVALUATION_SCHEMA_VERSION: &str =
    "shardloom.encoded_predicate_evaluation.v1";

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
            Self::Extension(value) => value.as_str(),
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
    Sequence,
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
            Self::Sequence => "sequence",
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

/// One run in a run-length encoded value batch.
#[derive(Debug, Clone, PartialEq)]
pub struct EncodedValueRun {
    pub value: Option<StatValue>,
    pub len: u64,
}
impl EncodedValueRun {
    pub fn new(value: Option<StatValue>, len: u64) -> Self {
        Self { value, len }
    }
}

/// Minimal encoded-value batch used by native predicate kernels.
///
/// This is an execution-kernel input, not a file reader. It lets `ShardLoom`
/// evaluate predicates against encoded forms such as constants, dictionary
/// codes, run-length runs, bit-packed integer lanes, and arithmetic sequences
/// without adding a fallback engine or requiring decoded row materialization.
#[derive(Debug, Clone, PartialEq)]
pub enum EncodedValueBatch {
    Constant {
        value: Option<StatValue>,
        row_count: u64,
    },
    Dictionary {
        dictionary: Vec<Option<StatValue>>,
        codes: Vec<Option<u32>>,
    },
    RunLength {
        runs: Vec<EncodedValueRun>,
    },
    BitPackedUnsigned {
        bit_width: u8,
        values: Vec<u64>,
    },
    ArithmeticSequence {
        base: StatValue,
        multiplier: StatValue,
        row_count: u64,
    },
}
impl EncodedValueBatch {
    #[must_use]
    pub fn row_count(&self) -> Option<u64> {
        match self {
            Self::Dictionary { codes, .. } => u64::try_from(codes.len()).ok(),
            Self::RunLength { runs } => runs
                .iter()
                .try_fold(0_u64, |total, run| total.checked_add(run.len)),
            Self::BitPackedUnsigned { values, .. } => u64::try_from(values.len()).ok(),
            Self::Constant { row_count, .. } | Self::ArithmeticSequence { row_count, .. } => {
                Some(*row_count)
            }
        }
    }

    #[must_use]
    pub const fn encoding_kind(&self) -> EncodingKind {
        match self {
            Self::Constant { .. } => EncodingKind::Constant,
            Self::Dictionary { .. } => EncodingKind::Dictionary,
            Self::RunLength { .. } => EncodingKind::RunLength,
            Self::BitPackedUnsigned { .. } => EncodingKind::BitPacked,
            Self::ArithmeticSequence { .. } => EncodingKind::Sequence,
        }
    }

    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Self::Constant { .. } => "constant",
            Self::Dictionary { .. } => "dictionary",
            Self::RunLength { .. } => "run_length",
            Self::BitPackedUnsigned { .. } => "bit_packed",
            Self::ArithmeticSequence { .. } => "sequence",
        }
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
    #[must_use]
    pub const fn column(&self) -> Option<&ColumnRef> {
        match self {
            Self::IsNull { column } | Self::IsNotNull { column } | Self::Compare { column, .. } => {
                Some(column)
            }
            Self::AlwaysTrue | Self::AlwaysFalse => None,
        }
    }

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

/// Segment-local status for predicate evaluation over an encoded segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodedPredicateEvaluationStatus {
    /// A full segment selection vector was emitted without reading encoded values.
    SelectedAll,
    /// An empty selection vector was emitted without reading encoded values.
    SelectedNone,
    /// A sparse selection vector was emitted from encoded values.
    SelectedIndices,
    /// The metadata proof is conservative and an encoded-value kernel is required.
    NeedsEncodedValues,
    /// Required segment metadata is missing before a stable selection vector can be emitted.
    MissingSegmentMetadata,
    /// The predicate cannot be evaluated against this encoded segment.
    Unsupported,
}

impl EncodedPredicateEvaluationStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::SelectedAll => "selected_all",
            Self::SelectedNone => "selected_none",
            Self::SelectedIndices => "selected_indices",
            Self::NeedsEncodedValues => "needs_encoded_values",
            Self::MissingSegmentMetadata => "missing_segment_metadata",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }

    #[must_use]
    pub const fn emits_selection_vector(&self) -> bool {
        matches!(
            self,
            Self::SelectedAll | Self::SelectedNone | Self::SelectedIndices
        )
    }
}

/// Report emitted when a predicate is evaluated as far as possible against one
/// encoded segment without decoding or materializing values.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct EncodedPredicateEvaluationReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub segment_id: SegmentId,
    pub segment_column: ColumnRef,
    pub predicate: PredicateExpr,
    pub proof: PredicateProof,
    pub status: EncodedPredicateEvaluationStatus,
    pub capability: EncodedEvalCapability,
    pub execution_state: ExecutionState,
    pub selection_vector: Option<SelectionVector>,
    pub row_count: Option<u64>,
    pub selected_count: Option<u64>,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl EncodedPredicateEvaluationReport {
    #[must_use]
    fn selected(
        segment: &EncodedSegment,
        predicate: &PredicateExpr,
        proof: PredicateProof,
        selection_vector: SelectionVector,
        execution_state: ExecutionState,
    ) -> Self {
        let selected_count = Some(selection_vector.selected_count());
        let status = match &selection_vector {
            SelectionVector::All { .. } => EncodedPredicateEvaluationStatus::SelectedAll,
            SelectionVector::None => EncodedPredicateEvaluationStatus::SelectedNone,
            SelectionVector::Indices(_) => EncodedPredicateEvaluationStatus::SelectedIndices,
        };
        let reason = proof.reason().to_string();
        let capability = if matches!(
            execution_state,
            ExecutionState::MetadataOnly | ExecutionState::Pruned
        ) {
            EncodedEvalCapability::MetadataOnly { reason }
        } else {
            EncodedEvalCapability::Encoded { reason }
        };
        Self::new(
            segment,
            predicate,
            proof,
            status,
            capability,
            execution_state,
            Some(selection_vector),
            selected_count,
            Vec::new(),
        )
    }

    #[must_use]
    fn blocked(
        segment: &EncodedSegment,
        predicate: &PredicateExpr,
        proof: PredicateProof,
        status: EncodedPredicateEvaluationStatus,
        capability: EncodedEvalCapability,
        diagnostic: Option<Diagnostic>,
    ) -> Self {
        let diagnostics = diagnostic.into_iter().collect();
        Self::new(
            segment,
            predicate,
            proof,
            status,
            capability,
            status.execution_state(),
            None,
            None,
            diagnostics,
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[must_use]
    fn new(
        segment: &EncodedSegment,
        predicate: &PredicateExpr,
        proof: PredicateProof,
        status: EncodedPredicateEvaluationStatus,
        capability: EncodedEvalCapability,
        execution_state: ExecutionState,
        selection_vector: Option<SelectionVector>,
        selected_count: Option<u64>,
        diagnostics: Vec<Diagnostic>,
    ) -> Self {
        Self {
            schema_version: ENCODED_PREDICATE_EVALUATION_SCHEMA_VERSION,
            report_id: format!("{}.predicate-evaluation", segment.id.as_str()),
            segment_id: segment.id.clone(),
            segment_column: segment.column.clone(),
            predicate: predicate.clone(),
            proof,
            status,
            capability,
            execution_state,
            selection_vector,
            row_count: segment.stats.row_count,
            selected_count,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            diagnostics,
        }
    }

    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.fallback_attempted
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    crate::DiagnosticSeverity::Error | crate::DiagnosticSeverity::Fatal
                )
            })
    }
}

impl EncodedPredicateEvaluationStatus {
    const fn execution_state(self) -> ExecutionState {
        match self {
            Self::SelectedAll => ExecutionState::MetadataOnly,
            Self::SelectedNone => ExecutionState::Pruned,
            Self::SelectedIndices | Self::NeedsEncodedValues => ExecutionState::EncodedEvaluation,
            Self::MissingSegmentMetadata => ExecutionState::PartialDecode,
            Self::Unsupported => ExecutionState::Unsupported,
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

#[must_use]
pub fn prove_predicate_from_stats(
    predicate: &PredicateExpr,
    stats: &SegmentStats,
) -> PredicateProof {
    match predicate {
        PredicateExpr::AlwaysTrue => PredicateProof::AlwaysTrue {
            reason: "always true predicate".to_string(),
        },
        PredicateExpr::AlwaysFalse => PredicateProof::AlwaysFalse {
            reason: "always false predicate".to_string(),
        },
        PredicateExpr::IsNull { .. } => match (stats.row_count, stats.null_count) {
            (Some(0), _) => PredicateProof::AlwaysFalse {
                reason: "segment row_count == 0".to_string(),
            },
            (_, Some(0)) => PredicateProof::AlwaysFalse {
                reason: "null_count == 0".to_string(),
            },
            (Some(r), Some(n)) if r == n => PredicateProof::AlwaysTrue {
                reason: "all rows are null".to_string(),
            },
            _ => PredicateProof::Unknown {
                reason: "insufficient null statistics".to_string(),
            },
        },
        PredicateExpr::IsNotNull { .. } => match (stats.row_count, stats.null_count) {
            (Some(0), _) => PredicateProof::AlwaysFalse {
                reason: "segment row_count == 0".to_string(),
            },
            (_, Some(0)) => PredicateProof::AlwaysTrue {
                reason: "null_count == 0".to_string(),
            },
            (Some(r), Some(n)) if r == n => PredicateProof::AlwaysFalse {
                reason: "all rows are null".to_string(),
            },
            _ => PredicateProof::Unknown {
                reason: "insufficient null statistics".to_string(),
            },
        },
        PredicateExpr::Compare { op, value, .. } => prove_comparison_from_stats(*op, value, stats),
    }
}

#[must_use]
#[allow(clippy::too_many_lines)]
pub fn evaluate_predicate_on_encoded_segment(
    predicate: &PredicateExpr,
    segment: &EncodedSegment,
) -> EncodedPredicateEvaluationReport {
    if let Some(column) = predicate.column()
        && column != &segment.column
    {
        let reason = format!(
            "predicate column {} does not match encoded segment column {}",
            column.as_str(),
            segment.column.as_str()
        );
        return EncodedPredicateEvaluationReport::blocked(
            segment,
            predicate,
            PredicateProof::Unsupported {
                reason: reason.clone(),
            },
            EncodedPredicateEvaluationStatus::Unsupported,
            EncodedEvalCapability::Unsupported {
                reason: reason.clone(),
            },
            Some(Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "encoded_predicate_evaluation",
                reason,
                Some(
                    "Evaluate this predicate against the matching encoded column segment."
                        .to_string(),
                ),
            )),
        );
    }

    let proof = prove_predicate_from_stats(predicate, &segment.stats);
    match &proof {
        PredicateProof::AlwaysTrue { reason } => {
            let Some(row_count) = segment.stats.row_count else {
                return EncodedPredicateEvaluationReport::blocked(
                    segment,
                    predicate,
                    proof.clone(),
                    EncodedPredicateEvaluationStatus::MissingSegmentMetadata,
                    EncodedEvalCapability::PartialDecodeRequired {
                        reason: "row_count metadata is required to emit a full selection vector"
                            .to_string(),
                    },
                    Some(Diagnostic::not_implemented(
                        "encoded_predicate_evaluation",
                        "row_count metadata is required to emit a full selection vector",
                        "Provide segment row_count metadata before using metadata-proven all-row predicate evaluation.",
                    )),
                );
            };
            EncodedPredicateEvaluationReport::selected(
                segment,
                predicate,
                PredicateProof::AlwaysTrue {
                    reason: reason.clone(),
                },
                SelectionVector::all(row_count),
                ExecutionState::MetadataOnly,
            )
        }
        PredicateProof::AlwaysFalse { reason } => EncodedPredicateEvaluationReport::selected(
            segment,
            predicate,
            PredicateProof::AlwaysFalse {
                reason: reason.clone(),
            },
            SelectionVector::none(),
            ExecutionState::Pruned,
        ),
        PredicateProof::MayMatch { reason } => EncodedPredicateEvaluationReport::blocked(
            segment,
            predicate,
            PredicateProof::MayMatch {
                reason: reason.clone(),
            },
            EncodedPredicateEvaluationStatus::NeedsEncodedValues,
            EncodedEvalCapability::Encoded {
                reason: reason.clone(),
            },
            None,
        ),
        PredicateProof::Unknown { reason } => EncodedPredicateEvaluationReport::blocked(
            segment,
            predicate,
            PredicateProof::Unknown {
                reason: reason.clone(),
            },
            EncodedPredicateEvaluationStatus::NeedsEncodedValues,
            EncodedEvalCapability::Encoded {
                reason: "metadata proof is inconclusive; encoded values are required".to_string(),
            },
            None,
        ),
        PredicateProof::Unsupported { reason } => EncodedPredicateEvaluationReport::blocked(
            segment,
            predicate,
            PredicateProof::Unsupported {
                reason: reason.clone(),
            },
            EncodedPredicateEvaluationStatus::Unsupported,
            EncodedEvalCapability::Unsupported {
                reason: reason.clone(),
            },
            Some(Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "encoded_predicate_evaluation",
                reason.clone(),
                Some("Use a supported native predicate expression.".to_string()),
            )),
        ),
    }
}

/// Evaluates a predicate against an already available encoded-value batch.
///
/// This is the first native encoded-value predicate kernel boundary: it
/// evaluates constant, dictionary-coded, and run-length encoded batches without
/// decoding into rows, converting to Arrow, materializing data, touching object
/// stores, writing outputs, spilling, or invoking a fallback engine.
#[must_use]
pub fn evaluate_predicate_on_encoded_values(
    predicate: &PredicateExpr,
    segment: &EncodedSegment,
    values: &EncodedValueBatch,
) -> EncodedPredicateEvaluationReport {
    if let Some(column) = predicate.column()
        && column != &segment.column
    {
        let reason = format!(
            "predicate column {} does not match encoded segment column {}",
            column.as_str(),
            segment.column.as_str()
        );
        return encoded_value_predicate_blocked(segment, predicate, reason);
    }

    let Some(row_count) = values.row_count() else {
        return encoded_value_predicate_blocked(
            segment,
            predicate,
            "encoded value row count overflow".to_string(),
        );
    };
    if let Some(expected) = segment.stats.row_count
        && expected != row_count
    {
        return encoded_value_predicate_blocked(
            segment,
            predicate,
            format!(
                "encoded value row count {row_count} did not match segment row_count {expected}"
            ),
        );
    }

    let selection_vector = match encoded_value_selection_vector(predicate, values, row_count) {
        Ok(selection_vector) => selection_vector,
        Err(reason) => return encoded_value_predicate_blocked(segment, predicate, reason),
    };
    let selected_count = selection_vector.selected_count();
    let proof = if selected_count == row_count {
        PredicateProof::AlwaysTrue {
            reason: format!("{} encoded values proved all rows selected", values.label()),
        }
    } else if selected_count == 0 {
        PredicateProof::AlwaysFalse {
            reason: format!("{} encoded values proved no rows selected", values.label()),
        }
    } else {
        PredicateProof::MayMatch {
            reason: format!(
                "{} encoded values emitted sparse selection vector with {selected_count}/{row_count} rows",
                values.label()
            ),
        }
    };

    EncodedPredicateEvaluationReport::selected(
        segment,
        predicate,
        proof,
        selection_vector,
        ExecutionState::EncodedEvaluation,
    )
}

fn encoded_value_predicate_blocked(
    segment: &EncodedSegment,
    predicate: &PredicateExpr,
    reason: String,
) -> EncodedPredicateEvaluationReport {
    EncodedPredicateEvaluationReport::blocked(
        segment,
        predicate,
        PredicateProof::Unsupported {
            reason: reason.clone(),
        },
        EncodedPredicateEvaluationStatus::Unsupported,
        EncodedEvalCapability::Unsupported {
            reason: reason.clone(),
        },
        Some(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            "encoded_value_predicate_evaluation",
            reason,
            Some(
                "Use a supported encoded value batch, matching segment metadata, and matching predicate column."
                    .to_string(),
            ),
        )),
    )
}

fn encoded_value_selection_vector(
    predicate: &PredicateExpr,
    values: &EncodedValueBatch,
    row_count: u64,
) -> std::result::Result<SelectionVector, String> {
    match values {
        EncodedValueBatch::Constant { value, .. } => {
            if predicate_matches_encoded_value(predicate, value.as_ref())? {
                Ok(SelectionVector::all(row_count))
            } else {
                Ok(SelectionVector::none())
            }
        }
        EncodedValueBatch::Dictionary { dictionary, codes } => {
            dictionary_selection_vector(predicate, dictionary, codes, row_count)
        }
        EncodedValueBatch::RunLength { runs } => {
            let mut indices = Vec::new();
            let mut row_index = 0_u64;
            for run in runs {
                let selected = predicate_matches_encoded_value(predicate, run.value.as_ref())?;
                if selected {
                    for offset in 0..run.len {
                        indices.push(
                            row_index.checked_add(offset).ok_or_else(|| {
                                "run-length selected row index overflow".to_string()
                            })?,
                        );
                    }
                }
                row_index = row_index
                    .checked_add(run.len)
                    .ok_or_else(|| "run-length row count overflow".to_string())?;
            }
            Ok(selection_vector_from_indices(indices, row_count))
        }
        EncodedValueBatch::BitPackedUnsigned { values, .. } => {
            bitpacked_unsigned_selection_vector(predicate, values, row_count)
        }
        EncodedValueBatch::ArithmeticSequence {
            base, multiplier, ..
        } => encoded_sequence_selection_vector(predicate, base, multiplier, row_count),
    }
}

fn dictionary_selection_vector(
    predicate: &PredicateExpr,
    dictionary: &[Option<StatValue>],
    codes: &[Option<u32>],
    row_count: u64,
) -> std::result::Result<SelectionVector, String> {
    let code_stats = dictionary_code_stats(codes, dictionary.len())?;
    match predicate {
        PredicateExpr::AlwaysTrue => return Ok(SelectionVector::all(row_count)),
        PredicateExpr::AlwaysFalse => return Ok(SelectionVector::none()),
        PredicateExpr::IsNull { .. } => {
            return Ok(selection_vector_from_indices(
                code_nullity_indices(codes, true)?,
                row_count,
            ));
        }
        PredicateExpr::IsNotNull { .. } => {
            if code_stats.null_count == 0 {
                return Ok(SelectionVector::all(row_count));
            }
            return Ok(selection_vector_from_indices(
                code_nullity_indices(codes, false)?,
                row_count,
            ));
        }
        PredicateExpr::Compare { .. } => {}
    }
    let dictionary_matches = dictionary
        .iter()
        .map(|value| predicate_matches_encoded_value(predicate, value.as_ref()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if code_stats.null_count == 0 && dictionary_matches.iter().all(|selected| *selected) {
        return Ok(SelectionVector::all(row_count));
    }
    if dictionary_matches.iter().all(|selected| !*selected) {
        return Ok(SelectionVector::none());
    }
    let mut indices = Vec::new();
    for (row_index, code) in codes.iter().enumerate() {
        let selected = match code {
            Some(code) => {
                let code = usize::try_from(*code)
                    .map_err(|_| format!("dictionary code {code} does not fit usize"))?;
                *dictionary_matches
                    .get(code)
                    .ok_or_else(|| format!("dictionary code {code} is outside dictionary values"))?
            }
            None => predicate_matches_encoded_value(predicate, None)?,
        };
        if selected {
            indices.push(
                u64::try_from(row_index)
                    .map_err(|_| "dictionary row index does not fit u64".to_string())?,
            );
        }
    }
    Ok(selection_vector_from_indices(indices, row_count))
}

fn bitpacked_unsigned_selection_vector(
    predicate: &PredicateExpr,
    values: &[u64],
    row_count: u64,
) -> std::result::Result<SelectionVector, String> {
    match predicate {
        PredicateExpr::AlwaysTrue | PredicateExpr::IsNotNull { .. } => {
            return Ok(SelectionVector::all(row_count));
        }
        PredicateExpr::AlwaysFalse | PredicateExpr::IsNull { .. } => {
            return Ok(SelectionVector::none());
        }
        PredicateExpr::Compare { .. } => {}
    }
    let mut indices = Vec::with_capacity(values.len().min(4_096));
    if let Some((op, rhs)) = u64_comparison_predicate(predicate) {
        for (row_index, value) in values.iter().enumerate() {
            if compare_u64(*value, op, rhs) {
                indices.push(
                    u64::try_from(row_index)
                        .map_err(|_| "bit-packed row index does not fit u64".to_string())?,
                );
            }
        }
    } else {
        for (row_index, value) in values.iter().enumerate() {
            if predicate_matches_encoded_value(predicate, Some(&StatValue::UInt64(*value)))? {
                indices.push(
                    u64::try_from(row_index)
                        .map_err(|_| "bit-packed row index does not fit u64".to_string())?,
                );
            }
        }
    }
    Ok(selection_vector_from_indices(indices, row_count))
}

struct DictionaryCodeStats {
    null_count: usize,
}

fn dictionary_code_stats(
    codes: &[Option<u32>],
    dictionary_len: usize,
) -> std::result::Result<DictionaryCodeStats, String> {
    let mut null_count = 0_usize;
    for code in codes {
        match code {
            Some(code) => {
                let code = usize::try_from(*code)
                    .map_err(|_| format!("dictionary code {code} does not fit usize"))?;
                if code >= dictionary_len {
                    return Err(format!(
                        "dictionary code {code} is outside dictionary values"
                    ));
                }
            }
            None => {
                null_count = null_count.saturating_add(1);
            }
        }
    }
    Ok(DictionaryCodeStats { null_count })
}

fn code_nullity_indices(
    codes: &[Option<u32>],
    select_null: bool,
) -> std::result::Result<Vec<u64>, String> {
    let mut indices = Vec::new();
    for (row_index, code) in codes.iter().enumerate() {
        if code.is_none() == select_null {
            indices.push(
                u64::try_from(row_index)
                    .map_err(|_| "dictionary row index does not fit u64".to_string())?,
            );
        }
    }
    Ok(indices)
}

fn u64_comparison_predicate(predicate: &PredicateExpr) -> Option<(ComparisonOp, u64)> {
    let PredicateExpr::Compare {
        op,
        value: StatValue::UInt64(rhs),
        ..
    } = predicate
    else {
        return None;
    };
    Some((*op, *rhs))
}

fn compare_u64(left: u64, op: ComparisonOp, right: u64) -> bool {
    match op {
        ComparisonOp::Eq => left == right,
        ComparisonOp::NotEq => left != right,
        ComparisonOp::Lt => left < right,
        ComparisonOp::LtEq => left <= right,
        ComparisonOp::Gt => left > right,
        ComparisonOp::GtEq => left >= right,
    }
}

fn selection_vector_from_indices(indices: Vec<u64>, row_count: u64) -> SelectionVector {
    if indices.is_empty() {
        SelectionVector::none()
    } else if u64::try_from(indices.len()).ok() == Some(row_count) {
        SelectionVector::all(row_count)
    } else {
        SelectionVector::from_indices(indices)
    }
}

fn encoded_sequence_selection_vector(
    predicate: &PredicateExpr,
    base: &StatValue,
    multiplier: &StatValue,
    row_count: u64,
) -> std::result::Result<SelectionVector, String> {
    match predicate {
        PredicateExpr::AlwaysFalse | PredicateExpr::IsNull { .. } => {
            return Ok(SelectionVector::none());
        }
        PredicateExpr::AlwaysTrue | PredicateExpr::IsNotNull { .. } => {
            return Ok(SelectionVector::all(row_count));
        }
        PredicateExpr::Compare { .. } => {}
    }

    let mut indices = Vec::new();
    for row_index in 0..row_count {
        let value = sequence_value_at(base, multiplier, row_index)?;
        if predicate_matches_encoded_value(predicate, Some(&value))? {
            indices.push(row_index);
        }
    }
    Ok(selection_vector_from_indices(indices, row_count))
}

fn sequence_value_at(
    base: &StatValue,
    multiplier: &StatValue,
    row_index: u64,
) -> std::result::Result<StatValue, String> {
    match (base, multiplier) {
        (StatValue::UInt64(base), StatValue::UInt64(multiplier)) => {
            let delta = multiplier
                .checked_mul(row_index)
                .ok_or_else(|| "unsigned sequence multiplier overflow".to_string())?;
            base.checked_add(delta)
                .map(StatValue::UInt64)
                .ok_or_else(|| "unsigned sequence value overflow".to_string())
        }
        (StatValue::Int64(base), StatValue::Int64(multiplier)) => {
            let index = i64::try_from(row_index)
                .map_err(|_| "sequence row index does not fit i64".to_string())?;
            let delta = multiplier
                .checked_mul(index)
                .ok_or_else(|| "signed sequence multiplier overflow".to_string())?;
            base.checked_add(delta)
                .map(StatValue::Int64)
                .ok_or_else(|| "signed sequence value overflow".to_string())
        }
        _ => Err(format!(
            "unsupported arithmetic sequence base dtype {} and multiplier dtype {}",
            base.dtype().as_str(),
            multiplier.dtype().as_str()
        )),
    }
}

fn predicate_matches_encoded_value(
    predicate: &PredicateExpr,
    value: Option<&StatValue>,
) -> std::result::Result<bool, String> {
    match predicate {
        PredicateExpr::AlwaysTrue => Ok(true),
        PredicateExpr::AlwaysFalse => Ok(false),
        PredicateExpr::IsNull { .. } => Ok(value.is_none()),
        PredicateExpr::IsNotNull { .. } => Ok(value.is_some()),
        PredicateExpr::Compare { op, value: rhs, .. } => {
            let Some(lhs) = value else {
                return Ok(false);
            };
            let Some(ordering) = cmp_stat_values(lhs, rhs) else {
                return Err(format!(
                    "cannot compare encoded value dtype {} with predicate dtype {}",
                    lhs.dtype().as_str(),
                    rhs.dtype().as_str()
                ));
            };
            Ok(match op {
                ComparisonOp::Eq => ordering == 0,
                ComparisonOp::NotEq => ordering != 0,
                ComparisonOp::Lt => ordering < 0,
                ComparisonOp::LtEq => ordering <= 0,
                ComparisonOp::Gt => ordering > 0,
                ComparisonOp::GtEq => ordering >= 0,
            })
        }
    }
}

fn prove_comparison_from_stats(
    op: ComparisonOp,
    value: &StatValue,
    stats: &SegmentStats,
) -> PredicateProof {
    if stats.row_count == Some(0) {
        return PredicateProof::AlwaysFalse {
            reason: "segment row_count == 0".to_string(),
        };
    }
    let (Some(min), Some(max)) = (&stats.min_value, &stats.max_value) else {
        return PredicateProof::Unknown {
            reason: "min/max statistics unavailable".to_string(),
        };
    };
    let max_ord = cmp_stat_values(max, value);
    let min_ord = cmp_stat_values(min, value);
    match op {
        ComparisonOp::Gt if matches!(max_ord, Some(v) if v <= 0) => PredicateProof::AlwaysFalse {
            reason: "max <= value".to_string(),
        },
        ComparisonOp::GtEq if matches!(max_ord, Some(v) if v < 0) => PredicateProof::AlwaysFalse {
            reason: "max < value".to_string(),
        },
        ComparisonOp::Lt if matches!(min_ord, Some(v) if v >= 0) => PredicateProof::AlwaysFalse {
            reason: "min >= value".to_string(),
        },
        ComparisonOp::LtEq if matches!(min_ord, Some(v) if v > 0) => PredicateProof::AlwaysFalse {
            reason: "min > value".to_string(),
        },
        ComparisonOp::Eq => {
            if let (Some(c1), Some(c2)) = (cmp_stat_values(value, min), cmp_stat_values(value, max))
            {
                if c1 < 0 || c2 > 0 {
                    return PredicateProof::AlwaysFalse {
                        reason: "value outside min/max".to_string(),
                    };
                }
                if c1 == 0 && c2 == 0 && stats.null_count == Some(0) {
                    return PredicateProof::AlwaysTrue {
                        reason: "value equals constant non-null segment".to_string(),
                    };
                }
            }
            PredicateProof::MayMatch {
                reason: "min/max cannot exclude eq".to_string(),
            }
        }
        ComparisonOp::NotEq => {
            if matches!(
                (cmp_stat_values(min, value), cmp_stat_values(max, value)),
                (Some(0), Some(0))
            ) {
                return PredicateProof::AlwaysFalse {
                    reason: "constant segment equals not-eq value".to_string(),
                };
            }
            PredicateProof::MayMatch {
                reason: "conservative not-eq proof".to_string(),
            }
        }
        _ => PredicateProof::MayMatch {
            reason: "min/max cannot exclude".to_string(),
        },
    }
}

fn cmp_stat_values(a: &StatValue, b: &StatValue) -> Option<i8> {
    match (a, b) {
        (StatValue::Int64(x), StatValue::Int64(y)) => Some(match x.cmp(y) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }),
        (StatValue::UInt64(x), StatValue::UInt64(y)) => Some(match x.cmp(y) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }),
        (StatValue::Float64(x), StatValue::Float64(y)) => {
            x.partial_cmp(y).map(|ordering| match ordering {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Equal => 0,
                std::cmp::Ordering::Greater => 1,
            })
        }
        (StatValue::Utf8(x), StatValue::Utf8(y)) => Some(match x.cmp(y) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }),
        (StatValue::Boolean(x), StatValue::Boolean(y)) => Some(match x.cmp(y) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        }),
        _ => None,
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

    /// Intersects two segment-local selection vectors without decoding rows.
    ///
    /// `All` vectors carry the segment row count, so mismatched `All` row
    /// counts or sparse indices outside an `All` boundary are rejected instead
    /// of silently producing misleading evidence.
    ///
    /// # Errors
    /// Returns an error when row-count boundaries are inconsistent or sparse
    /// indices exceed a known `All` vector row count.
    pub fn try_intersect(&self, other: &Self) -> std::result::Result<Self, String> {
        match (self, other) {
            (Self::None, _) | (_, Self::None) => Ok(Self::none()),
            (Self::All { row_count: left }, Self::All { row_count: right }) => {
                if left == right {
                    Ok(Self::all(*left))
                } else {
                    Err(format!(
                        "cannot intersect all-row selection vectors with different row counts: left={left} right={right}"
                    ))
                }
            }
            (Self::All { row_count }, Self::Indices(indices))
            | (Self::Indices(indices), Self::All { row_count }) => {
                bounded_sparse_selection(indices, *row_count)
            }
            (Self::Indices(left), Self::Indices(right)) => {
                let right = right
                    .iter()
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>();
                let mut seen = std::collections::BTreeSet::new();
                let mut indices = left
                    .iter()
                    .copied()
                    .filter(|index| right.contains(index) && seen.insert(*index))
                    .collect::<Vec<_>>();
                indices.sort_unstable();
                Ok(selection_vector_from_indices(indices, u64::MAX))
            }
        }
    }
}

/// Intersects one or more segment-local selection vectors.
///
/// An empty input is rejected because there is no neutral row-count-safe value
/// unless the caller already knows the segment cardinality.
///
/// # Errors
/// Returns an error when the input is empty, row-count boundaries are
/// inconsistent, or sparse indices exceed a known row-count boundary.
pub fn intersect_selection_vectors<'a>(
    vectors: impl IntoIterator<Item = &'a SelectionVector>,
) -> std::result::Result<SelectionVector, String> {
    let mut vectors = vectors.into_iter();
    let Some(first) = vectors.next() else {
        return Err("cannot intersect an empty selection-vector set".to_string());
    };
    vectors.try_fold(first.clone(), |acc, vector| acc.try_intersect(vector))
}

fn bounded_sparse_selection(
    indices: &[u64],
    row_count: u64,
) -> std::result::Result<SelectionVector, String> {
    let mut seen = std::collections::BTreeSet::new();
    let mut bounded = Vec::new();
    for index in indices {
        if *index >= row_count {
            return Err(format!(
                "selection vector index {index} is outside row count {row_count}"
            ));
        }
        if seen.insert(*index) {
            bounded.push(*index);
        }
    }
    bounded.sort_unstable();
    Ok(selection_vector_from_indices(bounded, row_count))
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
    fn selection_vector_intersection_handles_sparse_all_and_none() {
        let left = SelectionVector::from_indices(vec![1, 3, 5, 7]);
        let right = SelectionVector::from_indices(vec![0, 3, 5, 8]);

        assert_eq!(
            left.try_intersect(&right).unwrap(),
            SelectionVector::from_indices(vec![3, 5])
        );
        assert_eq!(
            intersect_selection_vectors([
                &SelectionVector::all(8),
                &SelectionVector::from_indices(vec![1, 3, 7]),
                &SelectionVector::from_indices(vec![3, 4, 7]),
            ])
            .unwrap(),
            SelectionVector::from_indices(vec![3, 7])
        );
        assert_eq!(
            left.try_intersect(&SelectionVector::none()).unwrap(),
            SelectionVector::none()
        );
    }

    #[test]
    fn selection_vector_intersection_rejects_unsafe_boundaries() {
        let row_count_error = SelectionVector::all(3)
            .try_intersect(&SelectionVector::all(4))
            .unwrap_err();
        assert!(row_count_error.contains("different row counts"));

        let sparse_error = SelectionVector::all(3)
            .try_intersect(&SelectionVector::from_indices(vec![1, 3]))
            .unwrap_err();
        assert!(sparse_error.contains("outside row count"));

        let empty_error =
            intersect_selection_vectors(std::iter::empty::<&SelectionVector>()).unwrap_err();
        assert!(empty_error.contains("empty selection-vector set"));
    }

    fn segment_with_stats(column: &str, stats: SegmentStats) -> EncodedSegment {
        EncodedSegment::new(
            SegmentId::new("segment-1").unwrap(),
            ColumnRef::new(column).unwrap(),
            LogicalDType::Int64,
            Nullability::Nullable,
            SegmentLayout::new(
                EncodingKind::VortexNative("test".to_string()),
                LayoutKind::Flat,
            ),
            stats,
        )
    }

    #[test]
    fn encoded_predicate_evaluation_selects_all_for_metadata_true() {
        let mut stats = SegmentStats::with_row_count(3);
        stats.null_count = Some(0);
        let segment = segment_with_stats("x", stats);
        let report = evaluate_predicate_on_encoded_segment(
            &PredicateExpr::IsNotNull {
                column: ColumnRef::new("x").unwrap(),
            },
            &segment,
        );

        assert_eq!(report.status, EncodedPredicateEvaluationStatus::SelectedAll);
        assert_eq!(report.execution_state, ExecutionState::MetadataOnly);
        assert_eq!(report.selection_vector, Some(SelectionVector::all(3)));
        assert_eq!(report.selected_count, Some(3));
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn encoded_predicate_evaluation_selects_none_for_metadata_false() {
        let mut stats = SegmentStats::with_row_count(7);
        stats.null_count = Some(0);
        let segment = segment_with_stats("x", stats);
        let report = evaluate_predicate_on_encoded_segment(
            &PredicateExpr::IsNull {
                column: ColumnRef::new("x").unwrap(),
            },
            &segment,
        );

        assert_eq!(
            report.status,
            EncodedPredicateEvaluationStatus::SelectedNone
        );
        assert_eq!(report.execution_state, ExecutionState::Pruned);
        assert_eq!(report.selection_vector, Some(SelectionVector::none()));
        assert_eq!(report.selected_count, Some(0));
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn encoded_predicate_evaluation_defers_may_match_to_encoded_values() {
        let mut stats = SegmentStats::with_row_count(8);
        stats.min_value = Some(StatValue::Int64(1));
        stats.max_value = Some(StatValue::Int64(9));
        let segment = segment_with_stats("x", stats);
        let report = evaluate_predicate_on_encoded_segment(
            &PredicateExpr::Compare {
                column: ColumnRef::new("x").unwrap(),
                op: ComparisonOp::Eq,
                value: StatValue::Int64(5),
            },
            &segment,
        );

        assert_eq!(
            report.status,
            EncodedPredicateEvaluationStatus::NeedsEncodedValues
        );
        assert_eq!(report.execution_state, ExecutionState::EncodedEvaluation);
        assert!(matches!(
            report.capability,
            EncodedEvalCapability::Encoded { .. }
        ));
        assert_eq!(report.selection_vector, None);
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn encoded_value_dictionary_predicate_emits_sparse_selection_vector() {
        let segment = segment_with_stats("x", SegmentStats::with_row_count(5));
        let values = EncodedValueBatch::Dictionary {
            dictionary: vec![Some(StatValue::Int64(1)), Some(StatValue::Int64(5)), None],
            codes: vec![Some(0), Some(1), None, Some(1), Some(0)],
        };
        let report = evaluate_predicate_on_encoded_values(
            &PredicateExpr::Compare {
                column: ColumnRef::new("x").unwrap(),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(5),
            },
            &segment,
            &values,
        );

        assert_eq!(
            report.status,
            EncodedPredicateEvaluationStatus::SelectedIndices
        );
        assert_eq!(report.execution_state, ExecutionState::EncodedEvaluation);
        assert!(matches!(
            report.capability,
            EncodedEvalCapability::Encoded { .. }
        ));
        assert_eq!(
            report.selection_vector,
            Some(SelectionVector::from_indices(vec![1, 3]))
        );
        assert_eq!(report.selected_count, Some(2));
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn encoded_value_dictionary_nullity_fast_paths_select_all_or_none() {
        let segment = segment_with_stats("x", SegmentStats::with_row_count(4));
        let values = EncodedValueBatch::Dictionary {
            dictionary: vec![Some(StatValue::Int64(1)), Some(StatValue::Int64(5))],
            codes: vec![Some(0), Some(1), Some(0), Some(1)],
        };
        let is_not_null = evaluate_predicate_on_encoded_values(
            &PredicateExpr::IsNotNull {
                column: ColumnRef::new("x").unwrap(),
            },
            &segment,
            &values,
        );
        assert_eq!(is_not_null.selection_vector, Some(SelectionVector::all(4)));
        assert_eq!(is_not_null.selected_count, Some(4));

        let is_null = evaluate_predicate_on_encoded_values(
            &PredicateExpr::IsNull {
                column: ColumnRef::new("x").unwrap(),
            },
            &segment,
            &values,
        );
        assert_eq!(is_null.selection_vector, Some(SelectionVector::none()));
        assert_eq!(is_null.selected_count, Some(0));
        assert!(is_null.is_side_effect_free());
    }

    #[test]
    fn encoded_value_run_length_predicate_emits_sparse_selection_vector() {
        let segment = segment_with_stats("x", SegmentStats::with_row_count(6));
        let values = EncodedValueBatch::RunLength {
            runs: vec![
                EncodedValueRun::new(Some(StatValue::Int64(1)), 2),
                EncodedValueRun::new(Some(StatValue::Int64(5)), 3),
                EncodedValueRun::new(None, 1),
            ],
        };
        let report = evaluate_predicate_on_encoded_values(
            &PredicateExpr::Compare {
                column: ColumnRef::new("x").unwrap(),
                op: ComparisonOp::Gt,
                value: StatValue::Int64(2),
            },
            &segment,
            &values,
        );

        assert_eq!(
            report.status,
            EncodedPredicateEvaluationStatus::SelectedIndices
        );
        assert_eq!(
            report.selection_vector,
            Some(SelectionVector::from_indices(vec![2, 3, 4]))
        );
        assert_eq!(report.selected_count, Some(3));
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn encoded_value_bit_packed_predicate_emits_sparse_selection_vector() {
        let segment = segment_with_stats("x", SegmentStats::with_row_count(5));
        let values = EncodedValueBatch::BitPackedUnsigned {
            bit_width: 1,
            values: vec![0, 1, 0, 1, 1],
        };
        let report = evaluate_predicate_on_encoded_values(
            &PredicateExpr::Compare {
                column: ColumnRef::new("x").unwrap(),
                op: ComparisonOp::Eq,
                value: StatValue::UInt64(1),
            },
            &segment,
            &values,
        );

        assert_eq!(
            report.status,
            EncodedPredicateEvaluationStatus::SelectedIndices
        );
        assert_eq!(
            report.selection_vector,
            Some(SelectionVector::from_indices(vec![1, 3, 4]))
        );
        assert_eq!(report.selected_count, Some(3));
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn encoded_value_bit_packed_nullity_fast_paths_select_all_or_none() {
        let segment = segment_with_stats("x", SegmentStats::with_row_count(5));
        let values = EncodedValueBatch::BitPackedUnsigned {
            bit_width: 1,
            values: vec![0, 1, 0, 1, 1],
        };
        let is_not_null = evaluate_predicate_on_encoded_values(
            &PredicateExpr::IsNotNull {
                column: ColumnRef::new("x").unwrap(),
            },
            &segment,
            &values,
        );
        assert_eq!(is_not_null.selection_vector, Some(SelectionVector::all(5)));
        assert_eq!(is_not_null.selected_count, Some(5));

        let is_null = evaluate_predicate_on_encoded_values(
            &PredicateExpr::IsNull {
                column: ColumnRef::new("x").unwrap(),
            },
            &segment,
            &values,
        );
        assert_eq!(is_null.selection_vector, Some(SelectionVector::none()));
        assert_eq!(is_null.selected_count, Some(0));
        assert!(is_null.is_side_effect_free());
    }

    #[test]
    fn encoded_value_arithmetic_sequence_predicate_emits_sparse_selection_vector() {
        let segment = segment_with_stats("x", SegmentStats::with_row_count(6));
        let values = EncodedValueBatch::ArithmeticSequence {
            base: StatValue::UInt64(0),
            multiplier: StatValue::UInt64(17),
            row_count: 6,
        };
        let report = evaluate_predicate_on_encoded_values(
            &PredicateExpr::Compare {
                column: ColumnRef::new("x").unwrap(),
                op: ComparisonOp::GtEq,
                value: StatValue::UInt64(50),
            },
            &segment,
            &values,
        );

        assert_eq!(
            report.status,
            EncodedPredicateEvaluationStatus::SelectedIndices
        );
        assert_eq!(
            report.selection_vector,
            Some(SelectionVector::from_indices(vec![3, 4, 5]))
        );
        assert_eq!(report.selected_count, Some(3));
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn encoded_value_constant_null_is_null_selects_all() {
        let segment = segment_with_stats("x", SegmentStats::with_row_count(4));
        let values = EncodedValueBatch::Constant {
            value: None,
            row_count: 4,
        };
        let report = evaluate_predicate_on_encoded_values(
            &PredicateExpr::IsNull {
                column: ColumnRef::new("x").unwrap(),
            },
            &segment,
            &values,
        );

        assert_eq!(report.status, EncodedPredicateEvaluationStatus::SelectedAll);
        assert_eq!(report.execution_state, ExecutionState::EncodedEvaluation);
        assert_eq!(report.selection_vector, Some(SelectionVector::all(4)));
        assert_eq!(report.selected_count, Some(4));
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn encoded_value_type_mismatch_is_unsupported_without_fallback() {
        let segment = segment_with_stats("x", SegmentStats::with_row_count(1));
        let values = EncodedValueBatch::Constant {
            value: Some(StatValue::Utf8("a".to_string())),
            row_count: 1,
        };
        let report = evaluate_predicate_on_encoded_values(
            &PredicateExpr::Compare {
                column: ColumnRef::new("x").unwrap(),
                op: ComparisonOp::Eq,
                value: StatValue::Int64(1),
            },
            &segment,
            &values,
        );

        assert_eq!(report.status, EncodedPredicateEvaluationStatus::Unsupported);
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }

    #[test]
    fn encoded_value_row_count_mismatch_is_unsupported_without_fallback() {
        let segment = segment_with_stats("x", SegmentStats::with_row_count(3));
        let values = EncodedValueBatch::Constant {
            value: Some(StatValue::Int64(1)),
            row_count: 4,
        };
        let report =
            evaluate_predicate_on_encoded_values(&PredicateExpr::AlwaysTrue, &segment, &values);

        assert_eq!(report.status, EncodedPredicateEvaluationStatus::Unsupported);
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }

    #[test]
    fn encoded_predicate_evaluation_blocks_full_selection_without_row_count() {
        let mut stats = SegmentStats::unknown();
        stats.null_count = Some(0);
        let segment = segment_with_stats("x", stats);
        let report = evaluate_predicate_on_encoded_segment(
            &PredicateExpr::IsNotNull {
                column: ColumnRef::new("x").unwrap(),
            },
            &segment,
        );

        assert_eq!(
            report.status,
            EncodedPredicateEvaluationStatus::MissingSegmentMetadata
        );
        assert_eq!(report.execution_state, ExecutionState::PartialDecode);
        assert_eq!(report.selection_vector, None);
        assert!(report.is_side_effect_free());
        assert!(report.has_errors());
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }

    #[test]
    fn encoded_predicate_evaluation_rejects_wrong_segment_column() {
        let segment = segment_with_stats("x", SegmentStats::with_row_count(4));
        let report = evaluate_predicate_on_encoded_segment(
            &PredicateExpr::IsNull {
                column: ColumnRef::new("y").unwrap(),
            },
            &segment,
        );

        assert_eq!(report.status, EncodedPredicateEvaluationStatus::Unsupported);
        assert_eq!(report.execution_state, ExecutionState::Unsupported);
        assert_eq!(report.selection_vector, None);
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }

    #[test]
    fn encoded_predicate_evaluation_empty_segment_selects_none() {
        let segment = segment_with_stats("x", SegmentStats::with_row_count(0));
        let report = evaluate_predicate_on_encoded_segment(
            &PredicateExpr::Compare {
                column: ColumnRef::new("x").unwrap(),
                op: ComparisonOp::Gt,
                value: StatValue::Int64(5),
            },
            &segment,
        );

        assert_eq!(
            report.status,
            EncodedPredicateEvaluationStatus::SelectedNone
        );
        assert_eq!(report.selection_vector, Some(SelectionVector::none()));
        assert_eq!(report.selected_count, Some(0));
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn prove_predicate_from_stats_recognizes_constant_eq_true() {
        let mut stats = SegmentStats::with_row_count(2);
        stats.null_count = Some(0);
        stats.min_value = Some(StatValue::Int64(7));
        stats.max_value = Some(StatValue::Int64(7));

        let proof = prove_predicate_from_stats(
            &PredicateExpr::Compare {
                column: ColumnRef::new("x").unwrap(),
                op: ComparisonOp::Eq,
                value: StatValue::Int64(7),
            },
            &stats,
        );

        assert!(matches!(proof, PredicateProof::AlwaysTrue { .. }));
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
