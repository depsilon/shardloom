use crate::{EstimateConfidence, PlanNodeId};
use shardloom_core::{
    Diagnostic, DiagnosticCode, DiagnosticSeverity, EncodedSegment, Result, ShardLoomError,
};

/// Planning-only optimizer phases for the `ShardLoom` skeleton.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerPhase {
    FrontendNormalization,
    Logical,
    VortexPhysical,
    RuntimeAdaptive,
    PostExecutionDiagnostics,
    Unsupported,
}
impl OptimizerPhase {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FrontendNormalization => "frontend_normalization",
            Self::Logical => "logical",
            Self::VortexPhysical => "vortex_physical",
            Self::RuntimeAdaptive => "runtime_adaptive",
            Self::PostExecutionDiagnostics => "post_execution_diagnostics",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn canonical_label(&self) -> &'static str {
        self.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OptimizerRuleId(String);
impl OptimizerRuleId {
    /// Creates a validated optimizer rule identifier.
    ///
    /// # Errors
    /// Returns `ShardLoomError::InvalidOperation` for empty or whitespace-only values.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "optimizer rule id must not be empty".to_string(),
            ));
        }
        Ok(Self(value))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerRuleKind {
    PredicatePushdown,
    ProjectionPushdown,
    MetadataOnlyAnswer,
    SegmentPruning,
    EncodedKernelSelection,
    PartialDecodePlanning,
    LateMaterialization,
    RuntimeFilterPushdown,
    DynamicPruning,
    JoinStrategySelection,
    AggregateStrategySelection,
    SkewHandling,
    SinkDrivenPlanning,
    MemorySpillAwarePlanning,
    ObjectStorePlanning,
    Unsupported,
}
impl OptimizerRuleKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PredicatePushdown => "predicate_pushdown",
            Self::ProjectionPushdown => "projection_pushdown",
            Self::MetadataOnlyAnswer => "metadata_only_answer",
            Self::SegmentPruning => "segment_pruning",
            Self::EncodedKernelSelection => "encoded_kernel_selection",
            Self::PartialDecodePlanning => "partial_decode_planning",
            Self::LateMaterialization => "late_materialization",
            Self::RuntimeFilterPushdown => "runtime_filter_pushdown",
            Self::DynamicPruning => "dynamic_pruning",
            Self::JoinStrategySelection => "join_strategy_selection",
            Self::AggregateStrategySelection => "aggregate_strategy_selection",
            Self::SkewHandling => "skew_handling",
            Self::SinkDrivenPlanning => "sink_driven_planning",
            Self::MemorySpillAwarePlanning => "memory_spill_aware_planning",
            Self::ObjectStorePlanning => "object_store_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_vortex_native(&self) -> bool {
        matches!(
            self,
            Self::MetadataOnlyAnswer
                | Self::SegmentPruning
                | Self::EncodedKernelSelection
                | Self::PartialDecodePlanning
                | Self::LateMaterialization
        )
    }
    #[must_use]
    pub const fn is_runtime_adaptive(&self) -> bool {
        matches!(
            self,
            Self::RuntimeFilterPushdown
                | Self::DynamicPruning
                | Self::SkewHandling
                | Self::MemorySpillAwarePlanning
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerRuleStatus {
    Applied,
    NotApplicable,
    Deferred,
    Rejected,
    Unsupported,
}
impl OptimizerRuleStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::NotApplicable => "not_applicable",
            Self::Deferred => "deferred",
            Self::Rejected => "rejected",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Rejected | Self::Unsupported)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OptimizerRuleDecision {
    pub rule_id: OptimizerRuleId,
    pub phase: OptimizerPhase,
    pub kind: OptimizerRuleKind,
    pub status: OptimizerRuleStatus,
    pub reason: String,
    pub diagnostics: Vec<Diagnostic>,
}
impl OptimizerRuleDecision {
    #[must_use]
    pub fn applied(
        rule_id: OptimizerRuleId,
        phase: OptimizerPhase,
        kind: OptimizerRuleKind,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            rule_id,
            phase,
            kind,
            status: OptimizerRuleStatus::Applied,
            reason: reason.into(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn not_applicable(
        rule_id: OptimizerRuleId,
        phase: OptimizerPhase,
        kind: OptimizerRuleKind,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            rule_id,
            phase,
            kind,
            status: OptimizerRuleStatus::NotApplicable,
            reason: reason.into(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn unsupported(
        rule_id: OptimizerRuleId,
        phase: OptimizerPhase,
        kind: OptimizerRuleKind,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let reason = reason.into();
        Self {
            rule_id,
            phase,
            kind,
            status: OptimizerRuleStatus::Unsupported,
            reason: reason.clone(),
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                feature,
                reason,
                Some(
                    "Native optimizer rule execution is not implemented in this skeleton."
                        .to_string(),
                ),
            )],
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "rule={} phase={} kind={} status={} fallback_execution=disabled",
            self.rule_id.as_str(),
            self.phase.as_str(),
            self.kind.as_str(),
            self.status.as_str()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostMetric {
    Rows,
    EncodedBytes,
    DecodedBytes,
    MaterializedRows,
    ObjectStoreRequests,
    MemoryBytes,
    SpillBytes,
    ShuffleBytes,
    OutputBytes,
    EffectCalls,
    Unknown,
}
impl CostMetric {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Rows => "rows",
            Self::EncodedBytes => "encoded_bytes",
            Self::DecodedBytes => "decoded_bytes",
            Self::MaterializedRows => "materialized_rows",
            Self::ObjectStoreRequests => "object_store_requests",
            Self::MemoryBytes => "memory_bytes",
            Self::SpillBytes => "spill_bytes",
            Self::ShuffleBytes => "shuffle_bytes",
            Self::OutputBytes => "output_bytes",
            Self::EffectCalls => "effect_calls",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CostValue {
    Known(u64),
    Estimated(u64),
    Unknown,
}
impl CostValue {
    #[must_use]
    pub const fn known(value: u64) -> Self {
        Self::Known(value)
    }
    #[must_use]
    pub const fn estimated(value: u64) -> Self {
        Self::Estimated(value)
    }
    #[must_use]
    pub const fn unknown() -> Self {
        Self::Unknown
    }
    #[must_use]
    pub const fn is_known_or_estimated(&self) -> bool {
        !matches!(self, Self::Unknown)
    }
    #[must_use]
    pub const fn unwrap_or(&self, fallback: u64) -> u64 {
        match self {
            Self::Known(v) | Self::Estimated(v) => *v,
            Self::Unknown => fallback,
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        match self {
            Self::Known(v) => format!("known:{v}"),
            Self::Estimated(v) => format!("estimated:{v}"),
            Self::Unknown => "unknown".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CostEstimate {
    pub metric: CostMetric,
    pub value: CostValue,
    pub confidence: EstimateConfidence,
    pub reason: Option<String>,
}
impl CostEstimate {
    #[must_use]
    pub fn new(metric: CostMetric, value: CostValue, confidence: EstimateConfidence) -> Self {
        Self {
            metric,
            value,
            confidence,
            reason: None,
        }
    }
    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "{}={} confidence={}",
            self.metric.as_str(),
            self.value.summary(),
            self.confidence.as_str()
        )
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq)]
pub struct CostModelInput {
    pub row_count: Option<u64>,
    pub encoded_bytes: Option<u64>,
    pub estimated_decoded_bytes: Option<u64>,
    pub selected_column_count: Option<usize>,
    pub total_column_count: Option<usize>,
    pub segment_count: Option<usize>,
    pub has_statistics: bool,
    pub has_byte_ranges: bool,
    pub streaming_required: bool,
    pub spill_supported: bool,
    pub native_vortex_output: bool,
}
impl CostModelInput {
    #[must_use]
    pub fn unknown() -> Self {
        Self {
            row_count: None,
            encoded_bytes: None,
            estimated_decoded_bytes: None,
            selected_column_count: None,
            total_column_count: None,
            segment_count: None,
            has_statistics: false,
            has_byte_ranges: false,
            streaming_required: false,
            spill_supported: false,
            native_vortex_output: true,
        }
    }
    #[must_use]
    pub fn from_segment(segment: &EncodedSegment) -> Self {
        Self {
            row_count: segment.stats.row_count,
            encoded_bytes: segment.layout.physical_size_bytes,
            estimated_decoded_bytes: None,
            selected_column_count: Some(1),
            total_column_count: Some(1),
            segment_count: Some(1),
            has_statistics: segment.can_use_metadata(),
            has_byte_ranges: segment.has_byte_ranges(),
            streaming_required: false,
            spill_supported: false,
            native_vortex_output: true,
        }
    }
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn projection_ratio(&self) -> Option<f64> {
        match (self.selected_column_count, self.total_column_count) {
            (Some(s), Some(t)) if t > 0 => Some(s as f64 / t as f64),
            _ => None,
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "rows={:?} encoded_bytes={:?} has_statistics={} has_byte_ranges={} native_vortex_output={}",
            self.row_count,
            self.encoded_bytes,
            self.has_statistics,
            self.has_byte_ranges,
            self.native_vortex_output
        )
    }
}

// omitted: define remaining types similarly
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeFilterKind {
    BloomLike,
    DictionaryId,
    Range,
    Constant,
    NullAware,
    SemiJoinReduction,
    DynamicPartition,
    Unsupported,
}
impl RuntimeFilterKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::BloomLike => "bloom_like",
            Self::DictionaryId => "dictionary_id",
            Self::Range => "range",
            Self::Constant => "constant",
            Self::NullAware => "null_aware",
            Self::SemiJoinReduction => "semi_join_reduction",
            Self::DynamicPartition => "dynamic_partition",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_conservative_candidate(&self) -> bool {
        !matches!(self, Self::Unsupported)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeFilterStatus {
    Planned,
    Generated,
    Applied,
    Rejected,
    Unsupported,
}
impl RuntimeFilterStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Generated => "generated",
            Self::Applied => "applied",
            Self::Rejected => "rejected",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Rejected | Self::Unsupported)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeFilter {
    pub kind: RuntimeFilterKind,
    pub status: RuntimeFilterStatus,
    pub source_node: Option<PlanNodeId>,
    pub target_node: Option<PlanNodeId>,
    pub conservative: bool,
    pub reason: String,
    pub diagnostics: Vec<Diagnostic>,
}
impl RuntimeFilter {
    #[must_use]
    pub fn planned(kind: RuntimeFilterKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            status: RuntimeFilterStatus::Planned,
            source_node: None,
            target_node: None,
            conservative: kind.is_conservative_candidate(),
            reason: reason.into(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn unsupported(
        kind: RuntimeFilterKind,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let reason = reason.into();
        Self {
            kind,
            status: RuntimeFilterStatus::Unsupported,
            source_node: None,
            target_node: None,
            conservative: false,
            reason: reason.clone(),
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                feature,
                reason,
                Some("Runtime filters are planning metadata only in this skeleton.".to_string()),
            )],
        }
    }
    #[must_use]
    pub fn with_source_node(mut self, source_node: PlanNodeId) -> Self {
        self.source_node = Some(source_node);
        self
    }
    #[must_use]
    pub fn with_target_node(mut self, target_node: PlanNodeId) -> Self {
        self.target_node = Some(target_node);
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "runtime_filter kind={} status={} conservative={} (planning metadata only)",
            self.kind.as_str(),
            self.status.as_str(),
            self.conservative
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DynamicPruningDecision {
    NotNeeded { reason: String },
    Candidate { reason: String },
    Applied { reason: String },
    Rejected { reason: String },
    Unsupported { reason: String },
}
impl DynamicPruningDecision {
    #[must_use]
    pub fn reason(&self) -> &str {
        match self {
            Self::NotNeeded { reason }
            | Self::Candidate { reason }
            | Self::Applied { reason }
            | Self::Rejected { reason }
            | Self::Unsupported { reason } => reason,
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Rejected { .. } | Self::Unsupported { .. })
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "dynamic_pruning={} reason={}",
            match self {
                Self::NotNeeded { .. } => "not_needed",
                Self::Candidate { .. } => "candidate",
                Self::Applied { .. } => "applied",
                Self::Rejected { .. } => "rejected",
                Self::Unsupported { .. } => "unsupported",
            },
            self.reason()
        )
    }
}

// ... keep concise
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveTriggerKind {
    RuntimeRowCount,
    RuntimeBytesRead,
    MemoryPressure,
    SpillPressure,
    SkewDetected,
    SinkBackpressure,
    RuntimeFilterAvailable,
    ObjectStoreLatency,
    Unknown,
}
impl AdaptiveTriggerKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::RuntimeRowCount => "runtime_row_count",
            Self::RuntimeBytesRead => "runtime_bytes_read",
            Self::MemoryPressure => "memory_pressure",
            Self::SpillPressure => "spill_pressure",
            Self::SkewDetected => "skew_detected",
            Self::SinkBackpressure => "sink_backpressure",
            Self::RuntimeFilterAvailable => "runtime_filter_available",
            Self::ObjectStoreLatency => "object_store_latency",
            Self::Unknown => "unknown",
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AdaptiveTrigger {
    pub kind: AdaptiveTriggerKind,
    pub reason: String,
}
impl AdaptiveTrigger {
    #[must_use]
    pub fn new(kind: AdaptiveTriggerKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            reason: reason.into(),
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!("trigger={} reason={}", self.kind.as_str(), self.reason)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdaptiveDecisionKind {
    ReduceParallelism,
    IncreaseParallelism,
    CoalesceTasks,
    SplitTasks,
    ApplyRuntimeFilter,
    ReplanJoin,
    ReplanAggregation,
    TriggerSpill,
    ChangeMaterialization,
    NoChange,
    Unsupported,
}
impl AdaptiveDecisionKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReduceParallelism => "reduce_parallelism",
            Self::IncreaseParallelism => "increase_parallelism",
            Self::CoalesceTasks => "coalesce_tasks",
            Self::SplitTasks => "split_tasks",
            Self::ApplyRuntimeFilter => "apply_runtime_filter",
            Self::ReplanJoin => "replan_join",
            Self::ReplanAggregation => "replan_aggregation",
            Self::TriggerSpill => "trigger_spill",
            Self::ChangeMaterialization => "change_materialization",
            Self::NoChange => "no_change",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn changes_plan(&self) -> bool {
        !matches!(self, Self::NoChange | Self::Unsupported)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct AdaptiveExecutionDecision {
    pub trigger: AdaptiveTrigger,
    pub kind: AdaptiveDecisionKind,
    pub reason: String,
    pub diagnostics: Vec<Diagnostic>,
}
impl AdaptiveExecutionDecision {
    #[must_use]
    pub fn new(
        trigger: AdaptiveTrigger,
        kind: AdaptiveDecisionKind,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            trigger,
            kind,
            reason: reason.into(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn unsupported(
        trigger: AdaptiveTrigger,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let reason = reason.into();
        Self {
            trigger,
            kind: AdaptiveDecisionKind::Unsupported,
            reason: reason.clone(),
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                feature,
                reason,
                Some(
                    "Adaptive execution behavior is not implemented in this skeleton.".to_string(),
                ),
            )],
        }
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub const fn changes_plan(&self) -> bool {
        self.kind.changes_plan()
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        matches!(self.kind, AdaptiveDecisionKind::Unsupported)
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "adaptive kind={} trigger={} fallback_execution=disabled",
            self.kind.as_str(),
            self.trigger.kind.as_str()
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinStrategy {
    BroadcastSmallSide,
    HashJoin,
    SortMergeJoin,
    RuntimeFilteredJoin,
    SemiJoinReduction,
    RangeAwareJoin,
    DictionaryAwareJoin,
    Unsupported,
}
impl JoinStrategy {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::BroadcastSmallSide => "broadcast_small_side",
            Self::HashJoin => "hash_join",
            Self::SortMergeJoin => "sort_merge_join",
            Self::RuntimeFilteredJoin => "runtime_filtered_join",
            Self::SemiJoinReduction => "semi_join_reduction",
            Self::RangeAwareJoin => "range_aware_join",
            Self::DictionaryAwareJoin => "dictionary_aware_join",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn requires_shuffle(&self) -> bool {
        matches!(self, Self::HashJoin | Self::SortMergeJoin)
    }
    #[must_use]
    pub const fn is_encoded_friendly(&self) -> bool {
        matches!(
            self,
            Self::RuntimeFilteredJoin
                | Self::SemiJoinReduction
                | Self::RangeAwareJoin
                | Self::DictionaryAwareJoin
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateStrategy {
    MetadataOnly,
    SegmentLocalPartial,
    EncodedAggregate,
    HashAggregate,
    SortAggregate,
    SpillableAggregate,
    StreamingAggregate,
    Unsupported,
}
impl AggregateStrategy {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::SegmentLocalPartial => "segment_local_partial",
            Self::EncodedAggregate => "encoded_aggregate",
            Self::HashAggregate => "hash_aggregate",
            Self::SortAggregate => "sort_aggregate",
            Self::SpillableAggregate => "spillable_aggregate",
            Self::StreamingAggregate => "streaming_aggregate",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_vortex_native(&self) -> bool {
        matches!(
            self,
            Self::MetadataOnly | Self::SegmentLocalPartial | Self::EncodedAggregate
        )
    }
    #[must_use]
    pub const fn may_require_spill(&self) -> bool {
        matches!(
            self,
            Self::HashAggregate | Self::SortAggregate | Self::SpillableAggregate
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkewSignalKind {
    SegmentRowCount,
    SegmentByteSize,
    KeyFrequency,
    DictionarySkew,
    RuntimePartitionSize,
    TaskDuration,
    SpillPressure,
    ObjectStoreLatency,
    Unknown,
}
impl SkewSignalKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::SegmentRowCount => "segment_row_count",
            Self::SegmentByteSize => "segment_byte_size",
            Self::KeyFrequency => "key_frequency",
            Self::DictionarySkew => "dictionary_skew",
            Self::RuntimePartitionSize => "runtime_partition_size",
            Self::TaskDuration => "task_duration",
            Self::SpillPressure => "spill_pressure",
            Self::ObjectStoreLatency => "object_store_latency",
            Self::Unknown => "unknown",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkewSeverity {
    None,
    Low,
    Medium,
    High,
    Critical,
    Unknown,
}
impl SkewSeverity {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Critical => "critical",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn requires_action(&self) -> bool {
        matches!(self, Self::High | Self::Critical)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct SkewSignal {
    pub kind: SkewSignalKind,
    pub severity: SkewSeverity,
    pub reason: String,
}
impl SkewSignal {
    #[must_use]
    pub fn new(kind: SkewSignalKind, severity: SkewSeverity, reason: impl Into<String>) -> Self {
        Self {
            kind,
            severity,
            reason: reason.into(),
        }
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "skew={} severity={} reason={}",
            self.kind.as_str(),
            self.severity.as_str(),
            self.reason
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkewHandlingStrategy {
    None,
    SplitLargeSegments,
    SplitHotKeys,
    BroadcastSmallSide,
    RangePartition,
    SkewAwareScheduling,
    IsolateSpillHeavyPartition,
    DynamicRepartition,
    Unsupported,
}
impl SkewHandlingStrategy {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::SplitLargeSegments => "split_large_segments",
            Self::SplitHotKeys => "split_hot_keys",
            Self::BroadcastSmallSide => "broadcast_small_side",
            Self::RangePartition => "range_partition",
            Self::SkewAwareScheduling => "skew_aware_scheduling",
            Self::IsolateSpillHeavyPartition => "isolate_spill_heavy_partition",
            Self::DynamicRepartition => "dynamic_repartition",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn requires_repartition(&self) -> bool {
        matches!(self, Self::RangePartition | Self::DynamicRepartition)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerPlanStatus {
    Planned,
    PartiallyPlanned,
    Unsupported,
    NotImplemented,
}
impl OptimizerPlanStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::PartiallyPlanned => "partially_planned",
            Self::Unsupported => "unsupported",
            Self::NotImplemented => "not_implemented",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported | Self::NotImplemented)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct OptimizerPlanSkeleton {
    pub status: OptimizerPlanStatus,
    pub phase: OptimizerPhase,
    pub rule_decisions: Vec<OptimizerRuleDecision>,
    pub cost_estimates: Vec<CostEstimate>,
    pub runtime_filters: Vec<RuntimeFilter>,
    pub adaptive_decisions: Vec<AdaptiveExecutionDecision>,
    pub skew_signals: Vec<SkewSignal>,
    pub diagnostics: Vec<Diagnostic>,
}
impl OptimizerPlanSkeleton {
    #[must_use]
    pub fn new(phase: OptimizerPhase) -> Self {
        Self {
            status: OptimizerPlanStatus::Planned,
            phase,
            rule_decisions: Vec::new(),
            cost_estimates: Vec::new(),
            runtime_filters: Vec::new(),
            adaptive_decisions: Vec::new(),
            skew_signals: Vec::new(),
            diagnostics: Vec::new(),
        }
    }
    #[must_use]
    pub fn not_implemented(
        phase: OptimizerPhase,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::new(phase);
        s.status = OptimizerPlanStatus::NotImplemented;
        s.add_diagnostic(Diagnostic::not_implemented(
            feature,
            reason,
            "Use planner skeleton outputs only; execution is not performed.",
        ));
        s
    }
    #[must_use]
    pub fn unsupported(
        phase: OptimizerPhase,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::new(phase);
        s.status = OptimizerPlanStatus::Unsupported;
        s.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("No fallback execution is allowed.".to_string()),
        ));
        s
    }
    pub fn add_rule_decision(&mut self, decision: OptimizerRuleDecision) {
        self.rule_decisions.push(decision);
    }
    pub fn add_cost_estimate(&mut self, estimate: CostEstimate) {
        self.cost_estimates.push(estimate);
    }
    pub fn add_runtime_filter(&mut self, filter: RuntimeFilter) {
        self.runtime_filters.push(filter);
    }
    pub fn add_adaptive_decision(&mut self, decision: AdaptiveExecutionDecision) {
        self.adaptive_decisions.push(decision);
    }
    pub fn add_skew_signal(&mut self, signal: SkewSignal) {
        self.skew_signals.push(signal);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
            || self
                .rule_decisions
                .iter()
                .any(OptimizerRuleDecision::has_errors)
            || self.runtime_filters.iter().any(RuntimeFilter::has_errors)
            || self
                .adaptive_decisions
                .iter()
                .any(AdaptiveExecutionDecision::has_errors)
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "optimizer plan status={} phase={}\nplanning skeleton only (no optimization execution performed)\nfallback execution: disabled",
            self.status.as_str(),
            self.phase.as_str()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn optimizer_rule_id_rejects_empty_ids() {
        assert!(OptimizerRuleId::new(" ").is_err());
    }
    #[test]
    fn optimizer_phase_label_stable() {
        assert_eq!(
            OptimizerPhase::VortexPhysical.canonical_label(),
            "vortex_physical"
        );
    }
    #[test]
    fn segment_pruning_is_vortex_native() {
        assert!(OptimizerRuleKind::SegmentPruning.is_vortex_native());
    }
    #[test]
    fn runtime_filter_pushdown_runtime_adaptive() {
        assert!(OptimizerRuleKind::RuntimeFilterPushdown.is_runtime_adaptive());
    }
    #[test]
    fn unsupported_rule_status_is_error() {
        assert!(OptimizerRuleStatus::Unsupported.is_error());
    }
    #[test]
    fn rule_unsupported_has_fallback_false() {
        let id = OptimizerRuleId::new("r").unwrap();
        let d = OptimizerRuleDecision::unsupported(
            id,
            OptimizerPhase::Logical,
            OptimizerRuleKind::Unsupported,
            "x",
            "y",
        );
        assert!(d.has_errors());
        assert!(!d.diagnostics[0].fallback.attempted);
    }
    #[test]
    fn cost_value_behavior() {
        assert!(CostValue::known(1).is_known_or_estimated());
        assert!(CostValue::estimated(2).is_known_or_estimated());
        assert!(!CostValue::unknown().is_known_or_estimated());
    }
    #[test]
    fn cost_model_unknown_projection_none() {
        assert_eq!(CostModelInput::unknown().projection_ratio(), None);
    }
    #[test]
    fn dict_filter_conservative() {
        assert!(RuntimeFilterKind::DictionaryId.is_conservative_candidate());
    }
    #[test]
    fn runtime_filter_unsupported_has_errors() {
        let f = RuntimeFilter::unsupported(RuntimeFilterKind::Unsupported, "x", "y");
        assert!(f.has_errors());
        assert!(!f.diagnostics[0].fallback.attempted);
    }
    #[test]
    fn dynamic_pruning_rejected_error() {
        assert!(DynamicPruningDecision::Rejected { reason: "x".into() }.is_error());
    }
    #[test]
    fn adaptive_nochange_no_plan_change() {
        assert!(!AdaptiveDecisionKind::NoChange.changes_plan());
    }
    #[test]
    fn adaptive_split_changes_plan() {
        assert!(AdaptiveDecisionKind::SplitTasks.changes_plan());
    }
    #[test]
    fn adaptive_unsupported_has_errors() {
        let d = AdaptiveExecutionDecision::unsupported(
            AdaptiveTrigger::new(AdaptiveTriggerKind::Unknown, "x"),
            "f",
            "r",
        );
        assert!(d.has_errors());
    }
    #[test]
    fn broadcast_no_shuffle() {
        assert!(!JoinStrategy::BroadcastSmallSide.requires_shuffle());
    }
    #[test]
    fn dictionary_join_encoded_friendly() {
        assert!(JoinStrategy::DictionaryAwareJoin.is_encoded_friendly());
    }
    #[test]
    fn metadata_only_agg_vortex_native() {
        assert!(AggregateStrategy::MetadataOnly.is_vortex_native());
    }
    #[test]
    fn hash_agg_may_spill() {
        assert!(AggregateStrategy::HashAggregate.may_require_spill());
    }
    #[test]
    fn high_skew_requires_action() {
        assert!(SkewSeverity::High.requires_action());
    }
    #[test]
    fn dynamic_repartition_requires_repartition() {
        assert!(SkewHandlingStrategy::DynamicRepartition.requires_repartition());
    }
    #[test]
    fn skeleton_not_implemented_has_errors() {
        assert!(
            OptimizerPlanSkeleton::not_implemented(OptimizerPhase::Logical, "x", "y").has_errors()
        );
    }
    #[test]
    fn skeleton_human_mentions_fallback_disabled() {
        assert!(
            OptimizerPlanSkeleton::new(OptimizerPhase::Logical)
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
}
