//! Adaptive sizing and parallelism planning skeleton for `ShardLoom`.
//!
//! This module is planning-only in the current phase. It does not perform IO,
//! execution, object-store access, or fallback delegation. Sizing decisions are
//! advisory and prefer Vortex-native segment/layout boundaries over arbitrary
//! file chunking.

#![allow(
    clippy::must_use_candidate,
    clippy::return_self_not_must_use,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::missing_errors_doc
)]

use shardloom_core::{
    Diagnostic, DiagnosticSeverity, MaterializationPolicy, Result, SegmentId, ShardLoomError,
};

use crate::streaming::BackpressurePlanReport;

/// Byte size helper for deterministic planning arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ByteSize(u64);

impl ByteSize {
    pub const fn from_bytes(value: u64) -> Self {
        Self(value)
    }
    pub const fn from_kib(value: u64) -> Self {
        Self(value.saturating_mul(1024))
    }
    pub const fn from_mib(value: u64) -> Self {
        Self::from_kib(value.saturating_mul(1024))
    }
    pub const fn from_gib(value: u64) -> Self {
        Self::from_mib(value.saturating_mul(1024))
    }
    pub const fn as_bytes(&self) -> u64 {
        self.0
    }
    pub const fn saturating_mul(&self, rhs: u64) -> Self {
        Self(self.0.saturating_mul(rhs))
    }
    pub const fn saturating_div(&self, rhs: u64) -> Self {
        match self.0.checked_div(rhs) {
            Some(value) => Self(value),
            None => Self::from_bytes(0),
        }
    }
    pub fn to_human_text(&self) -> String {
        let v = self.0;
        if v >= Self::from_gib(1).as_bytes() {
            format!("{:.2} GiB", v as f64 / Self::from_gib(1).as_bytes() as f64)
        } else if v >= Self::from_mib(1).as_bytes() {
            format!("{:.2} MiB", v as f64 / Self::from_mib(1).as_bytes() as f64)
        } else if v >= Self::from_kib(1).as_bytes() {
            format!("{:.2} KiB", v as f64 / Self::from_kib(1).as_bytes() as f64)
        } else {
            format!("{v} B")
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParallelismLimit {
    Auto,
    Fixed(usize),
}
impl ParallelismLimit {
    pub const fn auto() -> Self {
        Self::Auto
    }
    pub fn fixed(value: usize) -> Result<Self> {
        if value == 0 {
            return Err(ShardLoomError::InvalidOperation(
                "fixed parallelism must be greater than zero".to_string(),
            ));
        }
        Ok(Self::Fixed(value))
    }
    pub const fn resolve(&self, available_threads: usize) -> usize {
        match self {
            Self::Auto => {
                if available_threads == 0 {
                    1
                } else {
                    available_threads
                }
            }
            Self::Fixed(v) => {
                if *v == 0 {
                    1
                } else {
                    *v
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskSizingMode {
    MetadataOnly,
    EncodedBytes,
    EstimatedDecodedBytes,
    RowCount,
    Hybrid,
}
impl TaskSizingMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::EncodedBytes => "encoded_bytes",
            Self::EstimatedDecodedBytes => "estimated_decoded_bytes",
            Self::RowCount => "row_count",
            Self::Hybrid => "hybrid",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveSizingPolicy {
    pub target_task_bytes: ByteSize,
    pub min_task_bytes: ByteSize,
    pub max_task_bytes: ByteSize,
    pub max_memory_bytes: Option<ByteSize>,
    pub max_parallelism: ParallelismLimit,
    pub max_object_store_requests: Option<u64>,
    pub mode: TaskSizingMode,
    pub allow_coalescing: bool,
    pub allow_splitting: bool,
}
impl AdaptiveSizingPolicy {
    pub const fn default_local() -> Self {
        Self {
            target_task_bytes: ByteSize::from_mib(256),
            min_task_bytes: ByteSize::from_mib(32),
            max_task_bytes: ByteSize::from_gib(1),
            max_memory_bytes: None,
            max_parallelism: ParallelismLimit::Auto,
            max_object_store_requests: None,
            mode: TaskSizingMode::Hybrid,
            allow_coalescing: true,
            allow_splitting: true,
        }
    }
    pub fn memory_limited(max_memory_bytes: ByteSize) -> Self {
        let min = ByteSize::from_mib(16);
        let target = std::cmp::max(max_memory_bytes.saturating_div(4), min);
        let max_task = std::cmp::max(max_memory_bytes.saturating_div(2), min);
        Self {
            target_task_bytes: target,
            min_task_bytes: min,
            max_task_bytes: max_task,
            max_memory_bytes: Some(max_memory_bytes),
            ..Self::default_local()
        }
    }
    pub fn with_target_task_bytes(mut self, value: ByteSize) -> Self {
        self.target_task_bytes = value;
        self
    }
    pub fn with_min_task_bytes(mut self, value: ByteSize) -> Self {
        self.min_task_bytes = value;
        self
    }
    pub fn with_max_task_bytes(mut self, value: ByteSize) -> Self {
        self.max_task_bytes = value;
        self
    }
    pub fn with_max_parallelism(mut self, value: ParallelismLimit) -> Self {
        self.max_parallelism = value;
        self
    }
    pub fn with_max_object_store_requests(mut self, value: u64) -> Self {
        self.max_object_store_requests = Some(value);
        self
    }
    pub fn with_mode(mut self, mode: TaskSizingMode) -> Self {
        self.mode = mode;
        self
    }
    pub fn disable_coalescing(mut self) -> Self {
        self.allow_coalescing = false;
        self
    }
    pub fn disable_splitting(mut self) -> Self {
        self.allow_splitting = false;
        self
    }
    pub fn summary(&self) -> String {
        format!(
            "mode={}, target={}, min={}, max={}, max_memory={}, max_parallelism={:?}, max_object_store_requests={:?}, coalescing={}, splitting={}",
            self.mode.as_str(),
            self.target_task_bytes.to_human_text(),
            self.min_task_bytes.to_human_text(),
            self.max_task_bytes.to_human_text(),
            self.max_memory_bytes
                .map_or_else(|| "none".to_string(), |v| v.to_human_text()),
            self.max_parallelism,
            self.max_object_store_requests,
            self.allow_coalescing,
            self.allow_splitting
        )
    }

    /// Returns canonical terminology for adaptive sizing policy.
    ///
    /// This helper is label-only and does not modify sizing decisions.
    #[must_use]
    pub const fn canonical_label(&self) -> &'static str {
        "adaptive_sizing_policy"
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizeEstimate {
    pub encoded_bytes: Option<ByteSize>,
    pub estimated_decoded_bytes: Option<ByteSize>,
    pub row_count: Option<u64>,
    pub selected_column_count: Option<usize>,
    pub total_column_count: Option<usize>,
}
impl SizeEstimate {
    pub const fn unknown() -> Self {
        Self {
            encoded_bytes: None,
            estimated_decoded_bytes: None,
            row_count: None,
            selected_column_count: None,
            total_column_count: None,
        }
    }
    pub const fn from_encoded_bytes(encoded_bytes: ByteSize) -> Self {
        Self {
            encoded_bytes: Some(encoded_bytes),
            ..Self::unknown()
        }
    }
    pub const fn best_bytes_for_mode(&self, mode: TaskSizingMode) -> Option<ByteSize> {
        match mode {
            TaskSizingMode::MetadataOnly | TaskSizingMode::RowCount => None,
            TaskSizingMode::EncodedBytes => self.encoded_bytes,
            TaskSizingMode::EstimatedDecodedBytes => self.estimated_decoded_bytes,
            TaskSizingMode::Hybrid => {
                if let Some(decoded) = self.estimated_decoded_bytes {
                    Some(decoded)
                } else {
                    self.encoded_bytes
                }
            }
        }
    }
    pub fn projection_ratio(&self) -> Option<f64> {
        let selected = self.selected_column_count?;
        let total = self.total_column_count?;
        if total == 0 {
            None
        } else {
            Some(selected as f64 / total as f64)
        }
    }
    pub fn summary(&self) -> String {
        format!(
            "encoded={}, decoded_est={}, rows={:?}, projection={:?}",
            self.encoded_bytes
                .map_or_else(|| "unknown".to_string(), |v| v.to_human_text()),
            self.estimated_decoded_bytes
                .map_or_else(|| "unknown".to_string(), |v| v.to_human_text()),
            self.row_count,
            self.projection_ratio()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizingInput {
    pub segment_id: SegmentId,
    pub size_estimate: SizeEstimate,
    pub has_byte_ranges: bool,
    pub can_use_metadata: bool,
    pub materialization_policy: MaterializationPolicy,
}
impl SizingInput {
    pub fn new(segment_id: SegmentId, size_estimate: SizeEstimate) -> Self {
        Self {
            segment_id,
            size_estimate,
            has_byte_ranges: false,
            can_use_metadata: false,
            materialization_policy: MaterializationPolicy::Late,
        }
    }
    pub fn with_byte_ranges(mut self, value: bool) -> Self {
        self.has_byte_ranges = value;
        self
    }
    pub fn with_metadata_available(mut self, value: bool) -> Self {
        self.can_use_metadata = value;
        self
    }
    pub fn with_materialization_policy(mut self, policy: MaterializationPolicy) -> Self {
        self.materialization_policy = policy;
        self
    }
    pub fn summary(&self) -> String {
        format!(
            "segment={}, byte_ranges={}, metadata={}, materialization={:?}, estimate=({})",
            self.segment_id.as_str(),
            self.has_byte_ranges,
            self.can_use_metadata,
            self.materialization_policy,
            self.size_estimate.summary()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskSizingDecisionKind {
    MetadataOnly,
    KeepSingle,
    Split,
    CoalesceCandidate,
    NeedsEstimate,
    Unsupported,
}
impl TaskSizingDecisionKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::KeepSingle => "keep_single",
            Self::Split => "split",
            Self::CoalesceCandidate => "coalesce_candidate",
            Self::NeedsEstimate => "needs_estimate",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSizingDecision {
    pub kind: TaskSizingDecisionKind,
    pub planned_task_count: usize,
    pub reason: String,
}
impl TaskSizingDecision {
    pub fn metadata_only(reason: impl Into<String>) -> Self {
        Self {
            kind: TaskSizingDecisionKind::MetadataOnly,
            planned_task_count: 1,
            reason: reason.into(),
        }
    }
    pub fn keep_single(reason: impl Into<String>) -> Self {
        Self {
            kind: TaskSizingDecisionKind::KeepSingle,
            planned_task_count: 1,
            reason: reason.into(),
        }
    }
    pub fn split(planned_task_count: usize, reason: impl Into<String>) -> Self {
        Self {
            kind: TaskSizingDecisionKind::Split,
            planned_task_count: planned_task_count.max(2),
            reason: reason.into(),
        }
    }
    pub fn coalesce_candidate(reason: impl Into<String>) -> Self {
        Self {
            kind: TaskSizingDecisionKind::CoalesceCandidate,
            planned_task_count: 1,
            reason: reason.into(),
        }
    }
    pub fn needs_estimate(reason: impl Into<String>) -> Self {
        Self {
            kind: TaskSizingDecisionKind::NeedsEstimate,
            planned_task_count: 1,
            reason: reason.into(),
        }
    }
    pub fn unsupported(reason: impl Into<String>) -> Self {
        Self {
            kind: TaskSizingDecisionKind::Unsupported,
            planned_task_count: 1,
            reason: reason.into(),
        }
    }
    pub const fn requires_split(&self) -> bool {
        matches!(self.kind, TaskSizingDecisionKind::Split)
    }
    pub fn summary(&self) -> String {
        format!(
            "kind={}, tasks={}, reason={}",
            self.kind.as_str(),
            self.planned_task_count,
            self.reason
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveSizer {
    pub policy: AdaptiveSizingPolicy,
}
impl AdaptiveSizer {
    pub const fn new(policy: AdaptiveSizingPolicy) -> Self {
        Self { policy }
    }
    pub fn decide_for_segment(&self, input: &SizingInput) -> TaskSizingDecision {
        if input.can_use_metadata && self.policy.mode == TaskSizingMode::MetadataOnly {
            return TaskSizingDecision::metadata_only(
                "metadata-only sizing mode with metadata available",
            );
        }
        let Some(bytes) = input.size_estimate.best_bytes_for_mode(self.policy.mode) else {
            return TaskSizingDecision::needs_estimate(
                "size estimate unavailable for selected sizing mode",
            );
        };
        if bytes.as_bytes() <= self.policy.min_task_bytes.as_bytes() && self.policy.allow_coalescing
        {
            return TaskSizingDecision::coalesce_candidate(
                "segment is smaller than or equal to minimum task size",
            );
        }
        if bytes.as_bytes() <= self.policy.max_task_bytes.as_bytes() {
            return TaskSizingDecision::keep_single("segment fits within max task size");
        }
        if self.policy.allow_splitting && input.has_byte_ranges {
            let n = bytes
                .as_bytes()
                .saturating_add(self.policy.target_task_bytes.as_bytes().saturating_sub(1))
                / self.policy.target_task_bytes.as_bytes().max(1);
            return TaskSizingDecision::split(
                n as usize,
                "segment exceeds max task size and supports byte-range splitting",
            );
        }
        TaskSizingDecision::keep_single(
            "segment exceeds max task size but splitting is unavailable",
        )
    }
    pub const fn resolved_parallelism(&self, available_threads: usize) -> usize {
        self.policy.max_parallelism.resolve(available_threads)
    }
    pub fn summary(&self) -> String {
        format!("AdaptiveSizer(policy: {})", self.policy.summary())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoalescingPolicy {
    pub target_coalesced_bytes: ByteSize,
    pub max_segments_per_task: usize,
}
impl Default for CoalescingPolicy {
    fn default() -> Self {
        Self {
            target_coalesced_bytes: ByteSize::from_mib(256),
            max_segments_per_task: 32,
        }
    }
}
impl CoalescingPolicy {
    pub fn new(target_coalesced_bytes: ByteSize, max_segments_per_task: usize) -> Result<Self> {
        if max_segments_per_task == 0 {
            return Err(ShardLoomError::InvalidOperation(
                "max_segments_per_task must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            target_coalesced_bytes,
            max_segments_per_task,
        })
    }
    pub fn summary(&self) -> String {
        format!(
            "target={}, max_segments_per_task={}",
            self.target_coalesced_bytes.to_human_text(),
            self.max_segments_per_task
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParallelismPlan {
    pub requested_parallelism: ParallelismLimit,
    pub resolved_parallelism: usize,
    pub task_count: usize,
    pub reason: String,
}
impl ParallelismPlan {
    pub fn new(
        requested_parallelism: ParallelismLimit,
        available_threads: usize,
        task_count: usize,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            requested_parallelism,
            resolved_parallelism: requested_parallelism.resolve(available_threads),
            task_count,
            reason: reason.into(),
        }
    }
    pub const fn effective_parallelism(&self) -> usize {
        if self.task_count == 0 {
            1
        } else {
            let min = if self.resolved_parallelism < self.task_count {
                self.resolved_parallelism
            } else {
                self.task_count
            };
            if min == 0 { 1 } else { min }
        }
    }
    pub fn summary(&self) -> String {
        format!(
            "requested={:?}, resolved={}, tasks={}, effective={}, reason={}",
            self.requested_parallelism,
            self.resolved_parallelism,
            self.task_count,
            self.effective_parallelism(),
            self.reason
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizingPlan {
    pub policy: AdaptiveSizingPolicy,
    pub decisions: Vec<(SegmentId, TaskSizingDecision)>,
    pub parallelism: ParallelismPlan,
    pub diagnostics: Vec<Diagnostic>,
}
impl SizingPlan {
    pub fn new(policy: AdaptiveSizingPolicy, parallelism: ParallelismPlan) -> Self {
        Self {
            policy,
            decisions: Vec::new(),
            parallelism,
            diagnostics: Vec::new(),
        }
    }
    pub fn add_decision(&mut self, segment_id: SegmentId, decision: TaskSizingDecision) {
        self.decisions.push((segment_id, decision));
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn planned_task_count(&self) -> usize {
        self.decisions
            .iter()
            .map(|(_, d)| d.planned_task_count)
            .sum()
    }
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    pub fn to_human_text(&self) -> String {
        format!(
            "sizing plan\npolicy: {}\nparallelism: {}\nplanned tasks: {}\ndecisions: {}\ndiagnostics: {}\nfallback execution: disabled",
            self.policy.summary(),
            self.parallelism.summary(),
            self.planned_task_count(),
            self.decisions.len(),
            self.diagnostics.len()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DynamicSizingFeedbackStatus {
    NoFeedback,
    Planned,
    TargetReduced,
    TargetIncreased,
    MixedSignals,
    Unsupported,
}
impl DynamicSizingFeedbackStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NoFeedback => "no_feedback",
            Self::Planned => "planned",
            Self::TargetReduced => "target_reduced",
            Self::TargetIncreased => "target_increased",
            Self::MixedSignals => "mixed_signals",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DynamicSizingFeedbackMode {
    PlanOnly,
    TargetAdjustment,
    Unsupported,
}
impl DynamicSizingFeedbackMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PlanOnly => "plan_only",
            Self::TargetAdjustment => "target_adjustment",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn executes_feedback(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizingFeedbackSignalKind {
    Stable,
    TaskTooLarge,
    TaskTooSmall,
    MemoryPressureHigh,
    ObjectStoreThrottled,
}
impl SizingFeedbackSignalKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Stable => "stable",
            Self::TaskTooLarge => "task_too_large",
            Self::TaskTooSmall => "task_too_small",
            Self::MemoryPressureHigh => "memory_pressure_high",
            Self::ObjectStoreThrottled => "object_store_throttled",
        }
    }
    pub const fn recommends_smaller_tasks(&self) -> bool {
        matches!(self, Self::TaskTooLarge | Self::MemoryPressureHigh)
    }
    pub const fn recommends_larger_tasks(&self) -> bool {
        matches!(self, Self::TaskTooSmall | Self::ObjectStoreThrottled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizingFeedbackSignal {
    pub kind: SizingFeedbackSignalKind,
    pub reason: String,
}
impl SizingFeedbackSignal {
    pub fn new(kind: SizingFeedbackSignalKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            reason: reason.into(),
        }
    }
    pub fn stable() -> Self {
        Self::new(
            SizingFeedbackSignalKind::Stable,
            "runtime feedback is stable",
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicSizingFeedbackInput {
    pub current_policy: AdaptiveSizingPolicy,
    pub signals: Vec<SizingFeedbackSignal>,
    pub diagnostics: Vec<Diagnostic>,
}
impl DynamicSizingFeedbackInput {
    pub fn new(current_policy: AdaptiveSizingPolicy) -> Self {
        Self {
            current_policy,
            signals: vec![],
            diagnostics: vec![],
        }
    }
    pub fn add_signal(&mut self, signal: SizingFeedbackSignal) {
        self.signals.push(signal);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
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
pub struct DynamicSizingFeedbackReport {
    pub status: DynamicSizingFeedbackStatus,
    pub mode: DynamicSizingFeedbackMode,
    pub input: DynamicSizingFeedbackInput,
    pub recommended_policy: AdaptiveSizingPolicy,
    pub signal_count: usize,
    pub reduce_signal_count: usize,
    pub increase_signal_count: usize,
    pub stable_signal_count: usize,
    pub current_target_task_bytes: ByteSize,
    pub recommended_target_task_bytes: ByteSize,
    pub tasks_executed: bool,
    pub data_read: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub feedback_applied: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl DynamicSizingFeedbackReport {
    pub fn from_input(input: DynamicSizingFeedbackInput) -> Self {
        let current_target = input.current_policy.target_task_bytes;
        let mut recommended_policy = input.current_policy.clone();
        let reduce_count = input
            .signals
            .iter()
            .filter(|s| s.kind.recommends_smaller_tasks())
            .count();
        let increase_count = input
            .signals
            .iter()
            .filter(|s| s.kind.recommends_larger_tasks())
            .count();
        let stable_count = input
            .signals
            .iter()
            .filter(|s| s.kind == SizingFeedbackSignalKind::Stable)
            .count();
        let mut status = if input.signals.is_empty() {
            DynamicSizingFeedbackStatus::NoFeedback
        } else {
            DynamicSizingFeedbackStatus::Planned
        };
        let mut mode = DynamicSizingFeedbackMode::PlanOnly;
        if input.has_errors() {
            status = DynamicSizingFeedbackStatus::Unsupported;
            mode = DynamicSizingFeedbackMode::Unsupported;
        } else if reduce_count > 0 {
            recommended_policy.target_task_bytes = std::cmp::max(
                current_target.saturating_div(2),
                input.current_policy.min_task_bytes,
            );
            status = if increase_count > 0 {
                DynamicSizingFeedbackStatus::MixedSignals
            } else {
                DynamicSizingFeedbackStatus::TargetReduced
            };
            mode = DynamicSizingFeedbackMode::TargetAdjustment;
        } else if increase_count > 0 {
            recommended_policy.target_task_bytes = std::cmp::min(
                current_target.saturating_mul(2),
                input.current_policy.max_task_bytes,
            );
            status = DynamicSizingFeedbackStatus::TargetIncreased;
            mode = DynamicSizingFeedbackMode::TargetAdjustment;
        }
        let recommended_target = recommended_policy.target_task_bytes;
        Self {
            status,
            mode,
            signal_count: input.signals.len(),
            reduce_signal_count: reduce_count,
            increase_signal_count: increase_count,
            stable_signal_count: stable_count,
            current_target_task_bytes: current_target,
            recommended_target_task_bytes: recommended_target,
            recommended_policy,
            diagnostics: input.diagnostics.clone(),
            input,
            tasks_executed: false,
            data_read: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            feedback_applied: false,
            fallback_execution_allowed: false,
        }
    }
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
    pub const fn is_side_effect_free(&self) -> bool {
        !self.tasks_executed
            && !self.data_read
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.feedback_applied
            && !self.fallback_execution_allowed
    }
    pub fn to_human_text(&self) -> String {
        format!(
            "dynamic sizing feedback status: {}\nmode: {}\nsignals: {}\nreduce signals: {}\nincrease signals: {}\nstable signals: {}\ncurrent target task bytes: {}\nrecommended target task bytes: {}\ntasks executed: false\ndata read: false\nobject-store IO: false\nwrite IO: false\nspill IO performed: false\nfeedback applied: false\nfallback execution: disabled",
            self.status.as_str(),
            self.mode.as_str(),
            self.signal_count,
            self.reduce_signal_count,
            self.increase_signal_count,
            self.stable_signal_count,
            self.current_target_task_bytes.as_bytes(),
            self.recommended_target_task_bytes.as_bytes(),
        )
    }
}

pub fn plan_dynamic_sizing_feedback(
    input: DynamicSizingFeedbackInput,
) -> DynamicSizingFeedbackReport {
    DynamicSizingFeedbackReport::from_input(input)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DynamicWorkShapingStatus {
    PlanReady,
    NeedsRuntimeIntegration,
    UnsafeFallbackPolicy,
}

impl DynamicWorkShapingStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PlanReady => "plan_ready",
            Self::NeedsRuntimeIntegration => "needs_runtime_integration",
            Self::UnsafeFallbackPolicy => "unsafe_fallback_policy",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::UnsafeFallbackPolicy)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct DynamicWorkShapingReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub profile: String,
    pub status: DynamicWorkShapingStatus,
    pub planned_surface_count: usize,
    pub blocked_surface_count: usize,
    pub blocked_surface_order: Vec<String>,
    pub feedback_status: DynamicSizingFeedbackStatus,
    pub feedback_mode: DynamicSizingFeedbackMode,
    pub signal_count: usize,
    pub reduce_signal_count: usize,
    pub increase_signal_count: usize,
    pub stable_signal_count: usize,
    pub target_task_bytes_changed: bool,
    pub current_target_task_bytes: ByteSize,
    pub recommended_target_task_bytes: ByteSize,
    pub adaptive_splitting_allowed: bool,
    pub adaptive_coalescing_allowed: bool,
    pub backpressure_status: crate::streaming::BackpressurePlanStatus,
    pub backpressure_mode: crate::streaming::BackpressurePlanMode,
    pub bounded_backpressure: bool,
    pub max_parallelism: usize,
    pub max_in_flight_chunks: Option<usize>,
    pub max_buffered_bytes: Option<ByteSize>,
    pub estimated_chunk_bytes: Option<ByteSize>,
    pub bounded_memory_required: bool,
    pub spill_allowed: bool,
    pub runtime_feedback_loop_ready: bool,
    pub policy_application_ready: bool,
    pub benchmark_evidence_ready: bool,
    pub streams_executed: bool,
    pub tasks_executed: bool,
    pub feedback_applied: bool,
    pub policy_mutated: bool,
    pub data_read: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl DynamicWorkShapingReport {
    #[must_use]
    pub fn surface_order() -> Vec<&'static str> {
        vec![
            "adaptive_sizing_policy",
            "feedback_signals",
            "target_task_policy",
            "backpressure_policy",
            "bounded_memory_policy",
            "scheduler_queue_policy",
            "runtime_application_loop",
            "benchmark_evidence",
            "no_fallback_policy",
        ]
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.streams_executed
            && !self.tasks_executed
            && !self.feedback_applied
            && !self.policy_mutated
            && !self.data_read
            && !self.data_materialized
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.fallback_execution_allowed
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "dynamic work shaping status: {}\nprofile: {}\nplanned surfaces: {}\nblocked surfaces: {}\nfeedback status: {}\nbackpressure status: {}\ncurrent target task bytes: {}\nrecommended target task bytes: {}\nruntime feedback loop ready: {}\npolicy application ready: {}\nbenchmark evidence ready: {}\nstreams executed: false\ntasks executed: false\ndata read: false\nobject-store IO: false\nwrite IO: false\nspill IO performed: false\nfeedback applied: false\nfallback execution: disabled",
            self.status.as_str(),
            self.profile,
            self.planned_surface_count,
            self.blocked_surface_count,
            self.feedback_status.as_str(),
            self.backpressure_status.as_str(),
            self.current_target_task_bytes.as_bytes(),
            self.recommended_target_task_bytes.as_bytes(),
            self.runtime_feedback_loop_ready,
            self.policy_application_ready,
            self.benchmark_evidence_ready,
        )
    }
}

#[must_use]
pub fn plan_dynamic_work_shaping(
    profile: impl Into<String>,
    feedback: &DynamicSizingFeedbackReport,
    backpressure: &BackpressurePlanReport,
) -> DynamicWorkShapingReport {
    let fallback_attempted = false;
    let fallback_execution_allowed =
        feedback.fallback_execution_allowed || backpressure.fallback_execution_allowed;
    let runtime_feedback_loop_ready = false;
    let policy_application_ready = false;
    let benchmark_evidence_ready = false;
    let mut blocked_surface_order = Vec::new();
    if feedback.has_errors() {
        blocked_surface_order.push("feedback_signals".to_string());
    }
    if backpressure.has_errors() {
        blocked_surface_order.push("backpressure_policy".to_string());
    }
    if !runtime_feedback_loop_ready {
        blocked_surface_order.push("runtime_application_loop".to_string());
    }
    if !benchmark_evidence_ready {
        blocked_surface_order.push("benchmark_evidence".to_string());
    }
    if fallback_execution_allowed || fallback_attempted {
        blocked_surface_order.push("no_fallback_policy".to_string());
    }
    let blocked_surface_count = blocked_surface_order.len();
    let planned_surface_count = DynamicWorkShapingReport::surface_order()
        .len()
        .saturating_sub(blocked_surface_count);
    let mut diagnostics = feedback.diagnostics.clone();
    diagnostics.extend(backpressure.diagnostics.clone());
    let status = if fallback_execution_allowed || fallback_attempted {
        DynamicWorkShapingStatus::UnsafeFallbackPolicy
    } else if runtime_feedback_loop_ready && policy_application_ready && benchmark_evidence_ready {
        DynamicWorkShapingStatus::PlanReady
    } else {
        DynamicWorkShapingStatus::NeedsRuntimeIntegration
    };

    DynamicWorkShapingReport {
        schema_version: "shardloom.dynamic_work_shaping.v1",
        report_id: "cg8.dynamic_work_shaping.aggregate",
        profile: profile.into(),
        status,
        planned_surface_count,
        blocked_surface_count,
        blocked_surface_order,
        feedback_status: feedback.status,
        feedback_mode: feedback.mode,
        signal_count: feedback.signal_count,
        reduce_signal_count: feedback.reduce_signal_count,
        increase_signal_count: feedback.increase_signal_count,
        stable_signal_count: feedback.stable_signal_count,
        target_task_bytes_changed: feedback.current_target_task_bytes
            != feedback.recommended_target_task_bytes,
        current_target_task_bytes: feedback.current_target_task_bytes,
        recommended_target_task_bytes: feedback.recommended_target_task_bytes,
        adaptive_splitting_allowed: feedback.recommended_policy.allow_splitting,
        adaptive_coalescing_allowed: feedback.recommended_policy.allow_coalescing,
        backpressure_status: backpressure.status,
        backpressure_mode: backpressure.mode,
        bounded_backpressure: backpressure.bounded,
        max_parallelism: backpressure.input.max_parallelism,
        max_in_flight_chunks: backpressure.max_in_flight_chunks,
        max_buffered_bytes: backpressure.max_buffered_bytes,
        estimated_chunk_bytes: backpressure.estimated_chunk_bytes,
        bounded_memory_required: backpressure.memory_required,
        spill_allowed: backpressure.spill_allowed,
        runtime_feedback_loop_ready,
        policy_application_ready,
        benchmark_evidence_ready,
        streams_executed: feedback.tasks_executed || backpressure.streams_executed,
        tasks_executed: feedback.tasks_executed || backpressure.tasks_executed,
        feedback_applied: feedback.feedback_applied,
        policy_mutated: false,
        data_read: feedback.data_read || backpressure.data_read,
        data_materialized: backpressure.data_materialized,
        object_store_io: feedback.object_store_io || backpressure.object_store_io,
        write_io: feedback.write_io || backpressure.write_io,
        spill_io_performed: feedback.spill_io_performed || backpressure.spill_io_performed,
        fallback_execution_allowed,
        fallback_attempted,
        diagnostics,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DynamicRuntimePromotionSurface {
    DynamicSizingFeedbackApplication,
    BoundedParallelEncodedReadRuntime,
    SourceBackedReaderSplitParallelism,
    SchedulerRequeuePolicy,
    BoundedQueueBackpressureRuntime,
    MemorySpillReservationRuntime,
    ObjectStoreRequestBudgetRuntime,
    BenchmarkCertificateCloseout,
}

impl DynamicRuntimePromotionSurface {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::DynamicSizingFeedbackApplication => "dynamic_sizing_feedback_application",
            Self::BoundedParallelEncodedReadRuntime => "bounded_parallel_encoded_read_runtime",
            Self::SourceBackedReaderSplitParallelism => "source_backed_reader_split_parallelism",
            Self::SchedulerRequeuePolicy => "scheduler_requeue_policy",
            Self::BoundedQueueBackpressureRuntime => "bounded_queue_backpressure_runtime",
            Self::MemorySpillReservationRuntime => "memory_spill_reservation_runtime",
            Self::ObjectStoreRequestBudgetRuntime => "object_store_request_budget_runtime",
            Self::BenchmarkCertificateCloseout => "benchmark_certificate_closeout",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DynamicRuntimePromotionStatus {
    ExistingNarrowLocalEvidence,
    BlockedUntilCertified,
}

impl DynamicRuntimePromotionStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ExistingNarrowLocalEvidence => "existing_narrow_local_evidence",
            Self::BlockedUntilCertified => "blocked_until_certified",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct DynamicRuntimePromotionGateEntry {
    pub surface: DynamicRuntimePromotionSurface,
    pub status: DynamicRuntimePromotionStatus,
    pub required_evidence: &'static str,
    pub existing_limited_local_evidence: bool,
    pub requires_runtime_metrics: bool,
    pub requires_target_task_policy: bool,
    pub requires_scheduler_queue_policy: bool,
    pub requires_memory_reservation_evidence: bool,
    pub requires_spill_policy_evidence: bool,
    pub requires_backpressure_evidence: bool,
    pub requires_cancellation_retry_evidence: bool,
    pub requires_execution_certificate: bool,
    pub requires_native_io_certificate: bool,
    pub requires_benchmark_evidence: bool,
    pub runtime_promotion_allowed: bool,
}

impl DynamicRuntimePromotionGateEntry {
    const fn existing_limited(
        surface: DynamicRuntimePromotionSurface,
        required_evidence: &'static str,
    ) -> Self {
        Self {
            surface,
            status: DynamicRuntimePromotionStatus::ExistingNarrowLocalEvidence,
            required_evidence,
            existing_limited_local_evidence: true,
            requires_runtime_metrics: true,
            requires_target_task_policy: true,
            requires_scheduler_queue_policy: true,
            requires_memory_reservation_evidence: true,
            requires_spill_policy_evidence: true,
            requires_backpressure_evidence: true,
            requires_cancellation_retry_evidence: true,
            requires_execution_certificate: true,
            requires_native_io_certificate: true,
            requires_benchmark_evidence: true,
            runtime_promotion_allowed: false,
        }
    }

    const fn blocked(
        surface: DynamicRuntimePromotionSurface,
        required_evidence: &'static str,
    ) -> Self {
        Self {
            surface,
            status: DynamicRuntimePromotionStatus::BlockedUntilCertified,
            required_evidence,
            existing_limited_local_evidence: false,
            requires_runtime_metrics: true,
            requires_target_task_policy: true,
            requires_scheduler_queue_policy: true,
            requires_memory_reservation_evidence: true,
            requires_spill_policy_evidence: true,
            requires_backpressure_evidence: true,
            requires_cancellation_retry_evidence: true,
            requires_execution_certificate: true,
            requires_native_io_certificate: true,
            requires_benchmark_evidence: true,
            runtime_promotion_allowed: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct DynamicRuntimePromotionGateReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub entries: Vec<DynamicRuntimePromotionGateEntry>,
    pub existing_local_streaming_scan_evidence_present: bool,
    pub existing_local_bounded_metadata_noop_evidence_present: bool,
    pub existing_local_filter_project_bounded_scan_evidence_present: bool,
    pub dynamic_feedback_application_allowed: bool,
    pub bounded_parallel_encoded_read_allowed: bool,
    pub source_backed_parallel_reader_allowed: bool,
    pub scheduler_requeue_allowed: bool,
    pub bounded_backpressure_runtime_allowed: bool,
    pub memory_spill_reservation_runtime_allowed: bool,
    pub object_store_request_budget_runtime_allowed: bool,
    pub runtime_policy_mutation_allowed: bool,
    pub large_workload_claim_allowed: bool,
    pub runtime_metrics_required: bool,
    pub target_task_policy_required: bool,
    pub scheduler_queue_policy_required: bool,
    pub memory_reservation_evidence_required: bool,
    pub spill_policy_evidence_required: bool,
    pub backpressure_evidence_required: bool,
    pub cancellation_retry_evidence_required: bool,
    pub execution_certificate_required: bool,
    pub native_io_certificate_required: bool,
    pub benchmark_evidence_required: bool,
    pub runtime_execution_performed: bool,
    pub tasks_executed: bool,
    pub data_read: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub feedback_applied: bool,
    pub policy_mutated: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl DynamicRuntimePromotionGateReport {
    #[must_use]
    pub fn planning_default() -> Self {
        Self {
            schema_version: "shardloom.dynamic_runtime_promotion_gate.v1",
            report_id: "cg8.dynamic_runtime_promotion_gate",
            entries: vec![
                DynamicRuntimePromotionGateEntry::existing_limited(
                    DynamicRuntimePromotionSurface::DynamicSizingFeedbackApplication,
                    "existing dynamic sizing feedback reports remain advisory; runtime application requires observed metrics, conservative mutation policy, scheduler requeue semantics, memory/spill proof, certificates, and benchmark evidence",
                ),
                DynamicRuntimePromotionGateEntry::existing_limited(
                    DynamicRuntimePromotionSurface::BoundedParallelEncodedReadRuntime,
                    "existing narrow local streaming scan and filter/project evidence remains fixture/local scoped; broader parallel encoded reads require source-backed split scheduling, bounded queues, Native I/O certificates, execution certificates, and benchmark evidence",
                ),
                DynamicRuntimePromotionGateEntry::blocked(
                    DynamicRuntimePromotionSurface::SourceBackedReaderSplitParallelism,
                    "reader-generated split identity, source-backed prepared batches, residual boundaries, selection-vector propagation, cancellation/retry checkpoints, and certificate pairs",
                ),
                DynamicRuntimePromotionGateEntry::blocked(
                    DynamicRuntimePromotionSurface::SchedulerRequeuePolicy,
                    "stable task identity, queue ownership, requeue invariants, idempotent attempt records, cancellation propagation, and fairness/resource accounting",
                ),
                DynamicRuntimePromotionGateEntry::blocked(
                    DynamicRuntimePromotionSurface::BoundedQueueBackpressureRuntime,
                    "bounded in-flight chunks, buffered-byte limits, backpressure transitions, downstream sink pressure, spill handoff policy, and runtime metric evidence",
                ),
                DynamicRuntimePromotionGateEntry::blocked(
                    DynamicRuntimePromotionSurface::MemorySpillReservationRuntime,
                    "operator memory reservations, spill admission, fail-before-OOM diagnostics, cleanup/recovery evidence, and no unbounded buffering proof",
                ),
                DynamicRuntimePromotionGateEntry::blocked(
                    DynamicRuntimePromotionSurface::ObjectStoreRequestBudgetRuntime,
                    "range request budget, coalescing/prefetch policy, retry/idempotency evidence, object-store request metrics, and cost/fairness accounting",
                ),
                DynamicRuntimePromotionGateEntry::blocked(
                    DynamicRuntimePromotionSurface::BenchmarkCertificateCloseout,
                    "workload-scoped correctness, benchmark rows, reproducibility, execution certificates, Native I/O certificates, and no-fallback evidence for the promoted runtime",
                ),
            ],
            existing_local_streaming_scan_evidence_present: true,
            existing_local_bounded_metadata_noop_evidence_present: true,
            existing_local_filter_project_bounded_scan_evidence_present: true,
            dynamic_feedback_application_allowed: false,
            bounded_parallel_encoded_read_allowed: false,
            source_backed_parallel_reader_allowed: false,
            scheduler_requeue_allowed: false,
            bounded_backpressure_runtime_allowed: false,
            memory_spill_reservation_runtime_allowed: false,
            object_store_request_budget_runtime_allowed: false,
            runtime_policy_mutation_allowed: false,
            large_workload_claim_allowed: false,
            runtime_metrics_required: true,
            target_task_policy_required: true,
            scheduler_queue_policy_required: true,
            memory_reservation_evidence_required: true,
            spill_policy_evidence_required: true,
            backpressure_evidence_required: true,
            cancellation_retry_evidence_required: true,
            execution_certificate_required: true,
            native_io_certificate_required: true,
            benchmark_evidence_required: true,
            runtime_execution_performed: false,
            tasks_executed: false,
            data_read: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            feedback_applied: false,
            policy_mutated: false,
            fallback_execution_allowed: false,
            fallback_attempted: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn surface_count(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn existing_limited_surface_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.existing_limited_local_evidence)
            .count()
    }

    #[must_use]
    pub fn blocked_surface_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.status == DynamicRuntimePromotionStatus::BlockedUntilCertified)
            .count()
    }

    #[must_use]
    pub fn runtime_ready_surface_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.runtime_promotion_allowed)
            .count()
    }

    #[must_use]
    pub fn surface_order(&self) -> Vec<&'static str> {
        self.entries
            .iter()
            .map(|entry| entry.surface.as_str())
            .collect()
    }

    #[must_use]
    pub fn runtime_promotions_blocked(&self) -> bool {
        !self.dynamic_feedback_application_allowed
            && !self.bounded_parallel_encoded_read_allowed
            && !self.source_backed_parallel_reader_allowed
            && !self.scheduler_requeue_allowed
            && !self.bounded_backpressure_runtime_allowed
            && !self.memory_spill_reservation_runtime_allowed
            && !self.object_store_request_budget_runtime_allowed
            && !self.runtime_policy_mutation_allowed
            && self.runtime_ready_surface_count() == 0
    }

    #[must_use]
    pub const fn side_effect_free(&self) -> bool {
        !self.runtime_execution_performed
            && !self.tasks_executed
            && !self.data_read
            && !self.data_materialized
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.feedback_applied
            && !self.policy_mutated
            && !self.fallback_execution_allowed
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn claim_blocked(&self) -> bool {
        !self.large_workload_claim_allowed
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.side_effect_free()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "dynamic runtime promotion gate\nschema_version: {}\nreport_id: {}\nruntime promotions blocked: {}\nlarge workload claim allowed: {}\nruntime execution: false\ndata read: false\nobject-store IO: false\nwrite IO: false\nspill IO performed: false\nfeedback applied: false\nfallback execution: disabled",
            self.schema_version,
            self.report_id,
            self.runtime_promotions_blocked(),
            self.large_workload_claim_allowed,
        )
    }
}

#[must_use]
pub fn plan_dynamic_runtime_promotion_gate() -> DynamicRuntimePromotionGateReport {
    DynamicRuntimePromotionGateReport::planning_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::{BackpressurePlanInput, BoundedMemoryPolicy, plan_backpressure};

    fn seg() -> SegmentId {
        SegmentId::new("s1").expect("valid")
    }

    fn dynamic_feedback_with_signal(
        signal: SizingFeedbackSignalKind,
    ) -> DynamicSizingFeedbackReport {
        let mut input = DynamicSizingFeedbackInput::new(AdaptiveSizingPolicy::memory_limited(
            ByteSize::from_gib(8),
        ));
        input.add_signal(SizingFeedbackSignal::new(signal, signal.as_str()));
        plan_dynamic_sizing_feedback(input)
    }

    fn bounded_backpressure() -> BackpressurePlanReport {
        plan_backpressure(
            BackpressurePlanInput::new(
                BoundedMemoryPolicy::required(ByteSize::from_gib(8)).with_spill(true),
                4,
            )
            .expect("backpressure input")
            .with_estimated_chunk_bytes(ByteSize::from_mib(256)),
        )
        .expect("backpressure report")
    }

    #[test]
    fn byte_size_from_gib_expected() {
        assert_eq!(ByteSize::from_gib(1).as_bytes(), 1024 * 1024 * 1024);
    }
    #[test]
    fn byte_size_div_zero_returns_zero() {
        assert_eq!(ByteSize::from_mib(4).saturating_div(0).as_bytes(), 0);
    }
    #[test]
    fn parallelism_fixed_rejects_zero() {
        assert!(ParallelismLimit::fixed(0).is_err());
    }
    #[test]
    fn parallelism_auto_resolves_at_least_one() {
        assert_eq!(ParallelismLimit::auto().resolve(0), 1);
    }
    #[test]
    fn default_local_flags_enabled() {
        let p = AdaptiveSizingPolicy::default_local();
        assert!(p.allow_coalescing && p.allow_splitting);
    }
    #[test]
    fn memory_limited_sets_max_memory() {
        let p = AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(8));
        assert_eq!(p.max_memory_bytes, Some(ByteSize::from_gib(8)));
    }
    #[test]
    fn adaptive_sizing_policy_canonical_label_is_stable() {
        assert_eq!(
            AdaptiveSizingPolicy::default_local().canonical_label(),
            "adaptive_sizing_policy"
        );
    }
    #[test]
    fn size_unknown_no_hybrid_best() {
        assert!(
            SizeEstimate::unknown()
                .best_bytes_for_mode(TaskSizingMode::Hybrid)
                .is_none()
        );
    }
    #[test]
    fn size_from_encoded_works() {
        assert_eq!(
            SizeEstimate::from_encoded_bytes(ByteSize::from_mib(1))
                .best_bytes_for_mode(TaskSizingMode::EncodedBytes),
            Some(ByteSize::from_mib(1))
        );
    }
    #[test]
    fn size_hybrid_prefers_decoded() {
        let s = SizeEstimate {
            encoded_bytes: Some(ByteSize::from_mib(10)),
            estimated_decoded_bytes: Some(ByteSize::from_mib(20)),
            row_count: None,
            selected_column_count: None,
            total_column_count: None,
        };
        assert_eq!(
            s.best_bytes_for_mode(TaskSizingMode::Hybrid),
            Some(ByteSize::from_mib(20))
        );
    }
    #[test]
    fn size_projection_ratio() {
        let s = SizeEstimate {
            encoded_bytes: None,
            estimated_decoded_bytes: None,
            row_count: None,
            selected_column_count: Some(1),
            total_column_count: Some(4),
        };
        assert_eq!(s.projection_ratio(), Some(0.25));
    }
    #[test]
    fn sizing_input_defaults_late() {
        assert_eq!(
            SizingInput::new(seg(), SizeEstimate::unknown()).materialization_policy,
            MaterializationPolicy::Late
        );
    }
    #[test]
    fn split_clamps_to_two() {
        assert_eq!(TaskSizingDecision::split(1, "x").planned_task_count, 2);
    }
    #[test]
    fn sizer_needs_estimate_when_unknown() {
        let d = AdaptiveSizer::new(AdaptiveSizingPolicy::default_local())
            .decide_for_segment(&SizingInput::new(seg(), SizeEstimate::unknown()));
        assert_eq!(d.kind, TaskSizingDecisionKind::NeedsEstimate);
    }
    #[test]
    fn metadata_only_without_metadata_needs_estimate() {
        let d = AdaptiveSizer::new(
            AdaptiveSizingPolicy::default_local().with_mode(TaskSizingMode::MetadataOnly),
        )
        .decide_for_segment(
            &SizingInput::new(seg(), SizeEstimate::unknown()).with_metadata_available(false),
        );
        assert_eq!(d.kind, TaskSizingDecisionKind::NeedsEstimate);
    }
    #[test]
    fn sizer_coalesce_for_small() {
        let d = AdaptiveSizer::new(AdaptiveSizingPolicy::default_local()).decide_for_segment(
            &SizingInput::new(
                seg(),
                SizeEstimate::from_encoded_bytes(ByteSize::from_mib(1)),
            ),
        );
        assert_eq!(d.kind, TaskSizingDecisionKind::CoalesceCandidate);
    }
    #[test]
    fn sizer_keep_single_under_max() {
        let d = AdaptiveSizer::new(AdaptiveSizingPolicy::default_local()).decide_for_segment(
            &SizingInput::new(
                seg(),
                SizeEstimate::from_encoded_bytes(ByteSize::from_mib(128)),
            ),
        );
        assert_eq!(d.kind, TaskSizingDecisionKind::KeepSingle);
    }
    #[test]
    fn sizer_split_large_with_ranges() {
        let d = AdaptiveSizer::new(AdaptiveSizingPolicy::default_local()).decide_for_segment(
            &SizingInput::new(
                seg(),
                SizeEstimate::from_encoded_bytes(ByteSize::from_gib(2)),
            )
            .with_byte_ranges(true),
        );
        assert_eq!(d.kind, TaskSizingDecisionKind::Split);
    }
    #[test]
    fn sizer_keep_large_without_ranges() {
        let d = AdaptiveSizer::new(AdaptiveSizingPolicy::default_local()).decide_for_segment(
            &SizingInput::new(
                seg(),
                SizeEstimate::from_encoded_bytes(ByteSize::from_gib(2)),
            )
            .with_byte_ranges(false),
        );
        assert_eq!(d.kind, TaskSizingDecisionKind::KeepSingle);
    }
    #[test]
    fn coalescing_rejects_zero_segments() {
        assert!(CoalescingPolicy::new(ByteSize::from_mib(1), 0).is_err());
    }
    #[test]
    fn parallelism_effective_min() {
        let p = ParallelismPlan::new(ParallelismLimit::Fixed(8), 8, 3, "test");
        assert_eq!(p.effective_parallelism(), 3);
    }
    #[test]
    fn sizing_plan_sums_decisions() {
        let mut p = SizingPlan::new(
            AdaptiveSizingPolicy::default_local(),
            ParallelismPlan::new(ParallelismLimit::Auto, 1, 1, "x"),
        );
        p.add_decision(
            SegmentId::new("a").unwrap(),
            TaskSizingDecision::keep_single("a"),
        );
        p.add_decision(
            SegmentId::new("b").unwrap(),
            TaskSizingDecision::split(4, "b"),
        );
        assert_eq!(p.planned_task_count(), 5);
    }
    #[test]
    fn sizing_plan_text_mentions_fallback_disabled() {
        let p = SizingPlan::new(
            AdaptiveSizingPolicy::default_local(),
            ParallelismPlan::new(ParallelismLimit::Auto, 1, 1, "x"),
        );
        assert!(p.to_human_text().contains("fallback execution: disabled"));
    }
    #[test]
    fn dynamic_feedback_reduces_target_for_memory_pressure() {
        let mut input = DynamicSizingFeedbackInput::new(AdaptiveSizingPolicy::default_local());
        input.add_signal(SizingFeedbackSignal::new(
            SizingFeedbackSignalKind::MemoryPressureHigh,
            "memory pressure crossed soft limit",
        ));
        let report = plan_dynamic_sizing_feedback(input);
        assert_eq!(report.status, DynamicSizingFeedbackStatus::TargetReduced);
        assert_eq!(
            report.recommended_target_task_bytes,
            ByteSize::from_mib(128)
        );
        assert!(report.is_side_effect_free());
        assert!(!report.feedback_applied);
    }
    #[test]
    fn dynamic_feedback_increases_target_for_small_tasks() {
        let mut input = DynamicSizingFeedbackInput::new(AdaptiveSizingPolicy::default_local());
        input.add_signal(SizingFeedbackSignal::new(
            SizingFeedbackSignalKind::TaskTooSmall,
            "scheduler overhead dominated useful work",
        ));
        let report = plan_dynamic_sizing_feedback(input);
        assert_eq!(report.status, DynamicSizingFeedbackStatus::TargetIncreased);
        assert_eq!(
            report.recommended_target_task_bytes,
            ByteSize::from_mib(512)
        );
        assert!(report.is_side_effect_free());
    }
    #[test]
    fn dynamic_feedback_no_signals_is_no_feedback() {
        let report = plan_dynamic_sizing_feedback(DynamicSizingFeedbackInput::new(
            AdaptiveSizingPolicy::default_local(),
        ));
        assert_eq!(report.status, DynamicSizingFeedbackStatus::NoFeedback);
        assert_eq!(
            report.current_target_task_bytes,
            report.recommended_target_task_bytes
        );
    }
    #[test]
    fn dynamic_feedback_mixed_signals_chooses_safer_smaller_target() {
        let mut input = DynamicSizingFeedbackInput::new(AdaptiveSizingPolicy::default_local());
        input.add_signal(SizingFeedbackSignal::new(
            SizingFeedbackSignalKind::TaskTooSmall,
            "small task overhead",
        ));
        input.add_signal(SizingFeedbackSignal::new(
            SizingFeedbackSignalKind::TaskTooLarge,
            "task exceeded memory budget",
        ));
        let report = plan_dynamic_sizing_feedback(input);
        assert_eq!(report.status, DynamicSizingFeedbackStatus::MixedSignals);
        assert_eq!(
            report.recommended_target_task_bytes,
            ByteSize::from_mib(128)
        );
    }

    #[test]
    fn dynamic_work_shaping_aggregates_feedback_and_backpressure() {
        let feedback = dynamic_feedback_with_signal(SizingFeedbackSignalKind::MemoryPressureHigh);
        let backpressure = bounded_backpressure();
        let report = plan_dynamic_work_shaping("memory-pressure", &feedback, &backpressure);

        assert_eq!(report.schema_version, "shardloom.dynamic_work_shaping.v1");
        assert_eq!(report.report_id, "cg8.dynamic_work_shaping.aggregate");
        assert_eq!(
            report.status,
            DynamicWorkShapingStatus::NeedsRuntimeIntegration
        );
        assert_eq!(
            report.feedback_status,
            DynamicSizingFeedbackStatus::TargetReduced
        );
        assert_eq!(
            report.backpressure_status,
            crate::streaming::BackpressurePlanStatus::Bounded
        );
        assert!(report.target_task_bytes_changed);
        assert!(report.bounded_backpressure);
        assert_eq!(report.max_parallelism, 4);
        assert_eq!(report.max_in_flight_chunks, Some(4));
        assert_eq!(report.max_buffered_bytes, Some(ByteSize::from_gib(8)));
        assert_eq!(report.estimated_chunk_bytes, Some(ByteSize::from_mib(256)));
        assert_eq!(
            report.blocked_surface_order,
            vec!["runtime_application_loop", "benchmark_evidence"]
        );
        assert_eq!(report.blocked_surface_count, 2);
        assert_eq!(report.planned_surface_count, 7);
        assert!(!report.runtime_feedback_loop_ready);
        assert!(!report.policy_application_ready);
        assert!(!report.benchmark_evidence_ready);
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn dynamic_work_shaping_preserves_no_fallback_boundary() {
        let feedback = dynamic_feedback_with_signal(SizingFeedbackSignalKind::ObjectStoreThrottled);
        let backpressure = bounded_backpressure();
        let report = plan_dynamic_work_shaping("object-store-throttled", &feedback, &backpressure);

        assert_eq!(
            DynamicWorkShapingReport::surface_order(),
            vec![
                "adaptive_sizing_policy",
                "feedback_signals",
                "target_task_policy",
                "backpressure_policy",
                "bounded_memory_policy",
                "scheduler_queue_policy",
                "runtime_application_loop",
                "benchmark_evidence",
                "no_fallback_policy"
            ]
        );
        assert!(!report.streams_executed);
        assert!(!report.tasks_executed);
        assert!(!report.feedback_applied);
        assert!(!report.policy_mutated);
        assert!(!report.data_read);
        assert!(!report.data_materialized);
        assert!(!report.object_store_io);
        assert!(!report.write_io);
        assert!(!report.spill_io_performed);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.fallback_attempted);
        assert!(
            report
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }

    #[test]
    fn dynamic_runtime_promotion_gate_tracks_runtime_surfaces() {
        let report = plan_dynamic_runtime_promotion_gate();
        assert_eq!(
            report.schema_version,
            "shardloom.dynamic_runtime_promotion_gate.v1"
        );
        assert_eq!(report.report_id, "cg8.dynamic_runtime_promotion_gate");
        assert_eq!(report.surface_count(), 8);
        assert_eq!(report.existing_limited_surface_count(), 2);
        assert_eq!(report.blocked_surface_count(), 6);
        assert_eq!(report.runtime_ready_surface_count(), 0);
        assert!(
            report.surface_order().contains(
                &DynamicRuntimePromotionSurface::DynamicSizingFeedbackApplication.as_str()
            )
        );
        assert!(
            report.surface_order().contains(
                &DynamicRuntimePromotionSurface::BoundedParallelEncodedReadRuntime.as_str()
            )
        );
        assert!(
            report
                .surface_order()
                .contains(&DynamicRuntimePromotionSurface::BenchmarkCertificateCloseout.as_str())
        );
    }

    #[test]
    fn dynamic_runtime_promotion_gate_blocks_runtime_and_claims() {
        let report = plan_dynamic_runtime_promotion_gate();
        assert!(report.existing_local_streaming_scan_evidence_present);
        assert!(report.existing_local_bounded_metadata_noop_evidence_present);
        assert!(report.existing_local_filter_project_bounded_scan_evidence_present);
        assert!(report.runtime_promotions_blocked());
        assert!(report.claim_blocked());
        assert!(report.side_effect_free());
        assert!(!report.has_errors());
        assert!(!report.dynamic_feedback_application_allowed);
        assert!(!report.bounded_parallel_encoded_read_allowed);
        assert!(!report.source_backed_parallel_reader_allowed);
        assert!(!report.scheduler_requeue_allowed);
        assert!(!report.bounded_backpressure_runtime_allowed);
        assert!(!report.memory_spill_reservation_runtime_allowed);
        assert!(!report.object_store_request_budget_runtime_allowed);
        assert!(!report.runtime_policy_mutation_allowed);
        assert!(!report.large_workload_claim_allowed);
        assert!(report.runtime_metrics_required);
        assert!(report.target_task_policy_required);
        assert!(report.scheduler_queue_policy_required);
        assert!(report.memory_reservation_evidence_required);
        assert!(report.spill_policy_evidence_required);
        assert!(report.backpressure_evidence_required);
        assert!(report.cancellation_retry_evidence_required);
        assert!(report.execution_certificate_required);
        assert!(report.native_io_certificate_required);
        assert!(report.benchmark_evidence_required);
        assert!(!report.runtime_execution_performed);
        assert!(!report.tasks_executed);
        assert!(!report.data_read);
        assert!(!report.data_materialized);
        assert!(!report.object_store_io);
        assert!(!report.write_io);
        assert!(!report.spill_io_performed);
        assert!(!report.feedback_applied);
        assert!(!report.policy_mutated);
        assert!(!report.fallback_execution_allowed);
        assert!(!report.fallback_attempted);
    }
}
