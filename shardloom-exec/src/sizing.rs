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

#[cfg(test)]
mod tests {
    use super::*;
    fn seg() -> SegmentId {
        SegmentId::new("s1").expect("valid")
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
}
