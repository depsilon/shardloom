use std::fmt::Write as _;

use shardloom_core::{
    Diagnostic, DiagnosticCode, DiagnosticSeverity, MaterializationPolicy, Result, SegmentId,
};
use shardloom_exec::{
    AdaptiveSizer, AdaptiveSizingPolicy, ParallelismPlan, SizeEstimate, SizingInput, SizingPlan,
    TaskId, TaskSizingDecisionKind, TaskSizingMode,
};

/// Planning-only adaptive sizing status for bridging `Vortex` read/runtime reports into `ShardLoom` sizing decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexAdaptiveSizingStatus {
    Planned,
    Sized,
    NeedsEstimate,
    NoTasksRequired,
    BlockedByMissingMetadata,
    Unsupported,
}
impl VortexAdaptiveSizingStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Sized => "sized",
            Self::NeedsEstimate => "needs_estimate",
            Self::NoTasksRequired => "no_tasks_required",
            Self::BlockedByMissingMetadata => "blocked_by_missing_metadata",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
    #[must_use]
    pub const fn requires_future_execution(&self) -> bool {
        matches!(self, Self::Sized)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexAdaptiveSizingMode {
    PlanOnly,
    MetadataOnly,
    MemoryAwareReadPlanning,
    Unsupported,
}
impl VortexAdaptiveSizingMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PlanOnly => "plan_only",
            Self::MetadataOnly => "metadata_only",
            Self::MemoryAwareReadPlanning => "memory_aware_read_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn executes_tasks(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexSizingEstimateSource {
    SegmentStats,
    ByteRangeIntent,
    ReadSplitDescriptor,
    RuntimeTaskMapping,
    Unknown,
}
impl VortexSizingEstimateSource {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::SegmentStats => "segment_stats",
            Self::ByteRangeIntent => "byte_range_intent",
            Self::ReadSplitDescriptor => "read_split_descriptor",
            Self::RuntimeTaskMapping => "runtime_task_mapping",
            Self::Unknown => "unknown",
        }
    }
    #[must_use]
    pub const fn is_metadata_based(&self) -> bool {
        !matches!(self, Self::Unknown)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexSegmentSizingInput {
    pub segment_id: Option<SegmentId>,
    pub split_id: Option<String>,
    pub task_id: Option<TaskId>,
    pub estimate_source: VortexSizingEstimateSource,
    pub size_estimate: SizeEstimate,
    pub has_byte_ranges: bool,
    pub can_use_metadata: bool,
    pub materialization_policy: MaterializationPolicy,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexSegmentSizingInput {
    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            segment_id: None,
            split_id: None,
            task_id: None,
            estimate_source: VortexSizingEstimateSource::Unknown,
            size_estimate: SizeEstimate::unknown(),
            has_byte_ranges: false,
            can_use_metadata: false,
            materialization_policy: MaterializationPolicy::Late,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn from_read_split(split: &crate::VortexReadSplitDescriptor) -> Self {
        let mut out = Self::unknown();
        out.segment_id.clone_from(&split.segment_id);
        out.split_id = Some(split.split_id.clone());
        out.has_byte_ranges = !split.byte_ranges.is_empty();
        out.materialization_policy = MaterializationPolicy::Late;
        out.can_use_metadata = true;
        out.estimate_source = if out.has_byte_ranges {
            VortexSizingEstimateSource::ByteRangeIntent
        } else {
            VortexSizingEstimateSource::ReadSplitDescriptor
        };
        out
    }
    #[must_use]
    pub fn from_task_mapping(mapping: &crate::VortexTaskMapping) -> Self {
        let mut out = Self::unknown();
        out.segment_id.clone_from(&mapping.segment_id);
        out.split_id.clone_from(&mapping.split_id);
        out.task_id.clone_from(&mapping.task_id);
        out.estimate_source = VortexSizingEstimateSource::RuntimeTaskMapping;
        out.can_use_metadata = true;
        if let Some(task) = &mapping.task {
            out.materialization_policy = task.materialization.clone();
            out.has_byte_ranges = !task.byte_ranges.is_empty();
            out.size_estimate.selected_column_count = Some(task.required_columns.len());
        }
        out
    }
    #[must_use]
    pub fn with_segment_id(mut self, segment_id: SegmentId) -> Self {
        self.segment_id = Some(segment_id);
        self
    }
    #[must_use]
    pub fn with_task_id(mut self, task_id: TaskId) -> Self {
        self.task_id = Some(task_id);
        self
    }
    #[must_use]
    pub fn with_size_estimate(mut self, estimate: SizeEstimate) -> Self {
        self.size_estimate = estimate;
        self
    }
    #[must_use]
    pub fn with_byte_ranges(mut self, value: bool) -> Self {
        self.has_byte_ranges = value;
        self
    }
    #[must_use]
    pub fn with_metadata_available(mut self, value: bool) -> Self {
        self.can_use_metadata = value;
        self
    }
    #[must_use]
    pub fn with_materialization_policy(mut self, policy: MaterializationPolicy) -> Self {
        self.materialization_policy = policy;
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    /// # Errors
    /// Returns errors from underlying `SizingInput` construction when `SegmentId` is invalid.
    pub fn to_sizing_input(&self) -> Result<Option<SizingInput>> {
        let Some(seg) = self.segment_id.clone() else {
            return Ok(None);
        };
        Ok(Some(
            SizingInput::new(seg, self.size_estimate.clone())
                .with_byte_ranges(self.has_byte_ranges)
                .with_metadata_available(self.can_use_metadata)
                .with_materialization_policy(self.materialization_policy.clone()),
        ))
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
            "segment={} source={} metadata={} byte_ranges={}",
            self.segment_id
                .as_ref()
                .map_or("<unknown>", SegmentId::as_str),
            self.estimate_source.as_str(),
            self.can_use_metadata,
            self.has_byte_ranges
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexAdaptiveSizingInput {
    pub runtime_bridge_report: Option<crate::VortexRuntimeBridgeReport>,
    pub read_planning_report: Option<crate::VortexReadPlanningReport>,
    pub policy: AdaptiveSizingPolicy,
    pub available_threads: usize,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexAdaptiveSizingInput {
    #[must_use]
    pub fn new(policy: AdaptiveSizingPolicy) -> Self {
        Self {
            runtime_bridge_report: None,
            read_planning_report: None,
            policy,
            available_threads: 1,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn from_runtime_bridge_report(
        report: crate::VortexRuntimeBridgeReport,
        policy: AdaptiveSizingPolicy,
    ) -> Self {
        Self {
            runtime_bridge_report: Some(report),
            ..Self::new(policy)
        }
    }
    #[must_use]
    pub fn from_read_planning_report(
        report: crate::VortexReadPlanningReport,
        policy: AdaptiveSizingPolicy,
    ) -> Self {
        Self {
            read_planning_report: Some(report),
            ..Self::new(policy)
        }
    }
    #[must_use]
    pub fn with_available_threads(mut self, available_threads: usize) -> Self {
        self.available_threads = available_threads.max(1);
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
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "policy={} available_threads={}",
            self.policy.summary(),
            self.available_threads
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexAdaptiveSizingReport {
    pub status: VortexAdaptiveSizingStatus,
    pub mode: VortexAdaptiveSizingMode,
    pub input: VortexAdaptiveSizingInput,
    pub segment_inputs: Vec<VortexSegmentSizingInput>,
    pub sizing_plan: SizingPlan,
    pub planned_task_count: usize,
    pub split_decision_count: usize,
    pub coalesce_candidate_count: usize,
    pub needs_estimate_count: usize,
    pub keep_single_count: usize,
    pub metadata_only_count: usize,
    pub data_executed: bool,
    pub data_read: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexAdaptiveSizingReport {
    /// # Errors
    /// Returns errors propagated from sizing input conversion.
    pub fn from_input(input: VortexAdaptiveSizingInput) -> Result<Self> {
        let sizer = AdaptiveSizer::new(input.policy.clone());
        let mut segs = vec![];
        let mut bridged_runtime_has_errors = false;
        let mut bridged_runtime_status_unsupported = false;
        let mut bridged_runtime_diagnostics: Vec<Diagnostic> = vec![];
        if let Some(runtime) = &input.runtime_bridge_report {
            bridged_runtime_has_errors = runtime.has_errors();
            bridged_runtime_status_unsupported =
                runtime.status == crate::VortexRuntimeBridgeStatus::Unsupported;
            bridged_runtime_diagnostics.clone_from(&runtime.diagnostics);
            for m in &runtime.mappings {
                segs.push(VortexSegmentSizingInput::from_task_mapping(m));
            }
        } else if let Some(read) = &input.read_planning_report {
            for s in &read.split_descriptors {
                segs.push(VortexSegmentSizingInput::from_read_split(s));
            }
            if segs.is_empty() {
                for i in &read.segment_intents {
                    segs.push(
                        VortexSegmentSizingInput::unknown()
                            .with_materialization_policy(i.materialization.clone())
                            .with_metadata_available(true),
                    );
                }
            }
        }
        let mut plan = SizingPlan::new(
            input.policy.clone(),
            ParallelismPlan::new(
                input.policy.max_parallelism,
                input.available_threads,
                segs.len(),
                "planning only",
            ),
        );
        for seg in &segs {
            if let Some(sinput) = seg.to_sizing_input()? {
                let decision = sizer.decide_for_segment(&sinput);
                plan.add_decision(sinput.segment_id.clone(), decision);
            }
        }
        let mut out = Self {
            status: if segs.is_empty() {
                VortexAdaptiveSizingStatus::NoTasksRequired
            } else {
                VortexAdaptiveSizingStatus::Planned
            },
            mode: if input.policy.mode == TaskSizingMode::MetadataOnly {
                VortexAdaptiveSizingMode::MetadataOnly
            } else {
                VortexAdaptiveSizingMode::MemoryAwareReadPlanning
            },
            input,
            segment_inputs: segs,
            sizing_plan: plan,
            planned_task_count: 0,
            split_decision_count: 0,
            coalesce_candidate_count: 0,
            needs_estimate_count: 0,
            keep_single_count: 0,
            metadata_only_count: 0,
            data_executed: false,
            data_read: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        };
        out.diagnostics.extend(bridged_runtime_diagnostics);
        out.recompute_counts();
        if bridged_runtime_status_unsupported || bridged_runtime_has_errors {
            out.status = VortexAdaptiveSizingStatus::Unsupported;
        } else if out.needs_estimate_count > 0 {
            out.status = VortexAdaptiveSizingStatus::NeedsEstimate;
        } else if out.planned_task_count > 0 {
            out.status = VortexAdaptiveSizingStatus::Sized;
        }
        Ok(out)
    }
    /// # Errors
    /// Returns errors propagated from `from_input`.
    pub fn from_runtime_bridge_report(
        report: crate::VortexRuntimeBridgeReport,
        policy: AdaptiveSizingPolicy,
    ) -> Result<Self> {
        Self::from_input(VortexAdaptiveSizingInput::from_runtime_bridge_report(
            report, policy,
        ))
    }
    pub fn unsupported(
        input: VortexAdaptiveSizingInput,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut out = Self {
            status: VortexAdaptiveSizingStatus::Unsupported,
            mode: VortexAdaptiveSizingMode::Unsupported,
            sizing_plan: SizingPlan::new(
                input.policy.clone(),
                ParallelismPlan::new(
                    input.policy.max_parallelism,
                    input.available_threads,
                    0,
                    "unsupported",
                ),
            ),
            input,
            segment_inputs: vec![],
            planned_task_count: 0,
            split_decision_count: 0,
            coalesce_candidate_count: 0,
            needs_estimate_count: 0,
            keep_single_count: 0,
            metadata_only_count: 0,
            data_executed: false,
            data_read: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        };
        out.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback attempted: false".to_string()),
        ));
        out
    }
    pub fn add_segment_input(&mut self, input: VortexSegmentSizingInput) {
        self.segment_inputs.push(input);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn recompute_counts(&mut self) {
        self.planned_task_count = self.sizing_plan.planned_task_count();
        self.split_decision_count = self
            .sizing_plan
            .decisions
            .iter()
            .filter(|(_, d)| d.requires_split())
            .count();
        self.coalesce_candidate_count = self
            .sizing_plan
            .decisions
            .iter()
            .filter(|(_, d)| d.kind == TaskSizingDecisionKind::CoalesceCandidate)
            .count();
        self.needs_estimate_count = self
            .sizing_plan
            .decisions
            .iter()
            .filter(|(_, d)| d.kind == TaskSizingDecisionKind::NeedsEstimate)
            .count();
        self.keep_single_count = self
            .sizing_plan
            .decisions
            .iter()
            .filter(|(_, d)| d.kind == TaskSizingDecisionKind::KeepSingle)
            .count();
        self.metadata_only_count = self
            .sizing_plan
            .decisions
            .iter()
            .filter(|(_, d)| d.kind == TaskSizingDecisionKind::MetadataOnly)
            .count();
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.input.has_errors()
            || self
                .segment_inputs
                .iter()
                .any(VortexSegmentSizingInput::has_errors)
            || self.sizing_plan.has_errors()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.data_executed
            && !self.data_read
            && !self.data_materialized
            && !self.object_store_io
            && !self.write_io
            && !self.external_effects_executed
            && !self.fallback_execution_allowed
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = write!(
            out,
            "adaptive sizing status: {}\nmode: {}\npolicy: {}\nplanned task count: {}\nsplit decision count: {}\ncoalesce candidate count: {}\nneeds estimate count: {}\nkeep single count: {}\nmetadata only count: {}\ndata executed: false\ndata read: false\ndata materialized: false\nobject-store IO: false\nwrite IO: false\nexternal effects executed: false\nfallback execution disabled",
            self.status.as_str(),
            self.mode.as_str(),
            self.input.policy.summary(),
            self.planned_task_count,
            self.split_decision_count,
            self.coalesce_candidate_count,
            self.needs_estimate_count,
            self.keep_single_count,
            self.metadata_only_count
        );
        if !self.diagnostics.is_empty() {
            out.push_str("\ndiagnostics:");
            for d in &self.diagnostics {
                let _ = write!(out, "\n- {}", d.to_human_text());
            }
        }
        out
    }
}

/// # Errors
/// Returns errors propagated from report construction.
pub fn size_vortex_runtime_task_graph(
    report: crate::VortexRuntimeBridgeReport,
    policy: AdaptiveSizingPolicy,
) -> Result<VortexAdaptiveSizingReport> {
    VortexAdaptiveSizingReport::from_runtime_bridge_report(report, policy)
}
#[must_use]
pub fn vortex_adaptive_sizing_is_side_effect_free(report: &VortexAdaptiveSizingReport) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::FallbackStatus;
    #[test]
    fn status_unsupported_is_error() {
        assert!(VortexAdaptiveSizingStatus::Unsupported.is_error());
    }
    #[test]
    fn status_sized_future_exec() {
        assert!(VortexAdaptiveSizingStatus::Sized.requires_future_execution());
    }
    #[test]
    fn mode_does_not_execute() {
        assert!(!VortexAdaptiveSizingMode::MemoryAwareReadPlanning.executes_tasks());
    }
    #[test]
    fn src_byte_range_metadata() {
        assert!(VortexSizingEstimateSource::ByteRangeIntent.is_metadata_based());
    }
    #[test]
    fn src_unknown_not_metadata() {
        assert!(!VortexSizingEstimateSource::Unknown.is_metadata_based());
    }
    #[test]
    fn unknown_estimate_is_unknown() {
        assert_eq!(
            VortexSegmentSizingInput::unknown().size_estimate,
            SizeEstimate::unknown()
        );
    }
    #[test]
    fn to_sizing_none_without_segment() {
        assert!(
            VortexSegmentSizingInput::unknown()
                .to_sizing_input()
                .expect("ok")
                .is_none()
        );
    }
    #[test]
    fn to_sizing_some_with_segment() {
        let s = SegmentId::new("s1").expect("v");
        assert!(
            VortexSegmentSizingInput::unknown()
                .with_segment_id(s)
                .to_sizing_input()
                .expect("ok")
                .is_some()
        );
    }
    #[test]
    fn input_defaults_threads_one() {
        assert_eq!(
            VortexAdaptiveSizingInput::new(AdaptiveSizingPolicy::default_local()).available_threads,
            1
        );
    }
    #[test]
    fn unsupported_has_errors_no_fallback() {
        let r = VortexAdaptiveSizingReport::unsupported(
            VortexAdaptiveSizingInput::new(AdaptiveSizingPolicy::default_local()),
            "f",
            "r",
        );
        assert!(r.has_errors());
        assert!(!r.fallback_execution_allowed);
    }
    #[test]
    fn empty_is_side_effect_free() {
        let r = VortexAdaptiveSizingReport::from_input(VortexAdaptiveSizingInput::new(
            AdaptiveSizingPolicy::default_local(),
        ))
        .expect("ok");
        assert!(r.is_side_effect_free());
    }
    #[test]
    fn unknown_estimate_produces_needs_estimate() {
        let seg = SegmentId::new("s1").expect("v");
        let mut r = VortexAdaptiveSizingReport::from_input(VortexAdaptiveSizingInput::new(
            AdaptiveSizingPolicy::default_local(),
        ))
        .expect("ok");
        r.segment_inputs
            .push(VortexSegmentSizingInput::unknown().with_segment_id(seg.clone()));
        r.sizing_plan.add_decision(
            seg,
            shardloom_exec::TaskSizingDecision::needs_estimate("unknown"),
        );
        r.recompute_counts();
        assert!(r.needs_estimate_count > 0);
    }
    #[test]
    fn human_text_fields() {
        let mut r = VortexAdaptiveSizingReport::from_input(VortexAdaptiveSizingInput::new(
            AdaptiveSizingPolicy::default_local(),
        ))
        .expect("ok");
        r.add_diagnostic(Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Info,
            shardloom_core::DiagnosticCategory::Planning,
            "d",
            None,
            None,
            None,
            FallbackStatus::disabled_by_policy(),
        ));
        let t = r.to_human_text();
        assert!(
            t.contains("fallback execution disabled")
                && t.contains("data read: false")
                && t.contains("data materialized: false")
                && t.contains("diagnostics:")
        );
    }

    #[test]
    fn runtime_bridge_unsupported_is_preserved() {
        let input_plan = crate::plan_native_vortex_universal_input(
            shardloom_core::UniversalInputSource::from_dataset_uri(
                shardloom_core::DatasetUri::new("file://tmp/test.vortex").expect("uri"),
            )
            .expect("source"),
        )
        .expect("input");
        let read_report = crate::plan_vortex_read_from_universal_input(input_plan).expect("read");
        let runtime = crate::VortexRuntimeBridgeReport::unsupported(
            crate::VortexRuntimeBridgeInput::new(read_report),
            "feature",
            "reason",
        );
        let out = VortexAdaptiveSizingReport::from_runtime_bridge_report(
            runtime,
            AdaptiveSizingPolicy::default_local(),
        )
        .expect("sizing");
        assert_eq!(out.status, VortexAdaptiveSizingStatus::Unsupported);
        assert!(out.has_errors());
        assert!(!out.diagnostics.is_empty());
    }
}
