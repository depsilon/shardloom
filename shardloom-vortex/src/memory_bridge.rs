use std::fmt::Write as _;

use shardloom_core::{Diagnostic, DiagnosticCode, DiagnosticSeverity, Result, SegmentId};
use shardloom_exec::{
    ByteSize, MemoryBudget, MemoryPoolPlan, OomSafetyPlan, SpillDecision, SpillPlan, SpillPolicy,
    TaskId, TaskSizingDecisionKind,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMemoryBridgeStatus {
    Planned,
    MemorySafe,
    NeedsEstimate,
    SpillMayBeRequired,
    SpillRequiredButNotImplemented,
    BlockedByMemoryPolicy,
    NoTasksRequired,
    Unsupported,
}
impl VortexMemoryBridgeStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::MemorySafe => "memory_safe",
            Self::NeedsEstimate => "needs_estimate",
            Self::SpillMayBeRequired => "spill_may_be_required",
            Self::SpillRequiredButNotImplemented => "spill_required_but_not_implemented",
            Self::BlockedByMemoryPolicy => "blocked_by_memory_policy",
            Self::NoTasksRequired => "no_tasks_required",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::Unsupported | Self::SpillRequiredButNotImplemented | Self::BlockedByMemoryPolicy
        )
    }
    #[must_use]
    pub const fn requires_future_action(&self) -> bool {
        matches!(
            self,
            Self::NeedsEstimate
                | Self::SpillMayBeRequired
                | Self::SpillRequiredButNotImplemented
                | Self::BlockedByMemoryPolicy
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMemoryBridgeMode {
    PlanOnly,
    MemoryBudgetPlanning,
    SpillPolicyPlanning,
    OomSafetyPlanning,
    Unsupported,
}
impl VortexMemoryBridgeMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PlanOnly => "plan_only",
            Self::MemoryBudgetPlanning => "memory_budget_planning",
            Self::SpillPolicyPlanning => "spill_policy_planning",
            Self::OomSafetyPlanning => "oom_safety_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn executes_memory_actions(&self) -> bool {
        false
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexTaskMemoryClass {
    MetadataOnly,
    EncodedRead,
    PartialDecode,
    UnknownEstimate,
    Unsupported,
}
impl VortexTaskMemoryClass {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::EncodedRead => "encoded_read",
            Self::PartialDecode => "partial_decode",
            Self::UnknownEstimate => "unknown_estimate",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn may_materialize(&self) -> bool {
        matches!(
            self,
            Self::PartialDecode | Self::UnknownEstimate | Self::Unsupported
        )
    }
    #[must_use]
    pub const fn may_need_spill(&self) -> bool {
        !matches!(self, Self::MetadataOnly)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexTaskMemoryDecisionKind {
    NoMemoryActionNeeded,
    ReserveMemoryPlanned,
    NeedsEstimate,
    ReduceParallelism,
    SpillMayBeRequired,
    SpillRequiredButNotImplemented,
    Unsupported,
}
impl VortexTaskMemoryDecisionKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NoMemoryActionNeeded => "no_memory_action_needed",
            Self::ReserveMemoryPlanned => "reserve_memory_planned",
            Self::NeedsEstimate => "needs_estimate",
            Self::ReduceParallelism => "reduce_parallelism",
            Self::SpillMayBeRequired => "spill_may_be_required",
            Self::SpillRequiredButNotImplemented => "spill_required_but_not_implemented",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn requires_action(&self) -> bool {
        !matches!(
            self,
            Self::NoMemoryActionNeeded | Self::ReserveMemoryPlanned
        )
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct VortexTaskMemoryDecision {
    pub kind: VortexTaskMemoryDecisionKind,
    pub memory_class: VortexTaskMemoryClass,
    pub task_id: Option<TaskId>,
    pub segment_id: Option<SegmentId>,
    pub estimated_bytes: Option<ByteSize>,
    pub spill_policy: SpillPolicy,
    pub spill_decision: SpillDecision,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexTaskMemoryDecision {
    fn base(
        kind: VortexTaskMemoryDecisionKind,
        task_id: Option<TaskId>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            memory_class: VortexTaskMemoryClass::UnknownEstimate,
            task_id,
            segment_id: None,
            estimated_bytes: None,
            spill_policy: SpillPolicy::BestEffort,
            spill_decision: SpillDecision::keep_in_memory(reason),
            diagnostics: vec![],
        }
    }
    pub fn no_action(reason: impl Into<String>) -> Self {
        Self::base(
            VortexTaskMemoryDecisionKind::NoMemoryActionNeeded,
            None,
            reason,
        )
        .with_memory_class(VortexTaskMemoryClass::MetadataOnly)
    }
    pub fn reserve_memory_planned(
        task_id: Option<TaskId>,
        estimated_bytes: ByteSize,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::base(
            VortexTaskMemoryDecisionKind::ReserveMemoryPlanned,
            task_id,
            reason,
        );
        s.estimated_bytes = Some(estimated_bytes);
        s.with_memory_class(VortexTaskMemoryClass::EncodedRead)
    }
    pub fn needs_estimate(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(VortexTaskMemoryDecisionKind::NeedsEstimate, task_id, reason)
    }
    pub fn spill_may_be_required(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        let mut s = Self::base(
            VortexTaskMemoryDecisionKind::SpillMayBeRequired,
            task_id,
            "spill may be required",
        );
        s.spill_decision = SpillDecision::spill_later(reason);
        s.with_memory_class(VortexTaskMemoryClass::PartialDecode)
    }
    pub fn spill_required_not_implemented(
        task_id: Option<TaskId>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::base(
            VortexTaskMemoryDecisionKind::SpillRequiredButNotImplemented,
            task_id,
            reason.into(),
        );
        s.spill_policy = SpillPolicy::Required;
        s.spill_decision = SpillDecision::fail_before_oom("spill required but not implemented");
        s.diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            "spill",
            "spill required but unavailable",
            Some("Fallback attempted: false".to_string()),
        ));
        s
    }
    pub fn unsupported(
        task_id: Option<TaskId>,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::base(
            VortexTaskMemoryDecisionKind::Unsupported,
            task_id,
            "unsupported",
        );
        s.memory_class = VortexTaskMemoryClass::Unsupported;
        s.spill_decision = SpillDecision::unsupported("unsupported planning path");
        s.diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback attempted: false".to_string()),
        ));
        s
    }
    #[must_use]
    pub fn with_segment_id(mut self, segment_id: SegmentId) -> Self {
        self.segment_id = Some(segment_id);
        self
    }
    #[must_use]
    pub fn with_memory_class(mut self, memory_class: VortexTaskMemoryClass) -> Self {
        self.memory_class = memory_class;
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    #[must_use]
    pub const fn requires_action(&self) -> bool {
        self.kind.requires_action()
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
            "kind={} memory_class={} planning-only execution=not_performed",
            self.kind.as_str(),
            self.memory_class.as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexMemoryBridgeInput {
    pub adaptive_sizing_report: Option<crate::VortexAdaptiveSizingReport>,
    pub runtime_bridge_report: Option<crate::VortexRuntimeBridgeReport>,
    pub memory_budget: MemoryBudget,
    pub spill_policy: SpillPolicy,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexMemoryBridgeInput {
    #[must_use]
    pub fn new(memory_budget: MemoryBudget) -> Self {
        Self {
            adaptive_sizing_report: None,
            runtime_bridge_report: None,
            memory_budget,
            spill_policy: SpillPolicy::BestEffort,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn from_adaptive_sizing_report(
        report: crate::VortexAdaptiveSizingReport,
        memory_budget: MemoryBudget,
    ) -> Self {
        let mut s = Self::new(memory_budget);
        s.adaptive_sizing_report = Some(report);
        s
    }
    #[must_use]
    pub fn from_runtime_bridge_report(
        report: crate::VortexRuntimeBridgeReport,
        memory_budget: MemoryBudget,
    ) -> Self {
        let mut s = Self::new(memory_budget);
        s.runtime_bridge_report = Some(report);
        s
    }
    #[must_use]
    pub fn with_spill_policy(mut self, policy: SpillPolicy) -> Self {
        self.spill_policy = policy;
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
            "budget=[{}] spill_policy={}",
            self.memory_budget.summary(),
            self.spill_policy.as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexPlanOnlyIoFlags {
    pub data_executed: bool,
    pub data_read: bool,
    pub data_materialized: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexPlanOnlyEffectFlags {
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexPlanOnlyExecutionPolicyFlags {
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexMemoryBridgeReport {
    pub status: VortexMemoryBridgeStatus,
    pub mode: VortexMemoryBridgeMode,
    pub input: VortexMemoryBridgeInput,
    pub memory_pool_plan: MemoryPoolPlan,
    pub oom_safety_plan: OomSafetyPlan,
    pub spill_plans: Vec<SpillPlan>,
    pub task_decisions: Vec<VortexTaskMemoryDecision>,
    pub tasks_considered: usize,
    pub tasks_needing_estimate: usize,
    pub tasks_memory_safe: usize,
    pub tasks_spill_may_be_required: usize,
    pub tasks_spill_required_not_implemented: usize,
    pub io_flags: VortexPlanOnlyIoFlags,
    pub effect_flags: VortexPlanOnlyEffectFlags,
    pub execution_policy_flags: VortexPlanOnlyExecutionPolicyFlags,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexMemoryBridgeReport {
    #[must_use]
    fn default_io_flags() -> VortexPlanOnlyIoFlags {
        VortexPlanOnlyIoFlags {
            data_executed: false,
            data_read: false,
            data_materialized: false,
        }
    }
    #[must_use]
    fn default_effect_flags() -> VortexPlanOnlyEffectFlags {
        VortexPlanOnlyEffectFlags {
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
        }
    }
    #[must_use]
    fn default_execution_policy_flags() -> VortexPlanOnlyExecutionPolicyFlags {
        VortexPlanOnlyExecutionPolicyFlags {
            external_effects_executed: false,
            fallback_execution_allowed: false,
        }
    }
    #[must_use]
    fn computed_status(&self) -> VortexMemoryBridgeStatus {
        if self.tasks_considered == 0 {
            VortexMemoryBridgeStatus::NoTasksRequired
        } else if self.tasks_spill_required_not_implemented > 0 {
            VortexMemoryBridgeStatus::SpillRequiredButNotImplemented
        } else if self.tasks_spill_may_be_required > 0 {
            VortexMemoryBridgeStatus::SpillMayBeRequired
        } else if self.tasks_needing_estimate > 0 {
            VortexMemoryBridgeStatus::NeedsEstimate
        } else {
            VortexMemoryBridgeStatus::MemorySafe
        }
    }
    /// # Errors
    /// Returns an error when constructing `MemoryBudget`-derived planning state fails.
    pub fn from_input(input: VortexMemoryBridgeInput) -> Result<Self> {
        let mp = MemoryPoolPlan::new(input.memory_budget.clone());
        let oop = OomSafetyPlan::new(mp.clone());
        let mut out = Self {
            status: VortexMemoryBridgeStatus::Planned,
            mode: VortexMemoryBridgeMode::PlanOnly,
            input,
            memory_pool_plan: mp,
            oom_safety_plan: oop,
            spill_plans: vec![],
            task_decisions: vec![],
            tasks_considered: 0,
            tasks_needing_estimate: 0,
            tasks_memory_safe: 0,
            tasks_spill_may_be_required: 0,
            tasks_spill_required_not_implemented: 0,
            io_flags: Self::default_io_flags(),
            effect_flags: Self::default_effect_flags(),
            execution_policy_flags: Self::default_execution_policy_flags(),
            diagnostics: vec![],
        };
        if let Some(r) = &out.input.adaptive_sizing_report {
            out.diagnostics.extend(r.diagnostics.clone());
            if r.has_errors() || r.status == crate::VortexAdaptiveSizingStatus::Unsupported {
                out.status = VortexMemoryBridgeStatus::Unsupported;
            }
            let decisions = r.sizing_plan.decisions.clone();
            for (seg, d) in &decisions {
                let decision = match d.kind {
                    TaskSizingDecisionKind::NeedsEstimate => {
                        VortexTaskMemoryDecision::needs_estimate(None, "missing size estimate")
                            .with_segment_id(seg.clone())
                    }
                    _ => VortexTaskMemoryDecision::needs_estimate(
                        None,
                        format!("unavailable explicit byte estimate: {}", d.reason),
                    )
                    .with_segment_id(seg.clone()),
                };
                out.add_task_decision(decision);
            }
        }
        if let Some(r) = &out.input.runtime_bridge_report {
            out.diagnostics.extend(r.diagnostics.clone());
            if r.has_errors() || r.status == crate::VortexRuntimeBridgeStatus::Unsupported {
                out.status = VortexMemoryBridgeStatus::Unsupported;
            }
            if out.input.adaptive_sizing_report.is_none() {
                let mappings = r.mappings.clone();
                for mapping in &mappings {
                    let decision = match mapping.kind {
                        crate::VortexTaskMappingKind::NoTaskNeeded
                        | crate::VortexTaskMappingKind::MetadataTask => {
                            VortexTaskMemoryDecision::no_action("runtime mapping is metadata-only")
                        }
                        crate::VortexTaskMappingKind::PartialDecodeTask => {
                            VortexTaskMemoryDecision::spill_may_be_required(
                                mapping.task_id.clone(),
                                "partial decode mapping may require spill planning",
                            )
                            .with_memory_class(VortexTaskMemoryClass::PartialDecode)
                        }
                        crate::VortexTaskMappingKind::SegmentScanTask
                        | crate::VortexTaskMappingKind::EncodedEvaluateTask => {
                            VortexTaskMemoryDecision::needs_estimate(
                                mapping.task_id.clone(),
                                "runtime bridge mapping does not provide explicit bytes",
                            )
                            .with_memory_class(VortexTaskMemoryClass::EncodedRead)
                        }
                        crate::VortexTaskMappingKind::Unsupported => {
                            VortexTaskMemoryDecision::unsupported(
                                mapping.task_id.clone(),
                                "runtime-bridge-mapping",
                                "unsupported runtime mapping for memory planning",
                            )
                        }
                    };
                    let decision = if let Some(seg) = &mapping.segment_id {
                        decision.with_segment_id(seg.clone())
                    } else {
                        decision
                    };
                    out.add_task_decision(decision);
                }
            }
        }
        out.recompute_counts();
        if out.status != VortexMemoryBridgeStatus::Unsupported {
            out.status = out.computed_status();
        }
        Ok(out)
    }
    /// # Errors
    /// Returns errors propagated from `from_input`.
    pub fn from_adaptive_sizing_report(
        report: crate::VortexAdaptiveSizingReport,
        memory_budget: MemoryBudget,
    ) -> Result<Self> {
        Self::from_input(VortexMemoryBridgeInput::from_adaptive_sizing_report(
            report,
            memory_budget,
        ))
    }
    pub fn unsupported(
        input: VortexMemoryBridgeInput,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self {
            status: VortexMemoryBridgeStatus::Unsupported,
            mode: VortexMemoryBridgeMode::Unsupported,
            memory_pool_plan: MemoryPoolPlan::new(input.memory_budget.clone()),
            oom_safety_plan: OomSafetyPlan::new(MemoryPoolPlan::new(input.memory_budget.clone())),
            input,
            spill_plans: vec![],
            task_decisions: vec![],
            tasks_considered: 0,
            tasks_needing_estimate: 0,
            tasks_memory_safe: 0,
            tasks_spill_may_be_required: 0,
            tasks_spill_required_not_implemented: 0,
            io_flags: Self::default_io_flags(),
            effect_flags: Self::default_effect_flags(),
            execution_policy_flags: Self::default_execution_policy_flags(),
            diagnostics: vec![],
        };
        s.diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback attempted: false".to_string()),
        ));
        s
    }
    pub fn add_task_decision(&mut self, decision: VortexTaskMemoryDecision) {
        self.task_decisions.push(decision);
    }
    pub fn add_spill_plan(&mut self, spill_plan: SpillPlan) {
        self.spill_plans.push(spill_plan);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn recompute_counts(&mut self) {
        self.tasks_considered = self.task_decisions.len();
        self.tasks_needing_estimate = self
            .task_decisions
            .iter()
            .filter(|d| d.kind == VortexTaskMemoryDecisionKind::NeedsEstimate)
            .count();
        self.tasks_spill_may_be_required = self
            .task_decisions
            .iter()
            .filter(|d| d.kind == VortexTaskMemoryDecisionKind::SpillMayBeRequired)
            .count();
        self.tasks_spill_required_not_implemented = self
            .task_decisions
            .iter()
            .filter(|d| d.kind == VortexTaskMemoryDecisionKind::SpillRequiredButNotImplemented)
            .count();
        self.tasks_memory_safe = self
            .task_decisions
            .iter()
            .filter(|d| {
                matches!(
                    d.kind,
                    VortexTaskMemoryDecisionKind::NoMemoryActionNeeded
                        | VortexTaskMemoryDecisionKind::ReserveMemoryPlanned
                )
            })
            .count();
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.input.has_errors()
            || self.memory_pool_plan.has_errors()
            || self.oom_safety_plan.has_errors()
            || self.spill_plans.iter().any(SpillPlan::has_errors)
            || self
                .task_decisions
                .iter()
                .any(VortexTaskMemoryDecision::has_errors)
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.io_flags.data_executed
            && !self.io_flags.data_read
            && !self.io_flags.data_materialized
            && !self.effect_flags.object_store_io
            && !self.effect_flags.write_io
            && !self.effect_flags.spill_io_performed
            && !self.execution_policy_flags.external_effects_executed
            && !self.execution_policy_flags.fallback_execution_allowed
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = write!(
            out,
            "memory bridge status: {}\nmode: {}\nmemory budget: {}\nspill policy: {}\ntasks considered: {}\ntasks needing estimate: {}\ntasks memory safe: {}\ntasks spill may be required: {}\ntasks spill required but not implemented: {}\ndata executed: false\ndata read: false\ndata materialized: false\nobject-store IO: false\nwrite IO: false\nspill IO performed: false\nexternal effects executed: false\nfallback execution disabled",
            self.status.as_str(),
            self.mode.as_str(),
            self.input.memory_budget.summary(),
            self.input.spill_policy.as_str(),
            self.tasks_considered,
            self.tasks_needing_estimate,
            self.tasks_memory_safe,
            self.tasks_spill_may_be_required,
            self.tasks_spill_required_not_implemented
        );
        if !self.diagnostics.is_empty() {
            out.push_str("\ndiagnostics:");
            for d in &self.diagnostics {
                let _ = write!(out, "\n- {}", d.to_human_text());
                if let Some(feature) = &d.feature {
                    let _ = write!(out, " feature={feature}");
                }
                if let Some(reason) = &d.reason {
                    let _ = write!(out, " reason={reason}");
                }
                if let Some(next_step) = &d.suggested_next_step {
                    let _ = write!(out, " next_step={next_step}");
                }
            }
        }
        out
    }
}

/// # Errors
/// Returns errors propagated from `VortexMemoryBridgeReport::from_adaptive_sizing_report`.
pub fn plan_vortex_memory_safety(
    report: crate::VortexAdaptiveSizingReport,
    memory_budget: MemoryBudget,
) -> Result<VortexMemoryBridgeReport> {
    VortexMemoryBridgeReport::from_adaptive_sizing_report(report, memory_budget)
}
#[must_use]
pub fn vortex_memory_bridge_is_side_effect_free(report: &VortexMemoryBridgeReport) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_exec::AdaptiveSizingPolicy;

    #[test]
    fn adaptive_non_needs_estimate_stays_needs_estimate_without_bytes() {
        let input_plan = crate::plan_native_vortex_universal_input(
            shardloom_core::UniversalInputSource::from_dataset_uri(
                shardloom_core::DatasetUri::new("file://tmp/test.vortex").expect("uri"),
            )
            .expect("source"),
        )
        .expect("input");
        let read = crate::plan_vortex_read_from_universal_input(input_plan).expect("read");
        let runtime = crate::build_vortex_runtime_task_graph(read).expect("runtime");
        let sizing =
            crate::size_vortex_runtime_task_graph(runtime, AdaptiveSizingPolicy::default_local())
                .expect("sizing");
        let report = VortexMemoryBridgeReport::from_adaptive_sizing_report(
            sizing,
            MemoryBudget::from_gib(4).expect("budget"),
        )
        .expect("bridge");
        assert!(
            report.tasks_needing_estimate
                >= report
                    .tasks_considered
                    .saturating_sub(report.tasks_memory_safe)
        );
    }

    #[test]
    fn runtime_only_input_creates_decisions_when_mappings_exist() {
        let input_plan = crate::plan_native_vortex_universal_input(
            shardloom_core::UniversalInputSource::from_dataset_uri(
                shardloom_core::DatasetUri::new("file://tmp/test.vortex").expect("uri"),
            )
            .expect("source"),
        )
        .expect("input");
        let read = crate::plan_vortex_read_from_universal_input(input_plan).expect("read");
        let runtime = crate::build_vortex_runtime_task_graph(read).expect("runtime");
        let report = VortexMemoryBridgeReport::from_input(
            VortexMemoryBridgeInput::from_runtime_bridge_report(
                runtime,
                MemoryBudget::from_gib(4).expect("budget"),
            ),
        )
        .expect("bridge");
        assert!(
            report.tasks_considered > 0
                || report.status == VortexMemoryBridgeStatus::NoTasksRequired
        );
    }
}
