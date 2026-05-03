use std::fmt::Write as _;

use shardloom_core::{
    Diagnostic, DiagnosticCode, DiagnosticSeverity, Result, SegmentId, ShardLoomError,
};
use shardloom_exec::TaskId;

use crate::{
    VortexMemoryBridgeReport, VortexTaskMemoryClass, VortexTaskMemoryDecision,
    VortexTaskMemoryDecisionKind,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexSchedulerBridgeStatus {
    Planned,
    Ready,
    MetadataOnly,
    NeedsEstimate,
    BlockedByMemoryPolicy,
    SpillRequiredButNotImplemented,
    NoTasksRequired,
    Unsupported,
}
impl VortexSchedulerBridgeStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Ready => "ready",
            Self::MetadataOnly => "metadata_only",
            Self::NeedsEstimate => "needs_estimate",
            Self::BlockedByMemoryPolicy => "blocked_by_memory_policy",
            Self::SpillRequiredButNotImplemented => "spill_required_but_not_implemented",
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
                | Self::BlockedByMemoryPolicy
                | Self::SpillRequiredButNotImplemented
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexSchedulerBridgeMode {
    PlanOnly,
    QueuePlanning,
    BatchPlanning,
    Unsupported,
}
impl VortexSchedulerBridgeMode {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PlanOnly => "plan_only",
            Self::QueuePlanning => "queue_planning",
            Self::BatchPlanning => "batch_planning",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn executes_tasks(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexTaskQueueClass {
    Ready,
    MetadataOnly,
    NeedsEstimate,
    WaitingForMemory,
    SpillBlocked,
    Unsupported,
}
impl VortexTaskQueueClass {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::MetadataOnly => "metadata_only",
            Self::NeedsEstimate => "needs_estimate",
            Self::WaitingForMemory => "waiting_for_memory",
            Self::SpillBlocked => "spill_blocked",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_blocked(&self) -> bool {
        matches!(
            self,
            Self::NeedsEstimate | Self::WaitingForMemory | Self::SpillBlocked | Self::Unsupported
        )
    }
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::SpillBlocked | Self::Unsupported)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexSchedulingDecisionKind {
    ScheduleNow,
    ScheduleMetadataOnly,
    HoldForEstimate,
    HoldForMemory,
    HoldForSpillSupport,
    SkipPruned,
    Unsupported,
}
impl VortexSchedulingDecisionKind {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ScheduleNow => "schedule_now",
            Self::ScheduleMetadataOnly => "schedule_metadata_only",
            Self::HoldForEstimate => "hold_for_estimate",
            Self::HoldForMemory => "hold_for_memory",
            Self::HoldForSpillSupport => "hold_for_spill_support",
            Self::SkipPruned => "skip_pruned",
            Self::Unsupported => "unsupported",
        }
    }
    #[must_use]
    pub const fn is_scheduled(&self) -> bool {
        matches!(self, Self::ScheduleNow | Self::ScheduleMetadataOnly)
    }
    #[must_use]
    pub const fn is_blocked(&self) -> bool {
        matches!(
            self,
            Self::HoldForEstimate
                | Self::HoldForMemory
                | Self::HoldForSpillSupport
                | Self::Unsupported
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexTaskSchedulingDecision {
    pub kind: VortexSchedulingDecisionKind,
    pub queue_class: VortexTaskQueueClass,
    pub task_id: Option<TaskId>,
    pub segment_id: Option<SegmentId>,
    pub batch_id: Option<String>,
    pub reason: String,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexTaskSchedulingDecision {
    fn base(
        kind: VortexSchedulingDecisionKind,
        queue_class: VortexTaskQueueClass,
        task_id: Option<TaskId>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            queue_class,
            task_id,
            segment_id: None,
            batch_id: None,
            reason: reason.into(),
            diagnostics: vec![],
        }
    }
    pub fn schedule_now(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexSchedulingDecisionKind::ScheduleNow,
            VortexTaskQueueClass::Ready,
            task_id,
            reason,
        )
    }
    pub fn schedule_metadata_only(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexSchedulingDecisionKind::ScheduleMetadataOnly,
            VortexTaskQueueClass::MetadataOnly,
            task_id,
            reason,
        )
    }
    pub fn hold_for_estimate(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexSchedulingDecisionKind::HoldForEstimate,
            VortexTaskQueueClass::NeedsEstimate,
            task_id,
            reason,
        )
    }
    pub fn hold_for_memory(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexSchedulingDecisionKind::HoldForMemory,
            VortexTaskQueueClass::WaitingForMemory,
            task_id,
            reason,
        )
    }
    pub fn hold_for_spill_support(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexSchedulingDecisionKind::HoldForSpillSupport,
            VortexTaskQueueClass::SpillBlocked,
            task_id,
            reason,
        )
    }
    pub fn skip_pruned(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexSchedulingDecisionKind::SkipPruned,
            VortexTaskQueueClass::MetadataOnly,
            task_id,
            reason,
        )
    }
    pub fn unsupported(
        task_id: Option<TaskId>,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::base(
            VortexSchedulingDecisionKind::Unsupported,
            VortexTaskQueueClass::Unsupported,
            task_id,
            "unsupported scheduling path",
        );
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
    pub fn with_batch_id(mut self, batch_id: impl Into<String>) -> Self {
        self.batch_id = Some(batch_id.into());
        self
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    #[must_use]
    pub const fn is_scheduled(&self) -> bool {
        self.kind.is_scheduled()
    }
    #[must_use]
    pub const fn is_blocked(&self) -> bool {
        self.kind.is_blocked()
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.queue_class.is_error()
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
            "scheduling-decision kind={} queue={} plan_only=true tasks_executed=false",
            self.kind.as_str(),
            self.queue_class.as_str()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexTaskBatchPlan {
    pub batch_id: String,
    pub decisions: Vec<VortexTaskSchedulingDecision>,
    pub max_parallelism: usize,
    pub estimated_memory_bytes: Option<u64>,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexTaskBatchPlan {
    /// # Errors
    /// Returns error if batch id is empty or parallelism is zero.
    pub fn new(batch_id: impl Into<String>, max_parallelism: usize) -> Result<Self> {
        let id = batch_id.into();
        if id.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "batch_id cannot be empty".to_string(),
            ));
        }
        if max_parallelism == 0 {
            return Err(ShardLoomError::InvalidOperation(
                "max_parallelism must be > 0".to_string(),
            ));
        }
        Ok(Self {
            batch_id: id,
            decisions: vec![],
            max_parallelism,
            estimated_memory_bytes: None,
            diagnostics: vec![],
        })
    }
    pub fn add_decision(&mut self, d: VortexTaskSchedulingDecision) {
        self.decisions.push(d);
    }
    #[must_use]
    pub fn scheduled_count(&self) -> usize {
        self.decisions.iter().filter(|d| d.is_scheduled()).count()
    }
    #[must_use]
    pub fn blocked_count(&self) -> usize {
        self.decisions.iter().filter(|d| d.is_blocked()).count()
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        }) || self
            .decisions
            .iter()
            .any(VortexTaskSchedulingDecision::has_errors)
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "batch={} decisions={} max_parallelism={} execution=not_performed",
            self.batch_id,
            self.decisions.len(),
            self.max_parallelism
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexSchedulerBridgeInput {
    pub memory_bridge_report: VortexMemoryBridgeReport,
    pub max_parallelism: usize,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexSchedulerBridgeInput {
    #[must_use]
    pub fn new(memory_bridge_report: VortexMemoryBridgeReport) -> Self {
        Self {
            memory_bridge_report,
            max_parallelism: 1,
            diagnostics: vec![],
        }
    }
    #[must_use]
    pub fn with_max_parallelism(mut self, max_parallelism: usize) -> Self {
        if max_parallelism == 0 {
            self.max_parallelism = 1;
            self.diagnostics.push(Diagnostic::invalid_input(
                "max_parallelism",
                "value 0 is invalid",
                "use a value greater than zero",
            ));
        } else {
            self.max_parallelism = max_parallelism;
        }
        self
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        }) || self.memory_bridge_report.has_errors()
    }
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "scheduler-input max_parallelism={} planning_only=true",
            self.max_parallelism
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexSchedulerBridgeReport {
    pub status: VortexSchedulerBridgeStatus,
    pub mode: VortexSchedulerBridgeMode,
    pub input: VortexSchedulerBridgeInput,
    pub decisions: Vec<VortexTaskSchedulingDecision>,
    pub batches: Vec<VortexTaskBatchPlan>,
    pub scheduled_task_count: usize,
    pub metadata_only_task_count: usize,
    pub blocked_task_count: usize,
    pub unsupported_task_count: usize,
    pub tasks_executed: bool,
    pub data_executed: bool,
    pub data_read: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexSchedulerBridgeReport {
    /// # Errors
    /// Returns error when batch construction fails.
    pub fn from_input(input: VortexSchedulerBridgeInput) -> Result<Self> {
        let mut out = Self {
            status: VortexSchedulerBridgeStatus::Planned,
            mode: VortexSchedulerBridgeMode::QueuePlanning,
            input,
            decisions: vec![],
            batches: vec![],
            scheduled_task_count: 0,
            metadata_only_task_count: 0,
            blocked_task_count: 0,
            unsupported_task_count: 0,
            tasks_executed: false,
            data_executed: false,
            data_read: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        };
        out.diagnostics
            .extend(out.input.memory_bridge_report.diagnostics.clone());
        let memory_decisions = out.input.memory_bridge_report.task_decisions.clone();
        for m in &memory_decisions {
            out.add_decision(map_memory_decision(m));
        }
        let sched: Vec<_> = out
            .decisions
            .iter()
            .filter(|d| d.is_scheduled())
            .cloned()
            .collect();
        for (batch_ix, chunk) in sched.chunks(out.input.max_parallelism).enumerate() {
            let mut batch =
                VortexTaskBatchPlan::new(format!("batch-{batch_ix}"), out.input.max_parallelism)?;
            for d in chunk {
                batch.add_decision(d.clone().with_batch_id(format!("batch-{batch_ix}")));
            }
            out.add_batch(batch);
        }
        out.recompute_counts();
        out.status = derive_status(&out);
        Ok(out)
    }
    /// # Errors
    /// Returns error propagated from `from_input`.
    pub fn from_memory_bridge_report(
        report: VortexMemoryBridgeReport,
        max_parallelism: usize,
    ) -> Result<Self> {
        Self::from_input(
            VortexSchedulerBridgeInput::new(report).with_max_parallelism(max_parallelism),
        )
    }
    #[must_use]
    pub fn unsupported(
        input: VortexSchedulerBridgeInput,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::from_input(input).unwrap_or_else(|_| Self::empty_unsupported());
        s.status = VortexSchedulerBridgeStatus::Unsupported;
        s.mode = VortexSchedulerBridgeMode::Unsupported;
        s.diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback attempted: false".to_string()),
        ));
        s
    }
    fn empty_unsupported() -> Self {
        Self {
            status: VortexSchedulerBridgeStatus::Unsupported,
            mode: VortexSchedulerBridgeMode::Unsupported,
            input: VortexSchedulerBridgeInput::new(
                VortexMemoryBridgeReport::from_input(crate::VortexMemoryBridgeInput::new(
                    shardloom_exec::MemoryBudget::from_gib(1).expect("valid default memory budget"),
                ))
                .expect("valid empty memory bridge report"),
            ),
            decisions: vec![],
            batches: vec![],
            scheduled_task_count: 0,
            metadata_only_task_count: 0,
            blocked_task_count: 0,
            unsupported_task_count: 0,
            tasks_executed: false,
            data_executed: false,
            data_read: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        }
    }
    pub fn add_decision(&mut self, d: VortexTaskSchedulingDecision) {
        self.decisions.push(d);
    }
    pub fn add_batch(&mut self, b: VortexTaskBatchPlan) {
        self.batches.push(b);
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    pub fn recompute_counts(&mut self) {
        self.scheduled_task_count = self
            .decisions
            .iter()
            .filter(|d| matches!(d.kind, VortexSchedulingDecisionKind::ScheduleNow))
            .count();
        self.metadata_only_task_count = self
            .decisions
            .iter()
            .filter(|d| matches!(d.kind, VortexSchedulingDecisionKind::ScheduleMetadataOnly))
            .count();
        self.blocked_task_count = self.decisions.iter().filter(|d| d.is_blocked()).count();
        self.unsupported_task_count = self
            .decisions
            .iter()
            .filter(|d| matches!(d.kind, VortexSchedulingDecisionKind::Unsupported))
            .count();
    }
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.unsupported_task_count > 0
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
            || self
                .decisions
                .iter()
                .any(VortexTaskSchedulingDecision::has_errors)
    }
    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.tasks_executed
            && !self.data_executed
            && !self.data_read
            && !self.data_materialized
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_effects_executed
            && !self.fallback_execution_allowed
    }
    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "Vortex scheduler queue planning report");
        let _ = writeln!(out, "status: {}", self.status.as_str());
        let _ = writeln!(out, "mode: {}", self.mode.as_str());
        let _ = writeln!(
            out,
            "fallback execution allowed: {}",
            self.fallback_execution_allowed
        );
        let _ = writeln!(out, "tasks executed: {}", self.tasks_executed);
        let _ = writeln!(out, "data read: {}", self.data_read);
        let _ = writeln!(out, "spill IO performed: {}", self.spill_io_performed);
        let _ = writeln!(out, "scheduled tasks: {}", self.scheduled_task_count);
        if self.diagnostics.is_empty() {
            let _ = write!(out, "diagnostics: none");
        } else {
            let _ = writeln!(out, "diagnostics:");
            for d in &self.diagnostics {
                let _ = writeln!(out, "- {} [{}]", d.message, d.severity.as_str());
            }
        }
        out
    }
}

fn map_memory_decision(m: &VortexTaskMemoryDecision) -> VortexTaskSchedulingDecision {
    let mut out = match m.kind {
        VortexTaskMemoryDecisionKind::NoMemoryActionNeeded => match m.memory_class {
            VortexTaskMemoryClass::MetadataOnly => {
                VortexTaskSchedulingDecision::schedule_metadata_only(
                    m.task_id.clone(),
                    "no memory action needed metadata only",
                )
            }
            _ => VortexTaskSchedulingDecision::schedule_now(
                m.task_id.clone(),
                "no memory action needed",
            ),
        },
        VortexTaskMemoryDecisionKind::ReserveMemoryPlanned => {
            VortexTaskSchedulingDecision::schedule_now(
                m.task_id.clone(),
                "memory reservation planned",
            )
        }
        VortexTaskMemoryDecisionKind::NeedsEstimate => {
            VortexTaskSchedulingDecision::hold_for_estimate(m.task_id.clone(), "needs estimate")
        }
        VortexTaskMemoryDecisionKind::ReduceParallelism => {
            VortexTaskSchedulingDecision::hold_for_memory(
                m.task_id.clone(),
                "reduce parallelism required",
            )
        }
        VortexTaskMemoryDecisionKind::SpillMayBeRequired
        | VortexTaskMemoryDecisionKind::SpillRequiredButNotImplemented => {
            VortexTaskSchedulingDecision::hold_for_spill_support(
                m.task_id.clone(),
                "spill support required",
            )
        }
        VortexTaskMemoryDecisionKind::Unsupported => VortexTaskSchedulingDecision::unsupported(
            m.task_id.clone(),
            "scheduler planning",
            "unsupported memory decision",
        ),
    };
    out.segment_id.clone_from(&m.segment_id);
    out.diagnostics.extend(m.diagnostics.clone());
    out
}

fn derive_status(out: &VortexSchedulerBridgeReport) -> VortexSchedulerBridgeStatus {
    if out.has_errors() || out.unsupported_task_count > 0 {
        return VortexSchedulerBridgeStatus::Unsupported;
    }
    if out.decisions.is_empty() {
        return VortexSchedulerBridgeStatus::NoTasksRequired;
    }
    let has_spill = out
        .decisions
        .iter()
        .any(|d| matches!(d.kind, VortexSchedulingDecisionKind::HoldForSpillSupport));
    if has_spill {
        return VortexSchedulerBridgeStatus::SpillRequiredButNotImplemented;
    }
    if out.scheduled_task_count == 0
        && out.metadata_only_task_count > 0
        && out.blocked_task_count == 0
    {
        return VortexSchedulerBridgeStatus::MetadataOnly;
    }
    if out.scheduled_task_count > 0 {
        return VortexSchedulerBridgeStatus::Ready;
    }
    if out
        .decisions
        .iter()
        .any(|d| matches!(d.kind, VortexSchedulingDecisionKind::HoldForEstimate))
    {
        return VortexSchedulerBridgeStatus::NeedsEstimate;
    }
    if out.blocked_task_count > 0 {
        return VortexSchedulerBridgeStatus::BlockedByMemoryPolicy;
    }
    VortexSchedulerBridgeStatus::Planned
}

/// # Errors
/// Returns errors propagated from `VortexSchedulerBridgeReport::from_memory_bridge_report`.
pub fn plan_vortex_scheduler_queue(
    report: VortexMemoryBridgeReport,
    max_parallelism: usize,
) -> Result<VortexSchedulerBridgeReport> {
    VortexSchedulerBridgeReport::from_memory_bridge_report(report, max_parallelism)
}

#[must_use]
pub fn vortex_scheduler_bridge_is_side_effect_free(report: &VortexSchedulerBridgeReport) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_exec::ByteSize;

    fn empty_memory_report() -> VortexMemoryBridgeReport {
        VortexMemoryBridgeReport::from_input(crate::VortexMemoryBridgeInput::new(
            shardloom_exec::MemoryBudget::from_gib(1).expect("valid test memory budget"),
        ))
        .expect("empty memory bridge report")
    }

    fn sample_report(kind: VortexTaskMemoryDecisionKind) -> VortexMemoryBridgeReport {
        let mut r = empty_memory_report();
        r.task_decisions = vec![match kind {
            VortexTaskMemoryDecisionKind::NeedsEstimate => {
                VortexTaskMemoryDecision::needs_estimate(None, "x")
            }
            VortexTaskMemoryDecisionKind::ReserveMemoryPlanned => {
                VortexTaskMemoryDecision::reserve_memory_planned(
                    None,
                    ByteSize::from_bytes(10),
                    "x",
                )
            }
            _ => VortexTaskMemoryDecision::no_action("x"),
        }];
        r
    }
    #[test]
    fn status_unsupported_error() {
        assert!(VortexSchedulerBridgeStatus::Unsupported.is_error());
    }
    #[test]
    fn status_needs_estimate_action() {
        assert!(VortexSchedulerBridgeStatus::NeedsEstimate.requires_future_action());
    }
    #[test]
    fn status_ready_no_action() {
        assert!(!VortexSchedulerBridgeStatus::Ready.requires_future_action());
    }
    #[test]
    fn mode_queue_no_exec() {
        assert!(!VortexSchedulerBridgeMode::QueuePlanning.executes_tasks());
    }
    #[test]
    fn queue_needs_estimate_blocked() {
        assert!(VortexTaskQueueClass::NeedsEstimate.is_blocked());
    }
    #[test]
    fn queue_unsupported_error() {
        assert!(VortexTaskQueueClass::Unsupported.is_error());
    }
    #[test]
    fn kind_schedule_now_scheduled() {
        assert!(VortexSchedulingDecisionKind::ScheduleNow.is_scheduled());
    }
    #[test]
    fn kind_hold_estimate_blocked() {
        assert!(VortexSchedulingDecisionKind::HoldForEstimate.is_blocked());
    }
    #[test]
    fn decision_unsupported_has_error() {
        let d = VortexTaskSchedulingDecision::unsupported(None, "x", "y");
        assert!(d.has_errors());
        assert!(d.diagnostics.iter().any(|x| {
            x.suggested_next_step
                .as_deref()
                .unwrap_or_default()
                .contains("Fallback attempted: false")
        }));
    }
    #[test]
    fn batch_rejects_empty() {
        assert!(VortexTaskBatchPlan::new("", 1).is_err());
    }
    #[test]
    fn batch_rejects_zero_parallel() {
        assert!(VortexTaskBatchPlan::new("b", 0).is_err());
    }
    #[test]
    fn batch_counts() {
        let mut b = VortexTaskBatchPlan::new("b", 1).expect("ok");
        b.add_decision(VortexTaskSchedulingDecision::schedule_now(None, "x"));
        b.add_decision(VortexTaskSchedulingDecision::hold_for_estimate(None, "x"));
        assert_eq!(b.scheduled_count(), 1);
        assert_eq!(b.blocked_count(), 1);
    }
    #[test]
    fn input_default_parallelism() {
        let r = VortexSchedulerBridgeInput::new(empty_memory_report());
        assert_eq!(r.max_parallelism, 1);
    }
    #[test]
    fn report_unsupported_has_error() {
        let i = VortexSchedulerBridgeInput::new(empty_memory_report());
        let r = VortexSchedulerBridgeReport::unsupported(i, "x", "y");
        assert!(r.has_errors());
        assert!(!r.fallback_execution_allowed);
    }
    #[test]
    fn from_input_empty_side_effect_free() {
        let r = VortexSchedulerBridgeReport::from_input(VortexSchedulerBridgeInput::new(
            empty_memory_report(),
        ))
        .expect("ok");
        assert!(r.is_side_effect_free());
    }
    #[test]
    fn from_input_needs_estimate_blocked() {
        let r = VortexSchedulerBridgeReport::from_input(VortexSchedulerBridgeInput::new(
            sample_report(VortexTaskMemoryDecisionKind::NeedsEstimate),
        ))
        .expect("ok");
        assert_eq!(r.blocked_task_count, 1);
    }
    #[test]
    fn from_input_reserve_scheduled() {
        let r = VortexSchedulerBridgeReport::from_input(VortexSchedulerBridgeInput::new(
            sample_report(VortexTaskMemoryDecisionKind::ReserveMemoryPlanned),
        ))
        .expect("ok");
        assert_eq!(r.scheduled_task_count, 1);
    }
    #[test]
    fn from_input_batches_max_size() {
        let mut m = empty_memory_report();
        m.task_decisions = vec![
            VortexTaskMemoryDecision::reserve_memory_planned(None, ByteSize::from_bytes(1), "x"),
            VortexTaskMemoryDecision::reserve_memory_planned(None, ByteSize::from_bytes(2), "x"),
            VortexTaskMemoryDecision::reserve_memory_planned(None, ByteSize::from_bytes(3), "x"),
        ];
        let r = VortexSchedulerBridgeReport::from_input(
            VortexSchedulerBridgeInput::new(m).with_max_parallelism(2),
        )
        .expect("ok");
        assert!(r.batches.iter().all(|b| b.decisions.len() <= 2));
    }
    #[test]
    fn recompute_counts_updates() {
        let mut r = VortexSchedulerBridgeReport::from_input(VortexSchedulerBridgeInput::new(
            sample_report(VortexTaskMemoryDecisionKind::ReserveMemoryPlanned),
        ))
        .expect("ok");
        r.decisions
            .push(VortexTaskSchedulingDecision::hold_for_estimate(None, "x"));
        r.recompute_counts();
        assert_eq!(r.blocked_task_count, 1);
    }
    #[test]
    fn side_effect_free_true() {
        let r = VortexSchedulerBridgeReport::from_input(VortexSchedulerBridgeInput::new(
            empty_memory_report(),
        ))
        .expect("ok");
        assert!(vortex_scheduler_bridge_is_side_effect_free(&r));
    }
    #[test]
    fn human_text_flags() {
        let mut r = VortexSchedulerBridgeReport::from_input(VortexSchedulerBridgeInput::new(
            empty_memory_report(),
        ))
        .expect("ok");
        r.add_diagnostic(Diagnostic::invalid_input("x", "invalid", "fix"));
        let t = r.to_human_text();
        assert!(t.contains("fallback execution allowed: false"));
        assert!(t.contains("tasks executed: false"));
        assert!(t.contains("data read: false"));
        assert!(t.contains("spill IO performed: false"));
        assert!(t.contains("diagnostics:"));
    }
    #[test]
    fn plan_queue_no_io() {
        let r = plan_vortex_scheduler_queue(empty_memory_report(), 1).expect("ok");
        assert!(r.is_side_effect_free());
    }
}
