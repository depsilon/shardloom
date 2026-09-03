use std::{fmt::Write as _, sync::Arc, time::Instant};

use shardloom_core::{
    Diagnostic, DiagnosticCode, DiagnosticSeverity, Result, SegmentId, ShardLoomError,
};
use shardloom_exec::TaskId;

use crate::{
    VortexMemoryBridgeReport, VortexTaskMemoryClass, VortexTaskMemoryDecision,
    VortexTaskMemoryDecisionKind,
};

pub const VORTEX_MORSEL_SCHEDULER_SCHEMA_VERSION: &str = "shardloom.vortex_morsel_scheduler.v1";

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
        out.diagnostics.extend(out.input.diagnostics.clone());
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
    let has_diagnostic_errors = out.diagnostics.iter().any(|d| {
        matches!(
            d.severity,
            DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
        )
    }) || out
        .decisions
        .iter()
        .any(VortexTaskSchedulingDecision::has_errors);
    if has_diagnostic_errors {
        return VortexSchedulerBridgeStatus::Unsupported;
    }
    if out.unsupported_task_count > 0 {
        return VortexSchedulerBridgeStatus::Unsupported;
    }
    let has_spill = out
        .decisions
        .iter()
        .any(|d| matches!(d.kind, VortexSchedulingDecisionKind::HoldForSpillSupport));
    if has_spill {
        return VortexSchedulerBridgeStatus::SpillRequiredButNotImplemented;
    }
    if out.decisions.is_empty() {
        return VortexSchedulerBridgeStatus::NoTasksRequired;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMorselSchedulerStatus {
    Planned,
    Executed,
    MetadataOnlyPruned,
    BlockedByMemoryPolicy,
    BlockedByWorkerFailure,
    Unsupported,
}

impl VortexMorselSchedulerStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Executed => "executed",
            Self::MetadataOnlyPruned => "metadata_only_pruned",
            Self::BlockedByMemoryPolicy => "blocked_by_memory_policy",
            Self::BlockedByWorkerFailure => "blocked_by_worker_failure",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_error(self) -> bool {
        matches!(
            self,
            Self::BlockedByMemoryPolicy | Self::BlockedByWorkerFailure | Self::Unsupported
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexMorselWorkUnit {
    pub work_id: String,
    pub family: String,
    pub segment_ref: String,
    pub start_row: u64,
    pub row_count: u64,
    pub estimated_bytes: u64,
    pub pruned_by_metadata: bool,
}

impl VortexMorselWorkUnit {
    /// # Errors
    /// Returns an error when family or segment ref is empty.
    pub fn new(
        family: impl Into<String>,
        segment_ref: impl Into<String>,
        start_row: u64,
        row_count: u64,
        estimated_bytes: u64,
    ) -> Result<Self> {
        let family = family.into();
        let segment_ref = segment_ref.into();
        if family.trim().is_empty() || segment_ref.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "morsel work unit requires family and segment ref".to_string(),
            ));
        }
        Ok(Self {
            work_id: format!("{family}:{segment_ref}:{start_row}:{row_count}"),
            family,
            segment_ref,
            start_row,
            row_count,
            estimated_bytes,
            pruned_by_metadata: false,
        })
    }

    #[must_use]
    pub fn pruned_by_metadata(mut self, value: bool) -> Self {
        self.pruned_by_metadata = value;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexMorselSchedulerPolicy {
    pub max_parallelism: usize,
    pub queue_limit: usize,
    pub worker_memory_budget_bytes: u64,
}

impl VortexMorselSchedulerPolicy {
    /// # Errors
    /// Returns an error when max parallelism is zero.
    pub fn new(max_parallelism: usize) -> Result<Self> {
        if max_parallelism == 0 {
            return Err(ShardLoomError::InvalidOperation(
                "morsel scheduler max_parallelism must be greater than zero".to_string(),
            ));
        }
        Ok(Self {
            max_parallelism,
            queue_limit: max_parallelism.saturating_mul(2).max(1),
            worker_memory_budget_bytes: 64 * 1024 * 1024,
        })
    }

    #[must_use]
    pub fn with_queue_limit(mut self, queue_limit: usize) -> Self {
        self.queue_limit = queue_limit.max(1);
        self
    }

    #[must_use]
    pub const fn with_worker_memory_budget_bytes(mut self, bytes: u64) -> Self {
        self.worker_memory_budget_bytes = bytes;
        self
    }

    #[must_use]
    pub fn applied_workers(&self, runnable_morsels: usize) -> usize {
        self.max_parallelism.min(runnable_morsels.max(1))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexMorselSchedulerPlan {
    pub schema_version: &'static str,
    pub scheduler_id: String,
    pub family: String,
    pub policy: VortexMorselSchedulerPolicy,
    pub work_units: Vec<VortexMorselWorkUnit>,
}

impl VortexMorselSchedulerPlan {
    /// # Errors
    /// Returns an error when the scheduler family is empty.
    pub fn new(
        family: impl Into<String>,
        work_units: Vec<VortexMorselWorkUnit>,
        policy: VortexMorselSchedulerPolicy,
    ) -> Result<Self> {
        let family = family.into();
        if family.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "morsel scheduler family must not be empty".to_string(),
            ));
        }
        Ok(Self {
            schema_version: VORTEX_MORSEL_SCHEDULER_SCHEMA_VERSION,
            scheduler_id: format!("vortex.morsel-scheduler.{family}"),
            family,
            policy,
            work_units,
        })
    }

    #[must_use]
    pub fn runnable_work_units(&self) -> Vec<VortexMorselWorkUnit> {
        self.work_units
            .iter()
            .filter(|work| !work.pruned_by_metadata)
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn runnable_morsel_count(&self) -> usize {
        self.work_units
            .iter()
            .filter(|work| !work.pruned_by_metadata)
            .count()
    }

    #[must_use]
    pub fn pruned_morsel_count(&self) -> usize {
        self.work_units
            .iter()
            .filter(|work| work.pruned_by_metadata)
            .count()
    }

    #[must_use]
    pub fn queue_limit_enforced(&self) -> bool {
        self.policy.queue_limit >= self.policy.applied_workers(self.runnable_morsel_count())
    }

    #[must_use]
    pub fn queue_wave_count(&self) -> usize {
        let runnable = self.runnable_morsel_count();
        if runnable == 0 {
            0
        } else {
            runnable.div_ceil(self.policy.queue_limit.max(1))
        }
    }

    #[must_use]
    pub fn memory_envelope_admitted(&self) -> bool {
        self.work_units.iter().all(|work| {
            work.pruned_by_metadata
                || work.estimated_bytes <= self.policy.worker_memory_budget_bytes
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexMorselWorkerUtilization {
    pub worker_index: usize,
    pub assigned_morsels: usize,
    pub completed_morsels: usize,
    pub rows_processed: u64,
    pub estimated_bytes_processed: u64,
    pub stage_micros: u128,
}

impl VortexMorselWorkerUtilization {
    #[must_use]
    pub fn summary(&self) -> String {
        format!(
            "worker={}:assigned={}:completed={}:rows={}:bytes={}:micros={}",
            self.worker_index,
            self.assigned_morsels,
            self.completed_morsels,
            self.rows_processed,
            self.estimated_bytes_processed,
            self.stage_micros
        )
    }
}

pub trait VortexMorselThreadLocalState: Send + 'static {
    fn new(worker_index: usize) -> Self
    where
        Self: Sized;
    /// # Errors
    /// Returns an error when the state cannot consume the morsel without
    /// violating its exactness, overflow, or resource contract.
    fn observe_morsel(&mut self, work: &VortexMorselWorkUnit) -> Result<()>;
    /// # Errors
    /// Returns an error when merging the worker-local state would violate the
    /// state's deterministic merge, overflow, or exactness contract.
    fn merge_from(&mut self, other: Self) -> Result<()>;
    fn evidence_summary(&self) -> String;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexMorselRowCountState {
    pub worker_index: usize,
    pub row_count: u64,
    pub observed_work_ids: Vec<String>,
}

impl VortexMorselThreadLocalState for VortexMorselRowCountState {
    fn new(worker_index: usize) -> Self {
        Self {
            worker_index,
            row_count: 0,
            observed_work_ids: Vec::new(),
        }
    }

    fn observe_morsel(&mut self, work: &VortexMorselWorkUnit) -> Result<()> {
        self.row_count = self.row_count.checked_add(work.row_count).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "morsel row-count state overflowed; fallback execution was not attempted"
                    .to_string(),
            )
        })?;
        self.observed_work_ids.push(work.work_id.clone());
        Ok(())
    }

    fn merge_from(&mut self, other: Self) -> Result<()> {
        self.row_count = self.row_count.checked_add(other.row_count).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "morsel row-count merge overflowed; fallback execution was not attempted"
                    .to_string(),
            )
        })?;
        self.observed_work_ids.extend(other.observed_work_ids);
        self.observed_work_ids.sort();
        Ok(())
    }

    fn evidence_summary(&self) -> String {
        format!(
            "row_count={}:work_ids={}",
            self.row_count,
            self.observed_work_ids.join(",")
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexMorselSchedulerExecutionReport {
    pub schema_version: &'static str,
    pub scheduler_id: String,
    pub family: String,
    pub status: VortexMorselSchedulerStatus,
    pub requested_workers: usize,
    pub applied_workers: usize,
    pub queue_limit: usize,
    pub queue_wave_count: usize,
    pub queue_limit_enforced: bool,
    pub runnable_morsels: usize,
    pub pruned_morsels: usize,
    pub completed_morsels: usize,
    pub failed_morsels: usize,
    pub thread_local_state_count: usize,
    pub deterministic_merge_count: usize,
    pub rows_processed: u64,
    pub worker_summaries: Vec<String>,
    pub mean_stage_micros: u128,
    pub max_stage_micros: u128,
    pub skew_ratio_x1000: u128,
    pub merge_micros: u128,
    pub rows_per_second: u64,
    pub memory_envelope_admitted: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub external_engine_invoked: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexMorselSchedulerExecutionReport {
    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.failed_morsels > 0
            || self.fallback_attempted
            || self.fallback_execution_allowed
            || self.external_engine_invoked
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn evidence_fields(&self, prefix: &str) -> Vec<(String, String)> {
        vec![
            (
                format!("{prefix}_morsel_scheduler_schema_version"),
                self.schema_version.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_id"),
                self.scheduler_id.clone(),
            ),
            (
                format!("{prefix}_morsel_scheduler_family"),
                self.family.clone(),
            ),
            (
                format!("{prefix}_morsel_scheduler_status"),
                self.status.as_str().to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_requested_workers"),
                self.requested_workers.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_applied_workers"),
                self.applied_workers.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_queue_limit"),
                self.queue_limit.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_queue_wave_count"),
                self.queue_wave_count.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_queue_limit_enforced"),
                self.queue_limit_enforced.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_runnable_morsels"),
                self.runnable_morsels.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_pruned_morsels"),
                self.pruned_morsels.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_completed_morsels"),
                self.completed_morsels.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_failed_morsels"),
                self.failed_morsels.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_thread_local_state_count"),
                self.thread_local_state_count.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_deterministic_merge_count"),
                self.deterministic_merge_count.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_rows_processed"),
                self.rows_processed.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_worker_summaries"),
                self.worker_summaries.join("|"),
            ),
            (
                format!("{prefix}_morsel_scheduler_mean_stage_micros"),
                self.mean_stage_micros.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_max_stage_micros"),
                self.max_stage_micros.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_skew_ratio_x1000"),
                self.skew_ratio_x1000.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_merge_micros"),
                self.merge_micros.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_rows_per_second"),
                self.rows_per_second.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_memory_envelope_admitted"),
                self.memory_envelope_admitted.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_fallback_attempted"),
                self.fallback_attempted.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_external_engine_invoked"),
                self.external_engine_invoked.to_string(),
            ),
            (
                format!("{prefix}_morsel_scheduler_fallback_execution_allowed"),
                self.fallback_execution_allowed.to_string(),
            ),
        ]
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexMorselSchedulerRun<S> {
    pub report: VortexMorselSchedulerExecutionReport,
    pub merged_state: S,
}

/// Executes a bounded local morsel schedule with deterministic thread-local merge.
///
/// The scheduler uses deterministic worker assignment (`morsel_index % workers`)
/// and merges worker states in worker-index order. It does not read source data
/// itself; callers attach source/segment-specific morsels and state update
/// logic.
///
/// # Errors
/// Returns an error when worker state update, join, or deterministic merge fails.
pub fn execute_vortex_morsel_scheduler_with_state<S>(
    plan: VortexMorselSchedulerPlan,
) -> Result<VortexMorselSchedulerRun<S>>
where
    S: VortexMorselThreadLocalState,
{
    execute_vortex_morsel_scheduler_with_observer(plan, |state: &mut S, work| {
        state.observe_morsel(work)
    })
}

/// Executes a bounded local morsel schedule with caller-supplied worker logic.
///
/// The observer runs once per runnable morsel against the worker's
/// thread-local state. Worker states are merged deterministically by worker
/// index after all runnable morsels complete.
///
/// # Errors
/// Returns an error when worker observer logic, join, or deterministic merge fails.
#[allow(clippy::too_many_lines)]
pub fn execute_vortex_morsel_scheduler_with_observer<S, F>(
    plan: VortexMorselSchedulerPlan,
    observer: F,
) -> Result<VortexMorselSchedulerRun<S>>
where
    S: VortexMorselThreadLocalState,
    F: Fn(&mut S, &VortexMorselWorkUnit) -> Result<()> + Send + Sync + 'static,
{
    let runnable = plan.runnable_work_units();
    let applied_workers = plan.policy.applied_workers(runnable.len());
    if !plan.memory_envelope_admitted() {
        return Ok(VortexMorselSchedulerRun {
            report: blocked_morsel_scheduler_report(
                &plan,
                VortexMorselSchedulerStatus::BlockedByMemoryPolicy,
                applied_workers,
                "morsel estimated bytes exceed per-worker memory budget",
            ),
            merged_state: S::new(usize::MAX),
        });
    }
    if runnable.is_empty() {
        return Ok(VortexMorselSchedulerRun {
            report: metadata_only_morsel_scheduler_report(&plan, applied_workers),
            merged_state: S::new(usize::MAX),
        });
    }

    let mut assignments = vec![Vec::<VortexMorselWorkUnit>::new(); applied_workers];
    for (index, work) in runnable.into_iter().enumerate() {
        assignments[index % applied_workers].push(work);
    }

    let observer = Arc::new(observer);
    let execution_start = Instant::now();
    let handles = assignments
        .into_iter()
        .enumerate()
        .map(|(worker_index, work_units)| {
            let observer = Arc::clone(&observer);
            std::thread::spawn(move || -> Result<(usize, S, VortexMorselWorkerUtilization)> {
                let stage_start = Instant::now();
                let assigned_morsels = work_units.len();
                let mut rows_processed = 0_u64;
                let mut estimated_bytes_processed = 0_u64;
                let mut state = S::new(worker_index);
                for work in &work_units {
                    observer(&mut state, work)?;
                    rows_processed = rows_processed.checked_add(work.row_count).ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "morsel worker row count overflowed; fallback execution was not attempted"
                                .to_string(),
                        )
                    })?;
                    estimated_bytes_processed = estimated_bytes_processed
                        .checked_add(work.estimated_bytes)
                        .ok_or_else(|| {
                            ShardLoomError::InvalidOperation(
                                "morsel worker estimated bytes overflowed; fallback execution was not attempted"
                                    .to_string(),
                            )
                        })?;
                }
                Ok((
                    worker_index,
                    state,
                    VortexMorselWorkerUtilization {
                        worker_index,
                        assigned_morsels,
                        completed_morsels: assigned_morsels,
                        rows_processed,
                        estimated_bytes_processed,
                        stage_micros: stage_start.elapsed().as_micros(),
                    },
                ))
            })
        })
        .collect::<Vec<_>>();

    let mut worker_states = Vec::new();
    let mut worker_utilization = Vec::new();
    for handle in handles {
        match handle.join() {
            Ok(Ok((worker_index, state, utilization))) => {
                worker_states.push((worker_index, state));
                worker_utilization.push(utilization);
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(ShardLoomError::InvalidOperation(
                    "morsel scheduler worker panicked; fallback execution was not attempted"
                        .to_string(),
                ));
            }
        }
    }

    worker_states.sort_by_key(|(worker_index, _)| *worker_index);
    worker_utilization.sort_by_key(|utilization| utilization.worker_index);
    let merge_start = Instant::now();
    let mut merged_state = S::new(usize::MAX);
    for (_, state) in worker_states {
        merged_state.merge_from(state)?;
    }
    let merge_micros = merge_start.elapsed().as_micros();
    let elapsed_micros = execution_start.elapsed().as_micros().max(1);
    let rows_processed = worker_utilization
        .iter()
        .map(|worker| worker.rows_processed)
        .fold(0_u64, u64::saturating_add);
    let completed_morsels = worker_utilization
        .iter()
        .map(|worker| worker.completed_morsels)
        .sum::<usize>();
    let active_worker_count = worker_utilization
        .iter()
        .filter(|worker| worker.assigned_morsels > 0)
        .count()
        .max(1);
    let total_stage_micros = worker_utilization
        .iter()
        .map(|worker| worker.stage_micros)
        .sum::<u128>();
    let mean_stage_micros =
        total_stage_micros / u128::try_from(active_worker_count).unwrap_or(u128::MAX);
    let max_stage_micros = worker_utilization
        .iter()
        .map(|worker| worker.stage_micros)
        .max()
        .unwrap_or(0);
    let skew_ratio_x1000 = max_stage_micros
        .saturating_mul(1000)
        .checked_div(mean_stage_micros)
        .unwrap_or(0);
    let rows_per_second = rows_processed
        .saturating_mul(1_000_000)
        .checked_div(u64::try_from(elapsed_micros).unwrap_or(u64::MAX).max(1))
        .unwrap_or(0);

    let queue_wave_count = plan.queue_wave_count();
    let queue_limit_enforced = plan.queue_limit_enforced();
    let pruned_morsels = plan.pruned_morsel_count();

    Ok(VortexMorselSchedulerRun {
        report: VortexMorselSchedulerExecutionReport {
            schema_version: plan.schema_version,
            scheduler_id: plan.scheduler_id,
            family: plan.family,
            status: VortexMorselSchedulerStatus::Executed,
            requested_workers: plan.policy.max_parallelism,
            applied_workers,
            queue_limit: plan.policy.queue_limit,
            queue_wave_count,
            queue_limit_enforced,
            runnable_morsels: completed_morsels,
            pruned_morsels,
            completed_morsels,
            failed_morsels: 0,
            thread_local_state_count: active_worker_count,
            deterministic_merge_count: active_worker_count,
            rows_processed,
            worker_summaries: worker_utilization
                .iter()
                .map(VortexMorselWorkerUtilization::summary)
                .collect(),
            mean_stage_micros,
            max_stage_micros,
            skew_ratio_x1000,
            merge_micros,
            rows_per_second,
            memory_envelope_admitted: true,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            external_engine_invoked: false,
            diagnostics: Vec::new(),
        },
        merged_state,
    })
}

fn metadata_only_morsel_scheduler_report(
    plan: &VortexMorselSchedulerPlan,
    applied_workers: usize,
) -> VortexMorselSchedulerExecutionReport {
    VortexMorselSchedulerExecutionReport {
        schema_version: plan.schema_version,
        scheduler_id: plan.scheduler_id.clone(),
        family: plan.family.clone(),
        status: VortexMorselSchedulerStatus::MetadataOnlyPruned,
        requested_workers: plan.policy.max_parallelism,
        applied_workers,
        queue_limit: plan.policy.queue_limit,
        queue_wave_count: 0,
        queue_limit_enforced: plan.queue_limit_enforced(),
        runnable_morsels: 0,
        pruned_morsels: plan.pruned_morsel_count(),
        completed_morsels: 0,
        failed_morsels: 0,
        thread_local_state_count: 0,
        deterministic_merge_count: 0,
        rows_processed: 0,
        worker_summaries: Vec::new(),
        mean_stage_micros: 0,
        max_stage_micros: 0,
        skew_ratio_x1000: 0,
        merge_micros: 0,
        rows_per_second: 0,
        memory_envelope_admitted: true,
        fallback_attempted: false,
        fallback_execution_allowed: false,
        external_engine_invoked: false,
        diagnostics: Vec::new(),
    }
}

fn blocked_morsel_scheduler_report(
    plan: &VortexMorselSchedulerPlan,
    status: VortexMorselSchedulerStatus,
    applied_workers: usize,
    reason: impl Into<String>,
) -> VortexMorselSchedulerExecutionReport {
    VortexMorselSchedulerExecutionReport {
        schema_version: plan.schema_version,
        scheduler_id: plan.scheduler_id.clone(),
        family: plan.family.clone(),
        status,
        requested_workers: plan.policy.max_parallelism,
        applied_workers,
        queue_limit: plan.policy.queue_limit,
        queue_wave_count: plan.queue_wave_count(),
        queue_limit_enforced: plan.queue_limit_enforced(),
        runnable_morsels: plan.runnable_morsel_count(),
        pruned_morsels: plan.pruned_morsel_count(),
        completed_morsels: 0,
        failed_morsels: 0,
        thread_local_state_count: 0,
        deterministic_merge_count: 0,
        rows_processed: 0,
        worker_summaries: Vec::new(),
        mean_stage_micros: 0,
        max_stage_micros: 0,
        skew_ratio_x1000: 0,
        merge_micros: 0,
        rows_per_second: 0,
        memory_envelope_admitted: false,
        fallback_attempted: false,
        fallback_execution_allowed: false,
        external_engine_invoked: false,
        diagnostics: vec![Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            "vortex_morsel_scheduler",
            reason,
            Some("Fallback attempted: false".to_string()),
        )],
    }
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

    fn scheduler_work_units() -> Vec<VortexMorselWorkUnit> {
        (0_u64..9)
            .map(|index| {
                VortexMorselWorkUnit::new(
                    "test_family",
                    format!("segment-{index}"),
                    index.saturating_mul(10),
                    index + 1,
                    (index + 1).saturating_mul(128),
                )
                .expect("valid test work unit")
            })
            .collect()
    }

    fn test_scheduler_plan(
        max_parallelism: usize,
        work_units: Vec<VortexMorselWorkUnit>,
    ) -> VortexMorselSchedulerPlan {
        VortexMorselSchedulerPlan::new(
            "test_family",
            work_units,
            VortexMorselSchedulerPolicy::new(max_parallelism)
                .expect("valid test scheduler policy")
                .with_queue_limit(3),
        )
        .expect("valid test scheduler plan")
    }

    fn field_value(fields: &[(String, String)], key: &str) -> String {
        fields
            .iter()
            .find_map(|(field_key, value)| (field_key == key).then_some(value.clone()))
            .unwrap_or_else(|| panic!("missing field {key}"))
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct WeightedRowsState {
        worker_index: usize,
        weighted_rows: u64,
        observed_work_ids: Vec<String>,
    }

    impl VortexMorselThreadLocalState for WeightedRowsState {
        fn new(worker_index: usize) -> Self {
            Self {
                worker_index,
                weighted_rows: 0,
                observed_work_ids: Vec::new(),
            }
        }

        fn observe_morsel(&mut self, work: &VortexMorselWorkUnit) -> Result<()> {
            self.weighted_rows =
                self.weighted_rows
                    .checked_add(work.row_count)
                    .ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "weighted rows state overflowed; fallback execution was not attempted"
                                .to_string(),
                        )
                    })?;
            self.observed_work_ids.push(work.work_id.clone());
            Ok(())
        }

        fn merge_from(&mut self, other: Self) -> Result<()> {
            self.weighted_rows = self
                .weighted_rows
                .checked_add(other.weighted_rows)
                .ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "weighted rows merge overflowed; fallback execution was not attempted"
                            .to_string(),
                    )
                })?;
            self.observed_work_ids.extend(other.observed_work_ids);
            self.observed_work_ids.sort();
            Ok(())
        }

        fn evidence_summary(&self) -> String {
            format!(
                "weighted_rows={}:work_ids={}",
                self.weighted_rows,
                self.observed_work_ids.join(",")
            )
        }
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
    fn empty_decisions_with_error_diagnostics_are_unsupported() {
        let mut input = VortexSchedulerBridgeInput::new(empty_memory_report());
        input.add_diagnostic(Diagnostic::configuration_error(
            "scheduler bridge",
            "simulated planner failure",
            "for test",
        ));
        let report = VortexSchedulerBridgeReport::from_input(input).expect("ok");
        assert_eq!(report.status, VortexSchedulerBridgeStatus::Unsupported);
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

    #[test]
    fn morsel_scheduler_merges_deterministically_across_worker_counts() {
        let expected_rows = scheduler_work_units()
            .iter()
            .map(|work| work.row_count)
            .sum::<u64>();
        let expected_work_ids = {
            let mut ids = scheduler_work_units()
                .iter()
                .map(|work| work.work_id.clone())
                .collect::<Vec<_>>();
            ids.sort();
            ids
        };

        for max_parallelism in [1_usize, 2, 3, 12] {
            let run = execute_vortex_morsel_scheduler_with_state::<VortexMorselRowCountState>(
                test_scheduler_plan(max_parallelism, scheduler_work_units()),
            )
            .expect("scheduler run");

            assert_eq!(run.report.status, VortexMorselSchedulerStatus::Executed);
            assert_eq!(run.report.requested_workers, max_parallelism);
            assert_eq!(run.report.applied_workers, max_parallelism.min(9));
            assert_eq!(run.report.completed_morsels, 9);
            assert_eq!(run.report.failed_morsels, 0);
            assert_eq!(run.report.rows_processed, expected_rows);
            assert_eq!(run.merged_state.row_count, expected_rows);
            assert_eq!(run.merged_state.observed_work_ids, expected_work_ids);
            assert_eq!(
                run.report.thread_local_state_count,
                run.report.applied_workers
            );
            assert_eq!(
                run.report.deterministic_merge_count,
                run.report.applied_workers
            );
            assert!(!run.report.fallback_attempted);
            assert!(!run.report.external_engine_invoked);
            assert!(!run.report.fallback_execution_allowed);
        }
    }

    #[test]
    fn morsel_scheduler_blocks_over_budget_work_without_fallback() {
        let work_units = vec![
            VortexMorselWorkUnit::new("test_family", "segment-0", 0, 10, 1_024)
                .expect("valid work unit"),
            VortexMorselWorkUnit::new("test_family", "segment-1", 10, 10, 4_096)
                .expect("valid work unit"),
        ];
        let plan = VortexMorselSchedulerPlan::new(
            "test_family",
            work_units,
            VortexMorselSchedulerPolicy::new(4)
                .expect("valid test scheduler policy")
                .with_worker_memory_budget_bytes(2_048),
        )
        .expect("valid test scheduler plan");

        let run = execute_vortex_morsel_scheduler_with_state::<VortexMorselRowCountState>(plan)
            .expect("scheduler should return deterministic blocked report");

        assert_eq!(
            run.report.status,
            VortexMorselSchedulerStatus::BlockedByMemoryPolicy
        );
        assert!(run.report.has_errors());
        assert_eq!(run.report.completed_morsels, 0);
        assert_eq!(run.report.rows_processed, 0);
        assert_eq!(run.merged_state.row_count, 0);
        assert!(!run.report.fallback_attempted);
        assert!(!run.report.external_engine_invoked);
        assert!(!run.report.fallback_execution_allowed);
        assert!(run.report.diagnostics.iter().any(|diagnostic| {
            diagnostic
                .suggested_next_step
                .as_deref()
                .unwrap_or_default()
                .contains("Fallback attempted: false")
        }));
    }

    #[test]
    fn morsel_scheduler_metadata_pruned_work_does_not_execute() {
        let work_units = vec![
            VortexMorselWorkUnit::new("test_family", "segment-0", 0, 10, 1_024)
                .expect("valid work unit")
                .pruned_by_metadata(true),
            VortexMorselWorkUnit::new("test_family", "segment-1", 10, 10, 1_024)
                .expect("valid work unit")
                .pruned_by_metadata(true),
        ];
        let run = execute_vortex_morsel_scheduler_with_state::<VortexMorselRowCountState>(
            test_scheduler_plan(12, work_units),
        )
        .expect("scheduler run");

        assert_eq!(
            run.report.status,
            VortexMorselSchedulerStatus::MetadataOnlyPruned
        );
        assert_eq!(run.report.requested_workers, 12);
        assert_eq!(run.report.applied_workers, 1);
        assert_eq!(run.report.runnable_morsels, 0);
        assert_eq!(run.report.pruned_morsels, 2);
        assert_eq!(run.report.completed_morsels, 0);
        assert_eq!(run.report.thread_local_state_count, 0);
        assert_eq!(run.report.deterministic_merge_count, 0);
        assert_eq!(run.merged_state.row_count, 0);
        assert!(!run.report.has_errors());
    }

    #[test]
    fn morsel_scheduler_evidence_fields_report_worker_and_fallback_contract() {
        let run = execute_vortex_morsel_scheduler_with_state::<VortexMorselRowCountState>(
            test_scheduler_plan(3, scheduler_work_units()),
        )
        .expect("scheduler run");
        let fields = run.report.evidence_fields("test");

        assert_eq!(
            field_value(&fields, "test_morsel_scheduler_status"),
            "executed"
        );
        assert_eq!(
            field_value(&fields, "test_morsel_scheduler_requested_workers"),
            "3"
        );
        assert_eq!(
            field_value(&fields, "test_morsel_scheduler_applied_workers"),
            "3"
        );
        assert_eq!(
            field_value(&fields, "test_morsel_scheduler_queue_limit"),
            "3"
        );
        assert_eq!(
            field_value(&fields, "test_morsel_scheduler_queue_wave_count"),
            "3"
        );
        assert_eq!(
            field_value(&fields, "test_morsel_scheduler_deterministic_merge_count"),
            "3"
        );
        assert_eq!(
            field_value(&fields, "test_morsel_scheduler_memory_envelope_admitted"),
            "true"
        );
        assert_eq!(
            field_value(&fields, "test_morsel_scheduler_fallback_attempted"),
            "false"
        );
        assert_eq!(
            field_value(&fields, "test_morsel_scheduler_external_engine_invoked"),
            "false"
        );
        assert_eq!(
            field_value(&fields, "test_morsel_scheduler_fallback_execution_allowed"),
            "false"
        );
        assert!(
            field_value(&fields, "test_morsel_scheduler_worker_summaries").contains("worker=0")
        );
    }

    #[test]
    fn morsel_scheduler_observer_runs_custom_thread_local_operator_logic() {
        let expected = scheduler_work_units()
            .iter()
            .map(|work| work.row_count.saturating_mul(work.start_row + 1))
            .sum::<u64>();
        let run = execute_vortex_morsel_scheduler_with_observer::<WeightedRowsState, _>(
            test_scheduler_plan(4, scheduler_work_units()),
            |state, work| {
                let weighted = work.row_count.checked_mul(work.start_row + 1).ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "weighted rows multiplication overflowed; fallback execution was not attempted"
                            .to_string(),
                    )
                })?;
                state.weighted_rows = state.weighted_rows.checked_add(weighted).ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "weighted rows accumulation overflowed; fallback execution was not attempted"
                            .to_string(),
                    )
                })?;
                state.observed_work_ids.push(work.work_id.clone());
                Ok(())
            },
        )
        .expect("scheduler run");

        assert_eq!(run.report.status, VortexMorselSchedulerStatus::Executed);
        assert_eq!(run.report.applied_workers, 4);
        assert_eq!(run.report.completed_morsels, 9);
        assert_eq!(run.merged_state.weighted_rows, expected);
        assert_eq!(run.merged_state.observed_work_ids.len(), 9);
        assert!(!run.report.has_errors());
    }
}
