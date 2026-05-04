#![allow(clippy::must_use_candidate)]
#![allow(clippy::return_self_not_must_use)]
#![allow(clippy::too_many_lines)]

use std::fmt::Write as _;

use shardloom_core::{
    Diagnostic, DiagnosticCode, DiagnosticSeverity, Result, SegmentId, ShardLoomError,
};
use shardloom_exec::{
    MemoryBudget, SpillLifecycleRequest, SpillPolicy, SpillReservationIntegrationReport,
    SpillReservationIntegrationRequest, TaskId, plan_spill_reservation_integration,
};

use crate::{
    VortexLocalExecutionReport, VortexLocalExecutionStatus, VortexMemoryBridgeReport,
    VortexMemoryBridgeStatus, VortexQueryDecisionTrace, VortexSchedulerBridgeReport,
    VortexSchedulerBridgeStatus, VortexWorkAvoidedReport,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexBoundedExecutionStatus {
    Planned,
    MetadataTasksCompleted,
    NoOpTasksCompleted,
    ReadyButNoExecutableTasks,
    NeedsEncodedRead,
    NeedsPredicateEvaluation,
    BlockedByMemoryPolicy,
    BlockedByMissingEstimate,
    BlockedByScheduler,
    BlockedByDecodeRisk,
    BlockedByMaterializationRisk,
    BlockedByObjectStoreIo,
    BlockedByWriteIo,
    BlockedBySpillIo,
    BlockedByExternalEffect,
    Unsupported,
}
impl VortexBoundedExecutionStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::MetadataTasksCompleted => "metadata_tasks_completed",
            Self::NoOpTasksCompleted => "no_op_tasks_completed",
            Self::ReadyButNoExecutableTasks => "ready_but_no_executable_tasks",
            Self::NeedsEncodedRead => "needs_encoded_read",
            Self::NeedsPredicateEvaluation => "needs_predicate_evaluation",
            Self::BlockedByMemoryPolicy => "blocked_by_memory_policy",
            Self::BlockedByMissingEstimate => "blocked_by_missing_estimate",
            Self::BlockedByScheduler => "blocked_by_scheduler",
            Self::BlockedByDecodeRisk => "blocked_by_decode_risk",
            Self::BlockedByMaterializationRisk => "blocked_by_materialization_risk",
            Self::BlockedByObjectStoreIo => "blocked_by_object_store_io",
            Self::BlockedByWriteIo => "blocked_by_write_io",
            Self::BlockedBySpillIo => "blocked_by_spill_io",
            Self::BlockedByExternalEffect => "blocked_by_external_effect",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::BlockedByMemoryPolicy
                | Self::BlockedByMissingEstimate
                | Self::BlockedByScheduler
                | Self::BlockedByDecodeRisk
                | Self::BlockedByMaterializationRisk
                | Self::BlockedByObjectStoreIo
                | Self::BlockedByWriteIo
                | Self::BlockedBySpillIo
                | Self::BlockedByExternalEffect
                | Self::Unsupported
        )
    }
    pub const fn completed_without_data_read(&self) -> bool {
        matches!(
            self,
            Self::MetadataTasksCompleted | Self::NoOpTasksCompleted
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexBoundedExecutionMode {
    MetadataOnly,
    NoOp,
    ScheduledPlanOnly,
    Blocked,
    Unsupported,
}
impl VortexBoundedExecutionMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MetadataOnly => "metadata_only",
            Self::NoOp => "no_op",
            Self::ScheduledPlanOnly => "scheduled_plan_only",
            Self::Blocked => "blocked",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn reads_data(&self) -> bool {
        false
    }
    pub const fn decodes_data(&self) -> bool {
        false
    }
    pub const fn materializes_data(&self) -> bool {
        false
    }
    pub const fn writes_data(&self) -> bool {
        false
    }
    pub const fn executes_tasks(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexBoundedExecutionPolicy {
    pub memory_budget: MemoryBudget,
    pub max_parallelism: usize,
    pub allow_metadata_tasks: bool,
    pub allow_noop_tasks: bool,
    pub allow_encoded_read_tasks: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexBoundedExecutionPolicy {
    pub fn new(memory_budget: MemoryBudget) -> Self {
        Self {
            memory_budget,
            max_parallelism: 1,
            allow_metadata_tasks: true,
            allow_noop_tasks: true,
            allow_encoded_read_tasks: false,
            diagnostics: vec![],
        }
    }
    /// # Errors
    /// Returns an error when `memory_gb` is zero, `max_parallelism` is zero, or budget construction fails.
    pub fn memory_limited(memory_gb: u64, max_parallelism: usize) -> Result<Self> {
        if max_parallelism == 0 {
            return Err(ShardLoomError::InvalidOperation(
                "max_parallelism must be >= 1".to_string(),
            ));
        }
        Ok(Self::new(MemoryBudget::from_gib(memory_gb)?).with_max_parallelism(max_parallelism))
    }
    pub fn with_max_parallelism(mut self, v: usize) -> Self {
        self.max_parallelism = v.max(1);
        self
    }
    pub fn allow_metadata_tasks(mut self, v: bool) -> Self {
        self.allow_metadata_tasks = v;
        self
    }
    pub fn allow_noop_tasks(mut self, v: bool) -> Self {
        self.allow_noop_tasks = v;
        self
    }
    pub fn allow_encoded_read_tasks(mut self, v: bool) -> Self {
        self.allow_encoded_read_tasks = v;
        self
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
    }
    pub fn summary(&self) -> String {
        format!(
            "memory={} max_parallelism={} allow_metadata_tasks={} allow_noop_tasks={} allow_encoded_read_tasks={}",
            self.memory_budget.summary(),
            self.max_parallelism,
            self.allow_metadata_tasks,
            self.allow_noop_tasks,
            self.allow_encoded_read_tasks
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexBoundedExecutionDecisionKind {
    ExecuteMetadataOnly,
    CompleteNoOp,
    DeferEncodedRead,
    DeferPredicateEvaluation,
    HoldForMemory,
    HoldForEstimate,
    HoldForScheduler,
    BlockDecode,
    BlockMaterialization,
    BlockObjectStore,
    BlockWrite,
    BlockSpill,
    BlockExternalEffect,
    Unsupported,
}
impl VortexBoundedExecutionDecisionKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ExecuteMetadataOnly => "execute_metadata_only",
            Self::CompleteNoOp => "complete_noop",
            Self::DeferEncodedRead => "defer_encoded_read",
            Self::DeferPredicateEvaluation => "defer_predicate_evaluation",
            Self::HoldForMemory => "hold_for_memory",
            Self::HoldForEstimate => "hold_for_estimate",
            Self::HoldForScheduler => "hold_for_scheduler",
            Self::BlockDecode => "block_decode",
            Self::BlockMaterialization => "block_materialization",
            Self::BlockObjectStore => "block_object_store",
            Self::BlockWrite => "block_write",
            Self::BlockSpill => "block_spill",
            Self::BlockExternalEffect => "block_external_effect",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_completed(&self) -> bool {
        matches!(self, Self::ExecuteMetadataOnly | Self::CompleteNoOp)
    }
    pub const fn is_blocked(&self) -> bool {
        matches!(
            self,
            Self::HoldForMemory
                | Self::HoldForEstimate
                | Self::HoldForScheduler
                | Self::BlockDecode
                | Self::BlockMaterialization
                | Self::BlockObjectStore
                | Self::BlockWrite
                | Self::BlockSpill
                | Self::BlockExternalEffect
                | Self::Unsupported
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexBoundedExecutionDecision {
    pub kind: VortexBoundedExecutionDecisionKind,
    pub task_id: Option<TaskId>,
    pub segment_id: Option<SegmentId>,
    pub batch_id: Option<String>,
    pub reason: String,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexBoundedExecutionDecision {
    fn base(
        kind: VortexBoundedExecutionDecisionKind,
        task_id: Option<TaskId>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            task_id,
            segment_id: None,
            batch_id: None,
            reason: reason.into(),
            diagnostics: vec![],
        }
    }
    pub fn execute_metadata_only(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexBoundedExecutionDecisionKind::ExecuteMetadataOnly,
            task_id,
            reason,
        )
    }
    pub fn complete_noop(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexBoundedExecutionDecisionKind::CompleteNoOp,
            task_id,
            reason,
        )
    }
    pub fn defer_encoded_read(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexBoundedExecutionDecisionKind::DeferEncodedRead,
            task_id,
            reason,
        )
    }
    pub fn defer_predicate_evaluation(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexBoundedExecutionDecisionKind::DeferPredicateEvaluation,
            task_id,
            reason,
        )
    }
    pub fn hold_for_memory(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexBoundedExecutionDecisionKind::HoldForMemory,
            task_id,
            reason,
        )
    }
    pub fn hold_for_estimate(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexBoundedExecutionDecisionKind::HoldForEstimate,
            task_id,
            reason,
        )
    }
    pub fn hold_for_scheduler(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexBoundedExecutionDecisionKind::HoldForScheduler,
            task_id,
            reason,
        )
    }
    pub fn block_spill(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexBoundedExecutionDecisionKind::BlockSpill,
            task_id,
            reason,
        )
    }
    pub fn unsupported(
        task_id: Option<TaskId>,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let reason = reason.into();
        let mut s = Self::base(
            VortexBoundedExecutionDecisionKind::Unsupported,
            task_id,
            reason.clone(),
        );
        s.diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            feature,
            "Unsupported bounded execution behavior.",
            Some(format!("{reason}. Fallback attempted: false")),
        ));
        s
    }
    pub fn with_segment_id(mut self, segment_id: SegmentId) -> Self {
        self.segment_id = Some(segment_id);
        self
    }
    pub fn with_batch_id(mut self, batch_id: impl Into<String>) -> Self {
        self.batch_id = Some(batch_id.into());
        self
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    pub const fn is_completed(&self) -> bool {
        self.kind.is_completed()
    }
    pub const fn is_blocked(&self) -> bool {
        self.kind.is_blocked()
    }
    pub fn has_errors(&self) -> bool {
        self.kind == VortexBoundedExecutionDecisionKind::Unsupported
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    pub fn summary(&self) -> String {
        format!("{}: {}", self.kind.as_str(), self.reason)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexBoundedExecutionInput {
    pub local_execution_report: VortexLocalExecutionReport,
    pub scheduler_report: Option<VortexSchedulerBridgeReport>,
    pub memory_report: Option<VortexMemoryBridgeReport>,
    pub policy: VortexBoundedExecutionPolicy,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexBoundedExecutionInput {
    pub fn new(
        local_execution_report: VortexLocalExecutionReport,
        policy: VortexBoundedExecutionPolicy,
    ) -> Self {
        Self {
            local_execution_report,
            scheduler_report: None,
            memory_report: None,
            policy,
            diagnostics: vec![],
        }
    }
    pub fn with_scheduler_report(mut self, r: VortexSchedulerBridgeReport) -> Self {
        self.scheduler_report = Some(r);
        self
    }
    pub fn with_memory_report(mut self, r: VortexMemoryBridgeReport) -> Self {
        self.memory_report = Some(r);
        self
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    pub fn has_errors(&self) -> bool {
        self.local_execution_report.has_errors()
            || self.policy.has_errors()
            || self
                .scheduler_report
                .as_ref()
                .is_some_and(VortexSchedulerBridgeReport::has_errors)
            || self
                .memory_report
                .as_ref()
                .is_some_and(VortexMemoryBridgeReport::has_errors)
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    pub fn summary(&self) -> String {
        format!(
            "local_status={} has_scheduler={} has_memory={} policy=({})",
            self.local_execution_report.status.as_str(),
            self.scheduler_report.is_some(),
            self.memory_report.is_some(),
            self.policy.summary()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexBoundedExecutionReport {
    pub status: VortexBoundedExecutionStatus,
    pub mode: VortexBoundedExecutionMode,
    pub input: VortexBoundedExecutionInput,
    pub decisions: Vec<VortexBoundedExecutionDecision>,
    pub local_execution_report: VortexLocalExecutionReport,
    pub decision_trace: Option<VortexQueryDecisionTrace>,
    pub work_avoided: Option<VortexWorkAvoidedReport>,
    pub metadata_tasks_completed: usize,
    pub noop_tasks_completed: usize,
    pub encoded_read_tasks_deferred: usize,
    pub blocked_task_count: usize,
    pub max_parallelism: usize,
    pub memory_budget_summary: String,
    pub tasks_executed: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexBoundedExecutionReport {
    /// # Errors
    /// Returns an error if bounded report synthesis fails.
    pub fn from_input(input: VortexBoundedExecutionInput) -> Result<Self> {
        let local = input.local_execution_report.clone();
        let mut out = Self {
            status: VortexBoundedExecutionStatus::Planned,
            mode: VortexBoundedExecutionMode::ScheduledPlanOnly,
            input,
            decisions: vec![],
            local_execution_report: local.clone(),
            decision_trace: local
                .analysis_report
                .as_ref()
                .map(|a| a.decision_trace.clone()),
            work_avoided: local
                .analysis_report
                .as_ref()
                .map(|a| a.work_avoided.clone()),
            metadata_tasks_completed: 0,
            noop_tasks_completed: 0,
            encoded_read_tasks_deferred: 0,
            blocked_task_count: 0,
            max_parallelism: local.input.diagnostics.len(),
            memory_budget_summary: String::new(),
            tasks_executed: false,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        };
        out.max_parallelism = out.input.policy.max_parallelism;
        out.memory_budget_summary = out.input.policy.memory_budget.summary();
        let memory_status = out.input.memory_report.as_ref().map(|r| r.status);
        if matches!(
            memory_status,
            Some(VortexMemoryBridgeStatus::BlockedByMemoryPolicy)
        ) {
            out.status = VortexBoundedExecutionStatus::BlockedByMemoryPolicy;
            out.mode = VortexBoundedExecutionMode::Blocked;
            out.add_decision(VortexBoundedExecutionDecision::hold_for_memory(
                None,
                "memory bridge blocked execution",
            ));
        }
        if matches!(
            memory_status,
            Some(VortexMemoryBridgeStatus::SpillRequiredButNotImplemented)
        ) {
            out.status = VortexBoundedExecutionStatus::BlockedBySpillIo;
            out.mode = VortexBoundedExecutionMode::Blocked;
            out.add_decision(VortexBoundedExecutionDecision::block_spill(
                None,
                "spill required but unavailable",
            ));
        }
        if let Some(s) = &out.input.scheduler_report {
            if matches!(
                s.status,
                VortexSchedulerBridgeStatus::BlockedByMemoryPolicy
                    | VortexSchedulerBridgeStatus::SpillRequiredButNotImplemented
                    | VortexSchedulerBridgeStatus::Unsupported
            ) {
                out.status = VortexBoundedExecutionStatus::BlockedByScheduler;
                out.mode = VortexBoundedExecutionMode::Blocked;
                out.add_decision(VortexBoundedExecutionDecision::hold_for_scheduler(
                    None,
                    "scheduler blocked queue execution",
                ));
            }
        }
        if !matches!(
            out.status,
            VortexBoundedExecutionStatus::BlockedByMemoryPolicy
                | VortexBoundedExecutionStatus::BlockedByScheduler
                | VortexBoundedExecutionStatus::BlockedBySpillIo
                | VortexBoundedExecutionStatus::Unsupported
        ) {
            match local.status {
                VortexLocalExecutionStatus::MetadataExecuted
                    if out.input.policy.allow_metadata_tasks =>
                {
                    out.status = VortexBoundedExecutionStatus::MetadataTasksCompleted;
                    out.mode = VortexBoundedExecutionMode::MetadataOnly;
                    out.add_decision(VortexBoundedExecutionDecision::execute_metadata_only(
                        None,
                        "metadata-only local execution completed",
                    ));
                }
                VortexLocalExecutionStatus::NoOpCompleted if out.input.policy.allow_noop_tasks => {
                    out.status = VortexBoundedExecutionStatus::NoOpTasksCompleted;
                    out.mode = VortexBoundedExecutionMode::NoOp;
                    out.add_decision(VortexBoundedExecutionDecision::complete_noop(
                        None,
                        "no-op local execution completed",
                    ));
                }
                VortexLocalExecutionStatus::NeedsEncodedRead => {
                    out.status = VortexBoundedExecutionStatus::NeedsEncodedRead;
                    out.mode = VortexBoundedExecutionMode::ScheduledPlanOnly;
                    out.add_decision(VortexBoundedExecutionDecision::defer_encoded_read(
                        None,
                        "encoded-read work deferred by bounded execution policy",
                    ));
                }
                VortexLocalExecutionStatus::NeedsPredicateEvaluation => {
                    out.status = VortexBoundedExecutionStatus::NeedsPredicateEvaluation;
                    out.mode = VortexBoundedExecutionMode::ScheduledPlanOnly;
                    out.add_decision(VortexBoundedExecutionDecision::defer_predicate_evaluation(
                        None,
                        "predicate evaluation deferred by bounded execution policy",
                    ));
                }
                VortexLocalExecutionStatus::MissingMetadata => {
                    out.status = VortexBoundedExecutionStatus::BlockedByMissingEstimate;
                    out.mode = VortexBoundedExecutionMode::Blocked;
                    out.add_decision(VortexBoundedExecutionDecision::hold_for_estimate(
                        None,
                        "metadata missing for bounded execution",
                    ));
                }
                VortexLocalExecutionStatus::Unsupported => {
                    return Ok(Self::unsupported(
                        out.input,
                        "vortex_bounded_execution",
                        "local execution reported unsupported status",
                    ));
                }
                _ => {
                    if out.status == VortexBoundedExecutionStatus::Planned {
                        out.status = VortexBoundedExecutionStatus::ReadyButNoExecutableTasks;
                    }
                }
            }
        }
        out.recompute_counts();
        Ok(out)
    }
    pub fn unsupported(
        input: VortexBoundedExecutionInput,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let local_execution_report = input.local_execution_report.clone();
        let mut s = Self {
            status: VortexBoundedExecutionStatus::Unsupported,
            mode: VortexBoundedExecutionMode::Unsupported,
            max_parallelism: input.policy.max_parallelism,
            memory_budget_summary: input.policy.memory_budget.summary(),
            input,
            decisions: vec![],
            local_execution_report,
            decision_trace: None,
            work_avoided: None,
            metadata_tasks_completed: 0,
            noop_tasks_completed: 0,
            encoded_read_tasks_deferred: 0,
            blocked_task_count: 0,
            tasks_executed: false,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        };
        s.add_decision(VortexBoundedExecutionDecision::unsupported(
            None, feature, reason,
        ));
        s.recompute_counts();
        s
    }
    pub fn add_decision(&mut self, d: VortexBoundedExecutionDecision) {
        self.decisions.push(d);
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    pub fn recompute_counts(&mut self) {
        self.metadata_tasks_completed = self
            .decisions
            .iter()
            .filter(|d| d.kind == VortexBoundedExecutionDecisionKind::ExecuteMetadataOnly)
            .count();
        self.noop_tasks_completed = self
            .decisions
            .iter()
            .filter(|d| d.kind == VortexBoundedExecutionDecisionKind::CompleteNoOp)
            .count();
        self.encoded_read_tasks_deferred = self
            .decisions
            .iter()
            .filter(|d| d.kind == VortexBoundedExecutionDecisionKind::DeferEncodedRead)
            .count();
        self.blocked_task_count = self.decisions.iter().filter(|d| d.is_blocked()).count();
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
            || self
                .decisions
                .iter()
                .any(VortexBoundedExecutionDecision::has_errors)
            || self.input.has_errors()
    }
    pub const fn is_side_effect_free(&self) -> bool {
        !self.tasks_executed
            && !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_effects_executed
            && !self.fallback_execution_allowed
    }
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "Vortex bounded local execution report\nstatus: {}\nmode: {}\nmetadata tasks completed: {}\nno-op tasks completed: {}\nencoded-read tasks deferred: {}\nblocked task count: {}\nmax parallelism: {}\nmemory budget: {}",
            self.status.as_str(),
            self.mode.as_str(),
            self.metadata_tasks_completed,
            self.noop_tasks_completed,
            self.encoded_read_tasks_deferred,
            self.blocked_task_count,
            self.max_parallelism,
            self.memory_budget_summary
        );
        if let Some(trace) = &self.decision_trace {
            let _ = writeln!(out, "decision trace count: {}", trace.entry_count());
        }
        if let Some(work) = &self.work_avoided {
            let _ = writeln!(out, "work avoided metric count: {}", work.metric_count());
        }
        let _ = write!(
            out,
            "tasks executed: {}\ndata read: {}\ndata decoded: {}\ndata materialized: {}\nobject-store IO: {}\nwrite IO: {}\nspill IO: {}\nexternal effects executed: {}\nfallback execution: disabled",
            self.tasks_executed,
            self.data_read,
            self.data_decoded,
            self.data_materialized,
            self.object_store_io,
            self.write_io,
            self.spill_io_performed,
            self.external_effects_executed
        );
        out
    }
}

/// # Errors
/// Returns an error if bounded report creation fails.
pub fn execute_vortex_bounded_local_query(
    local_execution_report: VortexLocalExecutionReport,
    policy: VortexBoundedExecutionPolicy,
) -> Result<VortexBoundedExecutionReport> {
    VortexBoundedExecutionReport::from_input(VortexBoundedExecutionInput::new(
        local_execution_report,
        policy,
    ))
}
pub fn vortex_bounded_execution_is_side_effect_free(report: &VortexBoundedExecutionReport) -> bool {
    report.is_side_effect_free()
}

/// # Errors
/// Returns an error when building or planning a `SpillReservationIntegrationReport` fails.
pub fn plan_bounded_execution_spill_reservation(
    bounded_report: &VortexBoundedExecutionReport,
    lifecycle_request: Option<SpillLifecycleRequest>,
) -> Result<Option<SpillReservationIntegrationReport>> {
    let spill_blocked = bounded_report
        .decisions
        .iter()
        .any(|d| matches!(d.kind, VortexBoundedExecutionDecisionKind::BlockSpill));
    if !spill_blocked {
        return Ok(None);
    }
    let mut request = SpillReservationIntegrationRequest::new(
        format!("vortex-bounded-{}", bounded_report.blocked_task_count),
        SpillPolicy::Required,
    )?;
    if let Some(lifecycle_request) = lifecycle_request {
        request = request.with_lifecycle_request(lifecycle_request);
    }
    for diagnostic in &bounded_report.diagnostics {
        request.add_diagnostic(diagnostic.clone());
    }
    Ok(Some(plan_spill_reservation_integration(request)?))
}
