#![allow(clippy::must_use_candidate)]
#![allow(clippy::return_self_not_must_use)]
#![allow(clippy::too_many_lines)]

use std::fmt::Write as _;

use shardloom_core::{
    Diagnostic, DiagnosticCode, DiagnosticSeverity, Result, SegmentId, ShardLoomError,
};
use shardloom_exec::recovery::{
    RecoveryArtifactRef, ShardLoomRecoveryIntegrationReport, ShardLoomRecoveryIntegrationRequest,
    plan_recovery_integration,
};
use shardloom_exec::{
    MemoryBudget, SpillLifecycleRequest, SpillPayloadFsRef, SpillPayloadRoundTripReport,
    SpillPayloadRoundTripRequest, SpillPayloadWriteRequest, SpillPolicy,
    SpillReservationIntegrationReport, SpillReservationIntegrationRequest,
    SpillReservationIntegrationStatus, SyntheticSpillPayload, TaskId,
    plan_spill_reservation_integration, roundtrip_spill_payload,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexBoundedSpillIntegrationStatus {
    NotRequired,
    Planned,
    ReservationRequired,
    ReservationReady,
    PayloadPlanReady,
    PayloadWriteAllowed,
    PayloadRoundTripAvailable,
    BlockedByMissingEstimate,
    BlockedByMemoryPolicy,
    BlockedBySpillPolicy,
    BlockedByFeatureGate,
    Unsupported,
}
impl VortexBoundedSpillIntegrationStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotRequired => "not_required",
            Self::Planned => "planned",
            Self::ReservationRequired => "reservation_required",
            Self::ReservationReady => "reservation_ready",
            Self::PayloadPlanReady => "payload_plan_ready",
            Self::PayloadWriteAllowed => "payload_write_allowed",
            Self::PayloadRoundTripAvailable => "payload_roundtrip_available",
            Self::BlockedByMissingEstimate => "blocked_by_missing_estimate",
            Self::BlockedByMemoryPolicy => "blocked_by_memory_policy",
            Self::BlockedBySpillPolicy => "blocked_by_spill_policy",
            Self::BlockedByFeatureGate => "blocked_by_feature_gate",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::BlockedByMissingEstimate
                | Self::BlockedByMemoryPolicy
                | Self::BlockedBySpillPolicy
                | Self::BlockedByFeatureGate
                | Self::Unsupported
        )
    }
    pub const fn requires_action(&self) -> bool {
        !matches!(self, Self::NotRequired | Self::PayloadRoundTripAvailable)
    }
    pub const fn is_blocking(&self) -> bool {
        matches!(
            self,
            Self::BlockedByMissingEstimate
                | Self::BlockedByMemoryPolicy
                | Self::BlockedBySpillPolicy
                | Self::BlockedByFeatureGate
                | Self::Unsupported
        )
    }
    pub const fn allows_payload_roundtrip_available(&self) -> bool {
        matches!(self, Self::ReservationReady | Self::PayloadWriteAllowed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexBoundedSpillIntegrationMode {
    ReportOnly,
    ReservationPlanning,
    SyntheticPayloadPlanning,
    SyntheticPayloadAvailable,
    Unsupported,
}
impl VortexBoundedSpillIntegrationMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::ReservationPlanning => "reservation_planning",
            Self::SyntheticPayloadPlanning => "synthetic_payload_planning",
            Self::SyntheticPayloadAvailable => "synthetic_payload_available",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn writes_query_spill_data(&self) -> bool {
        false
    }
    pub const fn writes_synthetic_payload(&self) -> bool {
        matches!(self, Self::SyntheticPayloadAvailable)
    }
    pub const fn reads_synthetic_payload(&self) -> bool {
        matches!(self, Self::SyntheticPayloadAvailable)
    }
    pub const fn touches_object_store(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexBoundedSpillIntegrationRequest {
    pub bounded_report: VortexBoundedExecutionReport,
    pub lifecycle_request: Option<SpillLifecycleRequest>,
    pub payload_fs_ref: Option<SpillPayloadFsRef>,
    pub synthetic_payload: Option<SyntheticSpillPayload>,
    pub allow_synthetic_payload_roundtrip: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexBoundedSpillIntegrationRequest {
    pub fn new(bounded_report: VortexBoundedExecutionReport) -> Self {
        Self {
            bounded_report,
            lifecycle_request: None,
            payload_fs_ref: None,
            synthetic_payload: None,
            allow_synthetic_payload_roundtrip: false,
            diagnostics: vec![],
        }
    }
    pub fn with_lifecycle_request(mut self, v: SpillLifecycleRequest) -> Self {
        self.lifecycle_request = Some(v);
        self
    }
    pub fn with_payload_fs_ref(mut self, v: SpillPayloadFsRef) -> Self {
        self.payload_fs_ref = Some(v);
        self
    }
    pub fn with_synthetic_payload(mut self, v: SyntheticSpillPayload) -> Self {
        self.synthetic_payload = Some(v);
        self
    }
    pub fn allow_synthetic_payload_roundtrip(mut self, v: bool) -> Self {
        self.allow_synthetic_payload_roundtrip = v;
        self
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    pub fn has_errors(&self) -> bool {
        self.bounded_report.has_errors()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    pub fn summary(&self) -> String {
        format!(
            "bounded_status={} allow_synthetic_payload_roundtrip={} has_lifecycle_request={} has_payload_fs_ref={} has_synthetic_payload={}",
            self.bounded_report.status.as_str(),
            self.allow_synthetic_payload_roundtrip,
            self.lifecycle_request.is_some(),
            self.payload_fs_ref.is_some(),
            self.synthetic_payload.is_some()
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexBoundedSpillIntegrationReport {
    pub status: VortexBoundedSpillIntegrationStatus,
    pub mode: VortexBoundedSpillIntegrationMode,
    pub request: VortexBoundedSpillIntegrationRequest,
    pub spill_reservation_report: Option<SpillReservationIntegrationReport>,
    pub payload_roundtrip_report: Option<SpillPayloadRoundTripReport>,
    pub reservation_required: bool,
    pub reservation_status: Option<String>,
    pub payload_write_allowed: bool,
    pub payload_written: bool,
    pub payload_read: bool,
    pub cleanup_performed: bool,
    pub spill_data_is_synthetic: bool,
    pub query_spill_data_written: bool,
    pub object_store_io: bool,
    pub output_dataset_write: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexBoundedSpillIntegrationReport {
    fn base(
        status: VortexBoundedSpillIntegrationStatus,
        mode: VortexBoundedSpillIntegrationMode,
        request: VortexBoundedSpillIntegrationRequest,
    ) -> Self {
        Self {
            status,
            mode,
            request,
            spill_reservation_report: None,
            payload_roundtrip_report: None,
            reservation_required: false,
            reservation_status: None,
            payload_write_allowed: false,
            payload_written: false,
            payload_read: false,
            cleanup_performed: false,
            spill_data_is_synthetic: false,
            query_spill_data_written: false,
            object_store_io: false,
            output_dataset_write: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        }
    }
    /// # Errors
    /// Returns an error if reservation or synthetic payload roundtrip planning fails.
    pub fn from_request(request: VortexBoundedSpillIntegrationRequest) -> Result<Self> {
        let blocked_by_memory = request
            .bounded_report
            .decisions
            .iter()
            .any(|d| matches!(d.kind, VortexBoundedExecutionDecisionKind::HoldForMemory));
        let blocked_by_estimate = request
            .bounded_report
            .decisions
            .iter()
            .any(|d| matches!(d.kind, VortexBoundedExecutionDecisionKind::HoldForEstimate));
        let needs_spill = request.bounded_report.decisions.iter().any(|d| {
            matches!(
                d.kind,
                VortexBoundedExecutionDecisionKind::BlockSpill
                    | VortexBoundedExecutionDecisionKind::HoldForMemory
                    | VortexBoundedExecutionDecisionKind::HoldForEstimate
            )
        });
        if !needs_spill {
            return Ok(Self::not_required(request));
        }
        let mut out = Self::planned(request);
        out.reservation_required = true;
        out.mode = VortexBoundedSpillIntegrationMode::ReservationPlanning;
        out.status = VortexBoundedSpillIntegrationStatus::ReservationRequired;
        if let Some(r) = plan_bounded_execution_spill_reservation(
            &out.request.bounded_report,
            out.request.lifecycle_request.clone(),
        )? {
            out.reservation_status = Some(r.status.as_str().to_string());
            out.status = match r.status {
                SpillReservationIntegrationStatus::LifecycleReady => {
                    VortexBoundedSpillIntegrationStatus::ReservationReady
                }
                SpillReservationIntegrationStatus::NeedsEstimate => {
                    VortexBoundedSpillIntegrationStatus::BlockedByMissingEstimate
                }
                SpillReservationIntegrationStatus::BlockedByPolicy => {
                    VortexBoundedSpillIntegrationStatus::BlockedBySpillPolicy
                }
                SpillReservationIntegrationStatus::Unsupported => {
                    VortexBoundedSpillIntegrationStatus::Unsupported
                }
                SpillReservationIntegrationStatus::NeedsWorkspace
                | SpillReservationIntegrationStatus::LifecycleDeferred
                | SpillReservationIntegrationStatus::Planned
                | SpillReservationIntegrationStatus::NotRequired => {
                    VortexBoundedSpillIntegrationStatus::ReservationRequired
                }
            };
            out.spill_reservation_report = Some(r);
        } else if blocked_by_memory {
            out.status = VortexBoundedSpillIntegrationStatus::BlockedByMemoryPolicy;
        } else if blocked_by_estimate {
            out.status = VortexBoundedSpillIntegrationStatus::BlockedByMissingEstimate;
        }
        if out.request.allow_synthetic_payload_roundtrip {
            match (&out.request.payload_fs_ref, &out.request.synthetic_payload) {
                (Some(fs), Some(payload)) if !out.status.is_error() => {
                    out.payload_write_allowed = true;
                    let req = SpillPayloadRoundTripRequest::new(SpillPayloadWriteRequest::new(
                        fs.clone(),
                        payload.clone(),
                    ));
                    let rep = roundtrip_spill_payload(req)?;
                    out.payload_written = rep.payload_written();
                    out.payload_read = rep.payload_read();
                    out.cleanup_performed = rep.cleanup_performed();
                    out.spill_data_is_synthetic = out.payload_written && out.payload_read;
                    out.mode = VortexBoundedSpillIntegrationMode::SyntheticPayloadPlanning;
                    out.status = VortexBoundedSpillIntegrationStatus::PayloadWriteAllowed;
                    if !rep.has_errors()
                        && rep.payload_written()
                        && rep.payload_read()
                        && out.status.allows_payload_roundtrip_available()
                    {
                        out.mode = VortexBoundedSpillIntegrationMode::SyntheticPayloadAvailable;
                        out.status = VortexBoundedSpillIntegrationStatus::PayloadRoundTripAvailable;
                        out.spill_data_is_synthetic = true;
                    } else {
                        match rep.status.as_str() {
                            "feature_disabled" => {
                                out.status =
                                    VortexBoundedSpillIntegrationStatus::BlockedByFeatureGate;
                                out.spill_data_is_synthetic = false;
                                out.payload_write_allowed = false;
                            }
                            "unsupported" => {
                                out.status = VortexBoundedSpillIntegrationStatus::Unsupported;
                                out.payload_write_allowed = false;
                            }
                            _ if rep.has_errors() => {
                                out.status =
                                    VortexBoundedSpillIntegrationStatus::BlockedBySpillPolicy;
                                out.payload_write_allowed = false;
                            }
                            _ => {}
                        }
                    }
                    out.payload_roundtrip_report = Some(rep);
                    if out.status.is_error() {
                        out.payload_write_allowed = false;
                    }
                }
                _ => {
                    out.mode = VortexBoundedSpillIntegrationMode::SyntheticPayloadPlanning;
                    if !out.status.is_blocking() {
                        out.status = VortexBoundedSpillIntegrationStatus::PayloadPlanReady;
                    }
                    out.add_diagnostic(Diagnostic::invalid_input(
                        "vortex_bounded_spill_integration",
                        "synthetic payload roundtrip requested but payload refs or reservation readiness are missing",
                        "provide payload fs ref and synthetic payload and ensure reservation is ready",
                    ));
                }
            }
        }
        Ok(out)
    }
    pub fn not_required(request: VortexBoundedSpillIntegrationRequest) -> Self {
        Self::base(
            VortexBoundedSpillIntegrationStatus::NotRequired,
            VortexBoundedSpillIntegrationMode::ReportOnly,
            request,
        )
    }
    pub fn planned(request: VortexBoundedSpillIntegrationRequest) -> Self {
        Self::base(
            VortexBoundedSpillIntegrationStatus::Planned,
            VortexBoundedSpillIntegrationMode::ReportOnly,
            request,
        )
    }
    pub fn blocked(
        request: VortexBoundedSpillIntegrationRequest,
        status: VortexBoundedSpillIntegrationStatus,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::base(
            status,
            VortexBoundedSpillIntegrationMode::Unsupported,
            request,
        );
        s.add_diagnostic(Diagnostic::no_fallback_execution(reason.into()));
        s
    }
    pub fn unsupported(
        request: VortexBoundedSpillIntegrationRequest,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::base(
            VortexBoundedSpillIntegrationStatus::Unsupported,
            VortexBoundedSpillIntegrationMode::Unsupported,
            request,
        );
        s.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            feature,
            "Unsupported bounded spill integration behavior.",
            Some(format!("{}. Fallback attempted: false", reason.into())),
        ));
        s
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.request.has_errors()
            || self
                .spill_reservation_report
                .as_ref()
                .is_some_and(SpillReservationIntegrationReport::has_errors)
            || self
                .payload_roundtrip_report
                .as_ref()
                .is_some_and(SpillPayloadRoundTripReport::has_errors)
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    pub const fn is_side_effect_free(&self) -> bool {
        !self.payload_written
            && !self.payload_read
            && !self.cleanup_performed
            && !self.query_spill_data_written
            && !self.object_store_io
            && !self.output_dataset_write
            && !self.fallback_execution_allowed
    }
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "Vortex bounded spill integration report
status: {}
mode: {}
reservation required: {}",
            self.status.as_str(),
            self.mode.as_str(),
            self.reservation_required
        );
        if let Some(status) = &self.reservation_status {
            let _ = writeln!(out, "reservation status: {status}");
        }
        if let Some(roundtrip_report) = &self.payload_roundtrip_report {
            let _ = writeln!(
                out,
                "nested payload roundtrip status: {}",
                roundtrip_report.status.as_str()
            );
        }
        let _ = write!(
            out,
            "payload write allowed: {}
payload written: {}
payload read: {}
cleanup performed: {}
spill data is synthetic: {}
query spill data written: {}
object-store IO: {}
output dataset write: {}
fallback execution: disabled",
            self.payload_write_allowed,
            self.payload_written,
            self.payload_read,
            self.cleanup_performed,
            self.spill_data_is_synthetic,
            self.query_spill_data_written,
            self.object_store_io,
            self.output_dataset_write
        );
        out
    }
}

/// # Errors
/// Returns an error if planning the `VortexBoundedSpillIntegrationReport` fails.
pub fn plan_bounded_execution_spill_payload_integration(
    bounded_report: VortexBoundedExecutionReport,
    lifecycle_request: Option<SpillLifecycleRequest>,
    payload_fs_ref: Option<SpillPayloadFsRef>,
    synthetic_payload: Option<SyntheticSpillPayload>,
) -> Result<VortexBoundedSpillIntegrationReport> {
    let request = VortexBoundedSpillIntegrationRequest::new(bounded_report)
        .with_lifecycle_request_opt(lifecycle_request)
        .with_payload_fs_ref_opt(payload_fs_ref)
        .with_synthetic_payload_opt(synthetic_payload);
    VortexBoundedSpillIntegrationReport::from_request(request)
}

/// # Errors
/// Returns an error if planning or executing synthetic roundtrip for `VortexBoundedSpillIntegrationReport` fails.
pub fn plan_bounded_execution_spill_payload_roundtrip(
    bounded_report: VortexBoundedExecutionReport,
    lifecycle_request: Option<SpillLifecycleRequest>,
    payload_fs_ref: Option<SpillPayloadFsRef>,
    synthetic_payload: Option<SyntheticSpillPayload>,
) -> Result<VortexBoundedSpillIntegrationReport> {
    let request = VortexBoundedSpillIntegrationRequest::new(bounded_report)
        .with_lifecycle_request_opt(lifecycle_request)
        .with_payload_fs_ref_opt(payload_fs_ref)
        .with_synthetic_payload_opt(synthetic_payload)
        .allow_synthetic_payload_roundtrip(true);
    VortexBoundedSpillIntegrationReport::from_request(request)
}

/// # Errors
/// Returns an error if planning the recovery integration report fails.
pub fn plan_bounded_spill_recovery(
    report: &VortexBoundedSpillIntegrationReport,
) -> Result<ShardLoomRecoveryIntegrationReport> {
    let mut request = ShardLoomRecoveryIntegrationRequest::new()
        .with_bounded_spill_report_summary(report.to_human_text());
    if report.payload_written && !report.cleanup_performed {
        if let Some(payload_ref) = &report.request.payload_fs_ref {
            request.add_artifact(RecoveryArtifactRef::synthetic_spill_payload(payload_ref));
        } else {
            request.add_artifact(RecoveryArtifactRef::unknown(
                "synthetic-spill-payload",
                "payload was written but payload filesystem reference is unavailable",
            ));
        }
    }
    plan_recovery_integration(request)
}

impl VortexBoundedSpillIntegrationRequest {
    fn with_lifecycle_request_opt(self, v: Option<SpillLifecycleRequest>) -> Self {
        match v {
            Some(x) => self.with_lifecycle_request(x),
            None => self,
        }
    }
    fn with_payload_fs_ref_opt(self, v: Option<SpillPayloadFsRef>) -> Self {
        match v {
            Some(x) => self.with_payload_fs_ref(x),
            None => self,
        }
    }
    fn with_synthetic_payload_opt(self, v: Option<SyntheticSpillPayload>) -> Self {
        match v {
            Some(x) => self.with_synthetic_payload(x),
            None => self,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::DatasetUri;
    use shardloom_exec::{
        SpillPayloadId, SpillPayloadPath, SpillPayloadRef, SpillWorkspaceId, SpillWorkspacePath,
    };
    fn sample_bounded() -> VortexBoundedExecutionReport {
        let req = crate::VortexQueryPrimitiveRequest::count_all(
            DatasetUri::new("file://tmp/test.vortex").expect("uri"),
        );
        let local = VortexLocalExecutionReport::unsupported(
            crate::VortexLocalExecutionInput::new(req),
            "test",
            "unsupported",
        );
        let policy = VortexBoundedExecutionPolicy::new(MemoryBudget::from_gib(1).expect("budget"));
        VortexBoundedExecutionReport::unsupported(
            VortexBoundedExecutionInput::new(local, policy),
            "test",
            "unsupported",
        )
    }
    #[test]
    fn spill_integration_status_checks() {
        assert!(!VortexBoundedSpillIntegrationStatus::NotRequired.is_error());
        assert!(VortexBoundedSpillIntegrationStatus::BlockedBySpillPolicy.is_error());
    }
    #[test]
    fn spill_integration_mode_report_only_flags() {
        assert!(!VortexBoundedSpillIntegrationMode::ReportOnly.writes_query_spill_data());
        assert!(!VortexBoundedSpillIntegrationMode::ReportOnly.touches_object_store());
    }
    #[test]
    fn request_defaults() {
        let req = VortexBoundedSpillIntegrationRequest::new(sample_bounded());
        assert!(!req.allow_synthetic_payload_roundtrip);
    }
    fn blocked_spill_bounded() -> VortexBoundedExecutionReport {
        let mut report = sample_bounded();
        report.decisions = vec![VortexBoundedExecutionDecision::block_spill(
            None,
            "spill required for bounded execution",
        )];
        report.add_diagnostic(Diagnostic::invalid_input(
            "bounded_execution",
            "forced blocked status for reservation test",
            "test-only blocked diagnostic",
        ));
        report
    }
    fn sample_payload_fs_ref() -> SpillPayloadFsRef {
        let payload_id = SpillPayloadId::new("payload-1").expect("payload id");
        let payload_ref = SpillPayloadRef::new(payload_id, "workspace-a").expect("payload ref");
        let workspace =
            SpillPayloadPath::new(std::env::temp_dir().display().to_string()).expect("workspace");
        SpillPayloadFsRef::new(payload_ref, workspace)
    }
    fn lifecycle_request_with_error() -> SpillLifecycleRequest {
        let mut req = SpillLifecycleRequest::report_only(
            SpillWorkspaceId::new("ws-1").expect("workspace id"),
            SpillWorkspacePath::new(std::env::temp_dir().display().to_string())
                .expect("workspace path"),
        );
        req.add_diagnostic(Diagnostic::invalid_input(
            "spill_lifecycle",
            "forced blocking diagnostic for test",
            "keep test deterministic",
        ));
        req
    }
    #[test]
    fn feature_disabled_roundtrip_not_available() {
        let report = plan_bounded_execution_spill_payload_roundtrip(
            blocked_spill_bounded(),
            None,
            Some(sample_payload_fs_ref()),
            Some(SyntheticSpillPayload::from_text("abc").expect("payload")),
        )
        .expect("report");
        assert_ne!(
            report.status,
            VortexBoundedSpillIntegrationStatus::PayloadRoundTripAvailable
        );
        assert_ne!(
            report.mode,
            VortexBoundedSpillIntegrationMode::SyntheticPayloadAvailable
        );
        assert!(!report.payload_written);
        assert!(!report.payload_read);
        assert!(!report.payload_write_allowed);
        assert!(!report.query_spill_data_written);
        assert!(!report.object_store_io);
        assert!(!report.output_dataset_write);
        assert!(!report.fallback_execution_allowed);
    }
    #[test]
    fn blocked_reservation_not_downgraded_to_payload_plan_ready() {
        let report = plan_bounded_execution_spill_payload_roundtrip(
            blocked_spill_bounded(),
            Some(lifecycle_request_with_error()),
            None,
            None,
        )
        .expect("report");
        assert_ne!(
            report.status,
            VortexBoundedSpillIntegrationStatus::PayloadPlanReady
        );
        assert!(report.has_errors());
        assert!(!report.fallback_execution_allowed);
    }
    #[test]
    fn missing_payload_inputs_plans_when_not_blocked() {
        let report = plan_bounded_execution_spill_payload_roundtrip(
            blocked_spill_bounded(),
            None,
            None,
            None,
        )
        .expect("report");
        assert_ne!(
            report.status,
            VortexBoundedSpillIntegrationStatus::PayloadRoundTripAvailable
        );
        assert!(!report.payload_written);
        assert!(!report.payload_read);
    }
    #[test]
    fn bounded_spill_recovery_no_payload_written_is_cleanup_not_required() {
        let report = plan_bounded_execution_spill_payload_integration(
            blocked_spill_bounded(),
            None,
            None,
            None,
        )
        .expect("spill report");
        let recovery = plan_bounded_spill_recovery(&report).expect("recovery report");
        assert_eq!(recovery.status.as_str(), "cleanup_not_required");
        assert!(recovery.is_side_effect_free());
        assert!(!recovery.fallback_execution.allowed());
    }
    #[test]
    fn bounded_spill_recovery_payload_written_without_cleanup_requires_cleanup_or_unknown() {
        let mut report = plan_bounded_execution_spill_payload_integration(
            blocked_spill_bounded(),
            None,
            None,
            None,
        )
        .expect("spill report");
        report.payload_written = true;
        report.cleanup_performed = false;
        report.request.payload_fs_ref = Some(sample_payload_fs_ref());
        let recovery = plan_bounded_spill_recovery(&report).expect("recovery report");
        assert!(matches!(
            recovery.status.as_str(),
            "cleanup_required" | "retry_allowed_after_cleanup" | "blocked_by_unknown_artifact"
        ));
        assert!(recovery.is_side_effect_free());
        assert!(!recovery.fallback_execution.allowed());
    }
}
