#![allow(
    clippy::must_use_candidate,
    clippy::return_self_not_must_use,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::too_many_lines,
    clippy::struct_excessive_bools
)]

use std::fmt::Write as _;

use shardloom_core::{Diagnostic, DiagnosticCode, DiagnosticSeverity, Result, SegmentId};
use shardloom_exec::TaskId;

use crate::{
    VortexExecutionReadinessReport, VortexExecutionReadinessStatus, VortexSchedulingDecisionKind,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataExecutorFeatureStatus {
    Disabled,
    Enabled,
    Unsupported,
}
impl VortexMetadataExecutorFeatureStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Enabled => "enabled",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_enabled(&self) -> bool {
        matches!(self, Self::Enabled)
    }
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataExecutionStatus {
    FeatureDisabled,
    Ready,
    ExecutedMetadataOnly,
    NoTasksRequired,
    BlockedByReadiness,
    BlockedByDataRead,
    BlockedByMaterialization,
    BlockedByObjectStoreIo,
    BlockedByWriteIo,
    BlockedBySpillIo,
    BlockedByExternalEffect,
    Unsupported,
}
impl VortexMetadataExecutionStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::Ready => "ready",
            Self::ExecutedMetadataOnly => "executed_metadata_only",
            Self::NoTasksRequired => "no_tasks_required",
            Self::BlockedByReadiness => "blocked_by_readiness",
            Self::BlockedByDataRead => "blocked_by_data_read",
            Self::BlockedByMaterialization => "blocked_by_materialization",
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
            Self::BlockedByReadiness
                | Self::BlockedByDataRead
                | Self::BlockedByMaterialization
                | Self::BlockedByObjectStoreIo
                | Self::BlockedByWriteIo
                | Self::BlockedBySpillIo
                | Self::BlockedByExternalEffect
                | Self::Unsupported
        )
    }
    pub const fn executed_anything(&self) -> bool {
        matches!(self, Self::ExecutedMetadataOnly)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataExecutionMode {
    ReportOnly,
    MetadataOnly,
    Unsupported,
}
impl VortexMetadataExecutionMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::MetadataOnly => "metadata_only",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn reads_data(&self) -> bool {
        false
    }
    pub const fn materializes_data(&self) -> bool {
        false
    }
    pub const fn writes_data(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataExecutableDecisionKind {
    NoOp,
    MetadataOnly,
    BlockedWouldReadData,
    BlockedWouldMaterialize,
    BlockedWouldWrite,
    BlockedWouldUseObjectStore,
    BlockedWouldSpill,
    BlockedWouldMemory,
    BlockedExternalEffect,
    BlockedUnsupported,
}
impl VortexMetadataExecutableDecisionKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NoOp => "no_op",
            Self::MetadataOnly => "metadata_only",
            Self::BlockedWouldReadData => "blocked_would_read_data",
            Self::BlockedWouldMaterialize => "blocked_would_materialize",
            Self::BlockedWouldWrite => "blocked_would_write",
            Self::BlockedWouldUseObjectStore => "blocked_would_use_object_store",
            Self::BlockedWouldSpill => "blocked_would_spill",
            Self::BlockedWouldMemory => "blocked_would_memory",
            Self::BlockedExternalEffect => "blocked_external_effect",
            Self::BlockedUnsupported => "blocked_unsupported",
        }
    }
    pub const fn is_executable_metadata_only(&self) -> bool {
        matches!(self, Self::NoOp | Self::MetadataOnly)
    }
    pub const fn is_blocked(&self) -> bool {
        !self.is_executable_metadata_only()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexMetadataExecutionDecision {
    pub kind: VortexMetadataExecutableDecisionKind,
    pub task_id: Option<TaskId>,
    pub segment_id: Option<SegmentId>,
    pub scheduler_decision_kind: Option<String>,
    pub reason: String,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexMetadataExecutionDecision {
    fn base(
        kind: VortexMetadataExecutableDecisionKind,
        task_id: Option<TaskId>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            task_id,
            segment_id: None,
            scheduler_decision_kind: None,
            reason: reason.into(),
            diagnostics: vec![],
        }
    }
    pub fn no_op(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(VortexMetadataExecutableDecisionKind::NoOp, task_id, reason)
    }
    pub fn metadata_only(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexMetadataExecutableDecisionKind::MetadataOnly,
            task_id,
            reason,
        )
    }
    pub fn blocked_would_read_data(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexMetadataExecutableDecisionKind::BlockedWouldReadData,
            task_id,
            reason,
        )
    }
    pub fn blocked_would_materialize(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexMetadataExecutableDecisionKind::BlockedWouldMaterialize,
            task_id,
            reason,
        )
    }
    pub fn blocked_would_write(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexMetadataExecutableDecisionKind::BlockedWouldWrite,
            task_id,
            reason,
        )
    }
    pub fn blocked_would_use_object_store(
        task_id: Option<TaskId>,
        reason: impl Into<String>,
    ) -> Self {
        Self::base(
            VortexMetadataExecutableDecisionKind::BlockedWouldUseObjectStore,
            task_id,
            reason,
        )
    }
    pub fn blocked_would_spill(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexMetadataExecutableDecisionKind::BlockedWouldSpill,
            task_id,
            reason,
        )
    }
    pub fn blocked_would_memory(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexMetadataExecutableDecisionKind::BlockedWouldMemory,
            task_id,
            reason,
        )
    }
    pub fn blocked_external_effect(task_id: Option<TaskId>, reason: impl Into<String>) -> Self {
        Self::base(
            VortexMetadataExecutableDecisionKind::BlockedExternalEffect,
            task_id,
            reason,
        )
    }
    pub fn blocked_unsupported(
        task_id: Option<TaskId>,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::base(
            VortexMetadataExecutableDecisionKind::BlockedUnsupported,
            task_id,
            "unsupported metadata-only executor decision",
        );
        s.diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback attempted: false".to_string()),
        ));
        s
    }
    pub fn with_segment_id(mut self, segment_id: SegmentId) -> Self {
        self.segment_id = Some(segment_id);
        self
    }
    pub fn with_scheduler_decision_kind(
        mut self,
        scheduler_decision_kind: impl Into<String>,
    ) -> Self {
        self.scheduler_decision_kind = Some(scheduler_decision_kind.into());
        self
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    pub const fn is_executable_metadata_only(&self) -> bool {
        self.kind.is_executable_metadata_only()
    }
    pub const fn is_blocked(&self) -> bool {
        self.kind.is_blocked()
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
            "metadata-only executor decision kind={} reason={}",
            self.kind.as_str(),
            self.reason
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexMetadataExecutionInput {
    pub readiness_report: VortexExecutionReadinessReport,
    pub allow_metadata_only_execution: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexMetadataExecutionInput {
    pub fn new(readiness_report: VortexExecutionReadinessReport) -> Self {
        Self {
            readiness_report,
            allow_metadata_only_execution: true,
            diagnostics: vec![],
        }
    }
    pub const fn allow_metadata_only_execution(mut self, value: bool) -> Self {
        self.allow_metadata_only_execution = value;
        self
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    pub fn has_errors(&self) -> bool {
        self.readiness_report.has_errors()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    pub fn summary(&self) -> String {
        format!(
            "metadata-only execution input allow_metadata_only_execution={}",
            self.allow_metadata_only_execution
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexMetadataExecutionReport {
    pub feature_status: VortexMetadataExecutorFeatureStatus,
    pub status: VortexMetadataExecutionStatus,
    pub mode: VortexMetadataExecutionMode,
    pub input: VortexMetadataExecutionInput,
    pub decisions: Vec<VortexMetadataExecutionDecision>,
    pub metadata_tasks_executed: usize,
    pub no_op_tasks_completed: usize,
    pub blocked_task_count: usize,
    pub tasks_that_would_read_data: usize,
    pub tasks_that_would_materialize: usize,
    pub tasks_that_would_write: usize,
    pub tasks_that_would_use_object_store: usize,
    pub tasks_that_would_spill: usize,
    pub external_effect_tasks_blocked: usize,
    pub data_read: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexMetadataExecutionReport {
    pub fn feature_disabled(input: VortexMetadataExecutionInput) -> Self {
        Self {
            feature_status: VortexMetadataExecutorFeatureStatus::Disabled,
            status: VortexMetadataExecutionStatus::FeatureDisabled,
            mode: VortexMetadataExecutionMode::ReportOnly,
            input,
            decisions: vec![],
            metadata_tasks_executed: 0,
            no_op_tasks_completed: 0,
            blocked_task_count: 0,
            tasks_that_would_read_data: 0,
            tasks_that_would_materialize: 0,
            tasks_that_would_write: 0,
            tasks_that_would_use_object_store: 0,
            tasks_that_would_spill: 0,
            external_effect_tasks_blocked: 0,
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
    pub fn unsupported(
        input: VortexMetadataExecutionInput,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::feature_disabled(input);
        s.feature_status = VortexMetadataExecutorFeatureStatus::Unsupported;
        s.status = VortexMetadataExecutionStatus::Unsupported;
        s.mode = VortexMetadataExecutionMode::Unsupported;
        s.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback attempted: false".to_string()),
        ));
        s
    }
    pub fn from_input(input: VortexMetadataExecutionInput) -> Result<Self> {
        if !vortex_metadata_executor_feature_enabled() {
            return Ok(Self::feature_disabled(input));
        }
        let mut out = Self::feature_disabled(input.clone());
        out.feature_status = VortexMetadataExecutorFeatureStatus::Enabled;
        out.status = VortexMetadataExecutionStatus::Ready;
        out.mode = VortexMetadataExecutionMode::MetadataOnly;
        out.diagnostics.extend(input.diagnostics.clone());
        out.diagnostics
            .extend(input.readiness_report.diagnostics.clone());
        out.diagnostics
            .extend(input.readiness_report.input.diagnostics.clone());
        out.status = match input.readiness_report.status {
            VortexExecutionReadinessStatus::Unsupported => {
                VortexMetadataExecutionStatus::Unsupported
            }
            VortexExecutionReadinessStatus::BlockedByUnsupportedInput
            | VortexExecutionReadinessStatus::BlockedByMissingMetadata
            | VortexExecutionReadinessStatus::BlockedByMissingEstimate
            | VortexExecutionReadinessStatus::BlockedByMemoryPolicy
            | VortexExecutionReadinessStatus::BlockedBySpillPolicy
            | VortexExecutionReadinessStatus::BlockedByFeatureGate => {
                VortexMetadataExecutionStatus::BlockedByReadiness
            }
            _ => out.status,
        };
        for d in &input.readiness_report.input.scheduler_report.decisions {
            let mapped = match d.kind {
                VortexSchedulingDecisionKind::SkipPruned => {
                    VortexMetadataExecutionDecision::no_op(d.task_id.clone(), &d.reason)
                }
                VortexSchedulingDecisionKind::ScheduleMetadataOnly => {
                    VortexMetadataExecutionDecision::metadata_only(d.task_id.clone(), &d.reason)
                }
                VortexSchedulingDecisionKind::ScheduleNow => {
                    VortexMetadataExecutionDecision::blocked_would_read_data(
                        d.task_id.clone(),
                        &d.reason,
                    )
                }
                VortexSchedulingDecisionKind::HoldForEstimate => {
                    VortexMetadataExecutionDecision::blocked_would_materialize(
                        d.task_id.clone(),
                        &d.reason,
                    )
                }
                VortexSchedulingDecisionKind::HoldForMemory => {
                    VortexMetadataExecutionDecision::blocked_would_memory(
                        d.task_id.clone(),
                        &d.reason,
                    )
                }
                VortexSchedulingDecisionKind::HoldForSpillSupport => {
                    VortexMetadataExecutionDecision::blocked_would_spill(
                        d.task_id.clone(),
                        &d.reason,
                    )
                }
                VortexSchedulingDecisionKind::Unsupported => {
                    VortexMetadataExecutionDecision::blocked_unsupported(
                        d.task_id.clone(),
                        "vortex_metadata_executor",
                        &d.reason,
                    )
                }
            }
            .with_scheduler_decision_kind(d.kind.as_str());
            out.add_decision(mapped);
        }
        out.recompute_counts();
        if !matches!(
            out.status,
            VortexMetadataExecutionStatus::Unsupported
                | VortexMetadataExecutionStatus::BlockedByReadiness
        ) {
            if out.decisions.is_empty() {
                out.status = VortexMetadataExecutionStatus::NoTasksRequired;
            } else if out.blocked_task_count > 0 {
                out.status = if out.tasks_that_would_read_data > 0 {
                    VortexMetadataExecutionStatus::BlockedByDataRead
                } else if out.tasks_that_would_materialize > 0 {
                    VortexMetadataExecutionStatus::BlockedByMaterialization
                } else if out.tasks_that_would_write > 0 {
                    VortexMetadataExecutionStatus::BlockedByWriteIo
                } else if out.tasks_that_would_use_object_store > 0 {
                    VortexMetadataExecutionStatus::BlockedByObjectStoreIo
                } else if out.tasks_that_would_spill > 0 {
                    VortexMetadataExecutionStatus::BlockedBySpillIo
                } else if out.external_effect_tasks_blocked > 0 {
                    VortexMetadataExecutionStatus::BlockedByExternalEffect
                } else {
                    VortexMetadataExecutionStatus::BlockedByReadiness
                };
            } else {
                out.status = VortexMetadataExecutionStatus::ExecutedMetadataOnly;
            }
        }
        Ok(out)
    }
    pub fn add_decision(&mut self, d: VortexMetadataExecutionDecision) {
        self.decisions.push(d);
    }
    pub fn add_diagnostic(&mut self, d: Diagnostic) {
        self.diagnostics.push(d);
    }
    pub fn recompute_counts(&mut self) {
        self.metadata_tasks_executed = self
            .decisions
            .iter()
            .filter(|d| matches!(d.kind, VortexMetadataExecutableDecisionKind::MetadataOnly))
            .count();
        self.no_op_tasks_completed = self
            .decisions
            .iter()
            .filter(|d| matches!(d.kind, VortexMetadataExecutableDecisionKind::NoOp))
            .count();
        self.blocked_task_count = self.decisions.iter().filter(|d| d.is_blocked()).count();
        self.tasks_that_would_read_data = self
            .decisions
            .iter()
            .filter(|d| {
                matches!(
                    d.kind,
                    VortexMetadataExecutableDecisionKind::BlockedWouldReadData
                )
            })
            .count();
        self.tasks_that_would_materialize = self
            .decisions
            .iter()
            .filter(|d| {
                matches!(
                    d.kind,
                    VortexMetadataExecutableDecisionKind::BlockedWouldMaterialize
                )
            })
            .count();
        self.tasks_that_would_write = self
            .decisions
            .iter()
            .filter(|d| {
                matches!(
                    d.kind,
                    VortexMetadataExecutableDecisionKind::BlockedWouldWrite
                )
            })
            .count();
        self.tasks_that_would_use_object_store = self
            .decisions
            .iter()
            .filter(|d| {
                matches!(
                    d.kind,
                    VortexMetadataExecutableDecisionKind::BlockedWouldUseObjectStore
                )
            })
            .count();
        self.tasks_that_would_spill = self
            .decisions
            .iter()
            .filter(|d| {
                matches!(
                    d.kind,
                    VortexMetadataExecutableDecisionKind::BlockedWouldSpill
                )
            })
            .count();
        self.external_effect_tasks_blocked = self
            .decisions
            .iter()
            .filter(|d| {
                matches!(
                    d.kind,
                    VortexMetadataExecutableDecisionKind::BlockedExternalEffect
                )
            })
            .count();
    }
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self
                .diagnostics
                .iter()
                .chain(self.input.diagnostics.iter())
                .any(|d| {
                    matches!(
                        d.severity,
                        DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                    )
                })
            || self
                .decisions
                .iter()
                .any(VortexMetadataExecutionDecision::has_errors)
            || self.input.has_errors()
    }
    pub const fn is_side_effect_free(&self) -> bool {
        !self.data_read
            && !self.data_materialized
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_effects_executed
            && !self.fallback_execution_allowed
    }
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "Vortex metadata-only executor report");
        let _ = writeln!(out, "feature status: {}", self.feature_status.as_str());
        let _ = writeln!(out, "execution status: {}", self.status.as_str());
        let _ = writeln!(out, "mode: {}", self.mode.as_str());
        let _ = writeln!(
            out,
            "metadata tasks executed: {}",
            self.metadata_tasks_executed
        );
        let _ = writeln!(out, "no-op tasks completed: {}", self.no_op_tasks_completed);
        let _ = writeln!(out, "blocked task count: {}", self.blocked_task_count);
        let _ = writeln!(
            out,
            "tasks that would read data: {}",
            self.tasks_that_would_read_data
        );
        let _ = writeln!(
            out,
            "tasks that would materialize: {}",
            self.tasks_that_would_materialize
        );
        let _ = writeln!(
            out,
            "tasks that would write: {}",
            self.tasks_that_would_write
        );
        let _ = writeln!(
            out,
            "tasks that would use object store: {}",
            self.tasks_that_would_use_object_store
        );
        let _ = writeln!(
            out,
            "tasks that would spill: {}",
            self.tasks_that_would_spill
        );
        let _ = writeln!(
            out,
            "external effect tasks blocked: {}",
            self.external_effect_tasks_blocked
        );
        let _ = writeln!(out, "data read: false");
        let _ = writeln!(out, "data materialized: false");
        let _ = writeln!(out, "object-store IO: false");
        let _ = writeln!(out, "write IO: false");
        let _ = writeln!(out, "spill IO performed: false");
        let _ = writeln!(out, "external effects executed: false");
        let _ = writeln!(out, "fallback execution disabled");
        if !self.diagnostics.is_empty() {
            let _ = writeln!(out, "diagnostics:");
            for d in &self.diagnostics {
                let _ = writeln!(out, "- {}: {}", d.code.as_str(), d.message);
            }
        }
        out
    }
}

pub const fn vortex_metadata_executor_feature_enabled() -> bool {
    cfg!(feature = "vortex-metadata-executor")
}

/// Execute `Vortex` metadata-only decisions for `ShardLoom` planning contracts.
/// # Errors
/// Returns an error only if building the metadata-only report fails.
pub fn execute_vortex_metadata_only(
    readiness_report: VortexExecutionReadinessReport,
) -> Result<VortexMetadataExecutionReport> {
    VortexMetadataExecutionReport::from_input(VortexMetadataExecutionInput::new(readiness_report))
}

pub fn vortex_metadata_execution_is_side_effect_free(
    report: &VortexMetadataExecutionReport,
) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        VortexMemoryBridgeInput, VortexMemoryBridgeReport, evaluate_vortex_execution_readiness,
        plan_vortex_scheduler_queue,
    };
    use shardloom_exec::MemoryBudget;
    fn sample_readiness() -> VortexExecutionReadinessReport {
        let memory = VortexMemoryBridgeReport::from_input(VortexMemoryBridgeInput::new(
            MemoryBudget::from_gib(1).expect("budget"),
        ))
        .expect("memory");
        let sched = plan_vortex_scheduler_queue(memory, 1).expect("sched");
        evaluate_vortex_execution_readiness(sched).expect("ready")
    }
    #[cfg(not(feature = "vortex-metadata-executor"))]
    #[test]
    fn feature_flag_default_false() {
        assert!(!vortex_metadata_executor_feature_enabled());
    }
    #[test]
    fn disabled_not_enabled() {
        assert!(!VortexMetadataExecutorFeatureStatus::Disabled.is_enabled());
    }
    #[test]
    fn executed_status_anything() {
        assert!(VortexMetadataExecutionStatus::ExecutedMetadataOnly.executed_anything());
    }
    #[test]
    fn mode_no_data() {
        assert!(!VortexMetadataExecutionMode::MetadataOnly.reads_data());
        assert!(!VortexMetadataExecutionMode::MetadataOnly.materializes_data());
        assert!(!VortexMetadataExecutionMode::MetadataOnly.writes_data());
    }
    #[test]
    fn kind_checks() {
        assert!(VortexMetadataExecutableDecisionKind::NoOp.is_executable_metadata_only());
        assert!(VortexMetadataExecutableDecisionKind::BlockedWouldReadData.is_blocked());
    }
    #[test]
    fn blocked_unsupported_has_error() {
        let d = VortexMetadataExecutionDecision::blocked_unsupported(None, "f", "r");
        assert!(d.has_errors());
        assert!(
            d.diagnostics
                .iter()
                .any(|x| x.suggested_next_step.as_deref() == Some("Fallback attempted: false"))
        );
    }
    #[test]
    fn feature_disabled_safe() {
        let rpt = VortexMetadataExecutionReport::feature_disabled(
            VortexMetadataExecutionInput::new(sample_readiness()),
        );
        assert!(rpt.is_side_effect_free());
        assert!(!rpt.fallback_execution_allowed);
    }
    #[cfg(not(feature = "vortex-metadata-executor"))]
    #[test]
    fn execute_default_feature_disabled() {
        let r = execute_vortex_metadata_only(sample_readiness()).expect("ok");
        assert_eq!(r.status, VortexMetadataExecutionStatus::FeatureDisabled);
    }
    #[cfg(feature = "vortex-metadata-executor")]
    #[test]
    fn feature_flag_enabled_true() {
        assert!(vortex_metadata_executor_feature_enabled());
    }
    #[cfg(feature = "vortex-metadata-executor")]
    #[test]
    fn execute_feature_enabled_stays_side_effect_free() {
        let r = execute_vortex_metadata_only(sample_readiness()).expect("ok");
        assert_eq!(
            r.feature_status,
            VortexMetadataExecutorFeatureStatus::Enabled
        );
        assert_ne!(r.status, VortexMetadataExecutionStatus::FeatureDisabled);
        assert!(matches!(
            r.status,
            VortexMetadataExecutionStatus::ExecutedMetadataOnly
                | VortexMetadataExecutionStatus::NoTasksRequired
                | VortexMetadataExecutionStatus::BlockedByDataRead
                | VortexMetadataExecutionStatus::BlockedByMaterialization
                | VortexMetadataExecutionStatus::BlockedByObjectStoreIo
                | VortexMetadataExecutionStatus::BlockedByWriteIo
                | VortexMetadataExecutionStatus::BlockedBySpillIo
                | VortexMetadataExecutionStatus::BlockedByExternalEffect
                | VortexMetadataExecutionStatus::BlockedByReadiness
                | VortexMetadataExecutionStatus::Unsupported
        ));
        assert!(!r.data_read);
        assert!(!r.data_materialized);
        assert!(!r.object_store_io);
        assert!(!r.write_io);
        assert!(!r.spill_io_performed);
        assert!(!r.external_effects_executed);
        assert!(!r.fallback_execution_allowed);
        assert!(r.is_side_effect_free());
    }
    #[cfg(all(feature = "vortex-file-io", feature = "vortex-metadata-executor"))]
    #[test]
    fn file_io_feature_does_not_disable_metadata_executor() {
        assert!(vortex_metadata_executor_feature_enabled());
        let r = execute_vortex_metadata_only(sample_readiness()).expect("ok");
        assert_eq!(
            r.feature_status,
            VortexMetadataExecutorFeatureStatus::Enabled
        );
        assert_ne!(r.status, VortexMetadataExecutionStatus::FeatureDisabled);
        assert!(!r.fallback_execution_allowed);
        assert!(r.is_side_effect_free());
    }
    #[test]
    fn human_text_fields() {
        let mut rpt = VortexMetadataExecutionReport::feature_disabled(
            VortexMetadataExecutionInput::new(sample_readiness()),
        );
        rpt.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            "metadata-executor-test",
            "diagnostic",
            Some("Fallback attempted: false".to_string()),
        ));
        let t = rpt.to_human_text();
        assert!(t.contains("fallback execution disabled"));
        assert!(t.contains("data read: false"));
        assert!(t.contains("data materialized: false"));
        assert!(t.contains("object-store IO: false"));
        assert!(t.contains("spill IO performed: false"));
        assert!(t.contains("diagnostics:"));
    }
    #[test]
    fn side_effect_free_helper() {
        let r = VortexMetadataExecutionReport::feature_disabled(VortexMetadataExecutionInput::new(
            sample_readiness(),
        ));
        assert!(vortex_metadata_execution_is_side_effect_free(&r));
    }
}
