#![allow(
    clippy::must_use_candidate,
    clippy::return_self_not_must_use,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::too_many_lines,
    clippy::if_not_else,
    clippy::struct_excessive_bools
)]

use std::fmt::Write as _;

use shardloom_core::{Diagnostic, DiagnosticCode, DiagnosticSeverity, Result};

use crate::{
    VortexSchedulerBridgeReport, VortexSchedulerBridgeStatus, VortexSchedulingDecisionKind,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexExecutionReadinessStatus {
    ReadyForDryRun,
    ReadyForFutureExecution,
    BlockedByUnsupportedInput,
    BlockedByMissingMetadata,
    BlockedByMissingEstimate,
    BlockedByMemoryPolicy,
    BlockedBySpillPolicy,
    BlockedByFeatureGate,
    NoTasksRequired,
    Unsupported,
}
impl VortexExecutionReadinessStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReadyForDryRun => "ready_for_dry_run",
            Self::ReadyForFutureExecution => "ready_for_future_execution",
            Self::BlockedByUnsupportedInput => "blocked_by_unsupported_input",
            Self::BlockedByMissingMetadata => "blocked_by_missing_metadata",
            Self::BlockedByMissingEstimate => "blocked_by_missing_estimate",
            Self::BlockedByMemoryPolicy => "blocked_by_memory_policy",
            Self::BlockedBySpillPolicy => "blocked_by_spill_policy",
            Self::BlockedByFeatureGate => "blocked_by_feature_gate",
            Self::NoTasksRequired => "no_tasks_required",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::BlockedByUnsupportedInput
                | Self::BlockedByMemoryPolicy
                | Self::BlockedBySpillPolicy
                | Self::BlockedByFeatureGate
                | Self::Unsupported
        )
    }
    pub const fn allows_dry_run(&self) -> bool {
        matches!(
            self,
            Self::ReadyForDryRun | Self::ReadyForFutureExecution | Self::NoTasksRequired
        )
    }
    pub const fn allows_future_execution(&self) -> bool {
        matches!(self, Self::ReadyForFutureExecution)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexDryRunMode {
    ReportOnly,
    ValidateReadiness,
    ValidateTaskGraph,
    ValidateScheduler,
    Unsupported,
}
impl VortexDryRunMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::ReportOnly => "report_only",
            Self::ValidateReadiness => "validate_readiness",
            Self::ValidateTaskGraph => "validate_task_graph",
            Self::ValidateScheduler => "validate_scheduler",
            Self::Unsupported => "unsupported",
        }
    }
    pub const fn executes_tasks(&self) -> bool {
        false
    }
    pub const fn reads_data(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexReadinessGateKind {
    NativeVortexInput,
    MetadataAvailableOrDeferred,
    PruningConservative,
    ReadPlanAvailable,
    RuntimeTaskGraphAvailable,
    AdaptiveSizingAvailable,
    MemoryPlanAvailable,
    SchedulerPlanAvailable,
    NoMissingEstimates,
    NoSpillRequiredWithoutSupport,
    NoUnsupportedDiagnostics,
    NoFallbackExecution,
    NoDataRead,
    NoMaterialization,
    NoObjectStoreIo,
    NoWriteIo,
    NoSpillIo,
    NoExternalEffects,
}
impl VortexReadinessGateKind {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NativeVortexInput => "native_vortex_input",
            Self::MetadataAvailableOrDeferred => "metadata_available_or_deferred",
            Self::PruningConservative => "pruning_conservative",
            Self::ReadPlanAvailable => "read_plan_available",
            Self::RuntimeTaskGraphAvailable => "runtime_task_graph_available",
            Self::AdaptiveSizingAvailable => "adaptive_sizing_available",
            Self::MemoryPlanAvailable => "memory_plan_available",
            Self::SchedulerPlanAvailable => "scheduler_plan_available",
            Self::NoMissingEstimates => "no_missing_estimates",
            Self::NoSpillRequiredWithoutSupport => "no_spill_required_without_support",
            Self::NoUnsupportedDiagnostics => "no_unsupported_diagnostics",
            Self::NoFallbackExecution => "no_fallback_execution",
            Self::NoDataRead => "no_data_read",
            Self::NoMaterialization => "no_materialization",
            Self::NoObjectStoreIo => "no_object_store_io",
            Self::NoWriteIo => "no_write_io",
            Self::NoSpillIo => "no_spill_io",
            Self::NoExternalEffects => "no_external_effects",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexReadinessGateStatus {
    Passed,
    Warning,
    Blocked,
    Failed,
    NotApplicable,
}
impl VortexReadinessGateStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Warning => "warning",
            Self::Blocked => "blocked",
            Self::Failed => "failed",
            Self::NotApplicable => "not_applicable",
        }
    }
    pub const fn is_blocking(&self) -> bool {
        matches!(self, Self::Blocked | Self::Failed)
    }
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Failed)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexReadinessGateResult {
    pub kind: VortexReadinessGateKind,
    pub status: VortexReadinessGateStatus,
    pub reason: String,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexReadinessGateResult {
    fn new(
        kind: VortexReadinessGateKind,
        status: VortexReadinessGateStatus,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            status,
            reason: reason.into(),
            diagnostics: vec![],
        }
    }
    pub fn passed(kind: VortexReadinessGateKind, reason: impl Into<String>) -> Self {
        Self::new(kind, VortexReadinessGateStatus::Passed, reason)
    }
    pub fn warning(kind: VortexReadinessGateKind, reason: impl Into<String>) -> Self {
        Self::new(kind, VortexReadinessGateStatus::Warning, reason)
    }
    pub fn blocked(kind: VortexReadinessGateKind, reason: impl Into<String>) -> Self {
        Self::new(kind, VortexReadinessGateStatus::Blocked, reason)
    }
    pub fn failed(
        kind: VortexReadinessGateKind,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut s = Self::new(kind, VortexReadinessGateStatus::Failed, "gate failed");
        s.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback attempted: false".to_string()),
        ));
        s
    }
    pub fn not_applicable(kind: VortexReadinessGateKind, reason: impl Into<String>) -> Self {
        Self::new(kind, VortexReadinessGateStatus::NotApplicable, reason)
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub const fn is_blocking(&self) -> bool {
        self.status.is_blocking()
    }
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        }) || self.status.is_error()
    }
    pub fn summary(&self) -> String {
        format!(
            "gate={} status={} reason={}",
            self.kind.as_str(),
            self.status.as_str(),
            self.reason
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexDryRunContract {
    pub mode: VortexDryRunMode,
    pub tasks_would_execute: usize,
    pub tasks_would_read_data: usize,
    pub tasks_would_materialize: usize,
    pub tasks_blocked: usize,
    pub data_read: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexDryRunContract {
    pub fn from_scheduler_report(report: &VortexSchedulerBridgeReport) -> Self {
        Self {
            mode: VortexDryRunMode::ValidateScheduler,
            tasks_would_execute: report.scheduled_task_count,
            tasks_would_read_data: 0,
            tasks_would_materialize: 0,
            tasks_blocked: report.blocked_task_count,
            data_read: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics: report.diagnostics.clone(),
        }
    }
    pub fn unsupported(feature: impl Into<String>, reason: impl Into<String>) -> Self {
        let mut s = Self {
            mode: VortexDryRunMode::Unsupported,
            tasks_would_execute: 0,
            tasks_would_read_data: 0,
            tasks_would_materialize: 0,
            tasks_blocked: 0,
            data_read: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            diagnostics: vec![],
        };
        s.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback attempted: false".to_string()),
        ));
        s
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
    pub const fn is_side_effect_free(&self) -> bool {
        !self.data_read
            && !self.data_materialized
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io
            && !self.external_effects_executed
            && !self.fallback_execution_allowed
    }
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "Vortex dry-run contract");
        let _ = writeln!(out, "dry-run mode: {}", self.mode.as_str());
        let _ = writeln!(
            out,
            "tasks that would execute: {}",
            self.tasks_would_execute
        );
        let _ = writeln!(out, "tasks blocked: {}", self.tasks_blocked);
        let _ = writeln!(out, "data read: false");
        let _ = writeln!(out, "data materialized: false");
        let _ = writeln!(out, "object-store IO: false");
        let _ = writeln!(out, "write IO: false");
        let _ = writeln!(out, "spill IO: false");
        let _ = writeln!(out, "external effects executed: false");
        let _ = write!(out, "fallback execution disabled");
        out
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexExecutionReadinessInput {
    pub scheduler_report: VortexSchedulerBridgeReport,
    pub require_all_estimates: bool,
    pub require_spill_support: bool,
    pub require_feature_enabled: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexExecutionReadinessInput {
    pub fn new(scheduler_report: VortexSchedulerBridgeReport) -> Self {
        Self {
            scheduler_report,
            require_all_estimates: true,
            require_spill_support: true,
            require_feature_enabled: true,
            diagnostics: vec![],
        }
    }
    pub const fn require_all_estimates(mut self, value: bool) -> Self {
        self.require_all_estimates = value;
        self
    }
    pub const fn require_spill_support(mut self, value: bool) -> Self {
        self.require_spill_support = value;
        self
    }
    pub const fn require_feature_enabled(mut self, value: bool) -> Self {
        self.require_feature_enabled = value;
        self
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn has_errors(&self) -> bool {
        self.scheduler_report.has_errors()
            || self.diagnostics.iter().any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
    pub fn summary(&self) -> String {
        format!(
            "readiness-input require_all_estimates={} require_spill_support={} require_feature_enabled={}",
            self.require_all_estimates, self.require_spill_support, self.require_feature_enabled
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexExecutionReadinessReport {
    pub status: VortexExecutionReadinessStatus,
    pub input: VortexExecutionReadinessInput,
    pub gates: Vec<VortexReadinessGateResult>,
    pub dry_run_contract: VortexDryRunContract,
    pub ready_for_dry_run: bool,
    pub ready_for_future_execution: bool,
    pub blocking_gate_count: usize,
    pub warning_gate_count: usize,
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
impl VortexExecutionReadinessReport {
    pub fn from_input(input: VortexExecutionReadinessInput) -> Result<Self> {
        let gates = evaluate_scheduler_readiness_gates(&input);
        let dry_run_contract = VortexDryRunContract::from_scheduler_report(&input.scheduler_report);
        let mut out = Self {
            status: VortexExecutionReadinessStatus::ReadyForDryRun,
            input,
            gates,
            dry_run_contract,
            ready_for_dry_run: false,
            ready_for_future_execution: false,
            blocking_gate_count: 0,
            warning_gate_count: 0,
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
        out.recompute_status();
        Ok(out)
    }
    pub fn from_scheduler_report(report: VortexSchedulerBridgeReport) -> Result<Self> {
        Self::from_input(VortexExecutionReadinessInput::new(report))
    }
    pub fn unsupported(
        input: VortexExecutionReadinessInput,
        feature: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut out =
            Self::from_input(input).expect("readiness report from input should be infallible");
        out.status = VortexExecutionReadinessStatus::Unsupported;
        out.add_diagnostic(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            feature,
            reason,
            Some("Fallback attempted: false".to_string()),
        ));
        out
    }
    pub fn add_gate(&mut self, gate: VortexReadinessGateResult) {
        self.gates.push(gate);
    }
    pub fn add_diagnostic(&mut self, diagnostic: Diagnostic) {
        self.diagnostics.push(diagnostic);
    }
    pub fn recompute_status(&mut self) {
        self.blocking_gate_count = self.gates.iter().filter(|g| g.is_blocking()).count();
        self.warning_gate_count = self
            .gates
            .iter()
            .filter(|g| g.status == VortexReadinessGateStatus::Warning)
            .count();
        let hard_errors = self.has_errors();
        self.ready_for_dry_run = self.dry_run_contract.is_side_effect_free() && !hard_errors;
        self.ready_for_future_execution = self.blocking_gate_count == 0
            && !hard_errors
            && self.input.scheduler_report.scheduled_task_count > 0
            && !self.fallback_execution_allowed;
        self.status = if hard_errors
            || self
                .gates
                .iter()
                .any(|g| g.status == VortexReadinessGateStatus::Failed)
        {
            VortexExecutionReadinessStatus::Unsupported
        } else if self.gates.iter().any(|g| {
            g.kind == VortexReadinessGateKind::NoMissingEstimates
                && g.status == VortexReadinessGateStatus::Blocked
        }) {
            VortexExecutionReadinessStatus::BlockedByMissingEstimate
        } else if self
            .gates
            .iter()
            .any(|g| g.kind == VortexReadinessGateKind::SchedulerPlanAvailable && g.is_blocking())
            || self.input.scheduler_report.status
                == VortexSchedulerBridgeStatus::BlockedByMemoryPolicy
        {
            VortexExecutionReadinessStatus::BlockedByMemoryPolicy
        } else if self.gates.iter().any(|g| {
            g.kind == VortexReadinessGateKind::NoSpillRequiredWithoutSupport && g.is_blocking()
        }) || self.input.scheduler_report.status
            == VortexSchedulerBridgeStatus::SpillRequiredButNotImplemented
        {
            VortexExecutionReadinessStatus::BlockedBySpillPolicy
        } else if self.input.scheduler_report.status == VortexSchedulerBridgeStatus::NoTasksRequired
            && self.blocking_gate_count == 0
        {
            VortexExecutionReadinessStatus::NoTasksRequired
        } else if self.ready_for_future_execution {
            VortexExecutionReadinessStatus::ReadyForFutureExecution
        } else {
            VortexExecutionReadinessStatus::ReadyForDryRun
        };
    }
    pub fn has_errors(&self) -> bool {
        self.diagnostics
            .iter()
            .chain(self.input.diagnostics.iter())
            .chain(self.input.scheduler_report.diagnostics.iter())
            .chain(self.dry_run_contract.diagnostics.iter())
            .any(|d| {
                matches!(
                    d.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
            || self.gates.iter().any(VortexReadinessGateResult::has_errors)
    }
    pub fn is_side_effect_free(&self) -> bool {
        !self.tasks_executed
            && !self.data_executed
            && !self.data_read
            && !self.data_materialized
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_effects_executed
            && !self.fallback_execution_allowed
            && self.dry_run_contract.is_side_effect_free()
    }
    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "Vortex execution readiness report");
        let _ = writeln!(out, "readiness status: {}", self.status.as_str());
        let _ = writeln!(out, "ready for dry run: {}", self.ready_for_dry_run);
        let _ = writeln!(
            out,
            "ready for future execution: {}",
            self.ready_for_future_execution
        );
        let _ = writeln!(out, "blocking gate count: {}", self.blocking_gate_count);
        let _ = writeln!(out, "warning gate count: {}", self.warning_gate_count);
        let _ = writeln!(out, "{}", self.dry_run_contract.to_human_text());
        let _ = writeln!(out, "tasks executed: false");
        let _ = writeln!(out, "data executed: false");
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
                let _ = writeln!(out, "- {}", d.message);
            }
        }
        out
    }
}

fn evaluate_scheduler_readiness_gates(
    input: &VortexExecutionReadinessInput,
) -> Vec<VortexReadinessGateResult> {
    let r = &input.scheduler_report;
    let mut gates = vec![];
    gates.push(if !r.fallback_execution_allowed {
        VortexReadinessGateResult::passed(
            VortexReadinessGateKind::NoFallbackExecution,
            "fallback execution disabled",
        )
    } else {
        VortexReadinessGateResult::failed(
            VortexReadinessGateKind::NoFallbackExecution,
            "fallback_execution",
            "fallback execution must remain disabled",
        )
    });
    for (k, v, reason) in [
        (
            VortexReadinessGateKind::NoDataRead,
            r.data_read,
            "data read must remain false",
        ),
        (
            VortexReadinessGateKind::NoMaterialization,
            r.data_materialized,
            "data materialization must remain false",
        ),
        (
            VortexReadinessGateKind::NoObjectStoreIo,
            r.object_store_io,
            "object-store IO must remain false",
        ),
        (
            VortexReadinessGateKind::NoWriteIo,
            r.write_io,
            "write IO must remain false",
        ),
        (
            VortexReadinessGateKind::NoSpillIo,
            r.spill_io_performed,
            "spill IO must remain false",
        ),
        (
            VortexReadinessGateKind::NoExternalEffects,
            r.external_effects_executed,
            "external effects must remain false",
        ),
    ] {
        gates.push(if !v {
            VortexReadinessGateResult::passed(k, "side-effect-free")
        } else {
            VortexReadinessGateResult::failed(k, "side_effect_violation", reason)
        });
    }
    gates.push(if r.has_errors() {
        VortexReadinessGateResult::failed(
            VortexReadinessGateKind::SchedulerPlanAvailable,
            "scheduler_plan",
            "scheduler report has hard errors",
        )
    } else {
        VortexReadinessGateResult::passed(
            VortexReadinessGateKind::SchedulerPlanAvailable,
            "scheduler plan available",
        )
    });
    let unsupported = r
        .decisions
        .iter()
        .any(|d| d.kind == VortexSchedulingDecisionKind::Unsupported);
    gates.push(if unsupported {
        VortexReadinessGateResult::failed(
            VortexReadinessGateKind::NoUnsupportedDiagnostics,
            "unsupported_decision",
            "unsupported scheduler diagnostics present",
        )
    } else {
        VortexReadinessGateResult::passed(
            VortexReadinessGateKind::NoUnsupportedDiagnostics,
            "no unsupported diagnostics",
        )
    });
    let needs_estimate = r.status == VortexSchedulerBridgeStatus::NeedsEstimate
        || r.decisions
            .iter()
            .any(|d| d.kind == VortexSchedulingDecisionKind::HoldForEstimate);
    gates.push(if input.require_all_estimates && needs_estimate {
        VortexReadinessGateResult::blocked(
            VortexReadinessGateKind::NoMissingEstimates,
            "missing estimates block readiness",
        )
    } else if needs_estimate {
        VortexReadinessGateResult::warning(
            VortexReadinessGateKind::NoMissingEstimates,
            "missing estimates deferred",
        )
    } else {
        VortexReadinessGateResult::passed(
            VortexReadinessGateKind::NoMissingEstimates,
            "no missing estimates",
        )
    });
    let spill_blocked = r.status == VortexSchedulerBridgeStatus::SpillRequiredButNotImplemented
        || r.decisions
            .iter()
            .any(|d| d.kind == VortexSchedulingDecisionKind::HoldForSpillSupport);
    gates.push(if input.require_spill_support && spill_blocked {
        VortexReadinessGateResult::blocked(
            VortexReadinessGateKind::NoSpillRequiredWithoutSupport,
            "spill support required before execution",
        )
    } else {
        VortexReadinessGateResult::passed(
            VortexReadinessGateKind::NoSpillRequiredWithoutSupport,
            "spill policy gate satisfied",
        )
    });
    gates.push(VortexReadinessGateResult::warning(
        VortexReadinessGateKind::MetadataAvailableOrDeferred,
        "metadata may be deferred in plan-only mode",
    ));
    gates
}

pub fn evaluate_vortex_execution_readiness(
    scheduler_report: VortexSchedulerBridgeReport,
) -> Result<VortexExecutionReadinessReport> {
    VortexExecutionReadinessReport::from_scheduler_report(scheduler_report)
}
pub fn vortex_execution_readiness_is_side_effect_free(
    report: &VortexExecutionReadinessReport,
) -> bool {
    report.is_side_effect_free()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        VortexMemoryBridgeInput, VortexMemoryBridgeReport, VortexSchedulerBridgeReport,
        plan_vortex_scheduler_queue,
    };
    use shardloom_exec::MemoryBudget;
    fn scheduler() -> VortexSchedulerBridgeReport {
        let mem = VortexMemoryBridgeReport::from_input(VortexMemoryBridgeInput::new(
            MemoryBudget::from_gib(1).expect("ok"),
        ))
        .expect("ok");
        plan_vortex_scheduler_queue(mem, 1).expect("ok")
    }
    #[test]
    fn status_and_modes() {
        assert!(VortexExecutionReadinessStatus::Unsupported.is_error());
        assert!(VortexExecutionReadinessStatus::ReadyForDryRun.allows_dry_run());
        assert!(VortexExecutionReadinessStatus::ReadyForFutureExecution.allows_future_execution());
        assert!(
            !VortexExecutionReadinessStatus::BlockedByMissingEstimate.allows_future_execution()
        );
        assert!(!VortexDryRunMode::ValidateScheduler.executes_tasks());
        assert!(!VortexDryRunMode::ValidateScheduler.reads_data());
        assert!(VortexReadinessGateStatus::Blocked.is_blocking());
        assert!(VortexReadinessGateStatus::Failed.is_error());
    }
    #[test]
    fn gate_failed_has_error() {
        let g = VortexReadinessGateResult::failed(
            VortexReadinessGateKind::NoFallbackExecution,
            "x",
            "y",
        );
        assert!(g.has_errors());
    }
    #[test]
    fn dry_run_from_scheduler_side_effect_free() {
        let r = scheduler();
        let c = VortexDryRunContract::from_scheduler_report(&r);
        assert!(c.is_side_effect_free());
        let u = VortexDryRunContract::unsupported("f", "r");
        assert!(u.has_errors());
    }
    #[test]
    fn input_defaults() {
        let i = VortexExecutionReadinessInput::new(scheduler());
        assert!(i.require_all_estimates && i.require_spill_support && i.require_feature_enabled);
    }
    #[test]
    fn report_basics() {
        let s = scheduler();
        let input = VortexExecutionReadinessInput::new(s.clone());
        let mut r = VortexExecutionReadinessReport::from_input(input).expect("ok");
        assert!(r.is_side_effect_free());
        assert!(
            r.gates
                .iter()
                .any(|g| g.kind == VortexReadinessGateKind::NoFallbackExecution)
        );
        assert!(r.to_human_text().contains("fallback execution disabled"));
        assert!(r.to_human_text().contains("tasks executed: false"));
        assert!(r.to_human_text().contains("data read: false"));
        assert!(r.to_human_text().contains("Vortex dry-run contract"));
        r.add_diagnostic(Diagnostic::invalid_input("x", "y", "z"));
        assert!(r.to_human_text().contains("diagnostics:"));
        let u = VortexExecutionReadinessReport::unsupported(
            VortexExecutionReadinessInput::new(s),
            "f",
            "r",
        );
        assert!(u.has_errors());
        assert!(vortex_execution_readiness_is_side_effect_free(&r));
        assert!(evaluate_vortex_execution_readiness(scheduler()).is_ok());
    }
    #[test]
    fn estimate_blocks_when_required() {
        let mut s = scheduler();
        s.status = VortexSchedulerBridgeStatus::NeedsEstimate;
        let r = VortexExecutionReadinessReport::from_input(VortexExecutionReadinessInput::new(s))
            .expect("ok");
        assert_eq!(
            r.status,
            VortexExecutionReadinessStatus::BlockedByMissingEstimate
        );
        assert!(!r.ready_for_future_execution);
        assert!(r.ready_for_dry_run);
    }
}
