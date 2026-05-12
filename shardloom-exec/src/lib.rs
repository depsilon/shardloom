//! Execution facade for `ShardLoom`.
//!
//! This crate owns provider-neutral execution orchestration contracts with
//! explicit unsupported-path failures and no fallback delegation. The neutral
//! `execute` path returns report-only results or deterministic
//! provider-required blockers; it never reports no-op success for executable
//! plans. Provider crates attach concrete execution through the
//! `ShardLoomExecutionProvider` trait to avoid reversing crate dependencies.
//!
//! Memory, recovery, sizing, spill, runtime, and streaming exports are mostly
//! planning or promotion-gate surfaces. Narrow feature-gated local helpers stay
//! explicit and do not authorize object-store I/O, distributed execution,
//! external engine invocation, or fallback execution.

use shardloom_core::{Diagnostic, DiagnosticCode, ExecutionProviderKind, FallbackStatus, Result};
use shardloom_plan::{Plan, PlanKind};

pub mod memory;
pub mod recovery;
pub mod runtime;
pub mod sizing;
pub mod spill_lifecycle;
pub mod spill_payload;
pub mod streaming;

/// Reported status for the execution subsystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecStatus {
    /// Human-readable status line for `CLI` output.
    pub summary: String,
}

/// Top-level execution result status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardLoomExecutionStatus {
    Executed,
    ReportOnly,
    BlockedProviderDispatchRequired,
    BlockedUnsupported,
}

impl ShardLoomExecutionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Executed => "executed",
            Self::ReportOnly => "report_only",
            Self::BlockedProviderDispatchRequired => "blocked_provider_dispatch_required",
            Self::BlockedUnsupported => "blocked_unsupported",
        }
    }

    #[must_use]
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Executed | Self::ReportOnly)
    }
}

/// Typed top-level execution result returned by the execution facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardLoomExecutionResult {
    pub status: ShardLoomExecutionStatus,
    pub plan_id: String,
    pub plan_kind: String,
    pub engine_mode: String,
    pub execution_provider_kind: Option<ExecutionProviderKind>,
    pub provider_api_surface: Option<String>,
    pub source_refs: Vec<String>,
    pub split_refs: Vec<String>,
    pub result_refs: Vec<String>,
    pub artifact_refs: Vec<String>,
    pub execution_certificate_refs: Vec<String>,
    pub native_io_certificate_refs: Vec<String>,
    pub materialization_boundary_refs: Vec<String>,
    pub residual_boundary_refs: Vec<String>,
    pub representation_transitions: Vec<String>,
    pub diagnostics: Vec<Diagnostic>,
    pub fallback: FallbackStatus,
    pub external_engine_invoked: bool,
}

impl ShardLoomExecutionResult {
    #[must_use]
    pub fn from_plan(plan: &Plan, status: ShardLoomExecutionStatus) -> Self {
        Self {
            status,
            plan_id: plan.id.as_str().to_string(),
            plan_kind: plan.kind.as_str().to_string(),
            engine_mode: "batch".to_string(),
            execution_provider_kind: plan.provider_kind(),
            provider_api_surface: plan.provider_api_surface().map(str::to_string),
            source_refs: plan.source_refs(),
            split_refs: plan.split_refs(),
            result_refs: vec![],
            artifact_refs: vec![],
            execution_certificate_refs: vec![],
            native_io_certificate_refs: vec![],
            materialization_boundary_refs: vec![],
            residual_boundary_refs: plan.residual_boundary_refs(),
            representation_transitions: vec![],
            diagnostics: plan.diagnostics(),
            fallback: FallbackStatus::disabled_by_policy(),
            external_engine_invoked: false,
        }
    }

    #[must_use]
    pub fn report_only(plan: &Plan) -> Self {
        Self::from_plan(plan, ShardLoomExecutionStatus::ReportOnly)
    }

    #[must_use]
    pub fn blocked_provider_dispatch_required(plan: &Plan) -> Self {
        let mut result = Self::from_plan(
            plan,
            ShardLoomExecutionStatus::BlockedProviderDispatchRequired,
        );
        result.diagnostics.push(plan.unsupported_diagnostic());
        result
    }

    #[must_use]
    pub fn blocked_unsupported(plan: &Plan, diagnostic: Diagnostic) -> Self {
        let mut result = Self::from_plan(plan, ShardLoomExecutionStatus::BlockedUnsupported);
        result.diagnostics.push(diagnostic);
        result
    }

    #[must_use]
    pub fn executed(plan: &Plan) -> Self {
        Self::from_plan(plan, ShardLoomExecutionStatus::Executed)
    }

    #[must_use]
    pub const fn fallback_attempted(&self) -> bool {
        self.fallback.attempted
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.status.is_success()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    shardloom_core::DiagnosticSeverity::Error
                        | shardloom_core::DiagnosticSeverity::Fatal
                )
            })
    }
}

/// Provider-side top-level execution dispatch.
pub trait ShardLoomExecutionProvider {
    /// Execute a top-level plan through the provider.
    ///
    /// # Errors
    /// Returns an error when the provider cannot construct its typed execution result.
    fn execute_plan(&self, plan: &Plan) -> Result<ShardLoomExecutionResult>;
}

/// Return a simple system status for process validation.
#[must_use]
pub fn status() -> ExecStatus {
    ExecStatus {
        summary: "ShardLoom workspace initialized (native Vortex-first execution facade)"
            .to_string(),
    }
}

/// Execute a plan through the provider-neutral facade.
///
/// Provider-specific executable plans must be dispatched via
/// `execute_with_provider`. The provider-neutral path never returns no-op
/// success for executable plans.
///
/// # Errors
/// This provider-neutral facade currently constructs deterministic blocked
/// diagnostics and does not perform fallible IO or provider dispatch.
pub fn execute(plan: &Plan) -> Result<ShardLoomExecutionResult> {
    if matches!(plan.kind, PlanKind::ReportOnly(_)) {
        Ok(ShardLoomExecutionResult::report_only(plan))
    } else {
        Ok(ShardLoomExecutionResult::blocked_provider_dispatch_required(plan))
    }
}

/// Execute a plan through a concrete native provider.
///
/// # Errors
/// Returns provider errors from the selected execution provider.
pub fn execute_with_provider(
    plan: &Plan,
    provider: &dyn ShardLoomExecutionProvider,
) -> Result<ShardLoomExecutionResult> {
    provider.execute_plan(plan)
}

/// Fail explicitly for unsupported operations in the provider-neutral facade.
///
/// # Errors
/// Returns an error when the synthetic unsupported plan id cannot be constructed.
pub fn unsupported(operation: &str) -> Result<ShardLoomExecutionResult> {
    let plan = Plan::report_only(
        shardloom_plan::PlanId::new(format!("unsupported.{operation}"))?,
        shardloom_plan::ReportOnlyPlan::new("unsupported_operation"),
    );
    Ok(ShardLoomExecutionResult::blocked_unsupported(
        &plan,
        Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            operation,
            format!("unsupported execution path: {operation}; no fallback engines are enabled"),
            Some("Use a ShardLoom-native supported plan surface.".to_string()),
        ),
    ))
}

// Memory and spill planning/promotion surfaces; allocator/runtime spill remains
// evidence-gated unless a specific feature-gated helper says otherwise.
pub use memory::{
    MemoryAdmissionDecisionKind, MemoryAdmissionReport, MemoryBudget, MemoryOwner, MemoryPoolPlan,
    MemoryPoolSnapshot, MemoryPressureLevel, MemoryReservation, MemoryReservationId,
    MemoryReservationStatus, MemoryRuntimeHardeningGateEntry, MemoryRuntimeHardeningGateReport,
    MemoryRuntimeHardeningStatus, MemoryRuntimeHardeningSurface, OomSafetyPlan,
    OperatorMemoryClass, OperatorMemorySpillDeclaration, OperatorMemorySpillDeclarationReport,
    OperatorMemorySpillDeclarationStatus, SpillCompression, SpillDecision, SpillDecisionKind,
    SpillFileRef, SpillFileStatus, SpillFormat, SpillPartition, SpillPlan, SpillPlanStatus,
    SpillPolicy, SpillReport, plan_memory_runtime_hardening_gate,
    plan_operator_memory_spill_declarations,
};

// Recovery, retry, cancellation, cleanup, and commit promotion contracts.
pub use recovery::{
    AmbiguousCommitRecord, AttemptId, CancellationReason, CancellationRequest, CancellationScope,
    CancellationStatus, CleanupExecutionOption, CleanupRequirement, CleanupStatus,
    CleanupTargetKind, CommitExecutionPromotionGateEntry, CommitExecutionPromotionGateReport,
    CommitExecutionPromotionStatus, CommitExecutionPromotionSurface, CommitRecoveryState,
    FailureDomain, FailureKind, FailureRecord, FaultToleranceLevel, FaultTolerancePromotionArea,
    FaultTolerancePromotionGateEntry, FaultTolerancePromotionGateReport,
    FaultTolerancePromotionStatus, PartialOutputRecord, RecoveryAction, RecoveryActionKind,
    RecoveryPlan, RecoveryPlanStatus, RecoveryReport, RetryDecision, RetryDecisionKind,
    RetryEligibility, RetryPlan, ShardLoomCancellationExecutionGateEffect,
    ShardLoomCancellationExecutionGateMode, ShardLoomCancellationExecutionGateReport,
    ShardLoomCancellationExecutionGateRequest, ShardLoomCancellationExecutionGateSignal,
    ShardLoomCancellationExecutionGateStatus, ShardLoomCleanupExecutionEffect,
    ShardLoomCleanupExecutionMode, ShardLoomCleanupExecutionReport,
    ShardLoomCleanupExecutionRequest, ShardLoomCleanupExecutionStatus,
    ShardLoomRetryExecutionGateEffect, ShardLoomRetryExecutionGateMode,
    ShardLoomRetryExecutionGateReport, ShardLoomRetryExecutionGateRequest,
    ShardLoomRetryExecutionGateSignal, ShardLoomRetryExecutionGateStatus, TaskAttemptRecord,
    TaskAttemptStatus, cancellation_execution_gate_is_side_effect_free,
    cleanup_execution_plan_is_side_effect_free, plan_cancellation_execution_gate,
    plan_cleanup_execution, plan_commit_execution_promotion_gate,
    plan_fault_tolerance_promotion_gate, plan_retry_execution_gate,
    retry_execution_gate_is_side_effect_free,
};

// Adaptive sizing and bounded work-shaping planning surfaces.
pub use sizing::{
    AdaptiveSizer, AdaptiveSizingPolicy, ByteSize, CoalescingPolicy,
    DynamicRuntimePromotionGateEntry, DynamicRuntimePromotionGateReport,
    DynamicRuntimePromotionStatus, DynamicRuntimePromotionSurface, DynamicSizingFeedbackInput,
    DynamicSizingFeedbackMode, DynamicSizingFeedbackReport, DynamicSizingFeedbackStatus,
    DynamicWorkShapingReport, DynamicWorkShapingStatus, ParallelismLimit, ParallelismPlan,
    SizeEstimate, SizingFeedbackSignal, SizingFeedbackSignalKind, SizingInput, SizingPlan,
    TaskSizingDecision, TaskSizingDecisionKind, TaskSizingMode,
    plan_dynamic_runtime_promotion_gate, plan_dynamic_sizing_feedback, plan_dynamic_work_shaping,
};

// Streaming and zero-copy boundary planning surfaces; live execution is blocked.
pub use streaming::{
    BackpressurePlanInput, BackpressurePlanMode, BackpressurePlanReport, BackpressurePlanStatus,
    BackpressurePolicy, BoundaryInteropKind, BoundedMemoryPolicy, DataWorkLevel,
    EncodedBatchRepresentation, EncodedStreamingBatchPlanInput, EncodedStreamingBatchPlanReport,
    EncodedStreamingBatchPlanStatus, MaterializationBoundary, SinkRequirement, StreamingCapability,
    StreamingMode, StreamingOperator, StreamingOperatorKind, StreamingPlanSkeleton,
    StreamingPlanStatus, StreamingSink, StreamingSinkKind, StreamingSource, StreamingSourceKind,
    StreamingStage, ZeroCopyStatus, ZeroDecodeStatus, plan_backpressure,
    plan_encoded_streaming_batches,
};

pub use spill_lifecycle::*;

// Explicit spill-payload artifact helpers and report-only payload contracts.
pub use spill_payload::{
    SpillPayloadEffect, SpillPayloadFsFeatureStatus, SpillPayloadFsPlanMode,
    SpillPayloadFsPlanReport, SpillPayloadFsPlanStatus, SpillPayloadFsRef, SpillPayloadId,
    SpillPayloadMode, SpillPayloadPath, SpillPayloadPlanReport, SpillPayloadPlanRequest,
    SpillPayloadReadReport, SpillPayloadReadRequest, SpillPayloadRef, SpillPayloadRoundTripOption,
    SpillPayloadRoundTripReport, SpillPayloadRoundTripRequest, SpillPayloadStatus,
    SpillPayloadWriteOption, SpillPayloadWriteReport, SpillPayloadWriteRequest,
    SyntheticSpillPayload, plan_spill_payload, plan_spill_payload_filesystem_ref,
    read_spill_payload, roundtrip_spill_payload, spill_payload_fs_feature_enabled,
    spill_payload_plan_is_side_effect_free, write_spill_payload,
};

// Runtime task-graph planning surfaces; object-store/distributed task execution is blocked.
pub use runtime::{
    ByteRangeRequest, ObjectStoreKind, ObjectStoreRef, ReadPolicy, ResourceBudget, RetryPolicy,
    RuntimePlanSkeleton, RuntimePlanningStatus, SegmentTask, ShuffleRequirement, TaskGraph, TaskId,
    TaskKind, TaskStatus,
};

#[cfg(test)]
mod tests {
    use shardloom_plan::{Plan, PlanId, ReportOnlyPlan, build_vortex_count_all_plan};

    use super::{ShardLoomExecutionStatus, execute, status, unsupported};

    #[test]
    fn reports_status() {
        assert!(status().summary.contains("initialized"));
    }

    #[test]
    fn executable_plan_requires_provider_dispatch() {
        let plan =
            build_vortex_count_all_plan("plan.count", "file://tmp/data.vortex").expect("plan");
        let result = execute(&plan).expect("execution result");
        assert_eq!(
            result.status,
            ShardLoomExecutionStatus::BlockedProviderDispatchRequired
        );
        assert_eq!(
            result.provider_api_surface.as_deref(),
            Some("vortex_local_primitive")
        );
        assert!(!result.fallback_attempted());
        assert!(!result.external_engine_invoked);
        assert!(result.has_errors());
    }

    #[test]
    fn report_only_plan_is_not_noop_execution() {
        let plan = Plan::report_only(
            PlanId::new("plan.report").expect("plan id"),
            ReportOnlyPlan::new("architecture_spine"),
        );
        let result = execute(&plan).expect("execution result");
        assert_eq!(result.status, ShardLoomExecutionStatus::ReportOnly);
        assert!(!result.fallback_attempted());
        assert!(!result.external_engine_invoked);
        assert!(result.result_refs.is_empty());
    }

    #[test]
    fn unsupported_fails_explicitly() {
        let result = unsupported("join").expect("unsupported result");
        assert_eq!(result.status, ShardLoomExecutionStatus::BlockedUnsupported);
        assert!(result.has_errors());
        assert!(!result.fallback_attempted());
    }
}
