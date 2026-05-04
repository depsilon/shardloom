//! Execution skeleton for `ShardLoom`.
//!
//! This crate owns native execution orchestration with explicit unsupported-path
//! failures and no fallback delegation architecture.

use shardloom_core::{Result, ShardLoomError};
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

/// Return a simple system status for initial workspace validation.
#[must_use]
pub fn status() -> ExecStatus {
    ExecStatus {
        summary: "ShardLoom workspace initialized (native Vortex-first skeleton)".to_string(),
    }
}

/// Execute a plan in the native engine.
///
/// # Errors
/// This skeletal implementation reserves errors for future execution failures.
pub fn execute(plan: &Plan) -> Result<()> {
    match plan.kind {
        PlanKind::NativeVortexScan => Ok(()),
    }
}

/// Fail explicitly for unsupported operations in the early skeleton.
///
/// # Errors
/// Always returns an explicit unsupported-path error.
pub fn unsupported(operation: &str) -> Result<()> {
    Err(ShardLoomError::new(format!(
        "unsupported execution path: {operation}; no fallback engines are enabled"
    )))
}

pub use memory::{
    MemoryBudget, MemoryOwner, MemoryPoolPlan, MemoryPoolSnapshot, MemoryPressureLevel,
    MemoryReservation, MemoryReservationId, MemoryReservationStatus, OomSafetyPlan,
    OperatorMemoryClass, SpillCompression, SpillDecision, SpillDecisionKind, SpillFileRef,
    SpillFileStatus, SpillFormat, SpillPartition, SpillPlan, SpillPlanStatus, SpillPolicy,
    SpillReport,
};

pub use recovery::{
    AmbiguousCommitRecord, AttemptId, CancellationReason, CancellationRequest, CancellationScope,
    CancellationStatus, CleanupExecutionOption, CleanupRequirement, CleanupStatus,
    CleanupTargetKind, CommitRecoveryState, FailureDomain, FailureKind, FailureRecord,
    FaultToleranceLevel, PartialOutputRecord, RecoveryAction, RecoveryActionKind, RecoveryPlan,
    RecoveryPlanStatus, RecoveryReport, RetryDecision, RetryDecisionKind, RetryEligibility,
    RetryPlan, ShardLoomCleanupExecutionEffect, ShardLoomCleanupExecutionMode,
    ShardLoomCleanupExecutionReport, ShardLoomCleanupExecutionRequest,
    ShardLoomCleanupExecutionStatus, TaskAttemptRecord, TaskAttemptStatus,
    cleanup_execution_plan_is_side_effect_free, plan_cleanup_execution,
};

pub use sizing::{
    AdaptiveSizer, AdaptiveSizingPolicy, ByteSize, CoalescingPolicy, ParallelismLimit,
    ParallelismPlan, SizeEstimate, SizingInput, SizingPlan, TaskSizingDecision,
    TaskSizingDecisionKind, TaskSizingMode,
};

pub use streaming::{
    BackpressurePolicy, BoundaryInteropKind, BoundedMemoryPolicy, DataWorkLevel,
    MaterializationBoundary, SinkRequirement, StreamingCapability, StreamingMode,
    StreamingOperator, StreamingOperatorKind, StreamingPlanSkeleton, StreamingPlanStatus,
    StreamingSink, StreamingSinkKind, StreamingSource, StreamingSourceKind, StreamingStage,
    ZeroCopyStatus, ZeroDecodeStatus,
};

pub use spill_lifecycle::*;
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

pub use runtime::{
    ByteRangeRequest, ObjectStoreKind, ObjectStoreRef, ReadPolicy, ResourceBudget, RetryPolicy,
    RuntimePlanSkeleton, RuntimePlanningStatus, SegmentTask, ShuffleRequirement, TaskGraph, TaskId,
    TaskKind, TaskStatus,
};

#[cfg(test)]
mod tests {
    use shardloom_plan::build_native_vortex_scan_plan;

    use super::{execute, status, unsupported};

    #[test]
    fn reports_status() {
        assert!(status().summary.contains("initialized"));
    }

    #[test]
    fn executes_native_plan() {
        let plan = build_native_vortex_scan_plan().expect("plan");
        execute(&plan).expect("execution should succeed");
    }

    #[test]
    fn unsupported_fails_explicitly() {
        let error = unsupported("join").expect_err("must fail");
        assert!(error.to_string().contains("no fallback engines"));
    }
}
