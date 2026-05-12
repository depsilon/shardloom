//! Logical planning structures for `ShardLoom`.
//!
//! This crate models logical, physical, and top-level execution facade
//! artifacts without delegating to external engines.

pub mod estimate;
pub mod execution_facade;
pub mod explain;
pub mod input_bridge;
pub mod input_planning;
pub mod object_store;
pub mod optimizer;
pub mod plan_ir;
pub mod scan;

pub use estimate::{EstimateConfidence, EstimateReport, EstimateValue};
pub use execution_facade::{
    EncodedExecutionOperation, Plan, PlanKind, PreparedEncodedBatch, PreparedEncodedPlan,
    ReaderBackedEncodedPlan, ReaderBackedSplitRef, ReportOnlyPlan, SourceBackedEncodedPlan,
    SourceBackedPreparedEncodedBatch, VortexPrimitiveOperation, VortexPrimitivePlan,
    build_vortex_count_all_plan,
};
pub use explain::{ExecutionBoundary, ExplainPlanNode, ExplainReport, PlanNodeId, PlanNodeKind};
pub use input_bridge::input_source_to_scan_request;
pub use input_planning::{
    InputPlanningMode, InputPlanningStatus, UniversalInputPlanningReport,
    plan_universal_input_source, universal_input_planning_is_side_effect_free,
};
pub use object_store::{
    ObjectStoreCheckpointRetryInput, ObjectStoreCheckpointRetryReport,
    ObjectStoreCheckpointRetryStatus, ObjectStoreCommitProtocolInput,
    ObjectStoreCommitProtocolReport, ObjectStoreCommitProtocolStatus,
    ObjectStoreDistributedSchedulingPolicy, ObjectStoreDistributedSchedulingReport,
    ObjectStoreDistributedSchedulingStatus, ObjectStoreDistributedTaskPlan,
    ObjectStoreRangePlanningPolicy, ObjectStoreRangePlanningReport, ObjectStoreRangePlanningStatus,
    ObjectStoreRangeRequest, ObjectStoreRequestCoalescingDecision,
    ObjectStoreRequestCoalescingDecisionKind, ObjectStoreRequestCoalescingReport,
    ObjectStoreRequestCoalescingStatus, ObjectStoreRequestPlannerReport,
    ObjectStoreRequestPlannerStatus, ObjectStoreRuntimePromotionGateEntry,
    ObjectStoreRuntimePromotionGateReport, ObjectStoreRuntimePromotionRequirements,
    ObjectStoreRuntimePromotionStatus, ObjectStoreRuntimePromotionSurface,
    plan_object_store_checkpoint_retry, plan_object_store_commit_protocol,
    plan_object_store_distributed_scheduling, plan_object_store_ranges,
    plan_object_store_request_coalescing, plan_object_store_request_planner,
    plan_object_store_runtime_promotion_gate,
};
pub use optimizer::{
    AdaptiveDecisionKind, AdaptiveExecutionDecision, AdaptiveOptimizerMemoryReport,
    AdaptiveOptimizerMemoryStatus, AdaptiveTrigger, AdaptiveTriggerKind, AggregateStrategy,
    CostEstimate, CostMetric, CostModelInput, CostValue, DynamicPruningDecision, JoinStrategy,
    OptimizerPhase, OptimizerPlanSkeleton, OptimizerPlanStatus, OptimizerRuleDecision,
    OptimizerRuleId, OptimizerRuleKind, OptimizerRuleStatus, RuntimeFilter, RuntimeFilterKind,
    RuntimeFilterStatus, SkewHandlingStrategy, SkewSeverity, SkewSignal, SkewSignalKind,
    plan_adaptive_optimizer_memory,
};
pub use plan_ir::{
    EffectBoundary, ImportedPlanCapabilityGateReport, ImportedPlanCapabilityGateStatus,
    NativePlanDocument, NativePlanNode, NativePlanNodeKind, PlanBoundaryKind, PlanCapabilityKind,
    PlanCapabilityRequirement, PlanExportRequest, PlanExportStatus, PlanId, PlanImportRequest,
    PlanImportStatus, PlanInteropFormat, PlanLayer, PlanPortabilityDirection,
    PlanPortabilityReport, PlanPortabilityStatus, PlanSchemaVersion, PlanValidationReport,
    PlanValidationStatus, TranslationBoundary,
};
pub use scan::{ProjectionRequest, ScanMode, ScanPlanSkeleton, ScanPlanningStatus, ScanRequest};

#[cfg(test)]
mod tests {
    use shardloom_core::{DatasetUri, PredicateExpr};

    use super::{
        Plan, PlanId, PlanKind, ReportOnlyPlan, VortexPrimitiveOperation, VortexPrimitivePlan,
        build_vortex_count_all_plan,
    };

    #[test]
    fn builds_vortex_count_all_plan() {
        let plan = build_vortex_count_all_plan("plan.count", "file://tmp/data.vortex")
            .expect("plan should build");
        assert_eq!(plan.provider_api_surface(), Some("vortex_local_primitive"));
        assert_eq!(
            plan.source_refs(),
            vec!["file://tmp/data.vortex".to_string()]
        );
        assert!(plan.provider_dispatch_required());
        assert!(matches!(plan.kind, PlanKind::VortexPrimitive(_)));
    }

    #[test]
    fn primitive_plan_preserves_operation_and_predicate() {
        let source_uri = DatasetUri::new("file://tmp/data.vortex").expect("uri");
        let primitive = VortexPrimitivePlan::count_where(source_uri, PredicateExpr::AlwaysTrue);
        assert_eq!(primitive.operation, VortexPrimitiveOperation::CountWhere);
        assert!(primitive.predicate.is_some());
    }

    #[test]
    fn report_only_plan_does_not_require_provider_dispatch() {
        let plan = Plan::report_only(
            PlanId::new("plan.report").expect("plan id"),
            ReportOnlyPlan::new("architecture_spine"),
        );
        assert!(!plan.provider_dispatch_required());
        assert_eq!(plan.provider_kind(), None);
    }
}
