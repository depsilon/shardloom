//! Logical planning structures for `ShardLoom`.
//!
//! This crate intentionally stays small in the setup phase and models planning
//! artifacts without delegating to external engines.

pub mod estimate;
pub mod explain;
pub mod plan_ir;
pub mod scan;

use shardloom_core::{Result, ShardLoomError};

pub use estimate::{EstimateConfidence, EstimateReport, EstimateValue};
pub use explain::{ExecutionBoundary, ExplainPlanNode, ExplainReport, PlanNodeId, PlanNodeKind};
pub use plan_ir::{
    EffectBoundary, NativePlanDocument, NativePlanNode, NativePlanNodeKind, PlanBoundaryKind,
    PlanCapabilityKind, PlanCapabilityRequirement, PlanExportRequest, PlanExportStatus, PlanId,
    PlanImportRequest, PlanImportStatus, PlanInteropFormat, PlanLayer, PlanSchemaVersion,
    PlanValidationReport, PlanValidationStatus, TranslationBoundary,
};
pub use scan::{ProjectionRequest, ScanMode, ScanPlanSkeleton, ScanPlanningStatus, ScanRequest};

/// High-level operation categories for initial planning skeletons.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanKind {
    /// A placeholder for native `Vortex` scan planning.
    NativeVortexScan,
}

/// A minimal execution plan description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// The plan category.
    pub kind: PlanKind,
}

/// Build a placeholder native `Vortex` scan plan.
///
/// # Errors
/// Returns an error when `SHARDLOOM_FAIL_PLAN` is set in the environment.
pub fn build_native_vortex_scan_plan() -> Result<Plan> {
    if std::env::var("SHARDLOOM_FAIL_PLAN").is_ok() {
        return Err(ShardLoomError::new(
            "planning failed due to SHARDLOOM_FAIL_PLAN being set",
        ));
    }

    Ok(Plan {
        kind: PlanKind::NativeVortexScan,
    })
}

#[cfg(test)]
mod tests {
    use super::{PlanKind, build_native_vortex_scan_plan};

    #[test]
    fn builds_native_vortex_scan_plan() {
        let plan = build_native_vortex_scan_plan().expect("plan should build");
        assert_eq!(plan.kind, PlanKind::NativeVortexScan);
    }
}
