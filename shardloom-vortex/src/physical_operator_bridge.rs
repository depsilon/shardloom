use std::fmt::Write as _;

use shardloom_core::{
    BenchmarkEvidenceState, BenchmarkFallbackState, Diagnostic, KernelKind,
    OperatorMemoryCertification, PhysicalKernelRequirement, PhysicalOperatorContract,
    PhysicalOperatorExecutionLevel, PhysicalOperatorExecutionProfileMatrix, PhysicalOperatorKind,
    PhysicalOperatorPlan, PhysicalOperatorPlanningCertificate, Result,
};

use crate::{VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexPhysicalOperatorBridgeStatus {
    Planned,
    Unsupported,
}

impl VortexPhysicalOperatorBridgeStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexPhysicalOperatorBridgeReport {
    pub schema_version: &'static str,
    pub bridge_id: String,
    pub primitive_kind: VortexQueryPrimitiveKind,
    pub physical_plan: PhysicalOperatorPlan,
    pub planning_certificate: PhysicalOperatorPlanningCertificate,
    pub status: VortexPhysicalOperatorBridgeStatus,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexPhysicalOperatorBridgeReport {
    /// Builds a report-only physical-operator plan for a Vortex query primitive.
    ///
    /// # Errors
    /// Returns an error only if an internal static operator id is malformed.
    pub fn from_request(request: &VortexQueryPrimitiveRequest) -> Result<Self> {
        let physical_plan = physical_operator_plan_for_vortex_query_primitive(request)?;
        let planning_certificate = PhysicalOperatorPlanningCertificate::evaluate(
            &physical_plan,
            &PhysicalOperatorExecutionProfileMatrix::cg7_foundation(),
            BenchmarkEvidenceState::Missing,
            BenchmarkEvidenceState::Missing,
            OperatorMemoryCertification::unsupported(),
            BenchmarkFallbackState::NotAttempted,
        );
        let status = if physical_plan.unsupported_count() > 0 {
            VortexPhysicalOperatorBridgeStatus::Unsupported
        } else {
            VortexPhysicalOperatorBridgeStatus::Planned
        };
        let mut diagnostics = Vec::new();
        diagnostics.extend(request.diagnostics.clone());
        diagnostics.extend(physical_plan.diagnostics.clone());
        diagnostics.extend(planning_certificate.diagnostics.clone());
        Ok(Self {
            schema_version: "shardloom.vortex_physical_operator_bridge.v1",
            bridge_id: format!(
                "vortex.query-primitive.{}.physical-operator-bridge",
                request.kind.as_str()
            ),
            primitive_kind: request.kind,
            physical_plan,
            planning_certificate,
            status,
            diagnostics,
        })
    }

    #[must_use]
    pub const fn runtime_execution_allowed(&self) -> bool {
        false
    }

    #[must_use]
    pub const fn fallback_execution_allowed(&self) -> bool {
        false
    }

    #[must_use]
    pub fn is_side_effect_free(&self) -> bool {
        !self.runtime_execution_allowed() && !self.fallback_execution_allowed()
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut text = String::new();
        let _ = writeln!(text, "schema_version: {}", self.schema_version);
        let _ = writeln!(text, "bridge: {}", self.bridge_id);
        let _ = writeln!(text, "primitive: {}", self.primitive_kind.as_str());
        let _ = writeln!(text, "status: {}", self.status.as_str());
        let _ = writeln!(text, "operators: {}", self.physical_plan.operators.len());
        let _ = writeln!(
            text,
            "certificate: {}",
            self.planning_certificate.status.as_str()
        );
        let _ = writeln!(text, "runtime execution: disabled");
        let _ = writeln!(text, "fallback execution: disabled");
        text
    }
}

/// Builds a report-only physical-operator plan for a Vortex query primitive.
///
/// # Errors
/// Returns an error only if an internal static operator id is malformed.
pub fn physical_operator_plan_for_vortex_query_primitive(
    request: &VortexQueryPrimitiveRequest,
) -> Result<PhysicalOperatorPlan> {
    let operators = match request.kind {
        VortexQueryPrimitiveKind::CountAll => vec![bridge_operator(
            "vortex.query_primitive.count_all.count_aggregate",
            PhysicalOperatorKind::CountAggregate,
        )?],
        VortexQueryPrimitiveKind::CountWhere => vec![
            bridge_operator(
                "vortex.query_primitive.count_where.filter",
                PhysicalOperatorKind::Filter,
            )?,
            bridge_operator(
                "vortex.query_primitive.count_where.count_aggregate",
                PhysicalOperatorKind::CountAggregate,
            )?,
        ],
        VortexQueryPrimitiveKind::ProjectColumns => vec![bridge_operator(
            "vortex.query_primitive.project_columns.project",
            PhysicalOperatorKind::Project,
        )?],
        VortexQueryPrimitiveKind::FilterPredicate => vec![bridge_operator(
            "vortex.query_primitive.filter_predicate.filter",
            PhysicalOperatorKind::Filter,
        )?],
        VortexQueryPrimitiveKind::FilterAndProject => vec![
            bridge_operator(
                "vortex.query_primitive.filter_and_project.filter",
                PhysicalOperatorKind::Filter,
            )?,
            bridge_operator(
                "vortex.query_primitive.filter_and_project.project",
                PhysicalOperatorKind::Project,
            )?,
        ],
        VortexQueryPrimitiveKind::SimpleAggregate | VortexQueryPrimitiveKind::Unsupported => {
            vec![bridge_operator(
                "vortex.query_primitive.unsupported",
                PhysicalOperatorKind::Unsupported,
            )?]
        }
    };
    let mut plan = PhysicalOperatorPlan {
        schema_version: "shardloom.physical_operator_plan.v1",
        plan_id: format!(
            "vortex.query-primitive.{}.physical-plan",
            request.kind.as_str()
        ),
        operators,
        diagnostics: Vec::new(),
    };
    plan.refresh_diagnostics();
    Ok(plan)
}

/// Builds a side-effect-free bridge report for a Vortex query primitive.
///
/// # Errors
/// Returns an error only if an internal static operator id is malformed.
pub fn plan_vortex_query_primitive_physical_operators(
    request: &VortexQueryPrimitiveRequest,
) -> Result<VortexPhysicalOperatorBridgeReport> {
    VortexPhysicalOperatorBridgeReport::from_request(request)
}

fn bridge_operator(
    operator_id: &str,
    kind: PhysicalOperatorKind,
) -> Result<PhysicalOperatorContract> {
    let (execution_level, kernel_requirements) = if kind == PhysicalOperatorKind::Unsupported {
        (
            PhysicalOperatorExecutionLevel::Unsupported,
            vec![PhysicalKernelRequirement::missing(KernelKind::Unsupported)],
        )
    } else {
        (
            PhysicalOperatorExecutionLevel::EncodedNative,
            vec![
                PhysicalKernelRequirement::missing(KernelKind::Metadata),
                PhysicalKernelRequirement::missing(KernelKind::Encoded),
            ],
        )
    };
    PhysicalOperatorContract::new(operator_id, kind, execution_level, kernel_requirements)
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{ColumnRef, DatasetUri, PredicateExpr};

    fn uri() -> DatasetUri {
        DatasetUri::new("file:///tmp/test.vortex").expect("uri")
    }

    #[test]
    fn count_where_bridge_plans_filter_and_count_blockers_without_execution() {
        let request = VortexQueryPrimitiveRequest::count_where(
            uri(),
            PredicateExpr::IsNotNull {
                column: ColumnRef::new("flag").expect("column"),
            },
        );

        let report = plan_vortex_query_primitive_physical_operators(&request).expect("report");

        assert_eq!(
            report.schema_version,
            "shardloom.vortex_physical_operator_bridge.v1"
        );
        assert_eq!(report.status, VortexPhysicalOperatorBridgeStatus::Planned);
        assert!(
            report
                .physical_plan
                .has_operator_kind(PhysicalOperatorKind::Filter)
        );
        assert!(
            report
                .physical_plan
                .has_operator_kind(PhysicalOperatorKind::CountAggregate)
        );
        assert_eq!(report.physical_plan.operators.len(), 2);
        assert_eq!(report.physical_plan.missing_kernel_count(), 2);
        assert_eq!(report.planning_certificate.required_slot_count, 4);
        assert!(!report.planning_certificate.can_plan_native());
        assert!(!report.runtime_execution_allowed());
        assert!(!report.fallback_execution_allowed());
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn filter_project_bridge_preserves_operator_order() {
        let request = VortexQueryPrimitiveRequest {
            kind: VortexQueryPrimitiveKind::FilterAndProject,
            source_uri: Some(uri()),
            projection: shardloom_plan::ProjectionRequest::all(),
            predicate: Some(PredicateExpr::AlwaysTrue),
            diagnostics: Vec::new(),
        };

        let plan = physical_operator_plan_for_vortex_query_primitive(&request).expect("plan");

        assert_eq!(plan.operators.len(), 2);
        assert_eq!(plan.operators[0].kind, PhysicalOperatorKind::Filter);
        assert_eq!(plan.operators[1].kind, PhysicalOperatorKind::Project);
        assert!(!plan.fallback_execution_allowed());
    }

    #[test]
    fn unsupported_bridge_stays_diagnostic_and_no_fallback() {
        let request = VortexQueryPrimitiveRequest::unsupported("x", "test");

        let report = plan_vortex_query_primitive_physical_operators(&request).expect("report");

        assert_eq!(
            report.status,
            VortexPhysicalOperatorBridgeStatus::Unsupported
        );
        assert_eq!(report.physical_plan.unsupported_count(), 1);
        assert!(!report.diagnostics.is_empty());
        assert!(
            report
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }
}
