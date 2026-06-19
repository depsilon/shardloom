use std::fmt::Write as _;

use shardloom_core::{
    BenchmarkEvidenceState, BenchmarkFallbackState, Diagnostic, KernelKind,
    OperatorMemoryCertification, PhysicalKernelRequirement, PhysicalOperatorContract,
    PhysicalOperatorExecutionLevel, PhysicalOperatorExecutionProfileMatrix, PhysicalOperatorKind,
    PhysicalOperatorPlan, PhysicalOperatorPlanningCertificate, Result,
};

use crate::{VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest, VortexQueryPrimitiveResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexPhysicalOperatorBridgeStatus {
    Planned,
    MetadataReady,
    Unsupported,
}

impl VortexPhysicalOperatorBridgeStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::MetadataReady => "metadata_ready",
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
        Ok(Self::from_plan_with_evidence(
            request.kind,
            physical_plan,
            request.diagnostics.clone(),
            BenchmarkEvidenceState::Missing,
            BenchmarkEvidenceState::Missing,
            OperatorMemoryCertification::unsupported(),
            BenchmarkFallbackState::NotAttempted,
        ))
    }

    /// Builds a report-only physical-operator plan from an already evaluated Vortex primitive.
    ///
    /// Metadata-answered primitives can mark metadata kernel requirements present in
    /// the physical plan. Certificate admission still requires separate correctness,
    /// memory-safety, and benchmark evidence before any execution claim is allowed.
    ///
    /// # Errors
    /// Returns an error only if an internal static operator id is malformed.
    pub fn from_result(result: &VortexQueryPrimitiveResult) -> Result<Self> {
        Self::from_result_with_evidence(
            result,
            BenchmarkEvidenceState::Missing,
            BenchmarkEvidenceState::Missing,
            OperatorMemoryCertification::unsupported(),
            BenchmarkFallbackState::NotAttempted,
        )
    }

    /// Builds a report-only physical-operator plan from an evaluated Vortex primitive
    /// using explicit admission evidence.
    ///
    /// The evidence only affects planning-certificate readiness. Runtime execution and
    /// fallback execution remain disabled by this bridge.
    ///
    /// # Errors
    /// Returns an error only if an internal static operator id is malformed.
    pub fn from_result_with_evidence(
        result: &VortexQueryPrimitiveResult,
        correctness_evidence: BenchmarkEvidenceState,
        benchmark_evidence: BenchmarkEvidenceState,
        memory: OperatorMemoryCertification,
        fallback: BenchmarkFallbackState,
    ) -> Result<Self> {
        let physical_plan = physical_operator_plan_for_vortex_query_primitive_result(result)?;
        let mut diagnostics = result.request.diagnostics.clone();
        diagnostics.extend(result.diagnostics.clone());
        Ok(Self::from_plan_with_evidence(
            result.request.kind,
            physical_plan,
            diagnostics,
            correctness_evidence,
            benchmark_evidence,
            memory,
            fallback,
        ))
    }

    fn from_plan_with_evidence(
        primitive_kind: VortexQueryPrimitiveKind,
        physical_plan: PhysicalOperatorPlan,
        mut diagnostics: Vec<Diagnostic>,
        correctness_evidence: BenchmarkEvidenceState,
        benchmark_evidence: BenchmarkEvidenceState,
        memory: OperatorMemoryCertification,
        fallback: BenchmarkFallbackState,
    ) -> Self {
        let planning_certificate = PhysicalOperatorPlanningCertificate::evaluate(
            &physical_plan,
            &PhysicalOperatorExecutionProfileMatrix::cg7_foundation(),
            correctness_evidence,
            benchmark_evidence,
            memory,
            fallback,
        );
        let status = if physical_plan.unsupported_count() > 0 {
            VortexPhysicalOperatorBridgeStatus::Unsupported
        } else if physical_plan.all_ready_for_native_planning() {
            VortexPhysicalOperatorBridgeStatus::MetadataReady
        } else {
            VortexPhysicalOperatorBridgeStatus::Planned
        };
        diagnostics.extend(physical_plan.diagnostics.clone());
        diagnostics.extend(planning_certificate.diagnostics.clone());
        Self {
            schema_version: "shardloom.vortex_physical_operator_bridge.v1",
            bridge_id: format!(
                "vortex.query-primitive.{}.physical-operator-bridge",
                primitive_kind.as_str()
            ),
            primitive_kind,
            physical_plan,
            planning_certificate,
            status,
            diagnostics,
        }
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
#[allow(clippy::too_many_lines)]
pub fn physical_operator_plan_for_vortex_query_primitive(
    request: &VortexQueryPrimitiveRequest,
) -> Result<PhysicalOperatorPlan> {
    let mut operators = match request.kind {
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
        VortexQueryPrimitiveKind::TailRows => vec![
            bridge_operator(
                "vortex.query_primitive.tail_rows.project",
                PhysicalOperatorKind::Project,
            )?,
            bridge_operator(
                "vortex.query_primitive.tail_rows.source_order_window",
                PhysicalOperatorKind::Limit,
            )?,
        ],
        VortexQueryPrimitiveKind::SampleRows => vec![
            bridge_operator(
                "vortex.query_primitive.sample_rows.project",
                PhysicalOperatorKind::Project,
            )?,
            bridge_operator(
                "vortex.query_primitive.sample_rows.deterministic_sample",
                PhysicalOperatorKind::Limit,
            )?,
        ],
        VortexQueryPrimitiveKind::SortRows => vec![
            bridge_operator(
                "vortex.query_primitive.sort_rows.project",
                PhysicalOperatorKind::Project,
            )?,
            bridge_operator(
                "vortex.query_primitive.sort_rows.order_state",
                PhysicalOperatorKind::Sort,
            )?,
            bridge_operator(
                "vortex.query_primitive.sort_rows.source_order_window",
                PhysicalOperatorKind::Limit,
            )?,
        ],
        VortexQueryPrimitiveKind::DuplicateMaskRows => vec![
            bridge_operator(
                "vortex.query_primitive.duplicate_mask_rows.project",
                PhysicalOperatorKind::Project,
            )?,
            bridge_operator(
                "vortex.query_primitive.duplicate_mask_rows.row_key_state",
                PhysicalOperatorKind::Aggregate,
            )?,
        ],
        VortexQueryPrimitiveKind::ExpressionProjectRows => vec![
            bridge_operator(
                "vortex.query_primitive.expression_project_rows.project",
                PhysicalOperatorKind::Project,
            )?,
            bridge_operator(
                "vortex.query_primitive.expression_project_rows.typed_rewrite",
                PhysicalOperatorKind::Project,
            )?,
        ],
        VortexQueryPrimitiveKind::MeltRows => vec![
            bridge_operator(
                "vortex.query_primitive.melt_rows.project",
                PhysicalOperatorKind::Project,
            )?,
            bridge_operator(
                "vortex.query_primitive.melt_rows.row_expansion",
                PhysicalOperatorKind::Project,
            )?,
        ],
        VortexQueryPrimitiveKind::ExplodeRows => vec![
            bridge_operator(
                "vortex.query_primitive.explode_rows.project",
                PhysicalOperatorKind::Project,
            )?,
            bridge_operator(
                "vortex.query_primitive.explode_rows.row_expansion",
                PhysicalOperatorKind::Project,
            )?,
        ],
        VortexQueryPrimitiveKind::PivotRows => vec![
            bridge_operator(
                "vortex.query_primitive.pivot_rows.project",
                PhysicalOperatorKind::Project,
            )?,
            bridge_operator(
                "vortex.query_primitive.pivot_rows.wide_reshape_state",
                PhysicalOperatorKind::Aggregate,
            )?,
        ],
        VortexQueryPrimitiveKind::RollingWindowRows => vec![
            bridge_operator(
                "vortex.query_primitive.rolling_window_rows.project",
                PhysicalOperatorKind::Project,
            )?,
            bridge_operator(
                "vortex.query_primitive.rolling_window_rows.window_state",
                PhysicalOperatorKind::Window,
            )?,
        ],
        VortexQueryPrimitiveKind::DistinctRows
        | VortexQueryPrimitiveKind::SimpleAggregate
        | VortexQueryPrimitiveKind::Unsupported => vec![bridge_operator(
            "vortex.query_primitive.unsupported",
            PhysicalOperatorKind::Unsupported,
        )?],
    };
    if request.source_order_limit.is_some()
        && !matches!(
            request.kind,
            VortexQueryPrimitiveKind::CountAll
                | VortexQueryPrimitiveKind::CountWhere
                | VortexQueryPrimitiveKind::DuplicateMaskRows
                | VortexQueryPrimitiveKind::TailRows
                | VortexQueryPrimitiveKind::SampleRows
                | VortexQueryPrimitiveKind::SortRows
                | VortexQueryPrimitiveKind::ExpressionProjectRows
                | VortexQueryPrimitiveKind::MeltRows
                | VortexQueryPrimitiveKind::ExplodeRows
                | VortexQueryPrimitiveKind::PivotRows
                | VortexQueryPrimitiveKind::SimpleAggregate
                | VortexQueryPrimitiveKind::Unsupported
        )
    {
        operators.push(bridge_operator(
            "vortex.query_primitive.source_order_limit.limit",
            PhysicalOperatorKind::Limit,
        )?);
    }
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

/// Builds a report-only physical-operator plan from an evaluated Vortex primitive.
///
/// # Errors
/// Returns an error only if an internal static operator id is malformed.
pub fn physical_operator_plan_for_vortex_query_primitive_result(
    result: &VortexQueryPrimitiveResult,
) -> Result<PhysicalOperatorPlan> {
    if !result.status.has_result() {
        return physical_operator_plan_for_vortex_query_primitive(&result.request);
    }
    let operators = match result.request.kind {
        VortexQueryPrimitiveKind::CountAll => vec![metadata_bridge_operator(
            "vortex.query_primitive.count_all.metadata_count_aggregate",
            PhysicalOperatorKind::CountAggregate,
        )?],
        VortexQueryPrimitiveKind::CountWhere => vec![
            metadata_bridge_operator(
                "vortex.query_primitive.count_where.metadata_filter",
                PhysicalOperatorKind::Filter,
            )?,
            metadata_bridge_operator(
                "vortex.query_primitive.count_where.metadata_count_aggregate",
                PhysicalOperatorKind::CountAggregate,
            )?,
        ],
        VortexQueryPrimitiveKind::FilterPredicate => vec![metadata_bridge_operator(
            "vortex.query_primitive.filter_predicate.metadata_filter",
            PhysicalOperatorKind::Filter,
        )?],
        VortexQueryPrimitiveKind::ProjectColumns
        | VortexQueryPrimitiveKind::FilterAndProject
        | VortexQueryPrimitiveKind::DistinctRows
        | VortexQueryPrimitiveKind::DuplicateMaskRows
        | VortexQueryPrimitiveKind::TailRows
        | VortexQueryPrimitiveKind::SampleRows
        | VortexQueryPrimitiveKind::ExpressionProjectRows
        | VortexQueryPrimitiveKind::MeltRows
        | VortexQueryPrimitiveKind::ExplodeRows
        | VortexQueryPrimitiveKind::PivotRows
        | VortexQueryPrimitiveKind::RollingWindowRows
        | VortexQueryPrimitiveKind::SimpleAggregate
        | VortexQueryPrimitiveKind::SortRows
        | VortexQueryPrimitiveKind::Unsupported => {
            return physical_operator_plan_for_vortex_query_primitive(&result.request);
        }
    };
    let mut plan = PhysicalOperatorPlan {
        schema_version: "shardloom.physical_operator_plan.v1",
        plan_id: format!(
            "vortex.query-primitive.{}.metadata-result-physical-plan",
            result.request.kind.as_str()
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

/// Builds a side-effect-free bridge report from an evaluated Vortex query primitive.
///
/// # Errors
/// Returns an error only if an internal static operator id is malformed.
pub fn plan_vortex_query_primitive_result_physical_operators(
    result: &VortexQueryPrimitiveResult,
) -> Result<VortexPhysicalOperatorBridgeReport> {
    VortexPhysicalOperatorBridgeReport::from_result(result)
}

/// Builds a side-effect-free bridge report from an evaluated Vortex query primitive
/// using explicit admission evidence.
///
/// # Errors
/// Returns an error only if an internal static operator id is malformed.
pub fn plan_vortex_query_primitive_result_physical_operators_with_evidence(
    result: &VortexQueryPrimitiveResult,
    correctness_evidence: BenchmarkEvidenceState,
    benchmark_evidence: BenchmarkEvidenceState,
    memory: OperatorMemoryCertification,
    fallback: BenchmarkFallbackState,
) -> Result<VortexPhysicalOperatorBridgeReport> {
    VortexPhysicalOperatorBridgeReport::from_result_with_evidence(
        result,
        correctness_evidence,
        benchmark_evidence,
        memory,
        fallback,
    )
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

fn metadata_bridge_operator(
    operator_id: &str,
    kind: PhysicalOperatorKind,
) -> Result<PhysicalOperatorContract> {
    PhysicalOperatorContract::new(
        operator_id,
        kind,
        PhysicalOperatorExecutionLevel::MetadataOnly,
        vec![PhysicalKernelRequirement::present(KernelKind::Metadata)],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{
        ColumnRef, DatasetUri, PhysicalOperatorPlanningCertificateStatus, PredicateExpr,
    };

    fn uri() -> DatasetUri {
        DatasetUri::new("file:///tmp/test.vortex").expect("uri")
    }

    fn safe_streaming_memory() -> OperatorMemoryCertification {
        OperatorMemoryCertification {
            streaming: true,
            bounded_memory: true,
            spillable: false,
            requires_full_materialization: false,
            requires_shuffle: false,
            oom_safe: true,
        }
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
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            expression_projection: None,
            melt_projection: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            simple_aggregate: None,
            sort_rows: None,
            diagnostics: Vec::new(),
        };

        let plan = physical_operator_plan_for_vortex_query_primitive(&request).expect("plan");

        assert_eq!(plan.operators.len(), 2);
        assert_eq!(plan.operators[0].kind, PhysicalOperatorKind::Filter);
        assert_eq!(plan.operators[1].kind, PhysicalOperatorKind::Project);
        assert!(!plan.fallback_execution_allowed());
    }

    #[test]
    fn filter_project_limit_bridge_adds_limit_operator() {
        let request = VortexQueryPrimitiveRequest::filter_and_project(
            uri(),
            PredicateExpr::AlwaysTrue,
            shardloom_plan::ProjectionRequest::all(),
        )
        .with_source_order_limit(2);

        let plan = physical_operator_plan_for_vortex_query_primitive(&request).expect("plan");

        assert_eq!(plan.operators.len(), 3);
        assert_eq!(plan.operators[0].kind, PhysicalOperatorKind::Filter);
        assert_eq!(plan.operators[1].kind, PhysicalOperatorKind::Project);
        assert_eq!(plan.operators[2].kind, PhysicalOperatorKind::Limit);
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

    #[test]
    fn metadata_count_result_marks_metadata_count_kernel_present_without_execution() {
        let result = VortexQueryPrimitiveResult::metadata_answered(
            VortexQueryPrimitiveRequest::count_all(uri()),
            crate::VortexQueryPrimitiveValue::Count(9),
        );

        let report =
            plan_vortex_query_primitive_result_physical_operators(&result).expect("report");

        assert_eq!(
            report.status,
            VortexPhysicalOperatorBridgeStatus::MetadataReady
        );
        assert_eq!(report.physical_plan.ready_for_native_planning_count(), 1);
        assert_eq!(report.physical_plan.missing_kernel_count(), 0);
        assert_eq!(report.planning_certificate.required_slot_count, 1);
        assert_eq!(report.planning_certificate.missing_slot_count, 0);
        assert_eq!(report.planning_certificate.selection_blocked_count, 0);
        assert_eq!(report.planning_certificate.admission_blocked_count, 1);
        assert!(!report.runtime_execution_allowed());
        assert!(!report.fallback_execution_allowed());
    }

    #[test]
    fn metadata_result_with_correctness_and_memory_evidence_reaches_native_planning() {
        let result = VortexQueryPrimitiveResult::metadata_answered(
            VortexQueryPrimitiveRequest::count_all(uri()),
            crate::VortexQueryPrimitiveValue::Count(9),
        );

        let report = plan_vortex_query_primitive_result_physical_operators_with_evidence(
            &result,
            BenchmarkEvidenceState::Present,
            BenchmarkEvidenceState::Missing,
            safe_streaming_memory(),
            BenchmarkFallbackState::NotAttempted,
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexPhysicalOperatorBridgeStatus::MetadataReady
        );
        assert_eq!(
            report.planning_certificate.status,
            PhysicalOperatorPlanningCertificateStatus::ReadyForNativePlanning
        );
        assert_eq!(report.planning_certificate.required_slot_count, 1);
        assert_eq!(report.planning_certificate.missing_slot_count, 0);
        assert_eq!(report.planning_certificate.selection_blocked_count, 0);
        assert_eq!(report.planning_certificate.admission_blocked_count, 0);
        assert_eq!(report.planning_certificate.registry_ready_slot_count, 1);
        assert_eq!(report.planning_certificate.production_ready_slot_count, 0);
        assert!(report.planning_certificate.can_plan_native());
        assert!(!report.planning_certificate.can_satisfy_production_claim());
        assert!(!report.runtime_execution_allowed());
        assert!(!report.fallback_execution_allowed());
        assert!(report.diagnostics.is_empty());
    }

    #[test]
    fn metadata_result_with_benchmark_evidence_reaches_production_certificate() {
        let result = VortexQueryPrimitiveResult::metadata_answered(
            VortexQueryPrimitiveRequest::count_all(uri()),
            crate::VortexQueryPrimitiveValue::Count(9),
        );

        let report = plan_vortex_query_primitive_result_physical_operators_with_evidence(
            &result,
            BenchmarkEvidenceState::Present,
            BenchmarkEvidenceState::Present,
            safe_streaming_memory(),
            BenchmarkFallbackState::NotAttempted,
        )
        .expect("report");

        assert_eq!(
            report.planning_certificate.status,
            PhysicalOperatorPlanningCertificateStatus::ProductionCertified
        );
        assert_eq!(report.planning_certificate.production_ready_slot_count, 1);
        assert!(report.planning_certificate.can_plan_native());
        assert!(report.planning_certificate.can_satisfy_production_claim());
        assert!(!report.runtime_execution_allowed());
        assert!(!report.fallback_execution_allowed());
    }

    #[test]
    fn metadata_result_with_fallback_attempted_blocks_admission() {
        let result = VortexQueryPrimitiveResult::metadata_answered(
            VortexQueryPrimitiveRequest::count_all(uri()),
            crate::VortexQueryPrimitiveValue::Count(9),
        );

        let report = plan_vortex_query_primitive_result_physical_operators_with_evidence(
            &result,
            BenchmarkEvidenceState::Present,
            BenchmarkEvidenceState::Present,
            safe_streaming_memory(),
            BenchmarkFallbackState::Attempted,
        )
        .expect("report");

        assert_eq!(
            report.planning_certificate.status,
            PhysicalOperatorPlanningCertificateStatus::KernelAdmissionBlocked
        );
        assert_eq!(report.planning_certificate.admission_blocked_count, 1);
        assert!(report.planning_certificate.fallback_attempted);
        assert!(!report.planning_certificate.can_plan_native());
        assert!(!report.runtime_execution_allowed());
        assert!(!report.fallback_execution_allowed());
        assert!(!report.diagnostics.is_empty());
    }

    #[test]
    fn metadata_filter_result_marks_filter_metadata_kernel_present_without_execution() {
        let result = VortexQueryPrimitiveResult::metadata_answered(
            VortexQueryPrimitiveRequest::filter(
                uri(),
                PredicateExpr::IsNull {
                    column: ColumnRef::new("flag").expect("column"),
                },
            ),
            crate::VortexQueryPrimitiveValue::Boolean(false),
        );

        let plan = physical_operator_plan_for_vortex_query_primitive_result(&result).expect("plan");

        assert_eq!(plan.operators.len(), 1);
        assert_eq!(plan.operators[0].kind, PhysicalOperatorKind::Filter);
        assert_eq!(
            plan.operators[0].execution_level,
            PhysicalOperatorExecutionLevel::MetadataOnly
        );
        assert!(plan.operators[0].can_plan_native());
        assert!(!plan.fallback_execution_allowed());
    }

    #[test]
    fn non_metadata_result_keeps_original_missing_kernel_blockers() {
        let result = VortexQueryPrimitiveResult::needs_encoded_read(
            VortexQueryPrimitiveRequest::project(
                uri(),
                shardloom_plan::ProjectionRequest::columns(vec![
                    ColumnRef::new("col1").expect("column"),
                ]),
            ),
            "projection requires encoded read",
        );

        let report =
            plan_vortex_query_primitive_result_physical_operators(&result).expect("report");

        assert_eq!(report.status, VortexPhysicalOperatorBridgeStatus::Planned);
        assert_eq!(report.physical_plan.ready_for_native_planning_count(), 0);
        assert_eq!(report.physical_plan.missing_kernel_count(), 1);
        assert!(!report.planning_certificate.can_plan_native());
    }
}
