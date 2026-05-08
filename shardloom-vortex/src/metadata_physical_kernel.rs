use std::fmt::Write as _;

use shardloom_core::{
    Diagnostic, DiagnosticCode, KernelKind, PhysicalOperatorKind,
    PhysicalOperatorPlanningCertificateStatus,
};

use crate::{
    VortexPhysicalOperatorBridgeReport, VortexQueryPrimitiveKind, VortexQueryPrimitiveResult,
    VortexQueryPrimitiveStatus, VortexQueryPrimitiveValue,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexMetadataPhysicalKernelStatus {
    EvaluatedMetadataOnly,
    BlockedByCertificate,
    BlockedByPrimitive,
    BlockedByValue,
    Unsupported,
}

impl VortexMetadataPhysicalKernelStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::EvaluatedMetadataOnly => "evaluated_metadata_only",
            Self::BlockedByCertificate => "blocked_by_certificate",
            Self::BlockedByPrimitive => "blocked_by_primitive",
            Self::BlockedByValue => "blocked_by_value",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        !matches!(self, Self::EvaluatedMetadataOnly)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexMetadataPhysicalKernelReport {
    pub schema_version: &'static str,
    pub kernel_report_id: String,
    pub primitive_kind: VortexQueryPrimitiveKind,
    pub certificate_status: PhysicalOperatorPlanningCertificateStatus,
    pub status: VortexMetadataPhysicalKernelStatus,
    pub evaluated_operator_kinds: Vec<PhysicalOperatorKind>,
    pub kernel_kind: KernelKind,
    pub value: VortexQueryPrimitiveValue,
    pub metadata_kernel_count: usize,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexMetadataPhysicalKernelReport {
    #[must_use]
    pub fn evaluated(
        result: &VortexQueryPrimitiveResult,
        bridge: &VortexPhysicalOperatorBridgeReport,
        evaluated_operator_kinds: Vec<PhysicalOperatorKind>,
    ) -> Self {
        let metadata_kernel_count = evaluated_operator_kinds.len();
        Self {
            schema_version: "shardloom.vortex_metadata_physical_kernel.v1",
            kernel_report_id: format!(
                "vortex.query-primitive.{}.metadata-physical-kernel",
                result.request.kind.as_str()
            ),
            primitive_kind: result.request.kind,
            certificate_status: bridge.planning_certificate.status,
            status: VortexMetadataPhysicalKernelStatus::EvaluatedMetadataOnly,
            evaluated_operator_kinds,
            kernel_kind: KernelKind::Metadata,
            value: result.value.clone(),
            metadata_kernel_count,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            fallback_execution_allowed: false,
            diagnostics: Vec::new(),
        }
    }

    #[must_use]
    pub fn blocked(
        result: &VortexQueryPrimitiveResult,
        bridge: &VortexPhysicalOperatorBridgeReport,
        status: VortexMetadataPhysicalKernelStatus,
        diagnostic: Diagnostic,
    ) -> Self {
        let mut diagnostics = result.request.diagnostics.clone();
        diagnostics.extend(result.diagnostics.clone());
        diagnostics.extend(bridge.diagnostics.clone());
        diagnostics.push(diagnostic);
        Self {
            schema_version: "shardloom.vortex_metadata_physical_kernel.v1",
            kernel_report_id: format!(
                "vortex.query-primitive.{}.metadata-physical-kernel",
                result.request.kind.as_str()
            ),
            primitive_kind: result.request.kind,
            certificate_status: bridge.planning_certificate.status,
            status,
            evaluated_operator_kinds: Vec::new(),
            kernel_kind: KernelKind::Metadata,
            value: VortexQueryPrimitiveValue::Unknown,
            metadata_kernel_count: 0,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            fallback_execution_allowed: false,
            diagnostics,
        }
    }

    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    shardloom_core::DiagnosticSeverity::Error
                        | shardloom_core::DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut text = String::new();
        let _ = writeln!(text, "schema_version: {}", self.schema_version);
        let _ = writeln!(text, "kernel report: {}", self.kernel_report_id);
        let _ = writeln!(text, "primitive: {}", self.primitive_kind.as_str());
        let _ = writeln!(text, "status: {}", self.status.as_str());
        let _ = writeln!(text, "certificate: {}", self.certificate_status.as_str());
        let _ = writeln!(text, "kernel kind: {}", self.kernel_kind.as_str());
        let _ = writeln!(text, "metadata kernels: {}", self.metadata_kernel_count);
        let _ = writeln!(text, "data read: false");
        let _ = writeln!(text, "data decoded: false");
        let _ = writeln!(text, "data materialized: false");
        let _ = writeln!(text, "object-store io: false");
        let _ = writeln!(text, "write io: false");
        let _ = writeln!(text, "spill io performed: false");
        let _ = writeln!(text, "fallback execution: disabled");
        text
    }
}

/// Evaluates metadata-only physical kernels from an evidence-ready Vortex bridge.
///
/// This function consumes an already metadata-answered primitive result and a
/// matching physical-operator bridge report. It performs no file IO, scan,
/// encoded-data traversal, row read, decode, materialization, object-store IO,
/// write, spill IO, or fallback execution.
#[must_use]
pub fn evaluate_vortex_metadata_physical_kernels(
    result: &VortexQueryPrimitiveResult,
    bridge: &VortexPhysicalOperatorBridgeReport,
) -> VortexMetadataPhysicalKernelReport {
    if bridge.primitive_kind != result.request.kind {
        return VortexMetadataPhysicalKernelReport::blocked(
            result,
            bridge,
            VortexMetadataPhysicalKernelStatus::BlockedByPrimitive,
            Diagnostic::invalid_input(
                "vortex_metadata_physical_kernel",
                "bridge primitive kind does not match result primitive kind",
                "Use a physical-operator bridge produced from the same Vortex query primitive result.",
            ),
        );
    }
    if !bridge.planning_certificate.can_plan_native() {
        return VortexMetadataPhysicalKernelReport::blocked(
            result,
            bridge,
            VortexMetadataPhysicalKernelStatus::BlockedByCertificate,
            Diagnostic::not_implemented(
                "vortex_metadata_physical_kernel",
                "physical planning certificate is not ready for native metadata-only planning",
                "Supply correctness, memory-safety, and no-fallback evidence before evaluating metadata-only physical kernels.",
            ),
        );
    }
    if result.status != VortexQueryPrimitiveStatus::MetadataAnswered {
        return VortexMetadataPhysicalKernelReport::blocked(
            result,
            bridge,
            VortexMetadataPhysicalKernelStatus::BlockedByPrimitive,
            Diagnostic::not_implemented(
                "vortex_metadata_physical_kernel",
                "metadata-only physical kernels require an already metadata-answered primitive result",
                "Use encoded-native kernel planning for deferred or encoded-read primitive results.",
            ),
        );
    }

    match result.request.kind {
        VortexQueryPrimitiveKind::CountAll | VortexQueryPrimitiveKind::CountWhere => {
            evaluate_count_metadata_kernel(result, bridge)
        }
        VortexQueryPrimitiveKind::FilterPredicate => {
            evaluate_filter_metadata_kernel(result, bridge)
        }
        VortexQueryPrimitiveKind::ProjectColumns
        | VortexQueryPrimitiveKind::FilterAndProject
        | VortexQueryPrimitiveKind::SimpleAggregate
        | VortexQueryPrimitiveKind::Unsupported => VortexMetadataPhysicalKernelReport::blocked(
            result,
            bridge,
            VortexMetadataPhysicalKernelStatus::Unsupported,
            Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "vortex_metadata_physical_kernel",
                "metadata-only physical kernel evaluation is not supported for this primitive kind",
                Some(
                    "Use count/filter metadata primitives or wait for encoded-native kernels."
                        .to_string(),
                ),
            ),
        ),
    }
}

fn evaluate_count_metadata_kernel(
    result: &VortexQueryPrimitiveResult,
    bridge: &VortexPhysicalOperatorBridgeReport,
) -> VortexMetadataPhysicalKernelReport {
    if !matches!(result.value, VortexQueryPrimitiveValue::Count(_))
        || !bridge
            .physical_plan
            .has_operator_kind(PhysicalOperatorKind::CountAggregate)
    {
        return VortexMetadataPhysicalKernelReport::blocked(
            result,
            bridge,
            VortexMetadataPhysicalKernelStatus::BlockedByValue,
            Diagnostic::invalid_input(
                "vortex_metadata_physical_kernel",
                "count metadata kernel requires a count value and count aggregate physical operator",
                "Use a metadata CountAll or CountWhere primitive result with its matching physical bridge.",
            ),
        );
    }
    VortexMetadataPhysicalKernelReport::evaluated(result, bridge, operator_kinds(bridge))
}

fn evaluate_filter_metadata_kernel(
    result: &VortexQueryPrimitiveResult,
    bridge: &VortexPhysicalOperatorBridgeReport,
) -> VortexMetadataPhysicalKernelReport {
    if !matches!(result.value, VortexQueryPrimitiveValue::Boolean(_))
        || !bridge
            .physical_plan
            .has_operator_kind(PhysicalOperatorKind::Filter)
    {
        return VortexMetadataPhysicalKernelReport::blocked(
            result,
            bridge,
            VortexMetadataPhysicalKernelStatus::BlockedByValue,
            Diagnostic::invalid_input(
                "vortex_metadata_physical_kernel",
                "filter metadata kernel requires a boolean value and filter physical operator",
                "Use a metadata FilterPredicate primitive result with its matching physical bridge.",
            ),
        );
    }
    VortexMetadataPhysicalKernelReport::evaluated(result, bridge, operator_kinds(bridge))
}

fn operator_kinds(bridge: &VortexPhysicalOperatorBridgeReport) -> Vec<PhysicalOperatorKind> {
    bridge
        .physical_plan
        .operators
        .iter()
        .map(|operator| operator.kind)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{
        BenchmarkEvidenceState, BenchmarkFallbackState, ColumnRef, DatasetUri,
        OperatorMemoryCertification, PredicateExpr,
    };

    use crate::{
        VortexQueryPrimitiveRequest, plan_vortex_query_primitive_result_physical_operators,
        plan_vortex_query_primitive_result_physical_operators_with_evidence,
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

    fn evidence_ready_bridge(
        result: &VortexQueryPrimitiveResult,
    ) -> VortexPhysicalOperatorBridgeReport {
        plan_vortex_query_primitive_result_physical_operators_with_evidence(
            result,
            BenchmarkEvidenceState::Present,
            BenchmarkEvidenceState::Missing,
            safe_streaming_memory(),
            BenchmarkFallbackState::NotAttempted,
        )
        .expect("bridge")
    }

    #[test]
    fn count_where_metadata_physical_kernels_evaluate_without_io() {
        let result = VortexQueryPrimitiveResult::metadata_answered(
            VortexQueryPrimitiveRequest::count_where(
                uri(),
                PredicateExpr::IsNotNull {
                    column: ColumnRef::new("flag").expect("column"),
                },
            ),
            VortexQueryPrimitiveValue::Count(5),
        );
        let bridge = evidence_ready_bridge(&result);

        let report = evaluate_vortex_metadata_physical_kernels(&result, &bridge);

        assert_eq!(
            report.status,
            VortexMetadataPhysicalKernelStatus::EvaluatedMetadataOnly
        );
        assert_eq!(report.value, VortexQueryPrimitiveValue::Count(5));
        assert_eq!(report.metadata_kernel_count, 2);
        assert_eq!(
            report.evaluated_operator_kinds,
            vec![
                PhysicalOperatorKind::Filter,
                PhysicalOperatorKind::CountAggregate
            ]
        );
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
        assert!(
            report
                .to_human_text()
                .contains("fallback execution: disabled")
        );
    }

    #[test]
    fn default_missing_evidence_bridge_blocks_metadata_kernel_evaluation() {
        let result = VortexQueryPrimitiveResult::metadata_answered(
            VortexQueryPrimitiveRequest::count_all(uri()),
            VortexQueryPrimitiveValue::Count(5),
        );
        let bridge =
            plan_vortex_query_primitive_result_physical_operators(&result).expect("bridge");

        let report = evaluate_vortex_metadata_physical_kernels(&result, &bridge);

        assert_eq!(
            report.status,
            VortexMetadataPhysicalKernelStatus::BlockedByCertificate
        );
        assert_eq!(report.metadata_kernel_count, 0);
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn filter_predicate_metadata_kernel_accepts_boolean_metadata_value() {
        let result = VortexQueryPrimitiveResult::metadata_answered(
            VortexQueryPrimitiveRequest::filter(
                uri(),
                PredicateExpr::IsNull {
                    column: ColumnRef::new("flag").expect("column"),
                },
            ),
            VortexQueryPrimitiveValue::Boolean(false),
        );
        let bridge = evidence_ready_bridge(&result);

        let report = evaluate_vortex_metadata_physical_kernels(&result, &bridge);

        assert_eq!(
            report.status,
            VortexMetadataPhysicalKernelStatus::EvaluatedMetadataOnly
        );
        assert_eq!(report.value, VortexQueryPrimitiveValue::Boolean(false));
        assert_eq!(
            report.evaluated_operator_kinds,
            vec![PhysicalOperatorKind::Filter]
        );
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn non_metadata_result_blocks_metadata_physical_kernel_evaluation() {
        let result = VortexQueryPrimitiveResult::needs_encoded_read(
            VortexQueryPrimitiveRequest::project(
                uri(),
                shardloom_plan::ProjectionRequest::columns(vec![
                    ColumnRef::new("col1").expect("column"),
                ]),
            ),
            "projection requires encoded read",
        );
        let bridge = evidence_ready_bridge(&result);

        let report = evaluate_vortex_metadata_physical_kernels(&result, &bridge);

        assert_eq!(
            report.status,
            VortexMetadataPhysicalKernelStatus::BlockedByCertificate
        );
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn mismatched_bridge_blocks_metadata_physical_kernel_evaluation() {
        let result = VortexQueryPrimitiveResult::metadata_answered(
            VortexQueryPrimitiveRequest::count_all(uri()),
            VortexQueryPrimitiveValue::Count(5),
        );
        let filter_result = VortexQueryPrimitiveResult::metadata_answered(
            VortexQueryPrimitiveRequest::filter(uri(), PredicateExpr::AlwaysFalse),
            VortexQueryPrimitiveValue::Boolean(false),
        );
        let bridge = evidence_ready_bridge(&filter_result);

        let report = evaluate_vortex_metadata_physical_kernels(&result, &bridge);

        assert_eq!(
            report.status,
            VortexMetadataPhysicalKernelStatus::BlockedByPrimitive
        );
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
    }
}
