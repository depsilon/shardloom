use std::fmt::Write as _;

use shardloom_core::{
    BenchmarkEvidenceState, BenchmarkFallbackState, Diagnostic, DiagnosticCode, KernelKind,
    OperatorMemoryCertification, PhysicalKernelAdmissionReport, PhysicalKernelAdmissionStatus,
    PhysicalKernelRequirement, PhysicalKernelSlot, PhysicalOperatorContract,
    PhysicalOperatorExecutionLevel, PhysicalOperatorKind,
    PhysicalOperatorPlanningCertificateStatus, ShardLoomError,
};

use crate::{
    VortexPhysicalOperatorBridgeReport, VortexQueryPrimitiveKind, VortexQueryPrimitiveResult,
    VortexQueryPrimitiveStatus, VortexQueryPrimitiveValue,
};

const SCHEMA_VERSION: &str = "shardloom.vortex_metadata_physical_kernel.v1";
const COUNT_ADMISSION_SCHEMA_VERSION: &str = "shardloom.vortex_metadata_count_kernel_admission.v1";
const FILTER_ADMISSION_SCHEMA_VERSION: &str =
    "shardloom.vortex_metadata_filter_kernel_admission.v1";
const COUNT_ALL_OPERATOR_ID: &str = "vortex.query_primitive.count_all.metadata_count_aggregate";
const COUNT_WHERE_OPERATOR_ID: &str = "vortex.query_primitive.count_where.metadata_count_aggregate";
const FILTER_KERNEL_REPORT_ID: &str =
    "vortex.query-primitive.filter_predicate.metadata-physical-kernel";
const FILTER_OPERATOR_ID: &str = "vortex.query_primitive.filter_predicate.metadata_filter";

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
            schema_version: SCHEMA_VERSION,
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
            schema_version: SCHEMA_VERSION,
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
    pub fn is_safe_metadata_count_kernel_evidence(&self) -> bool {
        self.status == VortexMetadataPhysicalKernelStatus::EvaluatedMetadataOnly
            && matches!(
                self.primitive_kind,
                VortexQueryPrimitiveKind::CountAll | VortexQueryPrimitiveKind::CountWhere
            )
            && self.kernel_kind == KernelKind::Metadata
            && matches!(self.value, VortexQueryPrimitiveValue::Count(_))
            && self
                .evaluated_operator_kinds
                .contains(&PhysicalOperatorKind::CountAggregate)
            && self.is_side_effect_free()
            && !self.has_errors()
    }

    #[must_use]
    pub fn is_safe_metadata_filter_kernel_evidence(&self) -> bool {
        self.status == VortexMetadataPhysicalKernelStatus::EvaluatedMetadataOnly
            && self.primitive_kind == VortexQueryPrimitiveKind::FilterPredicate
            && self.kernel_kind == KernelKind::Metadata
            && matches!(self.value, VortexQueryPrimitiveValue::Boolean(_))
            && self.metadata_kernel_count == 1
            && matches!(
                self.evaluated_operator_kinds.as_slice(),
                [PhysicalOperatorKind::Filter]
            )
            && self.is_side_effect_free()
            && !self.has_errors()
            && self.kernel_report_id == FILTER_KERNEL_REPORT_ID
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

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexMetadataCountKernelAdmissionReport {
    pub schema_version: &'static str,
    pub admission_id: String,
    pub metadata_kernel_report_id: String,
    pub primitive_kind: VortexQueryPrimitiveKind,
    pub slot_id: String,
    pub operator_kind: PhysicalOperatorKind,
    pub required_kernel_kind: KernelKind,
    pub candidate_kernel_kind: KernelKind,
    pub correctness_evidence: BenchmarkEvidenceState,
    pub benchmark_evidence: BenchmarkEvidenceState,
    pub memory: OperatorMemoryCertification,
    pub fallback: BenchmarkFallbackState,
    pub status: PhysicalKernelAdmissionStatus,
    pub slot_marked_present: bool,
    pub production_claim_allowed: bool,
    pub runtime_execution_allowed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexMetadataCountKernelAdmissionReport {
    #[must_use]
    pub fn from_admission(
        metadata_kernel: &VortexMetadataPhysicalKernelReport,
        admission: PhysicalKernelAdmissionReport,
    ) -> Self {
        let mut diagnostics = metadata_kernel.diagnostics.clone();
        diagnostics.extend(admission.diagnostics.clone());
        let slot_marked_present = admission.can_mark_kernel_present();
        let production_claim_allowed = admission.can_satisfy_production_claim();
        Self {
            schema_version: COUNT_ADMISSION_SCHEMA_VERSION,
            admission_id: format!("{}.count-admission", metadata_kernel.kernel_report_id),
            metadata_kernel_report_id: metadata_kernel.kernel_report_id.clone(),
            primitive_kind: metadata_kernel.primitive_kind,
            slot_id: admission.slot_id,
            operator_kind: admission.operator_kind,
            required_kernel_kind: admission.required_kernel_kind,
            candidate_kernel_kind: admission.candidate_kernel_kind,
            correctness_evidence: admission.correctness_evidence,
            benchmark_evidence: admission.benchmark_evidence,
            memory: admission.memory,
            fallback: admission.fallback,
            status: admission.status,
            slot_marked_present,
            production_claim_allowed,
            runtime_execution_allowed: false,
            fallback_execution_allowed: false,
            diagnostics,
        }
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.status.can_enter_registry()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    shardloom_core::DiagnosticSeverity::Error
                        | shardloom_core::DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.runtime_execution_allowed && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "metadata count kernel admission\nschema_version: {}\nadmission: {}\nprimitive: {}\nslot: {}\noperator: {}\nrequired kernel: {}\ncandidate kernel: {}\nstatus: {}\nslot marked present: {}\nproduction claim allowed: {}\nruntime execution: disabled\nfallback execution: disabled",
            self.schema_version,
            self.admission_id,
            self.primitive_kind.as_str(),
            self.slot_id,
            self.operator_kind.as_str(),
            self.required_kernel_kind.as_str(),
            self.candidate_kernel_kind.as_str(),
            self.status.as_str(),
            self.slot_marked_present,
            self.production_claim_allowed
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexMetadataFilterKernelAdmissionReport {
    pub schema_version: &'static str,
    pub admission_id: String,
    pub metadata_kernel_report_id: String,
    pub slot_id: String,
    pub operator_kind: PhysicalOperatorKind,
    pub required_kernel_kind: KernelKind,
    pub candidate_kernel_kind: KernelKind,
    pub correctness_evidence: BenchmarkEvidenceState,
    pub benchmark_evidence: BenchmarkEvidenceState,
    pub memory: OperatorMemoryCertification,
    pub fallback: BenchmarkFallbackState,
    pub status: PhysicalKernelAdmissionStatus,
    pub slot_marked_present: bool,
    pub production_claim_allowed: bool,
    pub runtime_execution_allowed: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexMetadataFilterKernelAdmissionReport {
    #[must_use]
    pub fn from_admission(
        metadata_kernel: &VortexMetadataPhysicalKernelReport,
        admission: PhysicalKernelAdmissionReport,
    ) -> Self {
        let mut diagnostics = metadata_kernel.diagnostics.clone();
        diagnostics.extend(admission.diagnostics.clone());
        let slot_marked_present = admission.can_mark_kernel_present();
        let production_claim_allowed = admission.can_satisfy_production_claim();
        Self {
            schema_version: FILTER_ADMISSION_SCHEMA_VERSION,
            admission_id: format!("{}.filter-admission", metadata_kernel.kernel_report_id),
            metadata_kernel_report_id: metadata_kernel.kernel_report_id.clone(),
            slot_id: admission.slot_id,
            operator_kind: admission.operator_kind,
            required_kernel_kind: admission.required_kernel_kind,
            candidate_kernel_kind: admission.candidate_kernel_kind,
            correctness_evidence: admission.correctness_evidence,
            benchmark_evidence: admission.benchmark_evidence,
            memory: admission.memory,
            fallback: admission.fallback,
            status: admission.status,
            slot_marked_present,
            production_claim_allowed,
            runtime_execution_allowed: false,
            fallback_execution_allowed: false,
            diagnostics,
        }
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.status.can_enter_registry()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    shardloom_core::DiagnosticSeverity::Error
                        | shardloom_core::DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.runtime_execution_allowed && !self.fallback_execution_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        format!(
            "metadata filter kernel admission\nschema_version: {}\nadmission: {}\nslot: {}\noperator: {}\nrequired kernel: {}\ncandidate kernel: {}\nstatus: {}\nslot marked present: {}\nproduction claim allowed: {}\nruntime execution: disabled\nfallback execution: disabled",
            self.schema_version,
            self.admission_id,
            self.slot_id,
            self.operator_kind.as_str(),
            self.required_kernel_kind.as_str(),
            self.candidate_kernel_kind.as_str(),
            self.status.as_str(),
            self.slot_marked_present,
            self.production_claim_allowed
        )
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

/// Admits already evaluated metadata-only count evidence into the CG-7
/// count-aggregate metadata kernel slot.
///
/// This is a contextual evidence bridge. It does not register a global runtime
/// aggregate kernel, execute encoded data, claim production readiness, or close
/// broader count/aggregate kernel work.
///
/// # Errors
/// Returns an error if the metadata kernel report is not for a count primitive
/// or if the static metadata-count operator contract cannot be built.
pub fn admit_vortex_metadata_count_kernel(
    metadata_kernel: &VortexMetadataPhysicalKernelReport,
) -> shardloom_core::Result<VortexMetadataCountKernelAdmissionReport> {
    let slot = metadata_count_kernel_slot(metadata_kernel.primitive_kind)?;
    let safe_evidence = metadata_kernel.is_safe_metadata_count_kernel_evidence();
    let admission = PhysicalKernelAdmissionReport::evaluate(
        &slot,
        KernelKind::Metadata,
        if safe_evidence {
            BenchmarkEvidenceState::Present
        } else {
            BenchmarkEvidenceState::Missing
        },
        BenchmarkEvidenceState::Missing,
        if safe_evidence {
            safe_metadata_count_memory()
        } else {
            OperatorMemoryCertification::unsupported()
        },
        if metadata_kernel.fallback_execution_allowed {
            BenchmarkFallbackState::Attempted
        } else {
            BenchmarkFallbackState::NotAttempted
        },
    );
    Ok(VortexMetadataCountKernelAdmissionReport::from_admission(
        metadata_kernel,
        admission,
    ))
}

fn metadata_count_kernel_slot(
    primitive_kind: VortexQueryPrimitiveKind,
) -> shardloom_core::Result<PhysicalKernelSlot> {
    let operator_id = match primitive_kind {
        VortexQueryPrimitiveKind::CountAll => COUNT_ALL_OPERATOR_ID,
        VortexQueryPrimitiveKind::CountWhere => COUNT_WHERE_OPERATOR_ID,
        _ => {
            return Err(ShardLoomError::InvalidOperation(format!(
                "metadata count kernel admission requires a count primitive, got {}",
                primitive_kind.as_str()
            )));
        }
    };
    let operator = PhysicalOperatorContract::new(
        operator_id,
        PhysicalOperatorKind::CountAggregate,
        PhysicalOperatorExecutionLevel::MetadataOnly,
        vec![PhysicalKernelRequirement::missing(KernelKind::Metadata)],
    )?;
    Ok(PhysicalKernelSlot::from_requirement(
        &operator,
        PhysicalKernelRequirement::missing(KernelKind::Metadata),
    ))
}

const fn safe_metadata_count_memory() -> OperatorMemoryCertification {
    OperatorMemoryCertification {
        streaming: true,
        bounded_memory: true,
        spillable: false,
        requires_full_materialization: false,
        requires_shuffle: false,
        oom_safe: true,
    }
}

/// Admits already evaluated metadata-only filter evidence into the CG-7 filter
/// metadata kernel slot.
///
/// This is a contextual evidence bridge. It does not register a global runtime
/// filter kernel, execute encoded predicates, materialize rows, claim production
/// readiness, or close broader filter-kernel work.
///
/// # Errors
/// Returns an error only if the static metadata-filter operator contract cannot
/// be built.
pub fn admit_vortex_metadata_filter_kernel(
    metadata_kernel: &VortexMetadataPhysicalKernelReport,
) -> shardloom_core::Result<VortexMetadataFilterKernelAdmissionReport> {
    let slot = metadata_filter_kernel_slot()?;
    let safe_evidence = metadata_kernel.is_safe_metadata_filter_kernel_evidence();
    let admission = PhysicalKernelAdmissionReport::evaluate(
        &slot,
        KernelKind::Metadata,
        if safe_evidence {
            BenchmarkEvidenceState::Present
        } else {
            BenchmarkEvidenceState::Missing
        },
        BenchmarkEvidenceState::Missing,
        if safe_evidence {
            safe_metadata_filter_memory()
        } else {
            OperatorMemoryCertification::unsupported()
        },
        if metadata_kernel.fallback_execution_allowed {
            BenchmarkFallbackState::Attempted
        } else {
            BenchmarkFallbackState::NotAttempted
        },
    );
    Ok(VortexMetadataFilterKernelAdmissionReport::from_admission(
        metadata_kernel,
        admission,
    ))
}

fn metadata_filter_kernel_slot() -> shardloom_core::Result<PhysicalKernelSlot> {
    let operator = PhysicalOperatorContract::new(
        FILTER_OPERATOR_ID,
        PhysicalOperatorKind::Filter,
        PhysicalOperatorExecutionLevel::MetadataOnly,
        vec![PhysicalKernelRequirement::missing(KernelKind::Metadata)],
    )?;
    Ok(PhysicalKernelSlot::from_requirement(
        &operator,
        PhysicalKernelRequirement::missing(KernelKind::Metadata),
    ))
}

const fn safe_metadata_filter_memory() -> OperatorMemoryCertification {
    OperatorMemoryCertification {
        streaming: true,
        bounded_memory: true,
        spillable: false,
        requires_full_materialization: false,
        requires_shuffle: false,
        oom_safe: true,
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
        OperatorMemoryCertification, PhysicalKernelAdmissionStatus, PredicateExpr,
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
    fn safe_metadata_count_kernel_admits_metadata_slot_without_production_claim() {
        let result = VortexQueryPrimitiveResult::metadata_answered(
            VortexQueryPrimitiveRequest::count_all(uri()),
            VortexQueryPrimitiveValue::Count(7),
        );
        let bridge = evidence_ready_bridge(&result);
        let metadata_kernel = evaluate_vortex_metadata_physical_kernels(&result, &bridge);

        let admission =
            admit_vortex_metadata_count_kernel(&metadata_kernel).expect("count admission");

        assert_eq!(admission.schema_version, COUNT_ADMISSION_SCHEMA_VERSION);
        assert_eq!(
            admission.status,
            PhysicalKernelAdmissionStatus::RegistryReady
        );
        assert_eq!(
            admission.operator_kind,
            PhysicalOperatorKind::CountAggregate
        );
        assert_eq!(admission.required_kernel_kind, KernelKind::Metadata);
        assert_eq!(admission.candidate_kernel_kind, KernelKind::Metadata);
        assert_eq!(
            admission.correctness_evidence,
            BenchmarkEvidenceState::Present
        );
        assert_eq!(
            admission.benchmark_evidence,
            BenchmarkEvidenceState::Missing
        );
        assert_eq!(admission.primitive_kind, VortexQueryPrimitiveKind::CountAll);
        assert!(admission.slot_marked_present);
        assert!(!admission.production_claim_allowed);
        assert!(admission.is_side_effect_free());
        assert!(!admission.has_errors());
    }

    #[test]
    fn safe_metadata_filtered_count_admits_count_aggregate_slot() {
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
        let metadata_kernel = evaluate_vortex_metadata_physical_kernels(&result, &bridge);

        let admission =
            admit_vortex_metadata_count_kernel(&metadata_kernel).expect("count admission");

        assert_eq!(
            admission.status,
            PhysicalKernelAdmissionStatus::RegistryReady
        );
        assert_eq!(
            admission.primitive_kind,
            VortexQueryPrimitiveKind::CountWhere
        );
        assert_eq!(
            admission.operator_kind,
            PhysicalOperatorKind::CountAggregate
        );
        assert!(admission.slot_id.contains("count_where"));
        assert!(admission.slot_marked_present);
        assert!(!admission.production_claim_allowed);
    }

    #[test]
    fn blocked_metadata_count_kernel_cannot_admit_slot() {
        let result = VortexQueryPrimitiveResult::metadata_answered(
            VortexQueryPrimitiveRequest::count_all(uri()),
            VortexQueryPrimitiveValue::Count(7),
        );
        let bridge =
            plan_vortex_query_primitive_result_physical_operators(&result).expect("bridge");
        let metadata_kernel = evaluate_vortex_metadata_physical_kernels(&result, &bridge);

        let admission =
            admit_vortex_metadata_count_kernel(&metadata_kernel).expect("count admission");

        assert_eq!(
            admission.status,
            PhysicalKernelAdmissionStatus::BlockedMissingCorrectness
        );
        assert_eq!(
            admission.correctness_evidence,
            BenchmarkEvidenceState::Missing
        );
        assert!(!admission.slot_marked_present);
        assert!(!admission.production_claim_allowed);
        assert!(admission.has_errors());
        assert!(admission.is_side_effect_free());
    }

    #[test]
    fn metadata_count_admission_rejects_non_count_primitives() {
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
        let metadata_kernel = evaluate_vortex_metadata_physical_kernels(&result, &bridge);

        let error = admit_vortex_metadata_count_kernel(&metadata_kernel)
            .expect_err("filter cannot enter count admission");

        assert!(
            error.message().contains("filter_predicate"),
            "{}",
            error.message()
        );
    }

    #[test]
    fn safe_metadata_filter_kernel_admits_metadata_slot_without_production_claim() {
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
        let metadata_kernel = evaluate_vortex_metadata_physical_kernels(&result, &bridge);

        let admission =
            admit_vortex_metadata_filter_kernel(&metadata_kernel).expect("filter admission");

        assert_eq!(admission.schema_version, FILTER_ADMISSION_SCHEMA_VERSION);
        assert_eq!(
            admission.status,
            PhysicalKernelAdmissionStatus::RegistryReady
        );
        assert_eq!(admission.operator_kind, PhysicalOperatorKind::Filter);
        assert_eq!(admission.required_kernel_kind, KernelKind::Metadata);
        assert_eq!(admission.candidate_kernel_kind, KernelKind::Metadata);
        assert_eq!(
            admission.correctness_evidence,
            BenchmarkEvidenceState::Present
        );
        assert_eq!(
            admission.benchmark_evidence,
            BenchmarkEvidenceState::Missing
        );
        assert!(admission.slot_marked_present);
        assert!(!admission.production_claim_allowed);
        assert!(admission.memory.streaming);
        assert!(admission.memory.bounded_memory);
        assert!(admission.memory.oom_safe);
        assert!(!admission.memory.requires_full_materialization);
        assert!(admission.is_side_effect_free());
        assert!(!admission.has_errors());
    }

    #[test]
    fn blocked_metadata_filter_kernel_cannot_admit_slot() {
        let result = VortexQueryPrimitiveResult::metadata_answered(
            VortexQueryPrimitiveRequest::filter(uri(), PredicateExpr::AlwaysFalse),
            VortexQueryPrimitiveValue::Boolean(false),
        );
        let bridge =
            plan_vortex_query_primitive_result_physical_operators(&result).expect("bridge");
        let metadata_kernel = evaluate_vortex_metadata_physical_kernels(&result, &bridge);

        let admission =
            admit_vortex_metadata_filter_kernel(&metadata_kernel).expect("filter admission");

        assert_eq!(
            admission.status,
            PhysicalKernelAdmissionStatus::BlockedMissingCorrectness
        );
        assert_eq!(
            admission.correctness_evidence,
            BenchmarkEvidenceState::Missing
        );
        assert!(!admission.slot_marked_present);
        assert!(!admission.production_claim_allowed);
        assert!(admission.has_errors());
        assert!(admission.is_side_effect_free());
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
