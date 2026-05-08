use std::fmt::Write as _;

use shardloom_core::{
    BenchmarkEvidenceState, BenchmarkFallbackState, Diagnostic, DiagnosticCode, KernelKind,
    OperatorMemoryCertification, PhysicalKernelAdmissionReport, PhysicalKernelAdmissionStatus,
    PhysicalKernelRequirement, PhysicalKernelSlot, PhysicalOperatorContract,
    PhysicalOperatorExecutionLevel, PhysicalOperatorKind,
};

use crate::{VortexEncodedPredicateEvaluationReport, VortexEncodedPredicateEvaluationStatus};

const SCHEMA_VERSION: &str = "shardloom.vortex_selection_vector_filter_kernel.v1";
const ADMISSION_SCHEMA_VERSION: &str =
    "shardloom.vortex_selection_vector_filter_kernel_admission.v1";
const KERNEL_REPORT_ID: &str =
    "vortex.query-primitive.filter_predicate.selection-vector-filter-kernel";
const FILTER_OPERATOR_ID: &str = "vortex.query_primitive.filter_predicate.selection_vector_filter";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexSelectionVectorFilterKernelStatus {
    EvaluatedSelectionVectors,
    NeedsEncodedValues,
    BlockedByPredicateEvaluation,
    BlockedMissingSelectionVectors,
    Unsupported,
}

impl VortexSelectionVectorFilterKernelStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::EvaluatedSelectionVectors => "evaluated_selection_vectors",
            Self::NeedsEncodedValues => "needs_encoded_values",
            Self::BlockedByPredicateEvaluation => "blocked_by_predicate_evaluation",
            Self::BlockedMissingSelectionVectors => "blocked_missing_selection_vectors",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        !matches!(
            self,
            Self::EvaluatedSelectionVectors | Self::NeedsEncodedValues
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexSelectionVectorFilterKernelDiscoveryReport {
    pub schema_version: &'static str,
    pub kernel_report_id: &'static str,
    pub operator_kind: PhysicalOperatorKind,
    pub kernel_kind: KernelKind,
    pub execution_level: PhysicalOperatorExecutionLevel,
    pub contextual_only: bool,
    pub requires_encoded_predicate_evaluation: bool,
    pub requires_selection_vectors: bool,
    pub requires_correctness_evidence: bool,
    pub requires_memory_safety_evidence: bool,
    pub requires_benchmark_for_production: bool,
    pub discovery_reads_data: bool,
    pub runtime_execution_allowed_by_discovery: bool,
    pub fallback_execution_allowed: bool,
}

impl VortexSelectionVectorFilterKernelDiscoveryReport {
    #[must_use]
    pub const fn report_only() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            kernel_report_id: KERNEL_REPORT_ID,
            operator_kind: PhysicalOperatorKind::Filter,
            kernel_kind: KernelKind::Encoded,
            execution_level: PhysicalOperatorExecutionLevel::EncodedNative,
            contextual_only: true,
            requires_encoded_predicate_evaluation: true,
            requires_selection_vectors: true,
            requires_correctness_evidence: true,
            requires_memory_safety_evidence: true,
            requires_benchmark_for_production: true,
            discovery_reads_data: false,
            runtime_execution_allowed_by_discovery: false,
            fallback_execution_allowed: false,
        }
    }

    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.discovery_reads_data
            && !self.runtime_execution_allowed_by_discovery
            && !self.fallback_execution_allowed
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexSelectionVectorFilterKernelReport {
    pub schema_version: &'static str,
    pub kernel_report_id: String,
    pub predicate_evaluation_report_id: String,
    pub operator_kind: PhysicalOperatorKind,
    pub kernel_kind: KernelKind,
    pub execution_level: PhysicalOperatorExecutionLevel,
    pub status: VortexSelectionVectorFilterKernelStatus,
    pub segment_count: usize,
    pub selection_vector_count: usize,
    pub selected_row_count: Option<u64>,
    pub selected_all_count: usize,
    pub selected_none_count: usize,
    pub needs_encoded_values_count: usize,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub production_claim_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexSelectionVectorFilterKernelReport {
    #[must_use]
    fn evaluated(predicate_evaluation: &VortexEncodedPredicateEvaluationReport) -> Self {
        Self::new(
            predicate_evaluation,
            VortexSelectionVectorFilterKernelStatus::EvaluatedSelectionVectors,
            predicate_evaluation.selected_rows_metadata_count,
            Vec::new(),
        )
    }

    #[must_use]
    fn blocked(
        predicate_evaluation: &VortexEncodedPredicateEvaluationReport,
        status: VortexSelectionVectorFilterKernelStatus,
        diagnostic: Diagnostic,
    ) -> Self {
        let mut diagnostics = predicate_evaluation.diagnostics.clone();
        diagnostics.extend(
            predicate_evaluation
                .segment_reports
                .iter()
                .flat_map(|report| report.diagnostics.clone()),
        );
        diagnostics.push(diagnostic);
        Self::new(predicate_evaluation, status, None, diagnostics)
    }

    #[must_use]
    fn new(
        predicate_evaluation: &VortexEncodedPredicateEvaluationReport,
        status: VortexSelectionVectorFilterKernelStatus,
        selected_row_count: Option<u64>,
        diagnostics: Vec<Diagnostic>,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            kernel_report_id: KERNEL_REPORT_ID.to_string(),
            predicate_evaluation_report_id: predicate_evaluation.report_id.clone(),
            operator_kind: PhysicalOperatorKind::Filter,
            kernel_kind: KernelKind::Encoded,
            execution_level: PhysicalOperatorExecutionLevel::EncodedNative,
            status,
            segment_count: predicate_evaluation.segment_report_count,
            selection_vector_count: predicate_evaluation.selection_vectors_emitted,
            selected_row_count,
            selected_all_count: predicate_evaluation.selected_all_count,
            selected_none_count: predicate_evaluation.selected_none_count,
            needs_encoded_values_count: predicate_evaluation.needs_encoded_values_count,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
            production_claim_allowed: false,
            diagnostics,
        }
    }

    #[must_use]
    pub const fn is_side_effect_free(&self) -> bool {
        !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_effects_executed
            && !self.fallback_attempted
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
    pub fn is_safe_native_filter_kernel_evidence(&self) -> bool {
        matches!(
            self.status,
            VortexSelectionVectorFilterKernelStatus::EvaluatedSelectionVectors
        ) && self.segment_count > 0
            && self.selection_vector_count == self.segment_count
            && self.selected_row_count.is_some()
            && self.kernel_kind == KernelKind::Encoded
            && self.operator_kind == PhysicalOperatorKind::Filter
            && self.execution_level == PhysicalOperatorExecutionLevel::EncodedNative
            && self.is_side_effect_free()
            && !self.production_claim_allowed
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut text = String::new();
        let _ = writeln!(text, "schema_version: {}", self.schema_version);
        let _ = writeln!(text, "kernel report: {}", self.kernel_report_id);
        let _ = writeln!(text, "operator: {}", self.operator_kind.as_str());
        let _ = writeln!(text, "kernel kind: {}", self.kernel_kind.as_str());
        let _ = writeln!(text, "execution level: {}", self.execution_level.as_str());
        let _ = writeln!(text, "status: {}", self.status.as_str());
        let _ = writeln!(text, "segments: {}", self.segment_count);
        let _ = writeln!(text, "selection vectors: {}", self.selection_vector_count);
        let _ = writeln!(
            text,
            "selected rows: {}",
            self.selected_row_count
                .map_or_else(|| "unknown".to_string(), |count| count.to_string())
        );
        let _ = writeln!(text, "data read: false");
        let _ = writeln!(text, "data decoded: false");
        let _ = writeln!(text, "data materialized: false");
        let _ = writeln!(text, "fallback attempted: false");
        let _ = writeln!(text, "fallback execution: disabled");
        text
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexSelectionVectorFilterKernelAdmissionReport {
    pub schema_version: &'static str,
    pub admission_id: String,
    pub filter_kernel_report_id: String,
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

impl VortexSelectionVectorFilterKernelAdmissionReport {
    #[must_use]
    pub fn from_admission(
        filter_kernel: &VortexSelectionVectorFilterKernelReport,
        admission: PhysicalKernelAdmissionReport,
    ) -> Self {
        let mut diagnostics = filter_kernel.diagnostics.clone();
        diagnostics.extend(admission.diagnostics.clone());
        let slot_marked_present = admission.can_mark_kernel_present();
        let production_claim_allowed = admission.can_satisfy_production_claim();
        Self {
            schema_version: ADMISSION_SCHEMA_VERSION,
            admission_id: format!("{}.admission", filter_kernel.kernel_report_id),
            filter_kernel_report_id: filter_kernel.kernel_report_id.clone(),
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
            "selection-vector filter kernel admission\nschema_version: {}\nadmission: {}\nslot: {}\noperator: {}\nrequired kernel: {}\ncandidate kernel: {}\nstatus: {}\nslot marked present: {}\nproduction claim allowed: {}\nruntime execution: disabled\nfallback execution: disabled",
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

#[must_use]
pub fn evaluate_vortex_selection_vector_filter_kernel(
    predicate_evaluation: &VortexEncodedPredicateEvaluationReport,
) -> VortexSelectionVectorFilterKernelReport {
    if predicate_evaluation.has_errors() || !predicate_evaluation.is_side_effect_free() {
        return VortexSelectionVectorFilterKernelReport::blocked(
            predicate_evaluation,
            VortexSelectionVectorFilterKernelStatus::BlockedByPredicateEvaluation,
            Diagnostic::not_implemented(
                "vortex_selection_vector_filter_kernel",
                "selection-vector filter kernel requires a successful side-effect-free predicate evaluation report",
                "Provide a Vortex encoded predicate evaluation report without errors, IO, decode, materialization, or fallback.",
            ),
        );
    }
    if predicate_evaluation.status == VortexEncodedPredicateEvaluationStatus::NeedsEncodedValues {
        return VortexSelectionVectorFilterKernelReport::blocked(
            predicate_evaluation,
            VortexSelectionVectorFilterKernelStatus::NeedsEncodedValues,
            Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "vortex_selection_vector_filter_kernel",
                "encoded values are required before this filter kernel can emit a selection vector",
                Some(
                    "Implement an encoded-value predicate kernel before admitting this filter path."
                        .to_string(),
                ),
            ),
        );
    }
    if predicate_evaluation.status != VortexEncodedPredicateEvaluationStatus::EvaluatedSelections {
        return VortexSelectionVectorFilterKernelReport::blocked(
            predicate_evaluation,
            VortexSelectionVectorFilterKernelStatus::BlockedByPredicateEvaluation,
            Diagnostic::not_implemented(
                "vortex_selection_vector_filter_kernel",
                "predicate evaluation did not produce selection vectors",
                "Use a metadata-proven predicate evaluation report or wait for encoded-value predicates.",
            ),
        );
    }
    if predicate_evaluation.segment_report_count == 0
        || predicate_evaluation.selection_vectors_emitted
            != predicate_evaluation.segment_report_count
        || predicate_evaluation.selected_rows_metadata_count.is_none()
    {
        return VortexSelectionVectorFilterKernelReport::blocked(
            predicate_evaluation,
            VortexSelectionVectorFilterKernelStatus::BlockedMissingSelectionVectors,
            Diagnostic::not_implemented(
                "vortex_selection_vector_filter_kernel",
                "selection-vector filter kernel requires one selection vector per segment and a known selected row count",
                "Provide complete segment predicate evaluation reports before admitting the filter kernel.",
            ),
        );
    }

    VortexSelectionVectorFilterKernelReport::evaluated(predicate_evaluation)
}

/// Admits safe selection-vector filter-kernel evidence into the encoded filter
/// kernel slot.
///
/// # Errors
/// Returns an error only if the static encoded filter operator contract cannot
/// be built.
pub fn admit_vortex_selection_vector_filter_kernel(
    filter_kernel: &VortexSelectionVectorFilterKernelReport,
) -> shardloom_core::Result<VortexSelectionVectorFilterKernelAdmissionReport> {
    let slot = selection_vector_filter_kernel_slot()?;
    let safe_evidence = filter_kernel.is_safe_native_filter_kernel_evidence();
    let admission = PhysicalKernelAdmissionReport::evaluate(
        &slot,
        KernelKind::Encoded,
        if safe_evidence {
            BenchmarkEvidenceState::Present
        } else {
            BenchmarkEvidenceState::Missing
        },
        BenchmarkEvidenceState::Missing,
        if safe_evidence {
            safe_selection_vector_filter_memory()
        } else {
            OperatorMemoryCertification::unsupported()
        },
        if filter_kernel.fallback_attempted {
            BenchmarkFallbackState::Attempted
        } else {
            BenchmarkFallbackState::NotAttempted
        },
    );
    Ok(VortexSelectionVectorFilterKernelAdmissionReport::from_admission(filter_kernel, admission))
}

#[must_use]
pub const fn vortex_selection_vector_filter_kernel_discovery_report()
-> VortexSelectionVectorFilterKernelDiscoveryReport {
    VortexSelectionVectorFilterKernelDiscoveryReport::report_only()
}

fn selection_vector_filter_kernel_slot() -> shardloom_core::Result<PhysicalKernelSlot> {
    let operator = PhysicalOperatorContract::new(
        FILTER_OPERATOR_ID,
        PhysicalOperatorKind::Filter,
        PhysicalOperatorExecutionLevel::EncodedNative,
        vec![
            PhysicalKernelRequirement::missing(KernelKind::Metadata),
            PhysicalKernelRequirement::missing(KernelKind::Encoded),
        ],
    )?;
    Ok(PhysicalKernelSlot::from_requirement(
        &operator,
        PhysicalKernelRequirement::missing(KernelKind::Encoded),
    ))
}

const fn safe_selection_vector_filter_memory() -> OperatorMemoryCertification {
    OperatorMemoryCertification {
        streaming: true,
        bounded_memory: true,
        spillable: false,
        requires_full_materialization: false,
        requires_shuffle: false,
        oom_safe: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{
        ColumnRef, ComparisonOp, LogicalDType, SegmentStats, SelectionVector, StatValue,
    };

    use crate::{
        VortexColumnMetadataSummary, VortexFileMetadataSummary, VortexMetadataSummaryReport,
        VortexMetadataSummaryStatus, VortexSegmentMetadataSummary,
        evaluate_vortex_encoded_predicate_segments,
    };

    fn metadata_summary(stats: SegmentStats) -> VortexMetadataSummaryReport {
        let mut segment = VortexSegmentMetadataSummary::unknown().with_row_count(5);
        segment.add_column(
            VortexColumnMetadataSummary::new(ColumnRef::new("x").expect("column"))
                .with_dtype(LogicalDType::Int64)
                .with_stats(stats)
                .with_statistics_available(true),
        );
        let mut summary = VortexFileMetadataSummary::empty();
        summary.add_segment(segment);
        VortexMetadataSummaryReport {
            status: VortexMetadataSummaryStatus::Summarized,
            summary,
            diagnostics: Vec::new(),
        }
    }

    #[test]
    fn discovery_is_report_only() {
        let report = vortex_selection_vector_filter_kernel_discovery_report();

        assert_eq!(report.schema_version, SCHEMA_VERSION);
        assert_eq!(report.operator_kind, PhysicalOperatorKind::Filter);
        assert_eq!(report.kernel_kind, KernelKind::Encoded);
        assert!(report.requires_encoded_predicate_evaluation);
        assert!(report.requires_selection_vectors);
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn metadata_proven_selection_vectors_evaluate_filter_kernel() {
        let mut stats = SegmentStats::with_row_count(5);
        stats.null_count = Some(0);
        let predicate_evaluation = evaluate_vortex_encoded_predicate_segments(
            &shardloom_core::PredicateExpr::IsNotNull {
                column: ColumnRef::new("x").expect("column"),
            },
            &metadata_summary(stats),
        );
        assert_eq!(
            predicate_evaluation.segment_reports[0].selection_vector,
            Some(SelectionVector::all(5))
        );

        let filter_kernel = evaluate_vortex_selection_vector_filter_kernel(&predicate_evaluation);

        assert_eq!(
            filter_kernel.status,
            VortexSelectionVectorFilterKernelStatus::EvaluatedSelectionVectors
        );
        assert_eq!(filter_kernel.selection_vector_count, 1);
        assert_eq!(filter_kernel.selected_row_count, Some(5));
        assert!(filter_kernel.is_safe_native_filter_kernel_evidence());
        assert!(filter_kernel.is_side_effect_free());
        assert!(!filter_kernel.has_errors());
    }

    #[test]
    fn safe_filter_kernel_evidence_admits_encoded_slot_without_production_claim() {
        let mut stats = SegmentStats::with_row_count(5);
        stats.null_count = Some(0);
        let predicate_evaluation = evaluate_vortex_encoded_predicate_segments(
            &shardloom_core::PredicateExpr::IsNotNull {
                column: ColumnRef::new("x").expect("column"),
            },
            &metadata_summary(stats),
        );
        let filter_kernel = evaluate_vortex_selection_vector_filter_kernel(&predicate_evaluation);
        let admission =
            admit_vortex_selection_vector_filter_kernel(&filter_kernel).expect("admission");

        assert_eq!(
            admission.status,
            PhysicalKernelAdmissionStatus::RegistryReady
        );
        assert!(admission.slot_marked_present);
        assert!(!admission.production_claim_allowed);
        assert_eq!(admission.required_kernel_kind, KernelKind::Encoded);
        assert_eq!(admission.candidate_kernel_kind, KernelKind::Encoded);
        assert!(admission.is_side_effect_free());
        assert!(!admission.has_errors());
    }

    #[test]
    fn inconclusive_predicate_blocks_filter_kernel_until_encoded_values_exist() {
        let mut stats = SegmentStats::with_row_count(5);
        stats.min_value = Some(StatValue::Int64(1));
        stats.max_value = Some(StatValue::Int64(10));
        let predicate_evaluation = evaluate_vortex_encoded_predicate_segments(
            &shardloom_core::PredicateExpr::Compare {
                column: ColumnRef::new("x").expect("column"),
                op: ComparisonOp::Eq,
                value: StatValue::Int64(7),
            },
            &metadata_summary(stats),
        );

        let filter_kernel = evaluate_vortex_selection_vector_filter_kernel(&predicate_evaluation);

        assert_eq!(
            filter_kernel.status,
            VortexSelectionVectorFilterKernelStatus::NeedsEncodedValues
        );
        assert_eq!(filter_kernel.selection_vector_count, 0);
        assert!(!filter_kernel.is_safe_native_filter_kernel_evidence());
        assert!(filter_kernel.is_side_effect_free());
        assert!(filter_kernel.has_errors());
        assert!(
            filter_kernel
                .diagnostics
                .iter()
                .all(|d| !d.fallback.attempted)
        );
    }

    #[test]
    fn unsafe_filter_kernel_evidence_blocks_admission() {
        let mut stats = SegmentStats::with_row_count(5);
        stats.min_value = Some(StatValue::Int64(1));
        stats.max_value = Some(StatValue::Int64(10));
        let predicate_evaluation = evaluate_vortex_encoded_predicate_segments(
            &shardloom_core::PredicateExpr::Compare {
                column: ColumnRef::new("x").expect("column"),
                op: ComparisonOp::Eq,
                value: StatValue::Int64(7),
            },
            &metadata_summary(stats),
        );
        let filter_kernel = evaluate_vortex_selection_vector_filter_kernel(&predicate_evaluation);
        let admission =
            admit_vortex_selection_vector_filter_kernel(&filter_kernel).expect("admission");

        assert_eq!(
            admission.status,
            PhysicalKernelAdmissionStatus::BlockedMissingCorrectness
        );
        assert!(!admission.slot_marked_present);
        assert!(!admission.production_claim_allowed);
        assert!(admission.has_errors());
        assert!(admission.is_side_effect_free());
    }
}
