use std::fmt::Write as _;

use shardloom_core::{
    Diagnostic, DiagnosticCode, NativeIoAdapterFidelityReport, NativeIoCertificate,
    NativeIoRepresentationTransition, NativeIoSideEffectReport, NativeIoSinkRequirementReport,
    NativeIoSourceCapabilityReport, NativeIoSourcePushdownReport, PredicateExpr,
    RepresentationState, Result,
};

use crate::{
    VortexEncodedPredicateEvaluationReport, VortexEncodedPredicateEvaluationStatus,
    VortexEncodedValuePredicateBatch, VortexSelectionVectorFilterKernelAdmissionReport,
    VortexSelectionVectorFilterKernelReport, admit_vortex_selection_vector_filter_kernel,
    evaluate_vortex_encoded_value_predicate_batches,
    evaluate_vortex_selection_vector_filter_kernel,
};

const SCHEMA_VERSION: &str = "shardloom.vortex_generalized_encoded_filter_execution.v1";
const REPORT_ID: &str = "vortex.cg2.generalized-filter.prepared-encoded-values";
const EXECUTION_KIND: &str = "vortex.prepared_encoded_filter";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexGeneralizedEncodedFilterExecutionStatus {
    ExecutedPreparedEncodedValues,
    BlockedMissingEncodedBatches,
    BlockedPredicateEvaluation,
    BlockedUnsafeFilterKernel,
    BlockedNativeIoCertificate,
}

impl VortexGeneralizedEncodedFilterExecutionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExecutedPreparedEncodedValues => "executed_prepared_encoded_values",
            Self::BlockedMissingEncodedBatches => "blocked_missing_encoded_batches",
            Self::BlockedPredicateEvaluation => "blocked_predicate_evaluation",
            Self::BlockedUnsafeFilterKernel => "blocked_unsafe_filter_kernel",
            Self::BlockedNativeIoCertificate => "blocked_native_io_certificate",
        }
    }

    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::ExecutedPreparedEncodedValues)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexGeneralizedEncodedFilterExecutionReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub execution_kind: &'static str,
    pub predicate_summary: String,
    pub status: VortexGeneralizedEncodedFilterExecutionStatus,
    pub encoded_batch_count: usize,
    pub segment_count: usize,
    pub selection_vector_count: usize,
    pub selected_row_count: Option<u64>,
    pub predicate_evaluation: VortexEncodedPredicateEvaluationReport,
    pub filter_kernel: VortexSelectionVectorFilterKernelReport,
    pub filter_kernel_admission: VortexSelectionVectorFilterKernelAdmissionReport,
    pub native_io_certificate: NativeIoCertificate,
    pub runtime_execution_allowed: bool,
    pub prepared_encoded_values_consumed: bool,
    pub selection_vector_guaranteed: bool,
    pub correctness_certified: bool,
    pub production_claim_allowed: bool,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub fallback_attempted: bool,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexGeneralizedEncodedFilterExecutionReport {
    fn from_evidence(
        predicate: &PredicateExpr,
        encoded_batch_count: usize,
        predicate_evaluation: VortexEncodedPredicateEvaluationReport,
        filter_kernel: VortexSelectionVectorFilterKernelReport,
        filter_kernel_admission: VortexSelectionVectorFilterKernelAdmissionReport,
        native_io_certificate: NativeIoCertificate,
    ) -> Self {
        let mut diagnostics = predicate_evaluation.diagnostics.clone();
        diagnostics.extend(
            predicate_evaluation
                .segment_reports
                .iter()
                .flat_map(|report| report.diagnostics.clone()),
        );
        diagnostics.extend(filter_kernel.diagnostics.clone());
        diagnostics.extend(filter_kernel_admission.diagnostics.clone());
        diagnostics.extend(native_io_certificate.diagnostics.clone());

        let predicate_safe = predicate_evaluation.status
            == VortexEncodedPredicateEvaluationStatus::EvaluatedSelections
            && predicate_evaluation.is_side_effect_free()
            && !predicate_evaluation.has_errors();
        let filter_safe = filter_kernel.is_safe_native_filter_kernel_evidence()
            && filter_kernel_admission.slot_marked_present
            && !filter_kernel_admission.has_errors();
        let native_io_safe = native_io_certificate.is_certified();
        let status = if encoded_batch_count == 0 {
            VortexGeneralizedEncodedFilterExecutionStatus::BlockedMissingEncodedBatches
        } else if !predicate_safe {
            VortexGeneralizedEncodedFilterExecutionStatus::BlockedPredicateEvaluation
        } else if !filter_safe {
            VortexGeneralizedEncodedFilterExecutionStatus::BlockedUnsafeFilterKernel
        } else if !native_io_safe {
            VortexGeneralizedEncodedFilterExecutionStatus::BlockedNativeIoCertificate
        } else {
            VortexGeneralizedEncodedFilterExecutionStatus::ExecutedPreparedEncodedValues
        };
        let runtime_execution_allowed =
            status == VortexGeneralizedEncodedFilterExecutionStatus::ExecutedPreparedEncodedValues;

        Self {
            schema_version: SCHEMA_VERSION,
            report_id: REPORT_ID.to_string(),
            execution_kind: EXECUTION_KIND,
            predicate_summary: predicate.summary(),
            status,
            encoded_batch_count,
            segment_count: predicate_evaluation.segment_report_count,
            selection_vector_count: filter_kernel.selection_vector_count,
            selected_row_count: filter_kernel.selected_row_count,
            predicate_evaluation,
            filter_kernel,
            filter_kernel_admission,
            native_io_certificate,
            runtime_execution_allowed,
            prepared_encoded_values_consumed: encoded_batch_count > 0,
            selection_vector_guaranteed: runtime_execution_allowed,
            correctness_certified: false,
            production_claim_allowed: false,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            fallback_attempted: diagnostics
                .iter()
                .any(|diagnostic| diagnostic.fallback.attempted),
            diagnostics,
        }
    }

    #[must_use]
    pub const fn avoids_unsafe_effects(&self) -> bool {
        !self.data_read
            && !self.data_decoded
            && !self.data_materialized
            && !self.row_read
            && !self.arrow_converted
            && !self.object_store_io
            && !self.write_io
            && !self.spill_io_performed
            && !self.external_effects_executed
            && !self.fallback_execution_allowed
            && !self.fallback_attempted
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.production_claim_allowed
            || self.fallback_attempted
            || self.fallback_execution_allowed
            || self.native_io_certificate.has_errors()
            || self.filter_kernel.has_errors()
            || self.filter_kernel_admission.has_errors()
            || self.predicate_evaluation.has_errors()
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
        let mut out = String::new();
        let _ = writeln!(&mut out, "Vortex generalized encoded filter execution");
        let _ = writeln!(&mut out, "schema_version: {}", self.schema_version);
        let _ = writeln!(&mut out, "report: {}", self.report_id);
        let _ = writeln!(&mut out, "execution kind: {}", self.execution_kind);
        let _ = writeln!(&mut out, "predicate: {}", self.predicate_summary);
        let _ = writeln!(&mut out, "status: {}", self.status.as_str());
        let _ = writeln!(&mut out, "encoded batches: {}", self.encoded_batch_count);
        let _ = writeln!(&mut out, "segments: {}", self.segment_count);
        let _ = writeln!(
            &mut out,
            "selection vectors: {}",
            self.selection_vector_count
        );
        let _ = writeln!(
            &mut out,
            "selected rows: {}",
            self.selected_row_count
                .map_or_else(|| "unknown".to_string(), |count| count.to_string())
        );
        let _ = writeln!(
            &mut out,
            "runtime execution allowed: {}",
            self.runtime_execution_allowed
        );
        let _ = writeln!(
            &mut out,
            "selection vector guaranteed: {}",
            self.selection_vector_guaranteed
        );
        let _ = writeln!(&mut out, "correctness certified: false");
        let _ = writeln!(&mut out, "production claim allowed: false");
        let _ = writeln!(&mut out, "fallback execution allowed: false");
        out
    }
}

/// Executes a generalized encoded filter over already-prepared encoded-value
/// batches.
///
/// This is the reusable execution target that future Vortex readers and
/// adapters can feed after they have produced explicit encoded-value batches.
/// It does not open files, call object stores, decode rows, materialize values,
/// convert to Arrow, write output, spill, or invoke fallback execution.
///
/// # Errors
/// Returns an error only when the filter-kernel admission or Native I/O
/// certificate cannot be constructed.
pub fn execute_vortex_generalized_filter_from_encoded_value_batches(
    predicate: &PredicateExpr,
    batches: &[VortexEncodedValuePredicateBatch],
) -> Result<VortexGeneralizedEncodedFilterExecutionReport> {
    let predicate_evaluation = evaluate_vortex_encoded_value_predicate_batches(predicate, batches);
    let filter_kernel = evaluate_vortex_selection_vector_filter_kernel(&predicate_evaluation);
    let filter_kernel_admission = admit_vortex_selection_vector_filter_kernel(&filter_kernel)?;
    let native_io_certificate = prepared_encoded_filter_native_io_certificate(
        predicate,
        batches.len(),
        &predicate_evaluation,
        &filter_kernel,
        &filter_kernel_admission,
    )?;
    Ok(
        VortexGeneralizedEncodedFilterExecutionReport::from_evidence(
            predicate,
            batches.len(),
            predicate_evaluation,
            filter_kernel,
            filter_kernel_admission,
            native_io_certificate,
        ),
    )
}

fn prepared_encoded_filter_native_io_certificate(
    predicate: &PredicateExpr,
    encoded_batch_count: usize,
    predicate_evaluation: &VortexEncodedPredicateEvaluationReport,
    filter_kernel: &VortexSelectionVectorFilterKernelReport,
    filter_kernel_admission: &VortexSelectionVectorFilterKernelAdmissionReport,
) -> Result<NativeIoCertificate> {
    let safe = encoded_batch_count > 0
        && predicate_evaluation.status
            == VortexEncodedPredicateEvaluationStatus::EvaluatedSelections
        && predicate_evaluation.is_side_effect_free()
        && !predicate_evaluation.has_errors()
        && filter_kernel.is_safe_native_filter_kernel_evidence()
        && filter_kernel_admission.slot_marked_present
        && !filter_kernel_admission.has_errors();
    let diagnostics =
        prepared_encoded_filter_native_io_diagnostics(safe, predicate_evaluation, filter_kernel);
    let mut certificate = NativeIoCertificate::new(
        "cg19.prepared_encoded_filter.native_io",
        "prepared_vortex_encoded_batches_to_selection_vector_filter_result",
        prepared_encoded_filter_source_capability_report(safe, encoded_batch_count),
        prepared_encoded_filter_source_pushdown_report(safe, predicate),
        vec![NativeIoRepresentationTransition::new(
            RepresentationState::VortexEncoded,
            if safe {
                RepresentationState::SelectionVectorEncoded
            } else {
                RepresentationState::Unsupported
            },
            false,
        )],
        prepared_encoded_filter_sink_requirement_report(safe, filter_kernel),
        prepared_encoded_filter_adapter_fidelity_report(safe),
        Vec::new(),
        prepared_encoded_filter_side_effect_report(&diagnostics),
        diagnostics,
    )?;
    certificate.fallback_attempted = certificate
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.fallback.attempted);
    Ok(certificate)
}

fn prepared_encoded_filter_native_io_diagnostics(
    safe: bool,
    predicate_evaluation: &VortexEncodedPredicateEvaluationReport,
    filter_kernel: &VortexSelectionVectorFilterKernelReport,
) -> Vec<Diagnostic> {
    let mut diagnostics = predicate_evaluation.diagnostics.clone();
    diagnostics.extend(
        predicate_evaluation
            .segment_reports
            .iter()
            .flat_map(|report| report.diagnostics.clone()),
    );
    diagnostics.extend(filter_kernel.diagnostics.clone());
    if !safe {
        diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            "vortex_prepared_encoded_filter_native_io_certificate",
            "prepared encoded filter Native I/O certificate requires complete encoded-value predicate evaluation and safe selection-vector filter-kernel evidence with no decode, materialization, row reads, Arrow conversion, object-store IO, writes, spill, external effects, or fallback",
            Some("Provide one prepared encoded-value batch per segment before accepting this generalized filter path.".to_string()),
        ));
    }
    diagnostics
}

fn prepared_encoded_filter_source_capability_report(
    safe: bool,
    encoded_batch_count: usize,
) -> NativeIoSourceCapabilityReport {
    NativeIoSourceCapabilityReport {
        source_kind: "vortex_prepared_encoded_batches".to_string(),
        adapter_id: "shardloom.adapter.vortex.prepared_encoded_filter.v1".to_string(),
        schema_discovery_status: if encoded_batch_count > 0 {
            "prepared_encoded_segment_metadata_available".to_string()
        } else {
            "not_available".to_string()
        },
        statistics_availability: if encoded_batch_count > 0 {
            "prepared_row_counts_available".to_string()
        } else {
            "unknown".to_string()
        },
        pushdown_capabilities: if safe {
            "filter".to_string()
        } else {
            "none".to_string()
        },
        encoded_representation_preserved: safe,
        range_read_capability: false,
        streaming_capability: false,
        object_store_capability: false,
        fallback_attempted: false,
    }
}

fn prepared_encoded_filter_source_pushdown_report(
    safe: bool,
    predicate: &PredicateExpr,
) -> NativeIoSourcePushdownReport {
    NativeIoSourcePushdownReport {
        accepted_operations: if safe {
            vec!["filter".to_string()]
        } else {
            Vec::new()
        },
        rejected_operations: if safe {
            Vec::new()
        } else {
            vec!["filter".to_string()]
        },
        guarantee: if safe {
            "exact_selection_vector_from_prepared_encoded_values".to_string()
        } else {
            "unsupported".to_string()
        },
        proof_basis: if safe {
            "native encoded predicate kernel evaluated explicit Vortex encoded-value batches and emitted one selection vector per segment".to_string()
        } else {
            "Native I/O certificate blocked before accepting prepared encoded filter evidence"
                .to_string()
        },
        residual_expression: (!safe).then(|| predicate.summary()),
        conservative_false_positive_policy: false,
        unsafe_rejected_reason: (!safe)
            .then(|| "missing complete safe prepared encoded filter evidence".to_string()),
        fallback_attempted: false,
    }
}

fn prepared_encoded_filter_sink_requirement_report(
    safe: bool,
    filter_kernel: &VortexSelectionVectorFilterKernelReport,
) -> NativeIoSinkRequirementReport {
    NativeIoSinkRequirementReport {
        target_format: "selection_vector_filter_result".to_string(),
        accepts_encoded: safe,
        requires_decoded_columnar: false,
        requires_rows: false,
        preserves_metadata: safe,
        requires_ordering: false,
        requires_partitioning: false,
        requires_commit: false,
        supports_streaming: false,
        max_chunk_size: filter_kernel.selected_row_count,
        backpressure_policy: "not_applicable_prepared_encoded_batches".to_string(),
    }
}

fn prepared_encoded_filter_adapter_fidelity_report(safe: bool) -> NativeIoAdapterFidelityReport {
    NativeIoAdapterFidelityReport {
        adapter_id: "shardloom.adapter.vortex.prepared_encoded_filter.v1".to_string(),
        source_kind: "vortex_prepared_encoded_batches".to_string(),
        sink_kind: "selection_vector_filter_result".to_string(),
        metadata_preserved: safe,
        statistics_preserved: safe,
        encoded_representation_preserved: safe,
        materialization_required: false,
        fidelity_loss: if safe {
            "none_for_selection_vector_result".to_string()
        } else {
            "unsupported".to_string()
        },
        metadata_loss: "none".to_string(),
        fallback_attempted: false,
    }
}

fn prepared_encoded_filter_side_effect_report(
    diagnostics: &[Diagnostic],
) -> NativeIoSideEffectReport {
    NativeIoSideEffectReport {
        data_read: false,
        data_decoded: false,
        data_materialized: false,
        row_read: false,
        arrow_converted: false,
        object_store_io: false,
        write_io: false,
        spill_io_performed: false,
        external_effects_executed: false,
        fallback_attempted: diagnostics
            .iter()
            .any(|diagnostic| diagnostic.fallback.attempted),
        fallback_execution_allowed: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{
        ColumnRef, ComparisonOp, EncodedSegment, EncodedValueBatch, EncodedValueRun, EncodingKind,
        LayoutKind, LogicalDType, Nullability, SegmentId, SegmentLayout, SegmentStats, StatValue,
    };

    fn column_ref(name: &str) -> ColumnRef {
        ColumnRef::new(name).expect("column")
    }

    fn segment(id: &str, row_count: u64, encoding: EncodingKind) -> EncodedSegment {
        EncodedSegment::new(
            SegmentId::new(id).expect("segment"),
            column_ref("metric"),
            LogicalDType::Int64,
            Nullability::Nullable,
            SegmentLayout::new(encoding, LayoutKind::Flat),
            SegmentStats::with_row_count(row_count),
        )
    }

    #[test]
    fn prepared_encoded_filter_executes_dictionary_and_run_batches() {
        let predicate = PredicateExpr::Compare {
            column: column_ref("metric"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(5),
        };
        let batches = vec![
            VortexEncodedValuePredicateBatch::new(
                segment("segment-1.metric", 5, EncodingKind::Dictionary),
                EncodedValueBatch::Dictionary {
                    dictionary: vec![Some(StatValue::Int64(1)), Some(StatValue::Int64(5)), None],
                    codes: vec![Some(0), Some(1), None, Some(1), Some(0)],
                },
            ),
            VortexEncodedValuePredicateBatch::new(
                segment("segment-2.metric", 3, EncodingKind::RunLength),
                EncodedValueBatch::RunLength {
                    runs: vec![EncodedValueRun::new(Some(StatValue::Int64(9)), 3)],
                },
            ),
        ];

        let report =
            execute_vortex_generalized_filter_from_encoded_value_batches(&predicate, &batches)
                .expect("report");

        assert_eq!(
            report.status,
            VortexGeneralizedEncodedFilterExecutionStatus::ExecutedPreparedEncodedValues
        );
        assert_eq!(report.encoded_batch_count, 2);
        assert_eq!(report.segment_count, 2);
        assert_eq!(report.selection_vector_count, 2);
        assert_eq!(report.selected_row_count, Some(5));
        assert!(report.runtime_execution_allowed);
        assert!(report.prepared_encoded_values_consumed);
        assert!(report.selection_vector_guaranteed);
        assert!(!report.correctness_certified);
        assert!(!report.production_claim_allowed);
        assert!(report.avoids_unsafe_effects());
        assert!(report.native_io_certificate.is_certified());
        assert_eq!(
            report
                .native_io_certificate
                .representation_transition_order(),
            "vortex_encoded->selection_vector_encoded"
        );
        assert_eq!(
            report
                .native_io_certificate
                .source_pushdown_report
                .accepted_operation_order(),
            "filter"
        );
        assert!(!report.has_errors());
    }

    #[test]
    fn prepared_encoded_filter_blocks_empty_batches_without_fallback() {
        let report = execute_vortex_generalized_filter_from_encoded_value_batches(
            &PredicateExpr::AlwaysTrue,
            &[],
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexGeneralizedEncodedFilterExecutionStatus::BlockedMissingEncodedBatches
        );
        assert_eq!(report.encoded_batch_count, 0);
        assert!(!report.runtime_execution_allowed);
        assert!(!report.selection_vector_guaranteed);
        assert!(report.avoids_unsafe_effects());
        assert!(report.native_io_certificate.has_errors());
        assert!(report.has_errors());
        assert!(
            report
                .diagnostics
                .iter()
                .all(|diagnostic| !diagnostic.fallback.attempted)
        );
    }

    #[test]
    fn prepared_encoded_filter_blocks_encoding_mismatch_without_decode_or_fallback() {
        let predicate = PredicateExpr::Compare {
            column: column_ref("metric"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(5),
        };
        let batches = vec![VortexEncodedValuePredicateBatch::new(
            segment("segment-1.metric", 3, EncodingKind::Dictionary),
            EncodedValueBatch::Constant {
                value: Some(StatValue::Int64(5)),
                row_count: 3,
            },
        )];

        let report =
            execute_vortex_generalized_filter_from_encoded_value_batches(&predicate, &batches)
                .expect("report");

        assert_eq!(
            report.status,
            VortexGeneralizedEncodedFilterExecutionStatus::BlockedPredicateEvaluation
        );
        assert!(!report.runtime_execution_allowed);
        assert!(!report.selection_vector_guaranteed);
        assert!(report.avoids_unsafe_effects());
        assert!(report.native_io_certificate.has_errors());
        assert!(report.has_errors());
        assert!(
            report
                .diagnostics
                .iter()
                .all(|diagnostic| !diagnostic.fallback.attempted)
        );
    }
}
