use std::fmt::Write as _;

use shardloom_core::{
    ColumnRef, Diagnostic, EncodedPredicateEvaluationReport, EncodedPredicateEvaluationStatus,
    EncodedSegment, EncodedValueBatch, KernelKind, PhysicalOperatorExecutionLevel,
    PhysicalOperatorKind, PredicateExpr, SegmentId, SegmentLayout, SegmentStats,
    evaluate_predicate_on_encoded_segment, evaluate_predicate_on_encoded_values,
};

use crate::{VortexMetadataSummaryReport, VortexSegmentMetadataSummary};

const SCHEMA_VERSION: &str = "shardloom.vortex_encoded_predicate_evaluation.v1";
const REPORT_ID: &str = "vortex.query-primitive.filter_predicate.encoded-predicate-evaluation";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedPredicateEvaluationStatus {
    EvaluatedSelections,
    NeedsEncodedValues,
    MissingMetadata,
    Unsupported,
}

impl VortexEncodedPredicateEvaluationStatus {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::EvaluatedSelections => "evaluated_selections",
            Self::NeedsEncodedValues => "needs_encoded_values",
            Self::MissingMetadata => "missing_metadata",
            Self::Unsupported => "unsupported",
        }
    }

    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Unsupported)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexEncodedPredicateEvaluationDiscoveryReport {
    pub schema_version: &'static str,
    pub report_id: &'static str,
    pub operator_kind: PhysicalOperatorKind,
    pub kernel_kind: KernelKind,
    pub execution_level: PhysicalOperatorExecutionLevel,
    pub contextual_only: bool,
    pub emits_selection_vectors: bool,
    pub supports_metadata_proven_all: bool,
    pub supports_metadata_proven_none: bool,
    pub defers_inconclusive_predicates_to_encoded_values: bool,
    pub discovery_reads_data: bool,
    pub runtime_execution_allowed_by_discovery: bool,
    pub fallback_execution_allowed: bool,
}

impl VortexEncodedPredicateEvaluationDiscoveryReport {
    #[must_use]
    pub const fn report_only() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            report_id: REPORT_ID,
            operator_kind: PhysicalOperatorKind::Filter,
            kernel_kind: KernelKind::Encoded,
            execution_level: PhysicalOperatorExecutionLevel::EncodedNative,
            contextual_only: true,
            emits_selection_vectors: true,
            supports_metadata_proven_all: true,
            supports_metadata_proven_none: true,
            defers_inconclusive_predicates_to_encoded_values: true,
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
pub struct VortexEncodedPredicateEvaluationReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub predicate_summary: String,
    pub status: VortexEncodedPredicateEvaluationStatus,
    pub segment_report_count: usize,
    pub selected_all_count: usize,
    pub selected_none_count: usize,
    pub selected_indices_count: usize,
    pub needs_encoded_values_count: usize,
    pub missing_metadata_count: usize,
    pub unsupported_count: usize,
    pub selection_vectors_emitted: usize,
    pub selected_rows_metadata_count: Option<u64>,
    pub segment_reports: Vec<EncodedPredicateEvaluationReport>,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub fallback_attempted: bool,
    pub fallback_execution_allowed: bool,
    pub diagnostics: Vec<Diagnostic>,
}

/// One explicitly supplied encoded-value predicate input.
///
/// This is a prepared execution-kernel input, not a reader contract. It pairs
/// segment metadata with already available encoded values so Vortex-native
/// predicate evidence can be aggregated across segments without opening files,
/// decoding rows, materializing values, converting to Arrow, writing output,
/// spilling, or invoking fallback execution.
#[derive(Debug, Clone, PartialEq)]
pub struct VortexEncodedValuePredicateBatch {
    pub segment: EncodedSegment,
    pub values: EncodedValueBatch,
}

impl VortexEncodedValuePredicateBatch {
    #[must_use]
    pub const fn new(segment: EncodedSegment, values: EncodedValueBatch) -> Self {
        Self { segment, values }
    }
}

impl VortexEncodedPredicateEvaluationReport {
    #[must_use]
    fn from_segment_reports(
        predicate: &PredicateExpr,
        segment_reports: Vec<EncodedPredicateEvaluationReport>,
        diagnostics: Vec<Diagnostic>,
    ) -> Self {
        let selected_all_count = count_status(
            &segment_reports,
            EncodedPredicateEvaluationStatus::SelectedAll,
        );
        let selected_none_count = count_status(
            &segment_reports,
            EncodedPredicateEvaluationStatus::SelectedNone,
        );
        let selected_indices_count = count_status(
            &segment_reports,
            EncodedPredicateEvaluationStatus::SelectedIndices,
        );
        let needs_encoded_values_count = count_status(
            &segment_reports,
            EncodedPredicateEvaluationStatus::NeedsEncodedValues,
        );
        let missing_metadata_count = count_status(
            &segment_reports,
            EncodedPredicateEvaluationStatus::MissingSegmentMetadata,
        );
        let unsupported_count = count_status(
            &segment_reports,
            EncodedPredicateEvaluationStatus::Unsupported,
        );
        let selection_vectors_emitted = segment_reports
            .iter()
            .filter(|report| report.selection_vector.is_some())
            .count();
        let selected_rows_metadata_count = segment_reports
            .iter()
            .filter_map(|report| report.selected_count)
            .try_fold(0_u64, u64::checked_add);
        let status = if unsupported_count > 0 {
            VortexEncodedPredicateEvaluationStatus::Unsupported
        } else if missing_metadata_count > 0
            || !diagnostics.is_empty()
            || segment_reports.is_empty()
        {
            VortexEncodedPredicateEvaluationStatus::MissingMetadata
        } else if needs_encoded_values_count > 0 {
            VortexEncodedPredicateEvaluationStatus::NeedsEncodedValues
        } else {
            VortexEncodedPredicateEvaluationStatus::EvaluatedSelections
        };

        Self {
            schema_version: SCHEMA_VERSION,
            report_id: REPORT_ID.to_string(),
            predicate_summary: predicate.summary(),
            status,
            segment_report_count: segment_reports.len(),
            selected_all_count,
            selected_none_count,
            selected_indices_count,
            needs_encoded_values_count,
            missing_metadata_count,
            unsupported_count,
            selection_vectors_emitted,
            selected_rows_metadata_count,
            segment_reports,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
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
            || self
                .segment_reports
                .iter()
                .any(EncodedPredicateEvaluationReport::has_errors)
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut text = String::new();
        let _ = writeln!(text, "schema_version: {}", self.schema_version);
        let _ = writeln!(text, "predicate evaluation: {}", self.report_id);
        let _ = writeln!(text, "predicate: {}", self.predicate_summary);
        let _ = writeln!(text, "status: {}", self.status.as_str());
        let _ = writeln!(text, "segment reports: {}", self.segment_report_count);
        let _ = writeln!(
            text,
            "selection vectors: {}",
            self.selection_vectors_emitted
        );
        let _ = writeln!(
            text,
            "sparse selection vectors: {}",
            self.selected_indices_count
        );
        let _ = writeln!(
            text,
            "needs encoded values: {}",
            self.needs_encoded_values_count
        );
        let _ = writeln!(text, "data read: false");
        let _ = writeln!(text, "data decoded: false");
        let _ = writeln!(text, "data materialized: false");
        let _ = writeln!(text, "fallback attempted: false");
        let _ = writeln!(text, "fallback execution: disabled");
        text
    }
}

/// Plans segment-local encoded predicate evaluation from a normalized Vortex
/// metadata summary. This is a report-only bridge: it emits metadata-proven
/// selection vectors where possible and marks inconclusive segments as needing
/// encoded values, but it does not read data or execute a filter kernel.
#[must_use]
pub fn evaluate_vortex_encoded_predicate_segments(
    predicate: &PredicateExpr,
    summary: &VortexMetadataSummaryReport,
) -> VortexEncodedPredicateEvaluationReport {
    if summary.summary.segments.is_empty() {
        return VortexEncodedPredicateEvaluationReport::from_segment_reports(
            predicate,
            Vec::new(),
            vec![Diagnostic::not_implemented(
                "vortex_encoded_predicate_evaluation",
                "segment metadata is required before encoded predicate evaluation can be planned",
                "Provide a metadata summary with segment and column statistics.",
            )],
        );
    }

    let mut diagnostics = Vec::new();
    let mut segment_reports = Vec::new();
    for (index, segment) in summary.summary.segments.iter().enumerate() {
        match encoded_segment_for_predicate(predicate, segment, index) {
            Ok(encoded_segment) => {
                segment_reports.push(evaluate_predicate_on_encoded_segment(
                    predicate,
                    &encoded_segment,
                ));
            }
            Err(reason) => diagnostics.push(Diagnostic::not_implemented(
                "vortex_encoded_predicate_evaluation",
                reason,
                "Provide column metadata before planning encoded predicate evaluation for this segment.",
            )),
        }
    }

    VortexEncodedPredicateEvaluationReport::from_segment_reports(
        predicate,
        segment_reports,
        diagnostics,
    )
}

/// Evaluates one already-prepared Vortex encoded-value batch through the native
/// encoded predicate kernel.
///
/// This bridge is intentionally narrower than a reader: callers must provide
/// the encoded segment metadata and encoded-value batch explicitly. It does not
/// open files, call object stores, decode rows, materialize values, convert to
/// Arrow, write output, spill, or permit fallback execution.
#[must_use]
pub fn evaluate_vortex_encoded_value_predicate_batch(
    predicate: &PredicateExpr,
    segment: &EncodedSegment,
    values: &EncodedValueBatch,
) -> VortexEncodedPredicateEvaluationReport {
    evaluate_vortex_encoded_value_predicate_batches(
        predicate,
        &[VortexEncodedValuePredicateBatch::new(
            segment.clone(),
            values.clone(),
        )],
    )
}

/// Evaluates prepared Vortex encoded-value batches through the native encoded
/// predicate kernel and aggregates their selection-vector evidence.
///
/// This is the reusable generalized filter evidence target for future reader
/// wiring. It requires callers to provide segment metadata plus encoded values;
/// it does not open files, call object stores, decode rows, materialize values,
/// convert to Arrow, write output, spill, or permit fallback execution.
#[must_use]
pub fn evaluate_vortex_encoded_value_predicate_batches(
    predicate: &PredicateExpr,
    batches: &[VortexEncodedValuePredicateBatch],
) -> VortexEncodedPredicateEvaluationReport {
    if batches.is_empty() {
        return VortexEncodedPredicateEvaluationReport::from_segment_reports(
            predicate,
            Vec::new(),
            vec![Diagnostic::not_implemented(
                "vortex_encoded_value_predicate_batches",
                "encoded segment metadata and encoded-value batches are required before multi-segment encoded filter evaluation can run",
                "Provide at least one prepared encoded-value batch; reader wiring remains a separate phase.",
            )],
        );
    }

    let segment_reports = batches
        .iter()
        .map(|batch| evaluate_predicate_on_encoded_values(predicate, &batch.segment, &batch.values))
        .collect();
    VortexEncodedPredicateEvaluationReport::from_segment_reports(
        predicate,
        segment_reports,
        Vec::new(),
    )
}

#[must_use]
pub const fn vortex_encoded_predicate_evaluation_discovery_report()
-> VortexEncodedPredicateEvaluationDiscoveryReport {
    VortexEncodedPredicateEvaluationDiscoveryReport::report_only()
}

fn encoded_segment_for_predicate(
    predicate: &PredicateExpr,
    segment: &VortexSegmentMetadataSummary,
    index: usize,
) -> Result<EncodedSegment, String> {
    let column = predicate
        .column()
        .cloned()
        .or_else(|| {
            segment
                .columns
                .iter()
                .find_map(|column| column.column.clone())
        })
        .unwrap_or_else(|| ColumnRef::new("__segment__").expect("static column name"));
    let column_summary = segment
        .columns
        .iter()
        .find(|candidate| candidate.column.as_ref() == Some(&column));
    let mut stats =
        column_summary.map_or_else(SegmentStats::unknown, |summary| summary.stats.clone());
    if stats.row_count.is_none() {
        stats.row_count = segment.row_count;
    }
    let Some(column_summary) = column_summary else {
        return Err(format!(
            "missing column metadata for predicate column {}",
            column.as_str()
        ));
    };

    let segment_id = segment
        .segment_id
        .clone()
        .unwrap_or_else(|| SegmentId::new(format!("segment-{index}")).expect("generated id"));
    Ok(EncodedSegment::new(
        segment_id,
        column,
        column_summary.dtype.clone(),
        column_summary.nullability,
        SegmentLayout::new(
            column_summary.encoding.clone(),
            column_summary.layout.clone(),
        ),
        stats,
    ))
}

fn count_status(
    reports: &[EncodedPredicateEvaluationReport],
    status: EncodedPredicateEvaluationStatus,
) -> usize {
    reports
        .iter()
        .filter(|report| report.status == status)
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{
        ComparisonOp, EncodedValueBatch, EncodingKind, LayoutKind, LogicalDType, Nullability,
        SegmentId, SelectionVector, StatValue,
    };

    use crate::{
        VortexColumnMetadataSummary, VortexFileMetadataSummary, VortexMetadataSummaryStatus,
    };

    fn summary_with_segment(segment: VortexSegmentMetadataSummary) -> VortexMetadataSummaryReport {
        VortexMetadataSummaryReport {
            status: VortexMetadataSummaryStatus::Summarized,
            summary: {
                let mut summary = VortexFileMetadataSummary::empty();
                summary.add_segment(segment);
                summary
            },
            diagnostics: Vec::new(),
        }
    }

    fn segment(stats: SegmentStats) -> VortexSegmentMetadataSummary {
        let mut segment = VortexSegmentMetadataSummary::unknown().with_row_count(5);
        segment.add_column(
            VortexColumnMetadataSummary::new(ColumnRef::new("x").expect("column"))
                .with_dtype(LogicalDType::Int64)
                .with_encoding(EncodingKind::VortexNative("test".to_string()))
                .with_layout(LayoutKind::Flat)
                .with_stats(stats)
                .with_statistics_available(true),
        );
        segment
    }

    fn encoded_segment(row_count: u64, encoding: EncodingKind) -> EncodedSegment {
        encoded_segment_with_id("encoded-segment-1", row_count, encoding)
    }

    fn encoded_segment_with_id(id: &str, row_count: u64, encoding: EncodingKind) -> EncodedSegment {
        EncodedSegment::new(
            SegmentId::new(id).expect("segment"),
            ColumnRef::new("x").expect("column"),
            LogicalDType::Int64,
            Nullability::Nullable,
            SegmentLayout::new(encoding, LayoutKind::Flat),
            SegmentStats::with_row_count(row_count),
        )
    }

    #[test]
    fn discovery_is_report_only() {
        let report = vortex_encoded_predicate_evaluation_discovery_report();

        assert_eq!(report.schema_version, SCHEMA_VERSION);
        assert_eq!(report.operator_kind, PhysicalOperatorKind::Filter);
        assert_eq!(report.kernel_kind, KernelKind::Encoded);
        assert!(report.emits_selection_vectors);
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn metadata_proven_predicate_emits_segment_selection_vector() {
        let mut stats = SegmentStats::with_row_count(5);
        stats.null_count = Some(0);
        let report = evaluate_vortex_encoded_predicate_segments(
            &PredicateExpr::IsNotNull {
                column: ColumnRef::new("x").expect("column"),
            },
            &summary_with_segment(segment(stats)),
        );

        assert_eq!(
            report.status,
            VortexEncodedPredicateEvaluationStatus::EvaluatedSelections
        );
        assert_eq!(report.segment_report_count, 1);
        assert_eq!(report.selected_all_count, 1);
        assert_eq!(report.selection_vectors_emitted, 1);
        assert_eq!(report.selected_rows_metadata_count, Some(5));
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn inconclusive_predicate_defers_to_encoded_values_without_reading() {
        let mut stats = SegmentStats::with_row_count(5);
        stats.min_value = Some(StatValue::Int64(1));
        stats.max_value = Some(StatValue::Int64(9));
        let report = evaluate_vortex_encoded_predicate_segments(
            &PredicateExpr::Compare {
                column: ColumnRef::new("x").expect("column"),
                op: ComparisonOp::Eq,
                value: StatValue::Int64(4),
            },
            &summary_with_segment(segment(stats)),
        );

        assert_eq!(
            report.status,
            VortexEncodedPredicateEvaluationStatus::NeedsEncodedValues
        );
        assert_eq!(report.needs_encoded_values_count, 1);
        assert_eq!(report.selection_vectors_emitted, 0);
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn missing_column_metadata_blocks_with_no_fallback_diagnostic() {
        let report = evaluate_vortex_encoded_predicate_segments(
            &PredicateExpr::IsNull {
                column: ColumnRef::new("y").expect("column"),
            },
            &summary_with_segment(segment(SegmentStats::with_row_count(5))),
        );

        assert_eq!(
            report.status,
            VortexEncodedPredicateEvaluationStatus::MissingMetadata
        );
        assert_eq!(report.segment_report_count, 0);
        assert_eq!(report.diagnostics.len(), 1);
        assert!(report.is_side_effect_free());
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }

    #[test]
    fn encoded_value_dictionary_batch_feeds_sparse_selection_vector_filter_kernel() {
        let segment = encoded_segment(5, EncodingKind::Dictionary);
        let values = EncodedValueBatch::Dictionary {
            dictionary: vec![Some(StatValue::Int64(1)), Some(StatValue::Int64(5)), None],
            codes: vec![Some(0), Some(1), None, Some(1), Some(0)],
        };
        let report = evaluate_vortex_encoded_value_predicate_batch(
            &PredicateExpr::Compare {
                column: ColumnRef::new("x").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(5),
            },
            &segment,
            &values,
        );

        assert_eq!(
            report.status,
            VortexEncodedPredicateEvaluationStatus::EvaluatedSelections
        );
        assert_eq!(report.selected_indices_count, 1);
        assert_eq!(report.selection_vectors_emitted, 1);
        assert_eq!(report.selected_rows_metadata_count, Some(2));
        assert_eq!(
            report.segment_reports[0].selection_vector,
            Some(SelectionVector::from_indices(vec![1, 3]))
        );
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());

        let filter_kernel = crate::evaluate_vortex_selection_vector_filter_kernel(&report);
        assert_eq!(
            filter_kernel.status,
            crate::VortexSelectionVectorFilterKernelStatus::EvaluatedSelectionVectors
        );
        assert_eq!(filter_kernel.selection_vector_count, 1);
        assert_eq!(filter_kernel.selected_row_count, Some(2));
        assert!(filter_kernel.is_safe_native_filter_kernel_evidence());
        assert!(filter_kernel.is_side_effect_free());
        assert!(!filter_kernel.has_errors());
    }

    #[test]
    fn encoded_value_batches_aggregate_multi_segment_filter_evidence() {
        let batches = vec![
            VortexEncodedValuePredicateBatch::new(
                encoded_segment_with_id("constant-segment", 3, EncodingKind::Constant),
                EncodedValueBatch::Constant {
                    value: Some(StatValue::Int64(7)),
                    row_count: 3,
                },
            ),
            VortexEncodedValuePredicateBatch::new(
                encoded_segment_with_id("dictionary-segment", 5, EncodingKind::Dictionary),
                EncodedValueBatch::Dictionary {
                    dictionary: vec![Some(StatValue::Int64(1)), Some(StatValue::Int64(5)), None],
                    codes: vec![Some(0), Some(1), None, Some(1), Some(0)],
                },
            ),
            VortexEncodedValuePredicateBatch::new(
                encoded_segment_with_id("run-segment", 3, EncodingKind::RunLength),
                EncodedValueBatch::RunLength {
                    runs: vec![shardloom_core::EncodedValueRun::new(
                        Some(StatValue::Int64(0)),
                        3,
                    )],
                },
            ),
        ];

        let report = evaluate_vortex_encoded_value_predicate_batches(
            &PredicateExpr::Compare {
                column: ColumnRef::new("x").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(5),
            },
            &batches,
        );

        assert_eq!(
            report.status,
            VortexEncodedPredicateEvaluationStatus::EvaluatedSelections
        );
        assert_eq!(report.segment_report_count, 3);
        assert_eq!(report.selected_all_count, 1);
        assert_eq!(report.selected_indices_count, 1);
        assert_eq!(report.selected_none_count, 1);
        assert_eq!(report.selection_vectors_emitted, 3);
        assert_eq!(report.selected_rows_metadata_count, Some(5));
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());

        let filter_kernel = crate::evaluate_vortex_selection_vector_filter_kernel(&report);
        assert_eq!(
            filter_kernel.status,
            crate::VortexSelectionVectorFilterKernelStatus::EvaluatedSelectionVectors
        );
        assert_eq!(filter_kernel.segment_count, 3);
        assert_eq!(filter_kernel.selection_vector_count, 3);
        assert_eq!(filter_kernel.selected_row_count, Some(5));
        assert!(filter_kernel.is_safe_native_filter_kernel_evidence());
        assert!(filter_kernel.is_side_effect_free());
    }

    #[test]
    fn encoded_value_batch_unsupported_type_blocks_without_fallback() {
        let segment = encoded_segment(1, EncodingKind::Constant);
        let values = EncodedValueBatch::Constant {
            value: Some(StatValue::Utf8("a".to_string())),
            row_count: 1,
        };
        let report = evaluate_vortex_encoded_value_predicate_batch(
            &PredicateExpr::Compare {
                column: ColumnRef::new("x").expect("column"),
                op: ComparisonOp::Eq,
                value: StatValue::Int64(1),
            },
            &segment,
            &values,
        );

        assert_eq!(
            report.status,
            VortexEncodedPredicateEvaluationStatus::Unsupported
        );
        assert_eq!(report.unsupported_count, 1);
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
        assert!(
            report
                .segment_reports
                .iter()
                .flat_map(|segment| segment.diagnostics.iter())
                .all(|diagnostic| !diagnostic.fallback.attempted)
        );
    }

    #[test]
    fn encoded_value_batches_empty_input_blocks_without_fallback() {
        let report =
            evaluate_vortex_encoded_value_predicate_batches(&PredicateExpr::AlwaysTrue, &[]);

        assert_eq!(
            report.status,
            VortexEncodedPredicateEvaluationStatus::MissingMetadata
        );
        assert_eq!(report.segment_report_count, 0);
        assert_eq!(report.selection_vectors_emitted, 0);
        assert!(report.is_side_effect_free());
        assert!(!report.diagnostics[0].fallback.attempted);

        let filter_kernel = crate::evaluate_vortex_selection_vector_filter_kernel(&report);
        assert_eq!(
            filter_kernel.status,
            crate::VortexSelectionVectorFilterKernelStatus::BlockedByPredicateEvaluation
        );
        assert!(filter_kernel.is_side_effect_free());
        assert!(filter_kernel.has_errors());
        assert!(
            filter_kernel
                .diagnostics
                .iter()
                .all(|diagnostic| !diagnostic.fallback.attempted)
        );
    }
}
