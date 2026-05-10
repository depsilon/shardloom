use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use shardloom_core::{
    ColumnRef, Diagnostic, DiagnosticCode, EncodedSegment, EncodedValueBatch,
    PhysicalOperatorExecutionLevel,
};

use crate::VortexSelectionVectorFilterKernelReport;

const SCHEMA_VERSION: &str = "shardloom.vortex_encoded_projection_execution.v1";
const REPORT_ID: &str = "vortex.query-primitive.project_columns.prepared-encoded-projection";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexEncodedProjectionExecutionStatus {
    ProjectedEncodedBatches,
    BlockedMissingProjection,
    BlockedMissingColumns,
    BlockedIncompleteFilterProjectionCoverage,
    BlockedUnsafeFilterKernel,
}

impl VortexEncodedProjectionExecutionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProjectedEncodedBatches => "projected_encoded_batches",
            Self::BlockedMissingProjection => "blocked_missing_projection",
            Self::BlockedMissingColumns => "blocked_missing_columns",
            Self::BlockedIncompleteFilterProjectionCoverage => {
                "blocked_incomplete_filter_projection_coverage"
            }
            Self::BlockedUnsafeFilterKernel => "blocked_unsafe_filter_kernel",
        }
    }

    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::ProjectedEncodedBatches)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VortexPreparedEncodedProjectionColumn {
    pub segment: EncodedSegment,
    pub values: EncodedValueBatch,
}

impl VortexPreparedEncodedProjectionColumn {
    #[must_use]
    pub const fn new(segment: EncodedSegment, values: EncodedValueBatch) -> Self {
        Self { segment, values }
    }

    #[must_use]
    pub fn column(&self) -> &ColumnRef {
        &self.segment.column
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexEncodedProjectionExecutionReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub status: VortexEncodedProjectionExecutionStatus,
    pub execution_level: PhysicalOperatorExecutionLevel,
    pub requested_columns: Vec<String>,
    pub projected_columns: Vec<String>,
    pub input_batch_count: usize,
    pub projected_batch_count: usize,
    pub filter_kernel_report_id: Option<String>,
    pub selection_vector_preserved: bool,
    pub selected_row_count: Option<u64>,
    pub encoded_batches_preserved: bool,
    pub production_claim_allowed: bool,
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
    pub projected_batches: Vec<VortexPreparedEncodedProjectionColumn>,
    pub diagnostics: Vec<Diagnostic>,
}

impl VortexEncodedProjectionExecutionReport {
    fn projected(
        requested_columns: &[ColumnRef],
        input_batch_count: usize,
        projected_batches: Vec<VortexPreparedEncodedProjectionColumn>,
        filter_kernel: Option<&VortexSelectionVectorFilterKernelReport>,
    ) -> Self {
        let selected_row_count = filter_kernel.and_then(|kernel| kernel.selected_row_count);
        Self::new(
            VortexEncodedProjectionExecutionStatus::ProjectedEncodedBatches,
            requested_columns,
            input_batch_count,
            projected_batches,
            filter_kernel,
            filter_kernel.is_some(),
            selected_row_count,
            Vec::new(),
        )
    }

    fn blocked(
        status: VortexEncodedProjectionExecutionStatus,
        requested_columns: &[ColumnRef],
        input_batch_count: usize,
        filter_kernel: Option<&VortexSelectionVectorFilterKernelReport>,
        diagnostic: Diagnostic,
    ) -> Self {
        let mut diagnostics =
            filter_kernel.map_or_else(Vec::new, |kernel| kernel.diagnostics.clone());
        diagnostics.push(diagnostic);
        Self::new(
            status,
            requested_columns,
            input_batch_count,
            Vec::new(),
            filter_kernel,
            false,
            None,
            diagnostics,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        status: VortexEncodedProjectionExecutionStatus,
        requested_columns: &[ColumnRef],
        input_batch_count: usize,
        projected_batches: Vec<VortexPreparedEncodedProjectionColumn>,
        filter_kernel: Option<&VortexSelectionVectorFilterKernelReport>,
        selection_vector_preserved: bool,
        selected_row_count: Option<u64>,
        diagnostics: Vec<Diagnostic>,
    ) -> Self {
        let projected_columns = projected_batches
            .iter()
            .map(|batch| batch.column().as_str().to_string())
            .collect();
        Self {
            schema_version: SCHEMA_VERSION,
            report_id: REPORT_ID.to_string(),
            status,
            execution_level: PhysicalOperatorExecutionLevel::EncodedNative,
            requested_columns: requested_columns
                .iter()
                .map(|column| column.as_str().to_string())
                .collect(),
            projected_columns,
            input_batch_count,
            projected_batch_count: projected_batches.len(),
            filter_kernel_report_id: filter_kernel.map(|kernel| kernel.kernel_report_id.clone()),
            selection_vector_preserved,
            selected_row_count,
            encoded_batches_preserved: status
                == VortexEncodedProjectionExecutionStatus::ProjectedEncodedBatches,
            production_claim_allowed: false,
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
            projected_batches,
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
            || self.production_claim_allowed
            || self.fallback_attempted
            || self.fallback_execution_allowed
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    shardloom_core::DiagnosticSeverity::Error
                        | shardloom_core::DiagnosticSeverity::Fatal
                )
            })
    }

    #[must_use]
    pub fn is_safe_encoded_projection_evidence(&self) -> bool {
        self.status == VortexEncodedProjectionExecutionStatus::ProjectedEncodedBatches
            && self.projected_batch_count > 0
            && self.encoded_batches_preserved
            && self.execution_level == PhysicalOperatorExecutionLevel::EncodedNative
            && self.is_side_effect_free()
            && !self.production_claim_allowed
            && !self.has_errors()
    }

    #[must_use]
    pub fn to_human_text(&self) -> String {
        let mut text = String::new();
        let _ = writeln!(text, "schema_version: {}", self.schema_version);
        let _ = writeln!(text, "encoded projection: {}", self.report_id);
        let _ = writeln!(text, "status: {}", self.status.as_str());
        let _ = writeln!(
            text,
            "requested columns: {}",
            self.requested_columns.join(",")
        );
        let _ = writeln!(
            text,
            "projected columns: {}",
            self.projected_columns.join(",")
        );
        let _ = writeln!(text, "projected batches: {}", self.projected_batch_count);
        let _ = writeln!(
            text,
            "selection vector preserved: {}",
            self.selection_vector_preserved
        );
        let _ = writeln!(text, "data decoded: false");
        let _ = writeln!(text, "data materialized: false");
        let _ = writeln!(text, "fallback attempted: false");
        let _ = writeln!(text, "fallback execution: disabled");
        text
    }
}

#[must_use]
pub fn evaluate_vortex_prepared_encoded_projection(
    requested_columns: &[ColumnRef],
    batches: &[VortexPreparedEncodedProjectionColumn],
    filter_kernel: Option<&VortexSelectionVectorFilterKernelReport>,
) -> VortexEncodedProjectionExecutionReport {
    if requested_columns.is_empty() {
        return VortexEncodedProjectionExecutionReport::blocked(
            VortexEncodedProjectionExecutionStatus::BlockedMissingProjection,
            requested_columns,
            batches.len(),
            filter_kernel,
            Diagnostic::not_implemented(
                "vortex_prepared_encoded_projection",
                "encoded projection requires at least one requested column",
                "Provide projected columns before evaluating prepared encoded projection evidence.",
            ),
        );
    }

    if let Some(filter_kernel) = filter_kernel
        && !filter_kernel.is_safe_native_filter_kernel_evidence()
    {
        return VortexEncodedProjectionExecutionReport::blocked(
            VortexEncodedProjectionExecutionStatus::BlockedUnsafeFilterKernel,
            requested_columns,
            batches.len(),
            Some(filter_kernel),
            Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "vortex_prepared_encoded_projection",
                "filter-project evidence requires a safe encoded selection-vector filter kernel",
                Some("Provide complete side-effect-free filter-kernel evidence before composing filter-project projection evidence.".to_string()),
            ),
        );
    }

    let mut missing = Vec::new();
    for column in requested_columns {
        if !batches.iter().any(|batch| batch.column() == column) {
            missing.push(column.as_str().to_string());
        }
    }
    if !missing.is_empty() {
        return VortexEncodedProjectionExecutionReport::blocked(
            VortexEncodedProjectionExecutionStatus::BlockedMissingColumns,
            requested_columns,
            batches.len(),
            filter_kernel,
            Diagnostic::not_implemented(
                "vortex_prepared_encoded_projection",
                format!(
                    "prepared encoded projection batches are missing requested columns: {}",
                    missing.join(",")
                ),
                "Provide prepared encoded batches for every requested projection column.",
            ),
        );
    }

    if let Some(filter_kernel) = filter_kernel
        && let Some(diagnostic) =
            filter_projection_coverage_diagnostic(requested_columns, batches, filter_kernel)
    {
        return VortexEncodedProjectionExecutionReport::blocked(
            VortexEncodedProjectionExecutionStatus::BlockedIncompleteFilterProjectionCoverage,
            requested_columns,
            batches.len(),
            Some(filter_kernel),
            diagnostic,
        );
    }

    let projected_batches = batches
        .iter()
        .filter(|batch| {
            requested_columns
                .iter()
                .any(|column| column == batch.column())
        })
        .cloned()
        .collect();
    VortexEncodedProjectionExecutionReport::projected(
        requested_columns,
        batches.len(),
        projected_batches,
        filter_kernel,
    )
}

fn filter_projection_coverage_diagnostic(
    requested_columns: &[ColumnRef],
    batches: &[VortexPreparedEncodedProjectionColumn],
    filter_kernel: &VortexSelectionVectorFilterKernelReport,
) -> Option<Diagnostic> {
    let required_segment_count = filter_kernel.segment_count;
    if required_segment_count == 0 {
        return Some(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            "vortex_prepared_encoded_projection",
            "filter-project evidence requires filter segment coverage",
            Some("Provide a safe filter kernel with at least one filtered segment.".to_string()),
        ));
    }

    let mut projected_segments_by_column: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for batch in batches {
        projected_segments_by_column
            .entry(batch.column().as_str())
            .or_default()
            .insert(batch.segment.id.as_str());
    }

    let incomplete_columns = requested_columns
        .iter()
        .filter_map(|column| {
            let projected_segment_count = projected_segments_by_column
                .get(column.as_str())
                .map_or(0, BTreeSet::len);
            (projected_segment_count != required_segment_count).then(|| {
                format!(
                    "{}:{projected_segment_count}/{required_segment_count}",
                    column.as_str()
                )
            })
        })
        .collect::<Vec<_>>();

    if incomplete_columns.is_empty() {
        None
    } else {
        Some(Diagnostic::unsupported(
            DiagnosticCode::NotImplemented,
            "vortex_prepared_encoded_projection",
            format!(
                "filter-project projection coverage is incomplete for requested columns: {}",
                incomplete_columns.join(",")
            ),
            Some(
                "Provide one projected encoded batch per filtered segment for every requested projection column before carrying selection-vector evidence."
                    .to_string(),
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{
        ComparisonOp, EncodedValueRun, EncodingKind, LayoutKind, LogicalDType, Nullability,
        PredicateExpr, SegmentId, SegmentLayout, SegmentStats, StatValue,
    };

    use crate::{
        VortexEncodedValuePredicateBatch, evaluate_vortex_encoded_value_predicate_batches,
        evaluate_vortex_selection_vector_filter_kernel,
    };

    fn column_ref(name: &str) -> ColumnRef {
        ColumnRef::new(name).expect("column")
    }

    fn segment(column: &str, id: &str, row_count: u64, encoding: EncodingKind) -> EncodedSegment {
        EncodedSegment::new(
            SegmentId::new(id).expect("segment"),
            column_ref(column),
            LogicalDType::Int64,
            Nullability::Nullable,
            SegmentLayout::new(encoding, LayoutKind::Flat),
            SegmentStats::with_row_count(row_count),
        )
    }

    fn prepared_column(
        column: &str,
        id: &str,
        values: EncodedValueBatch,
    ) -> VortexPreparedEncodedProjectionColumn {
        VortexPreparedEncodedProjectionColumn::new(
            segment(
                column,
                id,
                values.row_count().expect("row count"),
                values.encoding_kind(),
            ),
            values,
        )
    }

    #[test]
    fn prepared_encoded_projection_preserves_requested_batches_without_materialization() {
        let metric = prepared_column(
            "metric",
            "segment-1.metric",
            EncodedValueBatch::Dictionary {
                dictionary: vec![Some(StatValue::Int64(10)), Some(StatValue::Int64(20))],
                codes: vec![Some(0), Some(1), Some(0)],
            },
        );
        let other = prepared_column(
            "other",
            "segment-1.other",
            EncodedValueBatch::Constant {
                value: Some(StatValue::Int64(1)),
                row_count: 3,
            },
        );
        let batches = vec![metric.clone(), other];

        let report =
            evaluate_vortex_prepared_encoded_projection(&[column_ref("metric")], &batches, None);

        assert_eq!(
            report.status,
            VortexEncodedProjectionExecutionStatus::ProjectedEncodedBatches
        );
        assert_eq!(report.input_batch_count, 2);
        assert_eq!(report.projected_batch_count, 1);
        assert_eq!(report.projected_batches, vec![metric]);
        assert_eq!(report.projected_columns, vec!["metric".to_string()]);
        assert!(report.encoded_batches_preserved);
        assert!(!report.selection_vector_preserved);
        assert!(report.is_safe_encoded_projection_evidence());
        assert!(report.is_side_effect_free());
        assert!(!report.has_errors());
    }

    #[test]
    fn prepared_encoded_filter_project_preserves_safe_selection_vector_evidence() {
        let predicate = PredicateExpr::Compare {
            column: column_ref("metric"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(5),
        };
        let filter_batches = vec![
            VortexEncodedValuePredicateBatch::new(
                segment("metric", "segment-1.metric", 5, EncodingKind::Dictionary),
                EncodedValueBatch::Dictionary {
                    dictionary: vec![Some(StatValue::Int64(1)), Some(StatValue::Int64(5)), None],
                    codes: vec![Some(0), Some(1), None, Some(1), Some(0)],
                },
            ),
            VortexEncodedValuePredicateBatch::new(
                segment("metric", "segment-2.metric", 3, EncodingKind::RunLength),
                EncodedValueBatch::RunLength {
                    runs: vec![EncodedValueRun::new(Some(StatValue::Int64(9)), 3)],
                },
            ),
        ];
        let predicate_report =
            evaluate_vortex_encoded_value_predicate_batches(&predicate, &filter_batches);
        let filter_kernel = evaluate_vortex_selection_vector_filter_kernel(&predicate_report);
        assert!(filter_kernel.is_safe_native_filter_kernel_evidence());

        let projection_batches = vec![
            prepared_column(
                "metric",
                "segment-1.metric",
                EncodedValueBatch::Dictionary {
                    dictionary: vec![Some(StatValue::Int64(1)), Some(StatValue::Int64(5)), None],
                    codes: vec![Some(0), Some(1), None, Some(1), Some(0)],
                },
            ),
            prepared_column(
                "payload",
                "segment-1.payload",
                EncodedValueBatch::Constant {
                    value: Some(StatValue::Int64(100)),
                    row_count: 5,
                },
            ),
            prepared_column(
                "payload",
                "segment-2.payload",
                EncodedValueBatch::RunLength {
                    runs: vec![EncodedValueRun::new(Some(StatValue::Int64(200)), 3)],
                },
            ),
        ];

        let report = evaluate_vortex_prepared_encoded_projection(
            &[column_ref("payload")],
            &projection_batches,
            Some(&filter_kernel),
        );

        assert_eq!(
            report.status,
            VortexEncodedProjectionExecutionStatus::ProjectedEncodedBatches
        );
        assert_eq!(report.projected_batch_count, 2);
        assert_eq!(
            report.filter_kernel_report_id,
            Some(filter_kernel.kernel_report_id)
        );
        assert!(report.selection_vector_preserved);
        assert_eq!(report.selected_row_count, Some(5));
        assert!(report.encoded_batches_preserved);
        assert!(report.is_safe_encoded_projection_evidence());
        assert!(report.is_side_effect_free());
    }

    #[test]
    fn prepared_encoded_projection_missing_column_blocks_without_fallback() {
        let batches = vec![prepared_column(
            "metric",
            "segment-1.metric",
            EncodedValueBatch::Constant {
                value: Some(StatValue::Int64(1)),
                row_count: 1,
            },
        )];

        let report =
            evaluate_vortex_prepared_encoded_projection(&[column_ref("payload")], &batches, None);

        assert_eq!(
            report.status,
            VortexEncodedProjectionExecutionStatus::BlockedMissingColumns
        );
        assert_eq!(report.projected_batch_count, 0);
        assert!(!report.encoded_batches_preserved);
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }

    #[test]
    fn prepared_encoded_filter_project_blocks_incomplete_projection_coverage_without_fallback() {
        let predicate = PredicateExpr::Compare {
            column: column_ref("metric"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(5),
        };
        let filter_batches = vec![
            VortexEncodedValuePredicateBatch::new(
                segment("metric", "segment-1.metric", 5, EncodingKind::Dictionary),
                EncodedValueBatch::Dictionary {
                    dictionary: vec![Some(StatValue::Int64(1)), Some(StatValue::Int64(5))],
                    codes: vec![Some(0), Some(1), Some(1), Some(0), Some(1)],
                },
            ),
            VortexEncodedValuePredicateBatch::new(
                segment("metric", "segment-2.metric", 3, EncodingKind::RunLength),
                EncodedValueBatch::RunLength {
                    runs: vec![EncodedValueRun::new(Some(StatValue::Int64(9)), 3)],
                },
            ),
        ];
        let predicate_report =
            evaluate_vortex_encoded_value_predicate_batches(&predicate, &filter_batches);
        let filter_kernel = evaluate_vortex_selection_vector_filter_kernel(&predicate_report);
        assert!(filter_kernel.is_safe_native_filter_kernel_evidence());

        let projection_batches = vec![prepared_column(
            "payload",
            "segment-1.payload",
            EncodedValueBatch::Constant {
                value: Some(StatValue::Int64(100)),
                row_count: 5,
            },
        )];

        let report = evaluate_vortex_prepared_encoded_projection(
            &[column_ref("payload")],
            &projection_batches,
            Some(&filter_kernel),
        );

        assert_eq!(
            report.status,
            VortexEncodedProjectionExecutionStatus::BlockedIncompleteFilterProjectionCoverage
        );
        assert_eq!(report.projected_batch_count, 0);
        assert!(!report.selection_vector_preserved);
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }

    #[test]
    fn prepared_encoded_filter_project_blocks_unsafe_filter_kernel_without_fallback() {
        let predicate_report =
            evaluate_vortex_encoded_value_predicate_batches(&PredicateExpr::AlwaysTrue, &[]);
        let filter_kernel = evaluate_vortex_selection_vector_filter_kernel(&predicate_report);
        assert!(!filter_kernel.is_safe_native_filter_kernel_evidence());
        let batches = vec![prepared_column(
            "payload",
            "segment-1.payload",
            EncodedValueBatch::Constant {
                value: Some(StatValue::Int64(1)),
                row_count: 1,
            },
        )];

        let report = evaluate_vortex_prepared_encoded_projection(
            &[column_ref("payload")],
            &batches,
            Some(&filter_kernel),
        );

        assert_eq!(
            report.status,
            VortexEncodedProjectionExecutionStatus::BlockedUnsafeFilterKernel
        );
        assert_eq!(report.projected_batch_count, 0);
        assert!(!report.selection_vector_preserved);
        assert!(report.has_errors());
        assert!(report.is_side_effect_free());
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }
}
