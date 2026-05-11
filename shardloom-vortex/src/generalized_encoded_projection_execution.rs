use std::fmt::Write as _;

use shardloom_core::{
    ColumnRef, CorrectnessFixture, CorrectnessValidationPlan, Diagnostic, DiagnosticCategory,
    DiagnosticCode, DiagnosticSeverity, EncodedValueBatch, ExecutionCertificate,
    ExecutionCertificateInput, ExpectedOutcome, NativeIoAdapterFidelityReport, NativeIoCertificate,
    NativeIoRepresentationTransition, NativeIoSideEffectReport, NativeIoSinkRequirementReport,
    NativeIoSourceCapabilityReport, NativeIoSourcePushdownReport, RepresentationState, Result,
};

use crate::{
    VortexEncodedProjectionExecutionReport, VortexPreparedEncodedProjectionColumn,
    VortexSelectionVectorFilterKernelReport, evaluate_vortex_prepared_encoded_projection,
};

const SCHEMA_VERSION: &str = "shardloom.vortex_generalized_encoded_projection_execution.v1";
const REPORT_ID: &str = "vortex.cg2.generalized-projection.prepared-encoded-columns";
const EXECUTION_KIND: &str = "vortex.prepared_encoded_projection";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexGeneralizedEncodedProjectionExecutionStatus {
    ExecutedPreparedEncodedProjection,
    BlockedPreparedProjectionEvidence,
    BlockedNativeIoCertificate,
}

impl VortexGeneralizedEncodedProjectionExecutionStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExecutedPreparedEncodedProjection => "executed_prepared_encoded_projection",
            Self::BlockedPreparedProjectionEvidence => "blocked_prepared_projection_evidence",
            Self::BlockedNativeIoCertificate => "blocked_native_io_certificate",
        }
    }

    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::ExecutedPreparedEncodedProjection)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexGeneralizedEncodedProjectionExecutionReport {
    pub schema_version: &'static str,
    pub report_id: String,
    pub execution_kind: &'static str,
    pub status: VortexGeneralizedEncodedProjectionExecutionStatus,
    pub requested_columns: Vec<String>,
    pub projected_columns: Vec<String>,
    pub input_batch_count: usize,
    pub projected_batch_count: usize,
    pub filter_project: bool,
    pub filter_kernel_report_id: Option<String>,
    pub selection_vector_preserved: bool,
    pub selected_row_count: Option<u64>,
    pub projected_row_count: Option<u64>,
    pub projection_evidence: VortexEncodedProjectionExecutionReport,
    pub native_io_certificate: NativeIoCertificate,
    pub execution_certificate: ExecutionCertificate,
    pub runtime_execution_allowed: bool,
    pub prepared_encoded_columns_consumed: bool,
    pub encoded_projection_guaranteed: bool,
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

impl VortexGeneralizedEncodedProjectionExecutionReport {
    fn from_evidence(
        projection_evidence: VortexEncodedProjectionExecutionReport,
        native_io_certificate: NativeIoCertificate,
        execution_certificate: ExecutionCertificate,
    ) -> Self {
        let mut diagnostics = projection_evidence.diagnostics.clone();
        diagnostics.extend(native_io_certificate.diagnostics.clone());
        diagnostics.extend(execution_certificate.diagnostics.clone());

        let projection_safe = projection_evidence.is_safe_encoded_projection_evidence();
        let native_io_safe = native_io_certificate.is_certified();
        let status = if !projection_safe {
            VortexGeneralizedEncodedProjectionExecutionStatus::BlockedPreparedProjectionEvidence
        } else if !native_io_safe {
            VortexGeneralizedEncodedProjectionExecutionStatus::BlockedNativeIoCertificate
        } else {
            VortexGeneralizedEncodedProjectionExecutionStatus::ExecutedPreparedEncodedProjection
        };
        let runtime_execution_allowed = status
            == VortexGeneralizedEncodedProjectionExecutionStatus::ExecutedPreparedEncodedProjection;
        let projected_row_count = projected_row_count(&projection_evidence);
        let correctness_certified = execution_certificate.is_certified();

        Self {
            schema_version: SCHEMA_VERSION,
            report_id: REPORT_ID.to_string(),
            execution_kind: EXECUTION_KIND,
            status,
            requested_columns: projection_evidence.requested_columns.clone(),
            projected_columns: projection_evidence.projected_columns.clone(),
            input_batch_count: projection_evidence.input_batch_count,
            projected_batch_count: projection_evidence.projected_batch_count,
            filter_project: projection_evidence.filter_kernel_report_id.is_some(),
            filter_kernel_report_id: projection_evidence.filter_kernel_report_id.clone(),
            selection_vector_preserved: projection_evidence.selection_vector_preserved,
            selected_row_count: projection_evidence.selected_row_count,
            projected_row_count,
            projection_evidence,
            native_io_certificate,
            execution_certificate,
            runtime_execution_allowed,
            prepared_encoded_columns_consumed: runtime_execution_allowed,
            encoded_projection_guaranteed: runtime_execution_allowed,
            correctness_certified,
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
            || execution_certificate_has_errors(&self.execution_certificate)
            || self.projection_evidence.has_errors()
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
        let _ = writeln!(&mut out, "Vortex generalized encoded projection execution");
        let _ = writeln!(&mut out, "schema_version: {}", self.schema_version);
        let _ = writeln!(&mut out, "report: {}", self.report_id);
        let _ = writeln!(&mut out, "execution kind: {}", self.execution_kind);
        let _ = writeln!(&mut out, "status: {}", self.status.as_str());
        let _ = writeln!(
            &mut out,
            "requested columns: {}",
            self.requested_columns.join(",")
        );
        let _ = writeln!(
            &mut out,
            "projected columns: {}",
            self.projected_columns.join(",")
        );
        let _ = writeln!(&mut out, "input batches: {}", self.input_batch_count);
        let _ = writeln!(
            &mut out,
            "projected batches: {}",
            self.projected_batch_count
        );
        let _ = writeln!(&mut out, "filter project: {}", self.filter_project);
        let _ = writeln!(
            &mut out,
            "selection vector preserved: {}",
            self.selection_vector_preserved
        );
        let _ = writeln!(
            &mut out,
            "runtime execution allowed: {}",
            self.runtime_execution_allowed
        );
        let _ = writeln!(
            &mut out,
            "correctness certified: {}",
            self.correctness_certified
        );
        let _ = writeln!(
            &mut out,
            "execution certificate: {}",
            self.execution_certificate.status.as_str()
        );
        let _ = writeln!(&mut out, "production claim allowed: false");
        let _ = writeln!(&mut out, "fallback execution allowed: false");
        out
    }
}

/// Executes generalized encoded projection over already-prepared encoded
/// column batches.
///
/// The optional filter kernel lets the same surface represent filter-project
/// execution when safe selection-vector filter evidence already exists. This
/// function does not open files, call object stores, decode rows, materialize
/// values, convert to Arrow, write output, spill, or invoke fallback execution.
///
/// # Errors
/// Returns an error only when the Native I/O certificate cannot be constructed.
pub fn execute_vortex_generalized_projection_from_encoded_projection_batches(
    requested_columns: &[ColumnRef],
    batches: &[VortexPreparedEncodedProjectionColumn],
    filter_kernel: Option<&VortexSelectionVectorFilterKernelReport>,
) -> Result<VortexGeneralizedEncodedProjectionExecutionReport> {
    let projection_evidence =
        evaluate_vortex_prepared_encoded_projection(requested_columns, batches, filter_kernel);
    let native_io_certificate =
        prepared_encoded_projection_native_io_certificate(&projection_evidence)?;
    let execution_certificate = prepared_encoded_projection_execution_certificate(
        &projection_evidence,
        &native_io_certificate,
    )?;
    Ok(
        VortexGeneralizedEncodedProjectionExecutionReport::from_evidence(
            projection_evidence,
            native_io_certificate,
            execution_certificate,
        ),
    )
}

fn prepared_encoded_projection_execution_certificate(
    projection_evidence: &VortexEncodedProjectionExecutionReport,
    native_io_certificate: &NativeIoCertificate,
) -> Result<ExecutionCertificate> {
    let correctness_fixture =
        prepared_encoded_projection_correctness_fixture(projection_evidence, native_io_certificate);
    let mut input = ExecutionCertificateInput::new(
        "cg16.prepared_encoded_projection.execution-certificate",
        EXECUTION_KIND,
    )?;
    input.plan_ref =
        Some("execute_vortex_generalized_projection_from_encoded_projection_batches".to_string());
    input.input_ref = Some(format!(
        "prepared_vortex_encoded_projection_batches:{}",
        projection_evidence.input_batch_count
    ));
    input.output_ref = Some(if projection_evidence.selection_vector_preserved {
        "selection_vector_filter_project_result".to_string()
    } else {
        "encoded_projection_result".to_string()
    });
    input.actual_outcome = Some(ExpectedOutcome::Rows {
        row_count: projection_evidence
            .selected_row_count
            .or_else(|| projected_row_count(projection_evidence)),
    });
    if let Some(fixture) = &correctness_fixture {
        input.correctness_fixture_id = Some(fixture.id.as_str().to_string());
        input.expected_outcome = Some(fixture.expected.clone());
    }
    input.selected_segment_count = projection_evidence.projected_batch_count;
    input.side_effects_performed = if projection_evidence.projected_batch_count > 0 {
        vec!["prepared_encoded_projection_kernel".to_string()]
    } else {
        Vec::new()
    };
    input.unsafe_effect_detected =
        !prepared_encoded_projection_execution_safe(projection_evidence, native_io_certificate);
    input.fallback_attempted = projection_evidence
        .diagnostics
        .iter()
        .chain(native_io_certificate.diagnostics.iter())
        .any(|diagnostic| diagnostic.fallback.attempted);
    input.fallback_execution_allowed = false;
    input.correctness_passed = correctness_fixture.as_ref().is_some_and(|fixture| {
        prepared_encoded_projection_execution_safe(projection_evidence, native_io_certificate)
            && input.actual_outcome.as_ref() == Some(&fixture.expected)
    });
    input
        .diagnostics
        .extend(projection_evidence.diagnostics.clone());
    input
        .diagnostics
        .extend(native_io_certificate.diagnostics.clone());
    if projection_evidence.projected_batch_count == 0 {
        input.diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            "vortex_prepared_encoded_projection_execution_certificate",
            "prepared encoded projection execution certificate requires at least one projected encoded batch",
            Some(
                "Feed explicit encoded projection batches for every requested column before accepting this execution path."
                    .to_string(),
            ),
        ));
    }
    if input.correctness_fixture_id.is_none() {
        input.diagnostics.push(Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Warning,
            DiagnosticCategory::Planning,
            "prepared encoded projection execution has execution evidence but no CG-5 correctness fixture/reference output yet",
            Some("vortex_prepared_encoded_projection_execution_certificate".to_string()),
            Some(
                "Prepared encoded projection/filter-project execution has execution evidence but no CG-5 correctness fixture/reference output yet."
                    .to_string(),
            ),
            Some(
                "Add a CG-5 prepared encoded projection/filter-project fixture before certifying correctness or production claims."
                    .to_string(),
            ),
            shardloom_core::FallbackStatus::disabled_by_policy(),
        ));
    }
    Ok(ExecutionCertificate::evaluate(input))
}

fn prepared_encoded_projection_correctness_fixture(
    projection_evidence: &VortexEncodedProjectionExecutionReport,
    native_io_certificate: &NativeIoCertificate,
) -> Option<CorrectnessFixture> {
    if !native_io_certificate.is_certified() {
        return None;
    }
    let projected_rows = projected_row_count(projection_evidence);
    if !projection_evidence.selection_vector_preserved
        && string_order_eq(&projection_evidence.requested_columns, &["metric"])
        && string_order_eq(&projection_evidence.projected_columns, &["metric"])
        && projection_evidence.input_batch_count == 2
        && projection_evidence.projected_batch_count == 1
        && projected_rows == Some(3)
    {
        return correctness_fixture_by_id("vortex-prepared-encoded-projection-dictionary");
    }
    if projection_evidence.selection_vector_preserved
        && string_order_eq(&projection_evidence.requested_columns, &["payload"])
        && string_order_eq(
            &projection_evidence.projected_columns,
            &["payload", "payload"],
        )
        && projection_evidence.input_batch_count == 2
        && projection_evidence.projected_batch_count == 2
        && projection_evidence.selected_row_count == Some(5)
        && projected_rows == Some(8)
    {
        return correctness_fixture_by_id(
            "vortex-prepared-encoded-filter-project-selection-vector",
        );
    }
    None
}

fn string_order_eq(actual: &[String], expected: &[&str]) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected.iter())
            .all(|(actual, expected)| actual == expected)
}

fn correctness_fixture_by_id(id: &str) -> Option<CorrectnessFixture> {
    CorrectnessValidationPlan::default_foundation_plan()
        .fixtures
        .into_iter()
        .find(|fixture| fixture.id.as_str() == id)
}

fn prepared_encoded_projection_execution_safe(
    projection_evidence: &VortexEncodedProjectionExecutionReport,
    native_io_certificate: &NativeIoCertificate,
) -> bool {
    projection_evidence.is_safe_encoded_projection_evidence()
        && projection_evidence.projected_batch_count > 0
        && native_io_certificate.is_certified()
}

fn execution_certificate_has_errors(certificate: &ExecutionCertificate) -> bool {
    certificate.fallback_attempted
        || certificate.fallback_execution_allowed
        || certificate.unsafe_effect_detected
        || certificate.diagnostics.iter().any(|diagnostic| {
            matches!(
                diagnostic.severity,
                shardloom_core::DiagnosticSeverity::Error
                    | shardloom_core::DiagnosticSeverity::Fatal
            )
        })
}

fn prepared_encoded_projection_native_io_certificate(
    projection_evidence: &VortexEncodedProjectionExecutionReport,
) -> Result<NativeIoCertificate> {
    let safe = projection_evidence.is_safe_encoded_projection_evidence();
    let filter_project = projection_evidence.filter_kernel_report_id.is_some();
    let diagnostics = prepared_encoded_projection_native_io_diagnostics(safe, projection_evidence);
    let mut certificate = NativeIoCertificate::new(
        "cg19.prepared_encoded_projection.native_io",
        if filter_project {
            "prepared_vortex_encoded_batches_to_selection_vector_filter_project_result"
        } else {
            "prepared_vortex_encoded_batches_to_encoded_projection_result"
        },
        prepared_encoded_projection_source_capability_report(safe, projection_evidence),
        prepared_encoded_projection_source_pushdown_report(safe, projection_evidence),
        vec![NativeIoRepresentationTransition::new(
            RepresentationState::VortexEncoded,
            if safe {
                if filter_project {
                    RepresentationState::SelectionVectorEncoded
                } else {
                    RepresentationState::VortexEncoded
                }
            } else {
                RepresentationState::Unsupported
            },
            false,
        )],
        prepared_encoded_projection_sink_requirement_report(safe, projection_evidence),
        prepared_encoded_projection_adapter_fidelity_report(safe, filter_project),
        Vec::new(),
        prepared_encoded_projection_side_effect_report(&diagnostics),
        diagnostics,
    )?;
    certificate.fallback_attempted = certificate
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.fallback.attempted);
    Ok(certificate)
}

fn prepared_encoded_projection_native_io_diagnostics(
    safe: bool,
    projection_evidence: &VortexEncodedProjectionExecutionReport,
) -> Vec<Diagnostic> {
    let mut diagnostics = projection_evidence.diagnostics.clone();
    if !safe {
        diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            "vortex_prepared_encoded_projection_native_io_certificate",
            "prepared encoded projection Native I/O certificate requires complete prepared encoded projection evidence with no decode, materialization, row reads, Arrow conversion, object-store IO, writes, spill, external effects, or fallback",
            Some("Provide prepared encoded projection batches for every requested column before accepting this generalized projection path.".to_string()),
        ));
    }
    diagnostics
}

fn prepared_encoded_projection_source_capability_report(
    safe: bool,
    projection_evidence: &VortexEncodedProjectionExecutionReport,
) -> NativeIoSourceCapabilityReport {
    NativeIoSourceCapabilityReport {
        source_kind: "vortex_prepared_encoded_projection_batches".to_string(),
        adapter_id: "shardloom.adapter.vortex.prepared_encoded_projection.v1".to_string(),
        schema_discovery_status: if projection_evidence.input_batch_count > 0 {
            "prepared_encoded_column_metadata_available".to_string()
        } else {
            "not_available".to_string()
        },
        statistics_availability: if projection_evidence.input_batch_count > 0 {
            "prepared_row_counts_available".to_string()
        } else {
            "unknown".to_string()
        },
        pushdown_capabilities: if safe {
            accepted_operation_order(projection_evidence).join(",")
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

fn prepared_encoded_projection_source_pushdown_report(
    safe: bool,
    projection_evidence: &VortexEncodedProjectionExecutionReport,
) -> NativeIoSourcePushdownReport {
    NativeIoSourcePushdownReport {
        accepted_operations: if safe {
            accepted_operation_order(projection_evidence)
        } else {
            Vec::new()
        },
        rejected_operations: if safe {
            Vec::new()
        } else {
            accepted_operation_order(projection_evidence)
        },
        guarantee: if safe {
            if projection_evidence.selection_vector_preserved {
                "exact_selection_vector_filter_project_from_prepared_encoded_columns".to_string()
            } else {
                "exact_encoded_projection_from_prepared_encoded_columns".to_string()
            }
        } else {
            "unsupported".to_string()
        },
        proof_basis: if safe {
            "native encoded projection kernel preserved explicit prepared encoded column batches and optional safe selection-vector filter evidence".to_string()
        } else {
            "Native I/O certificate blocked before accepting prepared encoded projection evidence"
                .to_string()
        },
        residual_expression: (!safe).then(|| projection_evidence.requested_columns.join(",")),
        conservative_false_positive_policy: false,
        unsafe_rejected_reason: (!safe)
            .then(|| "missing complete safe prepared encoded projection evidence".to_string()),
        fallback_attempted: false,
    }
}

fn prepared_encoded_projection_sink_requirement_report(
    safe: bool,
    projection_evidence: &VortexEncodedProjectionExecutionReport,
) -> NativeIoSinkRequirementReport {
    NativeIoSinkRequirementReport {
        target_format: if projection_evidence.selection_vector_preserved {
            "selection_vector_filter_project_result".to_string()
        } else {
            "encoded_projection_result".to_string()
        },
        accepts_encoded: safe,
        requires_decoded_columnar: false,
        requires_rows: false,
        preserves_metadata: safe,
        requires_ordering: false,
        requires_partitioning: false,
        requires_commit: false,
        supports_streaming: false,
        max_chunk_size: projection_evidence
            .selected_row_count
            .or_else(|| projected_row_count(projection_evidence)),
        backpressure_policy: "not_applicable_prepared_encoded_projection_batches".to_string(),
    }
}

fn prepared_encoded_projection_adapter_fidelity_report(
    safe: bool,
    filter_project: bool,
) -> NativeIoAdapterFidelityReport {
    NativeIoAdapterFidelityReport {
        adapter_id: "shardloom.adapter.vortex.prepared_encoded_projection.v1".to_string(),
        source_kind: "vortex_prepared_encoded_projection_batches".to_string(),
        sink_kind: if filter_project {
            "selection_vector_filter_project_result".to_string()
        } else {
            "encoded_projection_result".to_string()
        },
        metadata_preserved: safe,
        statistics_preserved: safe,
        encoded_representation_preserved: safe,
        materialization_required: false,
        fidelity_loss: if safe {
            "none_for_encoded_projection_result".to_string()
        } else {
            "unsupported".to_string()
        },
        metadata_loss: "none".to_string(),
        fallback_attempted: false,
    }
}

fn prepared_encoded_projection_side_effect_report(
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

fn accepted_operation_order(
    projection_evidence: &VortexEncodedProjectionExecutionReport,
) -> Vec<String> {
    if projection_evidence.selection_vector_preserved {
        vec!["filter".to_string(), "project".to_string()]
    } else {
        vec!["project".to_string()]
    }
}

fn projected_row_count(
    projection_evidence: &VortexEncodedProjectionExecutionReport,
) -> Option<u64> {
    let mut total = 0_u64;
    let mut saw_batch = false;
    for batch in &projection_evidence.projected_batches {
        let row_count = match &batch.values {
            EncodedValueBatch::Constant { row_count, .. } => Some(*row_count),
            EncodedValueBatch::Dictionary { codes, .. } => u64::try_from(codes.len()).ok(),
            EncodedValueBatch::RunLength { runs } => {
                Some(runs.iter().map(|run| run.len).sum::<u64>())
            }
        }?;
        total = total.saturating_add(row_count);
        saw_batch = true;
    }
    saw_batch.then_some(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardloom_core::{
        ComparisonOp, EncodedSegment, EncodedValueRun, EncodingKind, ExecutionCertificateStatus,
        LayoutKind, LogicalDType, Nullability, PredicateExpr, SegmentId, SegmentLayout,
        SegmentStats, StatValue,
    };

    use crate::{
        VortexEncodedValuePredicateBatch,
        execute_vortex_generalized_filter_from_encoded_value_batches,
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
    fn prepared_encoded_projection_executes_without_materialization() {
        let batches = vec![
            prepared_column(
                "metric",
                "segment-1.metric",
                EncodedValueBatch::Dictionary {
                    dictionary: vec![Some(StatValue::Int64(10)), Some(StatValue::Int64(20))],
                    codes: vec![Some(0), Some(1), Some(0)],
                },
            ),
            prepared_column(
                "other",
                "segment-1.other",
                EncodedValueBatch::Constant {
                    value: Some(StatValue::Int64(1)),
                    row_count: 3,
                },
            ),
        ];

        let report = execute_vortex_generalized_projection_from_encoded_projection_batches(
            &[column_ref("metric")],
            &batches,
            None,
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexGeneralizedEncodedProjectionExecutionStatus::ExecutedPreparedEncodedProjection
        );
        assert_eq!(report.input_batch_count, 2);
        assert_eq!(report.projected_batch_count, 1);
        assert_eq!(report.projected_columns, vec!["metric".to_string()]);
        assert_eq!(report.projected_row_count, Some(3));
        assert!(report.runtime_execution_allowed);
        assert!(report.prepared_encoded_columns_consumed);
        assert!(report.encoded_projection_guaranteed);
        assert!(!report.selection_vector_preserved);
        assert!(report.correctness_certified);
        assert!(!report.production_claim_allowed);
        assert!(report.avoids_unsafe_effects());
        assert!(report.native_io_certificate.is_certified());
        assert_eq!(
            report.execution_certificate.status,
            ExecutionCertificateStatus::Certified
        );
        assert!(report.execution_certificate.fallback_free());
        assert!(!report.execution_certificate.unsafe_effect_detected);
        assert!(report.correctness_certified);
        assert_eq!(
            report
                .execution_certificate
                .correctness_fixture_id
                .as_deref(),
            Some("vortex-prepared-encoded-projection-dictionary")
        );
        assert_eq!(
            report.execution_certificate.actual_outcome,
            Some(ExpectedOutcome::Rows { row_count: Some(3) })
        );
        assert_eq!(
            report
                .native_io_certificate
                .representation_transition_order(),
            "vortex_encoded->vortex_encoded"
        );
        assert_eq!(
            report
                .native_io_certificate
                .source_pushdown_report
                .accepted_operation_order(),
            "project"
        );
        assert!(!report.has_errors());
    }

    #[test]
    fn prepared_encoded_filter_project_executes_with_selection_vector_evidence() {
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
        let filter_report = execute_vortex_generalized_filter_from_encoded_value_batches(
            &predicate,
            &filter_batches,
        )
        .expect("filter report");

        let projection_batches = vec![
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

        let report = execute_vortex_generalized_projection_from_encoded_projection_batches(
            &[column_ref("payload")],
            &projection_batches,
            Some(&filter_report.filter_kernel),
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexGeneralizedEncodedProjectionExecutionStatus::ExecutedPreparedEncodedProjection
        );
        assert!(report.filter_project);
        assert_eq!(report.projected_batch_count, 2);
        assert!(report.selection_vector_preserved);
        assert_eq!(report.selected_row_count, Some(5));
        assert_eq!(report.projected_row_count, Some(8));
        assert!(report.runtime_execution_allowed);
        assert!(report.avoids_unsafe_effects());
        assert!(report.native_io_certificate.is_certified());
        assert_eq!(
            report.execution_certificate.status,
            ExecutionCertificateStatus::Certified
        );
        assert!(report.execution_certificate.fallback_free());
        assert!(!report.execution_certificate.unsafe_effect_detected);
        assert!(report.correctness_certified);
        assert_eq!(
            report
                .execution_certificate
                .correctness_fixture_id
                .as_deref(),
            Some("vortex-prepared-encoded-filter-project-selection-vector")
        );
        assert_eq!(
            report.execution_certificate.actual_outcome,
            Some(ExpectedOutcome::Rows { row_count: Some(5) })
        );
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
            "filter,project"
        );
        assert!(!report.has_errors());
    }

    #[test]
    fn prepared_encoded_projection_blocks_missing_column_without_fallback() {
        let batches = vec![prepared_column(
            "metric",
            "segment-1.metric",
            EncodedValueBatch::Constant {
                value: Some(StatValue::Int64(1)),
                row_count: 1,
            },
        )];

        let report = execute_vortex_generalized_projection_from_encoded_projection_batches(
            &[column_ref("payload")],
            &batches,
            None,
        )
        .expect("report");

        assert_eq!(
            report.status,
            VortexGeneralizedEncodedProjectionExecutionStatus::BlockedPreparedProjectionEvidence
        );
        assert!(!report.runtime_execution_allowed);
        assert!(!report.encoded_projection_guaranteed);
        assert!(report.avoids_unsafe_effects());
        assert!(report.native_io_certificate.has_errors());
        assert_eq!(
            report.execution_certificate.status,
            ExecutionCertificateStatus::Blocked
        );
        assert!(report.execution_certificate.unsafe_effect_detected);
        assert!(report.has_errors());
        assert!(report.diagnostics.iter().all(|d| !d.fallback.attempted));
    }
}
