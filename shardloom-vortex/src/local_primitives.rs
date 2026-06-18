#![allow(clippy::must_use_candidate)]

use std::fmt::Write as _;

#[cfg(feature = "vortex-local-primitives")]
use shardloom_core::{
    ColumnRef, EncodedSegment, EncodedValueBatch, EncodedValueRun, EncodingKind, LayoutKind,
    LogicalDType, Nullability as ShardLoomNullability, SegmentId, SegmentLayout, SegmentStats,
    UniversalInputSource, UriScheme,
};
use shardloom_core::{
    ComparisonOp, CorrectnessFixture, CorrectnessValidationPlan, DatasetUri, Diagnostic,
    DiagnosticCode, DiagnosticSeverity, ExecutionCertificate, ExecutionCertificateInput,
    ExecutionProviderKind, ExpectedOutcome, NativeIoAdapterFidelityReport, NativeIoCertificate,
    NativeIoMaterializationBoundaryReport, NativeIoRepresentationTransition,
    NativeIoSideEffectReport, NativeIoSinkRequirementReport, NativeIoSourceCapabilityReport,
    NativeIoSourcePushdownReport, PredicateExpr, RepresentationState, Result, ShardLoomError,
    StatValue,
};
use shardloom_plan::ProjectionRequest;

#[cfg(feature = "vortex-local-primitives")]
use crate::{
    VortexEncodedValuePredicateBatch, VortexReaderGeneratedEncodedKernelInput,
    plan_vortex_reader_generated_prepared_batch_envelopes,
    plan_vortex_reader_generated_prepared_batch_kernel_inputs,
};
use crate::{
    VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest, VortexReaderBackedSplitEvidence,
    VortexReaderGeneratedPreparedBatchReport,
};

/// Feature-gated local Vortex primitive execution status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLocalPrimitiveExecutionStatus {
    FeatureDisabled,
    Executed,
    BlockedByUnsupportedInput,
    BlockedByUnsupportedPrimitive,
    BlockedByUnsupportedDType,
    Unsupported,
}
impl VortexLocalPrimitiveExecutionStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::Executed => "executed",
            Self::BlockedByUnsupportedInput => "blocked_by_unsupported_input",
            Self::BlockedByUnsupportedPrimitive => "blocked_by_unsupported_primitive",
            Self::BlockedByUnsupportedDType => "blocked_by_unsupported_dtype",
            Self::Unsupported => "unsupported",
        }
    }

    pub const fn is_error(&self) -> bool {
        matches!(
            self,
            Self::BlockedByUnsupportedInput
                | Self::BlockedByUnsupportedPrimitive
                | Self::BlockedByUnsupportedDType
                | Self::Unsupported
        )
    }
}

/// Execution mode used by the local Vortex primitive executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLocalPrimitiveExecutionMode {
    FeatureDisabled,
    MetadataPreservingCount,
    VortexArrayPrimitive,
    VortexScanPushdown,
    Unsupported,
}
impl VortexLocalPrimitiveExecutionMode {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::FeatureDisabled => "feature_disabled",
            Self::MetadataPreservingCount => "metadata_preserving_count",
            Self::VortexArrayPrimitive => "vortex_array_primitive",
            Self::VortexScanPushdown => "vortex_scan_pushdown",
            Self::Unsupported => "unsupported",
        }
    }
}

/// Report emitted by the narrow local Vortex primitive executor.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexLocalPrimitiveExecutionReport {
    pub status: VortexLocalPrimitiveExecutionStatus,
    pub mode: VortexLocalPrimitiveExecutionMode,
    pub primitive_kind: VortexQueryPrimitiveKind,
    pub result_summary: Option<String>,
    pub rows_scanned: u64,
    pub rows_selected: Option<u64>,
    pub rows_projected: Option<u64>,
    pub projected_columns: Vec<String>,
    pub arrays_read_count: usize,
    pub reader_splits: Vec<VortexReaderBackedSplitEvidence>,
    pub reader_generated_prepared_batch_report: Option<VortexReaderGeneratedPreparedBatchReport>,
    pub max_chunk_rows: usize,
    pub streaming_scan_used: bool,
    pub full_stream_collected: bool,
    pub max_parallelism_requested: usize,
    pub scan_concurrency_per_worker: usize,
    pub filter_pushdown_applied: bool,
    pub projection_pushdown_applied: bool,
    pub upstream_filter_expression_used: bool,
    pub upstream_projection_expression_used: bool,
    pub source_order_limit_requested: Option<u64>,
    pub source_order_limit_applied: bool,
    pub source_order_limit_input_rows: Option<u64>,
    pub source_order_limit_rows_output: Option<u64>,
    pub data_read: bool,
    pub data_decoded: bool,
    pub data_materialized: bool,
    pub upstream_scan_called: bool,
    pub row_read: bool,
    pub arrow_converted: bool,
    pub object_store_io: bool,
    pub write_io: bool,
    pub spill_io_performed: bool,
    pub external_effects_executed: bool,
    pub fallback_execution_allowed: bool,
    pub materialization_boundary_reported: bool,
    pub diagnostics: Vec<Diagnostic>,
}
impl VortexLocalPrimitiveExecutionReport {
    pub fn feature_disabled(primitive_kind: VortexQueryPrimitiveKind) -> Self {
        Self {
            status: VortexLocalPrimitiveExecutionStatus::FeatureDisabled,
            mode: VortexLocalPrimitiveExecutionMode::FeatureDisabled,
            primitive_kind,
            result_summary: None,
            rows_scanned: 0,
            rows_selected: None,
            rows_projected: None,
            projected_columns: Vec::new(),
            arrays_read_count: 0,
            reader_splits: Vec::new(),
            reader_generated_prepared_batch_report: None,
            max_chunk_rows: 0,
            streaming_scan_used: false,
            full_stream_collected: false,
            max_parallelism_requested: 1,
            scan_concurrency_per_worker: 1,
            filter_pushdown_applied: false,
            projection_pushdown_applied: false,
            upstream_filter_expression_used: false,
            upstream_projection_expression_used: false,
            source_order_limit_requested: None,
            source_order_limit_applied: false,
            source_order_limit_input_rows: None,
            source_order_limit_rows_output: None,
            data_read: false,
            data_decoded: false,
            data_materialized: false,
            upstream_scan_called: false,
            row_read: false,
            arrow_converted: false,
            object_store_io: false,
            write_io: false,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_execution_allowed: false,
            materialization_boundary_reported: false,
            diagnostics: Vec::new(),
        }
    }

    #[cfg(feature = "vortex-local-primitives")]
    fn blocked(
        primitive_kind: VortexQueryPrimitiveKind,
        status: VortexLocalPrimitiveExecutionStatus,
        diagnostic: Diagnostic,
    ) -> Self {
        let mut out = Self::feature_disabled(primitive_kind);
        out.status = status;
        out.mode = VortexLocalPrimitiveExecutionMode::Unsupported;
        out.diagnostics.push(diagnostic);
        out
    }

    pub const fn has_errors(&self) -> bool {
        self.status.is_error()
    }

    pub fn to_human_text(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "local primitive status: {}", self.status.as_str());
        let _ = writeln!(out, "local primitive mode: {}", self.mode.as_str());
        let _ = writeln!(out, "primitive kind: {}", self.primitive_kind.as_str());
        if let Some(summary) = &self.result_summary {
            let _ = writeln!(out, "result summary: {summary}");
        }
        let _ = writeln!(out, "rows scanned: {}", self.rows_scanned);
        let _ = writeln!(
            out,
            "rows selected: {}",
            self.rows_selected
                .map_or_else(|| "none".to_string(), |value| value.to_string())
        );
        let _ = writeln!(
            out,
            "rows projected: {}",
            self.rows_projected
                .map_or_else(|| "none".to_string(), |value| value.to_string())
        );
        let _ = writeln!(
            out,
            "projected columns: {}",
            self.projected_columns.join(",")
        );
        let _ = writeln!(out, "arrays read count: {}", self.arrays_read_count);
        let _ = writeln!(
            out,
            "reader split evidence count: {}",
            self.reader_splits.len()
        );
        let _ = writeln!(
            out,
            "reader-generated prepared batch report: {}",
            self.reader_generated_prepared_batch_report
                .as_ref()
                .map_or("none", |report| report.status.as_str())
        );
        let _ = writeln!(
            out,
            "reader-generated kernel input available: {}",
            self.reader_generated_prepared_batch_report
                .as_ref()
                .is_some_and(|report| report.encoded_value_batch_available
                    || report.encoded_projection_batch_available)
        );
        let _ = writeln!(out, "max chunk rows: {}", self.max_chunk_rows);
        let _ = writeln!(out, "streaming scan used: {}", self.streaming_scan_used);
        let _ = writeln!(out, "full stream collected: {}", self.full_stream_collected);
        let _ = writeln!(
            out,
            "max parallelism requested: {}",
            self.max_parallelism_requested
        );
        let _ = writeln!(
            out,
            "scan concurrency per worker: {}",
            self.scan_concurrency_per_worker
        );
        let _ = writeln!(
            out,
            "filter pushdown applied: {}",
            self.filter_pushdown_applied
        );
        let _ = writeln!(
            out,
            "projection pushdown applied: {}",
            self.projection_pushdown_applied
        );
        self.write_source_order_limit_text(&mut out);
        let _ = writeln!(out, "data read: {}", self.data_read);
        let _ = writeln!(out, "data decoded: {}", self.data_decoded);
        let _ = writeln!(out, "data materialized: {}", self.data_materialized);
        let _ = writeln!(out, "upstream scan called: {}", self.upstream_scan_called);
        let _ = writeln!(out, "row read: {}", self.row_read);
        let _ = writeln!(out, "Arrow converted: {}", self.arrow_converted);
        let _ = writeln!(
            out,
            "materialization boundary reported: {}",
            self.materialization_boundary_reported
        );
        let _ = writeln!(out, "fallback execution disabled");
        out
    }

    fn write_source_order_limit_text(&self, out: &mut String) {
        let _ = writeln!(
            out,
            "source-order limit requested: {}",
            self.source_order_limit_requested
                .map_or_else(|| "none".to_string(), |value| value.to_string())
        );
        let _ = writeln!(
            out,
            "source-order residual limit applied: {}",
            self.source_order_limit_applied
        );
        let _ = writeln!(
            out,
            "source-order limit input rows: {}",
            self.source_order_limit_input_rows
                .map_or_else(|| "none".to_string(), |value| value.to_string())
        );
        let _ = writeln!(
            out,
            "source-order limit output rows: {}",
            self.source_order_limit_rows_output
                .map_or_else(|| "none".to_string(), |value| value.to_string())
        );
    }
}

/// Bounded execution policy applied to local Vortex primitive scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VortexLocalPrimitiveExecutionPolicy {
    pub max_parallelism: usize,
}
impl VortexLocalPrimitiveExecutionPolicy {
    /// # Errors
    /// Returns an error when max parallelism is zero.
    pub fn new(max_parallelism: usize) -> Result<Self> {
        if max_parallelism == 0 {
            return Err(ShardLoomError::InvalidOperation(
                "max_parallelism must be >= 1".to_string(),
            ));
        }
        Ok(Self { max_parallelism })
    }

    pub const fn single_threaded() -> Self {
        Self { max_parallelism: 1 }
    }

    pub const fn scan_concurrency_per_worker(&self) -> usize {
        self.max_parallelism
    }
}

/// Compatibility row-output format for a scoped local Vortex primitive export.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VortexLocalPrimitiveRowExportFormat {
    Jsonl,
    Csv,
}
impl VortexLocalPrimitiveRowExportFormat {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Jsonl => "jsonl",
            Self::Csv => "csv",
        }
    }
}

/// Materializing compatibility export report for supported local Vortex
/// primitive row streams.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexLocalPrimitiveRowExportReport {
    pub status: VortexLocalPrimitiveExecutionStatus,
    pub primitive_kind: VortexQueryPrimitiveKind,
    pub output_path: String,
    pub output_format: &'static str,
    pub rows_scanned: u64,
    pub rows_written: u64,
    pub pre_limit_result_row_count: u64,
    pub projected_columns: Vec<String>,
    pub arrays_read_count: usize,
    pub max_chunk_rows: usize,
    pub max_parallelism_requested: usize,
    pub scan_concurrency_per_worker: usize,
    pub source_order_limit_requested: Option<u64>,
    pub evidence: VortexLocalPrimitiveRowExportEvidence,
    pub diagnostics: Vec<Diagnostic>,
}

/// Pushdown evidence for a local primitive row export.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VortexLocalPrimitiveRowExportPushdownEvidence {
    pub filter_pushdown_applied: bool,
    pub projection_pushdown_applied: bool,
    pub source_order_limit_applied: bool,
}

/// Runtime evidence for a local primitive row export.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VortexLocalPrimitiveRowExportEvidence {
    pub pushdown: VortexLocalPrimitiveRowExportPushdownEvidence,
    pub side_effects: NativeIoSideEffectReport,
    pub upstream_scan_called: bool,
    pub materialization_boundary_reported: bool,
}

fn disabled_row_export_evidence() -> VortexLocalPrimitiveRowExportEvidence {
    VortexLocalPrimitiveRowExportEvidence {
        pushdown: VortexLocalPrimitiveRowExportPushdownEvidence {
            filter_pushdown_applied: false,
            projection_pushdown_applied: false,
            source_order_limit_applied: false,
        },
        side_effects: NativeIoSideEffectReport {
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
        },
        upstream_scan_called: false,
        materialization_boundary_reported: false,
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn executed_row_export_evidence(
    filter_pushdown_applied: bool,
    projection_pushdown_applied: bool,
    source_order_limit_applied: bool,
) -> VortexLocalPrimitiveRowExportEvidence {
    VortexLocalPrimitiveRowExportEvidence {
        pushdown: VortexLocalPrimitiveRowExportPushdownEvidence {
            filter_pushdown_applied,
            projection_pushdown_applied,
            source_order_limit_applied,
        },
        side_effects: NativeIoSideEffectReport {
            data_read: true,
            data_decoded: true,
            data_materialized: true,
            row_read: true,
            arrow_converted: false,
            object_store_io: false,
            write_io: true,
            spill_io_performed: false,
            external_effects_executed: false,
            fallback_attempted: false,
            fallback_execution_allowed: false,
        },
        upstream_scan_called: true,
        materialization_boundary_reported: true,
    }
}

impl VortexLocalPrimitiveRowExportReport {
    #[must_use]
    pub fn feature_disabled(
        primitive_kind: VortexQueryPrimitiveKind,
        output_path: &std::path::Path,
        output_format: VortexLocalPrimitiveRowExportFormat,
    ) -> Self {
        Self {
            status: VortexLocalPrimitiveExecutionStatus::FeatureDisabled,
            primitive_kind,
            output_path: output_path.display().to_string(),
            output_format: output_format.as_str(),
            rows_scanned: 0,
            rows_written: 0,
            pre_limit_result_row_count: 0,
            projected_columns: Vec::new(),
            arrays_read_count: 0,
            max_chunk_rows: 0,
            max_parallelism_requested: 1,
            scan_concurrency_per_worker: 1,
            source_order_limit_requested: None,
            evidence: disabled_row_export_evidence(),
            diagnostics: vec![Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "vortex_local_primitive_row_export",
                "local Vortex primitive row export requires the vortex-local-primitives feature",
                Some("Fallback attempted: false".to_string()),
            )],
        }
    }

    #[cfg(feature = "vortex-local-primitives")]
    fn blocked(
        primitive_kind: VortexQueryPrimitiveKind,
        output_path: &std::path::Path,
        output_format: VortexLocalPrimitiveRowExportFormat,
        diagnostic: Diagnostic,
    ) -> Self {
        let mut out = Self::feature_disabled(primitive_kind, output_path, output_format);
        out.status = VortexLocalPrimitiveExecutionStatus::BlockedByUnsupportedPrimitive;
        out.diagnostics.clear();
        out.diagnostics.push(diagnostic);
        out
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.status.is_error()
            || self.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
                )
            })
    }
}

/// Builds a CG-16 execution certificate for a completed local `.vortex`
/// primitive report.
///
/// The certificate is intentionally narrow: it verifies one local primitive
/// report against one correctness fixture, records pushdown/scan side effects,
/// and blocks certification whenever decode, materialization, row reads, Arrow
/// conversion, object-store IO, writes, spill, external effects, diagnostics, or
/// fallback evidence appear.
///
/// # Errors
/// Returns an error if the certificate input cannot be constructed.
pub fn local_primitive_execution_certificate(
    fixture: &CorrectnessFixture,
    request: &VortexQueryPrimitiveRequest,
    report: &VortexLocalPrimitiveExecutionReport,
) -> Result<ExecutionCertificate> {
    let certificate_id = format!(
        "{}.{}.execution-certificate",
        fixture.id.as_str(),
        report.primitive_kind.as_str()
    );
    let execution_kind = format!("vortex.local_primitive.{}", report.primitive_kind.as_str());
    let mut input = ExecutionCertificateInput::new(certificate_id, execution_kind)?;
    input.execution_provider_kind = ExecutionProviderKind::VortexScan;
    input.provider_crate = Some("vortex".to_string());
    input.provider_version = Some(crate::UPSTREAM_VORTEX_PROVIDER_VERSION.to_string());
    input.provider_api_surface = Some("VortexFile::scan.into_array_iter".to_string());
    input.shardloom_admission_policy = Some("shardloom.vortex.local_scan_primitive.v1".to_string());
    input.plan_ref = Some(format!("vortex-run:{}", report.primitive_kind.as_str()));
    input.input_ref = request
        .source_uri
        .as_ref()
        .map(|uri| uri.as_str().to_string())
        .or_else(|| fixture.source_ref.clone());
    input.output_ref = local_primitive_output_ref(report);
    input.correctness_fixture_id = Some(fixture.id.as_str().to_string());
    input.expected_outcome = Some(fixture.expected.clone());
    input.actual_outcome = local_primitive_actual_outcome(report, &fixture.expected);
    input.selected_segment_count = 0;
    input.skipped_segment_count = 0;
    input.side_effects_performed = local_primitive_side_effects(report);
    input.data_read = report.data_read;
    input.data_decoded = report.data_decoded;
    input.data_materialized = report.data_materialized;
    input.row_read = report.row_read;
    input.arrow_converted = report.arrow_converted;
    input.object_store_io = report.object_store_io;
    input.write_io = report.write_io;
    input.spill_io_performed = report.spill_io_performed;
    input.external_effects_executed = report.external_effects_executed;
    input.fallback_attempted = request
        .diagnostics
        .iter()
        .chain(report.diagnostics.iter())
        .any(|diagnostic| diagnostic.fallback.attempted);
    input.fallback_execution_allowed = report.fallback_execution_allowed;
    input.unsafe_effect_detected =
        request.kind != report.primitive_kind || local_primitive_unsafe_effect_detected(report);
    input.correctness_passed =
        local_primitive_correctness_passed(fixture, request, report, input.actual_outcome.as_ref());
    input.diagnostics.extend(request.diagnostics.clone());
    input.diagnostics.extend(report.diagnostics.clone());
    Ok(ExecutionCertificate::evaluate(input))
}

/// Finds the checked-in correctness fixture that exactly matches a local
/// primitive request/report pair.
///
/// This intentionally returns `None` for copied/non-fixture local `.vortex`
/// targets. Those paths can execute and emit Native I/O evidence, but they
/// remain uncertified until CG-5 fixture/reference coverage is widened.
#[must_use]
pub fn local_primitive_correctness_fixture_for_request(
    request: &VortexQueryPrimitiveRequest,
    report: &VortexLocalPrimitiveExecutionReport,
) -> Option<CorrectnessFixture> {
    if request.kind != report.primitive_kind
        || report.status != VortexLocalPrimitiveExecutionStatus::Executed
        || report.has_errors()
    {
        return None;
    }
    match request.kind {
        VortexQueryPrimitiveKind::CountAll => request
            .source_uri
            .as_ref()
            .and_then(local_encoded_count_correctness_fixture_for_target),
        VortexQueryPrimitiveKind::CountWhere => local_primitive_fixture_if(
            request,
            local_struct_value_gte_three_predicate(request)
                && local_struct_no_source_order_limit(request),
            "vortex-local-count-where-struct-five",
        ),
        VortexQueryPrimitiveKind::ProjectColumns => local_primitive_fixture_if(
            request,
            local_struct_metric_projection(request) && local_struct_no_source_order_limit(request),
            "vortex-local-project-struct-five",
        ),
        VortexQueryPrimitiveKind::FilterPredicate => local_primitive_fixture_if(
            request,
            local_struct_value_gte_three_predicate(request)
                && local_struct_no_source_order_limit(request),
            "vortex-local-filter-struct-five",
        ),
        VortexQueryPrimitiveKind::FilterAndProject => {
            if local_struct_value_gte_three_predicate(request)
                && local_struct_metric_projection(request)
                && local_struct_source_order_limit(request, 2)
            {
                local_primitive_fixture_if(
                    request,
                    true,
                    "vortex-local-filter-project-limit-struct-five",
                )
            } else {
                local_primitive_fixture_if(
                    request,
                    local_struct_value_gte_three_predicate(request)
                        && local_struct_metric_projection(request)
                        && local_struct_no_source_order_limit(request),
                    "vortex-local-filter-project-struct-five",
                )
            }
        }
        VortexQueryPrimitiveKind::DistinctRows
        | VortexQueryPrimitiveKind::TailRows
        | VortexQueryPrimitiveKind::SampleRows
        | VortexQueryPrimitiveKind::SimpleAggregate
        | VortexQueryPrimitiveKind::Unsupported => None,
    }
}

fn local_encoded_count_correctness_fixture_for_target(
    target_uri: &DatasetUri,
) -> Option<CorrectnessFixture> {
    CorrectnessValidationPlan::default_foundation_plan()
        .fixtures
        .into_iter()
        .find(|fixture| {
            matches!(fixture.expected, ExpectedOutcome::EncodedCount { .. })
                && fixture
                    .source_ref
                    .as_deref()
                    .is_some_and(|source_ref| local_fixture_ref_matches(target_uri, source_ref))
        })
}

fn local_foundation_fixture_for_target(
    target_uri: &DatasetUri,
    fixture_id: &str,
) -> Option<CorrectnessFixture> {
    CorrectnessValidationPlan::default_foundation_plan()
        .fixtures
        .into_iter()
        .find(|fixture| {
            fixture.id.as_str() == fixture_id
                && fixture
                    .source_ref
                    .as_deref()
                    .is_some_and(|source_ref| local_fixture_ref_matches(target_uri, source_ref))
        })
}

fn local_primitive_fixture_if(
    request: &VortexQueryPrimitiveRequest,
    matches_fixture_shape: bool,
    fixture_id: &str,
) -> Option<CorrectnessFixture> {
    matches_fixture_shape
        .then_some(request.source_uri.as_ref())
        .flatten()
        .and_then(|source_uri| local_foundation_fixture_for_target(source_uri, fixture_id))
}

fn local_struct_value_gte_three_predicate(request: &VortexQueryPrimitiveRequest) -> bool {
    matches!(
        request.predicate.as_ref(),
        Some(PredicateExpr::Compare {
            column,
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(3)
        }) if column.as_str() == "value"
    )
}

fn local_struct_metric_projection(request: &VortexQueryPrimitiveRequest) -> bool {
    matches!(
        &request.projection,
        ProjectionRequest::Columns(columns)
            if columns.len() == 1 && columns[0].as_str() == "metric"
    )
}

fn local_struct_source_order_limit(request: &VortexQueryPrimitiveRequest, limit: usize) -> bool {
    request.source_order_limit == Some(limit)
}

fn local_struct_no_source_order_limit(request: &VortexQueryPrimitiveRequest) -> bool {
    request.source_order_limit.is_none()
}

fn local_fixture_ref_matches(target_uri: &DatasetUri, source_ref: &str) -> bool {
    let Some(target_ref) = canonical_local_fixture_ref(target_uri.as_str()) else {
        return false;
    };
    let Some(workspace_source_ref) = canonical_workspace_fixture_ref(source_ref) else {
        return false;
    };
    target_ref == workspace_source_ref
}

fn canonical_workspace_fixture_ref(source_ref: &str) -> Option<String> {
    let source_ref = normalized_local_fixture_ref(source_ref);
    let source_path = std::path::Path::new(&source_ref);
    let absolute = if source_path.is_absolute() {
        source_path.to_path_buf()
    } else {
        workspace_root().join(source_path)
    };
    canonical_path_string(&absolute)
}

fn canonical_local_fixture_ref(value: &str) -> Option<String> {
    let target_ref = normalized_local_fixture_ref(value);
    let target_path = std::path::Path::new(&target_ref);
    let absolute = if target_path.is_absolute() {
        target_path.to_path_buf()
    } else {
        workspace_root().join(target_path)
    };
    canonical_path_string(&absolute)
}

fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .to_path_buf()
}

fn canonical_path_string(path: &std::path::Path) -> Option<String> {
    path.canonicalize()
        .ok()
        .map(|path| path.to_string_lossy().replace('\\', "/"))
}

fn normalized_local_fixture_ref(value: &str) -> String {
    let without_fragment = value
        .split_once(['?', '#'])
        .map_or(value, |(prefix, _)| prefix);
    let without_scheme = without_fragment
        .strip_prefix("file:///")
        .or_else(|| without_fragment.strip_prefix("file://"))
        .unwrap_or(without_fragment);
    without_scheme
        .replace('\\', "/")
        .trim_start_matches("./")
        .to_string()
}

/// Builds a CG-19 runtime native I/O certificate for an executed local
/// `.vortex` primitive path.
///
/// This certificate wraps the already-executed local primitive report with
/// source capability, pushdown, representation, sink, fidelity, side-effect,
/// and no-fallback evidence. It does not execute a scan itself, add adapter
/// behavior, decode values, materialize rows, convert to Arrow, touch object
/// stores, write outputs, spill data, or permit fallback execution.
///
/// # Errors
/// Returns an error if the native I/O certificate input is invalid.
pub fn local_primitive_native_io_certificate(
    request: &VortexQueryPrimitiveRequest,
    report: &VortexLocalPrimitiveExecutionReport,
) -> Result<NativeIoCertificate> {
    let safe = local_primitive_native_io_safe(request, report);
    let diagnostics = local_primitive_native_io_diagnostics(safe, request, report);
    let mut certificate = NativeIoCertificate::new(
        format!(
            "cg19.local_primitive.{}.native_io",
            report.primitive_kind.as_str()
        ),
        local_primitive_native_io_path_id(report),
        local_primitive_source_capability_report(safe, request, report),
        local_primitive_source_pushdown_report(safe, request, report),
        local_primitive_representation_transitions(safe, report),
        local_primitive_sink_requirement_report(safe, report),
        local_primitive_adapter_fidelity_report(safe, report),
        local_primitive_materialization_boundaries(report),
        local_primitive_native_io_side_effect_report(report, &diagnostics),
        diagnostics,
    )?;
    certificate.fallback_attempted = certificate
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.fallback.attempted);
    Ok(certificate)
}

fn local_primitive_native_io_path_id(report: &VortexLocalPrimitiveExecutionReport) -> &'static str {
    match report.primitive_kind {
        VortexQueryPrimitiveKind::CountAll | VortexQueryPrimitiveKind::CountWhere => {
            "native_vortex_source_to_scalar_count_result"
        }
        VortexQueryPrimitiveKind::FilterPredicate => "native_vortex_source_to_filtered_result",
        VortexQueryPrimitiveKind::ProjectColumns => "native_vortex_source_to_projected_result",
        VortexQueryPrimitiveKind::FilterAndProject => {
            "native_vortex_source_to_filtered_projected_result"
        }
        VortexQueryPrimitiveKind::DistinctRows => {
            "native_vortex_source_to_distinct_materialized_result"
        }
        VortexQueryPrimitiveKind::TailRows => "native_vortex_source_to_tail_source_order_result",
        VortexQueryPrimitiveKind::SampleRows => "native_vortex_source_to_sample_seeded_result",
        VortexQueryPrimitiveKind::SimpleAggregate | VortexQueryPrimitiveKind::Unsupported => {
            "native_vortex_source_to_unsupported_result"
        }
    }
}

fn local_primitive_native_io_safe(
    request: &VortexQueryPrimitiveRequest,
    report: &VortexLocalPrimitiveExecutionReport,
) -> bool {
    request.kind == report.primitive_kind
        && request.diagnostics.iter().all(|diagnostic| {
            !matches!(
                diagnostic.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            ) && !diagnostic.fallback.attempted
        })
        && report.status == VortexLocalPrimitiveExecutionStatus::Executed
        && report.upstream_scan_called
        && report.data_read
        && report.streaming_scan_used
        && (!report.full_stream_collected || local_primitive_materialization_declared(report))
        && local_primitive_row_count(report).is_some()
        && local_primitive_pushdown_evidence_is_sufficient(report)
        && !local_primitive_unsafe_effect_detected(report)
}

fn local_primitive_pushdown_evidence_is_sufficient(
    report: &VortexLocalPrimitiveExecutionReport,
) -> bool {
    match report.primitive_kind {
        VortexQueryPrimitiveKind::CountAll => true,
        VortexQueryPrimitiveKind::CountWhere | VortexQueryPrimitiveKind::FilterPredicate => {
            report.filter_pushdown_applied && report.upstream_filter_expression_used
        }
        VortexQueryPrimitiveKind::ProjectColumns => {
            local_primitive_projection_evidence_is_sufficient(report)
        }
        VortexQueryPrimitiveKind::FilterAndProject => {
            report.filter_pushdown_applied
                && report.upstream_filter_expression_used
                && local_primitive_projection_evidence_is_sufficient(report)
        }
        VortexQueryPrimitiveKind::DistinctRows
        | VortexQueryPrimitiveKind::TailRows
        | VortexQueryPrimitiveKind::SampleRows => {
            local_primitive_materialization_declared(report)
                && local_primitive_row_count(report).is_some()
        }
        VortexQueryPrimitiveKind::SimpleAggregate | VortexQueryPrimitiveKind::Unsupported => false,
    }
}

fn local_primitive_projection_evidence_is_sufficient(
    report: &VortexLocalPrimitiveExecutionReport,
) -> bool {
    !report.projected_columns.is_empty()
        && ((report.projection_pushdown_applied && report.upstream_projection_expression_used)
            || local_primitive_projection_passthrough_evidence(report))
}

fn local_primitive_projection_passthrough_evidence(
    report: &VortexLocalPrimitiveExecutionReport,
) -> bool {
    report.projected_columns.len() == 1
        && report.projected_columns[0] == "value"
        && !report.projection_pushdown_applied
        && !report.upstream_projection_expression_used
}

fn local_primitive_native_io_diagnostics(
    safe: bool,
    request: &VortexQueryPrimitiveRequest,
    report: &VortexLocalPrimitiveExecutionReport,
) -> Vec<Diagnostic> {
    let mut diagnostics = request.diagnostics.clone();
    diagnostics.extend(report.diagnostics.clone());
    if !safe {
        diagnostics.push(Diagnostic::unsupported(
            DiagnosticCode::NoFallbackExecution,
            "vortex_local_primitive_native_io_certificate",
            "local primitive native I/O certificate requires a successful local Vortex scan-pushdown report, declared materialization boundary when rows are decoded/materialized, and no Arrow conversion, object-store IO, writes, spill, external effects, or fallback",
            Some("Fallback attempted: false".to_string()),
        ));
    }
    diagnostics
}

fn local_primitive_source_capability_report(
    safe: bool,
    request: &VortexQueryPrimitiveRequest,
    report: &VortexLocalPrimitiveExecutionReport,
) -> NativeIoSourceCapabilityReport {
    NativeIoSourceCapabilityReport {
        source_kind: request
            .source_uri
            .as_ref()
            .filter(|uri| uri.looks_like_vortex())
            .map_or_else(|| "unknown".to_string(), |_| "vortex".to_string()),
        adapter_id: "shardloom.adapter.vortex.local_primitive.v1".to_string(),
        schema_discovery_status: if report.upstream_scan_called {
            "vortex_scan_schema_available".to_string()
        } else {
            "not_available".to_string()
        },
        statistics_availability: if report.rows_scanned > 0 {
            "row_count_available".to_string()
        } else {
            "unknown".to_string()
        },
        pushdown_capabilities: if safe {
            local_primitive_accepted_operations(report).join(",")
        } else {
            "none".to_string()
        },
        encoded_representation_preserved: safe,
        range_read_capability: false,
        streaming_capability: safe && report.streaming_scan_used,
        object_store_capability: false,
        fallback_attempted: false,
    }
}

fn local_primitive_source_pushdown_report(
    safe: bool,
    request: &VortexQueryPrimitiveRequest,
    report: &VortexLocalPrimitiveExecutionReport,
) -> NativeIoSourcePushdownReport {
    let rejected_operations = if safe {
        request
            .source_order_limit
            .map(|_| vec!["limit_pushdown".to_string()])
            .unwrap_or_default()
    } else {
        vec![report.primitive_kind.as_str().to_string()]
    };
    NativeIoSourcePushdownReport {
        accepted_operations: if safe {
            local_primitive_accepted_operations(report)
        } else {
            Vec::new()
        },
        rejected_operations,
        guarantee: if safe {
            local_primitive_pushdown_guarantee(report).to_string()
        } else {
            "unsupported".to_string()
        },
        proof_basis: if safe {
            local_primitive_pushdown_proof_basis(report).to_string()
        } else {
            "native I/O certificate blocked before accepting local primitive pushdown".to_string()
        },
        residual_expression: if safe {
            request
                .source_order_limit
                .map(|limit| format!("source_order_limit:{limit}"))
        } else {
            request.predicate.as_ref().map(PredicateExpr::summary)
        },
        conservative_false_positive_policy: false,
        unsafe_rejected_reason: (!safe)
            .then(|| "missing safe local primitive scan-pushdown evidence".to_string()),
        fallback_attempted: false,
    }
}

fn local_primitive_accepted_operations(
    report: &VortexLocalPrimitiveExecutionReport,
) -> Vec<String> {
    match report.primitive_kind {
        VortexQueryPrimitiveKind::CountAll => vec!["count_all".to_string()],
        VortexQueryPrimitiveKind::CountWhere => vec!["filter".to_string(), "count".to_string()],
        VortexQueryPrimitiveKind::FilterPredicate => vec!["filter".to_string()],
        VortexQueryPrimitiveKind::ProjectColumns => vec!["project".to_string()],
        VortexQueryPrimitiveKind::FilterAndProject => {
            vec!["filter".to_string(), "project".to_string()]
        }
        VortexQueryPrimitiveKind::DistinctRows => {
            let mut out = Vec::new();
            if report.filter_pushdown_applied {
                out.push("filter".to_string());
            }
            if !report.projected_columns.is_empty() {
                out.push("project".to_string());
            }
            out.push("distinct".to_string());
            out
        }
        VortexQueryPrimitiveKind::TailRows => {
            let mut out = Vec::new();
            if !report.projected_columns.is_empty() {
                out.push("project".to_string());
            }
            out.push("tail".to_string());
            out
        }
        VortexQueryPrimitiveKind::SampleRows => {
            let mut out = Vec::new();
            if report.filter_pushdown_applied {
                out.push("filter".to_string());
            }
            if !report.projected_columns.is_empty() {
                out.push("project".to_string());
            }
            out.push("sample".to_string());
            out
        }
        VortexQueryPrimitiveKind::SimpleAggregate | VortexQueryPrimitiveKind::Unsupported => {
            Vec::new()
        }
    }
}

fn local_primitive_pushdown_guarantee(
    report: &VortexLocalPrimitiveExecutionReport,
) -> &'static str {
    match report.primitive_kind {
        VortexQueryPrimitiveKind::CountAll => "exact_array_length_count",
        VortexQueryPrimitiveKind::CountWhere => "exact_filtered_count_from_vortex_scan_pushdown",
        VortexQueryPrimitiveKind::FilterPredicate => "exact_filter_from_vortex_scan_pushdown",
        VortexQueryPrimitiveKind::ProjectColumns => "exact_projection_from_vortex_scan_pushdown",
        VortexQueryPrimitiveKind::FilterAndProject => {
            if report.source_order_limit_applied {
                "exact_filter_project_from_single_vortex_scan_pushdown_with_shardloom_source_order_residual_limit"
            } else {
                "exact_filter_project_from_single_vortex_scan_pushdown"
            }
        }
        VortexQueryPrimitiveKind::DistinctRows => {
            "exact_distinct_from_vortex_scan_with_explicit_shardloom_row_key_materialization"
        }
        VortexQueryPrimitiveKind::TailRows => {
            "exact_source_order_tail_from_full_vortex_scan_with_explicit_shardloom_tail_boundary"
        }
        VortexQueryPrimitiveKind::SampleRows => {
            "exact_deterministic_sample_from_vortex_scan_with_explicit_shardloom_seeded_selection"
        }
        VortexQueryPrimitiveKind::SimpleAggregate | VortexQueryPrimitiveKind::Unsupported => {
            "unsupported"
        }
    }
}

fn local_primitive_pushdown_proof_basis(
    report: &VortexLocalPrimitiveExecutionReport,
) -> &'static str {
    match report.primitive_kind {
        VortexQueryPrimitiveKind::CountAll => {
            "local Vortex scan yielded arrays and ShardLoom counted array lengths without decoding or row materialization"
        }
        VortexQueryPrimitiveKind::CountWhere => {
            "local Vortex scan applied filter pushdown and ShardLoom counted selected array lengths without row reads"
        }
        VortexQueryPrimitiveKind::FilterPredicate => {
            "local Vortex scan applied filter pushdown without ShardLoom row reads or Arrow conversion"
        }
        VortexQueryPrimitiveKind::ProjectColumns => {
            "local Vortex scan applied projection pushdown or exact single-column passthrough without materialization"
        }
        VortexQueryPrimitiveKind::FilterAndProject => {
            if report.source_order_limit_applied {
                "local Vortex scan applied filter and projection pushdown in one scan, then ShardLoom applied a source-order residual limit without row reads"
            } else {
                "local Vortex scan applied filter and projection pushdown in one scan without row reads"
            }
        }
        VortexQueryPrimitiveKind::DistinctRows => {
            "local Vortex scan applied optional filter/projection pushdown, then ShardLoom materialized scoped primitive row keys for deterministic row-level distinct"
        }
        VortexQueryPrimitiveKind::TailRows => {
            "local Vortex scan applied projection pushdown, then ShardLoom retained the final source-order row window without invoking an external engine"
        }
        VortexQueryPrimitiveKind::SampleRows => {
            "local Vortex scan applied optional filter/projection pushdown, then ShardLoom applied deterministic seeded row selection without invoking an external engine"
        }
        VortexQueryPrimitiveKind::SimpleAggregate | VortexQueryPrimitiveKind::Unsupported => {
            "unsupported local primitive"
        }
    }
}

fn local_primitive_representation_transitions(
    safe: bool,
    report: &VortexLocalPrimitiveExecutionReport,
) -> Vec<NativeIoRepresentationTransition> {
    if !safe {
        return vec![NativeIoRepresentationTransition::new(
            RepresentationState::VortexEncoded,
            RepresentationState::Unsupported,
            false,
        )];
    }
    if local_primitive_materialization_declared(report) {
        return vec![
            NativeIoRepresentationTransition::new(
                RepresentationState::VortexEncoded,
                RepresentationState::DecodedColumnar,
                true,
            ),
            NativeIoRepresentationTransition::new(
                RepresentationState::DecodedColumnar,
                RepresentationState::MaterializedRows,
                true,
            ),
        ];
    }
    let to_state = match report.primitive_kind {
        VortexQueryPrimitiveKind::CountWhere
        | VortexQueryPrimitiveKind::FilterPredicate
        | VortexQueryPrimitiveKind::FilterAndProject => RepresentationState::SelectionVectorEncoded,
        VortexQueryPrimitiveKind::CountAll | VortexQueryPrimitiveKind::ProjectColumns => {
            RepresentationState::VortexEncoded
        }
        VortexQueryPrimitiveKind::DistinctRows
        | VortexQueryPrimitiveKind::TailRows
        | VortexQueryPrimitiveKind::SampleRows => RepresentationState::Unsupported,
        VortexQueryPrimitiveKind::SimpleAggregate | VortexQueryPrimitiveKind::Unsupported => {
            RepresentationState::Unsupported
        }
    };
    vec![NativeIoRepresentationTransition::new(
        RepresentationState::VortexEncoded,
        to_state,
        false,
    )]
}

fn local_primitive_sink_requirement_report(
    safe: bool,
    report: &VortexLocalPrimitiveExecutionReport,
) -> NativeIoSinkRequirementReport {
    let materialization_declared = local_primitive_materialization_declared(report);
    NativeIoSinkRequirementReport {
        target_format: local_primitive_sink_target_format(report).to_string(),
        accepts_encoded: safe && !materialization_declared,
        requires_decoded_columnar: materialization_declared,
        requires_rows: materialization_declared,
        preserves_metadata: !matches!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::CountAll | VortexQueryPrimitiveKind::CountWhere
        ),
        requires_ordering: false,
        requires_partitioning: false,
        requires_commit: false,
        supports_streaming: safe && report.streaming_scan_used && !materialization_declared,
        max_chunk_size: (report.max_chunk_rows > 0).then_some(report.max_chunk_rows as u64),
        backpressure_policy: "bounded_local_scan_chunks".to_string(),
    }
}

fn local_primitive_sink_target_format(
    report: &VortexLocalPrimitiveExecutionReport,
) -> &'static str {
    match report.primitive_kind {
        VortexQueryPrimitiveKind::CountAll | VortexQueryPrimitiveKind::CountWhere => {
            "scalar_count_result"
        }
        VortexQueryPrimitiveKind::FilterPredicate => "local_filtered_stream_summary",
        VortexQueryPrimitiveKind::ProjectColumns => "local_projected_stream_summary",
        VortexQueryPrimitiveKind::FilterAndProject => "local_filtered_projected_stream_summary",
        VortexQueryPrimitiveKind::DistinctRows => "local_distinct_row_summary",
        VortexQueryPrimitiveKind::TailRows => "local_tail_row_summary",
        VortexQueryPrimitiveKind::SampleRows => "local_sample_row_summary",
        VortexQueryPrimitiveKind::SimpleAggregate | VortexQueryPrimitiveKind::Unsupported => {
            "unsupported_result"
        }
    }
}

fn local_primitive_adapter_fidelity_report(
    safe: bool,
    report: &VortexLocalPrimitiveExecutionReport,
) -> NativeIoAdapterFidelityReport {
    let materialization_declared = local_primitive_materialization_declared(report);
    NativeIoAdapterFidelityReport {
        adapter_id: "shardloom.adapter.vortex.local_primitive.v1".to_string(),
        source_kind: "vortex".to_string(),
        sink_kind: local_primitive_sink_target_format(report).to_string(),
        metadata_preserved: safe && !materialization_declared,
        statistics_preserved: safe && !materialization_declared,
        encoded_representation_preserved: safe && !materialization_declared,
        materialization_required: materialization_declared,
        fidelity_loss: if safe && materialization_declared {
            "explicit_bounded_row_materialization".to_string()
        } else if safe {
            "none_for_local_primitive_summary".to_string()
        } else {
            "unsupported".to_string()
        },
        metadata_loss: if matches!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::CountAll | VortexQueryPrimitiveKind::CountWhere
        ) {
            "scalar_count_result_has_no_column_metadata".to_string()
        } else {
            "none".to_string()
        },
        fallback_attempted: false,
    }
}

fn local_primitive_materialization_declared(report: &VortexLocalPrimitiveExecutionReport) -> bool {
    report.materialization_boundary_reported
        && (report.data_decoded
            || report.data_materialized
            || report.row_read
            || report.full_stream_collected)
}

fn local_primitive_materialization_boundaries(
    report: &VortexLocalPrimitiveExecutionReport,
) -> Vec<NativeIoMaterializationBoundaryReport> {
    if !local_primitive_materialization_declared(report) {
        return Vec::new();
    }
    vec![NativeIoMaterializationBoundaryReport {
        boundary_id: format!(
            "local_primitive.{}.bounded_materialization",
            report.primitive_kind.as_str()
        ),
        from_state: RepresentationState::VortexEncoded,
        to_state: RepresentationState::MaterializedRows,
        required_by: report.primitive_kind.as_str().to_string(),
        reason:
            "explicit ShardLoom materialization boundary for scoped local Vortex primitive result"
                .to_string(),
        bytes_decoded: 0,
        rows_materialized: local_primitive_row_count(report).unwrap_or(0),
        fidelity_loss: "explicit_bounded_row_materialization".to_string(),
        fallback_attempted: false,
    }]
}

fn local_primitive_native_io_side_effect_report(
    report: &VortexLocalPrimitiveExecutionReport,
    diagnostics: &[Diagnostic],
) -> NativeIoSideEffectReport {
    NativeIoSideEffectReport {
        data_read: report.data_read,
        data_decoded: report.data_decoded,
        data_materialized: report.data_materialized,
        row_read: report.row_read,
        arrow_converted: report.arrow_converted,
        object_store_io: report.object_store_io,
        write_io: report.write_io,
        spill_io_performed: report.spill_io_performed,
        external_effects_executed: report.external_effects_executed,
        fallback_attempted: diagnostics
            .iter()
            .any(|diagnostic| diagnostic.fallback.attempted),
        fallback_execution_allowed: report.fallback_execution_allowed,
    }
}

fn local_primitive_output_ref(report: &VortexLocalPrimitiveExecutionReport) -> Option<String> {
    match report.primitive_kind {
        VortexQueryPrimitiveKind::CountAll => Some(format!("count_result={}", report.rows_scanned)),
        VortexQueryPrimitiveKind::CountWhere => report
            .rows_selected
            .map(|row_count| format!("count_result={row_count}")),
        VortexQueryPrimitiveKind::FilterPredicate => report
            .rows_selected
            .map(|rows| format!("rows_selected={rows}")),
        VortexQueryPrimitiveKind::ProjectColumns => report
            .rows_projected
            .map(|rows| format!("rows_projected={rows}")),
        VortexQueryPrimitiveKind::FilterAndProject => {
            local_primitive_row_count(report).map(|rows| {
                format!(
                    "rows_selected={rows};rows_projected={rows};projected_columns={};source_order_limit={}",
                    report.projected_columns.join(","),
                    report
                        .source_order_limit_requested
                        .map_or_else(|| "none".to_string(), |limit| limit.to_string())
                )
            })
        }
        VortexQueryPrimitiveKind::DistinctRows => {
            local_primitive_row_count(report).map(|rows| {
                format!(
                    "distinct_rows={rows};projected_columns={};source_order_limit={}",
                    report.projected_columns.join(","),
                    report
                        .source_order_limit_requested
                        .map_or_else(|| "none".to_string(), |limit| limit.to_string())
                )
            })
        }
        VortexQueryPrimitiveKind::TailRows => {
            local_primitive_row_count(report).map(|rows| {
                format!(
                    "tail_rows={rows};projected_columns={};source_order_limit={}",
                    report.projected_columns.join(","),
                    report
                        .source_order_limit_requested
                        .map_or_else(|| "none".to_string(), |limit| limit.to_string())
                )
            })
        }
        VortexQueryPrimitiveKind::SampleRows => {
            local_primitive_row_count(report).map(|rows| {
                format!(
                    "sample_rows={rows};projected_columns={};sample_size={}",
                    report.projected_columns.join(","),
                    report
                        .source_order_limit_requested
                        .map_or_else(|| "none".to_string(), |limit| limit.to_string())
                )
            })
        }
        VortexQueryPrimitiveKind::SimpleAggregate | VortexQueryPrimitiveKind::Unsupported => None,
    }
}

fn local_primitive_actual_outcome(
    report: &VortexLocalPrimitiveExecutionReport,
    expected: &ExpectedOutcome,
) -> Option<ExpectedOutcome> {
    let row_count = local_primitive_row_count(report)?;
    match expected {
        ExpectedOutcome::EncodedCount { .. } => {
            Some(ExpectedOutcome::EncodedCount { count: row_count })
        }
        ExpectedOutcome::Rows { .. } => Some(ExpectedOutcome::Rows {
            row_count: Some(row_count),
        }),
        ExpectedOutcome::NoSideEffects if !local_primitive_unsafe_effect_detected(report) => {
            Some(ExpectedOutcome::NoSideEffects)
        }
        _ => None,
    }
}

fn local_primitive_row_count(report: &VortexLocalPrimitiveExecutionReport) -> Option<u64> {
    match report.primitive_kind {
        VortexQueryPrimitiveKind::CountAll => Some(report.rows_scanned),
        VortexQueryPrimitiveKind::CountWhere | VortexQueryPrimitiveKind::FilterPredicate => {
            report.rows_selected
        }
        VortexQueryPrimitiveKind::ProjectColumns => report.rows_projected,
        VortexQueryPrimitiveKind::FilterAndProject
        | VortexQueryPrimitiveKind::DistinctRows
        | VortexQueryPrimitiveKind::TailRows
        | VortexQueryPrimitiveKind::SampleRows => {
            match (report.rows_selected, report.rows_projected) {
                (Some(selected), Some(projected)) if selected == projected => Some(selected),
                _ => None,
            }
        }
        VortexQueryPrimitiveKind::SimpleAggregate | VortexQueryPrimitiveKind::Unsupported => None,
    }
}

fn local_primitive_side_effects(report: &VortexLocalPrimitiveExecutionReport) -> Vec<String> {
    let mut effects = Vec::new();
    if report.upstream_scan_called {
        effects.push("local_vortex_scan".to_string());
    }
    if report.filter_pushdown_applied {
        effects.push("vortex_filter_pushdown".to_string());
    }
    if report.projection_pushdown_applied {
        effects.push("vortex_projection_pushdown".to_string());
    }
    if report.source_order_limit_applied {
        effects.push("shardloom_source_order_residual_limit".to_string());
    }
    if report.primitive_kind == VortexQueryPrimitiveKind::DistinctRows {
        effects.push("shardloom_distinct_row_key_materialization".to_string());
    }
    if report.primitive_kind == VortexQueryPrimitiveKind::TailRows {
        effects.push("shardloom_source_order_tail_window".to_string());
    }
    if report.primitive_kind == VortexQueryPrimitiveKind::SampleRows {
        effects.push("shardloom_deterministic_sample_selection".to_string());
    }
    effects
}

fn local_primitive_unsafe_effect_detected(report: &VortexLocalPrimitiveExecutionReport) -> bool {
    let materialization_declared = local_primitive_materialization_declared(report);
    report.has_errors()
        || report.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
        || (report.data_decoded && !materialization_declared)
        || (report.data_materialized && !materialization_declared)
        || (report.row_read && !materialization_declared)
        || (report.full_stream_collected && !materialization_declared)
        || report.arrow_converted
        || report.object_store_io
        || report.write_io
        || report.spill_io_performed
        || report.external_effects_executed
        || report.fallback_execution_allowed
}

fn local_primitive_correctness_passed(
    fixture: &CorrectnessFixture,
    request: &VortexQueryPrimitiveRequest,
    report: &VortexLocalPrimitiveExecutionReport,
    actual: Option<&ExpectedOutcome>,
) -> bool {
    let fixture_matches_request = local_primitive_correctness_fixture_for_request(request, report)
        .is_some_and(|matched| matched.id.as_str() == fixture.id.as_str());
    report.status == VortexLocalPrimitiveExecutionStatus::Executed
        && fixture_matches_request
        && request.kind == report.primitive_kind
        && Some(&fixture.expected) == actual
        && request.diagnostics.is_empty()
        && !local_primitive_unsafe_effect_detected(report)
}

/// Executes a narrow local Vortex query primitive when the feature gate is enabled.
///
/// The executor is intentionally limited to local `.vortex` files. `CountAll`
/// reads Vortex arrays and sums lengths without decoding or row materialization.
/// `CountWhere`, `FilterPredicate`, and `ProjectColumns` use upstream Vortex scan
/// filter/projection expressions for the currently supported local primitive
/// cases instead of hand-decoding fields after the scan.
///
/// # Errors
/// Returns an error only when internal report construction fails.
pub fn execute_vortex_local_primitive(
    request: &VortexQueryPrimitiveRequest,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    execute_vortex_local_primitive_with_policy(
        request,
        VortexLocalPrimitiveExecutionPolicy::single_threaded(),
    )
}

/// Executes a local Vortex query primitive using an explicit bounded scan policy.
///
/// # Errors
/// Returns an error only when internal report construction fails.
pub fn execute_vortex_local_primitive_with_policy(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    #[cfg(feature = "vortex-local-primitives")]
    {
        execute_vortex_local_primitive_enabled(request, policy)
    }
    #[cfg(not(feature = "vortex-local-primitives"))]
    {
        let _ = policy;
        Ok(VortexLocalPrimitiveExecutionReport::feature_disabled(
            request.kind,
        ))
    }
}

/// Executes a scoped local Vortex primitive and writes bounded rows to a
/// compatibility sink.
///
/// This is intentionally separate from `execute_vortex_local_primitive`: the
/// primitive report preserves zero-row-materialization evidence, while this
/// export reports an explicit decode/materialization/write boundary for JSONL
/// and CSV compatibility outputs.
///
/// # Errors
/// Returns an error when the local Vortex source, primitive, projected column
/// dtypes, output path, or output writer cannot satisfy the scoped export
/// contract.
pub fn execute_vortex_local_primitive_row_export_with_policy(
    request: &VortexQueryPrimitiveRequest,
    output_path: &std::path::Path,
    output_format: VortexLocalPrimitiveRowExportFormat,
    allow_overwrite: bool,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<VortexLocalPrimitiveRowExportReport> {
    #[cfg(feature = "vortex-local-primitives")]
    {
        execute_vortex_local_primitive_row_export_enabled(
            request,
            output_path,
            output_format,
            allow_overwrite,
            policy,
        )
    }
    #[cfg(not(feature = "vortex-local-primitives"))]
    {
        let _ = allow_overwrite;
        let _ = policy;
        Ok(VortexLocalPrimitiveRowExportReport::feature_disabled(
            request.kind,
            output_path,
            output_format,
        ))
    }
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::too_many_lines)]
fn execute_vortex_local_primitive_row_export_enabled(
    request: &VortexQueryPrimitiveRequest,
    output_path: &std::path::Path,
    output_format: VortexLocalPrimitiveRowExportFormat,
    allow_overwrite: bool,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<VortexLocalPrimitiveRowExportReport> {
    use std::io::Write as _;
    use vortex::VortexSessionDefault as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    if !matches!(
        request.kind,
        VortexQueryPrimitiveKind::FilterPredicate
            | VortexQueryPrimitiveKind::ProjectColumns
            | VortexQueryPrimitiveKind::FilterAndProject
            | VortexQueryPrimitiveKind::DistinctRows
            | VortexQueryPrimitiveKind::TailRows
            | VortexQueryPrimitiveKind::SampleRows
    ) {
        return Ok(VortexLocalPrimitiveRowExportReport::blocked(
            request.kind,
            output_path,
            output_format,
            Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "vortex_local_primitive_row_export",
                "local Vortex primitive row export supports filter, project, filter-project, distinct, tail, and sample row streams only",
                Some("use scalar collect for count primitives, or an admitted provider sink for provider-backed result summaries".to_string()),
            ),
        ));
    }
    let Some(uri) = request.source_uri.as_ref() else {
        return Ok(VortexLocalPrimitiveRowExportReport::blocked(
            request.kind,
            output_path,
            output_format,
            Diagnostic::invalid_input(
                "vortex_local_primitive_row_export",
                "local Vortex primitive row export requires a source URI",
                "provide a local `.vortex` source URI",
            ),
        ));
    };
    let Some(path) = local_vortex_path(uri, request.kind)? else {
        return Ok(VortexLocalPrimitiveRowExportReport::blocked(
            request.kind,
            output_path,
            output_format,
            Diagnostic::invalid_input(
                "vortex_local_primitive_row_export",
                format!(
                    "unsupported local Vortex row export target: {}",
                    uri.as_str()
                ),
                "provide an existing local path or file:// `.vortex` target",
            ),
        ));
    };

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(session.open_options().open_path(&path))
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to open local Vortex target for row export: {error}"
            ))
        })?;
    let source_row_count = file.row_count();
    let plan = row_export_scan_plan(request, file.dtype())?;
    let declared_columns = if plan.projected_columns.is_empty() {
        local_field_names(file.dtype(), request.kind)?
    } else {
        plan.projected_columns.clone()
    };

    let temp_path = temporary_output_path(output_path)?;
    prepare_output_target(output_path, &temp_path, allow_overwrite)?;
    let write_result = (|| -> Result<VortexLocalPrimitiveRowExportReport> {
        let mut output = std::fs::File::create_new(&temp_path).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local Vortex row export temp file {}: {error}",
                temp_path.display()
            ))
        })?;
        if output_format == VortexLocalPrimitiveRowExportFormat::Csv {
            write_csv_header(&mut output, &declared_columns)?;
        }

        let filter_pushdown_applied = plan.filter.is_some();
        let projection_pushdown_applied = plan.projection.is_some();
        let source_order_limit = plan.source_order_limit;
        let mut scan = file.scan().map_err(vortex_error)?;
        if let Some(filter) = plan.filter {
            scan = scan.with_filter(filter);
        }
        if let Some(projection) = plan.projection {
            scan = scan.with_projection(projection);
        }
        scan = scan.with_concurrency(policy.scan_concurrency_per_worker());

        let mut rows_written = 0usize;
        let mut pre_limit_result_row_count = 0usize;
        let mut arrays_read_count = 0usize;
        let mut max_chunk_rows = 0usize;
        let mut distinct_keys = std::collections::BTreeSet::new();
        let distinct_requested = request.kind == VortexQueryPrimitiveKind::DistinctRows;
        let tail_requested = request.kind == VortexQueryPrimitiveKind::TailRows;
        let sample_requested = request.kind == VortexQueryPrimitiveKind::SampleRows;
        let mut tail_rows = std::collections::VecDeque::<Vec<StatValue>>::new();
        let mut sample_rows = Vec::<(u64, usize, Vec<StatValue>)>::new();
        let sample_seed = request.sample_seed.unwrap_or(0);
        let sample_fraction = normalized_sample_fraction(request.sample_fraction)?;
        if tail_requested && source_order_limit.is_none() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local Vortex {} row export requires a bounded row count",
                request.kind.as_str()
            )));
        }
        if sample_requested && source_order_limit.is_some() && sample_fraction.is_some() {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex sample row export accepts either sample size or sample fraction, not both"
                    .to_string(),
            ));
        }
        if sample_requested && source_order_limit.is_none() && sample_fraction.is_none() {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex sample row export requires a sample size or sample fraction"
                    .to_string(),
            ));
        }
        if source_order_limit == Some(0) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local Vortex {} row export row count must be >= 1",
                request.kind.as_str()
            )));
        }
        for chunk in scan.into_array_iter(&runtime).map_err(vortex_error)? {
            let chunk = chunk.map_err(vortex_error)?;
            let chunk_rows = chunk.len();
            let columns = row_export_columns_from_chunk(&chunk, &declared_columns)?;
            if distinct_requested {
                let selected_rows = distinct_row_indices(
                    &columns,
                    &mut distinct_keys,
                    source_order_limit.map(|limit| limit.saturating_sub(rows_written)),
                )?;
                pre_limit_result_row_count = distinct_keys.len();
                write_row_export_selected_rows(
                    &mut output,
                    output_format,
                    &declared_columns,
                    &columns,
                    &selected_rows,
                )?;
                rows_written = rows_written
                    .checked_add(selected_rows.len())
                    .ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "local Vortex row export row count overflowed usize".to_string(),
                        )
                    })?;
            } else if tail_requested {
                let materialized_rows = row_export_materialized_row_count(&columns, chunk_rows)?;
                pre_limit_result_row_count = pre_limit_result_row_count
                    .checked_add(materialized_rows)
                    .ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "local Vortex row export pre-limit row count overflowed usize"
                                .to_string(),
                        )
                    })?;
                let limit = source_order_limit.expect("tail row export checked bounded limit");
                for row_index in 0..materialized_rows {
                    tail_rows.push_back(row_export_materialized_row(&columns, row_index)?);
                    if tail_rows.len() > limit {
                        tail_rows.pop_front();
                    }
                }
            } else if sample_requested {
                let materialized_rows = row_export_materialized_row_count(&columns, chunk_rows)?;
                for local_index in 0..materialized_rows {
                    let row_index = pre_limit_result_row_count
                        .checked_add(local_index)
                        .ok_or_else(|| {
                            ShardLoomError::InvalidOperation(
                                "local Vortex sample row export ordinal overflowed usize"
                                    .to_string(),
                            )
                        })?;
                    let score = deterministic_sample_score(sample_seed, row_index);
                    let row = row_export_materialized_row(&columns, local_index)?;
                    if let Some(limit) = source_order_limit {
                        insert_sample_row_export_candidate(
                            &mut sample_rows,
                            limit,
                            score,
                            row_index,
                            row,
                        );
                    } else {
                        sample_rows.push((score, row_index, row));
                    }
                }
                pre_limit_result_row_count = pre_limit_result_row_count
                    .checked_add(materialized_rows)
                    .ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "local Vortex row export pre-limit row count overflowed usize"
                                .to_string(),
                        )
                    })?;
            } else {
                pre_limit_result_row_count = pre_limit_result_row_count
                    .checked_add(chunk_rows)
                    .ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "local Vortex row export pre-limit row count overflowed usize"
                                .to_string(),
                        )
                    })?;
                let output_rows = source_order_limit.map_or(chunk_rows, |limit| {
                    limit.saturating_sub(rows_written).min(chunk_rows)
                });
                write_row_export_chunk(
                    &mut output,
                    output_format,
                    &declared_columns,
                    &columns,
                    output_rows,
                )?;
                rows_written = rows_written.checked_add(output_rows).ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex row export row count overflowed usize".to_string(),
                    )
                })?;
            }
            max_chunk_rows = max_chunk_rows.max(chunk_rows);
            arrays_read_count += 1;
            if source_order_limit.is_some_and(|limit| rows_written >= limit) {
                break;
            }
        }
        if tail_requested {
            let selected_rows = tail_rows.into_iter().collect::<Vec<_>>();
            write_row_export_materialized_rows(
                &mut output,
                output_format,
                &declared_columns,
                &selected_rows,
            )?;
            rows_written = selected_rows.len();
        } else if sample_requested {
            let target_count = sample_target_count(request, pre_limit_result_row_count)?;
            truncate_sample_candidates_to_target(&mut sample_rows, target_count);
            let selected_rows = sample_rows
                .into_iter()
                .map(|(_score, _row_index, row)| row)
                .collect::<Vec<_>>();
            write_row_export_materialized_rows(
                &mut output,
                output_format,
                &declared_columns,
                &selected_rows,
            )?;
            rows_written = selected_rows.len();
        }
        output.flush().map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to flush local Vortex row export {}: {error}",
                temp_path.display()
            ))
        })?;
        drop(output);
        std::fs::rename(&temp_path, output_path).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to commit local Vortex row export {} -> {}: {error}",
                temp_path.display(),
                output_path.display()
            ))
        })?;

        Ok(VortexLocalPrimitiveRowExportReport {
            status: VortexLocalPrimitiveExecutionStatus::Executed,
            primitive_kind: request.kind,
            output_path: output_path.display().to_string(),
            output_format: output_format.as_str(),
            rows_scanned: source_row_count,
            rows_written: usize_to_u64(rows_written)?,
            pre_limit_result_row_count: usize_to_u64(pre_limit_result_row_count)?,
            projected_columns: declared_columns,
            arrays_read_count,
            max_chunk_rows,
            max_parallelism_requested: policy.max_parallelism,
            scan_concurrency_per_worker: policy.scan_concurrency_per_worker(),
            source_order_limit_requested: source_order_limit.map(usize_to_u64).transpose()?,
            evidence: executed_row_export_evidence(
                filter_pushdown_applied,
                projection_pushdown_applied,
                source_order_limit.is_some(),
            ),
            diagnostics: Vec::new(),
        })
    })();
    if write_result.is_err() {
        let _ = std::fs::remove_file(&temp_path);
    }
    write_result
}

#[cfg(feature = "vortex-local-primitives")]
fn row_export_scan_plan(
    request: &VortexQueryPrimitiveRequest,
    dtype: &vortex::array::dtype::DType,
) -> Result<LocalVortexScanPlan> {
    match request.kind {
        VortexQueryPrimitiveKind::FilterPredicate => {
            let Some(predicate) = request.predicate.as_ref() else {
                return Err(ShardLoomError::InvalidOperation(
                    "local Vortex row export filter primitive was missing its predicate"
                        .to_string(),
                ));
            };
            let mut plan = LocalVortexScanPlan::filter(predicate_to_vortex_expr(
                predicate,
                dtype,
                request.kind,
            )?);
            plan.source_order_limit = request.source_order_limit;
            Ok(plan)
        }
        VortexQueryPrimitiveKind::ProjectColumns | VortexQueryPrimitiveKind::TailRows => {
            let mut plan = projection_scan_plan(dtype, &request.projection, request.kind)?;
            plan.source_order_limit = request.source_order_limit;
            Ok(plan)
        }
        VortexQueryPrimitiveKind::FilterAndProject => {
            let Some(predicate) = request.predicate.as_ref() else {
                return Err(ShardLoomError::InvalidOperation(
                    "local Vortex row export filter-project primitive was missing its predicate"
                        .to_string(),
                ));
            };
            let mut plan = projection_scan_plan(dtype, &request.projection, request.kind)?;
            plan.filter = Some(predicate_to_vortex_expr(predicate, dtype, request.kind)?);
            plan.source_order_limit = request.source_order_limit;
            Ok(plan)
        }
        VortexQueryPrimitiveKind::DistinctRows | VortexQueryPrimitiveKind::SampleRows => {
            let mut plan = projection_scan_plan(dtype, &request.projection, request.kind)?;
            if let Some(predicate) = request.predicate.as_ref() {
                plan.filter = Some(predicate_to_vortex_expr(predicate, dtype, request.kind)?);
            }
            plan.source_order_limit = request.source_order_limit;
            Ok(plan)
        }
        VortexQueryPrimitiveKind::CountAll
        | VortexQueryPrimitiveKind::CountWhere
        | VortexQueryPrimitiveKind::SimpleAggregate
        | VortexQueryPrimitiveKind::Unsupported => Err(ShardLoomError::InvalidOperation(
            "local Vortex row export supports filter, project, filter-project, distinct, tail, and sample primitives only"
                .to_string(),
        )),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn temporary_output_path(output_path: &std::path::Path) -> Result<std::path::PathBuf> {
    let parent = output_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty());
    let file_name = output_path
        .file_name()
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "local Vortex row export output path has no file name: {}",
                output_path.display()
            ))
        })?
        .to_string_lossy();
    let temp_name = format!(".{file_name}.shardloom-tmp-{}", std::process::id());
    Ok(parent.map_or_else(
        || std::path::PathBuf::from(&temp_name),
        |path| path.join(&temp_name),
    ))
}

#[cfg(feature = "vortex-local-primitives")]
fn prepare_output_target(
    output_path: &std::path::Path,
    temp_path: &std::path::Path,
    allow_overwrite: bool,
) -> Result<()> {
    let workspace_root = shardloom_core::infer_local_output_workspace_root(output_path)?;
    shardloom_core::plan_workspace_safe_local_output(workspace_root, output_path, allow_overwrite)?;
    if output_path.exists() && !allow_overwrite {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local Vortex row export output already exists: {}; pass allow_overwrite to replace it; no fallback execution was attempted",
            output_path.display()
        )));
    }
    if temp_path.exists() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local Vortex row export temp path already exists: {}; remove stale temp file before retrying; no fallback execution was attempted",
            temp_path.display()
        )));
    }
    if let Some(parent) = output_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local Vortex row export directory {}: {error}",
                parent.display()
            ))
        })?;
    }
    Ok(())
}

#[cfg(feature = "vortex-local-primitives")]
fn write_csv_header(output: &mut std::fs::File, columns: &[String]) -> Result<()> {
    use std::io::Write as _;

    let header = columns
        .iter()
        .map(|column| csv_escape(column))
        .collect::<Vec<_>>()
        .join(",");
    writeln!(output, "{header}").map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to write local Vortex row export CSV header: {error}"
        ))
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn row_export_columns_from_chunk(
    chunk: &vortex::array::ArrayRef,
    declared_columns: &[String],
) -> Result<Vec<Vec<StatValue>>> {
    let mut out = Vec::with_capacity(declared_columns.len());
    if chunk.dtype().is_struct() {
        let children = chunk
            .named_children()
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();
        for column in declared_columns {
            let Some(array) = children.get(column.as_str()) else {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "local Vortex row export column '{column}' was not present in scanned chunk; no fallback execution was attempted"
                )));
            };
            out.push(row_export_values_from_vortex_array(column, array)?);
        }
    } else {
        let column = declared_columns.first().map_or("value", String::as_str);
        out.push(row_export_values_from_vortex_array(column, chunk)?);
    }
    let Some(first_len) = out.first().map(Vec::len) else {
        return Ok(out);
    };
    if out.iter().any(|values| values.len() != first_len) {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex row export scanned columns had mismatched row counts; no fallback execution was attempted"
                .to_string(),
        ));
    }
    Ok(out)
}

#[cfg(feature = "vortex-local-primitives")]
fn row_export_values_from_vortex_array(
    column: &str,
    array: &vortex::array::ArrayRef,
) -> Result<Vec<StatValue>> {
    stat_values_from_vortex_array(array).ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!(
            "local Vortex row export column '{column}' has unsupported dtype or nullable validity for scoped JSONL/CSV export; no fallback execution was attempted"
        ))
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn write_row_export_chunk(
    output: &mut std::fs::File,
    format: VortexLocalPrimitiveRowExportFormat,
    columns: &[String],
    column_values: &[Vec<StatValue>],
    rows: usize,
) -> Result<()> {
    let row_indices = (0..rows).collect::<Vec<_>>();
    write_row_export_selected_rows(output, format, columns, column_values, &row_indices)
}

#[cfg(feature = "vortex-local-primitives")]
fn write_row_export_selected_rows(
    output: &mut std::fs::File,
    format: VortexLocalPrimitiveRowExportFormat,
    columns: &[String],
    column_values: &[Vec<StatValue>],
    row_indices: &[usize],
) -> Result<()> {
    use std::io::Write as _;

    for &row_index in row_indices {
        match format {
            VortexLocalPrimitiveRowExportFormat::Jsonl => {
                let mut row = serde_json::Map::with_capacity(columns.len());
                for (column_index, column) in columns.iter().enumerate() {
                    row.insert(
                        column.clone(),
                        stat_value_to_json_value(&column_values[column_index][row_index])?,
                    );
                }
                let line = serde_json::Value::Object(row).to_string();
                writeln!(output, "{line}").map_err(|error| {
                    ShardLoomError::InvalidOperation(format!(
                        "failed to write local Vortex row export JSONL row: {error}"
                    ))
                })?;
            }
            VortexLocalPrimitiveRowExportFormat::Csv => {
                let line = column_values
                    .iter()
                    .map(|values| stat_value_to_csv_cell(&values[row_index]))
                    .collect::<Vec<_>>()
                    .join(",");
                writeln!(output, "{line}").map_err(|error| {
                    ShardLoomError::InvalidOperation(format!(
                        "failed to write local Vortex row export CSV row: {error}"
                    ))
                })?;
            }
        }
    }
    Ok(())
}

#[cfg(feature = "vortex-local-primitives")]
fn write_row_export_materialized_rows(
    output: &mut std::fs::File,
    format: VortexLocalPrimitiveRowExportFormat,
    columns: &[String],
    rows: &[Vec<StatValue>],
) -> Result<()> {
    use std::io::Write as _;

    for row_values in rows {
        if row_values.len() != columns.len() {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex row export materialized rows had mismatched column counts; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        match format {
            VortexLocalPrimitiveRowExportFormat::Jsonl => {
                let mut row = serde_json::Map::with_capacity(columns.len());
                for (column, value) in columns.iter().zip(row_values) {
                    row.insert(column.clone(), stat_value_to_json_value(value)?);
                }
                let line = serde_json::Value::Object(row).to_string();
                writeln!(output, "{line}").map_err(|error| {
                    ShardLoomError::InvalidOperation(format!(
                        "failed to write local Vortex row export JSONL row: {error}"
                    ))
                })?;
            }
            VortexLocalPrimitiveRowExportFormat::Csv => {
                let line = row_values
                    .iter()
                    .map(stat_value_to_csv_cell)
                    .collect::<Vec<_>>()
                    .join(",");
                writeln!(output, "{line}").map_err(|error| {
                    ShardLoomError::InvalidOperation(format!(
                        "failed to write local Vortex row export CSV row: {error}"
                    ))
                })?;
            }
        }
    }
    Ok(())
}

#[cfg(feature = "vortex-local-primitives")]
fn row_export_materialized_row_count(
    column_values: &[Vec<StatValue>],
    fallback_rows: usize,
) -> Result<usize> {
    let Some(row_count) = column_values.first().map(Vec::len) else {
        return Ok(fallback_rows);
    };
    if column_values.iter().any(|values| values.len() != row_count) {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex row export scanned columns had mismatched row counts; no fallback execution was attempted"
                .to_string(),
        ));
    }
    Ok(row_count)
}

#[cfg(feature = "vortex-local-primitives")]
fn row_export_materialized_row(
    column_values: &[Vec<StatValue>],
    row_index: usize,
) -> Result<Vec<StatValue>> {
    column_values
        .iter()
        .map(|values| {
            values.get(row_index).cloned().ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex row export materialized row index was out of bounds; no fallback execution was attempted"
                        .to_string(),
                )
            })
        })
        .collect()
}

#[cfg(feature = "vortex-local-primitives")]
fn insert_sample_row_export_candidate(
    selected: &mut Vec<(u64, usize, Vec<StatValue>)>,
    limit: usize,
    score: u64,
    row_index: usize,
    row: Vec<StatValue>,
) {
    if selected.len() < limit {
        selected.push((score, row_index, row));
        return;
    }
    let Some((replace_index, lowest_score)) = selected
        .iter()
        .enumerate()
        .min_by_key(|(_index, (candidate_score, _row_index, _row))| *candidate_score)
        .map(|(index, (candidate_score, _row_index, _row))| (index, *candidate_score))
    else {
        return;
    };
    if score > lowest_score {
        selected[replace_index] = (score, row_index, row);
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn normalized_sample_fraction(value: Option<f64>) -> Result<Option<f64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if !value.is_finite() || value <= 0.0 || value > 1.0 {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex sample fraction must be finite and in the range (0, 1]".to_string(),
        ));
    }
    Ok(Some(value))
}

#[cfg(feature = "vortex-local-primitives")]
fn fractional_sample_size(row_count: usize, fraction: f64) -> Result<usize> {
    if row_count == 0 {
        return Ok(0);
    }
    let mut target = 0usize;
    let mut carry = 0.0_f64;
    for _ in 0..row_count {
        carry += fraction;
        if carry >= 1.0 {
            target = target.checked_add(1).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex sample fraction row count overflowed usize".to_string(),
                )
            })?;
            carry -= 1.0;
        }
    }
    if carry > f64::EPSILON {
        target = target.checked_add(1).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex sample fraction row count overflowed usize".to_string(),
            )
        })?;
    }
    Ok(target.max(1).min(row_count))
}

#[cfg(feature = "vortex-local-primitives")]
fn sample_target_count(request: &VortexQueryPrimitiveRequest, row_count: usize) -> Result<usize> {
    let sample_fraction = normalized_sample_fraction(request.sample_fraction)?;
    if request.source_order_limit.is_some() && sample_fraction.is_some() {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex sample accepts either sample size or sample fraction, not both"
                .to_string(),
        ));
    }
    if let Some(limit) = request.source_order_limit {
        if limit == 0 {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex sample size must be >= 1".to_string(),
            ));
        }
        return Ok(limit.min(row_count));
    }
    if let Some(fraction) = sample_fraction {
        return fractional_sample_size(row_count, fraction);
    }
    Err(ShardLoomError::InvalidOperation(
        "local Vortex sample requires a sample size or sample fraction".to_string(),
    ))
}

#[cfg(feature = "vortex-local-primitives")]
fn truncate_sample_candidates_to_target(
    sample_rows: &mut Vec<(u64, usize, Vec<StatValue>)>,
    target_count: usize,
) {
    sample_rows.sort_by_key(|(score, _row_index, _row)| std::cmp::Reverse(*score));
    sample_rows.truncate(target_count);
    sample_rows.sort_by_key(|(_score, row_index, _row)| *row_index);
}

#[cfg(feature = "vortex-local-primitives")]
fn format_sample_fraction(value: f64) -> String {
    format!("{value:.12}")
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

#[cfg(feature = "vortex-local-primitives")]
fn sample_shape_summary(request: &VortexQueryPrimitiveRequest) -> String {
    if let Some(fraction) = request.sample_fraction {
        format!("sample_fraction={}", format_sample_fraction(fraction))
    } else {
        format!(
            "sample_size={}",
            request
                .source_order_limit
                .map_or_else(|| "none".to_string(), |limit| limit.to_string())
        )
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn distinct_row_indices(
    column_values: &[Vec<StatValue>],
    seen: &mut std::collections::BTreeSet<String>,
    remaining_limit: Option<usize>,
) -> Result<Vec<usize>> {
    let Some(row_count) = column_values.first().map(Vec::len) else {
        return Ok(Vec::new());
    };
    if remaining_limit == Some(0) {
        return Ok(Vec::new());
    }
    let mut selected = Vec::new();
    for row_index in 0..row_count {
        let key = distinct_row_key(column_values, row_index)?;
        if seen.insert(key) {
            selected.push(row_index);
            if remaining_limit.is_some_and(|limit| selected.len() >= limit) {
                break;
            }
        }
    }
    Ok(selected)
}

#[cfg(feature = "vortex-local-primitives")]
fn distinct_row_key(column_values: &[Vec<StatValue>], row_index: usize) -> Result<String> {
    let mut out = String::new();
    for values in column_values {
        let value = values.get(row_index).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex distinct row-key columns had mismatched row counts; no fallback execution was attempted"
                    .to_string(),
            )
        })?;
        out.push('|');
        push_distinct_value_key(&mut out, value);
    }
    Ok(out)
}

#[cfg(feature = "vortex-local-primitives")]
fn push_distinct_value_key(out: &mut String, value: &StatValue) {
    match value {
        StatValue::Boolean(value) => {
            let _ = write!(out, "b:{value}");
        }
        StatValue::Int64(value) => {
            let _ = write!(out, "i:{value}");
        }
        StatValue::UInt64(value) => {
            let _ = write!(out, "u:{value}");
        }
        StatValue::Float64(value) => {
            let _ = write!(out, "f:{:016x}", value.to_bits());
        }
        StatValue::Utf8(value) => {
            let _ = write!(out, "s:{}:{value}", value.len());
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn stat_value_to_json_value(value: &StatValue) -> Result<serde_json::Value> {
    match value {
        StatValue::Boolean(value) => Ok(serde_json::Value::Bool(*value)),
        StatValue::Int64(value) => Ok(serde_json::Value::Number((*value).into())),
        StatValue::UInt64(value) => Ok(serde_json::Value::Number((*value).into())),
        StatValue::Float64(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex row export cannot serialize non-finite float values; no fallback execution was attempted"
                        .to_string(),
                )
            }),
        StatValue::Utf8(value) => Ok(serde_json::Value::String(value.clone())),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn stat_value_to_csv_cell(value: &StatValue) -> String {
    match value {
        StatValue::Boolean(value) => value.to_string(),
        StatValue::Int64(value) => value.to_string(),
        StatValue::UInt64(value) => value.to_string(),
        StatValue::Float64(value) => value.to_string(),
        StatValue::Utf8(value) => csv_escape(value),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::too_many_lines)]
fn execute_vortex_local_primitive_enabled(
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let Some(uri) = request.source_uri.as_ref() else {
        return Ok(VortexLocalPrimitiveExecutionReport::blocked(
            request.kind,
            VortexLocalPrimitiveExecutionStatus::BlockedByUnsupportedInput,
            Diagnostic::invalid_input(
                "vortex_local_primitive",
                "local primitive execution requires a source URI",
                "provide a local `.vortex` source URI",
            ),
        ));
    };
    let Some(path) = local_vortex_path(uri, request.kind)? else {
        return Ok(VortexLocalPrimitiveExecutionReport::blocked(
            request.kind,
            VortexLocalPrimitiveExecutionStatus::BlockedByUnsupportedInput,
            Diagnostic::invalid_input(
                "vortex_local_primitive",
                format!(
                    "unsupported local Vortex primitive target: {}",
                    uri.as_str()
                ),
                "provide an existing local path or file:// `.vortex` target",
            ),
        ));
    };
    match request.kind {
        VortexQueryPrimitiveKind::CountAll => {
            let scan = read_local_vortex_scan(uri, &path, request.kind, policy, |_| {
                Ok(LocalVortexScanPlan::passthrough())
            })?;
            Ok(count_all_report(request.kind, &scan))
        }
        VortexQueryPrimitiveKind::CountWhere | VortexQueryPrimitiveKind::FilterPredicate => {
            let Some(predicate) = request.predicate.as_ref() else {
                return Ok(VortexLocalPrimitiveExecutionReport::blocked(
                    request.kind,
                    VortexLocalPrimitiveExecutionStatus::BlockedByUnsupportedPrimitive,
                    Diagnostic::invalid_input(
                        "vortex_local_primitive",
                        "predicate primitive was missing its predicate",
                        "use count-where:<predicate> or filter:<predicate>",
                    ),
                ));
            };
            let scan = read_local_vortex_scan(uri, &path, request.kind, policy, |dtype| {
                let mut plan = LocalVortexScanPlan::filter(predicate_to_vortex_expr(
                    predicate,
                    dtype,
                    request.kind,
                )?);
                if request.kind == VortexQueryPrimitiveKind::FilterPredicate {
                    plan.source_order_limit = request.source_order_limit;
                }
                Ok(plan)
            })?;
            predicate_report(request.kind, &scan, predicate)
        }
        VortexQueryPrimitiveKind::ProjectColumns => {
            let scan = read_local_vortex_scan(uri, &path, request.kind, policy, |dtype| {
                let mut plan = projection_scan_plan(dtype, &request.projection, request.kind)?;
                plan.source_order_limit = request.source_order_limit;
                Ok(plan)
            })?;
            projection_report(request.kind, &scan)
        }
        VortexQueryPrimitiveKind::FilterAndProject => {
            let Some(predicate) = request.predicate.as_ref() else {
                return Ok(VortexLocalPrimitiveExecutionReport::blocked(
                    request.kind,
                    VortexLocalPrimitiveExecutionStatus::BlockedByUnsupportedPrimitive,
                    Diagnostic::invalid_input(
                        "vortex_local_primitive",
                        "filter-and-project primitive was missing its predicate",
                        "use filter-project:<predicate>|<columns>",
                    ),
                ));
            };
            let scan = read_local_vortex_scan(uri, &path, request.kind, policy, |dtype| {
                let mut plan = projection_scan_plan(dtype, &request.projection, request.kind)?;
                plan.filter = Some(predicate_to_vortex_expr(predicate, dtype, request.kind)?);
                plan.source_order_limit = request.source_order_limit;
                Ok(plan)
            })?;
            filter_and_project_report(request.kind, &scan)
        }
        VortexQueryPrimitiveKind::DistinctRows => {
            let scan = read_local_vortex_distinct_scan(uri, &path, request, policy)?;
            distinct_rows_report(request.kind, &scan)
        }
        VortexQueryPrimitiveKind::TailRows => {
            let scan = read_local_vortex_tail_scan(uri, &path, request, policy)?;
            tail_rows_report(request.kind, &scan)
        }
        VortexQueryPrimitiveKind::SampleRows => {
            let scan = read_local_vortex_sample_scan(uri, &path, request, policy)?;
            sample_rows_report(request, &scan)
        }
        VortexQueryPrimitiveKind::SimpleAggregate | VortexQueryPrimitiveKind::Unsupported => {
            Ok(VortexLocalPrimitiveExecutionReport::blocked(
                request.kind,
                VortexLocalPrimitiveExecutionStatus::BlockedByUnsupportedPrimitive,
                Diagnostic::unsupported(
                    DiagnosticCode::NotImplemented,
                    "vortex_local_primitive",
                    format!(
                        "local primitive execution does not yet support {}",
                        request.kind.as_str()
                    ),
                    Some("Fallback attempted: false".to_string()),
                ),
            ))
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
struct LocalVortexScan {
    source_row_count: u64,
    result_row_count: usize,
    pre_limit_result_row_count: usize,
    arrays_read_count: usize,
    reader_splits: Vec<VortexReaderBackedSplitEvidence>,
    reader_generated_prepared_batch_report: VortexReaderGeneratedPreparedBatchReport,
    max_chunk_rows: usize,
    max_parallelism_requested: usize,
    scan_concurrency_per_worker: usize,
    projected_columns: Vec<String>,
    filter_pushdown_applied: bool,
    projection_pushdown_applied: bool,
    source_order_limit: Option<usize>,
}

#[cfg(feature = "vortex-local-primitives")]
struct LocalVortexScanPlan {
    filter: Option<vortex::array::expr::Expression>,
    projection: Option<vortex::array::expr::Expression>,
    projected_columns: Vec<String>,
    source_order_limit: Option<usize>,
}
#[cfg(feature = "vortex-local-primitives")]
impl LocalVortexScanPlan {
    fn passthrough() -> Self {
        Self {
            filter: None,
            projection: None,
            projected_columns: Vec::new(),
            source_order_limit: None,
        }
    }

    fn filter(filter: vortex::array::expr::Expression) -> Self {
        Self {
            filter: Some(filter),
            projection: None,
            projected_columns: Vec::new(),
            source_order_limit: None,
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn local_vortex_path(
    target_uri: &DatasetUri,
    primitive_kind: VortexQueryPrimitiveKind,
) -> Result<Option<std::path::PathBuf>> {
    if !target_uri.looks_like_vortex() {
        return Ok(None);
    }
    let path = match target_uri.scheme() {
        UriScheme::LocalPath => std::path::PathBuf::from(target_uri.as_str()),
        UriScheme::File => std::path::PathBuf::from(
            target_uri
                .as_str()
                .strip_prefix("file://")
                .unwrap_or_else(|| target_uri.as_str()),
        ),
        UriScheme::S3 | UriScheme::Gcs | UriScheme::Adls | UriScheme::Other => return Ok(None),
    };
    let path = if path.is_relative() && !path.exists() {
        let workspace_candidate = workspace_root().join(&path);
        if workspace_candidate.exists() {
            workspace_candidate
        } else {
            path
        }
    } else {
        path
    };
    if !path.exists() {
        return Ok(None);
    }
    if !path.is_file() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local primitive {} target is not a file: {}",
            primitive_kind.as_str(),
            path.display()
        )));
    }
    Ok(Some(path))
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::too_many_lines)]
fn read_local_vortex_scan(
    source_uri: &DatasetUri,
    path: &std::path::Path,
    primitive_kind: VortexQueryPrimitiveKind,
    policy: VortexLocalPrimitiveExecutionPolicy,
    configure: impl FnOnce(&vortex::array::dtype::DType) -> Result<LocalVortexScanPlan>,
) -> Result<LocalVortexScan> {
    use vortex::VortexSessionDefault as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(session.open_options().open_path(path))
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to open local Vortex target for {}: {error}",
                primitive_kind.as_str()
            ))
        })?;
    let source_row_count = file.row_count();
    let plan = configure(file.dtype())?;
    if plan.source_order_limit == Some(0) {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex source-order limit must be >= 1".to_string(),
        ));
    }
    let filter_pushdown_applied = plan.filter.is_some();
    let projection_pushdown_applied = plan.projection.is_some();
    let source_order_limit = plan.source_order_limit;
    let mut scan = file.scan().map_err(vortex_error)?;
    if let Some(filter) = plan.filter {
        scan = scan.with_filter(filter);
    }
    if let Some(projection) = plan.projection {
        scan = scan.with_projection(projection);
    }
    scan = scan.with_concurrency(policy.scan_concurrency_per_worker());
    let mut result_row_count = 0usize;
    let mut pre_limit_result_row_count = 0usize;
    let mut arrays_read_count = 0usize;
    let mut reader_splits = Vec::new();
    let mut encoded_kernel_inputs = Vec::new();
    let mut max_chunk_rows = 0usize;
    for chunk in scan.into_array_iter(&runtime).map_err(vortex_error)? {
        let chunk = chunk.map_err(vortex_error)?;
        let rows = chunk.len();
        pre_limit_result_row_count =
            pre_limit_result_row_count
                .checked_add(rows)
                .ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex primitive pre-limit result row count overflowed usize"
                            .to_string(),
                    )
                })?;
        let split = VortexReaderBackedSplitEvidence::local_scan_chunk(
            source_uri.clone(),
            arrays_read_count,
            rows,
            chunk.dtype().to_string(),
            chunk.encoding_id().to_string(),
            chunk.nchildren(),
            chunk.nbuffers(),
        )?;
        encoded_kernel_inputs.extend(reader_generated_encoded_kernel_inputs_from_vortex_chunk(
            source_uri,
            &split.split_ref,
            &chunk,
        )?);
        reader_splits.push(split);
        let output_rows = source_order_limit.map_or(rows, |limit| {
            limit.saturating_sub(result_row_count).min(rows)
        });
        result_row_count = result_row_count.checked_add(output_rows).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex primitive result row count overflowed usize".to_string(),
            )
        })?;
        max_chunk_rows = max_chunk_rows.max(rows);
        arrays_read_count += 1;
        if source_order_limit.is_some_and(|limit| result_row_count >= limit) {
            break;
        }
    }
    let source = UniversalInputSource::from_dataset_uri(source_uri.clone())?;
    let reader_generated_prepared_batch_report = if encoded_kernel_inputs.is_empty() {
        plan_vortex_reader_generated_prepared_batch_envelopes(&source, &reader_splits)
    } else {
        plan_vortex_reader_generated_prepared_batch_kernel_inputs(
            &source,
            &reader_splits,
            &encoded_kernel_inputs,
        )
    };
    Ok(LocalVortexScan {
        source_row_count,
        result_row_count,
        pre_limit_result_row_count,
        arrays_read_count,
        reader_splits,
        reader_generated_prepared_batch_report,
        max_chunk_rows,
        max_parallelism_requested: policy.max_parallelism,
        scan_concurrency_per_worker: policy.scan_concurrency_per_worker(),
        projected_columns: plan.projected_columns,
        filter_pushdown_applied,
        projection_pushdown_applied,
        source_order_limit,
    })
}

#[cfg(feature = "vortex-local-primitives")]
pub(crate) fn reader_generated_encoded_kernel_inputs_from_vortex_chunk(
    source_uri: &DatasetUri,
    split_ref: &str,
    chunk: &vortex::array::ArrayRef,
) -> Result<Vec<VortexReaderGeneratedEncodedKernelInput>> {
    if chunk.dtype().is_struct() {
        return chunk
            .named_children()
            .into_iter()
            .filter_map(|(field_name, field)| {
                encoded_kernel_input_from_vortex_array(source_uri, split_ref, &field_name, &field)
                    .transpose()
            })
            .collect();
    }
    encoded_kernel_input_from_vortex_array(source_uri, split_ref, "value", chunk)
        .map(|input| input.into_iter().collect())
}

#[cfg(feature = "vortex-local-primitives")]
fn encoded_kernel_input_from_vortex_array(
    source_uri: &DatasetUri,
    split_ref: &str,
    column_name: &str,
    array: &vortex::array::ArrayRef,
) -> Result<Option<VortexReaderGeneratedEncodedKernelInput>> {
    let row_count = u64::try_from(array.len()).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "local Vortex reader child array row count overflowed u64: {error}"
        ))
    })?;
    if let Some(input) = constant_kernel_input_from_vortex_array(
        source_uri,
        split_ref,
        column_name,
        row_count,
        array,
    )? {
        return Ok(Some(input));
    }
    if let Some(input) =
        dictionary_kernel_input_from_vortex_array(source_uri, split_ref, column_name, array)?
    {
        return Ok(Some(input));
    }
    if let Some(input) =
        bitpacked_kernel_input_from_vortex_array(source_uri, split_ref, column_name, array)?
    {
        return Ok(Some(input));
    }
    if let Some(input) = sequence_kernel_input_from_vortex_array(
        source_uri,
        split_ref,
        column_name,
        row_count,
        array,
    )? {
        return Ok(Some(input));
    }
    run_end_kernel_input_from_vortex_array(source_uri, split_ref, column_name, row_count, array)
}

#[cfg(feature = "vortex-local-primitives")]
fn constant_kernel_input_from_vortex_array(
    source_uri: &DatasetUri,
    split_ref: &str,
    column_name: &str,
    row_count: u64,
    array: &vortex::array::ArrayRef,
) -> Result<Option<VortexReaderGeneratedEncodedKernelInput>> {
    let Some(constant) = array.as_constant() else {
        return Ok(None);
    };
    let Some(value) = vortex_scalar_to_stat_value(&constant) else {
        return Ok(None);
    };
    let mut stats = SegmentStats::with_row_count(row_count);
    stats.null_count = Some(0);
    stats.min_value = Some(value.clone());
    stats.max_value = Some(value.clone());
    stats.is_constant = Some(true);
    let segment = EncodedSegment::new(
        SegmentId::new(format!("{split_ref}.{column_name}.constant"))?,
        ColumnRef::new(column_name)?,
        value.dtype(),
        shardloom_nullability_from_vortex_dtype(array.dtype()),
        SegmentLayout::new(EncodingKind::Constant, LayoutKind::Flat),
        stats,
    );
    let batch = VortexEncodedValuePredicateBatch::new(
        segment,
        EncodedValueBatch::Constant {
            value: Some(value),
            row_count,
        },
    );
    VortexReaderGeneratedEncodedKernelInput::new(source_uri.clone(), split_ref, batch).map(Some)
}

#[cfg(feature = "vortex-local-primitives")]
fn dictionary_kernel_input_from_vortex_array(
    source_uri: &DatasetUri,
    split_ref: &str,
    column_name: &str,
    array: &vortex::array::ArrayRef,
) -> Result<Option<VortexReaderGeneratedEncodedKernelInput>> {
    use vortex::array::arrays::dict::DictArraySlotsExt as _;

    let Some(dictionary_array) = array.as_opt::<vortex::array::arrays::Dict>() else {
        return Ok(None);
    };
    let Some(dtype) = shardloom_logical_dtype_from_vortex_dtype(array.dtype()) else {
        return Ok(None);
    };
    let Some(dictionary) = stat_values_from_vortex_array(dictionary_array.values()) else {
        return Ok(None);
    };
    let Some(codes) = primitive_u32_codes_from_vortex_array(dictionary_array.codes()) else {
        return Ok(None);
    };
    let row_count = u64::try_from(codes.len()).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "local Vortex dictionary code count overflowed u64: {error}"
        ))
    })?;
    if row_count
        != u64::try_from(array.len()).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "local Vortex dictionary array row count overflowed u64: {error}"
            ))
        })?
    {
        return Ok(None);
    }

    let mut stats = SegmentStats::with_row_count(row_count);
    stats.null_count = Some(0);
    stats.is_constant = Some(dictionary.len() == 1);
    let segment = EncodedSegment::new(
        SegmentId::new(format!("{split_ref}.{column_name}.dictionary"))?,
        ColumnRef::new(column_name)?,
        dtype,
        ShardLoomNullability::NonNullable,
        SegmentLayout::new(EncodingKind::Dictionary, LayoutKind::Flat),
        stats,
    );
    let batch = VortexEncodedValuePredicateBatch::new(
        segment,
        EncodedValueBatch::Dictionary {
            dictionary: dictionary.into_iter().map(Some).collect(),
            codes: codes.into_iter().map(Some).collect(),
        },
    );
    VortexReaderGeneratedEncodedKernelInput::new(source_uri.clone(), split_ref, batch).map(Some)
}

#[cfg(feature = "vortex-local-primitives")]
fn bitpacked_kernel_input_from_vortex_array(
    source_uri: &DatasetUri,
    split_ref: &str,
    column_name: &str,
    array: &vortex::array::ArrayRef,
) -> Result<Option<VortexReaderGeneratedEncodedKernelInput>> {
    use vortex::array::dtype::PType;
    use vortex::array::validity::Validity;
    use vortex::encodings::fastlanes::BitPackedArrayExt as _;

    let Some(bitpacked_array) = array.as_opt::<vortex::encodings::fastlanes::BitPacked>() else {
        return Ok(None);
    };
    if !bitpacked_array.packed().is_on_host() {
        return Ok(None);
    }
    match bitpacked_array.validity().map_err(vortex_error)? {
        Validity::NonNullable | Validity::AllValid => {}
        Validity::AllInvalid | Validity::Array(_) => return Ok(None),
    }
    if bitpacked_array.patches().is_some() {
        return Ok(None);
    }

    let values = match array.dtype() {
        vortex::array::dtype::DType::Primitive(PType::U8, _) => {
            collect_bitpacked_unsigned_values::<u8>(&bitpacked_array)?
        }
        vortex::array::dtype::DType::Primitive(PType::U16, _) => {
            collect_bitpacked_unsigned_values::<u16>(&bitpacked_array)?
        }
        vortex::array::dtype::DType::Primitive(PType::U32, _) => {
            collect_bitpacked_unsigned_values::<u32>(&bitpacked_array)?
        }
        vortex::array::dtype::DType::Primitive(PType::U64, _) => {
            collect_bitpacked_unsigned_values::<u64>(&bitpacked_array)?
        }
        _ => return Ok(None),
    };
    let row_count = u64::try_from(values.len()).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "local Vortex bit-packed value count overflowed u64: {error}"
        ))
    })?;
    if values.len() != array.len() {
        return Ok(None);
    }

    let mut stats = SegmentStats::with_row_count(row_count);
    stats.null_count = Some(0);
    stats.min_value = values.iter().min().copied().map(StatValue::UInt64);
    stats.max_value = values.iter().max().copied().map(StatValue::UInt64);
    stats.is_constant = Some(stats.min_value == stats.max_value);
    let segment = EncodedSegment::new(
        SegmentId::new(format!("{split_ref}.{column_name}.bit_packed"))?,
        ColumnRef::new(column_name)?,
        LogicalDType::UInt64,
        ShardLoomNullability::NonNullable,
        SegmentLayout::new(EncodingKind::BitPacked, LayoutKind::Flat),
        stats,
    );
    let batch = VortexEncodedValuePredicateBatch::new(
        segment,
        EncodedValueBatch::BitPackedUnsigned {
            bit_width: bitpacked_array.bit_width(),
            values,
        },
    );
    VortexReaderGeneratedEncodedKernelInput::new(source_uri.clone(), split_ref, batch).map(Some)
}

#[cfg(feature = "vortex-local-primitives")]
fn collect_bitpacked_unsigned_values<T>(
    bitpacked_array: &vortex::array::ArrayView<'_, vortex::encodings::fastlanes::BitPacked>,
) -> Result<Vec<u64>>
where
    T: vortex::encodings::fastlanes::unpack_iter::BitPacked + Copy + Into<u64>,
{
    use lending_iterator::prelude::LendingIterator as _;
    use vortex::encodings::fastlanes::BitPackedArrayExt as _;

    let mut chunks = bitpacked_array
        .unpacked_chunks::<T>()
        .map_err(vortex_error)?;
    let mut values = Vec::with_capacity(bitpacked_array.as_ref().len());
    if let Some(initial) = chunks.initial() {
        values.extend(initial.iter().copied().map(Into::into));
    }
    {
        let mut full_chunks = chunks.full_chunks();
        while let Some(chunk) = full_chunks.next() {
            values.extend(chunk.iter().copied().map(Into::into));
        }
    }
    if let Some(trailer) = chunks.trailer() {
        values.extend(trailer.iter().copied().map(Into::into));
    }
    Ok(values)
}

#[cfg(feature = "vortex-local-primitives")]
fn sequence_kernel_input_from_vortex_array(
    source_uri: &DatasetUri,
    split_ref: &str,
    column_name: &str,
    row_count: u64,
    array: &vortex::array::ArrayRef,
) -> Result<Option<VortexReaderGeneratedEncodedKernelInput>> {
    let Some(sequence_array) = array.as_opt::<vortex::encodings::sequence::Sequence>() else {
        return Ok(None);
    };
    let Some(dtype) = shardloom_logical_dtype_from_vortex_dtype(array.dtype()) else {
        return Ok(None);
    };
    let Some(base) = vortex_pvalue_to_stat_value(sequence_array.base()) else {
        return Ok(None);
    };
    let Some(multiplier) = vortex_pvalue_to_stat_value(sequence_array.multiplier()) else {
        return Ok(None);
    };
    if base.dtype() != multiplier.dtype() {
        return Ok(None);
    }

    let mut stats = SegmentStats::with_row_count(row_count);
    stats.null_count = Some(0);
    stats.is_constant = Some(matches!(
        &multiplier,
        StatValue::UInt64(0) | StatValue::Int64(0)
    ));
    let segment = EncodedSegment::new(
        SegmentId::new(format!("{split_ref}.{column_name}.sequence"))?,
        ColumnRef::new(column_name)?,
        dtype,
        ShardLoomNullability::NonNullable,
        SegmentLayout::new(EncodingKind::Sequence, LayoutKind::Flat),
        stats,
    );
    let batch = VortexEncodedValuePredicateBatch::new(
        segment,
        EncodedValueBatch::ArithmeticSequence {
            base,
            multiplier,
            row_count,
        },
    );
    VortexReaderGeneratedEncodedKernelInput::new(source_uri.clone(), split_ref, batch).map(Some)
}

#[cfg(feature = "vortex-local-primitives")]
fn run_end_kernel_input_from_vortex_array(
    source_uri: &DatasetUri,
    split_ref: &str,
    column_name: &str,
    row_count: u64,
    array: &vortex::array::ArrayRef,
) -> Result<Option<VortexReaderGeneratedEncodedKernelInput>> {
    use vortex::encodings::runend::RunEndArrayExt as _;

    let Some(run_end_array) = array.as_opt::<vortex::encodings::runend::RunEnd>() else {
        return Ok(None);
    };
    if run_end_array.offset() != 0 {
        return Ok(None);
    }
    let Some(dtype) = shardloom_logical_dtype_from_vortex_dtype(array.dtype()) else {
        return Ok(None);
    };
    let Some(ends) = primitive_u64_values_from_vortex_array(run_end_array.ends()) else {
        return Ok(None);
    };
    let Some(values) = stat_values_from_vortex_array(run_end_array.values()) else {
        return Ok(None);
    };
    if ends.len() != values.len() {
        return Ok(None);
    }

    let mut previous_end = 0_u64;
    let mut runs = Vec::with_capacity(ends.len());
    for (end, value) in ends.into_iter().zip(values) {
        if end < previous_end || end > row_count {
            return Ok(None);
        }
        runs.push(EncodedValueRun {
            value: Some(value),
            len: end - previous_end,
        });
        previous_end = end;
    }
    if previous_end != row_count {
        return Ok(None);
    }

    let mut stats = SegmentStats::with_row_count(row_count);
    stats.null_count = Some(0);
    stats.run_count = Some(u64::try_from(runs.len()).map_err(|error| {
        ShardLoomError::InvalidOperation(format!("local Vortex run count overflowed u64: {error}"))
    })?);
    stats.is_constant = Some(runs.len() <= 1);
    let segment = EncodedSegment::new(
        SegmentId::new(format!("{split_ref}.{column_name}.run_end"))?,
        ColumnRef::new(column_name)?,
        dtype,
        ShardLoomNullability::NonNullable,
        SegmentLayout::new(EncodingKind::RunLength, LayoutKind::Flat),
        stats,
    );
    let batch =
        VortexEncodedValuePredicateBatch::new(segment, EncodedValueBatch::RunLength { runs });
    VortexReaderGeneratedEncodedKernelInput::new(source_uri.clone(), split_ref, batch).map(Some)
}

#[cfg(feature = "vortex-local-primitives")]
fn stat_values_from_vortex_array(array: &vortex::array::ArrayRef) -> Option<Vec<StatValue>> {
    use vortex::array::dtype::DType;

    match array.dtype() {
        DType::Primitive(_, _) => primitive_stat_values_from_vortex_array(array),
        DType::Utf8(_) => utf8_stat_values_from_vortex_array(array),
        DType::Bool(_) => bool_stat_values_from_vortex_array(array),
        DType::Null
        | DType::Decimal(_, _)
        | DType::Binary(_)
        | DType::Struct(_, _)
        | DType::List(_, _)
        | DType::FixedSizeList(_, _, _)
        | DType::Extension(_)
        | DType::Union(_)
        | DType::Variant(_) => None,
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn primitive_stat_values_from_vortex_array(
    array: &vortex::array::ArrayRef,
) -> Option<Vec<StatValue>> {
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::PrimitiveArray;

    if let Some(primitive) = direct_non_nullable_host_primitive(array) {
        return primitive_stat_values_from_primitive_array(&primitive);
    }

    let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
    let primitive = array.clone().execute::<PrimitiveArray>(&mut ctx).ok()?;
    primitive_stat_values_from_primitive_array(&primitive)
}

#[cfg(feature = "vortex-local-primitives")]
fn primitive_stat_values_from_primitive_array(
    primitive: &(impl vortex::array::arrays::primitive::PrimitiveArrayExt + ?Sized),
) -> Option<Vec<StatValue>> {
    use vortex::array::dtype::PType;
    use vortex::array::validity::Validity;

    match primitive.validity() {
        Validity::NonNullable | Validity::AllValid => {}
        Validity::AllInvalid | Validity::Array(_) => return None,
    }
    match primitive.ptype() {
        PType::U8 => Some(
            primitive
                .as_slice::<u8>()
                .iter()
                .map(|value| StatValue::UInt64(u64::from(*value)))
                .collect(),
        ),
        PType::U16 => Some(
            primitive
                .as_slice::<u16>()
                .iter()
                .map(|value| StatValue::UInt64(u64::from(*value)))
                .collect(),
        ),
        PType::U32 => Some(
            primitive
                .as_slice::<u32>()
                .iter()
                .map(|value| StatValue::UInt64(u64::from(*value)))
                .collect(),
        ),
        PType::U64 => Some(
            primitive
                .as_slice::<u64>()
                .iter()
                .map(|value| StatValue::UInt64(*value))
                .collect(),
        ),
        PType::I8 => Some(
            primitive
                .as_slice::<i8>()
                .iter()
                .map(|value| StatValue::Int64(i64::from(*value)))
                .collect(),
        ),
        PType::I16 => Some(
            primitive
                .as_slice::<i16>()
                .iter()
                .map(|value| StatValue::Int64(i64::from(*value)))
                .collect(),
        ),
        PType::I32 => Some(
            primitive
                .as_slice::<i32>()
                .iter()
                .map(|value| StatValue::Int64(i64::from(*value)))
                .collect(),
        ),
        PType::I64 => Some(
            primitive
                .as_slice::<i64>()
                .iter()
                .map(|value| StatValue::Int64(*value))
                .collect(),
        ),
        PType::F16 => None,
        PType::F32 => Some(
            primitive
                .as_slice::<f32>()
                .iter()
                .map(|value| StatValue::Float64(f64::from(*value)))
                .collect(),
        ),
        PType::F64 => Some(
            primitive
                .as_slice::<f64>()
                .iter()
                .map(|value| StatValue::Float64(*value))
                .collect(),
        ),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn utf8_stat_values_from_vortex_array(array: &vortex::array::ArrayRef) -> Option<Vec<StatValue>> {
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::VarBinViewArray;
    use vortex::array::arrays::varbinview::VarBinViewArrayExt as _;
    use vortex::array::validity::Validity;

    let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
    let utf8 = array.clone().execute::<VarBinViewArray>(&mut ctx).ok()?;
    match utf8.varbinview_validity() {
        Validity::NonNullable | Validity::AllValid => {}
        Validity::AllInvalid | Validity::Array(_) => return None,
    }
    let mut values = Vec::with_capacity(utf8.len());
    for index in 0..utf8.len() {
        let bytes = utf8.bytes_at(index);
        let text = std::str::from_utf8(bytes.as_slice()).ok()?;
        values.push(StatValue::Utf8(text.to_string()));
    }
    Some(values)
}

#[cfg(feature = "vortex-local-primitives")]
fn bool_stat_values_from_vortex_array(array: &vortex::array::ArrayRef) -> Option<Vec<StatValue>> {
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::BoolArray;
    use vortex::array::arrays::bool::BoolArrayExt as _;
    use vortex::array::validity::Validity;

    let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
    let bool_array = array.clone().execute::<BoolArray>(&mut ctx).ok()?;
    match bool_array.validity().ok()? {
        Validity::NonNullable | Validity::AllValid => {}
        Validity::AllInvalid | Validity::Array(_) => return None,
    }
    Some(
        bool_array
            .to_bit_buffer()
            .iter()
            .take(bool_array.len())
            .map(StatValue::Boolean)
            .collect(),
    )
}

#[cfg(feature = "vortex-local-primitives")]
fn primitive_u32_codes_from_vortex_array(array: &vortex::array::ArrayRef) -> Option<Vec<u32>> {
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::PrimitiveArray;
    use vortex::array::dtype::DType;

    if !matches!(array.dtype(), DType::Primitive(_, _)) {
        return None;
    }

    if let Some(primitive) = direct_non_nullable_host_primitive(array) {
        return primitive_u32_codes_from_primitive_array(&primitive);
    }

    let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
    let primitive = array.clone().execute::<PrimitiveArray>(&mut ctx).ok()?;
    primitive_u32_codes_from_primitive_array(&primitive)
}

#[cfg(feature = "vortex-local-primitives")]
fn primitive_u32_codes_from_primitive_array(
    primitive: &(impl vortex::array::arrays::primitive::PrimitiveArrayExt + ?Sized),
) -> Option<Vec<u32>> {
    use vortex::array::dtype::PType;
    use vortex::array::validity::Validity;

    match primitive.validity() {
        Validity::NonNullable | Validity::AllValid => {}
        Validity::AllInvalid | Validity::Array(_) => return None,
    }
    match primitive.ptype() {
        PType::U8 => Some(
            primitive
                .as_slice::<u8>()
                .iter()
                .map(|value| u32::from(*value))
                .collect(),
        ),
        PType::U16 => Some(
            primitive
                .as_slice::<u16>()
                .iter()
                .map(|value| u32::from(*value))
                .collect(),
        ),
        PType::U32 => Some(primitive.as_slice::<u32>().to_vec()),
        PType::U64 => primitive
            .as_slice::<u64>()
            .iter()
            .map(|value| u32::try_from(*value).ok())
            .collect(),
        PType::I8 | PType::I16 | PType::I32 | PType::I64 | PType::F16 | PType::F32 | PType::F64 => {
            None
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn primitive_u64_values_from_vortex_array(array: &vortex::array::ArrayRef) -> Option<Vec<u64>> {
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::PrimitiveArray;
    use vortex::array::dtype::DType;

    if !matches!(array.dtype(), DType::Primitive(_, _)) {
        return None;
    }

    if let Some(primitive) = direct_non_nullable_host_primitive(array) {
        return primitive_u64_values_from_primitive_array(&primitive);
    }

    let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
    let primitive = array.clone().execute::<PrimitiveArray>(&mut ctx).ok()?;
    primitive_u64_values_from_primitive_array(&primitive)
}

#[cfg(feature = "vortex-local-primitives")]
fn primitive_u64_values_from_primitive_array(
    primitive: &(impl vortex::array::arrays::primitive::PrimitiveArrayExt + ?Sized),
) -> Option<Vec<u64>> {
    use vortex::array::dtype::PType;
    use vortex::array::validity::Validity;

    match primitive.validity() {
        Validity::NonNullable | Validity::AllValid => {}
        Validity::AllInvalid | Validity::Array(_) => return None,
    }
    match primitive.ptype() {
        PType::U8 => Some(
            primitive
                .as_slice::<u8>()
                .iter()
                .map(|value| u64::from(*value))
                .collect(),
        ),
        PType::U16 => Some(
            primitive
                .as_slice::<u16>()
                .iter()
                .map(|value| u64::from(*value))
                .collect(),
        ),
        PType::U32 => Some(
            primitive
                .as_slice::<u32>()
                .iter()
                .map(|value| u64::from(*value))
                .collect(),
        ),
        PType::U64 => Some(primitive.as_slice::<u64>().to_vec()),
        PType::I8 | PType::I16 | PType::I32 | PType::I64 | PType::F16 | PType::F32 | PType::F64 => {
            None
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn direct_non_nullable_host_primitive(
    array: &vortex::array::ArrayRef,
) -> Option<vortex::array::ArrayView<'_, vortex::array::arrays::Primitive>> {
    use vortex::array::arrays::primitive::PrimitiveArrayExt;
    use vortex::array::validity::Validity;

    if !array.is_host() {
        return None;
    }
    let primitive = array.as_opt::<vortex::array::arrays::Primitive>()?;
    match PrimitiveArrayExt::validity(&primitive) {
        Validity::NonNullable | Validity::AllValid => Some(primitive),
        Validity::AllInvalid | Validity::Array(_) => None,
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn shardloom_logical_dtype_from_vortex_dtype(
    dtype: &vortex::array::dtype::DType,
) -> Option<LogicalDType> {
    use vortex::array::dtype::PType;

    match dtype {
        vortex::array::dtype::DType::Bool(_) => Some(LogicalDType::Boolean),
        vortex::array::dtype::DType::Primitive(ptype, _) => match ptype {
            PType::U8 | PType::U16 | PType::U32 | PType::U64 => Some(LogicalDType::UInt64),
            PType::I8 | PType::I16 | PType::I32 | PType::I64 => Some(LogicalDType::Int64),
            PType::F16 => None,
            PType::F32 | PType::F64 => Some(LogicalDType::Float64),
        },
        vortex::array::dtype::DType::Utf8(_) => Some(LogicalDType::Utf8),
        vortex::array::dtype::DType::Null
        | vortex::array::dtype::DType::Decimal(_, _)
        | vortex::array::dtype::DType::Binary(_)
        | vortex::array::dtype::DType::Struct(_, _)
        | vortex::array::dtype::DType::List(_, _)
        | vortex::array::dtype::DType::FixedSizeList(_, _, _)
        | vortex::array::dtype::DType::Extension(_)
        | vortex::array::dtype::DType::Union(_)
        | vortex::array::dtype::DType::Variant(_) => None,
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn vortex_scalar_to_stat_value(scalar: &vortex::array::scalar::Scalar) -> Option<StatValue> {
    use vortex::array::scalar::ScalarValue;

    match scalar.value()? {
        ScalarValue::Bool(value) => Some(StatValue::Boolean(*value)),
        ScalarValue::Primitive(value) => vortex_pvalue_to_stat_value(*value),
        ScalarValue::Utf8(value) => Some(StatValue::Utf8(value.as_str().to_string())),
        ScalarValue::Decimal(_)
        | ScalarValue::Binary(_)
        | ScalarValue::Tuple(_)
        | ScalarValue::Variant(_) => None,
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn vortex_pvalue_to_stat_value(value: vortex::array::scalar::PValue) -> Option<StatValue> {
    use vortex::array::scalar::PValue;

    match value {
        PValue::U8(value) => Some(StatValue::UInt64(u64::from(value))),
        PValue::U16(value) => Some(StatValue::UInt64(u64::from(value))),
        PValue::U32(value) => Some(StatValue::UInt64(u64::from(value))),
        PValue::U64(value) => Some(StatValue::UInt64(value)),
        PValue::I8(value) => Some(StatValue::Int64(i64::from(value))),
        PValue::I16(value) => Some(StatValue::Int64(i64::from(value))),
        PValue::I32(value) => Some(StatValue::Int64(i64::from(value))),
        PValue::I64(value) => Some(StatValue::Int64(value)),
        PValue::F16(_) => None,
        PValue::F32(value) => Some(StatValue::Float64(f64::from(value))),
        PValue::F64(value) => Some(StatValue::Float64(value)),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn shardloom_nullability_from_vortex_dtype(
    dtype: &vortex::array::dtype::DType,
) -> ShardLoomNullability {
    if dtype.is_nullable() {
        ShardLoomNullability::Nullable
    } else {
        ShardLoomNullability::NonNullable
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn count_all_report(
    primitive_kind: VortexQueryPrimitiveKind,
    scan: &LocalVortexScan,
) -> VortexLocalPrimitiveExecutionReport {
    let rows = scan.source_row_count;
    VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::MetadataPreservingCount,
        primitive_kind,
        result_summary: Some(rows.to_string()),
        rows_scanned: rows,
        rows_selected: Some(rows),
        rows_projected: None,
        projected_columns: Vec::new(),
        arrays_read_count: scan.arrays_read_count,
        reader_splits: scan.reader_splits.clone(),
        reader_generated_prepared_batch_report: Some(
            scan.reader_generated_prepared_batch_report.clone(),
        ),
        max_chunk_rows: scan.max_chunk_rows,
        streaming_scan_used: true,
        full_stream_collected: false,
        max_parallelism_requested: scan.max_parallelism_requested,
        scan_concurrency_per_worker: scan.scan_concurrency_per_worker,
        filter_pushdown_applied: scan.filter_pushdown_applied,
        projection_pushdown_applied: scan.projection_pushdown_applied,
        upstream_filter_expression_used: scan.filter_pushdown_applied,
        upstream_projection_expression_used: scan.projection_pushdown_applied,
        source_order_limit_requested: None,
        source_order_limit_applied: false,
        source_order_limit_input_rows: None,
        source_order_limit_rows_output: None,
        data_read: true,
        data_decoded: false,
        data_materialized: false,
        upstream_scan_called: true,
        row_read: false,
        arrow_converted: false,
        object_store_io: false,
        write_io: false,
        spill_io_performed: false,
        external_effects_executed: false,
        fallback_execution_allowed: false,
        materialization_boundary_reported: false,
        diagnostics: Vec::new(),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn predicate_report(
    primitive_kind: VortexQueryPrimitiveKind,
    scan: &LocalVortexScan,
    _predicate: &PredicateExpr,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let rows_selected = usize_to_u64(scan.result_row_count)?;
    Ok(VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::VortexScanPushdown,
        primitive_kind,
        result_summary: Some(rows_selected.to_string()),
        rows_scanned: scan.source_row_count,
        rows_selected: Some(rows_selected),
        rows_projected: None,
        projected_columns: Vec::new(),
        arrays_read_count: scan.arrays_read_count,
        reader_splits: scan.reader_splits.clone(),
        reader_generated_prepared_batch_report: Some(
            scan.reader_generated_prepared_batch_report.clone(),
        ),
        max_chunk_rows: scan.max_chunk_rows,
        streaming_scan_used: true,
        full_stream_collected: false,
        max_parallelism_requested: scan.max_parallelism_requested,
        scan_concurrency_per_worker: scan.scan_concurrency_per_worker,
        filter_pushdown_applied: scan.filter_pushdown_applied,
        projection_pushdown_applied: scan.projection_pushdown_applied,
        upstream_filter_expression_used: scan.filter_pushdown_applied,
        upstream_projection_expression_used: scan.projection_pushdown_applied,
        source_order_limit_requested: scan.source_order_limit.map(usize_to_u64).transpose()?,
        source_order_limit_applied: scan.source_order_limit.is_some(),
        source_order_limit_input_rows: Some(usize_to_u64(scan.pre_limit_result_row_count)?),
        source_order_limit_rows_output: Some(usize_to_u64(scan.result_row_count)?),
        data_read: true,
        data_decoded: false,
        data_materialized: false,
        upstream_scan_called: true,
        row_read: false,
        arrow_converted: false,
        object_store_io: false,
        write_io: false,
        spill_io_performed: false,
        external_effects_executed: false,
        fallback_execution_allowed: false,
        materialization_boundary_reported: false,
        diagnostics: Vec::new(),
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn projection_report(
    primitive_kind: VortexQueryPrimitiveKind,
    scan: &LocalVortexScan,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let rows = usize_to_u64(scan.result_row_count)?;
    Ok(VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::VortexScanPushdown,
        primitive_kind,
        result_summary: Some(format!(
            "projected_columns={} rows={}",
            scan.projected_columns.join(","),
            rows
        )),
        rows_scanned: scan.source_row_count,
        rows_selected: None,
        rows_projected: Some(rows),
        projected_columns: scan.projected_columns.clone(),
        arrays_read_count: scan.arrays_read_count,
        reader_splits: scan.reader_splits.clone(),
        reader_generated_prepared_batch_report: Some(
            scan.reader_generated_prepared_batch_report.clone(),
        ),
        max_chunk_rows: scan.max_chunk_rows,
        streaming_scan_used: true,
        full_stream_collected: false,
        max_parallelism_requested: scan.max_parallelism_requested,
        scan_concurrency_per_worker: scan.scan_concurrency_per_worker,
        filter_pushdown_applied: scan.filter_pushdown_applied,
        projection_pushdown_applied: scan.projection_pushdown_applied,
        upstream_filter_expression_used: scan.filter_pushdown_applied,
        upstream_projection_expression_used: scan.projection_pushdown_applied,
        source_order_limit_requested: scan.source_order_limit.map(usize_to_u64).transpose()?,
        source_order_limit_applied: scan.source_order_limit.is_some(),
        source_order_limit_input_rows: Some(usize_to_u64(scan.pre_limit_result_row_count)?),
        source_order_limit_rows_output: Some(usize_to_u64(scan.result_row_count)?),
        data_read: true,
        data_decoded: false,
        data_materialized: false,
        upstream_scan_called: true,
        row_read: false,
        arrow_converted: false,
        object_store_io: false,
        write_io: false,
        spill_io_performed: false,
        external_effects_executed: false,
        fallback_execution_allowed: false,
        materialization_boundary_reported: false,
        diagnostics: Vec::new(),
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn filter_and_project_report(
    primitive_kind: VortexQueryPrimitiveKind,
    scan: &LocalVortexScan,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let rows = usize_to_u64(scan.result_row_count)?;
    Ok(VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::VortexScanPushdown,
        primitive_kind,
        result_summary: Some(format!(
            "projected_columns={} rows={}",
            scan.projected_columns.join(","),
            rows
        )),
        rows_scanned: scan.source_row_count,
        rows_selected: Some(rows),
        rows_projected: Some(rows),
        projected_columns: scan.projected_columns.clone(),
        arrays_read_count: scan.arrays_read_count,
        reader_splits: scan.reader_splits.clone(),
        reader_generated_prepared_batch_report: Some(
            scan.reader_generated_prepared_batch_report.clone(),
        ),
        max_chunk_rows: scan.max_chunk_rows,
        streaming_scan_used: true,
        full_stream_collected: false,
        max_parallelism_requested: scan.max_parallelism_requested,
        scan_concurrency_per_worker: scan.scan_concurrency_per_worker,
        filter_pushdown_applied: scan.filter_pushdown_applied,
        projection_pushdown_applied: scan.projection_pushdown_applied,
        upstream_filter_expression_used: scan.filter_pushdown_applied,
        upstream_projection_expression_used: scan.projection_pushdown_applied,
        source_order_limit_requested: scan.source_order_limit.map(usize_to_u64).transpose()?,
        source_order_limit_applied: scan.source_order_limit.is_some(),
        source_order_limit_input_rows: Some(usize_to_u64(scan.pre_limit_result_row_count)?),
        source_order_limit_rows_output: Some(usize_to_u64(scan.result_row_count)?),
        data_read: true,
        data_decoded: false,
        data_materialized: false,
        upstream_scan_called: true,
        row_read: false,
        arrow_converted: false,
        object_store_io: false,
        write_io: false,
        spill_io_performed: false,
        external_effects_executed: false,
        fallback_execution_allowed: false,
        materialization_boundary_reported: false,
        diagnostics: Vec::new(),
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn distinct_rows_report(
    primitive_kind: VortexQueryPrimitiveKind,
    scan: &LocalVortexScan,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let rows = usize_to_u64(scan.result_row_count)?;
    Ok(VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::VortexScanPushdown,
        primitive_kind,
        result_summary: Some(format!(
            "distinct_rows={} projected_columns={}",
            rows,
            scan.projected_columns.join(",")
        )),
        rows_scanned: scan.source_row_count,
        rows_selected: Some(rows),
        rows_projected: Some(rows),
        projected_columns: scan.projected_columns.clone(),
        arrays_read_count: scan.arrays_read_count,
        reader_splits: scan.reader_splits.clone(),
        reader_generated_prepared_batch_report: Some(
            scan.reader_generated_prepared_batch_report.clone(),
        ),
        max_chunk_rows: scan.max_chunk_rows,
        streaming_scan_used: true,
        full_stream_collected: false,
        max_parallelism_requested: scan.max_parallelism_requested,
        scan_concurrency_per_worker: scan.scan_concurrency_per_worker,
        filter_pushdown_applied: scan.filter_pushdown_applied,
        projection_pushdown_applied: scan.projection_pushdown_applied,
        upstream_filter_expression_used: scan.filter_pushdown_applied,
        upstream_projection_expression_used: scan.projection_pushdown_applied,
        source_order_limit_requested: scan.source_order_limit.map(usize_to_u64).transpose()?,
        source_order_limit_applied: scan.source_order_limit.is_some(),
        source_order_limit_input_rows: Some(usize_to_u64(scan.pre_limit_result_row_count)?),
        source_order_limit_rows_output: Some(rows),
        data_read: true,
        data_decoded: true,
        data_materialized: true,
        upstream_scan_called: true,
        row_read: true,
        arrow_converted: false,
        object_store_io: false,
        write_io: false,
        spill_io_performed: false,
        external_effects_executed: false,
        fallback_execution_allowed: false,
        materialization_boundary_reported: true,
        diagnostics: Vec::new(),
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn tail_rows_report(
    primitive_kind: VortexQueryPrimitiveKind,
    scan: &LocalVortexScan,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let rows = usize_to_u64(scan.result_row_count)?;
    Ok(VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::VortexScanPushdown,
        primitive_kind,
        result_summary: Some(format!(
            "tail_rows={} projected_columns={}",
            rows,
            scan.projected_columns.join(",")
        )),
        rows_scanned: scan.source_row_count,
        rows_selected: Some(rows),
        rows_projected: Some(rows),
        projected_columns: scan.projected_columns.clone(),
        arrays_read_count: scan.arrays_read_count,
        reader_splits: scan.reader_splits.clone(),
        reader_generated_prepared_batch_report: Some(
            scan.reader_generated_prepared_batch_report.clone(),
        ),
        max_chunk_rows: scan.max_chunk_rows,
        streaming_scan_used: true,
        full_stream_collected: true,
        max_parallelism_requested: scan.max_parallelism_requested,
        scan_concurrency_per_worker: scan.scan_concurrency_per_worker,
        filter_pushdown_applied: scan.filter_pushdown_applied,
        projection_pushdown_applied: scan.projection_pushdown_applied,
        upstream_filter_expression_used: scan.filter_pushdown_applied,
        upstream_projection_expression_used: scan.projection_pushdown_applied,
        source_order_limit_requested: scan.source_order_limit.map(usize_to_u64).transpose()?,
        source_order_limit_applied: scan.source_order_limit.is_some(),
        source_order_limit_input_rows: Some(usize_to_u64(scan.pre_limit_result_row_count)?),
        source_order_limit_rows_output: Some(rows),
        data_read: true,
        data_decoded: true,
        data_materialized: true,
        upstream_scan_called: true,
        row_read: true,
        arrow_converted: false,
        object_store_io: false,
        write_io: false,
        spill_io_performed: false,
        external_effects_executed: false,
        fallback_execution_allowed: false,
        materialization_boundary_reported: true,
        diagnostics: Vec::new(),
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn sample_rows_report(
    request: &VortexQueryPrimitiveRequest,
    scan: &LocalVortexScan,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let rows = usize_to_u64(scan.result_row_count)?;
    let seed = request.sample_seed.unwrap_or(0);
    let sample_shape = sample_shape_summary(request);
    Ok(VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::VortexScanPushdown,
        primitive_kind: request.kind,
        result_summary: Some(format!(
            "sample_rows={} projected_columns={} sample_seed={} {}",
            rows,
            scan.projected_columns.join(","),
            seed,
            sample_shape
        )),
        rows_scanned: scan.source_row_count,
        rows_selected: Some(rows),
        rows_projected: Some(rows),
        projected_columns: scan.projected_columns.clone(),
        arrays_read_count: scan.arrays_read_count,
        reader_splits: scan.reader_splits.clone(),
        reader_generated_prepared_batch_report: Some(
            scan.reader_generated_prepared_batch_report.clone(),
        ),
        max_chunk_rows: scan.max_chunk_rows,
        streaming_scan_used: true,
        full_stream_collected: true,
        max_parallelism_requested: scan.max_parallelism_requested,
        scan_concurrency_per_worker: scan.scan_concurrency_per_worker,
        filter_pushdown_applied: scan.filter_pushdown_applied,
        projection_pushdown_applied: scan.projection_pushdown_applied,
        upstream_filter_expression_used: scan.filter_pushdown_applied,
        upstream_projection_expression_used: scan.projection_pushdown_applied,
        source_order_limit_requested: scan.source_order_limit.map(usize_to_u64).transpose()?,
        source_order_limit_applied: scan.source_order_limit.is_some(),
        source_order_limit_input_rows: Some(usize_to_u64(scan.pre_limit_result_row_count)?),
        source_order_limit_rows_output: Some(rows),
        data_read: true,
        data_decoded: true,
        data_materialized: true,
        upstream_scan_called: true,
        row_read: true,
        arrow_converted: false,
        object_store_io: false,
        write_io: false,
        spill_io_performed: false,
        external_effects_executed: false,
        fallback_execution_allowed: false,
        materialization_boundary_reported: true,
        diagnostics: Vec::new(),
    })
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::too_many_lines)]
fn read_local_vortex_distinct_scan(
    source_uri: &DatasetUri,
    path: &std::path::Path,
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<LocalVortexScan> {
    use vortex::VortexSessionDefault as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(session.open_options().open_path(path))
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to open local Vortex target for {}: {error}",
                request.kind.as_str()
            ))
        })?;
    let source_row_count = file.row_count();
    let plan = row_export_scan_plan(request, file.dtype())?;
    if plan.source_order_limit == Some(0) {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex distinct source-order limit must be >= 1".to_string(),
        ));
    }
    let declared_columns = if plan.projected_columns.is_empty() {
        local_field_names(file.dtype(), request.kind)?
    } else {
        plan.projected_columns.clone()
    };
    let filter_pushdown_applied = plan.filter.is_some();
    let projection_pushdown_applied = plan.projection.is_some();
    let source_order_limit = plan.source_order_limit;
    let mut scan = file.scan().map_err(vortex_error)?;
    if let Some(filter) = plan.filter {
        scan = scan.with_filter(filter);
    }
    if let Some(projection) = plan.projection {
        scan = scan.with_projection(projection);
    }
    scan = scan.with_concurrency(policy.scan_concurrency_per_worker());

    let mut seen = std::collections::BTreeSet::new();
    let mut result_row_count = 0usize;
    let mut arrays_read_count = 0usize;
    let mut reader_splits = Vec::new();
    let mut encoded_kernel_inputs = Vec::new();
    let mut max_chunk_rows = 0usize;
    for chunk in scan.into_array_iter(&runtime).map_err(vortex_error)? {
        let chunk = chunk.map_err(vortex_error)?;
        let rows = chunk.len();
        let split = VortexReaderBackedSplitEvidence::local_scan_chunk(
            source_uri.clone(),
            arrays_read_count,
            rows,
            chunk.dtype().to_string(),
            chunk.encoding_id().to_string(),
            chunk.nchildren(),
            chunk.nbuffers(),
        )?;
        encoded_kernel_inputs.extend(reader_generated_encoded_kernel_inputs_from_vortex_chunk(
            source_uri,
            &split.split_ref,
            &chunk,
        )?);
        reader_splits.push(split);
        let columns = row_export_columns_from_chunk(&chunk, &declared_columns)?;
        let selected = distinct_row_indices(
            &columns,
            &mut seen,
            source_order_limit.map(|limit| limit.saturating_sub(result_row_count)),
        )?;
        result_row_count = result_row_count
            .checked_add(selected.len())
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex distinct result row count overflowed usize".to_string(),
                )
            })?;
        max_chunk_rows = max_chunk_rows.max(rows);
        arrays_read_count += 1;
        if source_order_limit.is_some_and(|limit| result_row_count >= limit) {
            break;
        }
    }
    let source = UniversalInputSource::from_dataset_uri(source_uri.clone())?;
    let reader_generated_prepared_batch_report = if encoded_kernel_inputs.is_empty() {
        plan_vortex_reader_generated_prepared_batch_envelopes(&source, &reader_splits)
    } else {
        plan_vortex_reader_generated_prepared_batch_kernel_inputs(
            &source,
            &reader_splits,
            &encoded_kernel_inputs,
        )
    };
    Ok(LocalVortexScan {
        source_row_count,
        result_row_count,
        pre_limit_result_row_count: seen.len(),
        arrays_read_count,
        reader_splits,
        reader_generated_prepared_batch_report,
        max_chunk_rows,
        max_parallelism_requested: policy.max_parallelism,
        scan_concurrency_per_worker: policy.scan_concurrency_per_worker(),
        projected_columns: declared_columns,
        filter_pushdown_applied,
        projection_pushdown_applied,
        source_order_limit,
    })
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::too_many_lines)]
fn read_local_vortex_tail_scan(
    source_uri: &DatasetUri,
    path: &std::path::Path,
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<LocalVortexScan> {
    use vortex::VortexSessionDefault as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(session.open_options().open_path(path))
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to open local Vortex target for {}: {error}",
                request.kind.as_str()
            ))
        })?;
    let source_row_count = file.row_count();
    let Some(source_order_limit) = request.source_order_limit else {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex tail requires a source-order limit".to_string(),
        ));
    };
    if source_order_limit == 0 {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex tail source-order limit must be >= 1".to_string(),
        ));
    }
    let plan = projection_scan_plan(file.dtype(), &request.projection, request.kind)?;
    let declared_columns = if plan.projected_columns.is_empty() {
        local_field_names(file.dtype(), request.kind)?
    } else {
        plan.projected_columns.clone()
    };
    let projection_pushdown_applied = plan.projection.is_some();
    let mut scan = file.scan().map_err(vortex_error)?;
    if let Some(projection) = plan.projection {
        scan = scan.with_projection(projection);
    }
    scan = scan.with_concurrency(policy.scan_concurrency_per_worker());

    let mut pre_limit_result_row_count = 0usize;
    let mut arrays_read_count = 0usize;
    let mut reader_splits = Vec::new();
    let mut encoded_kernel_inputs = Vec::new();
    let mut max_chunk_rows = 0usize;
    for chunk in scan.into_array_iter(&runtime).map_err(vortex_error)? {
        let chunk = chunk.map_err(vortex_error)?;
        let rows = chunk.len();
        pre_limit_result_row_count =
            pre_limit_result_row_count
                .checked_add(rows)
                .ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex tail pre-limit result row count overflowed usize".to_string(),
                    )
                })?;
        let split = VortexReaderBackedSplitEvidence::local_scan_chunk(
            source_uri.clone(),
            arrays_read_count,
            rows,
            chunk.dtype().to_string(),
            chunk.encoding_id().to_string(),
            chunk.nchildren(),
            chunk.nbuffers(),
        )?;
        encoded_kernel_inputs.extend(reader_generated_encoded_kernel_inputs_from_vortex_chunk(
            source_uri,
            &split.split_ref,
            &chunk,
        )?);
        reader_splits.push(split);
        max_chunk_rows = max_chunk_rows.max(rows);
        arrays_read_count += 1;
    }
    let result_row_count = pre_limit_result_row_count.min(source_order_limit);
    let source = UniversalInputSource::from_dataset_uri(source_uri.clone())?;
    let reader_generated_prepared_batch_report = if encoded_kernel_inputs.is_empty() {
        plan_vortex_reader_generated_prepared_batch_envelopes(&source, &reader_splits)
    } else {
        plan_vortex_reader_generated_prepared_batch_kernel_inputs(
            &source,
            &reader_splits,
            &encoded_kernel_inputs,
        )
    };
    Ok(LocalVortexScan {
        source_row_count,
        result_row_count,
        pre_limit_result_row_count,
        arrays_read_count,
        reader_splits,
        reader_generated_prepared_batch_report,
        max_chunk_rows,
        max_parallelism_requested: policy.max_parallelism,
        scan_concurrency_per_worker: policy.scan_concurrency_per_worker(),
        projected_columns: declared_columns,
        filter_pushdown_applied: false,
        projection_pushdown_applied,
        source_order_limit: Some(source_order_limit),
    })
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::too_many_lines)]
fn read_local_vortex_sample_scan(
    source_uri: &DatasetUri,
    path: &std::path::Path,
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<LocalVortexScan> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    use vortex::VortexSessionDefault as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(session.open_options().open_path(path))
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to open local Vortex target for {}: {error}",
                request.kind.as_str()
            ))
        })?;
    let source_row_count = file.row_count();
    let mut plan = row_export_scan_plan(request, file.dtype())?;
    let source_order_limit = plan.source_order_limit;
    let sample_fraction = normalized_sample_fraction(request.sample_fraction)?;
    if source_order_limit.is_some() && sample_fraction.is_some() {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex sample accepts either sample size or sample fraction, not both"
                .to_string(),
        ));
    }
    if source_order_limit.is_none() && sample_fraction.is_none() {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex sample requires a sample size or sample fraction".to_string(),
        ));
    }
    if source_order_limit == Some(0) {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex sample size must be >= 1".to_string(),
        ));
    }
    let declared_columns = if plan.projected_columns.is_empty() {
        local_field_names(file.dtype(), request.kind)?
    } else {
        plan.projected_columns.clone()
    };
    let filter_pushdown_applied = plan.filter.is_some();
    let projection_pushdown_applied = plan.projection.is_some();
    let mut scan = file.scan().map_err(vortex_error)?;
    if let Some(filter) = plan.filter.take() {
        scan = scan.with_filter(filter);
    }
    if let Some(projection) = plan.projection.take() {
        scan = scan.with_projection(projection);
    }
    scan = scan.with_concurrency(policy.scan_concurrency_per_worker());

    let sample_seed = request.sample_seed.unwrap_or(0);
    let mut sample_scores = BinaryHeap::<Reverse<u64>>::new();
    let mut fraction_candidate_count = 0usize;
    let mut pre_limit_result_row_count = 0usize;
    let mut arrays_read_count = 0usize;
    let mut reader_splits = Vec::new();
    let mut encoded_kernel_inputs = Vec::new();
    let mut max_chunk_rows = 0usize;
    for chunk in scan.into_array_iter(&runtime).map_err(vortex_error)? {
        let chunk = chunk.map_err(vortex_error)?;
        let rows = chunk.len();
        let split = VortexReaderBackedSplitEvidence::local_scan_chunk(
            source_uri.clone(),
            arrays_read_count,
            rows,
            chunk.dtype().to_string(),
            chunk.encoding_id().to_string(),
            chunk.nchildren(),
            chunk.nbuffers(),
        )?;
        encoded_kernel_inputs.extend(reader_generated_encoded_kernel_inputs_from_vortex_chunk(
            source_uri,
            &split.split_ref,
            &chunk,
        )?);
        reader_splits.push(split);
        let materialized_columns = row_export_columns_from_chunk(&chunk, &declared_columns)?;
        let materialized_rows = materialized_columns.first().map_or(rows, Vec::len);
        for local_index in 0..materialized_rows {
            let row_index = pre_limit_result_row_count
                .checked_add(local_index)
                .ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex sample row ordinal overflowed usize".to_string(),
                    )
                })?;
            let score = deterministic_sample_score(sample_seed, row_index);
            if let Some(limit) = source_order_limit {
                if sample_scores.len() < limit {
                    sample_scores.push(Reverse(score));
                } else if let Some(Reverse(lowest_score)) = sample_scores.peek().copied()
                    && score > lowest_score
                {
                    sample_scores.pop();
                    sample_scores.push(Reverse(score));
                }
            } else {
                fraction_candidate_count =
                    fraction_candidate_count.checked_add(1).ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "local Vortex sample fraction candidate count overflowed usize"
                                .to_string(),
                        )
                    })?;
            }
        }
        pre_limit_result_row_count = pre_limit_result_row_count
            .checked_add(materialized_rows)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex sample pre-limit result row count overflowed usize".to_string(),
                )
            })?;
        max_chunk_rows = max_chunk_rows.max(rows);
        arrays_read_count += 1;
    }
    let result_row_count = if let Some(fraction) = sample_fraction {
        fractional_sample_size(fraction_candidate_count, fraction)?
    } else {
        sample_scores.len()
    };
    let source = UniversalInputSource::from_dataset_uri(source_uri.clone())?;
    let reader_generated_prepared_batch_report = if encoded_kernel_inputs.is_empty() {
        plan_vortex_reader_generated_prepared_batch_envelopes(&source, &reader_splits)
    } else {
        plan_vortex_reader_generated_prepared_batch_kernel_inputs(
            &source,
            &reader_splits,
            &encoded_kernel_inputs,
        )
    };
    Ok(LocalVortexScan {
        source_row_count,
        result_row_count,
        pre_limit_result_row_count,
        arrays_read_count,
        reader_splits,
        reader_generated_prepared_batch_report,
        max_chunk_rows,
        max_parallelism_requested: policy.max_parallelism,
        scan_concurrency_per_worker: policy.scan_concurrency_per_worker(),
        projected_columns: declared_columns,
        filter_pushdown_applied,
        projection_pushdown_applied,
        source_order_limit,
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn deterministic_sample_score(seed: u64, row_index: usize) -> u64 {
    let row = u64::try_from(row_index).map_or(u64::MAX, |value| value);
    let mut value = seed ^ row.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(feature = "vortex-local-primitives")]
fn projection_scan_plan(
    dtype: &vortex::array::dtype::DType,
    projection: &ProjectionRequest,
    primitive_kind: VortexQueryPrimitiveKind,
) -> Result<LocalVortexScanPlan> {
    use vortex::array::expr::{root, select};

    let projected_columns = projected_column_names(dtype, projection, primitive_kind)?;
    let projection_expr = if dtype.is_primitive() {
        None
    } else {
        let field_names = projected_columns
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        Some(select(field_names, root()))
    };
    Ok(LocalVortexScanPlan {
        filter: None,
        projection: projection_expr,
        projected_columns,
        source_order_limit: None,
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn projected_column_names(
    dtype: &vortex::array::dtype::DType,
    projection: &ProjectionRequest,
    primitive_kind: VortexQueryPrimitiveKind,
) -> Result<Vec<String>> {
    match projection {
        ProjectionRequest::All => local_field_names(dtype, primitive_kind),
        ProjectionRequest::Columns(columns) => {
            let available = local_field_names(dtype, primitive_kind)?;
            let available_set = available
                .iter()
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>();
            let mut out = Vec::new();
            for column in columns {
                if !available_set.contains(column.as_str()) {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "projection column '{}' was not found in local Vortex target",
                        column.as_str()
                    )));
                }
                out.push(column.as_str().to_string());
            }
            Ok(out)
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn local_field_names(
    dtype: &vortex::array::dtype::DType,
    primitive_kind: VortexQueryPrimitiveKind,
) -> Result<Vec<String>> {
    use vortex::array::dtype::DType;

    match dtype {
        DType::Struct(fields, _) => Ok(fields
            .names()
            .iter()
            .map(|name| name.as_ref().to_string())
            .collect()),
        DType::Primitive(_, _) => Ok(vec!["value".to_string()]),
        other => Err(ShardLoomError::InvalidOperation(format!(
            "local primitive {} does not support top-level dtype {other:?}",
            primitive_kind.as_str()
        ))),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn predicate_to_vortex_expr(
    predicate: &PredicateExpr,
    dtype: &vortex::array::dtype::DType,
    primitive_kind: VortexQueryPrimitiveKind,
) -> Result<vortex::array::expr::Expression> {
    use vortex::array::expr::{eq, gt, gt_eq, is_not_null, is_null, lit, lt, lt_eq, not_eq};

    match predicate {
        PredicateExpr::AlwaysTrue => Ok(lit(true)),
        PredicateExpr::AlwaysFalse => Ok(lit(false)),
        PredicateExpr::IsNull { column } => {
            let (lhs, _) = predicate_field_expr(dtype, column.as_str(), primitive_kind)?;
            Ok(is_null(lhs))
        }
        PredicateExpr::IsNotNull { column } => {
            let (lhs, _) = predicate_field_expr(dtype, column.as_str(), primitive_kind)?;
            Ok(is_not_null(lhs))
        }
        PredicateExpr::Compare { column, op, value } => {
            let (lhs, field_dtype) = predicate_field_expr(dtype, column.as_str(), primitive_kind)?;
            let rhs = stat_value_to_vortex_literal(value, &field_dtype, primitive_kind)?;
            Ok(match op {
                ComparisonOp::Eq => eq(lhs, rhs),
                ComparisonOp::NotEq => not_eq(lhs, rhs),
                ComparisonOp::Lt => lt(lhs, rhs),
                ComparisonOp::LtEq => lt_eq(lhs, rhs),
                ComparisonOp::Gt => gt(lhs, rhs),
                ComparisonOp::GtEq => gt_eq(lhs, rhs),
            })
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn predicate_field_expr(
    dtype: &vortex::array::dtype::DType,
    column: &str,
    primitive_kind: VortexQueryPrimitiveKind,
) -> Result<(vortex::array::expr::Expression, vortex::array::dtype::DType)> {
    use vortex::array::dtype::DType;
    use vortex::array::expr::{col, root};

    match dtype {
        DType::Primitive(_, _) if column == "value" => Ok((root(), dtype.clone())),
        DType::Primitive(_, _) => Err(ShardLoomError::InvalidOperation(format!(
            "top-level primitive Vortex arrays expose the implicit column `value`, not `{column}`"
        ))),
        DType::Struct(fields, _) => {
            let Some(field_dtype) = fields.field(column) else {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "predicate column '{column}' was not found in local Vortex target"
                )));
            };
            Ok((col(column.to_string()), field_dtype))
        }
        other => Err(ShardLoomError::InvalidOperation(format!(
            "local primitive {} does not support predicate dtype {other:?}",
            primitive_kind.as_str()
        ))),
    }
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn stat_value_to_vortex_literal(
    value: &StatValue,
    dtype: &vortex::array::dtype::DType,
    primitive_kind: VortexQueryPrimitiveKind,
) -> Result<vortex::array::expr::Expression> {
    use vortex::array::dtype::{DType, PType};
    use vortex::array::expr::lit;

    match dtype {
        DType::Bool(_) => match value {
            StatValue::Boolean(value) => Ok(lit(*value)),
            _ => Err(ShardLoomError::InvalidOperation(
                "local primitive boolean predicates require boolean literals".to_string(),
            )),
        },
        DType::Utf8(_) => match value {
            StatValue::Utf8(value) => Ok(lit(value.as_str())),
            _ => Err(ShardLoomError::InvalidOperation(
                "local primitive UTF-8 predicates require string literals".to_string(),
            )),
        },
        DType::Primitive(ptype, _) => match ptype {
            PType::U8 => Ok(lit(u8::try_from(stat_value_to_u64(value)?)
                .map_err(|_| literal_out_of_range("u8", primitive_kind))?)),
            PType::U16 => Ok(lit(u16::try_from(stat_value_to_u64(value)?)
                .map_err(|_| literal_out_of_range("u16", primitive_kind))?)),
            PType::U32 => Ok(lit(u32::try_from(stat_value_to_u64(value)?)
                .map_err(|_| literal_out_of_range("u32", primitive_kind))?)),
            PType::U64 => Ok(lit(stat_value_to_u64(value)?)),
            PType::I8 => Ok(lit(i8::try_from(stat_value_to_i64(value)?)
                .map_err(|_| literal_out_of_range("i8", primitive_kind))?)),
            PType::I16 => Ok(lit(i16::try_from(stat_value_to_i64(value)?)
                .map_err(|_| literal_out_of_range("i16", primitive_kind))?)),
            PType::I32 => Ok(lit(i32::try_from(stat_value_to_i64(value)?)
                .map_err(|_| literal_out_of_range("i32", primitive_kind))?)),
            PType::I64 => Ok(lit(stat_value_to_i64(value)?)),
            PType::F32 => Ok(lit(stat_value_to_f64(value)? as f32)),
            PType::F64 => Ok(lit(stat_value_to_f64(value)?)),
            other @ PType::F16 => Err(ShardLoomError::InvalidOperation(format!(
                "local primitive {} does not support predicate ptype {other:?}",
                primitive_kind.as_str()
            ))),
        },
        other => Err(ShardLoomError::InvalidOperation(format!(
            "local primitive {} does not support predicate literal dtype {other:?}",
            primitive_kind.as_str()
        ))),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn literal_out_of_range(
    type_name: &'static str,
    primitive_kind: VortexQueryPrimitiveKind,
) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local primitive {} predicate literal is out of range for {type_name}",
        primitive_kind.as_str()
    ))
}

#[cfg(feature = "vortex-local-primitives")]
fn stat_value_to_u64(value: &StatValue) -> Result<u64> {
    match value {
        StatValue::UInt64(value) => Ok(*value),
        StatValue::Int64(value) => u64::try_from(*value).map_err(|_| {
            ShardLoomError::InvalidOperation(
                "local primitive unsigned predicates require non-negative integer literals"
                    .to_string(),
            )
        }),
        _ => Err(ShardLoomError::InvalidOperation(
            "local primitive unsigned predicates require integer literals".to_string(),
        )),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn stat_value_to_i64(value: &StatValue) -> Result<i64> {
    match value {
        StatValue::Int64(value) => Ok(*value),
        StatValue::UInt64(value) => i64::try_from(*value).map_err(|_| {
            ShardLoomError::InvalidOperation(
                "local primitive signed predicate literal exceeded i64".to_string(),
            )
        }),
        _ => Err(ShardLoomError::InvalidOperation(
            "local primitive signed predicates require integer literals".to_string(),
        )),
    }
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::cast_precision_loss)]
fn stat_value_to_f64(value: &StatValue) -> Result<f64> {
    match value {
        StatValue::Float64(value) => Ok(*value),
        StatValue::Int64(value) => Ok(*value as f64),
        StatValue::UInt64(value) => Ok(*value as f64),
        _ => Err(ShardLoomError::InvalidOperation(
            "local primitive float predicates require numeric literals".to_string(),
        )),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn usize_to_u64(value: usize) -> Result<u64> {
    u64::try_from(value).map_err(|_| {
        ShardLoomError::InvalidOperation("local primitive row count exceeded u64".to_string())
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn vortex_error(error: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!("Vortex local primitive failed: {error}"))
}

#[cfg(all(test, feature = "vortex-local-primitives"))]
mod tests {
    use super::*;
    use crate::{
        VortexEncodedValuePredicateBatch, VortexReaderBackedEncodedExecutionStatus,
        VortexReaderGeneratedPreparedBatchStatus, VortexSourceBackedEncodedValuePredicateBatch,
        execute_vortex_reader_backed_filter_from_encoded_value_batches,
    };
    use shardloom_core::{
        ColumnRef, CorrectnessFixture, CorrectnessValidationPlan, EncodedSegment,
        EncodedValueBatch, EncodedValueRun, EncodingKind, ExecutionCertificateStatus, LayoutKind,
        LogicalDType, Nullability, SegmentId, SegmentLayout, SegmentStats, UniversalInputSource,
    };

    fn unique_vortex_path(name: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "shardloom-{name}-{}-{nanos}.vortex",
            std::process::id()
        ))
    }

    fn write_array(path: &std::path::Path, array: &vortex::array::ArrayRef) -> Result<()> {
        use vortex::VortexSessionDefault as _;
        use vortex::file::WriteOptionsSessionExt as _;
        use vortex::io::runtime::BlockingRuntime as _;
        use vortex::io::runtime::single::SingleThreadRuntime;
        use vortex::io::session::RuntimeSessionExt as _;
        use vortex::session::VortexSession;

        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let mut bytes = Vec::new();
        let summary = runtime
            .block_on(
                session
                    .write_options()
                    .write(&mut bytes, array.to_array_stream()),
            )
            .map_err(vortex_error)?;
        assert_eq!(
            summary.row_count(),
            u64::try_from(array.len()).expect("len")
        );
        let workspace_root = shardloom_core::infer_local_output_workspace_root(path)?;
        shardloom_core::write_workspace_safe_bytes(
            workspace_root,
            path,
            false,
            "local primitive Vortex fixture",
            &bytes,
        )
        .map(|_| ())
    }

    fn write_struct_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{PrimitiveArray, StructArray};
        use vortex::array::dtype::FieldNames;
        use vortex::array::validity::Validity;

        let array = StructArray::try_new(
            FieldNames::from(["value", "metric"]),
            vec![
                [1_u32, 2, 3, 4, 5]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
                [10_i64, 20, 30, 40, 50]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
            ],
            5,
            Validity::NonNullable,
        )
        .map_err(vortex_error)?;
        write_array(path, &array.into_array())
    }

    fn write_duplicate_struct_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{PrimitiveArray, StructArray};
        use vortex::array::dtype::FieldNames;
        use vortex::array::validity::Validity;

        let array = StructArray::try_new(
            FieldNames::from(["value", "metric"]),
            vec![
                [1_u32, 1, 2, 2, 3]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
                [10_i64, 10, 20, 99, 30]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
            ],
            5,
            Validity::NonNullable,
        )
        .map_err(vortex_error)?;
        write_array(path, &array.into_array())
    }

    fn write_nullable_struct_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{PrimitiveArray, StructArray};
        use vortex::array::dtype::FieldNames;
        use vortex::array::validity::Validity;

        let values = PrimitiveArray::new(
            vec![1_u32, 0, 3, 4, 5],
            Validity::from_iter([true, false, true, true, true]),
        )
        .into_array();
        let array = StructArray::try_new(
            FieldNames::from(["value"]),
            vec![values],
            5,
            Validity::NonNullable,
        )
        .map_err(vortex_error)?;
        write_array(path, &array.into_array())
    }

    #[test]
    #[ignore = "fixture regeneration helper; writes shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex"]
    fn regenerate_checked_in_local_primitive_struct_fixture() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("local_primitive_struct_five.vortex");
        write_struct_fixture(&path).expect("fixture");
    }

    fn write_primitive_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::PrimitiveArray;

        let array = [7_u64, 8, 9]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        write_array(path, &array)
    }

    fn write_constant_primitive_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::ConstantArray;

        let array = ConstantArray::new(7_i64, 4).into_array();
        write_array(path, &array)
    }

    fn correctness_fixture(id: &str) -> CorrectnessFixture {
        CorrectnessValidationPlan::default_foundation_plan()
            .fixtures
            .into_iter()
            .find(|fixture| fixture.id.as_str() == id)
            .expect("fixture")
    }

    fn checked_in_struct_fixture_path() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("local_primitive_struct_five.vortex")
    }

    fn checked_in_struct_fixture_uri() -> DatasetUri {
        DatasetUri::new(checked_in_struct_fixture_path().display().to_string()).expect("uri")
    }

    fn prepared_metric_segment(id: &str, row_count: u64) -> EncodedSegment {
        EncodedSegment::new(
            SegmentId::new(id).expect("segment"),
            ColumnRef::new("metric").expect("column"),
            LogicalDType::Int64,
            Nullability::Nullable,
            SegmentLayout::new(EncodingKind::Constant, LayoutKind::Flat),
            SegmentStats::with_row_count(row_count),
        )
    }

    #[test]
    fn count_all_scans_local_vortex_without_decode() {
        let path = unique_vortex_path("count-all");
        write_primitive_fixture(&path).expect("fixture");
        let uri = DatasetUri::new(path.display().to_string()).expect("uri");
        let request = VortexQueryPrimitiveRequest::count_all(uri);

        let report = execute_vortex_local_primitive(&request).expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.result_summary.as_deref(), Some("3"));
        assert!(report.data_read);
        assert!(report.streaming_scan_used);
        assert!(!report.full_stream_collected);
        assert_eq!(report.max_parallelism_requested, 1);
        assert_eq!(report.scan_concurrency_per_worker, 1);
        assert!(report.arrays_read_count > 0);
        assert!(report.max_chunk_rows > 0);
        assert!(!report.filter_pushdown_applied);
        assert!(!report.projection_pushdown_applied);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn local_scan_lowers_constant_reader_chunks_into_encoded_kernel_inputs() {
        let path = unique_vortex_path("constant-kernel-input");
        write_constant_primitive_fixture(&path).expect("fixture");
        let uri = DatasetUri::new(path.display().to_string()).expect("uri");
        let request = VortexQueryPrimitiveRequest::count_all(uri);

        let report = execute_vortex_local_primitive(&request).expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.result_summary.as_deref(), Some("4"));
        let prepared_report = report
            .reader_generated_prepared_batch_report
            .as_ref()
            .expect("reader-generated prepared batch report");
        assert_eq!(
            prepared_report.status,
            VortexReaderGeneratedPreparedBatchStatus::PreparedEncodedKernelInputs
        );
        assert!(prepared_report.reader_generated_prepared_batches);
        assert!(prepared_report.reader_chunk_envelopes_available);
        assert!(prepared_report.provider_boundary.is_policy_admitted());
        assert!(prepared_report.encoded_value_batch_available);
        assert!(prepared_report.encoded_projection_batch_available);
        assert_eq!(
            prepared_report.encoded_kernel_input_count,
            report.arrays_read_count
        );
        assert!(!prepared_report.kernel_input_lowering_blocked);
        assert!(prepared_report.runtime_execution_allowed);
        assert_eq!(prepared_report.residual_executor, "none");
        assert_eq!(
            prepared_report.representation_after,
            "reader_generated_prepared_encoded_kernel_input"
        );
        assert!(prepared_report.encoded_kernel_inputs_source_uri_matches_source);
        assert!(prepared_report.encoded_kernel_input_split_refs_covered_by_reader);
        assert!(prepared_report.encoded_kernel_input_row_counts_match_reader);
        assert!(prepared_report.encoded_kernel_input_mapping_evidence_complete);
        assert!(prepared_report.avoids_forbidden_effects());
        assert!(!prepared_report.has_errors());
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.row_read);
        assert!(!report.arrow_converted);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn reader_chunk_dictionary_values_lower_into_encoded_kernel_inputs() {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{DictArray, PrimitiveArray};

        let codes = [0_u8, 1, 0, 2]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let values = [10_u64, 20, 30]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let chunk = DictArray::try_new(codes, values)
            .expect("dictionary array")
            .into_array();
        let source_uri = DatasetUri::new("file:///tmp/dictionary-values.vortex").expect("uri");

        let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
            &source_uri,
            "split-dict",
            &chunk,
        )
        .expect("kernel inputs");

        assert_eq!(inputs.len(), 1);
        let input = inputs.first().expect("input");
        assert!(input.provider_boundary.is_policy_admitted());
        assert!(input.mapping_evidence_complete());
        assert!(!input.has_forbidden_effects());
        assert_eq!(
            input.batch.segment.layout.encoding,
            EncodingKind::Dictionary
        );
        assert_eq!(input.batch.segment.dtype, LogicalDType::UInt64);
        assert_eq!(input.batch.segment.stats.row_count, Some(4));
        assert_eq!(input.batch.segment.stats.null_count, Some(0));
        match &input.batch.values {
            EncodedValueBatch::Dictionary { dictionary, codes } => {
                assert_eq!(
                    dictionary,
                    &vec![
                        Some(StatValue::UInt64(10)),
                        Some(StatValue::UInt64(20)),
                        Some(StatValue::UInt64(30)),
                    ]
                );
                assert_eq!(codes, &vec![Some(0), Some(1), Some(0), Some(2)]);
            }
            other => panic!("expected dictionary batch, got {other:?}"),
        }
    }

    #[test]
    fn reader_chunk_utf8_dictionary_values_lower_into_encoded_kernel_inputs() {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{DictArray, PrimitiveArray, VarBinViewArray};

        let codes = [0_u8, 1, 0, 2]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let values = VarBinViewArray::from_iter_str(["alpha", "beta", "gamma"]).into_array();
        let chunk = DictArray::try_new(codes, values)
            .expect("utf8 dictionary array")
            .into_array();
        let source_uri = DatasetUri::new("file:///tmp/utf8-dictionary.vortex").expect("uri");

        let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
            &source_uri,
            "split-utf8-dict",
            &chunk,
        )
        .expect("kernel inputs");

        assert_eq!(inputs.len(), 1);
        let input = inputs.first().expect("input");
        assert!(input.provider_boundary.is_policy_admitted());
        assert!(input.mapping_evidence_complete());
        assert!(!input.has_forbidden_effects());
        assert_eq!(
            input.batch.segment.layout.encoding,
            EncodingKind::Dictionary
        );
        assert_eq!(input.batch.segment.dtype, LogicalDType::Utf8);
        assert_eq!(input.batch.segment.stats.row_count, Some(4));
        assert_eq!(input.batch.segment.stats.null_count, Some(0));
        match &input.batch.values {
            EncodedValueBatch::Dictionary { dictionary, codes } => {
                assert_eq!(
                    dictionary,
                    &vec![
                        Some(StatValue::Utf8("alpha".to_string())),
                        Some(StatValue::Utf8("beta".to_string())),
                        Some(StatValue::Utf8("gamma".to_string())),
                    ]
                );
                assert_eq!(codes, &vec![Some(0), Some(1), Some(0), Some(2)]);
            }
            other => panic!("expected dictionary batch, got {other:?}"),
        }
    }

    #[test]
    fn reader_chunk_struct_lowers_utf8_dictionary_child_without_panic() {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{DictArray, PrimitiveArray, StructArray, VarBinViewArray};
        use vortex::array::dtype::FieldNames;
        use vortex::array::validity::Validity;

        let dim_key = [1_u32, 2, 1, 3]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let label_codes = [0_u8, 1, 0, 2]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let label_values = VarBinViewArray::from_iter_str(["one", "two", "three"]).into_array();
        let dim_label = DictArray::try_new(label_codes, label_values)
            .expect("utf8 dictionary labels")
            .into_array();
        let chunk = StructArray::try_new(
            FieldNames::from(["dim_key", "dim_label"]),
            vec![dim_key, dim_label],
            4,
            Validity::NonNullable,
        )
        .expect("struct chunk")
        .into_array();
        let source_uri = DatasetUri::new("file:///tmp/hash-join-dim.vortex").expect("uri");

        let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
            &source_uri,
            "split-hash-join-dim",
            &chunk,
        )
        .expect("kernel inputs");

        assert_eq!(inputs.len(), 1);
        assert!(
            inputs
                .iter()
                .all(VortexReaderGeneratedEncodedKernelInput::mapping_evidence_complete)
        );
        assert!(inputs.iter().all(|input| !input.has_forbidden_effects()));
        let columns = inputs
            .iter()
            .map(|input| {
                (
                    input.batch.segment.column.as_str().to_string(),
                    input.batch.segment.dtype.clone(),
                    input.batch.segment.layout.encoding.clone(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            columns,
            vec![(
                "dim_label".to_string(),
                LogicalDType::Utf8,
                EncodingKind::Dictionary,
            )]
        );
    }

    #[test]
    fn reader_chunk_run_end_values_lower_into_encoded_kernel_inputs() {
        use vortex::array::IntoArray as _;
        use vortex::array::VortexSessionExecute as _;
        use vortex::array::arrays::PrimitiveArray;
        use vortex::encodings::runend::RunEnd;

        let ends = [2_u64, 5]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let values = [5_i64, 9]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
        let chunk = RunEnd::try_new(ends, values, &mut ctx)
            .expect("run-end array")
            .into_array();
        let source_uri = DatasetUri::new("file:///tmp/run-end-values.vortex").expect("uri");

        let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
            &source_uri,
            "split-run-end",
            &chunk,
        )
        .expect("kernel inputs");

        assert_eq!(inputs.len(), 1);
        let input = inputs.first().expect("input");
        assert!(input.provider_boundary.is_policy_admitted());
        assert!(input.mapping_evidence_complete());
        assert!(!input.has_forbidden_effects());
        assert_eq!(input.batch.segment.layout.encoding, EncodingKind::RunLength);
        assert_eq!(input.batch.segment.dtype, LogicalDType::Int64);
        assert_eq!(input.batch.segment.stats.row_count, Some(5));
        assert_eq!(input.batch.segment.stats.null_count, Some(0));
        assert_eq!(input.batch.segment.stats.run_count, Some(2));
        match &input.batch.values {
            EncodedValueBatch::RunLength { runs } => {
                assert_eq!(
                    runs,
                    &vec![
                        EncodedValueRun::new(Some(StatValue::Int64(5)), 2),
                        EncodedValueRun::new(Some(StatValue::Int64(9)), 3),
                    ]
                );
            }
            other => panic!("expected run-length batch, got {other:?}"),
        }
    }

    #[test]
    fn reader_chunk_bitpacked_values_lower_into_encoded_kernel_inputs() {
        use vortex::array::IntoArray as _;
        use vortex::array::VortexSessionExecute as _;
        use vortex::array::arrays::PrimitiveArray;
        use vortex::encodings::fastlanes::BitPackedData;

        let values = [0_u8, 1, 0, 1, 1]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
        let chunk = BitPackedData::encode(&values, 1, &mut ctx)
            .expect("bit-packed array")
            .as_array()
            .clone();
        let source_uri = DatasetUri::new("file:///tmp/bitpacked-values.vortex").expect("uri");

        let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
            &source_uri,
            "split-bitpacked",
            &chunk,
        )
        .expect("kernel inputs");

        assert_eq!(inputs.len(), 1);
        let input = inputs.first().expect("input");
        assert!(input.provider_boundary.is_policy_admitted());
        assert!(input.mapping_evidence_complete());
        assert!(!input.has_forbidden_effects());
        assert_eq!(input.batch.segment.layout.encoding, EncodingKind::BitPacked);
        assert_eq!(input.batch.segment.dtype, LogicalDType::UInt64);
        assert_eq!(input.batch.segment.stats.row_count, Some(5));
        assert_eq!(input.batch.segment.stats.null_count, Some(0));
        match &input.batch.values {
            EncodedValueBatch::BitPackedUnsigned { bit_width, values } => {
                assert_eq!(*bit_width, 1);
                assert_eq!(values, &vec![0, 1, 0, 1, 1]);
            }
            other => panic!("expected bit-packed batch, got {other:?}"),
        }
    }

    #[test]
    fn reader_chunk_sequence_values_lower_into_encoded_kernel_inputs() {
        use vortex::array::IntoArray as _;
        use vortex::array::dtype::{Nullability as VortexNullability, PType};
        use vortex::array::scalar::PValue;
        use vortex::encodings::sequence::Sequence;

        let chunk = Sequence::try_new(
            PValue::U32(10),
            PValue::U32(3),
            PType::U32,
            VortexNullability::NonNullable,
            4,
        )
        .expect("sequence array")
        .into_array();
        let source_uri = DatasetUri::new("file:///tmp/sequence-values.vortex").expect("uri");

        let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
            &source_uri,
            "split-sequence",
            &chunk,
        )
        .expect("kernel inputs");

        assert_eq!(inputs.len(), 1);
        let input = inputs.first().expect("input");
        assert!(input.provider_boundary.is_policy_admitted());
        assert!(input.mapping_evidence_complete());
        assert!(!input.has_forbidden_effects());
        assert_eq!(input.batch.segment.layout.encoding, EncodingKind::Sequence);
        assert_eq!(input.batch.segment.dtype, LogicalDType::UInt64);
        assert_eq!(input.batch.segment.stats.row_count, Some(4));
        assert_eq!(input.batch.segment.stats.null_count, Some(0));
        match &input.batch.values {
            EncodedValueBatch::ArithmeticSequence {
                base,
                multiplier,
                row_count,
            } => {
                assert_eq!(base, &StatValue::UInt64(10));
                assert_eq!(multiplier, &StatValue::UInt64(3));
                assert_eq!(*row_count, 4);
            }
            other => panic!("expected sequence batch, got {other:?}"),
        }
    }

    #[test]
    fn reader_generated_conjunctive_filter_intersects_bitpacked_and_sequence_inputs() {
        use vortex::array::IntoArray as _;
        use vortex::array::VortexSessionExecute as _;
        use vortex::array::arrays::{PrimitiveArray, StructArray};
        use vortex::array::dtype::{FieldNames, Nullability as VortexNullability, PType};
        use vortex::array::scalar::PValue;
        use vortex::array::validity::Validity;
        use vortex::encodings::fastlanes::BitPackedData;
        use vortex::encodings::sequence::Sequence;

        let flag_values = [0_u8, 1, 0, 1, 1]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
        let flag = BitPackedData::encode(&flag_values, 1, &mut ctx)
            .expect("bit-packed flag")
            .as_array()
            .clone();
        let value = Sequence::try_new(
            PValue::U32(4_990),
            PValue::U32(5),
            PType::U32,
            VortexNullability::NonNullable,
            5,
        )
        .expect("sequence value")
        .into_array();
        let chunk = StructArray::try_new(
            FieldNames::from(["flag", "value"]),
            vec![flag, value],
            5,
            Validity::NonNullable,
        )
        .expect("struct chunk")
        .into_array();
        let source_uri = DatasetUri::new("file:///tmp/filter-columns.vortex").expect("uri");
        let source = UniversalInputSource::from_dataset_uri(source_uri.clone()).expect("source");
        let split = VortexReaderBackedSplitEvidence::local_scan_chunk(
            source_uri.clone(),
            0,
            chunk.len(),
            chunk.dtype().to_string(),
            chunk.encoding_id().to_string(),
            chunk.nchildren(),
            chunk.nbuffers(),
        )
        .expect("split");
        let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
            &source_uri,
            &split.split_ref,
            &chunk,
        )
        .expect("kernel inputs");
        let predicates = vec![
            PredicateExpr::Compare {
                column: ColumnRef::new("flag").expect("flag column"),
                op: ComparisonOp::Eq,
                value: StatValue::UInt64(1),
            },
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("value column"),
                op: ComparisonOp::GtEq,
                value: StatValue::UInt64(5_000),
            },
        ];

        let bridge =
            crate::execute_vortex_reader_generated_conjunctive_filter_from_encoded_kernel_inputs(
                &predicates,
                &source,
                &[split],
                &inputs,
            )
            .expect("bridge");

        assert_eq!(inputs.len(), 2);
        assert_eq!(bridge.status.as_str(), "intersected_selection_vectors");
        assert_eq!(bridge.intersection_count, 1);
        assert_eq!(bridge.selected_row_count, Some(2));
        assert!(bridge.filter_column_batches_consumed);
        assert!(bridge.selection_vector_intersection_certified);
        assert!(!bridge.data_decoded);
        assert!(!bridge.data_materialized);
        assert!(!bridge.fallback_attempted);
        assert!(!bridge.external_engine_invoked);
    }

    #[test]
    fn nullable_dictionary_reader_chunk_lowering_stays_blocked() {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{DictArray, PrimitiveArray};
        use vortex::array::validity::Validity;

        let codes =
            PrimitiveArray::new(vec![0_u8, 0], Validity::from_iter([true, false])).into_array();
        let values =
            PrimitiveArray::new(vec![10_u64, 20], Validity::from_iter([true, true])).into_array();
        let chunk = DictArray::try_new(codes, values)
            .expect("nullable dictionary array")
            .into_array();
        let source_uri = DatasetUri::new("file:///tmp/nullable-dictionary.vortex").expect("uri");

        let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
            &source_uri,
            "split-nullable-dict",
            &chunk,
        )
        .expect("kernel inputs");

        assert!(inputs.is_empty());
    }

    #[test]
    fn nullable_utf8_dictionary_values_stay_blocked_before_bytes_access() {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{DictArray, PrimitiveArray, VarBinViewArray};

        let codes = [0_u8, 1, 0]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let values = VarBinViewArray::from_iter_nullable_str([Some("alpha"), None, Some("gamma")])
            .into_array();
        let chunk = DictArray::try_new(codes, values)
            .expect("nullable utf8 dictionary array")
            .into_array();
        let source_uri =
            DatasetUri::new("file:///tmp/nullable-utf8-dictionary.vortex").expect("uri");

        let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
            &source_uri,
            "split-nullable-utf8-dict",
            &chunk,
        )
        .expect("kernel inputs");

        assert!(inputs.is_empty());
    }

    #[test]
    fn nullable_run_end_reader_chunk_lowering_stays_blocked() {
        use vortex::array::IntoArray as _;
        use vortex::array::VortexSessionExecute as _;
        use vortex::array::arrays::PrimitiveArray;
        use vortex::array::validity::Validity;
        use vortex::encodings::runend::RunEnd;

        let ends = [2_u64, 5]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let values =
            PrimitiveArray::new(vec![5_i64, 9], Validity::from_iter([true, false])).into_array();
        let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
        let chunk = RunEnd::try_new(ends, values, &mut ctx)
            .expect("nullable run-end array")
            .into_array();
        let source_uri = DatasetUri::new("file:///tmp/nullable-run-end.vortex").expect("uri");

        let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
            &source_uri,
            "split-nullable-run-end",
            &chunk,
        )
        .expect("kernel inputs");

        assert!(inputs.is_empty());
    }

    #[test]
    fn sparse_reader_chunk_lowering_stays_blocked() {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::PrimitiveArray;
        use vortex::array::scalar::Scalar;
        use vortex::encodings::sparse::Sparse;

        let indices = [1_u64, 3]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let values = [42_i32, 77]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let chunk = Sparse::try_new(indices, values, 5, Scalar::from(0_i32))
            .expect("sparse array")
            .into_array();
        let source_uri = DatasetUri::new("file:///tmp/sparse.vortex").expect("uri");

        let inputs = reader_generated_encoded_kernel_inputs_from_vortex_chunk(
            &source_uri,
            "split-sparse",
            &chunk,
        )
        .expect("kernel inputs");

        assert!(inputs.is_empty());
    }

    #[test]
    fn count_where_executes_over_local_vortex_values() {
        let path = unique_vortex_path("count-where");
        write_struct_fixture(&path).expect("fixture");
        let uri = DatasetUri::new(path.display().to_string()).expect("uri");
        let request = VortexQueryPrimitiveRequest::count_where(
            uri,
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
        );

        let report = execute_vortex_local_primitive(&request).expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.mode,
            VortexLocalPrimitiveExecutionMode::VortexScanPushdown
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_selected, Some(3));
        assert_eq!(report.result_summary.as_deref(), Some("3"));
        assert!(report.data_read);
        assert!(report.streaming_scan_used);
        assert!(!report.full_stream_collected);
        assert_eq!(report.max_parallelism_requested, 1);
        assert_eq!(report.scan_concurrency_per_worker, 1);
        assert!(report.arrays_read_count > 0);
        assert_eq!(report.reader_splits.len(), report.arrays_read_count);
        assert!(report.reader_splits.iter().all(|split| split.data_read));
        assert!(
            report
                .reader_splits
                .iter()
                .all(|split| !split.has_forbidden_effects())
        );
        let prepared_report = report
            .reader_generated_prepared_batch_report
            .as_ref()
            .expect("reader-generated prepared batch report");
        assert!(prepared_report.reader_generated_prepared_batches);
        assert_eq!(
            prepared_report.generated_batch_count,
            report.arrays_read_count
        );
        assert_eq!(prepared_report.total_rows, 3);
        assert!(prepared_report.kernel_input_lowering_blocked);
        assert!(!prepared_report.encoded_value_batch_available);
        assert!(!prepared_report.encoded_projection_batch_available);
        assert!(!prepared_report.runtime_execution_allowed);
        assert!(prepared_report.avoids_forbidden_effects());
        assert!(report.max_chunk_rows > 0);
        assert!(report.filter_pushdown_applied);
        assert!(report.upstream_filter_expression_used);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.materialization_boundary_reported);
        assert!(!report.row_read);
        assert!(!report.arrow_converted);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn local_scan_split_refs_bind_reader_backed_prepared_filter_batches() {
        let path = unique_vortex_path("reader-backed-bind");
        write_struct_fixture(&path).expect("fixture");
        let uri = DatasetUri::new(path.display().to_string()).expect("uri");
        let predicate = PredicateExpr::Compare {
            column: ColumnRef::new("metric").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(5),
        };
        let request = VortexQueryPrimitiveRequest::count_where(uri.clone(), predicate.clone());

        let local_report = execute_vortex_local_primitive(&request).expect("local report");
        let source = UniversalInputSource::from_dataset_uri(uri).expect("source");
        let source_uri = source.uri.clone().expect("source uri");
        let first_split = local_report
            .reader_splits
            .first()
            .expect("reader split")
            .clone();
        let batch = VortexSourceBackedEncodedValuePredicateBatch::new(
            source_uri,
            first_split.split_ref.clone(),
            VortexEncodedValuePredicateBatch::new(
                prepared_metric_segment("reader-split.metric", first_split.row_count as u64),
                EncodedValueBatch::Constant {
                    value: Some(StatValue::Int64(5)),
                    row_count: first_split.row_count as u64,
                },
            ),
        )
        .expect("prepared batch");

        let bridge_report = execute_vortex_reader_backed_filter_from_encoded_value_batches(
            &predicate,
            &source,
            &local_report.reader_splits,
            &[batch],
        )
        .expect("bridge report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(
            bridge_report.status,
            VortexReaderBackedEncodedExecutionStatus::ExecutedReaderValidatedPreparedEncodedBatches
        );
        assert!(bridge_report.data_read);
        assert!(bridge_report.runtime_execution_allowed);
        assert!(bridge_report.prepared_batch_split_refs_covered_by_reader);
        assert!(bridge_report.reader_validated_prepared_batches_consumed);
        assert!(!bridge_report.reader_generated_prepared_batches);
        assert!(bridge_report.avoids_forbidden_effects());
        assert!(!bridge_report.has_errors());
    }

    #[test]
    fn count_where_comparison_excludes_null_values() {
        let path = unique_vortex_path("count-where-null");
        write_nullable_struct_fixture(&path).expect("fixture");
        let uri = DatasetUri::new(path.display().to_string()).expect("uri");
        let request = VortexQueryPrimitiveRequest::count_where(
            uri,
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(0),
            },
        );

        let report = execute_vortex_local_primitive(&request).expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_selected, Some(4));
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.row_read);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn count_where_metadata_predicate_avoids_decode_and_materialization() {
        let path = unique_vortex_path("count-where-always-false");
        write_struct_fixture(&path).expect("fixture");
        let uri = DatasetUri::new(path.display().to_string()).expect("uri");
        let request = VortexQueryPrimitiveRequest::count_where(uri, PredicateExpr::AlwaysFalse);

        let report = execute_vortex_local_primitive(&request).expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.rows_selected, Some(0));
        assert_eq!(report.result_summary.as_deref(), Some("0"));
        assert!(report.data_read);
        assert!(report.streaming_scan_used);
        assert!(!report.full_stream_collected);
        assert!(report.filter_pushdown_applied);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.materialization_boundary_reported);
        assert!(!report.row_read);
        assert!(!report.arrow_converted);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn filter_predicate_executes_over_local_vortex_values() {
        let path = unique_vortex_path("filter");
        write_struct_fixture(&path).expect("fixture");
        let uri = DatasetUri::new(path.display().to_string()).expect("uri");
        let request = VortexQueryPrimitiveRequest::filter(
            uri,
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
        );

        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(2).expect("policy"),
        )
        .expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.mode,
            VortexLocalPrimitiveExecutionMode::VortexScanPushdown
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_selected, Some(3));
        assert_eq!(report.rows_projected, None);
        assert_eq!(report.result_summary.as_deref(), Some("3"));
        assert!(report.data_read);
        assert!(report.streaming_scan_used);
        assert!(!report.full_stream_collected);
        assert_eq!(report.max_parallelism_requested, 2);
        assert_eq!(report.scan_concurrency_per_worker, 2);
        assert!(report.filter_pushdown_applied);
        assert!(report.upstream_filter_expression_used);
        assert!(!report.projection_pushdown_applied);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.materialization_boundary_reported);
        assert!(!report.row_read);
        assert!(!report.arrow_converted);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn projection_reports_projected_columns_from_local_vortex() {
        let path = unique_vortex_path("project");
        write_struct_fixture(&path).expect("fixture");
        let uri = DatasetUri::new(path.display().to_string()).expect("uri");
        let request = VortexQueryPrimitiveRequest::project(
            uri,
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        );

        let report = execute_vortex_local_primitive(&request).expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.mode,
            VortexLocalPrimitiveExecutionMode::VortexScanPushdown
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_projected, Some(5));
        assert_eq!(report.projected_columns, vec!["metric".to_string()]);
        assert!(report.data_read);
        assert!(report.streaming_scan_used);
        assert!(!report.full_stream_collected);
        assert!(report.projection_pushdown_applied);
        assert!(report.upstream_projection_expression_used);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.materialization_boundary_reported);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn project_and_filter_apply_source_order_limit() {
        let path = unique_vortex_path("project-filter-limit");
        write_struct_fixture(&path).expect("fixture");
        let uri = DatasetUri::new(path.display().to_string()).expect("uri");
        let project_request = VortexQueryPrimitiveRequest::project(
            uri.clone(),
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        )
        .with_source_order_limit(2);
        let filter_request = VortexQueryPrimitiveRequest::filter(
            uri,
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
        )
        .with_source_order_limit(2);

        let project = execute_vortex_local_primitive(&project_request).expect("project report");
        let filter = execute_vortex_local_primitive(&filter_request).expect("filter report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(
            project.status,
            VortexLocalPrimitiveExecutionStatus::Executed
        );
        assert_eq!(project.rows_projected, Some(2));
        assert_eq!(project.source_order_limit_requested, Some(2));
        assert!(project.source_order_limit_applied);
        assert_eq!(project.source_order_limit_rows_output, Some(2));
        assert!(project.projection_pushdown_applied);
        assert!(!project.filter_pushdown_applied);
        assert!(!project.data_decoded);
        assert!(!project.data_materialized);

        assert_eq!(filter.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(filter.rows_selected, Some(2));
        assert_eq!(filter.source_order_limit_requested, Some(2));
        assert!(filter.source_order_limit_applied);
        assert_eq!(filter.source_order_limit_input_rows, Some(3));
        assert_eq!(filter.source_order_limit_rows_output, Some(2));
        assert!(filter.filter_pushdown_applied);
        assert!(!filter.projection_pushdown_applied);
        assert!(!filter.data_decoded);
        assert!(!filter.data_materialized);
    }

    #[test]
    fn filter_and_project_uses_single_vortex_scan_pushdown_path() {
        let path = unique_vortex_path("filter-project");
        write_struct_fixture(&path).expect("fixture");
        let uri = DatasetUri::new(path.display().to_string()).expect("uri");
        let request = VortexQueryPrimitiveRequest::filter_and_project(
            uri,
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        );

        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(2).expect("policy"),
        )
        .expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.mode,
            VortexLocalPrimitiveExecutionMode::VortexScanPushdown
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_selected, Some(3));
        assert_eq!(report.rows_projected, Some(3));
        assert_eq!(report.projected_columns, vec!["metric".to_string()]);
        assert!(report.data_read);
        assert!(report.streaming_scan_used);
        assert!(!report.full_stream_collected);
        assert_eq!(report.max_parallelism_requested, 2);
        assert_eq!(report.scan_concurrency_per_worker, 2);
        assert!(report.filter_pushdown_applied);
        assert!(report.projection_pushdown_applied);
        assert!(report.upstream_filter_expression_used);
        assert!(report.upstream_projection_expression_used);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.materialization_boundary_reported);
        assert!(!report.row_read);
        assert!(!report.arrow_converted);
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn filter_and_project_applies_source_order_residual_limit_after_scan_pushdown() {
        let uri = checked_in_struct_fixture_uri();
        let request = VortexQueryPrimitiveRequest::filter_and_project(
            uri,
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        )
        .with_source_order_limit(2);

        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(2).expect("policy"),
        )
        .expect("report");
        let certificate =
            local_primitive_native_io_certificate(&request, &report).expect("certificate");
        let fixture = local_primitive_correctness_fixture_for_request(&request, &report)
            .expect("limit fixture");
        let execution_certificate =
            local_primitive_execution_certificate(&fixture, &request, &report)
                .expect("execution certificate");

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.mode,
            VortexLocalPrimitiveExecutionMode::VortexScanPushdown
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_selected, Some(2));
        assert_eq!(report.rows_projected, Some(2));
        assert_eq!(report.source_order_limit_requested, Some(2));
        assert!(report.source_order_limit_applied);
        assert_eq!(report.source_order_limit_input_rows, Some(3));
        assert_eq!(report.source_order_limit_rows_output, Some(2));
        assert!(report.filter_pushdown_applied);
        assert!(report.projection_pushdown_applied);
        assert!(report.upstream_filter_expression_used);
        assert!(report.upstream_projection_expression_used);
        assert!(!report.data_decoded);
        assert!(!report.data_materialized);
        assert!(!report.row_read);
        assert!(!report.arrow_converted);
        assert!(!report.fallback_execution_allowed);
        assert!(certificate.is_certified());
        assert_eq!(
            certificate
                .source_pushdown_report
                .accepted_operation_order(),
            "filter,project"
        );
        assert_eq!(
            certificate
                .source_pushdown_report
                .rejected_operation_order(),
            "limit_pushdown"
        );
        assert_eq!(
            certificate
                .source_pushdown_report
                .residual_expression
                .as_deref(),
            Some("source_order_limit:2")
        );
        assert_eq!(
            fixture.id.as_str(),
            "vortex-local-filter-project-limit-struct-five"
        );
        assert!(execution_certificate.is_certified());
        assert!(!execution_certificate.fallback_attempted);
    }

    #[test]
    fn distinct_rows_materialize_bounded_vortex_rows_without_fallback() {
        let path = unique_vortex_path("distinct-rows");
        write_duplicate_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::distinct_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
            Some(PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(2),
            }),
        )
        .with_source_order_limit(2);

        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(2).expect("policy"),
        )
        .expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.mode,
            VortexLocalPrimitiveExecutionMode::VortexScanPushdown
        );
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::DistinctRows
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_selected, Some(2));
        assert_eq!(report.rows_projected, Some(2));
        assert_eq!(report.projected_columns, vec!["value".to_string()]);
        assert_eq!(
            report.result_summary.as_deref(),
            Some("distinct_rows=2 projected_columns=value")
        );
        assert_eq!(report.max_parallelism_requested, 2);
        assert_eq!(report.scan_concurrency_per_worker, 2);
        assert!(report.filter_pushdown_applied);
        assert!(report.projection_pushdown_applied);
        assert!(report.upstream_filter_expression_used);
        assert!(report.upstream_projection_expression_used);
        assert_eq!(report.source_order_limit_requested, Some(2));
        assert!(report.source_order_limit_applied);
        assert_eq!(report.source_order_limit_input_rows, Some(2));
        assert_eq!(report.source_order_limit_rows_output, Some(2));
        assert!(report.data_read);
        assert!(report.streaming_scan_used);
        assert!(!report.full_stream_collected);
        assert!(report.data_decoded);
        assert!(report.data_materialized);
        assert!(report.row_read);
        assert!(!report.arrow_converted);
        assert!(!report.external_effects_executed);
        assert!(!report.fallback_execution_allowed);
        assert!(report.materialization_boundary_reported);

        let certificate =
            local_primitive_native_io_certificate(&request, &report).expect("certificate");
        assert!(certificate.is_certified());
        assert_eq!(certificate.status(), "certified");
        assert_eq!(
            certificate.representation_transition_order(),
            "vortex_encoded->decoded_columnar,decoded_columnar->materialized_rows"
        );
        assert_eq!(
            certificate.materialization_boundary_order(),
            "local_primitive.distinct_rows.bounded_materialization"
        );
        assert_eq!(
            certificate
                .source_pushdown_report
                .accepted_operation_order(),
            "filter,project,distinct"
        );
        assert!(certificate.sink_requirement_report.requires_rows);
        assert!(certificate.adapter_fidelity_report.materialization_required);
        assert!(!certificate.side_effects.fallback_attempted);
    }

    #[test]
    fn sample_rows_materialize_seeded_vortex_rows_without_fallback() {
        let path = unique_vortex_path("sample-rows");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::sample_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
            Some(PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(2),
            }),
            2,
            7,
        );

        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(2).expect("policy"),
        )
        .expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.mode,
            VortexLocalPrimitiveExecutionMode::VortexScanPushdown
        );
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_selected, Some(2));
        assert_eq!(report.rows_projected, Some(2));
        assert_eq!(report.projected_columns, vec!["metric".to_string()]);
        assert_eq!(
            report.result_summary.as_deref(),
            Some("sample_rows=2 projected_columns=metric sample_seed=7 sample_size=2")
        );
        assert_eq!(report.max_parallelism_requested, 2);
        assert_eq!(report.scan_concurrency_per_worker, 2);
        assert!(report.filter_pushdown_applied);
        assert!(report.projection_pushdown_applied);
        assert!(report.upstream_filter_expression_used);
        assert!(report.upstream_projection_expression_used);
        assert_eq!(report.source_order_limit_requested, Some(2));
        assert!(report.source_order_limit_applied);
        assert_eq!(report.source_order_limit_input_rows, Some(4));
        assert_eq!(report.source_order_limit_rows_output, Some(2));
        assert!(report.data_read);
        assert!(report.streaming_scan_used);
        assert!(report.full_stream_collected);
        assert!(report.data_decoded);
        assert!(report.data_materialized);
        assert!(report.row_read);
        assert!(!report.arrow_converted);
        assert!(!report.external_effects_executed);
        assert!(!report.fallback_execution_allowed);
        assert!(report.materialization_boundary_reported);
    }

    #[test]
    fn sample_rows_materialize_fractional_vortex_rows_without_fallback() {
        let path = unique_vortex_path("sample-fraction-rows");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::sample_fraction_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
            Some(PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(2),
            }),
            0.5,
            7,
        );

        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(1).expect("policy"),
        )
        .expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_selected, Some(2));
        assert_eq!(report.rows_projected, Some(2));
        assert_eq!(report.projected_columns, vec!["metric".to_string()]);
        assert_eq!(
            report.result_summary.as_deref(),
            Some("sample_rows=2 projected_columns=metric sample_seed=7 sample_fraction=0.5")
        );
        assert_eq!(report.source_order_limit_requested, None);
        assert!(!report.source_order_limit_applied);
        assert_eq!(report.source_order_limit_input_rows, Some(4));
        assert_eq!(report.source_order_limit_rows_output, Some(2));
        assert!(report.filter_pushdown_applied);
        assert!(report.projection_pushdown_applied);
        assert!(report.data_read);
        assert!(report.data_decoded);
        assert!(report.data_materialized);
        assert!(report.row_read);
        assert!(!report.fallback_execution_allowed);
        assert!(report.materialization_boundary_reported);
    }

    #[test]
    fn distinct_row_export_writes_unique_vortex_rows_with_materialization_evidence() {
        let path = unique_vortex_path("distinct-row-export");
        let output_path = unique_vortex_path("distinct-row-export-output").with_extension("jsonl");
        write_duplicate_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::distinct_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
            None,
        )
        .with_source_order_limit(2);

        let report = execute_vortex_local_primitive_row_export_with_policy(
            &request,
            &output_path,
            VortexLocalPrimitiveRowExportFormat::Jsonl,
            false,
            VortexLocalPrimitiveExecutionPolicy::new(2).expect("policy"),
        )
        .expect("report");
        let rows = std::fs::read_to_string(&output_path).expect("output");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&output_path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::DistinctRows
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 2);
        assert_eq!(report.pre_limit_result_row_count, 2);
        assert_eq!(report.projected_columns, vec!["value".to_string()]);
        assert_eq!(report.max_parallelism_requested, 2);
        assert_eq!(report.scan_concurrency_per_worker, 2);
        assert_eq!(report.source_order_limit_requested, Some(2));
        assert_eq!(rows, "{\"value\":1}\n{\"value\":2}\n");
        assert!(!report.evidence.pushdown.filter_pushdown_applied);
        assert!(report.evidence.pushdown.projection_pushdown_applied);
        assert!(report.evidence.pushdown.source_order_limit_applied);
        assert!(report.evidence.upstream_scan_called);
        assert!(report.evidence.materialization_boundary_reported);
        assert!(report.evidence.side_effects.data_read);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(report.evidence.side_effects.data_materialized);
        assert!(report.evidence.side_effects.row_read);
        assert!(report.evidence.side_effects.write_io);
        assert!(!report.evidence.side_effects.external_effects_executed);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(!report.evidence.side_effects.fallback_execution_allowed);
    }

    #[test]
    fn tail_row_export_writes_source_order_tail_rows_without_fallback() {
        let path = unique_vortex_path("tail-row-export");
        let output_path = unique_vortex_path("tail-row-export-output").with_extension("jsonl");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::tail_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
            2,
        );

        let report = execute_vortex_local_primitive_row_export_with_policy(
            &request,
            &output_path,
            VortexLocalPrimitiveRowExportFormat::Jsonl,
            false,
            VortexLocalPrimitiveExecutionPolicy::new(1).expect("policy"),
        )
        .expect("report");
        let rows = std::fs::read_to_string(&output_path).expect("output");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&output_path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::TailRows);
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 2);
        assert_eq!(report.pre_limit_result_row_count, 5);
        assert_eq!(report.projected_columns, vec!["metric".to_string()]);
        assert_eq!(report.source_order_limit_requested, Some(2));
        assert_eq!(rows, "{\"metric\":40}\n{\"metric\":50}\n");
        assert!(report.evidence.pushdown.projection_pushdown_applied);
        assert!(report.evidence.pushdown.source_order_limit_applied);
        assert!(report.evidence.side_effects.data_read);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(report.evidence.side_effects.data_materialized);
        assert!(report.evidence.side_effects.row_read);
        assert!(report.evidence.side_effects.write_io);
        assert!(!report.evidence.side_effects.external_effects_executed);
        assert!(!report.evidence.side_effects.fallback_attempted);
    }

    #[test]
    fn sample_row_export_writes_seeded_rows_without_fallback() {
        let path = unique_vortex_path("sample-row-export");
        let output_path = unique_vortex_path("sample-row-export-output").with_extension("jsonl");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::sample_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
            None,
            2,
            7,
        );

        let report = execute_vortex_local_primitive_row_export_with_policy(
            &request,
            &output_path,
            VortexLocalPrimitiveRowExportFormat::Jsonl,
            false,
            VortexLocalPrimitiveExecutionPolicy::new(1).expect("policy"),
        )
        .expect("report");
        let rows = std::fs::read_to_string(&output_path).expect("output");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&output_path);

        let metrics = [10_i64, 20, 30, 40, 50];
        let mut expected_indices = metrics
            .iter()
            .enumerate()
            .map(|(index, _metric)| (deterministic_sample_score(7, index), index))
            .collect::<Vec<_>>();
        expected_indices.sort_by_key(|(score, _index)| std::cmp::Reverse(*score));
        expected_indices.truncate(2);
        expected_indices.sort_by_key(|(_score, index)| *index);
        let expected = expected_indices
            .iter()
            .map(|(_score, index)| format!("{{\"metric\":{}}}", metrics[*index]))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 2);
        assert_eq!(report.pre_limit_result_row_count, 5);
        assert_eq!(report.projected_columns, vec!["metric".to_string()]);
        assert_eq!(report.source_order_limit_requested, Some(2));
        assert_eq!(rows, expected);
        assert!(report.evidence.pushdown.projection_pushdown_applied);
        assert!(report.evidence.pushdown.source_order_limit_applied);
        assert!(report.evidence.side_effects.data_read);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(report.evidence.side_effects.data_materialized);
        assert!(report.evidence.side_effects.row_read);
        assert!(report.evidence.side_effects.write_io);
        assert!(!report.evidence.side_effects.external_effects_executed);
        assert!(!report.evidence.side_effects.fallback_attempted);
    }

    #[test]
    fn sample_row_export_writes_fractional_seeded_rows_without_fallback() {
        let path = unique_vortex_path("sample-row-export-fraction");
        let output_path =
            unique_vortex_path("sample-row-export-fraction-output").with_extension("jsonl");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::sample_fraction_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
            None,
            0.4,
            7,
        );

        let report = execute_vortex_local_primitive_row_export_with_policy(
            &request,
            &output_path,
            VortexLocalPrimitiveRowExportFormat::Jsonl,
            false,
            VortexLocalPrimitiveExecutionPolicy::new(1).expect("policy"),
        )
        .expect("report");
        let rows = std::fs::read_to_string(&output_path).expect("output");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&output_path);

        let metrics = [10_i64, 20, 30, 40, 50];
        let mut expected_indices = metrics
            .iter()
            .enumerate()
            .map(|(index, _metric)| (deterministic_sample_score(7, index), index))
            .collect::<Vec<_>>();
        expected_indices.sort_by_key(|(score, _index)| std::cmp::Reverse(*score));
        expected_indices.truncate(2);
        expected_indices.sort_by_key(|(_score, index)| *index);
        let expected = expected_indices
            .iter()
            .map(|(_score, index)| format!("{{\"metric\":{}}}", metrics[*index]))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 2);
        assert_eq!(report.pre_limit_result_row_count, 5);
        assert_eq!(report.projected_columns, vec!["metric".to_string()]);
        assert_eq!(report.source_order_limit_requested, None);
        assert_eq!(rows, expected);
        assert!(report.evidence.pushdown.projection_pushdown_applied);
        assert!(!report.evidence.pushdown.source_order_limit_applied);
        assert!(report.evidence.side_effects.data_read);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(report.evidence.side_effects.data_materialized);
        assert!(report.evidence.side_effects.row_read);
        assert!(report.evidence.side_effects.write_io);
        assert!(!report.evidence.side_effects.fallback_attempted);
    }

    #[test]
    fn local_primitive_native_io_certificate_covers_filter_project_path() {
        let path = unique_vortex_path("native-io-filter-project");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::filter_and_project(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        );

        let report = execute_vortex_local_primitive(&request).expect("report");
        let certificate =
            local_primitive_native_io_certificate(&request, &report).expect("certificate");
        let _ = std::fs::remove_file(&path);

        assert!(certificate.is_certified());
        assert_eq!(certificate.status(), "certified");
        assert_eq!(
            certificate.certificate_id,
            "cg19.local_primitive.filter_and_project.native_io"
        );
        assert_eq!(
            certificate.path_id,
            "native_vortex_source_to_filtered_projected_result"
        );
        assert_eq!(
            certificate
                .source_pushdown_report
                .accepted_operation_order(),
            "filter,project"
        );
        assert_eq!(
            certificate.representation_transition_order(),
            "vortex_encoded->selection_vector_encoded"
        );
        assert_eq!(
            certificate.sink_requirement_report.target_format,
            "local_filtered_projected_stream_summary"
        );
        assert!(certificate.source_capability_report.streaming_capability);
        assert!(
            certificate
                .source_capability_report
                .encoded_representation_preserved
        );
        assert!(
            certificate
                .adapter_fidelity_report
                .encoded_representation_preserved
        );
        assert!(certificate.materializing_transitions_have_boundaries());
        assert_eq!(certificate.materialization_boundary_order(), "");
        assert!(certificate.side_effects.data_read);
        assert!(!certificate.side_effects.data_decoded);
        assert!(!certificate.side_effects.data_materialized);
        assert!(!certificate.side_effects.row_read);
        assert!(!certificate.side_effects.arrow_converted);
        assert!(!certificate.side_effects.object_store_io);
        assert!(!certificate.side_effects.write_io);
        assert!(!certificate.side_effects.spill_io_performed);
        assert!(!certificate.side_effects.fallback_attempted);
        assert!(!certificate.side_effects.fallback_execution_allowed);
    }

    #[test]
    fn local_primitive_native_io_certificate_covers_filter_path() {
        let path = unique_vortex_path("native-io-filter");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::filter(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
        );

        let report = execute_vortex_local_primitive(&request).expect("report");
        let certificate =
            local_primitive_native_io_certificate(&request, &report).expect("certificate");
        let _ = std::fs::remove_file(&path);

        assert!(certificate.is_certified());
        assert_eq!(
            certificate
                .source_pushdown_report
                .accepted_operation_order(),
            "filter"
        );
        assert_eq!(
            certificate.representation_transition_order(),
            "vortex_encoded->selection_vector_encoded"
        );
        assert_eq!(
            certificate.sink_requirement_report.target_format,
            "local_filtered_stream_summary"
        );
        assert!(certificate.sink_requirement_report.accepts_encoded);
        assert!(!certificate.sink_requirement_report.requires_rows);
        assert!(certificate.side_effects.data_read);
        assert!(!certificate.side_effects.data_decoded);
        assert!(!certificate.side_effects.data_materialized);
        assert!(!certificate.side_effects.row_read);
        assert!(!certificate.side_effects.arrow_converted);
        assert!(!certificate.side_effects.fallback_attempted);
    }

    #[test]
    fn local_primitive_native_io_certificate_covers_project_path() {
        let path = unique_vortex_path("native-io-project");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::project(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        );

        let report = execute_vortex_local_primitive(&request).expect("report");
        let certificate =
            local_primitive_native_io_certificate(&request, &report).expect("certificate");
        let _ = std::fs::remove_file(&path);

        assert!(certificate.is_certified());
        assert_eq!(
            certificate
                .source_pushdown_report
                .accepted_operation_order(),
            "project"
        );
        assert_eq!(
            certificate.representation_transition_order(),
            "vortex_encoded->vortex_encoded"
        );
        assert_eq!(
            certificate.sink_requirement_report.target_format,
            "local_projected_stream_summary"
        );
        assert!(certificate.sink_requirement_report.accepts_encoded);
        assert!(!certificate.sink_requirement_report.requires_rows);
    }

    #[test]
    fn local_primitive_native_io_certificate_allows_primitive_projection_passthrough() {
        let path = unique_vortex_path("native-io-primitive-project");
        write_primitive_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::project(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
        );

        let report = execute_vortex_local_primitive(&request).expect("report");
        let certificate =
            local_primitive_native_io_certificate(&request, &report).expect("certificate");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.rows_projected, Some(3));
        assert_eq!(report.projected_columns, vec!["value".to_string()]);
        assert!(!report.projection_pushdown_applied);
        assert!(!report.upstream_projection_expression_used);
        assert!(certificate.is_certified());
        assert_eq!(
            certificate
                .source_pushdown_report
                .accepted_operation_order(),
            "project"
        );
        assert_eq!(
            certificate.representation_transition_order(),
            "vortex_encoded->vortex_encoded"
        );
    }

    #[test]
    fn local_primitive_native_io_certificate_allows_primitive_filter_project_passthrough() {
        let path = unique_vortex_path("native-io-primitive-filter-project");
        write_primitive_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::filter_and_project(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(8),
            },
            ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
        );

        let report = execute_vortex_local_primitive(&request).expect("report");
        let certificate =
            local_primitive_native_io_certificate(&request, &report).expect("certificate");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.rows_selected, Some(2));
        assert_eq!(report.rows_projected, Some(2));
        assert!(report.filter_pushdown_applied);
        assert!(report.upstream_filter_expression_used);
        assert!(!report.projection_pushdown_applied);
        assert!(!report.upstream_projection_expression_used);
        assert!(certificate.is_certified());
        assert_eq!(
            certificate
                .source_pushdown_report
                .accepted_operation_order(),
            "filter,project"
        );
        assert_eq!(
            certificate.representation_transition_order(),
            "vortex_encoded->selection_vector_encoded"
        );
    }

    #[test]
    fn local_primitive_native_io_certificate_blocks_unsafe_effects() {
        let path = unique_vortex_path("native-io-blocked");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::filter_and_project(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        );

        let mut report = execute_vortex_local_primitive(&request).expect("report");
        let _ = std::fs::remove_file(&path);
        report.data_materialized = true;

        let certificate =
            local_primitive_native_io_certificate(&request, &report).expect("certificate");

        assert_eq!(certificate.status(), "blocked");
        assert!(!certificate.is_certified());
        assert_eq!(
            certificate.representation_transition_order(),
            "vortex_encoded->unsupported"
        );
        assert_eq!(
            certificate
                .source_pushdown_report
                .rejected_operation_order(),
            "filter_and_project"
        );
        assert!(certificate.side_effects.data_materialized);
        assert!(certificate.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == DiagnosticCode::NoFallbackExecution
                && diagnostic.severity == DiagnosticSeverity::Error
        }));
    }

    #[test]
    fn local_primitive_certificates_cover_broader_runtime_fixtures() {
        let cases = [
            (
                "vortex-local-count-all-struct-five",
                VortexQueryPrimitiveRequest::count_all(
                    DatasetUri::new("placeholder.vortex").expect("uri"),
                ),
            ),
            (
                "vortex-local-count-where-struct-five",
                VortexQueryPrimitiveRequest::count_where(
                    DatasetUri::new("placeholder.vortex").expect("uri"),
                    PredicateExpr::Compare {
                        column: ColumnRef::new("value").expect("column"),
                        op: ComparisonOp::GtEq,
                        value: StatValue::Int64(3),
                    },
                ),
            ),
            (
                "vortex-local-project-struct-five",
                VortexQueryPrimitiveRequest::project(
                    DatasetUri::new("placeholder.vortex").expect("uri"),
                    ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
                ),
            ),
            (
                "vortex-local-filter-struct-five",
                VortexQueryPrimitiveRequest::filter(
                    DatasetUri::new("placeholder.vortex").expect("uri"),
                    PredicateExpr::Compare {
                        column: ColumnRef::new("value").expect("column"),
                        op: ComparisonOp::GtEq,
                        value: StatValue::Int64(3),
                    },
                ),
            ),
            (
                "vortex-local-filter-project-struct-five",
                VortexQueryPrimitiveRequest::filter_and_project(
                    DatasetUri::new("placeholder.vortex").expect("uri"),
                    PredicateExpr::Compare {
                        column: ColumnRef::new("value").expect("column"),
                        op: ComparisonOp::GtEq,
                        value: StatValue::Int64(3),
                    },
                    ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
                ),
            ),
        ];

        for (fixture_id, mut request) in cases {
            request.source_uri = Some(checked_in_struct_fixture_uri());

            let report = execute_vortex_local_primitive(&request).expect("report");
            let certificate = local_primitive_execution_certificate(
                &correctness_fixture(fixture_id),
                &request,
                &report,
            )
            .expect("certificate");

            assert_eq!(
                certificate.status,
                ExecutionCertificateStatus::Certified,
                "{fixture_id}"
            );
            assert!(certificate.is_certified(), "{fixture_id}");
            assert_eq!(
                certificate.correctness_fixture_id.as_deref(),
                Some(fixture_id)
            );
            assert_eq!(certificate.expected_outcome, certificate.actual_outcome);
            assert!(certificate.data_read);
            assert!(!certificate.data_decoded);
            assert!(!certificate.data_materialized);
            assert!(!certificate.row_read);
            assert!(!certificate.arrow_converted);
            assert!(!certificate.object_store_io);
            assert!(!certificate.write_io);
            assert!(!certificate.spill_io_performed);
            assert!(!certificate.external_effects_executed);
            assert!(certificate.fallback_free());
        }
    }

    #[test]
    fn local_primitive_certificate_blocks_fixture_identity_mismatch() {
        let request = VortexQueryPrimitiveRequest::filter(
            checked_in_struct_fixture_uri(),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
        );

        let report = execute_vortex_local_primitive(&request).expect("report");
        let certificate = local_primitive_execution_certificate(
            &correctness_fixture("vortex-local-filter-project-struct-five"),
            &request,
            &report,
        )
        .expect("certificate");

        assert_eq!(
            certificate.status,
            ExecutionCertificateStatus::EvidenceIncomplete
        );
        assert!(!certificate.is_certified());
        assert_eq!(
            certificate.correctness_fixture_id.as_deref(),
            Some("vortex-local-filter-project-struct-five")
        );
        assert_eq!(certificate.expected_outcome, certificate.actual_outcome);
        assert!(certificate.fallback_free());
    }

    #[test]
    fn local_vortex_path_resolves_relative_targets_from_workspace_root() {
        let uri =
            DatasetUri::new("shardloom-vortex/tests/fixtures/local_primitive_struct_five.vortex")
                .expect("uri");

        let path = local_vortex_path(&uri, VortexQueryPrimitiveKind::CountAll)
            .expect("path")
            .expect("existing path");

        assert_eq!(
            path.canonicalize().expect("canonical"),
            checked_in_struct_fixture_path()
                .canonicalize()
                .expect("fixture canonical")
        );
    }

    #[test]
    fn local_primitive_certificate_blocks_unsafe_effects() {
        let path = unique_vortex_path("certificate-blocked");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::filter_and_project(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(3),
            },
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
        );

        let mut report = execute_vortex_local_primitive(&request).expect("report");
        let _ = std::fs::remove_file(&path);
        report.data_materialized = true;

        let certificate = local_primitive_execution_certificate(
            &correctness_fixture("vortex-local-filter-project-struct-five"),
            &request,
            &report,
        )
        .expect("certificate");

        assert_eq!(certificate.status, ExecutionCertificateStatus::Blocked);
        assert!(!certificate.is_certified());
        assert!(certificate.data_materialized);
    }
}
