#![allow(clippy::must_use_candidate)]

use std::fmt::Write as _;

#[cfg(feature = "vortex-local-primitives")]
use regex::Regex;
#[cfg(feature = "vortex-local-primitives")]
use shardloom_core::ScalarValue;
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
    VortexAggregateExpression, VortexAggregateHavingExpr, VortexDuplicateKeepPolicy,
    VortexEncodedValuePredicateBatch, VortexExplodeProjectionRequest,
    VortexExpressionProjectionRequest, VortexExpressionRewrite, VortexMeltProjectionRequest,
    VortexPivotProjectionRequest, VortexReaderGeneratedEncodedKernelInput,
    VortexRollingWindowRequest, VortexSimpleAggregateRequest, VortexSortRowsRequest,
    VortexSortTiePolicy, plan_vortex_reader_generated_prepared_batch_envelopes,
    plan_vortex_reader_generated_prepared_batch_kernel_inputs,
};
use crate::{
    VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest, VortexReaderBackedSplitEvidence,
    VortexReaderGeneratedPreparedBatchReport,
};
#[cfg(all(feature = "vortex-local-primitives", feature = "universal-format-io"))]
use crate::{VortexStructuredProjectionExpr, VortexStructuredProjectionRequest};

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

/// State-budget evidence for local Vortex primitive work that can grow with input scale.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct VortexLocalPrimitiveStateBudgetReport {
    pub schema_version: String,
    pub state_budget_required: bool,
    pub state_budget_status: String,
    pub state_family: String,
    pub capillary_work_units: Vec<String>,
    pub pulseweave_pressure_signals: Vec<String>,
    pub observed_state_items: u64,
    pub estimated_state_items: Option<u64>,
    pub budget_scope: String,
    pub spill_policy: String,
    pub spill_required: bool,
    pub spill_supported: bool,
    pub spill_io_performed: bool,
    pub fail_closed_if_spill_required: bool,
    pub diagnostic_code: String,
    pub next_action: String,
}
impl VortexLocalPrimitiveStateBudgetReport {
    const SCHEMA_VERSION: &'static str = "shardloom.local_vortex_state_budget.v1";

    #[must_use]
    pub fn not_required() -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION.to_string(),
            state_budget_required: false,
            state_budget_status: "not_required".to_string(),
            state_family: "stateless_scan_or_metadata".to_string(),
            capillary_work_units: Vec::new(),
            pulseweave_pressure_signals: Vec::new(),
            observed_state_items: 0,
            estimated_state_items: None,
            budget_scope: "local_vortex_primitive".to_string(),
            spill_policy: "not_applicable".to_string(),
            spill_required: false,
            spill_supported: false,
            spill_io_performed: false,
            fail_closed_if_spill_required: false,
            diagnostic_code: "none".to_string(),
            next_action: "none".to_string(),
        }
    }

    #[must_use]
    pub fn bounded_in_memory(
        state_family: impl Into<String>,
        capillary_work_units: Vec<&str>,
        pulseweave_pressure_signals: Vec<&str>,
        observed_state_items: u64,
        estimated_state_items: Option<u64>,
        budget_scope: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION.to_string(),
            state_budget_required: true,
            state_budget_status: "bounded_in_memory_observed_spill_not_certified".to_string(),
            state_family: state_family.into(),
            capillary_work_units: capillary_work_units
                .into_iter()
                .map(str::to_string)
                .collect(),
            pulseweave_pressure_signals: pulseweave_pressure_signals
                .into_iter()
                .map(str::to_string)
                .collect(),
            observed_state_items,
            estimated_state_items,
            budget_scope: budget_scope.into(),
            spill_policy: "fail_closed_before_uncertified_spill".to_string(),
            spill_required: false,
            spill_supported: false,
            spill_io_performed: false,
            fail_closed_if_spill_required: true,
            diagnostic_code: "none".to_string(),
            next_action: "certify native spill before admitting spill-required scale shapes"
                .to_string(),
        }
    }

    #[must_use]
    pub fn compact_summary(&self) -> String {
        format!(
            "schema={};required={};status={};family={};capillary={};pulseweave={};observed_state_items={};estimated_state_items={};spill_policy={};spill_required={};spill_supported={};fail_closed_if_spill_required={};diagnostic_code={}",
            self.schema_version,
            self.state_budget_required,
            self.state_budget_status,
            self.state_family,
            self.capillary_work_units.join(","),
            self.pulseweave_pressure_signals.join(","),
            self.observed_state_items,
            self.estimated_state_items
                .map_or_else(|| "none".to_string(), |value| value.to_string()),
            self.spill_policy,
            self.spill_required,
            self.spill_supported,
            self.fail_closed_if_spill_required,
            self.diagnostic_code
        )
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
    pub state_budget: VortexLocalPrimitiveStateBudgetReport,
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
            state_budget: VortexLocalPrimitiveStateBudgetReport::not_required(),
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
        let _ = writeln!(out, "state budget: {}", self.state_budget.compact_summary());
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
    Vortex,
    Parquet,
    ArrowIpc,
    Avro,
}
impl VortexLocalPrimitiveRowExportFormat {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Jsonl => "jsonl",
            Self::Csv => "csv",
            Self::Vortex => "vortex",
            Self::Parquet => "parquet",
            Self::ArrowIpc => "arrow-ipc",
            Self::Avro => "avro",
        }
    }

    #[must_use]
    pub const fn is_structured_projection_export(self) -> bool {
        matches!(
            self,
            Self::Vortex | Self::Parquet | Self::ArrowIpc | Self::Avro
        )
    }

    #[must_use]
    pub const fn is_compatibility_binary(self) -> bool {
        matches!(self, Self::Parquet | Self::ArrowIpc | Self::Avro)
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
    pub state_budget: VortexLocalPrimitiveStateBudgetReport,
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
            state_budget: VortexLocalPrimitiveStateBudgetReport::not_required(),
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
        | VortexQueryPrimitiveKind::DropDuplicateRows
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
        VortexQueryPrimitiveKind::DropDuplicateRows => {
            "native_vortex_source_to_drop_duplicates_materialized_result"
        }
        VortexQueryPrimitiveKind::DuplicateMaskRows => {
            "native_vortex_source_to_duplicate_mask_materialized_result"
        }
        VortexQueryPrimitiveKind::TailRows => "native_vortex_source_to_tail_source_order_result",
        VortexQueryPrimitiveKind::SampleRows => "native_vortex_source_to_sample_seeded_result",
        VortexQueryPrimitiveKind::ExpressionProjectRows => {
            "native_vortex_source_to_expression_project_materialized_result"
        }
        VortexQueryPrimitiveKind::MeltRows => "native_vortex_source_to_melt_materialized_result",
        VortexQueryPrimitiveKind::ExplodeRows => {
            "native_vortex_source_to_explode_materialized_result"
        }
        VortexQueryPrimitiveKind::PivotRows => "native_vortex_source_to_pivot_materialized_result",
        VortexQueryPrimitiveKind::RollingWindowRows => {
            "native_vortex_source_to_rolling_window_materialized_result"
        }
        VortexQueryPrimitiveKind::SimpleAggregate => {
            "native_vortex_source_to_scalar_aggregate_result"
        }
        VortexQueryPrimitiveKind::SortRows => "native_vortex_source_to_sort_materialized_result",
        VortexQueryPrimitiveKind::Unsupported => "native_vortex_source_to_unsupported_result",
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
        | VortexQueryPrimitiveKind::DropDuplicateRows
        | VortexQueryPrimitiveKind::DuplicateMaskRows
        | VortexQueryPrimitiveKind::TailRows
        | VortexQueryPrimitiveKind::SampleRows
        | VortexQueryPrimitiveKind::ExpressionProjectRows
        | VortexQueryPrimitiveKind::MeltRows
        | VortexQueryPrimitiveKind::ExplodeRows
        | VortexQueryPrimitiveKind::PivotRows
        | VortexQueryPrimitiveKind::RollingWindowRows
        | VortexQueryPrimitiveKind::SimpleAggregate
        | VortexQueryPrimitiveKind::SortRows => {
            local_primitive_materialization_declared(report)
                && local_primitive_row_count(report).is_some()
        }
        VortexQueryPrimitiveKind::Unsupported => false,
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
            local_primitive_optional_filter_project_operation(report, "distinct")
        }
        VortexQueryPrimitiveKind::DropDuplicateRows => {
            local_primitive_optional_project_operation(report, "drop_duplicates")
        }
        VortexQueryPrimitiveKind::DuplicateMaskRows => {
            local_primitive_optional_project_operation(report, "duplicate_mask")
        }
        VortexQueryPrimitiveKind::TailRows => {
            local_primitive_optional_project_operation(report, "tail")
        }
        VortexQueryPrimitiveKind::SampleRows => {
            local_primitive_optional_filter_project_operation(report, "sample")
        }
        VortexQueryPrimitiveKind::ExpressionProjectRows => {
            local_primitive_optional_project_operation(report, "expression_project")
        }
        VortexQueryPrimitiveKind::MeltRows => {
            local_primitive_optional_project_operation(report, "melt")
        }
        VortexQueryPrimitiveKind::ExplodeRows => {
            local_primitive_optional_project_operation(report, "explode")
        }
        VortexQueryPrimitiveKind::PivotRows => {
            local_primitive_optional_project_operation(report, "pivot")
        }
        VortexQueryPrimitiveKind::RollingWindowRows => {
            local_primitive_optional_project_operation(report, "rolling_window")
        }
        VortexQueryPrimitiveKind::SimpleAggregate => {
            local_primitive_optional_project_operation(report, "aggregate")
        }
        VortexQueryPrimitiveKind::SortRows => {
            local_primitive_optional_filter_project_operation(report, "sort")
        }
        VortexQueryPrimitiveKind::Unsupported => Vec::new(),
    }
}

fn local_primitive_optional_filter_project_operation(
    report: &VortexLocalPrimitiveExecutionReport,
    operation: &str,
) -> Vec<String> {
    let mut out = Vec::new();
    if report.filter_pushdown_applied {
        out.push("filter".to_string());
    }
    if !report.projected_columns.is_empty() {
        out.push("project".to_string());
    }
    out.push(operation.to_string());
    out
}

fn local_primitive_optional_project_operation(
    report: &VortexLocalPrimitiveExecutionReport,
    operation: &str,
) -> Vec<String> {
    let mut out = Vec::new();
    if !report.projected_columns.is_empty() {
        out.push("project".to_string());
    }
    out.push(operation.to_string());
    out
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
        VortexQueryPrimitiveKind::DropDuplicateRows => {
            "exact_drop_duplicates_from_vortex_scan_with_explicit_shardloom_row_key_retention_state"
        }
        VortexQueryPrimitiveKind::DuplicateMaskRows => {
            "exact_duplicate_mask_from_vortex_scan_with_explicit_shardloom_row_key_state"
        }
        VortexQueryPrimitiveKind::TailRows => {
            "exact_source_order_tail_from_full_vortex_scan_with_explicit_shardloom_tail_boundary"
        }
        VortexQueryPrimitiveKind::SampleRows => {
            "exact_deterministic_sample_from_vortex_scan_with_explicit_shardloom_seeded_selection"
        }
        VortexQueryPrimitiveKind::ExpressionProjectRows => {
            "exact_expression_project_from_vortex_scan_with_explicit_shardloom_rewrite_boundary"
        }
        VortexQueryPrimitiveKind::MeltRows => {
            "exact_melt_from_vortex_scan_with_explicit_shardloom_row_expansion_boundary"
        }
        VortexQueryPrimitiveKind::ExplodeRows => {
            "exact_explode_from_vortex_list_scan_with_explicit_shardloom_row_expansion_boundary"
        }
        VortexQueryPrimitiveKind::PivotRows => {
            "exact_pivot_from_vortex_scan_with_explicit_shardloom_wide_reshape_boundary"
        }
        VortexQueryPrimitiveKind::RollingWindowRows => {
            "exact_source_order_rolling_window_from_vortex_scan_with_explicit_shardloom_window_state"
        }
        VortexQueryPrimitiveKind::SimpleAggregate => {
            "exact_scalar_aggregate_from_vortex_scan_with_explicit_shardloom_aggregate_state"
        }
        VortexQueryPrimitiveKind::SortRows => {
            "exact_bounded_sort_from_vortex_scan_with_explicit_shardloom_order_state"
        }
        VortexQueryPrimitiveKind::Unsupported => "unsupported",
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
        VortexQueryPrimitiveKind::DropDuplicateRows => {
            "local Vortex scan applied projection pushdown, then ShardLoom retained full output rows by scoped row-key policy without invoking an external engine"
        }
        VortexQueryPrimitiveKind::DuplicateMaskRows => {
            "local Vortex scan applied projection pushdown, then ShardLoom produced a deterministic duplicate mask from scoped primitive row keys without invoking an external engine"
        }
        VortexQueryPrimitiveKind::TailRows => {
            "local Vortex scan applied projection pushdown, then ShardLoom retained the final source-order row window without invoking an external engine"
        }
        VortexQueryPrimitiveKind::SampleRows => {
            "local Vortex scan applied optional filter/projection pushdown, then ShardLoom applied deterministic seeded row selection without invoking an external engine"
        }
        VortexQueryPrimitiveKind::ExpressionProjectRows => {
            "local Vortex scan applied projection pushdown, then ShardLoom applied a typed expression rewrite at the explicit materialization boundary without invoking an external engine"
        }
        VortexQueryPrimitiveKind::MeltRows => {
            "local Vortex scan applied projection pushdown, then ShardLoom expanded scoped heterogeneous scalar value columns into rows at the explicit materialization boundary without invoking an external engine"
        }
        VortexQueryPrimitiveKind::ExplodeRows => {
            "local Vortex scan applied projection pushdown, then ShardLoom expanded a scoped Vortex list/fixed-size-list column into scalar rows at the explicit materialization boundary without invoking an external engine"
        }
        VortexQueryPrimitiveKind::PivotRows => {
            "local Vortex scan applied projection pushdown, then ShardLoom built scoped wide pivot state at the explicit materialization boundary without invoking an external engine"
        }
        VortexQueryPrimitiveKind::RollingWindowRows => {
            "local Vortex scan applied projection pushdown, then ShardLoom maintained bounded source-order rolling window state without invoking an external engine"
        }
        VortexQueryPrimitiveKind::SimpleAggregate => {
            "local Vortex scan applied projection pushdown, then ShardLoom accumulated scalar aggregate state without invoking an external engine"
        }
        VortexQueryPrimitiveKind::SortRows => {
            "local Vortex scan applied optional filter/projection pushdown, then ShardLoom retained bounded ordered rows without invoking an external engine"
        }
        VortexQueryPrimitiveKind::Unsupported => "unsupported local primitive",
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
        | VortexQueryPrimitiveKind::DropDuplicateRows
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
        | VortexQueryPrimitiveKind::Unsupported => RepresentationState::Unsupported,
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
        VortexQueryPrimitiveKind::DropDuplicateRows => "local_drop_duplicates_row_summary",
        VortexQueryPrimitiveKind::DuplicateMaskRows => "local_duplicate_mask_row_summary",
        VortexQueryPrimitiveKind::TailRows => "local_tail_row_summary",
        VortexQueryPrimitiveKind::SampleRows => "local_sample_row_summary",
        VortexQueryPrimitiveKind::ExpressionProjectRows => "local_expression_project_row_summary",
        VortexQueryPrimitiveKind::MeltRows => "local_melt_row_summary",
        VortexQueryPrimitiveKind::ExplodeRows => "local_explode_row_summary",
        VortexQueryPrimitiveKind::PivotRows => "local_pivot_row_summary",
        VortexQueryPrimitiveKind::RollingWindowRows => "local_rolling_window_row_summary",
        VortexQueryPrimitiveKind::SimpleAggregate => "scalar_aggregate_result",
        VortexQueryPrimitiveKind::SortRows => "local_sort_row_summary",
        VortexQueryPrimitiveKind::Unsupported => "unsupported_result",
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

#[allow(clippy::too_many_lines)]
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
        VortexQueryPrimitiveKind::DropDuplicateRows => {
            local_primitive_row_count(report).map(|rows| {
                format!(
                    "drop_duplicate_rows={rows};projected_columns={};source_order_limit={}",
                    report.projected_columns.join(","),
                    report
                        .source_order_limit_requested
                        .map_or_else(|| "none".to_string(), |limit| limit.to_string())
                )
            })
        }
        VortexQueryPrimitiveKind::DuplicateMaskRows => {
            local_primitive_row_count(report).map(|rows| {
                format!(
                    "duplicate_mask_rows={rows};projected_columns={};source_order_limit={}",
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
        VortexQueryPrimitiveKind::ExpressionProjectRows => {
            local_primitive_row_count(report).map(|rows| {
                format!(
                    "expression_project_rows={rows};projected_columns={}",
                    report.projected_columns.join(",")
                )
            })
        }
        VortexQueryPrimitiveKind::MeltRows => {
            local_primitive_row_count(report).map(|rows| {
                format!(
                    "melt_rows={rows};output_columns={};source_order_limit={}",
                    report.projected_columns.join(","),
                    report
                        .source_order_limit_requested
                        .map_or_else(|| "none".to_string(), |limit| limit.to_string())
                )
            })
        }
        VortexQueryPrimitiveKind::ExplodeRows => {
            local_primitive_row_count(report).map(|rows| {
                format!(
                    "explode_rows={rows};output_columns={};source_order_limit={}",
                    report.projected_columns.join(","),
                    report
                        .source_order_limit_requested
                        .map_or_else(|| "none".to_string(), |limit| limit.to_string())
                )
            })
        }
        VortexQueryPrimitiveKind::PivotRows => local_primitive_row_count(report).map(|rows| {
            format!(
                "pivot_rows={rows};output_columns={};source_order_limit={}",
                report.projected_columns.join(","),
                report
                    .source_order_limit_requested
                    .map_or_else(|| "none".to_string(), |limit| limit.to_string())
            )
        }),
        VortexQueryPrimitiveKind::RollingWindowRows => {
            local_primitive_row_count(report).map(|rows| {
                format!(
                    "rolling_window_rows={rows};output_columns={}",
                    report.projected_columns.join(",")
                )
            })
        }
        VortexQueryPrimitiveKind::SimpleAggregate => report
            .result_summary
            .as_ref()
            .map(|summary| format!("scalar_aggregate_result={summary}")),
        VortexQueryPrimitiveKind::SortRows => local_primitive_row_count(report).map(|rows| {
            format!(
                "sort_rows={rows};projected_columns={};source_order_limit={}",
                report.projected_columns.join(","),
                report
                    .source_order_limit_requested
                    .map_or_else(|| "none".to_string(), |limit| limit.to_string())
            )
        }),
        VortexQueryPrimitiveKind::Unsupported => None,
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
        | VortexQueryPrimitiveKind::DropDuplicateRows
        | VortexQueryPrimitiveKind::DuplicateMaskRows
        | VortexQueryPrimitiveKind::TailRows
        | VortexQueryPrimitiveKind::SampleRows
        | VortexQueryPrimitiveKind::ExpressionProjectRows
        | VortexQueryPrimitiveKind::MeltRows
        | VortexQueryPrimitiveKind::ExplodeRows
        | VortexQueryPrimitiveKind::PivotRows
        | VortexQueryPrimitiveKind::RollingWindowRows
        | VortexQueryPrimitiveKind::SimpleAggregate
        | VortexQueryPrimitiveKind::SortRows => {
            match (report.rows_selected, report.rows_projected) {
                (Some(selected), Some(projected)) if selected == projected => Some(selected),
                (Some(selected), Some(1))
                    if report.primitive_kind == VortexQueryPrimitiveKind::SimpleAggregate =>
                {
                    Some(selected)
                }
                _ => None,
            }
        }
        VortexQueryPrimitiveKind::Unsupported => None,
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
    if report.primitive_kind == VortexQueryPrimitiveKind::DropDuplicateRows {
        effects.push("shardloom_drop_duplicates_row_key_retention_state".to_string());
    }
    if report.primitive_kind == VortexQueryPrimitiveKind::DuplicateMaskRows {
        effects.push("shardloom_duplicate_mask_row_key_state".to_string());
    }
    if report.primitive_kind == VortexQueryPrimitiveKind::TailRows {
        effects.push("shardloom_source_order_tail_window".to_string());
    }
    if report.primitive_kind == VortexQueryPrimitiveKind::SampleRows {
        effects.push("shardloom_deterministic_sample_selection".to_string());
    }
    if report.primitive_kind == VortexQueryPrimitiveKind::ExpressionProjectRows {
        effects.push("shardloom_expression_projection_rewrite".to_string());
    }
    if report.primitive_kind == VortexQueryPrimitiveKind::MeltRows {
        effects.push("shardloom_melt_row_expansion".to_string());
    }
    if report.primitive_kind == VortexQueryPrimitiveKind::ExplodeRows {
        effects.push("shardloom_explode_list_row_expansion".to_string());
    }
    if report.primitive_kind == VortexQueryPrimitiveKind::PivotRows {
        effects.push("shardloom_pivot_wide_reshape".to_string());
    }
    if report.primitive_kind == VortexQueryPrimitiveKind::RollingWindowRows {
        effects.push("shardloom_rolling_window_state".to_string());
    }
    if report.primitive_kind == VortexQueryPrimitiveKind::SimpleAggregate {
        effects.push("shardloom_scalar_aggregate_state".to_string());
    }
    if report.primitive_kind == VortexQueryPrimitiveKind::SortRows {
        effects.push("shardloom_bounded_order_state".to_string());
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

    if request.kind == VortexQueryPrimitiveKind::PivotRows {
        return execute_vortex_local_pivot_row_export_enabled(
            request,
            output_path,
            output_format,
            allow_overwrite,
            policy,
        );
    }
    if request.kind == VortexQueryPrimitiveKind::SimpleAggregate {
        return execute_vortex_local_simple_aggregate_row_export_enabled(
            request,
            output_path,
            output_format,
            allow_overwrite,
            policy,
        );
    }
    if request.kind == VortexQueryPrimitiveKind::SortRows {
        return execute_vortex_local_sort_rows_row_export_enabled(
            request,
            output_path,
            output_format,
            allow_overwrite,
            policy,
        );
    }
    if !matches!(
        request.kind,
        VortexQueryPrimitiveKind::FilterPredicate
            | VortexQueryPrimitiveKind::ProjectColumns
            | VortexQueryPrimitiveKind::FilterAndProject
            | VortexQueryPrimitiveKind::DistinctRows
            | VortexQueryPrimitiveKind::DropDuplicateRows
            | VortexQueryPrimitiveKind::DuplicateMaskRows
            | VortexQueryPrimitiveKind::TailRows
            | VortexQueryPrimitiveKind::SampleRows
            | VortexQueryPrimitiveKind::ExpressionProjectRows
            | VortexQueryPrimitiveKind::MeltRows
            | VortexQueryPrimitiveKind::ExplodeRows
            | VortexQueryPrimitiveKind::RollingWindowRows
            | VortexQueryPrimitiveKind::SortRows
    ) {
        return Ok(VortexLocalPrimitiveRowExportReport::blocked(
            request.kind,
            output_path,
            output_format,
            Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "vortex_local_primitive_row_export",
                "local Vortex primitive row export supports filter, project, filter-project, distinct, drop-duplicates, duplicate-mask, tail, sample, expression-project, melt, explode, pivot, and rolling-window row streams only",
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
    if output_format.is_structured_projection_export() {
        return execute_vortex_local_structured_binary_row_export_enabled(
            request,
            &path,
            output_path,
            output_format,
            allow_overwrite,
            policy,
        );
    }

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
    let duplicate_mask_requested = request.kind == VortexQueryPrimitiveKind::DuplicateMaskRows;
    let drop_duplicate_requested = request.kind == VortexQueryPrimitiveKind::DropDuplicateRows;
    let melt_requested = request.kind == VortexQueryPrimitiveKind::MeltRows;
    let explode_requested = request.kind == VortexQueryPrimitiveKind::ExplodeRows;
    let rolling_requested = request.kind == VortexQueryPrimitiveKind::RollingWindowRows;
    let sample_requested = request.kind == VortexQueryPrimitiveKind::SampleRows;
    let declared_columns = if plan.projected_columns.is_empty() {
        local_field_names(file.dtype(), request.kind)?
    } else {
        plan.projected_columns.clone()
    };
    let melt_projection = if melt_requested {
        Some(required_melt_projection(request)?)
    } else {
        None
    };
    let explode_projection = if explode_requested {
        Some(required_explode_projection(request)?)
    } else {
        None
    };
    let rolling_window = if rolling_requested {
        Some(required_rolling_window(request)?)
    } else {
        None
    };
    let expression_projection = if request.kind == VortexQueryPrimitiveKind::ExpressionProjectRows {
        Some(request.expression_projection.as_ref().ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex expression-project row export requires a typed expression projection payload; no fallback execution was attempted"
                    .to_string(),
            )
        })?)
    } else {
        None
    };
    let output_columns = if duplicate_mask_requested {
        vec!["duplicated".to_string()]
    } else if drop_duplicate_requested {
        drop_duplicate_output_columns(file.dtype(), request)?
    } else if let Some(expression_projection) = expression_projection {
        expression_projection_output_columns(&declared_columns, expression_projection)
    } else if let Some(melt_projection) = melt_projection {
        melt_projection.output_columns()
    } else if let Some(explode_projection) = explode_projection {
        explode_projection.output_columns(&declared_columns)
    } else if let Some(rolling_window) = rolling_window {
        rolling_window.output_columns()
    } else if sample_requested {
        sample_output_columns(file.dtype(), request)?
    } else {
        declared_columns.clone()
    };
    if let Some(expression_projection) = expression_projection {
        validate_expression_projection_columns(expression_projection, &declared_columns)?;
    }

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
            write_csv_header(&mut output, &output_columns)?;
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
        let distinct_requested = request.kind == VortexQueryPrimitiveKind::DistinctRows;
        let mut distinct_keys = std::collections::BTreeSet::new();
        let mut duplicate_keys = std::collections::BTreeSet::new();
        let duplicate_keep = request.duplicate_keep;
        let duplicate_needs_full_scan =
            duplicate_mask_requested && duplicate_keep != VortexDuplicateKeepPolicy::First;
        let mut duplicate_counts = std::collections::BTreeMap::<String, usize>::new();
        let mut duplicate_last_positions = std::collections::BTreeMap::<String, usize>::new();
        let mut duplicate_output_keys = Vec::<(usize, String)>::new();
        let mut duplicate_global_index = 0usize;
        let drop_duplicate_key_columns = if drop_duplicate_requested {
            drop_duplicate_key_columns(file.dtype(), request)?
        } else {
            Vec::new()
        };
        let mut drop_duplicate_counts = std::collections::BTreeMap::<String, usize>::new();
        let mut drop_duplicate_first_positions = std::collections::BTreeMap::<String, usize>::new();
        let mut drop_duplicate_last_positions = std::collections::BTreeMap::<String, usize>::new();
        let mut drop_duplicate_rows = Vec::<(usize, String, Vec<StatValue>)>::new();
        let mut drop_duplicate_global_index = 0usize;
        let tail_requested = request.kind == VortexQueryPrimitiveKind::TailRows;
        let mut tail_rows = std::collections::VecDeque::<Vec<StatValue>>::new();
        let mut sample_rows = Vec::<(u64, usize, Vec<StatValue>)>::new();
        let mut weighted_sample_rows = Vec::<(f64, usize, Vec<StatValue>)>::new();
        let mut sample_replacement_population = Vec::<Vec<StatValue>>::new();
        let mut weighted_sample_replacement_population = Vec::<(Vec<StatValue>, f64)>::new();
        let mut expression_project_row_offset = 0_u64;
        let mut expression_projection_state = ExpressionProjectionState::default();
        let sample_weight_column_index = request
            .sample_weight_column
            .as_ref()
            .map(|column| column_index(&declared_columns, column.as_str()))
            .transpose()?;
        let mut melt_value_dtype: Option<LogicalDType> = None;
        let mut rolling_state =
            rolling_window.map(|request| RollingWindowState::new(request.window_size));
        let sample_seed = request.sample_seed.unwrap_or(0);
        let sample_fraction = normalized_sample_fraction(request.sample_fraction)?;
        let sample_fraction_candidate_cap = if sample_requested
            && !request.sample_with_replacement
            && source_order_limit.is_none()
        {
            sample_fraction
                .map(|fraction| {
                    let source_rows = usize::try_from(source_row_count).map_err(|_| {
                        ShardLoomError::InvalidOperation(
                            "local Vortex sample source row count did not fit usize; no fallback execution was attempted"
                                .to_string(),
                        )
                    })?;
                    fractional_sample_size(source_rows, fraction)
                })
                .transpose()?
        } else {
            None
        };
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
            if let Some(explode_projection) = explode_projection {
                let explode_columns =
                    explode_columns_from_chunk(&chunk, &declared_columns, explode_projection)?;
                pre_limit_result_row_count = pre_limit_result_row_count
                    .checked_add(explode_columns.expanded_rows)
                    .ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "local Vortex explode row export pre-limit row count overflowed usize"
                                .to_string(),
                        )
                    })?;
                let output_rows =
                    source_order_limit.map_or(explode_columns.expanded_rows, |limit| {
                        limit
                            .saturating_sub(rows_written)
                            .min(explode_columns.expanded_rows)
                    });
                let written = write_row_export_explode_rows(
                    &mut output,
                    output_format,
                    &explode_columns,
                    output_rows,
                )?;
                rows_written = rows_written.checked_add(written).ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex explode row export row count overflowed usize".to_string(),
                    )
                })?;
                max_chunk_rows = max_chunk_rows.max(chunk_rows);
                arrays_read_count += 1;
                if source_order_limit.is_some_and(|limit| rows_written >= limit) {
                    break;
                }
                continue;
            }
            let mut columns = if duplicate_mask_requested {
                Vec::new()
            } else if drop_duplicate_requested {
                row_export_columns_from_chunk(&chunk, &output_columns)?
            } else {
                row_export_columns_from_chunk(&chunk, &declared_columns)?
            };
            let mut chunk_columns = if drop_duplicate_requested {
                output_columns.clone()
            } else {
                declared_columns.clone()
            };
            if let Some(expression_projection) = expression_projection {
                apply_expression_projection_columns(
                    &mut chunk_columns,
                    &mut columns,
                    expression_projection,
                    expression_project_row_offset,
                    &mut expression_projection_state,
                )?;
                let materialized_rows = row_export_materialized_row_count(&columns, chunk_rows)?;
                expression_project_row_offset = expression_project_row_offset
                    .checked_add(usize_to_u64(materialized_rows)?)
                    .ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "local Vortex expression-project row-number offset overflowed; no fallback execution was attempted"
                                .to_string(),
                        )
                    })?;
            }
            if distinct_requested {
                let row_key_columns =
                    row_key_columns_from_chunk(&chunk, &declared_columns, &declared_columns)?;
                let selected_rows = distinct_row_indices(
                    &row_key_columns,
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
            } else if drop_duplicate_requested {
                let row_key_columns = row_key_columns_from_chunk(
                    &chunk,
                    &declared_columns,
                    &drop_duplicate_key_columns,
                )?;
                let materialized_rows = row_key_materialized_row_count(&row_key_columns)?;
                let output_rows = row_export_materialized_row_count(&columns, chunk_rows)?;
                if output_rows != materialized_rows {
                    return Err(ShardLoomError::InvalidOperation(
                        "local Vortex drop_duplicates row-key and output columns had mismatched row counts; no fallback execution was attempted"
                            .to_string(),
                    ));
                }
                for local_index in 0..materialized_rows {
                    let key = row_key_from_key_columns(&row_key_columns, local_index)?;
                    *drop_duplicate_counts.entry(key.clone()).or_insert(0) += 1;
                    drop_duplicate_first_positions
                        .entry(key.clone())
                        .or_insert(drop_duplicate_global_index);
                    drop_duplicate_last_positions.insert(key.clone(), drop_duplicate_global_index);
                    let row = row_export_materialized_projected_row(
                        &output_columns,
                        &columns,
                        &output_columns,
                        local_index,
                    )?;
                    drop_duplicate_rows.push((drop_duplicate_global_index, key, row));
                    drop_duplicate_global_index =
                        drop_duplicate_global_index.checked_add(1).ok_or_else(|| {
                            ShardLoomError::InvalidOperation(
                                "local Vortex drop_duplicates row ordinal overflowed usize"
                                    .to_string(),
                            )
                        })?;
                }
            } else if duplicate_mask_requested {
                let row_key_columns =
                    row_key_columns_from_chunk(&chunk, &declared_columns, &declared_columns)?;
                let materialized_rows = row_key_materialized_row_count(&row_key_columns)?;
                pre_limit_result_row_count =
                    pre_limit_result_row_count
                        .checked_add(materialized_rows)
                        .ok_or_else(|| {
                            ShardLoomError::InvalidOperation(
                                "local Vortex duplicate-mask row export pre-limit row count overflowed usize"
                                    .to_string(),
                            )
                        })?;
                if duplicate_needs_full_scan {
                    for row_index in 0..materialized_rows {
                        let key = row_key_from_key_columns(&row_key_columns, row_index)?;
                        *duplicate_counts.entry(key.clone()).or_insert(0) += 1;
                        duplicate_last_positions.insert(key.clone(), duplicate_global_index);
                        if source_order_limit
                            .is_none_or(|limit| duplicate_output_keys.len() < limit)
                        {
                            duplicate_output_keys.push((duplicate_global_index, key));
                        }
                        duplicate_global_index =
                            duplicate_global_index.checked_add(1).ok_or_else(|| {
                                ShardLoomError::InvalidOperation(
                                    "local Vortex duplicate-mask row ordinal overflowed usize"
                                        .to_string(),
                                )
                            })?;
                    }
                } else {
                    let output_rows = source_order_limit.map_or(materialized_rows, |limit| {
                        limit.saturating_sub(rows_written).min(materialized_rows)
                    });
                    let duplicate_values =
                        duplicate_mask_values(&row_key_columns, &mut duplicate_keys, output_rows)?;
                    write_row_export_chunk(
                        &mut output,
                        output_format,
                        &output_columns,
                        &[duplicate_values],
                        output_rows,
                    )?;
                    rows_written = rows_written.checked_add(output_rows).ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "local Vortex duplicate-mask row export row count overflowed usize"
                                .to_string(),
                        )
                    })?;
                }
            } else if let Some(melt_projection) = melt_projection {
                let materialized_rows = row_export_materialized_row_count(&columns, chunk_rows)?;
                validate_melt_value_dtype(&columns, melt_projection, &mut melt_value_dtype)?;
                let expanded_rows = checked_melt_expanded_rows(materialized_rows, melt_projection)?;
                pre_limit_result_row_count = pre_limit_result_row_count
                    .checked_add(expanded_rows)
                    .ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "local Vortex melt row export pre-limit row count overflowed usize"
                                .to_string(),
                        )
                    })?;
                let output_rows = source_order_limit.map_or(expanded_rows, |limit| {
                    limit.saturating_sub(rows_written).min(expanded_rows)
                });
                let written = write_row_export_melt_rows(
                    &mut output,
                    output_format,
                    melt_projection,
                    &columns,
                    materialized_rows,
                    output_rows,
                )?;
                rows_written = rows_written.checked_add(written).ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex melt row export row count overflowed usize".to_string(),
                    )
                })?;
            } else if let Some(rolling_window) = rolling_window {
                let materialized_rows = row_export_materialized_row_count(&columns, chunk_rows)?;
                let Some(state) = rolling_state.as_mut() else {
                    return Err(ShardLoomError::InvalidOperation(
                        "local Vortex rolling row export state was not initialized; no fallback execution was attempted"
                            .to_string(),
                    ));
                };
                let output_values = rolling_window_values(
                    &columns,
                    rolling_window,
                    state,
                    materialized_rows,
                    false,
                )?;
                pre_limit_result_row_count = pre_limit_result_row_count
                    .checked_add(output_values.len())
                    .ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "local Vortex rolling row export pre-limit row count overflowed usize"
                                .to_string(),
                        )
                    })?;
                let output_rows = source_order_limit.map_or(output_values.len(), |limit| {
                    limit.saturating_sub(rows_written).min(output_values.len())
                });
                write_row_export_chunk(
                    &mut output,
                    output_format,
                    &output_columns,
                    &[output_values],
                    output_rows,
                )?;
                rows_written = rows_written.checked_add(output_rows).ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex rolling row export row count overflowed usize".to_string(),
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
                    let row = row_export_materialized_projected_row(
                        &declared_columns,
                        &columns,
                        &output_columns,
                        local_index,
                    )?;
                    if let Some(weight_column_index) = sample_weight_column_index {
                        let weight =
                            sample_weight_value(&columns[weight_column_index][local_index])?;
                        let score =
                            deterministic_weighted_sample_score(sample_seed, row_index, weight);
                        if request.sample_with_replacement {
                            weighted_sample_replacement_population.push((row, weight));
                        } else if let Some(limit) = source_order_limit {
                            insert_weighted_sample_row_export_candidate(
                                &mut weighted_sample_rows,
                                limit,
                                score,
                                row_index,
                                row,
                            );
                        } else if let Some(candidate_cap) = sample_fraction_candidate_cap {
                            insert_weighted_sample_row_export_candidate(
                                &mut weighted_sample_rows,
                                candidate_cap,
                                score,
                                row_index,
                                row,
                            );
                        } else {
                            weighted_sample_rows.push((score, row_index, row));
                        }
                    } else {
                        let score = deterministic_sample_score(sample_seed, row_index);
                        if request.sample_with_replacement {
                            sample_replacement_population.push(row);
                        } else if let Some(limit) = source_order_limit {
                            insert_sample_row_export_candidate(
                                &mut sample_rows,
                                limit,
                                score,
                                row_index,
                                row,
                            );
                        } else if let Some(candidate_cap) = sample_fraction_candidate_cap {
                            insert_sample_row_export_candidate(
                                &mut sample_rows,
                                candidate_cap,
                                score,
                                row_index,
                                row,
                            );
                        } else {
                            sample_rows.push((score, row_index, row));
                        }
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
                    &chunk_columns,
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
            if !duplicate_needs_full_scan
                && source_order_limit.is_some_and(|limit| rows_written >= limit)
            {
                break;
            }
        }
        if let Some(rolling_window) = rolling_window
            && rolling_window.center
            && source_order_limit.is_none_or(|limit| rows_written < limit)
        {
            let Some(state) = rolling_state.as_mut() else {
                return Err(ShardLoomError::InvalidOperation(
                    "local Vortex centered rolling row export state was not initialized; no fallback execution was attempted"
                        .to_string(),
                ));
            };
            let output_values = state.emit_ready_centered(rolling_window, true)?;
            pre_limit_result_row_count = pre_limit_result_row_count
                .checked_add(output_values.len())
                .ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex centered rolling row export pre-limit row count overflowed usize"
                            .to_string(),
                    )
                })?;
            let output_rows = source_order_limit.map_or(output_values.len(), |limit| {
                limit.saturating_sub(rows_written).min(output_values.len())
            });
            write_row_export_chunk(
                &mut output,
                output_format,
                &output_columns,
                &[output_values],
                output_rows,
            )?;
            rows_written = rows_written.checked_add(output_rows).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex centered rolling row export row count overflowed usize"
                        .to_string(),
                )
            })?;
        }
        if drop_duplicate_requested {
            let mut selected_rows = retain_drop_duplicate_rows_from_policy(
                &drop_duplicate_rows,
                &drop_duplicate_counts,
                &drop_duplicate_first_positions,
                &drop_duplicate_last_positions,
                duplicate_keep,
                None,
            )?;
            pre_limit_result_row_count = selected_rows.len();
            if let Some(limit) = source_order_limit {
                selected_rows.truncate(limit);
            }
            write_row_export_materialized_rows(
                &mut output,
                output_format,
                &output_columns,
                &selected_rows,
            )?;
            rows_written = selected_rows.len();
        } else if duplicate_needs_full_scan {
            let duplicate_values = duplicate_mask_values_from_policy(
                &duplicate_output_keys,
                &duplicate_counts,
                &duplicate_last_positions,
                duplicate_keep,
            )?;
            write_row_export_chunk(
                &mut output,
                output_format,
                &output_columns,
                &[duplicate_values],
                duplicate_output_keys.len(),
            )?;
            rows_written = duplicate_output_keys.len();
        } else if tail_requested {
            let selected_rows = tail_rows.into_iter().collect::<Vec<_>>();
            write_row_export_materialized_rows(
                &mut output,
                output_format,
                &output_columns,
                &selected_rows,
            )?;
            rows_written = selected_rows.len();
        } else if sample_requested {
            let target_count = sample_target_count(request, pre_limit_result_row_count)?;
            let selected_rows = if request.sample_with_replacement {
                if sample_weight_column_index.is_some() {
                    deterministic_weighted_sample_replacement_rows(
                        sample_seed,
                        target_count,
                        &weighted_sample_replacement_population,
                    )?
                } else {
                    (0..target_count)
                        .map(|draw_index| {
                            let row_index = deterministic_sample_replacement_index(
                                sample_seed,
                                draw_index,
                                sample_replacement_population.len(),
                            );
                            sample_replacement_population[row_index].clone()
                        })
                        .collect::<Vec<_>>()
                }
            } else if sample_weight_column_index.is_some() {
                truncate_weighted_sample_candidates_to_target(
                    &mut weighted_sample_rows,
                    target_count,
                );
                weighted_sample_rows
                    .into_iter()
                    .map(|(_score, _row_index, row)| row)
                    .collect::<Vec<_>>()
            } else {
                truncate_sample_candidates_to_target(&mut sample_rows, target_count);
                sample_rows
                    .into_iter()
                    .map(|(_score, _row_index, row)| row)
                    .collect::<Vec<_>>()
            };
            write_row_export_materialized_rows(
                &mut output,
                output_format,
                &output_columns,
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
            projected_columns: output_columns,
            arrays_read_count,
            max_chunk_rows,
            max_parallelism_requested: policy.max_parallelism,
            scan_concurrency_per_worker: policy.scan_concurrency_per_worker(),
            source_order_limit_requested: source_order_limit.map(usize_to_u64).transpose()?,
            state_budget: row_export_state_budget_report(
                request,
                pre_limit_result_row_count,
                rows_written,
            )?,
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
#[allow(clippy::too_many_lines)]
fn execute_vortex_local_structured_binary_row_export_enabled(
    request: &VortexQueryPrimitiveRequest,
    source_path: &std::path::Path,
    output_path: &std::path::Path,
    output_format: VortexLocalPrimitiveRowExportFormat,
    allow_overwrite: bool,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<VortexLocalPrimitiveRowExportReport> {
    #[cfg(not(feature = "universal-format-io"))]
    {
        let _ = source_path;
        let _ = allow_overwrite;
        let _ = policy;
        return Ok(VortexLocalPrimitiveRowExportReport::blocked(
            request.kind,
            output_path,
            output_format,
            Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "vortex_local_structured_binary_row_export",
                "local Vortex structured Vortex/Parquet/Arrow IPC/Avro export requires universal-format-io",
                Some("use a release-user-surfaces build or rebuild with universal-format-io,vortex-local-primitives".to_string()),
            ),
        ));
    }
    #[cfg(feature = "universal-format-io")]
    {
        use std::io::Write as _;
        use vortex::VortexSessionDefault as _;
        use vortex::file::OpenOptionsSessionExt as _;
        use vortex::io::runtime::BlockingRuntime as _;
        use vortex::io::runtime::single::SingleThreadRuntime;
        use vortex::io::session::RuntimeSessionExt as _;
        use vortex::session::VortexSession;

        let Some(structured_projection) = request.structured_projection.as_ref() else {
            return Ok(VortexLocalPrimitiveRowExportReport::blocked(
                request.kind,
                output_path,
                output_format,
                Diagnostic::unsupported(
                    DiagnosticCode::NotImplemented,
                    "vortex_local_structured_binary_row_export",
                    "structured Vortex/compatibility row export requires a structured expression-project payload",
                    Some("pass expression-project JSON with structured_columns containing source, array, or struct projections".to_string()),
                ),
            ));
        };
        if structured_projection.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex structured row export requires at least one output column; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        let runtime = SingleThreadRuntime::default();
        let session = VortexSession::default().with_handle(runtime.handle());
        let file = runtime
            .block_on(session.open_options().open_path(source_path))
            .map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to open local Vortex target for structured row export: {error}"
                ))
            })?;
        let source_row_count = file.row_count();
        let plan = row_export_scan_plan(request, file.dtype())?;
        let declared_columns = if plan.projected_columns.is_empty() {
            structured_projection
                .source_columns()
                .into_iter()
                .map(|column| column.as_str().to_string())
                .collect::<Vec<_>>()
        } else {
            plan.projected_columns.clone()
        };
        if declared_columns.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex structured row export requires at least one source column so the route remains Vortex-derived; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        if plan.source_order_limit == Some(0) {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex structured row export row count must be >= 1".to_string(),
            ));
        }
        let output_columns = structured_projection.output_columns();
        let column_dtypes = structured_projection
            .columns
            .iter()
            .map(|column| match column.expr {
                VortexStructuredProjectionExpr::ArrayLiteral(_) => Some(LogicalDType::List),
                VortexStructuredProjectionExpr::StructColumns(_) => Some(LogicalDType::Struct),
                VortexStructuredProjectionExpr::SourceColumn(_) => None,
            })
            .collect::<Vec<_>>();

        let filter_pushdown_applied = plan.filter.is_some();
        let projection_pushdown_applied = plan.projection.is_some();
        let mut scan = file.scan().map_err(vortex_error)?;
        if let Some(filter) = plan.filter {
            scan = scan.with_filter(filter);
        }
        if let Some(projection) = plan.projection {
            scan = scan.with_projection(projection);
        }
        scan = scan.with_concurrency(policy.scan_concurrency_per_worker());

        let mut rows = Vec::<Vec<(String, ScalarValue)>>::new();
        let mut pre_limit_result_row_count = 0usize;
        let mut arrays_read_count = 0usize;
        let mut max_chunk_rows = 0usize;
        for chunk in scan.into_array_iter(&runtime).map_err(vortex_error)? {
            let chunk = chunk.map_err(vortex_error)?;
            let chunk_rows = chunk.len();
            let columns = row_export_columns_from_chunk(&chunk, &declared_columns)?;
            let materialized_rows = row_export_materialized_row_count(&columns, chunk_rows)?;
            pre_limit_result_row_count = pre_limit_result_row_count
                .checked_add(materialized_rows)
                .ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex structured binary row export pre-limit row count overflowed usize"
                            .to_string(),
                    )
                })?;
            let remaining = plan
                .source_order_limit
                .map_or(materialized_rows, |limit| limit.saturating_sub(rows.len()));
            let output_rows = remaining.min(materialized_rows);
            for row_index in 0..output_rows {
                rows.push(structured_projection_row(
                    structured_projection,
                    &declared_columns,
                    &columns,
                    row_index,
                )?);
            }
            max_chunk_rows = max_chunk_rows.max(chunk_rows);
            arrays_read_count += 1;
            if plan
                .source_order_limit
                .is_some_and(|limit| rows.len() >= limit)
            {
                break;
            }
        }

        if output_format == VortexLocalPrimitiveRowExportFormat::Vortex {
            return execute_vortex_local_structured_vortex_row_export_enabled(
                request,
                output_path,
                allow_overwrite,
                output_columns,
                column_dtypes,
                rows,
                source_row_count,
                pre_limit_result_row_count,
                arrays_read_count,
                max_chunk_rows,
                policy,
                plan.source_order_limit,
                filter_pushdown_applied,
                projection_pushdown_applied,
            );
        }

        let bytes = encode_structured_row_export_bytes(
            output_format,
            &output_columns,
            &column_dtypes,
            &rows,
        )?;
        let temp_path = temporary_output_path(output_path)?;
        prepare_output_target(output_path, &temp_path, allow_overwrite)?;
        let write_result = (|| -> Result<VortexLocalPrimitiveRowExportReport> {
            let mut output = std::fs::File::create_new(&temp_path).map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to create local Vortex structured row export temp file {}: {error}",
                    temp_path.display()
                ))
            })?;
            output.write_all(&bytes).map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to write local Vortex structured row export {}: {error}",
                    temp_path.display()
                ))
            })?;
            output.flush().map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to flush local Vortex structured row export {}: {error}",
                    temp_path.display()
                ))
            })?;
            drop(output);
            std::fs::rename(&temp_path, output_path).map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to commit local Vortex structured row export {} -> {}: {error}",
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
                rows_written: usize_to_u64(rows.len())?,
                pre_limit_result_row_count: usize_to_u64(pre_limit_result_row_count)?,
                projected_columns: output_columns,
                arrays_read_count,
                max_chunk_rows,
                max_parallelism_requested: policy.max_parallelism,
                scan_concurrency_per_worker: policy.scan_concurrency_per_worker(),
                source_order_limit_requested: plan
                    .source_order_limit
                    .map(usize_to_u64)
                    .transpose()?,
                state_budget: row_export_state_budget_report(
                    request,
                    pre_limit_result_row_count,
                    rows.len(),
                )?,
                evidence: executed_row_export_evidence(
                    filter_pushdown_applied,
                    projection_pushdown_applied,
                    plan.source_order_limit.is_some(),
                ),
                diagnostics: Vec::new(),
            })
        })();
        if write_result.is_err() {
            let _ = std::fs::remove_file(&temp_path);
        }
        write_result
    }
}

#[cfg(all(feature = "vortex-local-primitives", feature = "universal-format-io"))]
#[allow(clippy::too_many_arguments)]
fn execute_vortex_local_structured_vortex_row_export_enabled(
    request: &VortexQueryPrimitiveRequest,
    output_path: &std::path::Path,
    allow_overwrite: bool,
    output_columns: Vec<String>,
    column_dtypes: Vec<Option<LogicalDType>>,
    rows: Vec<Vec<(String, ScalarValue)>>,
    rows_scanned: u64,
    pre_limit_result_row_count: usize,
    arrays_read_count: usize,
    max_chunk_rows: usize,
    policy: VortexLocalPrimitiveExecutionPolicy,
    source_order_limit: Option<usize>,
    filter_pushdown_applied: bool,
    projection_pushdown_applied: bool,
) -> Result<VortexLocalPrimitiveRowExportReport> {
    #[cfg(not(feature = "vortex-write"))]
    {
        let _ = request;
        let _ = allow_overwrite;
        let _ = output_columns;
        let _ = column_dtypes;
        let _ = rows;
        let _ = rows_scanned;
        let _ = pre_limit_result_row_count;
        let _ = arrays_read_count;
        let _ = max_chunk_rows;
        let _ = policy;
        let _ = source_order_limit;
        let _ = filter_pushdown_applied;
        let _ = projection_pushdown_applied;
        Ok(VortexLocalPrimitiveRowExportReport::blocked(
            request.kind,
            output_path,
            VortexLocalPrimitiveRowExportFormat::Vortex,
            Diagnostic::unsupported(
                DiagnosticCode::NotImplemented,
                "vortex_local_structured_vortex_row_export",
                "local Vortex structured row export requires vortex-write",
                Some("use a release-user-surfaces build or rebuild with vortex-write,universal-format-io,vortex-local-primitives".to_string()),
            ),
        ))
    }
    #[cfg(feature = "vortex-write")]
    {
        let rows_written = rows.len();
        let write_report = crate::write_flat_scalar_vortex_prepared_state(
            crate::VortexPreparedStateWriteRequest::new(
                output_path.to_path_buf(),
                output_columns.clone(),
                rows,
            )
            .column_dtypes(column_dtypes)
            .allow_overwrite(allow_overwrite)
            .certification_level(crate::VortexIngestCertificationLevel::IngestCertified),
        )?;
        Ok(VortexLocalPrimitiveRowExportReport {
            status: VortexLocalPrimitiveExecutionStatus::Executed,
            primitive_kind: request.kind,
            output_path: write_report.target_path.display().to_string(),
            output_format: VortexLocalPrimitiveRowExportFormat::Vortex.as_str(),
            rows_scanned,
            rows_written: write_report.reopen_row_count,
            pre_limit_result_row_count: usize_to_u64(pre_limit_result_row_count)?,
            projected_columns: output_columns,
            arrays_read_count,
            max_chunk_rows,
            max_parallelism_requested: policy.max_parallelism,
            scan_concurrency_per_worker: policy.scan_concurrency_per_worker(),
            source_order_limit_requested: source_order_limit.map(usize_to_u64).transpose()?,
            state_budget: row_export_state_budget_report(
                request,
                pre_limit_result_row_count,
                rows_written,
            )?,
            evidence: executed_row_export_evidence(
                filter_pushdown_applied,
                projection_pushdown_applied,
                source_order_limit.is_some(),
            ),
            diagnostics: Vec::new(),
        })
    }
}

#[cfg(all(feature = "vortex-local-primitives", feature = "universal-format-io"))]
fn encode_structured_row_export_bytes(
    output_format: VortexLocalPrimitiveRowExportFormat,
    output_columns: &[String],
    column_dtypes: &[Option<LogicalDType>],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>> {
    match output_format {
        VortexLocalPrimitiveRowExportFormat::Parquet => {
            crate::encode_flat_parquet_rows_with_dtypes(output_columns, column_dtypes, rows)
        }
        VortexLocalPrimitiveRowExportFormat::ArrowIpc => {
            crate::encode_flat_arrow_ipc_rows_with_dtypes(output_columns, column_dtypes, rows)
        }
        VortexLocalPrimitiveRowExportFormat::Avro => {
            crate::encode_flat_avro_rows_with_dtypes(output_columns, column_dtypes, rows)
        }
        VortexLocalPrimitiveRowExportFormat::Jsonl
        | VortexLocalPrimitiveRowExportFormat::Csv
        | VortexLocalPrimitiveRowExportFormat::Vortex => {
            Err(ShardLoomError::InvalidOperation(
                "structured compatibility binary row export received a non-compatibility format; no fallback execution was attempted"
                    .to_string(),
            ))
        }
    }
}

#[cfg(all(feature = "vortex-local-primitives", feature = "universal-format-io"))]
fn structured_projection_row(
    projection: &VortexStructuredProjectionRequest,
    source_columns: &[String],
    column_values: &[Vec<StatValue>],
    row_index: usize,
) -> Result<Vec<(String, ScalarValue)>> {
    let mut row = Vec::with_capacity(projection.columns.len());
    for column in &projection.columns {
        let value = match &column.expr {
            VortexStructuredProjectionExpr::SourceColumn(source_column) => {
                source_scalar_value(source_columns, column_values, source_column, row_index)?
            }
            VortexStructuredProjectionExpr::ArrayLiteral(values) => {
                ScalarValue::List(values.clone())
            }
            VortexStructuredProjectionExpr::StructColumns(fields) => {
                let mut values = Vec::with_capacity(fields.len());
                for field in fields {
                    values.push((
                        field.as_str().to_string(),
                        source_scalar_value(source_columns, column_values, field, row_index)?,
                    ));
                }
                ScalarValue::Struct(values)
            }
        };
        row.push((column.output_column.clone(), value));
    }
    Ok(row)
}

#[cfg(all(feature = "vortex-local-primitives", feature = "universal-format-io"))]
fn source_scalar_value(
    source_columns: &[String],
    column_values: &[Vec<StatValue>],
    column: &ColumnRef,
    row_index: usize,
) -> Result<ScalarValue> {
    let Some(column_index) = source_columns
        .iter()
        .position(|candidate| candidate == column.as_str())
    else {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local Vortex structured row export source column '{}' was not scanned; no fallback execution was attempted",
            column.as_str()
        )));
    };
    let Some(value) = column_values
        .get(column_index)
        .and_then(|values| values.get(row_index))
    else {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local Vortex structured row export source column '{}' missing row {}; no fallback execution was attempted",
            column.as_str(),
            row_index
        )));
    };
    Ok(stat_value_to_scalar_value(value))
}

#[cfg(feature = "vortex-local-primitives")]
fn stat_value_to_scalar_value(value: &StatValue) -> ScalarValue {
    match value {
        StatValue::Null => ScalarValue::Null,
        StatValue::Boolean(value) => ScalarValue::Boolean(*value),
        StatValue::Int64(value) => ScalarValue::Int64(*value),
        StatValue::UInt64(value) => ScalarValue::UInt64(*value),
        StatValue::Float64(value) => ScalarValue::Float64(*value),
        StatValue::Utf8(value) => ScalarValue::Utf8(value.clone()),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn scalar_value_to_json_value(value: &ScalarValue) -> Result<serde_json::Value> {
    match value {
        ScalarValue::Null => Ok(serde_json::Value::Null),
        ScalarValue::Boolean(value) => Ok(serde_json::Value::Bool(*value)),
        ScalarValue::Int64(value) | ScalarValue::TimestampMicros(value) => {
            Ok(serde_json::Value::Number((*value).into()))
        }
        ScalarValue::UInt64(value) => Ok(serde_json::Value::Number((*value).into())),
        ScalarValue::Float64(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex typed row export cannot serialize non-finite float values; no fallback execution was attempted"
                        .to_string(),
                )
            }),
        ScalarValue::Utf8(value) => Ok(serde_json::Value::String(value.clone())),
        ScalarValue::Binary(value) => Ok(serde_json::Value::String(binary_to_hex(value))),
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => Ok(serde_json::Value::String(format!(
            "decimal128({precision},{scale}):{value}"
        ))),
        ScalarValue::Date32(value) => Ok(serde_json::Value::Number((*value).into())),
        ScalarValue::List(values) => values
            .iter()
            .map(scalar_value_to_json_value)
            .collect::<Result<Vec<_>>>()
            .map(serde_json::Value::Array),
        ScalarValue::Struct(fields) => {
            let mut object = serde_json::Map::with_capacity(fields.len());
            for (field, value) in fields {
                object.insert(field.clone(), scalar_value_to_json_value(value)?);
            }
            Ok(serde_json::Value::Object(object))
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn binary_to_hex(value: &[u8]) -> String {
    let mut out = String::with_capacity(value.len() * 2);
    for byte in value {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(feature = "vortex-local-primitives")]
fn scalar_value_to_csv_cell(value: &ScalarValue) -> Result<String> {
    match value {
        ScalarValue::Null => Ok(String::new()),
        ScalarValue::Boolean(value) => Ok(value.to_string()),
        ScalarValue::Int64(value) => Ok(value.to_string()),
        ScalarValue::UInt64(value) => Ok(value.to_string()),
        ScalarValue::Float64(value) => Ok(value.to_string()),
        ScalarValue::Utf8(value) => Ok(csv_escape(value)),
        ScalarValue::Binary(_)
        | ScalarValue::Decimal128 { .. }
        | ScalarValue::Date32(_)
        | ScalarValue::TimestampMicros(_)
        | ScalarValue::List(_)
        | ScalarValue::Struct(_) => {
            scalar_value_to_json_value(value).map(|json| csv_escape(&json.to_string()))
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::too_many_lines)]
fn execute_vortex_local_pivot_row_export_enabled(
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

    let Some(uri) = request.source_uri.as_ref() else {
        return Ok(VortexLocalPrimitiveRowExportReport::blocked(
            request.kind,
            output_path,
            output_format,
            Diagnostic::invalid_input(
                "vortex_local_primitive_row_export",
                "local Vortex pivot row export requires a source URI",
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
                    "unsupported local Vortex pivot row export target: {}",
                    uri.as_str()
                ),
                "provide an existing local path or file:// `.vortex` target",
            ),
        ));
    };
    let pivot_projection = required_pivot_projection(request)?;
    let aggregate = normalized_pivot_aggregate(pivot_projection)?;
    let temp_path = temporary_output_path(output_path)?;
    prepare_output_target(output_path, &temp_path, allow_overwrite)?;

    let runtime = SingleThreadRuntime::default();
    let session = VortexSession::default().with_handle(runtime.handle());
    let file = runtime
        .block_on(session.open_options().open_path(&path))
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to open local Vortex target for pivot row export: {error}"
            ))
        })?;
    let source_row_count = file.row_count();
    let plan = row_export_scan_plan(request, file.dtype())?;
    if plan.source_order_limit == Some(0) {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex pivot row export row count must be >= 1".to_string(),
        ));
    }
    let declared_columns = if plan.projected_columns.is_empty() {
        vec![
            pivot_projection.index_column.as_str().to_string(),
            pivot_projection.pivot_column.as_str().to_string(),
            pivot_projection.value_column.as_str().to_string(),
        ]
    } else {
        plan.projected_columns.clone()
    };
    let expected_columns = [
        pivot_projection.index_column.as_str(),
        pivot_projection.pivot_column.as_str(),
        pivot_projection.value_column.as_str(),
    ];
    if declared_columns
        .iter()
        .map(String::as_str)
        .ne(expected_columns)
    {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex pivot row export requires exactly index, pivot, and value projected columns; no fallback execution was attempted"
                .to_string(),
        ));
    }

    let projection_pushdown_applied = plan.projection.is_some();
    let mut scan = file.scan().map_err(vortex_error)?;
    if let Some(projection) = plan.projection {
        scan = scan.with_projection(projection);
    }
    scan = scan.with_concurrency(policy.scan_concurrency_per_worker());

    let mut arrays_read_count = 0usize;
    let mut max_chunk_rows = 0usize;
    let mut state = PivotRowExportState::default();
    for chunk in scan.into_array_iter(&runtime).map_err(vortex_error)? {
        let chunk = chunk.map_err(vortex_error)?;
        let chunk_rows = chunk.len();
        let columns = row_export_columns_from_chunk(&chunk, &declared_columns)?;
        if columns.len() != 3 {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex pivot row export requires exactly index, pivot, and value columns; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        state.update(
            pivot_projection,
            aggregate,
            &columns[0],
            &columns[1],
            &columns[2],
        )?;
        max_chunk_rows = max_chunk_rows.max(chunk_rows);
        arrays_read_count += 1;
    }
    let pre_limit_data_row_count = state.index_keys.len();
    let data_rows_written = plan
        .source_order_limit
        .map_or(pre_limit_data_row_count, |limit| {
            let limit_for_data_rows = if pivot_projection.margins {
                limit.saturating_sub(1)
            } else {
                limit
            };
            limit_for_data_rows.min(pre_limit_data_row_count)
        });
    let output_columns = state.output_columns(pivot_projection);
    let rows = state.materialized_rows(aggregate, pivot_projection, data_rows_written)?;
    let rows_written = rows.len();
    let pre_limit_result_row_count =
        pre_limit_data_row_count.saturating_add(usize::from(pivot_projection.margins));

    let write_result = (|| -> Result<VortexLocalPrimitiveRowExportReport> {
        let mut output = std::fs::File::create_new(&temp_path).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local Vortex pivot row export temp file {}: {error}",
                temp_path.display()
            ))
        })?;
        if output_format == VortexLocalPrimitiveRowExportFormat::Csv {
            write_csv_header(&mut output, &output_columns)?;
        }
        write_row_export_sparse_materialized_rows(
            &mut output,
            output_format,
            &output_columns,
            &rows,
        )?;
        output.flush().map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to flush local Vortex pivot row export {}: {error}",
                temp_path.display()
            ))
        })?;
        drop(output);
        std::fs::rename(&temp_path, output_path).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to commit local Vortex pivot row export {} -> {}: {error}",
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
            projected_columns: output_columns,
            arrays_read_count,
            max_chunk_rows,
            max_parallelism_requested: policy.max_parallelism,
            scan_concurrency_per_worker: policy.scan_concurrency_per_worker(),
            source_order_limit_requested: plan.source_order_limit.map(usize_to_u64).transpose()?,
            state_budget: pivot_row_export_state_budget_report(
                pre_limit_result_row_count,
                rows_written,
            )?,
            evidence: executed_row_export_evidence(
                false,
                projection_pushdown_applied,
                plan.source_order_limit.is_some(),
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
fn execute_vortex_local_simple_aggregate_row_export_enabled(
    request: &VortexQueryPrimitiveRequest,
    output_path: &std::path::Path,
    output_format: VortexLocalPrimitiveRowExportFormat,
    allow_overwrite: bool,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<VortexLocalPrimitiveRowExportReport> {
    use std::io::Write as _;

    let aggregate = required_simple_aggregate(request)?;
    let Some(uri) = request.source_uri.as_ref() else {
        return Ok(VortexLocalPrimitiveRowExportReport::blocked(
            request.kind,
            output_path,
            output_format,
            Diagnostic::invalid_input(
                "vortex_local_primitive_row_export",
                "local Vortex aggregate row export requires a source URI",
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
                    "unsupported local Vortex aggregate row export target: {}",
                    uri.as_str()
                ),
                "provide an existing local path or file:// `.vortex` target",
            ),
        ));
    };
    let scan = read_local_vortex_simple_aggregate_scan(uri, &path, request, policy)?;
    let output_columns = aggregate.output_columns();
    let result_rows = simple_aggregate_result_rows(&scan.result_summary, &output_columns)?;
    let temp_path = temporary_output_path(output_path)?;
    prepare_output_target(output_path, &temp_path, allow_overwrite)?;
    let write_result = (|| -> Result<VortexLocalPrimitiveRowExportReport> {
        let mut output = std::fs::File::create_new(&temp_path).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local Vortex aggregate row export temp file {}: {error}",
                temp_path.display()
            ))
        })?;
        if output_format == VortexLocalPrimitiveRowExportFormat::Csv {
            write_csv_header(&mut output, &output_columns)?;
        }
        write_simple_aggregate_result_rows(
            &mut output,
            output_format,
            &output_columns,
            &result_rows,
        )?;
        output.flush().map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to flush local Vortex aggregate row export {}: {error}",
                temp_path.display()
            ))
        })?;
        drop(output);
        std::fs::rename(&temp_path, output_path).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to commit local Vortex aggregate row export {} -> {}: {error}",
                temp_path.display(),
                output_path.display()
            ))
        })?;

        Ok(VortexLocalPrimitiveRowExportReport {
            status: VortexLocalPrimitiveExecutionStatus::Executed,
            primitive_kind: request.kind,
            output_path: output_path.display().to_string(),
            output_format: output_format.as_str(),
            rows_scanned: scan.scan.source_row_count,
            rows_written: usize_to_u64(result_rows.len())?,
            pre_limit_result_row_count: usize_to_u64(result_rows.len())?,
            projected_columns: output_columns,
            arrays_read_count: scan.scan.arrays_read_count,
            max_chunk_rows: scan.scan.max_chunk_rows,
            max_parallelism_requested: scan.scan.max_parallelism_requested,
            scan_concurrency_per_worker: scan.scan.scan_concurrency_per_worker,
            source_order_limit_requested: scan
                .scan
                .source_order_limit
                .map(usize_to_u64)
                .transpose()?,
            state_budget: scan.state_budget.clone(),
            evidence: executed_row_export_evidence(
                false,
                scan.scan.projection_pushdown_applied,
                scan.scan.source_order_limit.is_some(),
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
fn execute_vortex_local_sort_rows_row_export_enabled(
    request: &VortexQueryPrimitiveRequest,
    output_path: &std::path::Path,
    output_format: VortexLocalPrimitiveRowExportFormat,
    allow_overwrite: bool,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<VortexLocalPrimitiveRowExportReport> {
    use std::io::Write as _;

    let Some(uri) = request.source_uri.as_ref() else {
        return Ok(VortexLocalPrimitiveRowExportReport::blocked(
            request.kind,
            output_path,
            output_format,
            Diagnostic::invalid_input(
                "vortex_local_primitive_row_export",
                "local Vortex sort row export requires a source URI",
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
                    "unsupported local Vortex sort row export target: {}",
                    uri.as_str()
                ),
                "provide an existing local path or file:// `.vortex` target",
            ),
        ));
    };
    let scan = read_local_vortex_sort_rows_scan(uri, &path, request, policy)?;
    let output_columns = scan.scan.projected_columns.clone();
    let result_rows = sort_rows_result_rows(&scan.result_summary, &output_columns)?;
    let temp_path = temporary_output_path(output_path)?;
    prepare_output_target(output_path, &temp_path, allow_overwrite)?;
    let write_result = (|| -> Result<VortexLocalPrimitiveRowExportReport> {
        let mut output = std::fs::File::create_new(&temp_path).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local Vortex sort row export temp file {}: {error}",
                temp_path.display()
            ))
        })?;
        if output_format == VortexLocalPrimitiveRowExportFormat::Csv {
            write_csv_header(&mut output, &output_columns)?;
        }
        write_simple_aggregate_result_rows(
            &mut output,
            output_format,
            &output_columns,
            &result_rows,
        )?;
        output.flush().map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to flush local Vortex sort row export {}: {error}",
                temp_path.display()
            ))
        })?;
        drop(output);
        std::fs::rename(&temp_path, output_path).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to commit local Vortex sort row export {} -> {}: {error}",
                temp_path.display(),
                output_path.display()
            ))
        })?;
        Ok(VortexLocalPrimitiveRowExportReport {
            status: VortexLocalPrimitiveExecutionStatus::Executed,
            primitive_kind: request.kind,
            output_path: output_path.display().to_string(),
            output_format: output_format.as_str(),
            rows_scanned: scan.scan.source_row_count,
            rows_written: usize_to_u64(result_rows.len())?,
            pre_limit_result_row_count: usize_to_u64(scan.scan.pre_limit_result_row_count)?,
            projected_columns: output_columns,
            arrays_read_count: scan.scan.arrays_read_count,
            max_chunk_rows: scan.scan.max_chunk_rows,
            max_parallelism_requested: scan.scan.max_parallelism_requested,
            scan_concurrency_per_worker: scan.scan.scan_concurrency_per_worker,
            source_order_limit_requested: scan
                .scan
                .source_order_limit
                .map(usize_to_u64)
                .transpose()?,
            state_budget: sort_rows_state_budget_report(
                request,
                &scan.scan,
                usize_to_u64(scan.scan.pre_limit_result_row_count)?,
            )?,
            evidence: executed_row_export_evidence(
                scan.scan.filter_pushdown_applied,
                scan.scan.projection_pushdown_applied,
                scan.scan.source_order_limit.is_some(),
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
fn simple_aggregate_result_rows(
    result_summary: &str,
    output_columns: &[String],
) -> Result<Vec<Vec<serde_json::Value>>> {
    let payload = serde_json::from_str::<serde_json::Value>(result_summary).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "local Vortex aggregate result summary was not valid JSON: {error}; no fallback execution was attempted"
        ))
    })?;
    if payload
        .get("rows")
        .and_then(serde_json::Value::as_u64)
        .is_some_and(|rows| rows == 0)
    {
        return Ok(Vec::new());
    }
    let values = payload.get("values").ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "local Vortex aggregate result summary did not include values; no fallback execution was attempted"
                .to_string(),
        )
    })?;
    let rows = match values {
        serde_json::Value::Object(object) => vec![object],
        serde_json::Value::Array(rows) => rows
            .iter()
            .map(|row| {
                row.as_object().ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex grouped aggregate result row was not a JSON object; no fallback execution was attempted"
                            .to_string(),
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?,
        _ => {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex aggregate result summary values must be an object or row array; no fallback execution was attempted"
                    .to_string(),
            ));
        }
    };
    rows.into_iter()
        .map(|row| {
            output_columns
                .iter()
                .map(|column| {
                    row.get(column).cloned().ok_or_else(|| {
                        ShardLoomError::InvalidOperation(format!(
                            "local Vortex aggregate result summary did not include output alias '{column}'; no fallback execution was attempted"
                        ))
                    })
                })
                .collect()
        })
        .collect()
}

#[cfg(feature = "vortex-local-primitives")]
fn sort_rows_result_rows(
    result_summary: &str,
    output_columns: &[String],
) -> Result<Vec<Vec<serde_json::Value>>> {
    let payload = serde_json::from_str::<serde_json::Value>(result_summary).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "local Vortex sort rows result summary was not valid JSON: {error}; no fallback execution was attempted"
        ))
    })?;
    let values = payload
        .get("values")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex sort rows result summary did not include values array; no fallback execution was attempted"
                    .to_string(),
            )
        })?;
    values
        .iter()
        .map(|row| {
            let object = row.as_object().ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex sort rows result row was not a JSON object; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
            output_columns
                .iter()
                .map(|column| {
                    object.get(column).cloned().ok_or_else(|| {
                        ShardLoomError::InvalidOperation(format!(
                            "local Vortex sort rows result summary did not include output column '{column}'; no fallback execution was attempted"
                        ))
                    })
                })
                .collect()
        })
        .collect()
}

#[cfg(feature = "vortex-local-primitives")]
fn write_simple_aggregate_result_rows(
    output: &mut std::fs::File,
    format: VortexLocalPrimitiveRowExportFormat,
    output_columns: &[String],
    result_rows: &[Vec<serde_json::Value>],
) -> Result<()> {
    for row_values in result_rows {
        write_simple_aggregate_result_row(output, format, output_columns, row_values)?;
    }
    Ok(())
}

#[cfg(feature = "vortex-local-primitives")]
fn write_simple_aggregate_result_row(
    output: &mut std::fs::File,
    format: VortexLocalPrimitiveRowExportFormat,
    output_columns: &[String],
    row_values: &[serde_json::Value],
) -> Result<()> {
    use std::io::Write as _;

    if output_columns.len() != row_values.len() {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex aggregate row export had mismatched output column/value counts; no fallback execution was attempted"
                .to_string(),
        ));
    }
    match format {
        VortexLocalPrimitiveRowExportFormat::Jsonl => {
            let mut row = serde_json::Map::with_capacity(output_columns.len());
            for (column, value) in output_columns.iter().zip(row_values) {
                row.insert(column.clone(), value.clone());
            }
            let line = serde_json::Value::Object(row).to_string();
            writeln!(output, "{line}").map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to write local Vortex aggregate JSONL row: {error}"
                ))
            })?;
        }
        VortexLocalPrimitiveRowExportFormat::Csv => {
            let line = row_values
                .iter()
                .map(json_value_to_csv_cell)
                .collect::<Result<Vec<_>>>()?
                .join(",");
            writeln!(output, "{line}").map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to write local Vortex aggregate CSV row: {error}"
                ))
            })?;
        }
        VortexLocalPrimitiveRowExportFormat::Parquet
        | VortexLocalPrimitiveRowExportFormat::ArrowIpc
        | VortexLocalPrimitiveRowExportFormat::Avro
        | VortexLocalPrimitiveRowExportFormat::Vortex => {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex aggregate row export text writer received a structured or native Vortex format; no fallback execution was attempted"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "vortex-local-primitives")]
fn json_value_to_csv_cell(value: &serde_json::Value) -> Result<String> {
    match value {
        serde_json::Value::Null => Ok(String::new()),
        serde_json::Value::Bool(value) => Ok(value.to_string()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        serde_json::Value::String(value) => Ok(csv_escape(value)),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Err(
            ShardLoomError::InvalidOperation(
                "local Vortex aggregate CSV row export supports only scalar aggregate values; no fallback execution was attempted"
                    .to_string(),
            ),
        ),
    }
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
        VortexQueryPrimitiveKind::ProjectColumns
        | VortexQueryPrimitiveKind::TailRows
        | VortexQueryPrimitiveKind::DuplicateMaskRows
        | VortexQueryPrimitiveKind::ExpressionProjectRows
        | VortexQueryPrimitiveKind::MeltRows
        | VortexQueryPrimitiveKind::ExplodeRows
        | VortexQueryPrimitiveKind::PivotRows
        | VortexQueryPrimitiveKind::RollingWindowRows => {
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
        VortexQueryPrimitiveKind::DistinctRows => {
            let mut plan = projection_scan_plan(dtype, &request.projection, request.kind)?;
            if let Some(predicate) = request.predicate.as_ref() {
                plan.filter = Some(predicate_to_vortex_expr(predicate, dtype, request.kind)?);
            }
            plan.source_order_limit = request.source_order_limit;
            Ok(plan)
        }
        VortexQueryPrimitiveKind::DropDuplicateRows => drop_duplicate_scan_plan(dtype, request),
        VortexQueryPrimitiveKind::SampleRows => {
            let mut plan = sample_scan_plan(dtype, request)?;
            if let Some(predicate) = request.predicate.as_ref() {
                plan.filter = Some(predicate_to_vortex_expr(predicate, dtype, request.kind)?);
            }
            plan.source_order_limit = request.source_order_limit;
            Ok(plan)
        }
        VortexQueryPrimitiveKind::CountAll
        | VortexQueryPrimitiveKind::CountWhere
        | VortexQueryPrimitiveKind::SimpleAggregate
        | VortexQueryPrimitiveKind::SortRows
        | VortexQueryPrimitiveKind::Unsupported => Err(ShardLoomError::InvalidOperation(
            "local Vortex generic row export supports filter, project, filter-project, distinct, drop-duplicates, duplicate-mask, tail, sample, expression-project, melt, explode, pivot, and rolling-window primitives only; sort_rows uses its dedicated bounded row export path"
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
            VortexLocalPrimitiveRowExportFormat::Parquet
            | VortexLocalPrimitiveRowExportFormat::ArrowIpc
            | VortexLocalPrimitiveRowExportFormat::Avro
            | VortexLocalPrimitiveRowExportFormat::Vortex => {
                return Err(ShardLoomError::InvalidOperation(
                    "local Vortex row export text writer received a structured or native Vortex format; no fallback execution was attempted"
                        .to_string(),
                ));
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
            VortexLocalPrimitiveRowExportFormat::Parquet
            | VortexLocalPrimitiveRowExportFormat::ArrowIpc
            | VortexLocalPrimitiveRowExportFormat::Avro
            | VortexLocalPrimitiveRowExportFormat::Vortex => {
                return Err(ShardLoomError::InvalidOperation(
                    "local Vortex materialized row text writer received a structured or native Vortex format; no fallback execution was attempted"
                        .to_string(),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(feature = "vortex-local-primitives")]
fn write_row_export_sparse_materialized_rows(
    output: &mut std::fs::File,
    format: VortexLocalPrimitiveRowExportFormat,
    columns: &[String],
    rows: &[Vec<Option<StatValue>>],
) -> Result<()> {
    use std::io::Write as _;

    for row_values in rows {
        if row_values.len() != columns.len() {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex sparse row export materialized rows had mismatched column counts; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        match format {
            VortexLocalPrimitiveRowExportFormat::Jsonl => {
                let mut row = serde_json::Map::with_capacity(columns.len());
                for (column, value) in columns.iter().zip(row_values) {
                    row.insert(
                        column.clone(),
                        value
                            .as_ref()
                            .map_or(Ok(serde_json::Value::Null), stat_value_to_json_value)?,
                    );
                }
                let line = serde_json::Value::Object(row).to_string();
                writeln!(output, "{line}").map_err(|error| {
                    ShardLoomError::InvalidOperation(format!(
                        "failed to write local Vortex sparse row export JSONL row: {error}"
                    ))
                })?;
            }
            VortexLocalPrimitiveRowExportFormat::Csv => {
                let line = row_values
                    .iter()
                    .map(|value| {
                        value
                            .as_ref()
                            .map_or_else(String::new, stat_value_to_csv_cell)
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                writeln!(output, "{line}").map_err(|error| {
                    ShardLoomError::InvalidOperation(format!(
                        "failed to write local Vortex sparse row export CSV row: {error}"
                    ))
                })?;
            }
            VortexLocalPrimitiveRowExportFormat::Parquet
            | VortexLocalPrimitiveRowExportFormat::ArrowIpc
            | VortexLocalPrimitiveRowExportFormat::Avro
            | VortexLocalPrimitiveRowExportFormat::Vortex => {
                return Err(ShardLoomError::InvalidOperation(
                    "local Vortex sparse row text writer received a structured or native Vortex format; no fallback execution was attempted"
                        .to_string(),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(feature = "vortex-local-primitives")]
fn required_melt_projection(
    request: &VortexQueryPrimitiveRequest,
) -> Result<&VortexMeltProjectionRequest> {
    let Some(melt_projection) = request.melt_projection.as_ref() else {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex melt row export requires a typed melt projection payload; no fallback execution was attempted"
                .to_string(),
        ));
    };
    if melt_projection.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex melt row export requires at least one value column; no fallback execution was attempted"
                .to_string(),
        ));
    }
    if melt_projection.variable_column.trim().is_empty()
        || melt_projection.value_column.trim().is_empty()
    {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex melt row export requires non-empty variable and value output column names; no fallback execution was attempted"
                .to_string(),
        ));
    }
    if melt_projection.variable_column == melt_projection.value_column
        || melt_projection
            .id_columns
            .iter()
            .any(|column| column.as_str() == melt_projection.variable_column)
        || melt_projection
            .id_columns
            .iter()
            .any(|column| column.as_str() == melt_projection.value_column)
    {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex melt row export output column names must not collide with id columns; no fallback execution was attempted"
                .to_string(),
        ));
    }
    Ok(melt_projection)
}

#[cfg(feature = "vortex-local-primitives")]
fn checked_melt_expanded_rows(
    source_rows: usize,
    melt_projection: &VortexMeltProjectionRequest,
) -> Result<usize> {
    source_rows
        .checked_mul(melt_projection.value_columns.len())
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex melt row count overflowed usize; no fallback execution was attempted"
                    .to_string(),
            )
        })
}

#[cfg(feature = "vortex-local-primitives")]
fn validate_melt_value_dtype(
    column_values: &[Vec<StatValue>],
    melt_projection: &VortexMeltProjectionRequest,
    observed_dtype: &mut Option<LogicalDType>,
) -> Result<()> {
    let id_count = melt_projection.id_columns.len();
    let value_count = melt_projection.value_columns.len();
    if column_values.len() < id_count + value_count {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex melt row export scanned fewer columns than requested; no fallback execution was attempted"
                .to_string(),
        ));
    }
    for column_values in &column_values[id_count..id_count + value_count] {
        for value in column_values {
            let dtype = value.dtype();
            if observed_dtype.is_none() {
                *observed_dtype = Some(dtype);
            }
        }
    }
    Ok(())
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::needless_range_loop)]
fn write_row_export_melt_rows(
    output: &mut std::fs::File,
    format: VortexLocalPrimitiveRowExportFormat,
    melt_projection: &VortexMeltProjectionRequest,
    column_values: &[Vec<StatValue>],
    source_rows: usize,
    output_rows: usize,
) -> Result<usize> {
    use std::io::Write as _;

    let id_count = melt_projection.id_columns.len();
    let value_count = melt_projection.value_columns.len();
    if column_values.len() < id_count + value_count {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex melt row export scanned fewer columns than requested; no fallback execution was attempted"
                .to_string(),
        ));
    }
    let output_columns = melt_projection.output_columns();
    let mut written = 0usize;
    for row_index in 0..source_rows {
        for value_offset in 0..value_count {
            if written >= output_rows {
                return Ok(written);
            }
            let value_column = melt_projection.value_columns[value_offset].as_str();
            let value_index = id_count + value_offset;
            match format {
                VortexLocalPrimitiveRowExportFormat::Jsonl => {
                    let mut row = serde_json::Map::with_capacity(output_columns.len());
                    for (id_index, id_column) in melt_projection.id_columns.iter().enumerate() {
                        row.insert(
                            id_column.as_str().to_string(),
                            stat_value_to_json_value(&column_values[id_index][row_index])?,
                        );
                    }
                    row.insert(
                        melt_projection.variable_column.clone(),
                        serde_json::Value::String(value_column.to_string()),
                    );
                    row.insert(
                        melt_projection.value_column.clone(),
                        stat_value_to_json_value(&column_values[value_index][row_index])?,
                    );
                    let line = serde_json::Value::Object(row).to_string();
                    writeln!(output, "{line}").map_err(|error| {
                        ShardLoomError::InvalidOperation(format!(
                            "failed to write local Vortex melt JSONL row: {error}"
                        ))
                    })?;
                }
                VortexLocalPrimitiveRowExportFormat::Csv => {
                    let mut cells = Vec::with_capacity(output_columns.len());
                    for values in column_values.iter().take(id_count) {
                        cells.push(stat_value_to_csv_cell(&values[row_index]));
                    }
                    cells.push(csv_escape(value_column));
                    cells.push(stat_value_to_csv_cell(
                        &column_values[value_index][row_index],
                    ));
                    writeln!(output, "{}", cells.join(",")).map_err(|error| {
                        ShardLoomError::InvalidOperation(format!(
                            "failed to write local Vortex melt CSV row: {error}"
                        ))
                    })?;
                }
                VortexLocalPrimitiveRowExportFormat::Parquet
                | VortexLocalPrimitiveRowExportFormat::ArrowIpc
                | VortexLocalPrimitiveRowExportFormat::Avro
                | VortexLocalPrimitiveRowExportFormat::Vortex => {
                    return Err(ShardLoomError::InvalidOperation(
                        "local Vortex melt text writer received a structured or native Vortex format; no fallback execution was attempted"
                            .to_string(),
                    ));
                }
            }
            written = written.checked_add(1).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex melt row export output count overflowed usize; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
        }
    }
    Ok(written)
}

#[cfg(feature = "vortex-local-primitives")]
fn required_explode_projection(
    request: &VortexQueryPrimitiveRequest,
) -> Result<&VortexExplodeProjectionRequest> {
    request.explode_projection.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "local Vortex explode row export requires a typed explode projection payload; no fallback execution was attempted"
                .to_string(),
        )
    })
}

#[cfg(feature = "vortex-local-primitives")]
struct ExplodeChunkColumns {
    output_columns: Vec<String>,
    column_values: Vec<ExplodeOutputColumnValues>,
    row_lengths: Vec<usize>,
    source_rows: usize,
    expanded_rows: usize,
}

#[cfg(feature = "vortex-local-primitives")]
enum ExplodeOutputColumnValues {
    Scalar(Vec<ScalarValue>),
    Exploded(Vec<Vec<ScalarValue>>),
}

#[cfg(feature = "vortex-local-primitives")]
impl ExplodeOutputColumnValues {
    fn source_row_count(&self) -> usize {
        match self {
            Self::Scalar(values) => values.len(),
            Self::Exploded(rows) => rows.len(),
        }
    }

    fn value_at(&self, source_index: usize, element_index: usize) -> Result<&ScalarValue> {
        match self {
            Self::Scalar(values) => values.get(source_index).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex explode scalar companion row index was out of bounds; no fallback execution was attempted"
                        .to_string(),
                )
            }),
            Self::Exploded(rows) => rows
                .get(source_index)
                .and_then(|elements| elements.get(element_index))
                .ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex explode element row index was out of bounds; no fallback execution was attempted"
                            .to_string(),
                    )
                }),
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn explode_columns_from_chunk(
    chunk: &vortex::array::ArrayRef,
    declared_columns: &[String],
    explode_projection: &VortexExplodeProjectionRequest,
) -> Result<ExplodeChunkColumns> {
    let explode_column_names = explode_projection
        .explode_columns()
        .into_iter()
        .map(|column| column.as_str().to_string())
        .collect::<std::collections::BTreeSet<_>>();
    let output_columns = explode_projection.output_columns(declared_columns);
    for explode_column in &explode_column_names {
        if explode_projection.element_field.is_none()
            && !output_columns.iter().any(|column| column == explode_column)
        {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local Vortex explode output columns must include explode column '{explode_column}'; no fallback execution was attempted"
            )));
        }
    }

    let mut column_values = Vec::with_capacity(output_columns.len());
    let mut row_lengths: Option<Vec<usize>> = None;
    if chunk.dtype().is_struct() {
        let children = chunk
            .named_children()
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();
        if declared_columns.len() != output_columns.len() {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex explode scan and output column counts diverged; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        for (source_column, _output_column) in declared_columns.iter().zip(&output_columns) {
            let Some(array) = children.get(source_column.as_str()) else {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "local Vortex explode column '{source_column}' was not present in scanned chunk; no fallback execution was attempted"
                )));
            };
            if explode_column_names.contains(source_column) {
                let mut element_rows = list_element_rows_from_vortex_array(source_column, array)?;
                if let Some(field) = explode_projection.element_field.as_deref() {
                    element_rows =
                        project_explode_element_field(source_column, field, element_rows)?;
                }
                merge_explode_row_lengths(source_column, &element_rows, &mut row_lengths)?;
                column_values.push(ExplodeOutputColumnValues::Exploded(element_rows));
            } else {
                column_values.push(ExplodeOutputColumnValues::Scalar(
                    scalar_values_from_vortex_array(source_column, array)?,
                ));
            }
        }
    } else {
        if explode_column_names.len() != 1
            || output_columns.len() != 1
            || !explode_column_names.contains(&output_columns[0])
        {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex top-level explode requires a single projected list column; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        let explode_column = &output_columns[0];
        let element_rows = list_element_rows_from_vortex_array(explode_column, chunk)?;
        merge_explode_row_lengths(explode_column, &element_rows, &mut row_lengths)?;
        column_values.push(ExplodeOutputColumnValues::Exploded(element_rows));
    }
    let row_lengths = row_lengths.ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!(
            "local Vortex explode projection columns '{}' were not materialized; no fallback execution was attempted",
            explode_column_names.into_iter().collect::<Vec<_>>().join(",")
        ))
    })?;
    let source_rows = row_lengths.len();
    for (column, values) in output_columns.iter().zip(&column_values) {
        if values.source_row_count() != source_rows {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local Vortex explode column '{column}' row count did not match list row count; no fallback execution was attempted"
            )));
        }
    }
    let expanded_rows = row_lengths.iter().try_fold(0usize, |acc, values| {
        acc.checked_add(*values).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex explode expanded row count overflowed usize; no fallback execution was attempted"
                    .to_string(),
            )
        })
    })?;
    Ok(ExplodeChunkColumns {
        output_columns,
        column_values,
        row_lengths,
        source_rows,
        expanded_rows,
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn project_explode_element_field(
    column: &str,
    field: &str,
    element_rows: Vec<Vec<ScalarValue>>,
) -> Result<Vec<Vec<ScalarValue>>> {
    element_rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| match value {
                    ScalarValue::Struct(fields) => fields
                        .into_iter()
                        .find_map(|(candidate, value)| (candidate == field).then_some(value))
                        .ok_or_else(|| {
                            ShardLoomError::InvalidOperation(format!(
                                "local Vortex explode element field '{field}' was not present in struct elements for column '{column}'; no fallback execution was attempted"
                            ))
                        }),
                    ScalarValue::Null => Ok(ScalarValue::Null),
                    _ => Err(ShardLoomError::InvalidOperation(format!(
                        "local Vortex explode element field '{field}' requires struct elements for column '{column}'; no fallback execution was attempted"
                    ))),
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect()
}

#[cfg(feature = "vortex-local-primitives")]
fn merge_explode_row_lengths(
    column: &str,
    element_rows: &[Vec<ScalarValue>],
    row_lengths: &mut Option<Vec<usize>>,
) -> Result<()> {
    let lengths = element_rows.iter().map(Vec::len).collect::<Vec<_>>();
    if let Some(existing) = row_lengths {
        if existing.len() != lengths.len() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local Vortex multi-column explode column '{column}' row count did not match prior explode columns; no fallback execution was attempted"
            )));
        }
        for (row_index, (expected, observed)) in existing.iter().zip(&lengths).enumerate() {
            if expected != observed {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "local Vortex multi-column explode column '{column}' row {row_index} has {observed} elements but prior explode columns have {expected}; no fallback execution was attempted"
                )));
            }
        }
    } else {
        *row_lengths = Some(lengths);
    }
    Ok(())
}

#[cfg(feature = "vortex-local-primitives")]
fn list_element_rows_from_vortex_array(
    column: &str,
    array: &vortex::array::ArrayRef,
) -> Result<Vec<Vec<ScalarValue>>> {
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::fixed_size_list::FixedSizeListArrayExt as _;
    use vortex::array::arrays::listview::ListViewArrayExt as _;
    use vortex::array::arrays::{FixedSizeListArray, ListViewArray};
    use vortex::array::dtype::DType;

    match array.dtype() {
        DType::List(_, _) => {
            let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
            let list = array
                .clone()
                .execute::<ListViewArray>(&mut ctx)
                .map_err(vortex_error)?;
            let validity = list.listview_validity();
            let mut validity_ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
            let mut out = Vec::with_capacity(list.len());
            for row_index in 0..list.len() {
                if !validity
                    .execute_is_valid(row_index, &mut validity_ctx)
                    .map_err(vortex_error)?
                {
                    out.push(vec![ScalarValue::Null]);
                    continue;
                }
                let elements = list.list_elements_at(row_index).map_err(vortex_error)?;
                out.push(scalar_values_from_vortex_array(column, &elements)?);
            }
            Ok(out)
        }
        DType::FixedSizeList(_, _, _) => {
            let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
            let list = array
                .clone()
                .execute::<FixedSizeListArray>(&mut ctx)
                .map_err(vortex_error)?;
            let validity = list.fixed_size_list_validity();
            let mut validity_ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
            let mut out = Vec::with_capacity(list.len());
            for row_index in 0..list.len() {
                if !validity
                    .execute_is_valid(row_index, &mut validity_ctx)
                    .map_err(vortex_error)?
                {
                    out.push(vec![ScalarValue::Null]);
                    continue;
                }
                let elements = list
                    .fixed_size_list_elements_at(row_index)
                    .map_err(vortex_error)?;
                out.push(scalar_values_from_vortex_array(column, &elements)?);
            }
            Ok(out)
        }
        other => Err(ShardLoomError::InvalidOperation(format!(
            "local Vortex explode column '{column}' requires a list or fixed-size-list dtype, got {other:?}; no fallback execution was attempted"
        ))),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn scalar_values_from_vortex_array(
    column: &str,
    array: &vortex::array::ArrayRef,
) -> Result<Vec<ScalarValue>> {
    use vortex::array::dtype::DType;

    if let Some(values) = stat_values_from_vortex_array(array) {
        return Ok(values.iter().map(stat_value_to_scalar_value).collect());
    }
    match array.dtype() {
        DType::List(_, _) | DType::FixedSizeList(_, _, _) => {
            Ok(list_element_rows_from_vortex_array(column, array)?
                .into_iter()
                .map(ScalarValue::List)
                .collect())
        }
        DType::Struct(_, _) => struct_scalar_values_from_vortex_array(column, array),
        other => Err(ShardLoomError::InvalidOperation(format!(
            "local Vortex typed row value column '{column}' has unsupported dtype {other:?}; no fallback execution was attempted"
        ))),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn struct_scalar_values_from_vortex_array(
    column: &str,
    array: &vortex::array::ArrayRef,
) -> Result<Vec<ScalarValue>> {
    use vortex::array::VortexSessionExecute as _;

    let children = array
        .named_children()
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut field_values = Vec::with_capacity(children.len());
    for (field_name, field_array) in children {
        field_values.push((
            field_name.clone(),
            scalar_values_from_vortex_array(&field_name, &field_array)?,
        ));
    }
    let row_count = field_values
        .first()
        .map_or_else(|| array.len(), |(_field, values)| values.len());
    for (field_name, values) in &field_values {
        if values.len() != row_count {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local Vortex typed row value struct column '{column}' field '{field_name}' had mismatched row count; no fallback execution was attempted"
            )));
        }
    }
    let validity = array.validity().map_err(vortex_error)?;
    let mut validity_ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
    let mut out = Vec::with_capacity(row_count);
    for row_index in 0..row_count {
        if !validity
            .execute_is_valid(row_index, &mut validity_ctx)
            .map_err(vortex_error)?
        {
            out.push(ScalarValue::Null);
            continue;
        }
        let mut fields = Vec::with_capacity(field_values.len());
        for (field_name, values) in &field_values {
            let value = values.get(row_index).cloned().ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex typed struct value row index was out of bounds; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
            fields.push((field_name.clone(), value));
        }
        out.push(ScalarValue::Struct(fields));
    }
    Ok(out)
}

#[cfg(feature = "vortex-local-primitives")]
fn write_row_export_explode_rows(
    output: &mut std::fs::File,
    format: VortexLocalPrimitiveRowExportFormat,
    explode_columns: &ExplodeChunkColumns,
    output_rows: usize,
) -> Result<usize> {
    use std::io::Write as _;

    let mut written = 0usize;
    for source_index in 0..explode_columns.source_rows {
        for element_index in 0..explode_columns.row_lengths[source_index] {
            if written >= output_rows {
                return Ok(written);
            }
            match format {
                VortexLocalPrimitiveRowExportFormat::Jsonl => {
                    let mut row =
                        serde_json::Map::with_capacity(explode_columns.output_columns.len());
                    for (column, values) in explode_columns
                        .output_columns
                        .iter()
                        .zip(&explode_columns.column_values)
                    {
                        let value = values.value_at(source_index, element_index)?;
                        row.insert(column.clone(), scalar_value_to_json_value(value)?);
                    }
                    let line = serde_json::Value::Object(row).to_string();
                    writeln!(output, "{line}").map_err(|error| {
                        ShardLoomError::InvalidOperation(format!(
                            "failed to write local Vortex explode JSONL row: {error}"
                        ))
                    })?;
                }
                VortexLocalPrimitiveRowExportFormat::Csv => {
                    let mut cells = Vec::with_capacity(explode_columns.output_columns.len());
                    for values in &explode_columns.column_values {
                        let value = values.value_at(source_index, element_index)?;
                        cells.push(scalar_value_to_csv_cell(value)?);
                    }
                    writeln!(output, "{}", cells.join(",")).map_err(|error| {
                        ShardLoomError::InvalidOperation(format!(
                            "failed to write local Vortex explode CSV row: {error}"
                        ))
                    })?;
                }
                VortexLocalPrimitiveRowExportFormat::Parquet
                | VortexLocalPrimitiveRowExportFormat::ArrowIpc
                | VortexLocalPrimitiveRowExportFormat::Avro
                | VortexLocalPrimitiveRowExportFormat::Vortex => {
                    return Err(ShardLoomError::InvalidOperation(
                        "local Vortex explode text writer received a structured or native Vortex format; no fallback execution was attempted"
                            .to_string(),
                    ));
                }
            }
            written = written.checked_add(1).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex explode row export output count overflowed usize; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
        }
    }
    Ok(written)
}

#[cfg(feature = "vortex-local-primitives")]
fn required_rolling_window(
    request: &VortexQueryPrimitiveRequest,
) -> Result<&VortexRollingWindowRequest> {
    let Some(rolling_window) = request.rolling_window.as_ref() else {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex rolling window requires a typed rolling payload; no fallback execution was attempted"
                .to_string(),
        ));
    };
    validate_rolling_window_request(rolling_window)?;
    Ok(rolling_window)
}

#[cfg(feature = "vortex-local-primitives")]
fn validate_rolling_window_request(rolling_window: &VortexRollingWindowRequest) -> Result<()> {
    if rolling_window.window_size == 0 {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex rolling window size must be >= 1; no fallback execution was attempted"
                .to_string(),
        ));
    }
    if rolling_window.min_periods == 0 || rolling_window.min_periods > rolling_window.window_size {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex rolling min_periods must be in the range [1, window_size]; no fallback execution was attempted"
                .to_string(),
        ));
    }
    if rolling_window.output_column.trim().is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex rolling output column must be non-empty; no fallback execution was attempted"
                .to_string(),
        ));
    }
    if rolling_window.output_column == rolling_window.source_column.as_str() {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex rolling output column must not collide with the source column; no fallback execution was attempted"
                .to_string(),
        ));
    }
    if !matches!(
        rolling_window.aggregate.as_str(),
        "sum" | "mean" | "count" | "min" | "max"
    ) {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex rolling window supports aggregate='sum', 'mean', 'count', 'min', or 'max' in the scoped v1 route; no fallback execution was attempted"
                .to_string(),
        ));
    }
    Ok(())
}

#[cfg(feature = "vortex-local-primitives")]
struct RollingWindowState {
    values: std::collections::VecDeque<Option<f64>>,
    sum: f64,
    valid_count: usize,
    center_seen_rows: usize,
    center_next_output_row: usize,
    center_buffer_start_row: usize,
}

#[cfg(feature = "vortex-local-primitives")]
impl RollingWindowState {
    fn new(window_size: usize) -> Self {
        Self {
            values: std::collections::VecDeque::with_capacity(window_size),
            sum: 0.0,
            valid_count: 0,
            center_seen_rows: 0,
            center_next_output_row: 0,
            center_buffer_start_row: 0,
        }
    }

    fn push(&mut self, value: Option<f64>, window_size: usize) -> Result<()> {
        if self.values.len() == window_size
            && let Some(expired) = self.values.pop_front().flatten()
        {
            self.sum -= expired;
            self.valid_count = self.valid_count.saturating_sub(1);
        }
        if let Some(value) = value {
            self.sum += value;
            self.valid_count = self.valid_count.checked_add(1).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex rolling window valid count overflowed; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
            if !self.sum.is_finite() {
                return Err(ShardLoomError::InvalidOperation(
                    "local Vortex rolling window produced a non-finite sum; no fallback execution was attempted"
                        .to_string(),
                ));
            }
        }
        self.values.push_back(value);
        Ok(())
    }

    fn ready(&self, min_periods: usize) -> bool {
        self.valid_count >= min_periods
    }

    fn current_count(&self) -> usize {
        self.valid_count
    }

    fn input_rows_needed_for_outputs(&self, min_periods: usize, output_rows: usize) -> usize {
        if output_rows == 0 {
            return 0;
        }
        output_rows.saturating_add(
            min_periods
                .saturating_sub(self.values.len())
                .saturating_sub(1),
        )
    }

    fn push_centered(&mut self, value: Option<f64>) -> Result<()> {
        self.values.push_back(value);
        self.center_seen_rows = self.center_seen_rows.checked_add(1).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex centered rolling row ordinal overflowed; no fallback execution was attempted"
                    .to_string(),
            )
        })?;
        Ok(())
    }

    fn emit_ready_centered(
        &mut self,
        rolling_window: &VortexRollingWindowRequest,
        end_of_input: bool,
    ) -> Result<Vec<StatValue>> {
        let left_rows = rolling_center_left_rows(rolling_window.window_size);
        let right_rows = rolling_center_right_rows(rolling_window.window_size);
        let mut output = Vec::new();
        while self.center_next_output_row < self.center_seen_rows {
            let right_boundary = self.center_next_output_row.saturating_add(right_rows);
            if right_boundary >= self.center_seen_rows && !end_of_input {
                break;
            }
            let start = self.center_next_output_row.saturating_sub(left_rows);
            let end = right_boundary.min(self.center_seen_rows.saturating_sub(1));
            if start < self.center_buffer_start_row {
                return Err(ShardLoomError::InvalidOperation(
                    "local Vortex centered rolling buffer dropped a required row; no fallback execution was attempted"
                        .to_string(),
                ));
            }
            let start_offset = start - self.center_buffer_start_row;
            let end_offset = end - self.center_buffer_start_row;
            if let Some(value) = rolling_aggregate_from_window(
                &self.values,
                start_offset,
                end_offset,
                rolling_window,
            )? {
                output.push(value);
            }
            self.center_next_output_row =
                self.center_next_output_row.checked_add(1).ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex centered rolling output ordinal overflowed; no fallback execution was attempted"
                            .to_string(),
                    )
                })?;
            self.trim_centered_buffer(left_rows);
        }
        Ok(output)
    }

    fn trim_centered_buffer(&mut self, left_rows: usize) {
        let earliest_needed = self.center_next_output_row.saturating_sub(left_rows);
        while self.center_buffer_start_row < earliest_needed {
            if self.values.pop_front().is_none() {
                break;
            }
            self.center_buffer_start_row = self.center_buffer_start_row.saturating_add(1);
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::cast_precision_loss)]
fn rolling_window_values(
    column_values: &[Vec<StatValue>],
    rolling_window: &VortexRollingWindowRequest,
    state: &mut RollingWindowState,
    source_rows: usize,
    end_of_input: bool,
) -> Result<Vec<StatValue>> {
    if column_values.len() != 1 {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex rolling window requires exactly one projected source column; no fallback execution was attempted"
                .to_string(),
        ));
    }
    let values = &column_values[0];
    if values.len() < source_rows {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex rolling window scanned fewer values than rows; no fallback execution was attempted"
                .to_string(),
        ));
    }
    let mut output = Vec::new();
    for value in values.iter().take(source_rows) {
        let rolling_value = rolling_input_value(value, rolling_window.aggregate.as_str())?;
        if rolling_window.center {
            state.push_centered(rolling_value)?;
            continue;
        }
        state.push(rolling_value, rolling_window.window_size)?;
        if state.ready(rolling_window.min_periods) {
            let value = match rolling_window.aggregate.as_str() {
                "sum" => StatValue::Float64(state.sum),
                "mean" => {
                    let count = state.current_count();
                    if count == 0 {
                        return Err(ShardLoomError::InvalidOperation(
                            "local Vortex rolling mean had zero rows in state; no fallback execution was attempted"
                                .to_string(),
                        ));
                    }
                    let mean = state.sum / count as f64;
                    if !mean.is_finite() {
                        return Err(ShardLoomError::InvalidOperation(
                            "local Vortex rolling mean produced a non-finite value; no fallback execution was attempted"
                                .to_string(),
                        ));
                    }
                    StatValue::Float64(mean)
                }
                "count" => StatValue::UInt64(usize_to_u64(state.current_count())?),
                "min" => {
                    let value =
                        state
                            .values
                            .iter()
                            .flatten()
                            .copied()
                            .reduce(f64::min)
                            .ok_or_else(|| {
                                ShardLoomError::InvalidOperation(
                                    "local Vortex rolling min had zero rows in state; no fallback execution was attempted"
                                        .to_string(),
                                )
                            })?;
                    StatValue::Float64(value)
                }
                "max" => {
                    let value =
                        state
                            .values
                            .iter()
                            .flatten()
                            .copied()
                            .reduce(f64::max)
                            .ok_or_else(|| {
                                ShardLoomError::InvalidOperation(
                                    "local Vortex rolling max had zero rows in state; no fallback execution was attempted"
                                        .to_string(),
                                )
                            })?;
                    StatValue::Float64(value)
                }
                _ => {
                    return Err(ShardLoomError::InvalidOperation(
                        "local Vortex rolling window aggregate was not admitted; no fallback execution was attempted"
                            .to_string(),
                    ));
                }
            };
            output.push(value);
        }
    }
    if rolling_window.center {
        return state.emit_ready_centered(rolling_window, end_of_input);
    }
    Ok(output)
}

#[cfg(feature = "vortex-local-primitives")]
fn rolling_input_value(value: &StatValue, aggregate: &str) -> Result<Option<f64>> {
    match value {
        StatValue::Null => Ok(None),
        _ if aggregate == "count" => Ok(Some(0.0)),
        _ => stat_value_to_f64(value).map(Some),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn rolling_center_left_rows(window_size: usize) -> usize {
    window_size.saturating_sub(1) / 2
}

#[cfg(feature = "vortex-local-primitives")]
fn rolling_center_right_rows(window_size: usize) -> usize {
    window_size.saturating_sub(1) - rolling_center_left_rows(window_size)
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::cast_precision_loss)]
fn rolling_aggregate_from_window(
    values: &std::collections::VecDeque<Option<f64>>,
    start_offset: usize,
    end_offset: usize,
    rolling_window: &VortexRollingWindowRequest,
) -> Result<Option<StatValue>> {
    let mut sum = 0.0;
    let mut valid_count = 0usize;
    let mut min_value: Option<f64> = None;
    let mut max_value: Option<f64> = None;
    for offset in start_offset..=end_offset {
        let value = values.get(offset).copied().flatten();
        if let Some(value) = value {
            sum += value;
            valid_count = valid_count.checked_add(1).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex centered rolling valid count overflowed; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
            min_value = Some(min_value.map_or(value, |current| current.min(value)));
            max_value = Some(max_value.map_or(value, |current| current.max(value)));
        }
    }
    if valid_count < rolling_window.min_periods {
        return Ok(None);
    }
    let value = match rolling_window.aggregate.as_str() {
        "sum" => StatValue::Float64(sum),
        "mean" => {
            let mean = sum / valid_count as f64;
            if !mean.is_finite() {
                return Err(ShardLoomError::InvalidOperation(
                    "local Vortex centered rolling mean produced a non-finite value; no fallback execution was attempted"
                        .to_string(),
                ));
            }
            StatValue::Float64(mean)
        }
        "count" => StatValue::UInt64(usize_to_u64(valid_count)?),
        "min" => StatValue::Float64(min_value.ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex centered rolling min had zero rows in state; no fallback execution was attempted"
                    .to_string(),
            )
        })?),
        "max" => StatValue::Float64(max_value.ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex centered rolling max had zero rows in state; no fallback execution was attempted"
                    .to_string(),
            )
        })?),
        _ => {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex centered rolling aggregate was not admitted; no fallback execution was attempted"
                    .to_string(),
            ));
        }
    };
    Ok(Some(value))
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
fn row_export_materialized_projected_row(
    declared_columns: &[String],
    column_values: &[Vec<StatValue>],
    output_columns: &[String],
    row_index: usize,
) -> Result<Vec<StatValue>> {
    output_columns
        .iter()
        .map(|column| {
            let source_index = column_index(declared_columns, column)?;
            column_values[source_index]
                .get(row_index)
                .cloned()
                .ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex row export materialized projected row index was out of bounds; no fallback execution was attempted"
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
    if limit == 0 {
        return;
    }
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
fn insert_weighted_sample_row_export_candidate(
    selected: &mut Vec<(f64, usize, Vec<StatValue>)>,
    limit: usize,
    score: f64,
    row_index: usize,
    row: Vec<StatValue>,
) {
    if limit == 0 {
        return;
    }
    if selected.len() < limit {
        selected.push((score, row_index, row));
        return;
    }
    let Some((replace_index, lowest_score)) = selected
        .iter()
        .enumerate()
        .min_by(
            |(_left_index, (left_score, _left_row_index, _left_row)),
             (_right_index, (right_score, _right_row_index, _right_row))| {
                left_score
                    .partial_cmp(right_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            },
        )
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
        if request.sample_with_replacement {
            return Ok(if row_count == 0 { 0 } else { limit });
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
fn truncate_weighted_sample_candidates_to_target(
    sample_rows: &mut Vec<(f64, usize, Vec<StatValue>)>,
    target_count: usize,
) {
    sample_rows.sort_by(
        |(left_score, _left_row_index, _left_row), (right_score, _right_row_index, _right_row)| {
            right_score
                .partial_cmp(left_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        },
    );
    sample_rows.truncate(target_count);
    sample_rows.sort_by_key(|(_score, row_index, _row)| *row_index);
}

#[cfg(feature = "vortex-local-primitives")]
fn sample_weight_value(value: &StatValue) -> Result<f64> {
    let weight = match value {
        StatValue::Float64(value) => *value,
        StatValue::Int64(value) => int64_stat_to_float64(*value),
        StatValue::UInt64(value) => uint64_stat_to_float64(*value),
        StatValue::Null | StatValue::Boolean(_) | StatValue::Utf8(_) => {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex weighted sample requires a numeric weight column; no fallback execution was attempted"
                    .to_string(),
            ));
        }
    };
    if !weight.is_finite() || weight <= 0.0 {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex weighted sample requires positive finite weights; no fallback execution was attempted"
                .to_string(),
        ));
    }
    Ok(weight)
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::cast_precision_loss)]
fn int64_stat_to_float64(value: i64) -> f64 {
    value as f64
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::cast_precision_loss)]
fn uint64_stat_to_float64(value: u64) -> f64 {
    value as f64
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
    let replacement = if request.sample_with_replacement {
        " sample_replacement=true"
    } else {
        ""
    };
    let weight = request
        .sample_weight_column
        .as_ref()
        .map_or_else(String::new, |column| {
            format!(" sample_weight_column={}", column.as_str())
        });
    if let Some(fraction) = request.sample_fraction {
        format!(
            "sample_fraction={}{}{}",
            format_sample_fraction(fraction),
            replacement,
            weight
        )
    } else {
        format!(
            "sample_size={}{}{}",
            request
                .source_order_limit
                .map_or_else(|| "none".to_string(), |limit| limit.to_string()),
            replacement,
            weight
        )
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn deterministic_sample_replacement_index(
    seed: u64,
    draw_index: usize,
    population_size: usize,
) -> usize {
    if population_size == 0 {
        return 0;
    }
    usize::try_from(deterministic_sample_score(seed, draw_index)).unwrap_or(0) % population_size
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::cast_precision_loss)]
fn deterministic_sample_unit(seed: u64, row_index: usize) -> f64 {
    let numerator = deterministic_sample_score(seed, row_index) as f64 + 0.5;
    let denominator = u64::MAX as f64 + 1.0;
    (numerator / denominator).clamp(f64::MIN_POSITIVE, 1.0 - f64::EPSILON)
}

#[cfg(feature = "vortex-local-primitives")]
fn deterministic_weighted_sample_score(seed: u64, row_index: usize, weight: f64) -> f64 {
    deterministic_sample_unit(seed, row_index).ln() / weight
}

#[cfg(feature = "vortex-local-primitives")]
fn deterministic_weighted_sample_replacement_rows(
    seed: u64,
    target_count: usize,
    population: &[(Vec<StatValue>, f64)],
) -> Result<Vec<Vec<StatValue>>> {
    if population.is_empty() || target_count == 0 {
        return Ok(Vec::new());
    }
    let total_weight = population.iter().map(|(_row, weight)| weight).sum::<f64>();
    if !total_weight.is_finite() || total_weight <= 0.0 {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex weighted replacement sample total weight was not positive and finite; no fallback execution was attempted"
                .to_string(),
        ));
    }
    let mut selected = Vec::with_capacity(target_count);
    for draw_index in 0..target_count {
        let threshold =
            deterministic_sample_unit(seed ^ 0xa076_1d64_78bd_642f, draw_index) * total_weight;
        let mut cumulative = 0.0_f64;
        let mut fallback_row = None;
        for (row, weight) in population {
            cumulative += *weight;
            fallback_row = Some(row);
            if threshold <= cumulative {
                selected.push(row.clone());
                fallback_row = None;
                break;
            }
        }
        if let Some(row) = fallback_row {
            selected.push(row.clone());
        }
    }
    Ok(selected)
}

#[cfg(feature = "vortex-local-primitives")]
fn validate_expression_projection_columns(
    expression_projection: &VortexExpressionProjectionRequest,
    columns: &[String],
) -> Result<()> {
    let column_set = columns
        .iter()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    for rewrite in &expression_projection.rewrites {
        let target = rewrite.target_column().as_str();
        if !matches!(rewrite, VortexExpressionRewrite::RowNumber { .. })
            && !column_set.contains(target)
        {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local Vortex expression projection target column '{target}' is not in the projected output; include it in the projection or use a narrower rewrite"
            )));
        }
        if let VortexExpressionRewrite::MaskScalar { predicate, .. } = rewrite {
            validate_expression_projection_predicate_columns(predicate, &column_set)?;
        }
    }
    Ok(())
}

#[cfg(feature = "vortex-local-primitives")]
fn expression_projection_output_columns(
    source_columns: &[String],
    expression_projection: &VortexExpressionProjectionRequest,
) -> Vec<String> {
    let mut columns = source_columns.to_vec();
    for rewrite in &expression_projection.rewrites {
        if let VortexExpressionRewrite::RowNumber { target_column, .. } = rewrite {
            let target = target_column.as_str();
            if !columns.iter().any(|column| column == target) {
                columns.push(target.to_string());
            }
        }
    }
    columns
}

#[cfg(feature = "vortex-local-primitives")]
fn validate_expression_projection_predicate_columns(
    predicate: &PredicateExpr,
    columns: &std::collections::BTreeSet<&str>,
) -> Result<()> {
    let predicate_column = match predicate {
        PredicateExpr::AlwaysTrue | PredicateExpr::AlwaysFalse => return Ok(()),
        PredicateExpr::And(predicates) => {
            for predicate in predicates {
                validate_expression_projection_predicate_columns(predicate, columns)?;
            }
            return Ok(());
        }
        PredicateExpr::IsNull { column }
        | PredicateExpr::IsNotNull { column }
        | PredicateExpr::Compare { column, .. }
        | PredicateExpr::StringContains { column, .. }
        | PredicateExpr::InList { column, .. } => column.as_str(),
    };
    if !columns.contains(predicate_column) {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local Vortex expression projection predicate column '{predicate_column}' is not in the projected output; hidden predicate-only columns are not admitted for this scoped route"
        )));
    }
    Ok(())
}

#[cfg(feature = "vortex-local-primitives")]
fn apply_expression_projection_columns(
    columns: &mut Vec<String>,
    column_values: &mut Vec<Vec<StatValue>>,
    expression_projection: &VortexExpressionProjectionRequest,
    row_number_offset: u64,
    state: &mut ExpressionProjectionState,
) -> Result<()> {
    let row_count = row_export_materialized_row_count(column_values, 0)?;
    for rewrite in &expression_projection.rewrites {
        let target_index =
            expression_projection_target_index(columns, column_values, rewrite, row_count)?;
        let regex_replace = expression_projection_regex_replacement(rewrite)?;
        for row_index in 0..row_count {
            let current =
                expression_projection_current_value(column_values, target_index, row_index)?;
            let context = ExpressionProjectionRewriteContext {
                columns,
                column_values,
                row_index,
                row_number_offset,
                regex_replace: regex_replace.as_ref(),
            };
            let updated = apply_expression_projection_rewrite(rewrite, current, &context, state)?;
            column_values[target_index][row_index] = updated;
        }
    }
    Ok(())
}

#[cfg(feature = "vortex-local-primitives")]
fn expression_projection_target_index(
    columns: &mut Vec<String>,
    column_values: &mut Vec<Vec<StatValue>>,
    rewrite: &VortexExpressionRewrite,
    row_count: usize,
) -> Result<usize> {
    let target = rewrite.target_column().as_str();
    if let Some(index) = columns.iter().position(|column| column == target) {
        return Ok(index);
    }
    if matches!(rewrite, VortexExpressionRewrite::RowNumber { .. }) {
        columns.push(target.to_string());
        column_values.push(vec![StatValue::Null; row_count]);
        return Ok(columns.len() - 1);
    }
    Err(ShardLoomError::InvalidOperation(format!(
        "local Vortex expression projection target column '{target}' was not found; no fallback execution was attempted"
    )))
}

#[cfg(feature = "vortex-local-primitives")]
fn expression_projection_regex_replacement(
    rewrite: &VortexExpressionRewrite,
) -> Result<Option<(Regex, String)>> {
    match rewrite {
        VortexExpressionRewrite::RegexReplaceScalar {
            pattern,
            replacement,
            ..
        } => {
            let regex = Regex::new(pattern).map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "local Vortex expression projection regex replacement pattern was invalid: {error}; no fallback execution was attempted"
                ))
            })?;
            Ok(Some((
                regex,
                python_regex_replacement_for_rust_regex(replacement),
            )))
        }
        _ => Ok(None),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn expression_projection_current_value(
    column_values: &[Vec<StatValue>],
    target_index: usize,
    row_index: usize,
) -> Result<StatValue> {
    column_values[target_index]
        .get(row_index)
        .cloned()
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex expression projection row index was out of bounds; no fallback execution was attempted"
                    .to_string(),
            )
        })
}

#[cfg(feature = "vortex-local-primitives")]
struct ExpressionProjectionRewriteContext<'a> {
    columns: &'a [String],
    column_values: &'a [Vec<StatValue>],
    row_index: usize,
    row_number_offset: u64,
    regex_replace: Option<&'a (Regex, String)>,
}

#[cfg(feature = "vortex-local-primitives")]
fn apply_expression_projection_rewrite(
    rewrite: &VortexExpressionRewrite,
    current: StatValue,
    context: &ExpressionProjectionRewriteContext<'_>,
    state: &mut ExpressionProjectionState,
) -> Result<StatValue> {
    match rewrite {
        VortexExpressionRewrite::MaskScalar {
            predicate,
            replacement,
            ..
        } => {
            if predicate_matches_materialized_row(
                predicate,
                context.columns,
                context.column_values,
                context.row_index,
            )? {
                coerce_rewrite_value(&current, replacement)
            } else {
                Ok(current)
            }
        }
        VortexExpressionRewrite::ReplaceScalar {
            to_replace,
            replacement,
            ..
        } => {
            let comparable = coerce_rewrite_value(&current, to_replace)?;
            if stat_value_equal(&current, &comparable) {
                coerce_rewrite_value(&current, replacement)
            } else {
                Ok(current)
            }
        }
        VortexExpressionRewrite::StringReplaceScalar {
            needle,
            replacement,
            ..
        } => match current {
            StatValue::Utf8(value) => Ok(StatValue::Utf8(value.replace(needle, replacement))),
            _ => Err(ShardLoomError::InvalidOperation(
                "local Vortex expression projection string replacement requires a UTF-8 target column; no fallback execution was attempted"
                    .to_string(),
            )),
        },
        VortexExpressionRewrite::RegexReplaceScalar { .. } => {
            apply_regex_expression_projection_rewrite(current, context.regex_replace)
        }
        VortexExpressionRewrite::NumericScalarArithmetic {
            operator, operand, ..
        } => apply_numeric_scalar_arithmetic(&current, operator, operand),
        VortexExpressionRewrite::ForwardFillNull { limit, .. } => state.apply_forward_fill(
            rewrite.target_column().as_str(),
            current,
            *limit,
        ),
        VortexExpressionRewrite::RowNumber { start, .. } => {
            let row_ordinal = context
                .row_number_offset
                .checked_add(usize_to_u64(context.row_index)?)
                .and_then(|value| value.checked_add(*start))
                .ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex expression projection row-number overflowed; no fallback execution was attempted"
                            .to_string(),
                    )
                })?;
            Ok(StatValue::UInt64(row_ordinal))
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn apply_regex_expression_projection_rewrite(
    current: StatValue,
    regex_replace: Option<&(Regex, String)>,
) -> Result<StatValue> {
    match current {
        StatValue::Utf8(value) => {
            let (regex, replacement) = regex_replace.ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex expression projection regex replacement was not prepared; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
            Ok(StatValue::Utf8(
                regex
                    .replace_all(value.as_str(), replacement.as_str())
                    .into_owned(),
            ))
        }
        _ => Err(ShardLoomError::InvalidOperation(
            "local Vortex expression projection regex replacement requires a UTF-8 target column; no fallback execution was attempted"
                .to_string(),
        )),
    }
}

#[cfg(feature = "vortex-local-primitives")]
#[derive(Default)]
struct ExpressionProjectionState {
    forward_fill: std::collections::BTreeMap<String, ForwardFillColumnState>,
}

#[cfg(feature = "vortex-local-primitives")]
impl ExpressionProjectionState {
    fn apply_forward_fill(
        &mut self,
        target_column: &str,
        current: StatValue,
        limit: Option<usize>,
    ) -> Result<StatValue> {
        let state = self
            .forward_fill
            .entry(target_column.to_string())
            .or_default();
        match current {
            StatValue::Null => {
                if let Some(value) = state.last_value.clone()
                    && limit.is_none_or(|limit| state.consecutive_filled < limit)
                {
                    state.consecutive_filled =
                        state.consecutive_filled.checked_add(1).ok_or_else(|| {
                            ShardLoomError::InvalidOperation(
                                "local Vortex forward-fill null run length overflowed; no fallback execution was attempted"
                                    .to_string(),
                            )
                        })?;
                    Ok(value)
                } else {
                    Ok(StatValue::Null)
                }
            }
            value => {
                state.last_value = Some(value.clone());
                state.consecutive_filled = 0;
                Ok(value)
            }
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn python_regex_replacement_for_rust_regex(replacement: &str) -> String {
    let mut translated = String::with_capacity(replacement.len());
    let mut chars = replacement.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '$' {
            translated.push_str("$$");
            continue;
        }
        if ch != '\\' {
            translated.push(ch);
            continue;
        }
        let Some(next) = chars.next() else {
            translated.push('\\');
            break;
        };
        if next.is_ascii_digit() && next != '0' {
            let mut group = String::from(next);
            while chars.peek().is_some_and(char::is_ascii_digit) {
                group.push(chars.next().expect("peeked digit"));
            }
            translated.push_str("${");
            translated.push_str(&group);
            translated.push('}');
            continue;
        }
        if next == 'g' && chars.peek() == Some(&'<') {
            let _ = chars.next();
            let mut name = String::new();
            let mut closed = false;
            for part in chars.by_ref() {
                if part == '>' {
                    closed = true;
                    break;
                }
                name.push(part);
            }
            if closed && !name.is_empty() {
                translated.push_str("${");
                translated.push_str(&name);
                translated.push('}');
            } else {
                translated.push_str("\\g<");
                translated.push_str(&name);
            }
            continue;
        }
        translated.push('\\');
        translated.push(next);
    }
    translated
}

#[cfg(feature = "vortex-local-primitives")]
#[derive(Default)]
struct ForwardFillColumnState {
    last_value: Option<StatValue>,
    consecutive_filled: usize,
}

#[cfg(feature = "vortex-local-primitives")]
fn apply_numeric_scalar_arithmetic(
    current: &StatValue,
    operator: &str,
    operand: &StatValue,
) -> Result<StatValue> {
    let operator = operator.trim();
    let operand = coerce_rewrite_value(current, operand)?;
    match (current, &operand) {
        (StatValue::Int64(left), StatValue::Int64(right)) => match operator {
            "+" => left.checked_add(*right).map(StatValue::Int64),
            "-" => left.checked_sub(*right).map(StatValue::Int64),
            "*" => left.checked_mul(*right).map(StatValue::Int64),
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "local Vortex expression projection numeric scalar operator '{operator}' is not supported for int64 targets; no fallback execution was attempted"
                )));
            }
        }
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex expression projection int64 arithmetic overflowed; no fallback execution was attempted"
                    .to_string(),
            )
        }),
        (StatValue::UInt64(left), StatValue::UInt64(right)) => match operator {
            "+" => left.checked_add(*right).map(StatValue::UInt64),
            "-" => left.checked_sub(*right).map(StatValue::UInt64),
            "*" => left.checked_mul(*right).map(StatValue::UInt64),
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "local Vortex expression projection numeric scalar operator '{operator}' is not supported for uint64 targets; no fallback execution was attempted"
                )));
            }
        }
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex expression projection uint64 arithmetic overflowed or underflowed; no fallback execution was attempted"
                    .to_string(),
            )
        }),
        (StatValue::Float64(left), StatValue::Float64(right)) => {
            let value = match operator {
                "+" => left + right,
                "-" => left - right,
                "*" => left * right,
                "/" => {
                    if *right == 0.0 {
                        return Err(ShardLoomError::InvalidOperation(
                            "local Vortex expression projection float division by zero is not admitted; no fallback execution was attempted"
                                .to_string(),
                        ));
                    }
                    left / right
                }
                _ => {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "local Vortex expression projection numeric scalar operator '{operator}' is not supported; no fallback execution was attempted"
                    )));
                }
            };
            if value.is_finite() {
                Ok(StatValue::Float64(value))
            } else {
                Err(ShardLoomError::InvalidOperation(
                    "local Vortex expression projection float arithmetic produced a non-finite value; no fallback execution was attempted"
                        .to_string(),
                ))
            }
        }
        _ => Err(ShardLoomError::InvalidOperation(
            "local Vortex expression projection numeric scalar arithmetic requires an int64, uint64, or float64 target column; no fallback execution was attempted"
                .to_string(),
        )),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn column_index(columns: &[String], column: &str) -> Result<usize> {
    columns.iter().position(|value| value == column).ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!(
            "local Vortex expression projection column '{column}' was not found; no fallback execution was attempted"
        ))
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn predicate_matches_materialized_row(
    predicate: &PredicateExpr,
    columns: &[String],
    column_values: &[Vec<StatValue>],
    row_index: usize,
) -> Result<bool> {
    match predicate {
        PredicateExpr::AlwaysTrue | PredicateExpr::IsNotNull { .. } => Ok(true),
        PredicateExpr::AlwaysFalse | PredicateExpr::IsNull { .. } => Ok(false),
        PredicateExpr::And(predicates) => {
            predicates.iter().try_fold(true, |selected, predicate| {
                Ok(selected
                    && predicate_matches_materialized_row(
                        predicate,
                        columns,
                        column_values,
                        row_index,
                    )?)
            })
        }
        PredicateExpr::Compare { column, op, value } => {
            let column_index = column_index(columns, column.as_str())?;
            let current = column_values[column_index]
                .get(row_index)
                .ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex expression projection predicate row index was out of bounds; no fallback execution was attempted"
                            .to_string(),
                    )
                })?;
            compare_stat_value_with_op(current, *op, value)
        }
        PredicateExpr::StringContains {
            column,
            needle,
            negated,
        } => {
            let column_index = column_index(columns, column.as_str())?;
            let current = column_values[column_index].get(row_index).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex materialized predicate row index was out of bounds; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
            let StatValue::Utf8(value) = current else {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "local Vortex contains predicate requires UTF-8 column '{}'; no fallback execution was attempted",
                    column.as_str()
                )));
            };
            let matched = value.contains(needle);
            Ok(if *negated { !matched } else { matched })
        }
        PredicateExpr::InList {
            column,
            values,
            negated,
        } => {
            let column_index = column_index(columns, column.as_str())?;
            let current = column_values[column_index].get(row_index).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex materialized IN predicate row index was out of bounds; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
            let matched = values.iter().any(|value| {
                coerce_rewrite_value(current, value)
                    .ok()
                    .is_some_and(|value| stat_value_equal(current, &value))
            });
            Ok(if *negated { !matched } else { matched })
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn compare_stat_value_with_op(
    left: &StatValue,
    op: ComparisonOp,
    right: &StatValue,
) -> Result<bool> {
    let comparable = coerce_rewrite_value(left, right)?;
    let ordering = stat_value_cmp(left, &comparable).ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "local Vortex expression projection predicate has incompatible scalar types; no fallback execution was attempted"
                .to_string(),
        )
    })?;
    Ok(match op {
        ComparisonOp::Eq => ordering == std::cmp::Ordering::Equal,
        ComparisonOp::NotEq => ordering != std::cmp::Ordering::Equal,
        ComparisonOp::Lt => ordering == std::cmp::Ordering::Less,
        ComparisonOp::LtEq => {
            matches!(
                ordering,
                std::cmp::Ordering::Less | std::cmp::Ordering::Equal
            )
        }
        ComparisonOp::Gt => ordering == std::cmp::Ordering::Greater,
        ComparisonOp::GtEq => {
            matches!(
                ordering,
                std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
            )
        }
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn stat_value_cmp(left: &StatValue, right: &StatValue) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (StatValue::Null, StatValue::Null) => Some(std::cmp::Ordering::Equal),
        (StatValue::Boolean(left), StatValue::Boolean(right)) => Some(left.cmp(right)),
        (StatValue::Int64(left), StatValue::Int64(right)) => Some(left.cmp(right)),
        (StatValue::UInt64(left), StatValue::UInt64(right)) => Some(left.cmp(right)),
        (StatValue::Float64(left), StatValue::Float64(right)) => left.partial_cmp(right),
        (StatValue::Utf8(left), StatValue::Utf8(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn stat_value_equal(left: &StatValue, right: &StatValue) -> bool {
    stat_value_cmp(left, right) == Some(std::cmp::Ordering::Equal)
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::cast_precision_loss)]
fn coerce_rewrite_value(target_value: &StatValue, value: &StatValue) -> Result<StatValue> {
    match (target_value, value) {
        (_, StatValue::Null) => Ok(StatValue::Null),
        (StatValue::Boolean(_), StatValue::Boolean(value)) => Ok(StatValue::Boolean(*value)),
        (StatValue::Int64(_), StatValue::Int64(value)) => Ok(StatValue::Int64(*value)),
        (StatValue::Int64(_), StatValue::UInt64(value)) => {
            let value = i64::try_from(*value).map_err(|_| {
                ShardLoomError::InvalidOperation(
                    "local Vortex expression projection unsigned literal exceeds int64 target; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
            Ok(StatValue::Int64(value))
        }
        (StatValue::UInt64(_), StatValue::UInt64(value)) => Ok(StatValue::UInt64(*value)),
        (StatValue::UInt64(_), StatValue::Int64(value)) => {
            let value = u64::try_from(*value).map_err(|_| {
                ShardLoomError::InvalidOperation(
                    "local Vortex expression projection negative literal cannot target unsigned column; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
            Ok(StatValue::UInt64(value))
        }
        (StatValue::Float64(_), StatValue::Float64(value)) => Ok(StatValue::Float64(*value)),
        (StatValue::Float64(_), StatValue::Int64(value)) => Ok(StatValue::Float64(*value as f64)),
        (StatValue::Float64(_), StatValue::UInt64(value)) => Ok(StatValue::Float64(*value as f64)),
        (StatValue::Utf8(_), StatValue::Utf8(value)) => Ok(StatValue::Utf8(value.clone())),
        _ => Err(ShardLoomError::InvalidOperation(
            "local Vortex expression projection literal type is incompatible with target column; no fallback execution was attempted"
                .to_string(),
        )),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn distinct_row_indices(
    row_key_columns: &[Vec<String>],
    seen: &mut std::collections::BTreeSet<String>,
    remaining_limit: Option<usize>,
) -> Result<Vec<usize>> {
    let Some(row_count) = row_key_columns.first().map(Vec::len) else {
        return Ok(Vec::new());
    };
    if remaining_limit == Some(0) {
        return Ok(Vec::new());
    }
    let mut selected = Vec::new();
    for row_index in 0..row_count {
        let key = row_key_from_key_columns(row_key_columns, row_index)?;
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
fn duplicate_mask_values(
    row_key_columns: &[Vec<String>],
    seen: &mut std::collections::BTreeSet<String>,
    rows: usize,
) -> Result<Vec<StatValue>> {
    let available_rows = row_key_materialized_row_count(row_key_columns)?;
    if rows > available_rows {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex duplicate-mask requested more rows than the materialized chunk contains; no fallback execution was attempted"
                .to_string(),
        ));
    }
    let mut out = Vec::with_capacity(rows);
    for row_index in 0..rows {
        let key = row_key_from_key_columns(row_key_columns, row_index)?;
        out.push(StatValue::Boolean(!seen.insert(key)));
    }
    Ok(out)
}

#[cfg(feature = "vortex-local-primitives")]
fn row_key_materialized_row_count(row_key_columns: &[Vec<String>]) -> Result<usize> {
    let Some(row_count) = row_key_columns.first().map(Vec::len) else {
        return Ok(0);
    };
    if row_key_columns
        .iter()
        .any(|values| values.len() != row_count)
    {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex row-key columns had mismatched row counts; no fallback execution was attempted"
                .to_string(),
        ));
    }
    Ok(row_count)
}

#[cfg(feature = "vortex-local-primitives")]
fn duplicate_mask_values_from_policy(
    output_keys: &[(usize, String)],
    counts: &std::collections::BTreeMap<String, usize>,
    last_positions: &std::collections::BTreeMap<String, usize>,
    keep: VortexDuplicateKeepPolicy,
) -> Result<Vec<StatValue>> {
    output_keys
        .iter()
        .map(|(position, key)| {
            let duplicated = match keep {
                VortexDuplicateKeepPolicy::First => false,
                VortexDuplicateKeepPolicy::Last => {
                    last_positions.get(key).copied().ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "local Vortex duplicate-mask missing last-position state; no fallback execution was attempted"
                                .to_string(),
                        )
                    })? != *position
                }
                VortexDuplicateKeepPolicy::AllDuplicates => {
                    counts.get(key).copied().ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "local Vortex duplicate-mask missing count state; no fallback execution was attempted"
                                .to_string(),
                        )
                    })? > 1
                }
            };
            Ok(StatValue::Boolean(duplicated))
        })
        .collect()
}

#[cfg(feature = "vortex-local-primitives")]
fn retain_drop_duplicate_rows_from_policy(
    rows: &[(usize, String, Vec<StatValue>)],
    counts: &std::collections::BTreeMap<String, usize>,
    first_positions: &std::collections::BTreeMap<String, usize>,
    last_positions: &std::collections::BTreeMap<String, usize>,
    keep: VortexDuplicateKeepPolicy,
    source_order_limit: Option<usize>,
) -> Result<Vec<Vec<StatValue>>> {
    let mut selected = Vec::new();
    for (position, key, row) in rows {
        let retain = match keep {
            VortexDuplicateKeepPolicy::First => {
                first_positions.get(key).copied().ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex drop_duplicates missing first-position state; no fallback execution was attempted"
                            .to_string(),
                    )
                })? == *position
            }
            VortexDuplicateKeepPolicy::Last => {
                last_positions.get(key).copied().ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex drop_duplicates missing last-position state; no fallback execution was attempted"
                            .to_string(),
                    )
                })? == *position
            }
            VortexDuplicateKeepPolicy::AllDuplicates => counts.get(key).copied().ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex drop_duplicates missing count state; no fallback execution was attempted"
                        .to_string(),
                )
            })? == 1,
        };
        if retain {
            selected.push(row.clone());
            if source_order_limit.is_some_and(|limit| selected.len() >= limit) {
                break;
            }
        }
    }
    Ok(selected)
}

#[cfg(feature = "vortex-local-primitives")]
fn row_key_columns_from_chunk(
    chunk: &vortex::array::ArrayRef,
    declared_columns: &[String],
    key_columns: &[String],
) -> Result<Vec<Vec<String>>> {
    let mut out = Vec::with_capacity(key_columns.len());
    if chunk.dtype().is_struct() {
        let children = chunk
            .named_children()
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();
        for column in key_columns {
            let Some(array) = children.get(column.as_str()) else {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "local Vortex row-key column '{column}' was not present in scanned chunk; no fallback execution was attempted"
                )));
            };
            out.push(row_key_values_from_vortex_array(column, array)?);
        }
    } else {
        let column = declared_columns.first().map_or("value", String::as_str);
        if key_columns.len() != 1 || key_columns[0] != column {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex top-level row-key extraction requires the single projected value column; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        out.push(row_key_values_from_vortex_array(column, chunk)?);
    }
    let Some(row_count) = out.first().map(Vec::len) else {
        return Ok(out);
    };
    if out.iter().any(|values| values.len() != row_count) {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex row-key columns had mismatched row counts; no fallback execution was attempted"
                .to_string(),
        ));
    }
    Ok(out)
}

#[cfg(feature = "vortex-local-primitives")]
fn row_key_values_from_vortex_array(
    column: &str,
    array: &vortex::array::ArrayRef,
) -> Result<Vec<String>> {
    use vortex::array::dtype::DType;

    if let Some(values) = stat_values_from_vortex_array(array) {
        return Ok(values.iter().map(stat_value_key).collect());
    }
    match array.dtype() {
        DType::List(_, _) | DType::FixedSizeList(_, _, _) => {
            list_row_key_values_from_vortex_array(column, array)
        }
        DType::Struct(_, _) => struct_row_key_values_from_vortex_array(column, array),
        other => Err(ShardLoomError::InvalidOperation(format!(
            "local Vortex row-key column '{column}' has unsupported dtype {other:?}; no fallback execution was attempted"
        ))),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn list_row_key_values_from_vortex_array(
    column: &str,
    array: &vortex::array::ArrayRef,
) -> Result<Vec<String>> {
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::fixed_size_list::FixedSizeListArrayExt as _;
    use vortex::array::arrays::listview::ListViewArrayExt as _;
    use vortex::array::arrays::{FixedSizeListArray, ListViewArray};
    use vortex::array::dtype::DType;

    match array.dtype() {
        DType::List(_, _) => {
            let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
            let list = array
                .clone()
                .execute::<ListViewArray>(&mut ctx)
                .map_err(vortex_error)?;
            let validity = list.listview_validity();
            let mut validity_ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
            let mut out = Vec::with_capacity(list.len());
            for row_index in 0..list.len() {
                if !validity
                    .execute_is_valid(row_index, &mut validity_ctx)
                    .map_err(vortex_error)?
                {
                    out.push("l:parent-null".to_string());
                    continue;
                }
                let elements = list.list_elements_at(row_index).map_err(vortex_error)?;
                let values = scalar_values_from_vortex_array(column, &elements)?;
                out.push(list_row_key(&values));
            }
            Ok(out)
        }
        DType::FixedSizeList(_, _, _) => {
            let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
            let list = array
                .clone()
                .execute::<FixedSizeListArray>(&mut ctx)
                .map_err(vortex_error)?;
            let validity = list.fixed_size_list_validity();
            let mut validity_ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
            let mut out = Vec::with_capacity(list.len());
            for row_index in 0..list.len() {
                if !validity
                    .execute_is_valid(row_index, &mut validity_ctx)
                    .map_err(vortex_error)?
                {
                    out.push("l:parent-null".to_string());
                    continue;
                }
                let elements = list
                    .fixed_size_list_elements_at(row_index)
                    .map_err(vortex_error)?;
                let values = scalar_values_from_vortex_array(column, &elements)?;
                out.push(list_row_key(&values));
            }
            Ok(out)
        }
        other => Err(ShardLoomError::InvalidOperation(format!(
            "local Vortex row-key column '{column}' requires a list or fixed-size-list dtype, got {other:?}; no fallback execution was attempted"
        ))),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn struct_row_key_values_from_vortex_array(
    column: &str,
    array: &vortex::array::ArrayRef,
) -> Result<Vec<String>> {
    use vortex::array::VortexSessionExecute as _;

    let children = array
        .named_children()
        .into_iter()
        .collect::<std::collections::BTreeMap<_, _>>();
    let validity = array.validity().map_err(vortex_error)?;
    let mut validity_ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
    let mut field_values = Vec::with_capacity(children.len());
    for (field_name, field_array) in children {
        field_values.push((
            field_name.clone(),
            row_key_values_from_vortex_array(&field_name, &field_array)?,
        ));
    }
    let row_count = field_values
        .first()
        .map_or(array.len(), |(_, values)| values.len());
    for (field_name, values) in &field_values {
        if values.len() != row_count {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local Vortex row-key struct column '{column}' field '{field_name}' had mismatched row count; no fallback execution was attempted"
            )));
        }
    }
    let mut out = Vec::with_capacity(row_count);
    for row_index in 0..row_count {
        if !validity
            .execute_is_valid(row_index, &mut validity_ctx)
            .map_err(vortex_error)?
        {
            out.push("r:parent-null".to_string());
            continue;
        }
        let mut key = format!("r:{}:", field_values.len());
        for (field_name, values) in &field_values {
            let _ = write!(key, "{}:{}=", field_name.len(), field_name);
            let value = &values[row_index];
            let _ = write!(key, "{}:{};", value.len(), value);
        }
        out.push(key);
    }
    Ok(out)
}

#[cfg(feature = "vortex-local-primitives")]
fn row_key_from_key_columns(row_key_columns: &[Vec<String>], row_index: usize) -> Result<String> {
    let mut out = String::new();
    for values in row_key_columns {
        let value = values.get(row_index).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex row-key columns had mismatched row counts; no fallback execution was attempted"
                    .to_string(),
            )
        })?;
        let _ = write!(out, "|{}:{value}", value.len());
    }
    Ok(out)
}

#[cfg(feature = "vortex-local-primitives")]
fn stat_value_key(value: &StatValue) -> String {
    let mut out = String::new();
    push_distinct_value_key(&mut out, value);
    out
}

#[cfg(feature = "vortex-local-primitives")]
fn list_row_key(values: &[ScalarValue]) -> String {
    let mut out = format!("l:{}:", values.len());
    for value in values {
        let key = scalar_value_key(value);
        let _ = write!(out, "{}:{key};", key.len());
    }
    out
}

#[cfg(feature = "vortex-local-primitives")]
fn scalar_value_key(value: &ScalarValue) -> String {
    let mut out = String::new();
    push_scalar_value_key(&mut out, value);
    out
}

#[cfg(feature = "vortex-local-primitives")]
fn push_scalar_value_key(out: &mut String, value: &ScalarValue) {
    match value {
        ScalarValue::Null => {
            out.push_str("n:null");
        }
        ScalarValue::Boolean(value) => {
            let _ = write!(out, "b:{value}");
        }
        ScalarValue::Int64(value) => {
            let _ = write!(out, "i:{value}");
        }
        ScalarValue::UInt64(value) => {
            let _ = write!(out, "u:{value}");
        }
        ScalarValue::Float64(value) => {
            let _ = write!(out, "f:{:016x}", value.to_bits());
        }
        ScalarValue::Utf8(value) => {
            let _ = write!(out, "s:{}:{value}", value.len());
        }
        ScalarValue::Binary(value) => {
            let hex = binary_to_hex(value);
            let _ = write!(out, "x:{}:{hex}", hex.len());
        }
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => {
            let _ = write!(out, "d:{precision}:{scale}:{value}");
        }
        ScalarValue::Date32(value) => {
            let _ = write!(out, "date32:{value}");
        }
        ScalarValue::TimestampMicros(value) => {
            let _ = write!(out, "tsmicros:{value}");
        }
        ScalarValue::List(values) => {
            let _ = write!(out, "l:{}:", values.len());
            for value in values {
                let nested = scalar_value_key(value);
                let _ = write!(out, "{}:{nested};", nested.len());
            }
        }
        ScalarValue::Struct(fields) => {
            let _ = write!(out, "r:{}:", fields.len());
            for (field, value) in fields {
                let nested = scalar_value_key(value);
                let _ = write!(out, "{}:{field}={}:{};", field.len(), nested.len(), nested);
            }
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn push_distinct_value_key(out: &mut String, value: &StatValue) {
    match value {
        StatValue::Null => {
            out.push_str("n:null");
        }
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
        StatValue::Null => Ok(serde_json::Value::Null),
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
        StatValue::Null => String::new(),
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
        VortexQueryPrimitiveKind::DropDuplicateRows => {
            let scan = read_local_vortex_drop_duplicate_scan(uri, &path, request, policy)?;
            drop_duplicate_rows_report(&scan, request)
        }
        VortexQueryPrimitiveKind::DuplicateMaskRows => {
            let scan = read_local_vortex_duplicate_mask_scan(uri, &path, request, policy)?;
            duplicate_mask_rows_report(&scan, request)
        }
        VortexQueryPrimitiveKind::TailRows => {
            let scan = read_local_vortex_tail_scan(uri, &path, request, policy)?;
            tail_rows_report(request.kind, &scan)
        }
        VortexQueryPrimitiveKind::SampleRows => {
            let scan = read_local_vortex_sample_scan(uri, &path, request, policy)?;
            sample_rows_report(request, &scan)
        }
        VortexQueryPrimitiveKind::ExpressionProjectRows => {
            let scan = read_local_vortex_expression_project_scan(uri, &path, request, policy)?;
            expression_project_rows_report(request, &scan)
        }
        VortexQueryPrimitiveKind::MeltRows => {
            let scan = read_local_vortex_melt_scan(uri, &path, request, policy)?;
            melt_rows_report(request, &scan)
        }
        VortexQueryPrimitiveKind::ExplodeRows => {
            let scan = read_local_vortex_explode_scan(uri, &path, request, policy)?;
            explode_rows_report(request, &scan)
        }
        VortexQueryPrimitiveKind::PivotRows => {
            let scan = read_local_vortex_pivot_scan(uri, &path, request, policy)?;
            pivot_rows_report(request, &scan)
        }
        VortexQueryPrimitiveKind::RollingWindowRows => {
            let scan = read_local_vortex_rolling_window_scan(uri, &path, request, policy)?;
            rolling_window_rows_report(request, &scan)
        }
        VortexQueryPrimitiveKind::SimpleAggregate => {
            let aggregate = read_local_vortex_simple_aggregate_scan(uri, &path, request, policy)?;
            simple_aggregate_report(request, &aggregate)
        }
        VortexQueryPrimitiveKind::SortRows => {
            let rows = read_local_vortex_sort_rows_scan(uri, &path, request, policy)?;
            sort_rows_report(request, &rows)
        }
        VortexQueryPrimitiveKind::Unsupported => Ok(VortexLocalPrimitiveExecutionReport::blocked(
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
        )),
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
struct LocalVortexAggregateScan {
    scan: LocalVortexScan,
    result_summary: String,
    state_budget: VortexLocalPrimitiveStateBudgetReport,
}

#[cfg(feature = "vortex-local-primitives")]
struct LocalVortexRowsScan {
    scan: LocalVortexScan,
    result_summary: String,
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
fn sort_rows_state_budget_report(
    request: &VortexQueryPrimitiveRequest,
    scan: &LocalVortexScan,
    input_rows: u64,
) -> Result<VortexLocalPrimitiveStateBudgetReport> {
    let sort_rows = request.sort_rows.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "local Vortex sort state budget requires a typed sort payload; no fallback execution was attempted"
                .to_string(),
        )
    })?;
    let has_offset = sort_rows.offset > 0;
    let mut capillary_work_units = vec!["vortex_scan", "sort_key_projection", "topk_heap_state"];
    let mut pulseweave_pressure_signals = vec![
        "sort_candidate_rows",
        "topk_heap_rows",
        "materialized_sort_rows",
    ];
    if has_offset {
        capillary_work_units.push("offset_drain");
        pulseweave_pressure_signals.push("offset_drain_rows");
    }
    match sort_rows.tie_policy {
        VortexSortTiePolicy::First => {}
        VortexSortTiePolicy::Last => {
            capillary_work_units.push("source_order_tie_reversal");
            pulseweave_pressure_signals.push("tie_order_rows");
        }
        VortexSortTiePolicy::All => {
            capillary_work_units.push("cutoff_tie_expansion");
            pulseweave_pressure_signals.push("cutoff_tie_rows");
            pulseweave_pressure_signals.push("full_candidate_rows");
        }
    }
    let mut family = "raw_row_topk_sort_state".to_string();
    if has_offset {
        family.push_str("+offset");
    }
    if sort_rows.tie_policy != VortexSortTiePolicy::First {
        family.push_str("+tie_");
        family.push_str(sort_rows.tie_policy.as_str());
    }
    Ok(VortexLocalPrimitiveStateBudgetReport::bounded_in_memory(
        family,
        capillary_work_units,
        pulseweave_pressure_signals,
        input_rows,
        Some(input_rows.max(usize_to_u64(scan.result_row_count)?)),
        "local_vortex_sort_rows",
    ))
}

#[cfg(feature = "vortex-local-primitives")]
fn rolling_window_state_budget_report(
    request: &VortexQueryPrimitiveRequest,
    scan: &LocalVortexScan,
) -> Result<VortexLocalPrimitiveStateBudgetReport> {
    let rolling = request.rolling_window.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "local Vortex rolling-window state budget requires a typed rolling payload; no fallback execution was attempted"
                .to_string(),
        )
    })?;
    Ok(VortexLocalPrimitiveStateBudgetReport::bounded_in_memory(
        "rolling_window_state",
        vec![
            "vortex_scan",
            "source_order_boundary",
            "rolling_window_state_fragment",
        ],
        vec![
            "window_rows",
            "min_periods",
            "source_order_rows",
            "window_state_memory",
        ],
        usize_to_u64(rolling.window_size)?,
        Some(usize_to_u64(
            scan.pre_limit_result_row_count.max(rolling.window_size),
        )?),
        "local_vortex_rolling_window",
    ))
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::too_many_lines)]
fn row_export_state_budget_report(
    request: &VortexQueryPrimitiveRequest,
    input_rows: usize,
    output_rows: usize,
) -> Result<VortexLocalPrimitiveStateBudgetReport> {
    match request.kind {
        VortexQueryPrimitiveKind::RollingWindowRows => {
            let rolling = request.rolling_window.as_ref().ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex rolling-window row export state budget requires a typed rolling payload; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
            Ok(VortexLocalPrimitiveStateBudgetReport::bounded_in_memory(
                "rolling_window_state",
                vec![
                    "vortex_scan",
                    "source_order_boundary",
                    "rolling_window_state_fragment",
                    "compatibility_sink",
                ],
                vec![
                    "window_rows",
                    "min_periods",
                    "source_order_rows",
                    "window_state_memory",
                    "sink_rows",
                ],
                usize_to_u64(rolling.window_size)?,
                Some(usize_to_u64(input_rows.max(rolling.window_size))?),
                "local_vortex_rolling_window_row_export",
            ))
        }
        VortexQueryPrimitiveKind::DistinctRows => {
            Ok(VortexLocalPrimitiveStateBudgetReport::bounded_in_memory(
                "row_key_distinct_state",
                vec!["vortex_scan", "row_key_state", "compatibility_sink"],
                vec!["distinct_key_cardinality", "input_rows", "sink_rows"],
                usize_to_u64(output_rows)?,
                Some(usize_to_u64(input_rows.max(output_rows))?),
                "local_vortex_distinct_row_export",
            ))
        }
        VortexQueryPrimitiveKind::DropDuplicateRows => {
            Ok(VortexLocalPrimitiveStateBudgetReport::bounded_in_memory(
                "retained_row_deduplicate_state",
                vec![
                    "vortex_scan",
                    "row_key_state",
                    "retained_row_materialization",
                    "compatibility_sink",
                ],
                vec![
                    "deduplicate_key_cardinality",
                    "input_rows",
                    "retained_rows",
                    "sink_rows",
                ],
                usize_to_u64(input_rows)?,
                Some(usize_to_u64(input_rows.max(output_rows))?),
                "local_vortex_drop_duplicates_row_export",
            ))
        }
        VortexQueryPrimitiveKind::DuplicateMaskRows => {
            Ok(VortexLocalPrimitiveStateBudgetReport::bounded_in_memory(
                "duplicate_mask_row_key_state",
                vec![
                    "vortex_scan",
                    "row_key_state",
                    "duplicate_mask",
                    "compatibility_sink",
                ],
                vec!["duplicate_key_cardinality", "input_rows", "sink_rows"],
                usize_to_u64(input_rows)?,
                Some(usize_to_u64(input_rows.max(output_rows))?),
                "local_vortex_duplicate_mask_row_export",
            ))
        }
        VortexQueryPrimitiveKind::SampleRows => {
            let (family, capillary_work_units, pressure_signals) =
                if request.sample_with_replacement {
                    (
                        "sample_replacement_population_state",
                        vec![
                            "vortex_scan",
                            "seeded_population_state",
                            "compatibility_sink",
                        ],
                        vec!["population_rows", "sample_output_rows", "sink_rows"],
                    )
                } else {
                    (
                        "sample_selection_state",
                        vec![
                            "vortex_scan",
                            "seeded_selection_state",
                            "compatibility_sink",
                        ],
                        vec!["candidate_rows", "sample_output_rows", "sink_rows"],
                    )
                };
            Ok(VortexLocalPrimitiveStateBudgetReport::bounded_in_memory(
                family,
                capillary_work_units,
                pressure_signals,
                usize_to_u64(output_rows)?,
                Some(usize_to_u64(input_rows.max(output_rows))?),
                "local_vortex_sample_row_export",
            ))
        }
        VortexQueryPrimitiveKind::MeltRows | VortexQueryPrimitiveKind::ExplodeRows => {
            Ok(VortexLocalPrimitiveStateBudgetReport::bounded_in_memory(
                "row_expansion_stream_state",
                vec![
                    "vortex_scan",
                    "capillary_row_expansion",
                    "compatibility_sink",
                ],
                vec!["expanded_rows", "input_rows", "sink_rows"],
                usize_to_u64(output_rows)?,
                Some(usize_to_u64(input_rows.max(output_rows))?),
                "local_vortex_row_expansion_export",
            ))
        }
        _ => Ok(VortexLocalPrimitiveStateBudgetReport::not_required()),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn pivot_row_export_state_budget_report(
    input_rows: usize,
    output_rows: usize,
) -> Result<VortexLocalPrimitiveStateBudgetReport> {
    Ok(VortexLocalPrimitiveStateBudgetReport::bounded_in_memory(
        "pivot_wide_reshape_state",
        vec![
            "vortex_scan",
            "capillary_wide_reshape",
            "compatibility_sink",
        ],
        vec!["pivot_cell_cardinality", "wide_output_columns", "sink_rows"],
        usize_to_u64(input_rows.max(output_rows))?,
        Some(usize_to_u64(input_rows.max(output_rows))?),
        "local_vortex_pivot_row_export",
    ))
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

    let validity = primitive.validity();
    let values = match primitive.ptype() {
        PType::U8 => primitive
            .as_slice::<u8>()
            .iter()
            .map(|value| StatValue::UInt64(u64::from(*value)))
            .collect(),
        PType::U16 => primitive
            .as_slice::<u16>()
            .iter()
            .map(|value| StatValue::UInt64(u64::from(*value)))
            .collect(),
        PType::U32 => primitive
            .as_slice::<u32>()
            .iter()
            .map(|value| StatValue::UInt64(u64::from(*value)))
            .collect(),
        PType::U64 => primitive
            .as_slice::<u64>()
            .iter()
            .map(|value| StatValue::UInt64(*value))
            .collect(),
        PType::I8 => primitive
            .as_slice::<i8>()
            .iter()
            .map(|value| StatValue::Int64(i64::from(*value)))
            .collect(),
        PType::I16 => primitive
            .as_slice::<i16>()
            .iter()
            .map(|value| StatValue::Int64(i64::from(*value)))
            .collect(),
        PType::I32 => primitive
            .as_slice::<i32>()
            .iter()
            .map(|value| StatValue::Int64(i64::from(*value)))
            .collect(),
        PType::I64 => primitive
            .as_slice::<i64>()
            .iter()
            .map(|value| StatValue::Int64(*value))
            .collect(),
        PType::F16 => return None,
        PType::F32 => primitive
            .as_slice::<f32>()
            .iter()
            .map(|value| StatValue::Float64(f64::from(*value)))
            .collect(),
        PType::F64 => primitive
            .as_slice::<f64>()
            .iter()
            .map(|value| StatValue::Float64(*value))
            .collect(),
    };
    stat_values_with_validity(values, &validity)
}

#[cfg(feature = "vortex-local-primitives")]
fn stat_values_with_validity(
    mut values: Vec<StatValue>,
    validity: &vortex::array::validity::Validity,
) -> Option<Vec<StatValue>> {
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::validity::Validity;

    match validity {
        Validity::NonNullable | Validity::AllValid => Some(values),
        Validity::AllInvalid => Some(vec![StatValue::Null; values.len()]),
        Validity::Array(_) => {
            let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
            for (index, value) in values.iter_mut().enumerate() {
                if !validity.execute_is_valid(index, &mut ctx).ok()? {
                    *value = StatValue::Null;
                }
            }
            Some(values)
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn utf8_stat_values_from_vortex_array(array: &vortex::array::ArrayRef) -> Option<Vec<StatValue>> {
    use vortex::array::VortexSessionExecute as _;
    use vortex::array::arrays::VarBinViewArray;
    use vortex::array::arrays::varbinview::VarBinViewArrayExt as _;

    let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
    let utf8 = array.clone().execute::<VarBinViewArray>(&mut ctx).ok()?;
    let validity = utf8.varbinview_validity();
    let mut values = Vec::with_capacity(utf8.len());
    let mut validity_ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
    for index in 0..utf8.len() {
        if !validity.execute_is_valid(index, &mut validity_ctx).ok()? {
            values.push(StatValue::Null);
            continue;
        }
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

    let mut ctx = vortex::array::LEGACY_SESSION.create_execution_ctx();
    let bool_array = array.clone().execute::<BoolArray>(&mut ctx).ok()?;
    let validity = bool_array.validity().ok()?;
    let values = bool_array
        .to_bit_buffer()
        .iter()
        .take(bool_array.len())
        .map(StatValue::Boolean)
        .collect();
    stat_values_with_validity(values, &validity)
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
        state_budget: VortexLocalPrimitiveStateBudgetReport::not_required(),
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
        state_budget: VortexLocalPrimitiveStateBudgetReport::not_required(),
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
        state_budget: VortexLocalPrimitiveStateBudgetReport::not_required(),
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
        state_budget: VortexLocalPrimitiveStateBudgetReport::not_required(),
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
        state_budget: VortexLocalPrimitiveStateBudgetReport::not_required(),
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
fn drop_duplicate_rows_report(
    scan: &LocalVortexScan,
    request: &VortexQueryPrimitiveRequest,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let rows = usize_to_u64(scan.result_row_count)?;
    let state_bound_rows = usize::try_from(scan.source_row_count)
        .unwrap_or(scan.pre_limit_result_row_count)
        .max(scan.pre_limit_result_row_count);
    let key_columns = request
        .deduplicate_key_projection
        .as_ref()
        .map_or_else(|| "none".to_string(), ProjectionRequest::summary);
    Ok(VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::VortexScanPushdown,
        primitive_kind: VortexQueryPrimitiveKind::DropDuplicateRows,
        result_summary: Some(format!(
            "drop_duplicate_rows={} output_columns={} key_columns={} keep={}",
            rows,
            scan.projected_columns.join(","),
            key_columns,
            request.duplicate_keep.as_str()
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
        state_budget: VortexLocalPrimitiveStateBudgetReport::bounded_in_memory(
            "retained_row_deduplicate_state",
            vec![
                "vortex_scan",
                "row_key_state",
                "retained_row_materialization",
            ],
            vec!["deduplicate_key_cardinality", "input_rows", "retained_rows"],
            usize_to_u64(scan.pre_limit_result_row_count)?,
            Some(usize_to_u64(state_bound_rows)?),
            "local_vortex_drop_duplicates_collect",
        ),
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
fn duplicate_mask_rows_report(
    scan: &LocalVortexScan,
    request: &VortexQueryPrimitiveRequest,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let rows = usize_to_u64(scan.result_row_count)?;
    Ok(VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::VortexScanPushdown,
        primitive_kind: VortexQueryPrimitiveKind::DuplicateMaskRows,
        result_summary: Some(format!(
            "duplicate_mask_rows={} subset_columns={} output_columns=duplicated keep={}",
            rows,
            scan.projected_columns.join(","),
            request.duplicate_keep.as_str()
        )),
        rows_scanned: scan.source_row_count,
        rows_selected: Some(rows),
        rows_projected: Some(rows),
        projected_columns: vec!["duplicated".to_string()],
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
        filter_pushdown_applied: false,
        projection_pushdown_applied: scan.projection_pushdown_applied,
        upstream_filter_expression_used: false,
        upstream_projection_expression_used: scan.projection_pushdown_applied,
        source_order_limit_requested: scan.source_order_limit.map(usize_to_u64).transpose()?,
        source_order_limit_applied: scan.source_order_limit.is_some(),
        source_order_limit_input_rows: Some(usize_to_u64(scan.pre_limit_result_row_count)?),
        source_order_limit_rows_output: Some(rows),
        state_budget: VortexLocalPrimitiveStateBudgetReport::not_required(),
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
        state_budget: VortexLocalPrimitiveStateBudgetReport::not_required(),
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
        state_budget: VortexLocalPrimitiveStateBudgetReport::not_required(),
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
fn expression_project_rows_report(
    request: &VortexQueryPrimitiveRequest,
    scan: &LocalVortexScan,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let rows = usize_to_u64(scan.result_row_count)?;
    let expression_summary = request.expression_projection.as_ref().map_or_else(
        || "none".to_string(),
        VortexExpressionProjectionRequest::family_summary,
    );
    Ok(VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::VortexScanPushdown,
        primitive_kind: request.kind,
        result_summary: Some(format!(
            "expression_project_rows={} projected_columns={} expression_rewrites={}",
            rows,
            scan.projected_columns.join(","),
            expression_summary
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
        filter_pushdown_applied: false,
        projection_pushdown_applied: scan.projection_pushdown_applied,
        upstream_filter_expression_used: false,
        upstream_projection_expression_used: scan.projection_pushdown_applied,
        source_order_limit_requested: scan.source_order_limit.map(usize_to_u64).transpose()?,
        source_order_limit_applied: scan.source_order_limit.is_some(),
        source_order_limit_input_rows: Some(usize_to_u64(scan.pre_limit_result_row_count)?),
        source_order_limit_rows_output: Some(rows),
        state_budget: VortexLocalPrimitiveStateBudgetReport::not_required(),
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
fn melt_rows_report(
    request: &VortexQueryPrimitiveRequest,
    scan: &LocalVortexScan,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let rows = usize_to_u64(scan.result_row_count)?;
    let melt_summary = request
        .melt_projection
        .as_ref()
        .map_or_else(|| "none".to_string(), VortexMeltProjectionRequest::summary);
    let output_columns = request
        .melt_projection
        .as_ref()
        .map_or_else(Vec::new, VortexMeltProjectionRequest::output_columns);
    Ok(VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::VortexScanPushdown,
        primitive_kind: request.kind,
        result_summary: Some(format!(
            "melt_rows={} projected_columns={} {}",
            rows,
            scan.projected_columns.join(","),
            melt_summary
        )),
        rows_scanned: scan.source_row_count,
        rows_selected: Some(rows),
        rows_projected: Some(rows),
        projected_columns: output_columns,
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
        filter_pushdown_applied: false,
        projection_pushdown_applied: scan.projection_pushdown_applied,
        upstream_filter_expression_used: false,
        upstream_projection_expression_used: scan.projection_pushdown_applied,
        source_order_limit_requested: scan.source_order_limit.map(usize_to_u64).transpose()?,
        source_order_limit_applied: scan.source_order_limit.is_some(),
        source_order_limit_input_rows: Some(usize_to_u64(scan.pre_limit_result_row_count)?),
        source_order_limit_rows_output: Some(rows),
        state_budget: VortexLocalPrimitiveStateBudgetReport::not_required(),
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
fn explode_rows_report(
    request: &VortexQueryPrimitiveRequest,
    scan: &LocalVortexScan,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let rows = usize_to_u64(scan.result_row_count)?;
    let explode_summary = request.explode_projection.as_ref().map_or_else(
        || "none".to_string(),
        VortexExplodeProjectionRequest::summary,
    );
    Ok(VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::VortexScanPushdown,
        primitive_kind: request.kind,
        result_summary: Some(format!(
            "explode_rows={} projected_columns={} {}",
            rows,
            scan.projected_columns.join(","),
            explode_summary
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
        filter_pushdown_applied: false,
        projection_pushdown_applied: scan.projection_pushdown_applied,
        upstream_filter_expression_used: false,
        upstream_projection_expression_used: scan.projection_pushdown_applied,
        source_order_limit_requested: scan.source_order_limit.map(usize_to_u64).transpose()?,
        source_order_limit_applied: scan.source_order_limit.is_some(),
        source_order_limit_input_rows: Some(usize_to_u64(scan.pre_limit_result_row_count)?),
        source_order_limit_rows_output: Some(rows),
        state_budget: VortexLocalPrimitiveStateBudgetReport::not_required(),
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
fn pivot_rows_report(
    request: &VortexQueryPrimitiveRequest,
    scan: &LocalVortexScan,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let rows = usize_to_u64(scan.result_row_count)?;
    let pivot_summary = request
        .pivot_projection
        .as_ref()
        .map_or_else(|| "none".to_string(), VortexPivotProjectionRequest::summary);
    Ok(VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::VortexScanPushdown,
        primitive_kind: request.kind,
        result_summary: Some(format!(
            "pivot_rows={} projected_columns={} {}",
            rows,
            scan.projected_columns.join(","),
            pivot_summary
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
        filter_pushdown_applied: false,
        projection_pushdown_applied: scan.projection_pushdown_applied,
        upstream_filter_expression_used: false,
        upstream_projection_expression_used: scan.projection_pushdown_applied,
        source_order_limit_requested: scan.source_order_limit.map(usize_to_u64).transpose()?,
        source_order_limit_applied: scan.source_order_limit.is_some(),
        source_order_limit_input_rows: Some(usize_to_u64(scan.pre_limit_result_row_count)?),
        source_order_limit_rows_output: Some(rows),
        state_budget: VortexLocalPrimitiveStateBudgetReport::not_required(),
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
fn required_pivot_projection(
    request: &VortexQueryPrimitiveRequest,
) -> Result<&VortexPivotProjectionRequest> {
    request.pivot_projection.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "local Vortex pivot requires a typed pivot projection payload; no fallback execution was attempted"
                .to_string(),
        )
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn normalized_pivot_aggregate(projection: &VortexPivotProjectionRequest) -> Result<&str> {
    let aggregate = projection.aggregate.trim();
    match aggregate {
        "first" | "first_unique" | "sum" | "count" | "mean" | "min" | "max" => Ok(aggregate),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "local Vortex scoped pivot supports aggregate first_unique, first, sum, count, mean, min, or max, got '{aggregate}'; no fallback execution was attempted"
        ))),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn pivot_value_key(value: &StatValue) -> String {
    let mut out = String::new();
    push_distinct_value_key(&mut out, value);
    out
}

#[cfg(feature = "vortex-local-primitives")]
fn pivot_output_column_name(value: &StatValue) -> String {
    let raw = stat_value_to_csv_cell(value);
    let mut out = String::from("pivot_");
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    while out.ends_with('_') {
        out.pop();
    }
    if out == "pivot" {
        out.push_str("_value");
    }
    out
}

#[cfg(feature = "vortex-local-primitives")]
fn ensure_pivot_output_column_name(
    pivot_columns: &mut std::collections::BTreeMap<String, String>,
    pivot_key: &str,
    pivot_value: &StatValue,
) {
    if pivot_columns.contains_key(pivot_key) {
        return;
    }
    let base = pivot_output_column_name(pivot_value);
    let mut candidate = base.clone();
    let mut suffix = 2usize;
    while pivot_columns
        .values()
        .any(|existing| existing == &candidate)
    {
        candidate = format!("{base}_{suffix}");
        suffix += 1;
    }
    pivot_columns.insert(pivot_key.to_string(), candidate);
}

#[cfg(feature = "vortex-local-primitives")]
#[derive(Debug, Clone, Copy, Default)]
struct PivotAggregateCell {
    count: u64,
    sum: f64,
    min: Option<f64>,
    max: Option<f64>,
}

#[cfg(feature = "vortex-local-primitives")]
#[derive(Debug, Default)]
struct PivotRowExportState {
    index_keys: std::collections::BTreeSet<String>,
    index_values: std::collections::BTreeMap<String, StatValue>,
    pivot_columns: std::collections::BTreeMap<String, String>,
    first_cells: std::collections::BTreeMap<(String, String), StatValue>,
    aggregate_cells: std::collections::BTreeMap<(String, String), PivotAggregateCell>,
}

#[cfg(feature = "vortex-local-primitives")]
impl PivotRowExportState {
    fn update(
        &mut self,
        projection: &VortexPivotProjectionRequest,
        aggregate: &str,
        index_values: &[StatValue],
        pivot_values: &[StatValue],
        value_values: &[StatValue],
    ) -> Result<()> {
        if index_values.len() != pivot_values.len() || index_values.len() != value_values.len() {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex pivot row export scanned columns with mismatched row counts; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        for row_index in 0..index_values.len() {
            let index_key = pivot_value_key(&index_values[row_index]);
            let pivot_key = pivot_value_key(&pivot_values[row_index]);
            self.index_keys.insert(index_key.clone());
            self.index_values
                .entry(index_key.clone())
                .or_insert_with(|| index_values[row_index].clone());
            ensure_pivot_output_column_name(
                &mut self.pivot_columns,
                &pivot_key,
                &pivot_values[row_index],
            );
            let cell_key = (index_key, pivot_key);
            match aggregate {
                "first" | "first_unique" => {
                    if let Some(existing) = self.first_cells.get(&cell_key) {
                        if aggregate == "first_unique"
                            && !stat_value_equal(existing, &value_values[row_index])
                        {
                            return Err(ShardLoomError::InvalidOperation(format!(
                                "local Vortex scoped pivot row export found multiple values for index '{}' and pivot '{}'; use pivot_table with an explicit aggregate or provide unique cells; no fallback execution was attempted",
                                projection.index_column.as_str(),
                                projection.pivot_column.as_str()
                            )));
                        }
                    } else {
                        self.first_cells
                            .insert(cell_key, value_values[row_index].clone());
                    }
                }
                "count" => {
                    self.aggregate_cells.entry(cell_key).or_default().count += 1;
                }
                "sum" | "mean" | "min" | "max" => {
                    let value = stat_value_to_f64(&value_values[row_index]).map_err(|_| {
                        ShardLoomError::InvalidOperation(format!(
                            "local Vortex scoped pivot_table row export aggregate '{aggregate}' requires a numeric value column '{}'; no fallback execution was attempted",
                            projection.value_column.as_str()
                        ))
                    })?;
                    if !value.is_finite() {
                        return Err(ShardLoomError::InvalidOperation(format!(
                            "local Vortex scoped pivot_table row export aggregate '{aggregate}' found non-finite value in '{}'; no fallback execution was attempted",
                            projection.value_column.as_str()
                        )));
                    }
                    let cell = self.aggregate_cells.entry(cell_key).or_default();
                    cell.count += 1;
                    cell.sum += value;
                    cell.min = Some(cell.min.map_or(value, |current| current.min(value)));
                    cell.max = Some(cell.max.map_or(value, |current| current.max(value)));
                }
                _ => unreachable!("pivot aggregate normalized before row export state update"),
            }
        }
        Ok(())
    }

    fn output_columns(&self, projection: &VortexPivotProjectionRequest) -> Vec<String> {
        let active_pivot_keys = self.active_pivot_keys(projection.aggregate.as_str(), projection);
        let mut columns =
            Vec::with_capacity(1 + active_pivot_keys.len() + usize::from(projection.margins));
        columns.push(projection.index_column.as_str().to_string());
        columns.extend(
            active_pivot_keys
                .iter()
                .filter_map(|pivot_key| self.pivot_columns.get(pivot_key).cloned()),
        );
        if projection.margins {
            columns.push(pivot_output_column_name(&StatValue::Utf8(
                projection.margins_name.clone(),
            )));
        }
        columns
    }

    fn materialized_rows(
        &self,
        aggregate: &str,
        projection: &VortexPivotProjectionRequest,
        limit: usize,
    ) -> Result<Vec<Vec<Option<StatValue>>>> {
        if projection.margins && !matches!(aggregate, "count" | "sum" | "mean" | "min" | "max") {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex scoped pivot margins require pivot_table aggregate count, sum, mean, min, or max; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        let active_pivot_keys = self.active_pivot_keys(aggregate, projection);
        let row_count = limit.min(self.index_keys.len());
        let mut rows = Vec::with_capacity(row_count + usize::from(projection.margins));
        for index_key in self.index_keys.iter().take(limit) {
            let Some(index_value) = self.index_values.get(index_key) else {
                return Err(ShardLoomError::InvalidOperation(
                    "local Vortex pivot row export lost index value state; no fallback execution was attempted"
                        .to_string(),
                ));
            };
            let mut row =
                Vec::with_capacity(1 + active_pivot_keys.len() + usize::from(projection.margins));
            row.push(Some(index_value.clone()));
            for pivot_key in &active_pivot_keys {
                let cell_key = (index_key.clone(), pivot_key.clone());
                row.push(Self::apply_pivot_fill(
                    self.materialized_cell(aggregate, &cell_key),
                    projection,
                ));
            }
            if projection.margins {
                row.push(Self::apply_pivot_fill(
                    self.row_margin_value(aggregate, index_key, &active_pivot_keys)?,
                    projection,
                ));
            }
            rows.push(row);
        }
        if projection.margins {
            let mut margin_row = Vec::with_capacity(1 + active_pivot_keys.len() + 1);
            margin_row.push(Some(StatValue::Utf8(projection.margins_name.clone())));
            for pivot_key in &active_pivot_keys {
                margin_row.push(Self::apply_pivot_fill(
                    self.column_margin_value(
                        aggregate,
                        pivot_key,
                        self.index_keys.iter().take(limit),
                    )?,
                    projection,
                ));
            }
            margin_row.push(Self::apply_pivot_fill(
                self.grand_margin_value(
                    aggregate,
                    self.index_keys.iter().take(limit),
                    &active_pivot_keys,
                )?,
                projection,
            ));
            rows.push(margin_row);
        }
        Ok(rows)
    }

    fn active_pivot_keys(
        &self,
        aggregate: &str,
        projection: &VortexPivotProjectionRequest,
    ) -> Vec<String> {
        self.pivot_columns
            .keys()
            .filter(|pivot_key| {
                !projection.dropna
                    || self.index_keys.iter().any(|index_key| match aggregate {
                        "first" | "first_unique" => self
                            .first_cells
                            .contains_key(&(index_key.clone(), (*pivot_key).clone())),
                        _ => self
                            .aggregate_cells
                            .contains_key(&(index_key.clone(), (*pivot_key).clone())),
                    })
            })
            .cloned()
            .collect()
    }

    fn apply_pivot_fill(
        value: Option<StatValue>,
        projection: &VortexPivotProjectionRequest,
    ) -> Option<StatValue> {
        value.or_else(|| projection.fill_value.clone())
    }

    fn materialized_cell(&self, aggregate: &str, cell_key: &(String, String)) -> Option<StatValue> {
        match aggregate {
            "first" | "first_unique" => self.first_cells.get(cell_key).cloned(),
            "count" => self
                .aggregate_cells
                .get(cell_key)
                .map(|cell| StatValue::UInt64(cell.count)),
            "sum" => self
                .aggregate_cells
                .get(cell_key)
                .map(|cell| StatValue::Float64(cell.sum)),
            "mean" => self
                .aggregate_cells
                .get(cell_key)
                .and_then(|cell| pivot_mean_value(cell.sum, cell.count).map(StatValue::Float64)),
            "min" => self
                .aggregate_cells
                .get(cell_key)
                .and_then(|cell| cell.min)
                .map(StatValue::Float64),
            "max" => self
                .aggregate_cells
                .get(cell_key)
                .and_then(|cell| cell.max)
                .map(StatValue::Float64),
            _ => unreachable!("pivot aggregate normalized before row materialization"),
        }
    }

    fn row_margin_value(
        &self,
        aggregate: &str,
        index_key: &str,
        active_pivot_keys: &[String],
    ) -> Result<Option<StatValue>> {
        let cells = active_pivot_keys
            .iter()
            .filter_map(|pivot_key| {
                self.aggregate_cells
                    .get(&(index_key.to_string(), pivot_key.clone()))
            })
            .copied()
            .collect::<Vec<_>>();
        pivot_margin_from_cells(aggregate, &cells)
    }

    fn column_margin_value<'a>(
        &self,
        aggregate: &str,
        pivot_key: &str,
        index_keys: impl Iterator<Item = &'a String>,
    ) -> Result<Option<StatValue>> {
        let cells = index_keys
            .filter_map(|index_key| {
                self.aggregate_cells
                    .get(&(index_key.clone(), pivot_key.to_string()))
            })
            .copied()
            .collect::<Vec<_>>();
        pivot_margin_from_cells(aggregate, &cells)
    }

    fn grand_margin_value<'a>(
        &self,
        aggregate: &str,
        index_keys: impl Iterator<Item = &'a String>,
        active_pivot_keys: &[String],
    ) -> Result<Option<StatValue>> {
        let index_keys = index_keys.cloned().collect::<Vec<_>>();
        let cells = index_keys
            .iter()
            .flat_map(|index_key| {
                active_pivot_keys.iter().filter_map(move |pivot_key| {
                    self.aggregate_cells
                        .get(&(index_key.clone(), pivot_key.clone()))
                })
            })
            .copied()
            .collect::<Vec<_>>();
        pivot_margin_from_cells(aggregate, &cells)
    }
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::cast_precision_loss)]
fn pivot_mean_value(sum: f64, count: u64) -> Option<f64> {
    (count > 0).then_some(sum / count as f64)
}

#[cfg(feature = "vortex-local-primitives")]
fn pivot_margin_from_cells(
    aggregate: &str,
    cells: &[PivotAggregateCell],
) -> Result<Option<StatValue>> {
    if cells.is_empty() {
        return Ok(None);
    }
    match aggregate {
        "count" => Ok(Some(StatValue::UInt64(cells.iter().map(|cell| cell.count).sum()))),
        "sum" => Ok(Some(StatValue::Float64(
            cells.iter().map(|cell| cell.sum).sum(),
        ))),
        "mean" => {
            let count: u64 = cells.iter().map(|cell| cell.count).sum();
            let sum: f64 = cells.iter().map(|cell| cell.sum).sum();
            Ok(pivot_mean_value(sum, count).map(StatValue::Float64))
        }
        "min" => {
            let value = cells
                .iter()
                .filter_map(|cell| cell.min)
                .reduce(f64::min);
            Ok(value.map(StatValue::Float64))
        }
        "max" => {
            let value = cells
                .iter()
                .filter_map(|cell| cell.max)
                .reduce(f64::max);
            Ok(value.map(StatValue::Float64))
        }
        _ => Err(ShardLoomError::InvalidOperation(
            "local Vortex scoped pivot margin aggregate was not admitted; no fallback execution was attempted"
                .to_string(),
        )),
    }
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::too_many_arguments)]
fn update_pivot_state(
    projection: &VortexPivotProjectionRequest,
    aggregate: &str,
    index_values: &[StatValue],
    pivot_values: &[StatValue],
    value_values: &[StatValue],
    index_keys: &mut std::collections::BTreeSet<String>,
    pivot_columns: &mut std::collections::BTreeMap<String, String>,
    first_cells: &mut std::collections::BTreeMap<(String, String), StatValue>,
    aggregate_cells: &mut std::collections::BTreeMap<(String, String), PivotAggregateCell>,
) -> Result<()> {
    if index_values.len() != pivot_values.len() || index_values.len() != value_values.len() {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex pivot scanned columns with mismatched row counts; no fallback execution was attempted"
                .to_string(),
        ));
    }
    for row_index in 0..index_values.len() {
        let index_key = pivot_value_key(&index_values[row_index]);
        let pivot_key = pivot_value_key(&pivot_values[row_index]);
        index_keys.insert(index_key.clone());
        ensure_pivot_output_column_name(pivot_columns, &pivot_key, &pivot_values[row_index]);
        let cell_key = (index_key, pivot_key);
        match aggregate {
            "first" | "first_unique" => {
                if let Some(existing) = first_cells.get(&cell_key) {
                    if aggregate == "first_unique"
                        && !stat_value_equal(existing, &value_values[row_index])
                    {
                        return Err(ShardLoomError::InvalidOperation(format!(
                            "local Vortex scoped pivot found multiple values for index '{}' and pivot '{}'; use pivot_table with an explicit aggregate or provide unique cells; no fallback execution was attempted",
                            projection.index_column.as_str(),
                            projection.pivot_column.as_str()
                        )));
                    }
                } else {
                    first_cells.insert(cell_key, value_values[row_index].clone());
                }
            }
            "count" => {
                aggregate_cells.entry(cell_key).or_default().count += 1;
            }
            "sum" | "mean" | "min" | "max" => {
                let value = stat_value_to_f64(&value_values[row_index]).map_err(|_| {
                    ShardLoomError::InvalidOperation(format!(
                        "local Vortex scoped pivot_table aggregate '{aggregate}' requires a numeric value column '{}'; no fallback execution was attempted",
                        projection.value_column.as_str()
                    ))
                })?;
                if !value.is_finite() {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "local Vortex scoped pivot_table aggregate '{aggregate}' found non-finite value in '{}'; no fallback execution was attempted",
                        projection.value_column.as_str()
                    )));
                }
                let cell = aggregate_cells.entry(cell_key).or_default();
                cell.count += 1;
                cell.sum += value;
                cell.min = Some(cell.min.map_or(value, |current| current.min(value)));
                cell.max = Some(cell.max.map_or(value, |current| current.max(value)));
            }
            _ => unreachable!("pivot aggregate normalized before state update"),
        }
    }
    Ok(())
}

#[cfg(feature = "vortex-local-primitives")]
fn rolling_window_rows_report(
    request: &VortexQueryPrimitiveRequest,
    scan: &LocalVortexScan,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let rows = usize_to_u64(scan.result_row_count)?;
    let rolling_summary = request
        .rolling_window
        .as_ref()
        .map_or_else(|| "none".to_string(), VortexRollingWindowRequest::summary);
    let output_columns = request
        .rolling_window
        .as_ref()
        .map_or_else(Vec::new, VortexRollingWindowRequest::output_columns);
    Ok(VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::VortexScanPushdown,
        primitive_kind: request.kind,
        result_summary: Some(format!(
            "rolling_window_rows={} projected_columns={} {}",
            rows,
            scan.projected_columns.join(","),
            rolling_summary
        )),
        rows_scanned: scan.source_row_count,
        rows_selected: Some(rows),
        rows_projected: Some(rows),
        projected_columns: output_columns,
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
        filter_pushdown_applied: false,
        projection_pushdown_applied: scan.projection_pushdown_applied,
        upstream_filter_expression_used: false,
        upstream_projection_expression_used: scan.projection_pushdown_applied,
        source_order_limit_requested: scan.source_order_limit.map(usize_to_u64).transpose()?,
        source_order_limit_applied: scan.source_order_limit.is_some(),
        source_order_limit_input_rows: Some(usize_to_u64(scan.pre_limit_result_row_count)?),
        source_order_limit_rows_output: Some(rows),
        state_budget: rolling_window_state_budget_report(request, scan)?,
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
fn simple_aggregate_report(
    request: &VortexQueryPrimitiveRequest,
    aggregate: &LocalVortexAggregateScan,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let scan = &aggregate.scan;
    let input_rows = usize_to_u64(scan.pre_limit_result_row_count)?;
    let rows = usize_to_u64(scan.result_row_count)?;
    let aggregate_summary = request
        .simple_aggregate
        .as_ref()
        .map_or_else(|| "none".to_string(), VortexSimpleAggregateRequest::summary);
    Ok(VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::VortexScanPushdown,
        primitive_kind: request.kind,
        result_summary: Some(format!(
            "simple_aggregate input_rows={} output_rows={} projected_columns={} measures={} values={}",
            input_rows,
            rows,
            scan.projected_columns.join(","),
            aggregate_summary,
            aggregate.result_summary
        )),
        rows_scanned: scan.source_row_count,
        rows_selected: Some(input_rows),
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
        source_order_limit_input_rows: Some(input_rows),
        source_order_limit_rows_output: Some(rows),
        state_budget: aggregate.state_budget.clone(),
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
fn sort_rows_report(
    request: &VortexQueryPrimitiveRequest,
    rows_scan: &LocalVortexRowsScan,
) -> Result<VortexLocalPrimitiveExecutionReport> {
    let scan = &rows_scan.scan;
    let input_rows = usize_to_u64(scan.pre_limit_result_row_count)?;
    let rows = usize_to_u64(scan.result_row_count)?;
    let sort_summary = request
        .sort_rows
        .as_ref()
        .map_or_else(|| "none".to_string(), VortexSortRowsRequest::summary);
    Ok(VortexLocalPrimitiveExecutionReport {
        status: VortexLocalPrimitiveExecutionStatus::Executed,
        mode: VortexLocalPrimitiveExecutionMode::VortexScanPushdown,
        primitive_kind: request.kind,
        result_summary: Some(format!(
            "sort_rows input_rows={} output_rows={} projected_columns={} order={} values={}",
            input_rows,
            rows,
            scan.projected_columns.join(","),
            sort_summary,
            rows_scan.result_summary
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
        source_order_limit_input_rows: Some(input_rows),
        source_order_limit_rows_output: Some(rows),
        state_budget: sort_rows_state_budget_report(request, scan, input_rows)?,
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
        let row_key_columns =
            row_key_columns_from_chunk(&chunk, &declared_columns, &declared_columns)?;
        let selected = distinct_row_indices(
            &row_key_columns,
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
        projected_columns: plan.projected_columns,
        filter_pushdown_applied,
        projection_pushdown_applied,
        source_order_limit,
    })
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::too_many_lines)]
fn read_local_vortex_drop_duplicate_scan(
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
    let plan = drop_duplicate_scan_plan(file.dtype(), request)?;
    if plan.source_order_limit == Some(0) {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex drop_duplicates source-order limit must be >= 1".to_string(),
        ));
    }
    let output_columns = drop_duplicate_output_columns(file.dtype(), request)?;
    let key_columns = drop_duplicate_key_columns(file.dtype(), request)?;
    let declared_columns = plan.projected_columns.clone();
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

    let mut counts = std::collections::BTreeMap::<String, usize>::new();
    let mut first_positions = std::collections::BTreeMap::<String, usize>::new();
    let mut last_positions = std::collections::BTreeMap::<String, usize>::new();
    let mut rows = Vec::<(usize, String, Vec<StatValue>)>::new();
    let mut global_index = 0usize;
    let mut arrays_read_count = 0usize;
    let mut reader_splits = Vec::new();
    let mut encoded_kernel_inputs = Vec::new();
    let mut max_chunk_rows = 0usize;
    for chunk in scan.into_array_iter(&runtime).map_err(vortex_error)? {
        let chunk = chunk.map_err(vortex_error)?;
        let chunk_rows = chunk.len();
        let split = VortexReaderBackedSplitEvidence::local_scan_chunk(
            source_uri.clone(),
            arrays_read_count,
            chunk_rows,
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
        let output_column_values = row_export_columns_from_chunk(&chunk, &output_columns)?;
        let row_key_columns = row_key_columns_from_chunk(&chunk, &declared_columns, &key_columns)?;
        let materialized_rows = row_key_materialized_row_count(&row_key_columns)?;
        let output_rows = row_export_materialized_row_count(&output_column_values, chunk_rows)?;
        if output_rows != materialized_rows {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex drop_duplicates row-key and output columns had mismatched row counts; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        for row_index in 0..materialized_rows {
            let key = row_key_from_key_columns(&row_key_columns, row_index)?;
            *counts.entry(key.clone()).or_insert(0) += 1;
            first_positions.entry(key.clone()).or_insert(global_index);
            last_positions.insert(key.clone(), global_index);
            let row = row_export_materialized_projected_row(
                &output_columns,
                &output_column_values,
                &output_columns,
                row_index,
            )?;
            rows.push((global_index, key, row));
            global_index = global_index.checked_add(1).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex drop_duplicates row ordinal overflowed usize".to_string(),
                )
            })?;
        }
        max_chunk_rows = max_chunk_rows.max(chunk_rows);
        arrays_read_count += 1;
    }
    let retained_rows = retain_drop_duplicate_rows_from_policy(
        &rows,
        &counts,
        &first_positions,
        &last_positions,
        request.duplicate_keep,
        None,
    )?;
    let pre_limit_result_row_count = retained_rows.len();
    let result_row_count = source_order_limit.map_or(pre_limit_result_row_count, |limit| {
        limit.min(pre_limit_result_row_count)
    });
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
        projected_columns: output_columns,
        filter_pushdown_applied,
        projection_pushdown_applied,
        source_order_limit,
    })
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::too_many_lines)]
fn read_local_vortex_duplicate_mask_scan(
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
            "local Vortex duplicate-mask source-order limit must be >= 1".to_string(),
        ));
    }
    let declared_columns = if plan.projected_columns.is_empty() {
        local_field_names(file.dtype(), request.kind)?
    } else {
        plan.projected_columns.clone()
    };
    let projection_pushdown_applied = plan.projection.is_some();
    let source_order_limit = plan.source_order_limit;
    let mut scan = file.scan().map_err(vortex_error)?;
    if let Some(projection) = plan.projection {
        scan = scan.with_projection(projection);
    }
    scan = scan.with_concurrency(policy.scan_concurrency_per_worker());

    let mut seen = std::collections::BTreeSet::new();
    let duplicate_keep = request.duplicate_keep;
    let duplicate_needs_full_scan = duplicate_keep != VortexDuplicateKeepPolicy::First;
    let mut result_row_count = 0usize;
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
        let row_key_columns =
            row_key_columns_from_chunk(&chunk, &declared_columns, &declared_columns)?;
        let materialized_rows = row_key_materialized_row_count(&row_key_columns)?;
        let output_rows = source_order_limit.map_or(materialized_rows, |limit| {
            limit
                .saturating_sub(result_row_count)
                .min(materialized_rows)
        });
        if duplicate_needs_full_scan {
            for row_index in 0..materialized_rows {
                let _ = row_key_from_key_columns(&row_key_columns, row_index)?;
            }
            result_row_count = result_row_count.checked_add(output_rows).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex duplicate-mask result row count overflowed usize".to_string(),
                )
            })?;
        } else {
            let duplicate_values = duplicate_mask_values(&row_key_columns, &mut seen, output_rows)?;
            result_row_count = result_row_count
                .checked_add(duplicate_values.len())
                .ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex duplicate-mask result row count overflowed usize".to_string(),
                    )
                })?;
        }
        pre_limit_result_row_count = pre_limit_result_row_count
            .checked_add(output_rows)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex duplicate-mask pre-limit row count overflowed usize".to_string(),
                )
            })?;
        max_chunk_rows = max_chunk_rows.max(rows);
        arrays_read_count += 1;
        if !duplicate_needs_full_scan
            && source_order_limit.is_some_and(|limit| result_row_count >= limit)
        {
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
        projected_columns: declared_columns,
        filter_pushdown_applied: false,
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
    let mut plan = sample_scan_plan(file.dtype(), request)?;
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
    let output_columns = sample_output_columns(file.dtype(), request)?;
    let declared_columns = if plan.projected_columns.is_empty() {
        local_field_names(file.dtype(), request.kind)?
    } else {
        plan.projected_columns.clone()
    };
    let sample_weight_column_index = request
        .sample_weight_column
        .as_ref()
        .map(|column| column_index(&declared_columns, column.as_str()))
        .transpose()?;
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
        for (local_index, maybe_weight_value) in (0..materialized_rows).map(|local_index| {
            (
                local_index,
                sample_weight_column_index.map(|weight_column_index| {
                    &materialized_columns[weight_column_index][local_index]
                }),
            )
        }) {
            let row_index = pre_limit_result_row_count
                .checked_add(local_index)
                .ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex sample row ordinal overflowed usize".to_string(),
                    )
                })?;
            let score = deterministic_sample_score(sample_seed, row_index);
            if let Some(weight_value) = maybe_weight_value {
                let _ = sample_weight_value(weight_value)?;
            } else if request.sample_with_replacement {
                // Replacement collect only needs the admitted population count;
                // row export performs the deterministic duplicate-row draw.
            } else if let Some(limit) = source_order_limit {
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
    let result_row_count = if sample_weight_column_index.is_some()
        || sample_fraction.is_some()
        || request.sample_with_replacement
    {
        sample_target_count(request, pre_limit_result_row_count)?
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
        projected_columns: output_columns,
        filter_pushdown_applied,
        projection_pushdown_applied,
        source_order_limit,
    })
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::too_many_lines)]
fn read_local_vortex_expression_project_scan(
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

    let expression_projection = request.expression_projection.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "local Vortex expression projection requires a typed expression payload".to_string(),
        )
    })?;
    if expression_projection.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex expression projection requires at least one rewrite".to_string(),
        ));
    }

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
    let mut plan = projection_scan_plan(file.dtype(), &request.projection, request.kind)?;
    plan.source_order_limit = request.source_order_limit;
    if plan.source_order_limit == Some(0) {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex expression projection source-order limit must be >= 1".to_string(),
        ));
    }
    let declared_columns = if plan.projected_columns.is_empty() {
        local_field_names(file.dtype(), request.kind)?
    } else {
        plan.projected_columns.clone()
    };
    validate_expression_projection_columns(expression_projection, &declared_columns)?;

    let projection_pushdown_applied = plan.projection.is_some();
    let source_order_limit = plan.source_order_limit;
    let mut scan = file.scan().map_err(vortex_error)?;
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
    let mut row_number_offset = 0_u64;
    let mut expression_projection_state = ExpressionProjectionState::default();
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
        let mut columns = row_export_columns_from_chunk(&chunk, &declared_columns)?;
        let mut chunk_columns = declared_columns.clone();
        apply_expression_projection_columns(
            &mut chunk_columns,
            &mut columns,
            expression_projection,
            row_number_offset,
            &mut expression_projection_state,
        )?;
        let materialized_rows = row_export_materialized_row_count(&columns, rows)?;
        row_number_offset = row_number_offset
            .checked_add(usize_to_u64(materialized_rows)?)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex expression projection row-number offset overflowed; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
        pre_limit_result_row_count = pre_limit_result_row_count
            .checked_add(materialized_rows)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex expression projection pre-limit row count overflowed usize"
                        .to_string(),
                )
            })?;
        let output_rows = source_order_limit.map_or(materialized_rows, |limit| {
            limit
                .saturating_sub(result_row_count)
                .min(materialized_rows)
        });
        result_row_count = result_row_count.checked_add(output_rows).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex expression projection result row count overflowed usize".to_string(),
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
        projected_columns: declared_columns,
        filter_pushdown_applied: false,
        projection_pushdown_applied,
        source_order_limit,
    })
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::too_many_lines)]
fn read_local_vortex_melt_scan(
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

    let melt_projection = required_melt_projection(request)?;
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
    let mut plan = projection_scan_plan(file.dtype(), &request.projection, request.kind)?;
    plan.source_order_limit = request.source_order_limit;
    if plan.source_order_limit == Some(0) {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex melt source-order limit must be >= 1".to_string(),
        ));
    }
    let declared_columns = if plan.projected_columns.is_empty() {
        local_field_names(file.dtype(), request.kind)?
    } else {
        plan.projected_columns.clone()
    };
    let projection_pushdown_applied = plan.projection.is_some();
    let source_order_limit = plan.source_order_limit;
    let mut scan = file.scan().map_err(vortex_error)?;
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
    let mut melt_value_dtype: Option<LogicalDType> = None;
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
        validate_melt_value_dtype(&columns, melt_projection, &mut melt_value_dtype)?;
        let materialized_rows = row_export_materialized_row_count(&columns, rows)?;
        let expanded_rows = checked_melt_expanded_rows(materialized_rows, melt_projection)?;
        pre_limit_result_row_count = pre_limit_result_row_count
            .checked_add(expanded_rows)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex melt pre-limit row count overflowed usize".to_string(),
                )
            })?;
        let output_rows = source_order_limit.map_or(expanded_rows, |limit| {
            limit.saturating_sub(result_row_count).min(expanded_rows)
        });
        result_row_count = result_row_count.checked_add(output_rows).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex melt result row count overflowed usize".to_string(),
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
        projected_columns: melt_projection.output_columns(),
        filter_pushdown_applied: false,
        projection_pushdown_applied,
        source_order_limit,
    })
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::too_many_lines)]
fn read_local_vortex_pivot_scan(
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

    let pivot_projection = required_pivot_projection(request)?;
    let aggregate = normalized_pivot_aggregate(pivot_projection)?;
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
    let mut plan = projection_scan_plan(file.dtype(), &request.projection, request.kind)?;
    plan.source_order_limit = request.source_order_limit;
    if plan.source_order_limit == Some(0) {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex pivot source-order limit must be >= 1".to_string(),
        ));
    }
    let declared_columns = vec![
        pivot_projection.index_column.as_str().to_string(),
        pivot_projection.pivot_column.as_str().to_string(),
        pivot_projection.value_column.as_str().to_string(),
    ];
    let projection_pushdown_applied = plan.projection.is_some();
    let source_order_limit = plan.source_order_limit;
    let mut scan = file.scan().map_err(vortex_error)?;
    if let Some(projection) = plan.projection {
        scan = scan.with_projection(projection);
    }
    scan = scan.with_concurrency(policy.scan_concurrency_per_worker());

    let mut arrays_read_count = 0usize;
    let mut reader_splits = Vec::new();
    let mut encoded_kernel_inputs = Vec::new();
    let mut max_chunk_rows = 0usize;
    let mut index_keys = std::collections::BTreeSet::<String>::new();
    let mut pivot_columns = std::collections::BTreeMap::<String, String>::new();
    let mut first_cells = std::collections::BTreeMap::<(String, String), StatValue>::new();
    let mut aggregate_cells =
        std::collections::BTreeMap::<(String, String), PivotAggregateCell>::new();
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
        let column_values = row_export_columns_from_chunk(&chunk, &declared_columns)?;
        if column_values.len() != 3 {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex pivot requires exactly index, pivot, and value columns; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        update_pivot_state(
            pivot_projection,
            aggregate,
            &column_values[0],
            &column_values[1],
            &column_values[2],
            &mut index_keys,
            &mut pivot_columns,
            &mut first_cells,
            &mut aggregate_cells,
        )?;
        max_chunk_rows = max_chunk_rows.max(rows);
        arrays_read_count += 1;
    }
    let pre_limit_result_row_count = index_keys.len();
    let result_row_count = source_order_limit.map_or(pre_limit_result_row_count, |limit| {
        limit.min(pre_limit_result_row_count)
    });
    let mut projected_columns = Vec::with_capacity(1 + pivot_columns.len());
    projected_columns.push(pivot_projection.index_column.as_str().to_string());
    projected_columns.extend(pivot_columns.into_values());
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
        projected_columns,
        filter_pushdown_applied: false,
        projection_pushdown_applied,
        source_order_limit,
    })
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::too_many_lines)]
fn read_local_vortex_explode_scan(
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

    let explode_projection = required_explode_projection(request)?;
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
    let mut plan = projection_scan_plan(file.dtype(), &request.projection, request.kind)?;
    plan.source_order_limit = request.source_order_limit;
    if plan.source_order_limit == Some(0) {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex explode source-order limit must be >= 1".to_string(),
        ));
    }
    let declared_columns = if plan.projected_columns.is_empty() {
        local_field_names(file.dtype(), request.kind)?
    } else {
        plan.projected_columns.clone()
    };
    let output_columns = explode_projection.output_columns(&declared_columns);
    let projection_pushdown_applied = plan.projection.is_some();
    let source_order_limit = plan.source_order_limit;
    let mut scan = file.scan().map_err(vortex_error)?;
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
        let explode_columns =
            explode_columns_from_chunk(&chunk, &declared_columns, explode_projection)?;
        pre_limit_result_row_count = pre_limit_result_row_count
            .checked_add(explode_columns.expanded_rows)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex explode pre-limit row count overflowed usize".to_string(),
                )
            })?;
        let output_rows = source_order_limit.map_or(explode_columns.expanded_rows, |limit| {
            limit
                .saturating_sub(result_row_count)
                .min(explode_columns.expanded_rows)
        });
        result_row_count = result_row_count.checked_add(output_rows).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex explode result row count overflowed usize".to_string(),
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
        projected_columns: output_columns,
        filter_pushdown_applied: false,
        projection_pushdown_applied,
        source_order_limit,
    })
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::too_many_lines)]
fn read_local_vortex_rolling_window_scan(
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

    let rolling_window = required_rolling_window(request)?;
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
    let mut plan = projection_scan_plan(file.dtype(), &request.projection, request.kind)?;
    plan.source_order_limit = request.source_order_limit;
    if plan.source_order_limit == Some(0) {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex rolling source-order limit must be >= 1".to_string(),
        ));
    }
    let declared_columns = if plan.projected_columns.is_empty() {
        local_field_names(file.dtype(), request.kind)?
    } else {
        plan.projected_columns.clone()
    };
    if declared_columns.len() != 1
        || declared_columns.first().map(String::as_str)
            != Some(rolling_window.source_column.as_str())
    {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex rolling window requires exactly the declared source column projection; no fallback execution was attempted"
                .to_string(),
        ));
    }
    let projection_pushdown_applied = plan.projection.is_some();
    let source_order_limit = plan.source_order_limit;
    let mut scan = file.scan().map_err(vortex_error)?;
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
    let mut rolling_state = RollingWindowState::new(rolling_window.window_size);
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
        let materialized_rows = row_export_materialized_row_count(&columns, rows)?;
        let rows_to_process =
            if rolling_window.center {
                materialized_rows
            } else {
                source_order_limit.map_or(materialized_rows, |limit| {
                    let output_rows_needed = limit.saturating_sub(result_row_count);
                    materialized_rows.min(rolling_state.input_rows_needed_for_outputs(
                        rolling_window.min_periods,
                        output_rows_needed,
                    ))
                })
            };
        let output_values = rolling_window_values(
            &columns,
            rolling_window,
            &mut rolling_state,
            rows_to_process,
            false,
        )?;
        pre_limit_result_row_count = pre_limit_result_row_count
            .checked_add(output_values.len())
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex rolling pre-limit row count overflowed usize".to_string(),
                )
            })?;
        let output_rows = source_order_limit.map_or(output_values.len(), |limit| {
            limit
                .saturating_sub(result_row_count)
                .min(output_values.len())
        });
        result_row_count = result_row_count.checked_add(output_rows).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex rolling result row count overflowed usize".to_string(),
            )
        })?;
        max_chunk_rows = max_chunk_rows.max(rows);
        arrays_read_count += 1;
        if source_order_limit.is_some_and(|limit| result_row_count >= limit) {
            break;
        }
    }
    if rolling_window.center && source_order_limit.is_none_or(|limit| result_row_count < limit) {
        let output_values = rolling_state.emit_ready_centered(rolling_window, true)?;
        pre_limit_result_row_count = pre_limit_result_row_count
            .checked_add(output_values.len())
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex centered rolling pre-limit row count overflowed usize"
                        .to_string(),
                )
            })?;
        let output_rows = source_order_limit.map_or(output_values.len(), |limit| {
            limit
                .saturating_sub(result_row_count)
                .min(output_values.len())
        });
        result_row_count = result_row_count.checked_add(output_rows).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex centered rolling result row count overflowed usize".to_string(),
            )
        })?;
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
        projected_columns: rolling_window.output_columns(),
        filter_pushdown_applied: false,
        projection_pushdown_applied,
        source_order_limit,
    })
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::too_many_lines)]
fn read_local_vortex_simple_aggregate_scan(
    source_uri: &DatasetUri,
    path: &std::path::Path,
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<LocalVortexAggregateScan> {
    use vortex::VortexSessionDefault as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let aggregate = required_simple_aggregate(request)?;
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
    let mut projected_columns = aggregate.projected_columns();
    let residual_predicate = request
        .predicate
        .as_ref()
        .is_some_and(predicate_requires_materialized_evaluation);
    if residual_predicate && let Some(predicate) = request.predicate.as_ref() {
        append_predicate_columns(predicate, &mut projected_columns);
    }
    let projection = ProjectionRequest::columns(projected_columns);
    let mut plan = projection_scan_plan(file.dtype(), &projection, request.kind)?;
    if let Some(predicate) = request.predicate.as_ref() {
        if residual_predicate {
            plan.filter = None;
        } else {
            plan.filter = Some(predicate_to_vortex_expr(
                predicate,
                file.dtype(),
                request.kind,
            )?);
        }
    }
    let declared_columns = if plan.projected_columns.is_empty() {
        Vec::new()
    } else {
        plan.projected_columns.clone()
    };
    let filter_pushdown_applied = plan.filter.is_some();
    let projection_pushdown_applied = plan.projection.is_some();
    let aggregate_has_grouping =
        !aggregate.group_by.is_empty() || !aggregate.group_expressions.is_empty();
    let mut scalar_states = if aggregate_has_grouping {
        None
    } else {
        Some(SimpleAggregateStates::new(aggregate, &declared_columns)?)
    };
    let mut grouped_states = if aggregate_has_grouping {
        Some(GroupedAggregateStates::new(aggregate, &declared_columns)?)
    } else {
        None
    };
    let mut scan = file.scan().map_err(vortex_error)?;
    if let Some(filter) = plan.filter {
        scan = scan.with_filter(filter);
    }
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
        let materialized_rows = if columns.is_empty() {
            rows
        } else {
            row_export_materialized_row_count(&columns, rows)?
        };
        if residual_predicate {
            let predicate = request.predicate.as_ref().ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex aggregate residual predicate was missing; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
            let mut rows = Vec::new();
            for row_index in 0..materialized_rows {
                if predicate_matches_materialized_row(
                    predicate,
                    &declared_columns,
                    &columns,
                    row_index,
                )? {
                    rows.push(row_index);
                }
            }
            if let Some(states) = scalar_states.as_mut() {
                states.update_selected(&columns, &rows)?;
            }
            if let Some(states) = grouped_states.as_mut() {
                states.update_selected(&columns, &rows)?;
            }
            pre_limit_result_row_count = pre_limit_result_row_count
                .checked_add(rows.len())
                .ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex simple aggregate input row count overflowed usize"
                            .to_string(),
                    )
                })?;
        } else {
            if let Some(states) = scalar_states.as_mut() {
                states.update(&columns, materialized_rows)?;
            }
            if let Some(states) = grouped_states.as_mut() {
                states.update(&columns, materialized_rows)?;
            }
            pre_limit_result_row_count = pre_limit_result_row_count
                .checked_add(materialized_rows)
                .ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex simple aggregate input row count overflowed usize"
                            .to_string(),
                    )
                })?;
        }
        max_chunk_rows = max_chunk_rows.max(rows);
        arrays_read_count += 1;
    }
    let result_limit = request.source_order_limit;
    let (result_row_count, result_summary, state_budget) = if let Some(states) = grouped_states {
        let result_row_count = states.result_row_count(result_limit)?;
        (
            result_row_count,
            states.result_summary(result_limit)?,
            states.state_budget_report(aggregate, pre_limit_result_row_count, result_row_count)?,
        )
    } else {
        let states = scalar_states.ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex simple aggregate state was not initialized; no fallback execution was attempted"
                    .to_string(),
            )
        })?;
        let result_row_count = if aggregate.measures.is_empty() {
            0
        } else {
            states.result_row_count(&aggregate.having)?
        };
        (
            result_row_count,
            states.result_summary(&aggregate.having)?,
            states.state_budget_report(aggregate, pre_limit_result_row_count, result_row_count)?,
        )
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
    Ok(LocalVortexAggregateScan {
        scan: LocalVortexScan {
            source_row_count,
            result_row_count,
            pre_limit_result_row_count,
            arrays_read_count,
            reader_splits,
            reader_generated_prepared_batch_report,
            max_chunk_rows,
            max_parallelism_requested: policy.max_parallelism,
            scan_concurrency_per_worker: policy.scan_concurrency_per_worker(),
            projected_columns: aggregate.output_columns(),
            filter_pushdown_applied,
            projection_pushdown_applied,
            source_order_limit: result_limit,
        },
        result_summary,
        state_budget,
    })
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::too_many_lines)]
fn read_local_vortex_sort_rows_scan(
    source_uri: &DatasetUri,
    path: &std::path::Path,
    request: &VortexQueryPrimitiveRequest,
    policy: VortexLocalPrimitiveExecutionPolicy,
) -> Result<LocalVortexRowsScan> {
    use vortex::VortexSessionDefault as _;
    use vortex::file::OpenOptionsSessionExt as _;
    use vortex::io::runtime::BlockingRuntime as _;
    use vortex::io::runtime::single::SingleThreadRuntime;
    use vortex::io::session::RuntimeSessionExt as _;
    use vortex::session::VortexSession;

    let sort_rows = required_sort_rows(request)?;
    let Some(limit) = request.source_order_limit else {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex sort rows requires a bounded row count; no fallback execution was attempted"
                .to_string(),
        ));
    };
    if limit == 0 {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex sort rows limit must be >= 1; no fallback execution was attempted"
                .to_string(),
        ));
    }
    let retained_cap = limit.checked_add(sort_rows.offset).ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "local Vortex sort rows limit plus offset overflowed; no fallback execution was attempted"
                .to_string(),
        )
    })?;
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
    let output_columns = projected_column_names(file.dtype(), &request.projection, request.kind)?;
    let mut projected_columns = output_columns
        .iter()
        .map(ColumnRef::new)
        .collect::<Result<Vec<_>>>()?;
    if let Some(predicate) = request.predicate.as_ref()
        && predicate_requires_materialized_evaluation(predicate)
    {
        append_predicate_columns(predicate, &mut projected_columns);
    }
    for order in &sort_rows.order_by {
        let column = ColumnRef::new(&order.column)?;
        append_unique_column(&mut projected_columns, &column);
    }
    let projection = ProjectionRequest::columns(projected_columns);
    let mut plan = projection_scan_plan(file.dtype(), &projection, request.kind)?;
    let residual_predicate = request
        .predicate
        .as_ref()
        .is_some_and(predicate_requires_materialized_evaluation);
    if let Some(predicate) = request.predicate.as_ref() {
        if residual_predicate {
            plan.filter = None;
        } else {
            plan.filter = Some(predicate_to_vortex_expr(
                predicate,
                file.dtype(),
                request.kind,
            )?);
        }
    }
    let declared_columns = plan.projected_columns.clone();
    let filter_pushdown_applied = plan.filter.is_some();
    let projection_pushdown_applied = plan.projection.is_some();
    let mut scan = file.scan().map_err(vortex_error)?;
    if let Some(filter) = plan.filter {
        scan = scan.with_filter(filter);
    }
    if let Some(projection) = plan.projection {
        scan = scan.with_projection(projection);
    }
    scan = scan.with_concurrency(policy.scan_concurrency_per_worker());

    let mut candidates = Vec::<(usize, serde_json::Map<String, serde_json::Value>)>::new();
    let mut selected_rows = 0usize;
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
        let materialized_rows = row_export_materialized_row_count(&columns, rows)?;
        for row_index in 0..materialized_rows {
            if residual_predicate {
                let predicate = request.predicate.as_ref().ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex sort residual predicate was missing; no fallback execution was attempted"
                            .to_string(),
                    )
                })?;
                if !predicate_matches_materialized_row(
                    predicate,
                    &declared_columns,
                    &columns,
                    row_index,
                )? {
                    continue;
                }
            }
            let ordinal = selected_rows;
            selected_rows = selected_rows.checked_add(1).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex sort selected row count overflowed usize; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
            candidates.push((
                ordinal,
                materialized_row_map(&declared_columns, &columns, row_index)?,
            ));
        }
        if sort_rows.tie_policy != VortexSortTiePolicy::All
            && candidates.len()
                > retained_cap
                    .saturating_mul(2)
                    .max(retained_cap.saturating_add(1))
        {
            sort_materialized_rows(&mut candidates, &sort_rows.order_by, sort_rows.tie_policy);
            candidates.truncate(retained_cap);
        }
        max_chunk_rows = max_chunk_rows.max(rows);
        arrays_read_count += 1;
    }
    sort_materialized_rows(&mut candidates, &sort_rows.order_by, sort_rows.tie_policy);
    let selected_candidates = select_sort_rows_with_tie_policy(
        &candidates,
        &sort_rows.order_by,
        sort_rows.offset,
        limit,
        sort_rows.tie_policy,
    );
    let result_rows = selected_candidates
        .into_iter()
        .map(|(_ordinal, row)| {
            let mut out = serde_json::Map::with_capacity(output_columns.len());
            for column in &output_columns {
                out.insert(
                    column.clone(),
                    row.get(column).cloned().unwrap_or(serde_json::Value::Null),
                );
            }
            serde_json::Value::Object(out)
        })
        .collect::<Vec<_>>();
    let result_row_count = result_rows.len();
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
    let result_summary = serde_json::json!({
        "rows": result_rows.len(),
            "order_by": sort_rows
                .order_by
                .iter()
                .map(crate::VortexAggregateOrderExpr::summary)
            .collect::<Vec<_>>()
            .join(","),
        "offset": sort_rows.offset,
        "tie_policy": sort_rows.tie_policy.as_str(),
        "values": result_rows,
    })
    .to_string();
    Ok(LocalVortexRowsScan {
        scan: LocalVortexScan {
            source_row_count,
            result_row_count,
            pre_limit_result_row_count: selected_rows,
            arrays_read_count,
            reader_splits,
            reader_generated_prepared_batch_report,
            max_chunk_rows,
            max_parallelism_requested: policy.max_parallelism,
            scan_concurrency_per_worker: policy.scan_concurrency_per_worker(),
            projected_columns: output_columns,
            filter_pushdown_applied,
            projection_pushdown_applied,
            source_order_limit: Some(limit),
        },
        result_summary,
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn required_sort_rows(request: &VortexQueryPrimitiveRequest) -> Result<&VortexSortRowsRequest> {
    let sort_rows = request.sort_rows.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "local Vortex sort rows requires a typed sort payload; no fallback execution was attempted"
                .to_string(),
        )
    })?;
    if sort_rows.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex sort rows requires at least one order expression; no fallback execution was attempted"
                .to_string(),
        ));
    }
    Ok(sort_rows)
}

#[cfg(feature = "vortex-local-primitives")]
fn materialized_row_map(
    columns: &[String],
    column_values: &[Vec<StatValue>],
    row_index: usize,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    let mut row = serde_json::Map::with_capacity(columns.len());
    for (column_index, column) in columns.iter().enumerate() {
        let value = column_values
            .get(column_index)
            .and_then(|values| values.get(row_index))
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex materialized sort row had mismatched column lengths; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
        row.insert(column.clone(), stat_value_to_json_value(value)?);
    }
    Ok(row)
}

#[cfg(feature = "vortex-local-primitives")]
fn sort_materialized_rows(
    rows: &mut [(usize, serde_json::Map<String, serde_json::Value>)],
    order_by: &[crate::VortexAggregateOrderExpr],
    tie_policy: VortexSortTiePolicy,
) {
    rows.sort_by(|left, right| {
        for order in order_by {
            let left_value = left
                .1
                .get(&order.column)
                .unwrap_or(&serde_json::Value::Null);
            let right_value = right
                .1
                .get(&order.column)
                .unwrap_or(&serde_json::Value::Null);
            let ordering = compare_json_values(left_value, right_value);
            if ordering != std::cmp::Ordering::Equal {
                return if order.descending {
                    ordering.reverse()
                } else {
                    ordering
                };
            }
        }
        match tie_policy {
            VortexSortTiePolicy::Last => right.0.cmp(&left.0),
            VortexSortTiePolicy::First | VortexSortTiePolicy::All => left.0.cmp(&right.0),
        }
    });
}

#[cfg(feature = "vortex-local-primitives")]
fn select_sort_rows_with_tie_policy(
    rows: &[(usize, serde_json::Map<String, serde_json::Value>)],
    order_by: &[crate::VortexAggregateOrderExpr],
    offset: usize,
    limit: usize,
    tie_policy: VortexSortTiePolicy,
) -> Vec<(usize, serde_json::Map<String, serde_json::Value>)> {
    let start = offset.min(rows.len());
    let bounded_end = start.saturating_add(limit).min(rows.len());
    if tie_policy != VortexSortTiePolicy::All || bounded_end >= rows.len() {
        return rows[start..bounded_end].to_vec();
    }
    let cutoff = &rows[bounded_end - 1].1;
    let mut end = bounded_end;
    while end < rows.len() && sort_key_equal(&rows[end].1, cutoff, order_by) {
        end += 1;
    }
    rows[start..end].to_vec()
}

#[cfg(feature = "vortex-local-primitives")]
fn sort_key_equal(
    left: &serde_json::Map<String, serde_json::Value>,
    right: &serde_json::Map<String, serde_json::Value>,
    order_by: &[crate::VortexAggregateOrderExpr],
) -> bool {
    order_by.iter().all(|order| {
        let left_value = left.get(&order.column).unwrap_or(&serde_json::Value::Null);
        let right_value = right.get(&order.column).unwrap_or(&serde_json::Value::Null);
        compare_json_values(left_value, right_value) == std::cmp::Ordering::Equal
    })
}

#[cfg(feature = "vortex-local-primitives")]
fn required_simple_aggregate(
    request: &VortexQueryPrimitiveRequest,
) -> Result<&VortexSimpleAggregateRequest> {
    let aggregate = request.simple_aggregate.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "local Vortex simple aggregate requires a typed aggregate payload; no fallback execution was attempted"
                .to_string(),
        )
    })?;
    if aggregate.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex simple aggregate requires at least one measure; no fallback execution was attempted"
                .to_string(),
        ));
    }
    Ok(aggregate)
}

#[cfg(feature = "vortex-local-primitives")]
#[derive(Clone, Copy, PartialEq, Eq)]
enum SimpleAggregateFunction {
    Count,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
}

#[cfg(feature = "vortex-local-primitives")]
impl SimpleAggregateFunction {
    fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "count" => Ok(Self::Count),
            "count_distinct" | "count-distinct" | "distinct_count" | "distinct-count" => {
                Ok(Self::CountDistinct)
            }
            "sum" => Ok(Self::Sum),
            "avg" | "mean" => Ok(Self::Avg),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "local Vortex simple aggregate function '{other}' is not supported; expected count, count_distinct, sum, avg, min, or max; no fallback execution was attempted"
            ))),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::CountDistinct => "count_distinct",
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
struct SimpleAggregateStates {
    states: Vec<SimpleAggregateState>,
}

#[cfg(feature = "vortex-local-primitives")]
impl SimpleAggregateStates {
    fn new(request: &VortexSimpleAggregateRequest, columns: &[String]) -> Result<Self> {
        let mut aliases = std::collections::BTreeSet::new();
        let states = request
            .measures
            .iter()
            .map(|measure| {
                if measure.alias.trim().is_empty() {
                    return Err(ShardLoomError::InvalidOperation(
                        "local Vortex simple aggregate measure alias must not be empty; no fallback execution was attempted"
                            .to_string(),
                    ));
                }
                if !aliases.insert(measure.alias.clone()) {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "local Vortex simple aggregate duplicate output alias '{}'; no fallback execution was attempted",
                        measure.alias
                    )));
                }
                let function = SimpleAggregateFunction::parse(&measure.function)?;
                if measure.argument_offset.is_some()
                    && !matches!(
                        function,
                        SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg
                    )
                {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "local Vortex simple aggregate argument offset is only admitted for sum/avg measures, not {}; no fallback execution was attempted",
                        function.as_str()
                    )));
                }
                let column_index = match &measure.column {
                    Some(column) => Some(
                        columns
                            .iter()
                            .position(|value| value == column.as_str())
                            .ok_or_else(|| {
                                ShardLoomError::InvalidOperation(format!(
                                    "local Vortex simple aggregate column '{}' was not projected; no fallback execution was attempted",
                                    column.as_str()
                                ))
                            })?,
                    ),
                    None if matches!(function, SimpleAggregateFunction::Count) => None,
                    None => {
                        return Err(ShardLoomError::InvalidOperation(format!(
                            "local Vortex simple aggregate {} requires a column; no fallback execution was attempted",
                            function.as_str()
                        )));
                    }
                };
                Ok(SimpleAggregateState::new(
                    function,
                    column_index,
                    measure.alias.clone(),
                    measure.argument_offset,
                    AggregateValueTransform::from_measure_transform(
                        measure.value_transform.as_deref(),
                    )?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { states })
    }

    fn update(&mut self, columns: &[Vec<StatValue>], rows: usize) -> Result<()> {
        for state in &mut self.states {
            state.update(columns, rows)?;
        }
        Ok(())
    }

    fn update_selected(&mut self, columns: &[Vec<StatValue>], row_indices: &[usize]) -> Result<()> {
        for &row_index in row_indices {
            self.update_row(columns, row_index)?;
        }
        Ok(())
    }

    fn update_row(&mut self, columns: &[Vec<StatValue>], row_index: usize) -> Result<()> {
        for state in &mut self.states {
            state.update_row(columns, row_index)?;
        }
        Ok(())
    }

    fn functions_summary(&self) -> String {
        self.states
            .iter()
            .map(|state| state.function.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    fn result_value_pairs(&self) -> Result<Vec<(String, serde_json::Value)>> {
        let mut functions = Vec::with_capacity(self.states.len());
        self.states
            .iter()
            .map(|state| {
                functions.push(state.function.as_str());
                Ok((state.alias.clone(), state.result_json()?))
            })
            .collect()
    }

    fn result_values(&self) -> Result<serde_json::Map<String, serde_json::Value>> {
        let mut values = serde_json::Map::new();
        for (alias, value) in self.result_value_pairs()? {
            values.insert(alias, value);
        }
        Ok(values)
    }

    fn result_row_count(&self, having: &[VortexAggregateHavingExpr]) -> Result<usize> {
        let values = self.result_values()?;
        Ok(usize::from(aggregate_row_matches_having(&values, having)?))
    }

    fn result_summary(&self, having: &[VortexAggregateHavingExpr]) -> Result<String> {
        let values = self.result_values()?;
        let matched = aggregate_row_matches_having(&values, having)?;
        let payload = serde_json::json!({
            "rows": usize::from(matched),
            "functions": self.functions_summary(),
            "values": if matched { values } else { serde_json::Map::new() },
        });
        Ok(payload.to_string())
    }

    fn has_count_distinct(&self) -> bool {
        self.states
            .iter()
            .any(|state| state.function == SimpleAggregateFunction::CountDistinct)
    }

    fn count_distinct_state_entries(&self) -> Result<u64> {
        self.states
            .iter()
            .filter(|state| state.function == SimpleAggregateFunction::CountDistinct)
            .try_fold(0_u64, |total, state| {
                total
                    .checked_add(usize_to_u64(state.distinct_values.len())?)
                    .ok_or_else(|| {
                        ShardLoomError::InvalidOperation(
                            "local Vortex count-distinct state entry count overflowed u64; no fallback execution was attempted"
                                .to_string(),
                        )
                    })
            })
    }

    fn state_slot_count(&self) -> Result<u64> {
        usize_to_u64(self.states.len())
    }

    fn state_budget_report(
        &self,
        _request: &VortexSimpleAggregateRequest,
        input_rows: usize,
        result_row_count: usize,
    ) -> Result<VortexLocalPrimitiveStateBudgetReport> {
        let mut capillary_work_units = vec!["vortex_scan", "aggregate_state"];
        let mut pulseweave_pressure_signals =
            vec!["aggregate_measure_count", "aggregate_input_rows"];
        let has_count_distinct = self.has_count_distinct();
        if has_count_distinct {
            capillary_work_units.push("count_distinct_set");
            pulseweave_pressure_signals.push("distinct_value_cardinality");
        }
        let observed_state_items = self
            .state_slot_count()?
            .checked_add(self.count_distinct_state_entries()?)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex scalar aggregate observed state count overflowed u64; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
        Ok(VortexLocalPrimitiveStateBudgetReport::bounded_in_memory(
            if has_count_distinct {
                "scalar_count_distinct_state"
            } else {
                "scalar_aggregate_state"
            },
            capillary_work_units,
            pulseweave_pressure_signals,
            observed_state_items,
            Some(usize_to_u64(input_rows.max(result_row_count))?),
            "local_vortex_scalar_aggregate",
        ))
    }
}

#[cfg(feature = "vortex-local-primitives")]
struct GroupedAggregateStates<'a> {
    request: &'a VortexSimpleAggregateRequest,
    declared_columns: &'a [String],
    group_columns: Vec<AggregateGroupRuntimeColumn>,
    groups: std::collections::BTreeMap<String, GroupedAggregateState>,
}

#[cfg(feature = "vortex-local-primitives")]
struct GroupedAggregateState {
    group_values: Vec<StatValue>,
    states: SimpleAggregateStates,
}

#[cfg(feature = "vortex-local-primitives")]
#[derive(Clone)]
enum AggregateValueTransform {
    Identity,
    Length,
    ConstantInt(i64),
    AddOffset(i64),
    ExtractMinute,
    DateTruncMinute,
    UrlDomain,
    CaseSearchAdvZeroRefererElseEmpty,
}

#[cfg(feature = "vortex-local-primitives")]
impl AggregateValueTransform {
    fn from_parts(function: &str, argument_offset: Option<i64>) -> Result<Self> {
        let normalized = function.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "" | "identity" | "column" => {
                Ok(argument_offset.map_or(Self::Identity, Self::AddOffset))
            }
            "add_offset" | "offset" | "additive_offset" => {
                Ok(Self::AddOffset(argument_offset.unwrap_or(0)))
            }
            "constant_int" | "literal_int" => Ok(Self::ConstantInt(argument_offset.unwrap_or(0))),
            "length" | "byte_length" => Ok(Self::Length),
            "extract_minute" | "minute" => Ok(Self::ExtractMinute),
            "date_trunc_minute" | "truncate_minute" => Ok(Self::DateTruncMinute),
            "regex_domain" | "regex_replace_domain" | "url_domain" => Ok(Self::UrlDomain),
            "case_search_adv_zero_referer_else_empty" => {
                Ok(Self::CaseSearchAdvZeroRefererElseEmpty)
            }
            other => Err(ShardLoomError::InvalidOperation(format!(
                "local Vortex aggregate expression transform '{other}' is not admitted; no fallback execution was attempted"
            ))),
        }
    }

    fn from_expression(expression: &VortexAggregateExpression) -> Result<Self> {
        Self::from_parts(&expression.function, expression.argument_offset)
    }

    fn from_measure_transform(transform: Option<&str>) -> Result<Self> {
        transform.map_or(Ok(Self::Identity), |transform| {
            Self::from_parts(transform, None)
        })
    }

    fn apply(&self, value: &StatValue) -> Result<StatValue> {
        self.apply_values(value, &[])
    }

    fn apply_values(&self, value: &StatValue, extra_values: &[&StatValue]) -> Result<StatValue> {
        match self {
            Self::Identity => Ok(value.clone()),
            Self::Length => match value {
                StatValue::Utf8(value) => Ok(StatValue::UInt64(usize_to_u64(value.len())?)),
                other => Err(ShardLoomError::InvalidOperation(format!(
                    "local Vortex aggregate length transform requires UTF-8 input, got {}; no fallback execution was attempted",
                    other.dtype().as_str()
                ))),
            },
            Self::ConstantInt(value) => Ok(StatValue::Int64(*value)),
            Self::AddOffset(offset) => aggregate_add_offset(value, *offset),
            Self::ExtractMinute => aggregate_extract_minute(value),
            Self::DateTruncMinute => aggregate_date_trunc_minute(value),
            Self::UrlDomain => aggregate_url_domain(value),
            Self::CaseSearchAdvZeroRefererElseEmpty => {
                aggregate_case_search_adv_zero_referer_else_empty(value, extra_values)
            }
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
struct AggregateGroupRuntimeColumn {
    name: String,
    column_index: usize,
    extra_column_indices: Vec<usize>,
    transform: AggregateValueTransform,
}

#[cfg(feature = "vortex-local-primitives")]
impl<'a> GroupedAggregateStates<'a> {
    fn new(
        request: &'a VortexSimpleAggregateRequest,
        declared_columns: &'a [String],
    ) -> Result<Self> {
        let mut group_columns = request
            .group_by
            .iter()
            .map(|column| {
                let index = declared_columns
                    .iter()
                    .position(|value| value == column.as_str())
                    .ok_or_else(|| {
                        ShardLoomError::InvalidOperation(format!(
                            "local Vortex grouped aggregate column '{}' was not projected; no fallback execution was attempted",
                            column.as_str()
                        ))
                    })?;
                Ok(AggregateGroupRuntimeColumn {
                    name: column.as_str().to_string(),
                    column_index: index,
                    extra_column_indices: Vec::new(),
                    transform: AggregateValueTransform::Identity,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        for expression in &request.group_expressions {
            let index = declared_columns
                .iter()
                .position(|value| value == expression.column.as_str())
                .ok_or_else(|| {
                    ShardLoomError::InvalidOperation(format!(
                        "local Vortex grouped aggregate expression source column '{}' was not projected; no fallback execution was attempted",
                        expression.column.as_str()
                    ))
                })?;
            let extra_column_indices = expression
                .extra_columns
                .iter()
                .map(|column| {
                    declared_columns
                        .iter()
                        .position(|value| value == column.as_str())
                        .ok_or_else(|| {
                            ShardLoomError::InvalidOperation(format!(
                                "local Vortex grouped aggregate expression auxiliary column '{}' was not projected; no fallback execution was attempted",
                                column.as_str()
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            group_columns.push(AggregateGroupRuntimeColumn {
                name: expression.alias.clone(),
                column_index: index,
                extra_column_indices,
                transform: AggregateValueTransform::from_expression(expression)?,
            });
        }
        if group_columns.is_empty() {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex grouped aggregate requires at least one group column; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        let output_columns = request.output_columns();
        for order in &request.order_by {
            if !output_columns.iter().any(|column| column == &order.column) {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "local Vortex grouped aggregate order column '{}' is not a group column or aggregate alias; no fallback execution was attempted",
                    order.column
                )));
            }
        }
        Ok(Self {
            request,
            declared_columns,
            group_columns,
            groups: std::collections::BTreeMap::new(),
        })
    }

    fn update(&mut self, columns: &[Vec<StatValue>], rows: usize) -> Result<()> {
        for row_index in 0..rows {
            self.update_row(columns, row_index)?;
        }
        Ok(())
    }

    fn update_selected(&mut self, columns: &[Vec<StatValue>], row_indices: &[usize]) -> Result<()> {
        for &row_index in row_indices {
            self.update_row(columns, row_index)?;
        }
        Ok(())
    }

    fn update_row(&mut self, columns: &[Vec<StatValue>], row_index: usize) -> Result<()> {
        let group_values = self
                .group_columns
                .iter()
                .map(|group_column| {
                    let value = columns
                        .get(group_column.column_index)
                        .and_then(|values| values.get(row_index))
                        .ok_or_else(|| {
                            ShardLoomError::InvalidOperation(
                                "local Vortex grouped aggregate group column row was missing; no fallback execution was attempted"
                                    .to_string(),
                            )
                        })?;
                    let extra_values = group_column
                        .extra_column_indices
                        .iter()
                        .map(|column_index| {
                            columns
                                .get(*column_index)
                                .and_then(|values| values.get(row_index))
                                .ok_or_else(|| {
                                    ShardLoomError::InvalidOperation(
                                        "local Vortex grouped aggregate expression auxiliary column row was missing; no fallback execution was attempted"
                                            .to_string(),
                                    )
                                })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    group_column.transform.apply_values(value, &extra_values)
                })
                .collect::<Result<Vec<_>>>()?;
        let key = grouped_aggregate_key(&group_values);
        let entry = match self.groups.entry(key) {
            std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(GroupedAggregateState {
                    group_values,
                    states: SimpleAggregateStates::new(self.request, self.declared_columns)?,
                })
            }
        };
        entry.states.update_row(columns, row_index)?;
        Ok(())
    }

    fn result_row_count(&self, limit: Option<usize>) -> Result<usize> {
        let available = self
            .filtered_result_rows()?
            .len()
            .saturating_sub(self.request.offset);
        Ok(limit.map_or(available, |limit| available.min(limit)))
    }

    fn result_summary(&self, limit: Option<usize>) -> Result<String> {
        let group_by = self
            .group_columns
            .iter()
            .map(|group_column| group_column.name.as_str())
            .collect::<Vec<_>>();
        let mut rows = self.filtered_result_rows()?;
        self.sort_rows(&mut rows);
        let row_count = self.result_row_count(limit)?;
        let rows = rows
            .into_iter()
            .skip(self.request.offset)
            .take(row_count)
            .map(|(_key, row)| serde_json::Value::Object(row))
            .collect::<Vec<_>>();
        let functions = self
            .groups
            .values()
            .next()
            .map_or_else(String::new, |group| group.states.functions_summary());
        let payload = serde_json::json!({
            "rows": rows.len(),
            "group_by": group_by.join(","),
            "functions": functions,
            "order_by": self
                .request
                .order_by
                .iter()
                .map(crate::VortexAggregateOrderExpr::summary)
                .collect::<Vec<_>>()
                .join(","),
            "offset": self.request.offset,
            "values": rows,
        });
        Ok(payload.to_string())
    }

    fn filtered_result_rows(
        &self,
    ) -> Result<Vec<(String, serde_json::Map<String, serde_json::Value>)>> {
        let rows = self.result_rows()?;
        rows.into_iter()
            .filter_map(
                |row| match aggregate_row_matches_having(&row.1, &self.request.having) {
                    Ok(true) => Some(Ok(row)),
                    Ok(false) => None,
                    Err(error) => Some(Err(error)),
                },
            )
            .collect()
    }

    fn result_rows(&self) -> Result<Vec<(String, serde_json::Map<String, serde_json::Value>)>> {
        let mut rows = Vec::with_capacity(self.groups.len());
        for (key, group) in &self.groups {
            let mut row = serde_json::Map::new();
            for (group_column, value) in self.group_columns.iter().zip(&group.group_values) {
                row.insert(group_column.name.clone(), stat_value_to_json_value(value)?);
            }
            for (alias, value) in group.states.result_value_pairs()? {
                row.insert(alias, value);
            }
            rows.push((key.clone(), row));
        }
        Ok(rows)
    }

    fn sort_rows(&self, rows: &mut [(String, serde_json::Map<String, serde_json::Value>)]) {
        if self.request.order_by.is_empty() {
            return;
        }
        rows.sort_by(|left, right| {
            for order in &self.request.order_by {
                let left_value = left
                    .1
                    .get(&order.column)
                    .unwrap_or(&serde_json::Value::Null);
                let right_value = right
                    .1
                    .get(&order.column)
                    .unwrap_or(&serde_json::Value::Null);
                let ordering = compare_json_values(left_value, right_value);
                if ordering != std::cmp::Ordering::Equal {
                    return if order.descending {
                        ordering.reverse()
                    } else {
                        ordering
                    };
                }
            }
            left.0.cmp(&right.0)
        });
    }

    fn count_distinct_state_entries(&self) -> Result<u64> {
        self.groups.values().try_fold(0_u64, |total, group| {
            total
                .checked_add(group.states.count_distinct_state_entries()?)
                .ok_or_else(|| {
                    ShardLoomError::InvalidOperation(
                        "local Vortex grouped count-distinct state entry count overflowed u64; no fallback execution was attempted"
                            .to_string(),
                    )
                })
        })
    }

    fn state_budget_report(
        &self,
        request: &VortexSimpleAggregateRequest,
        input_rows: usize,
        result_row_count: usize,
    ) -> Result<VortexLocalPrimitiveStateBudgetReport> {
        let has_count_distinct = request.measures.iter().any(|measure| {
            matches!(
                SimpleAggregateFunction::parse(&measure.function),
                Ok(SimpleAggregateFunction::CountDistinct)
            )
        });
        let has_grouped_topk = !request.order_by.is_empty() || result_row_count < self.groups.len();
        let has_offset = request.offset > 0;
        let mut state_family = "grouped_aggregate_state".to_string();
        if has_count_distinct {
            state_family.push_str("+count_distinct");
        }
        if has_grouped_topk {
            state_family.push_str("+topk");
        }
        if has_offset {
            state_family.push_str("+offset");
        }
        let mut capillary_work_units = vec!["vortex_scan", "group_key_state", "aggregate_state"];
        let mut pulseweave_pressure_signals = vec![
            "group_cardinality",
            "aggregate_input_rows",
            "group_state_rows",
        ];
        if has_count_distinct {
            capillary_work_units.push("count_distinct_set");
            pulseweave_pressure_signals.push("distinct_value_cardinality");
        }
        if has_grouped_topk {
            capillary_work_units.push("grouped_topk_heap");
            pulseweave_pressure_signals.push("topk_heap_rows");
        }
        if has_offset {
            capillary_work_units.push("offset_drain");
            pulseweave_pressure_signals.push("offset_drain_rows");
        }
        if !request.having.is_empty() {
            capillary_work_units.push("having_filter");
            pulseweave_pressure_signals.push("having_selectivity");
        }
        let observed_state_items = usize_to_u64(self.groups.len())?
            .checked_add(self.count_distinct_state_entries()?)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex grouped aggregate observed state count overflowed u64; no fallback execution was attempted"
                        .to_string(),
                )
            })?;
        Ok(VortexLocalPrimitiveStateBudgetReport::bounded_in_memory(
            state_family,
            capillary_work_units,
            pulseweave_pressure_signals,
            observed_state_items,
            Some(usize_to_u64(input_rows.max(result_row_count))?),
            "local_vortex_grouped_aggregate",
        ))
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn compare_json_values(left: &serde_json::Value, right: &serde_json::Value) -> std::cmp::Ordering {
    use serde_json::Value;
    match (left, right) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::Number(left), Value::Number(right)) => compare_json_numbers(left, right),
        (Value::String(left), Value::String(right)) => left.cmp(right),
        _ => json_type_rank(left).cmp(&json_type_rank(right)),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn compare_json_numbers(
    left: &serde_json::Number,
    right: &serde_json::Number,
) -> std::cmp::Ordering {
    if let Some(ordering) = match (left.as_i64(), left.as_u64(), right.as_i64(), right.as_u64()) {
        (Some(left), _, Some(right), _) => Some(left.cmp(&right)),
        (_, Some(left), _, Some(right)) => Some(left.cmp(&right)),
        (Some(left), _, _, Some(right)) => {
            Some(u64::try_from(left).map_or(std::cmp::Ordering::Less, |left| left.cmp(&right)))
        }
        (_, Some(left), Some(right), _) => {
            Some(u64::try_from(right).map_or(std::cmp::Ordering::Greater, |right| left.cmp(&right)))
        }
        _ => None,
    } {
        return ordering;
    }
    left.as_f64()
        .and_then(|left| right.as_f64().and_then(|right| left.partial_cmp(&right)))
        .unwrap_or(std::cmp::Ordering::Equal)
}

#[cfg(feature = "vortex-local-primitives")]
fn json_type_rank(value: &serde_json::Value) -> u8 {
    match value {
        serde_json::Value::Null => 0,
        serde_json::Value::Bool(_) => 1,
        serde_json::Value::Number(_) => 2,
        serde_json::Value::String(_) => 3,
        serde_json::Value::Array(_) => 4,
        serde_json::Value::Object(_) => 5,
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn grouped_aggregate_key(values: &[StatValue]) -> String {
    values
        .iter()
        .map(stat_value_group_key)
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

#[cfg(feature = "vortex-local-primitives")]
fn stat_value_group_key(value: &StatValue) -> String {
    match value {
        StatValue::Null => "n:null".to_string(),
        StatValue::Boolean(value) => format!("b:{value}"),
        StatValue::Int64(value) => format!("i:{value}"),
        StatValue::UInt64(value) => format!("u:{value}"),
        StatValue::Float64(value) => format!("f:{:016x}", value.to_bits()),
        StatValue::Utf8(value) => format!("s:{}", value.replace('\u{1f}', "\\u001f")),
    }
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::cast_precision_loss)]
fn aggregate_add_offset(value: &StatValue, offset: i64) -> Result<StatValue> {
    match value {
        StatValue::Int64(value) => value
            .checked_add(offset)
            .map(StatValue::Int64)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex aggregate additive group expression overflowed int64; no fallback execution was attempted"
                        .to_string(),
                )
            }),
        StatValue::UInt64(value) if offset >= 0 => value
            .checked_add(offset.cast_unsigned())
            .map(StatValue::UInt64)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex aggregate additive group expression overflowed uint64; no fallback execution was attempted"
                        .to_string(),
                )
            }),
        StatValue::UInt64(value) => value
            .checked_sub(offset.unsigned_abs())
            .map(StatValue::UInt64)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex aggregate additive group expression underflowed uint64; no fallback execution was attempted"
                        .to_string(),
                )
            }),
        StatValue::Float64(value) => {
            let adjusted = *value + offset as f64;
            if adjusted.is_finite() {
                Ok(StatValue::Float64(adjusted))
            } else {
                Err(ShardLoomError::InvalidOperation(
                    "local Vortex aggregate additive group expression produced non-finite float64; no fallback execution was attempted"
                        .to_string(),
                ))
            }
        }
        other => Err(ShardLoomError::InvalidOperation(format!(
            "local Vortex aggregate additive group expression requires numeric input, got {}; no fallback execution was attempted",
            other.dtype().as_str()
        ))),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn aggregate_extract_minute(value: &StatValue) -> Result<StatValue> {
    match value {
        StatValue::Int64(value) => Ok(StatValue::Int64(value.rem_euclid(3600) / 60)),
        StatValue::UInt64(value) => Ok(StatValue::UInt64((value % 3600) / 60)),
        StatValue::Utf8(value) => parse_timestamp_minute(value)
            .map(|minute| StatValue::UInt64(u64::from(minute)))
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex aggregate extract-minute expression requires a parseable timestamp string; no fallback execution was attempted"
                        .to_string(),
                )
            }),
        other => Err(ShardLoomError::InvalidOperation(format!(
            "local Vortex aggregate extract-minute expression requires timestamp-compatible input, got {}; no fallback execution was attempted",
            other.dtype().as_str()
        ))),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn aggregate_date_trunc_minute(value: &StatValue) -> Result<StatValue> {
    match value {
        StatValue::Int64(value) => Ok(StatValue::Int64(value.div_euclid(60) * 60)),
        StatValue::UInt64(value) => Ok(StatValue::UInt64((value / 60) * 60)),
        StatValue::Utf8(value) => truncate_timestamp_string_to_minute(value)
            .map(StatValue::Utf8)
            .ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex aggregate date-trunc-minute expression requires a parseable timestamp string; no fallback execution was attempted"
                        .to_string(),
                )
            }),
        other => Err(ShardLoomError::InvalidOperation(format!(
            "local Vortex aggregate date-trunc-minute expression requires timestamp-compatible input, got {}; no fallback execution was attempted",
            other.dtype().as_str()
        ))),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn parse_timestamp_minute(value: &str) -> Option<u8> {
    let time = value.split_once('T').map_or_else(
        || value.split_whitespace().nth(1),
        |(_date, time)| Some(time),
    )?;
    let minute = time.split(':').nth(1)?.parse::<u8>().ok()?;
    (minute < 60).then_some(minute)
}

#[cfg(feature = "vortex-local-primitives")]
fn truncate_timestamp_string_to_minute(value: &str) -> Option<String> {
    let second_colon = value.match_indices(':').nth(1)?.0;
    let mut out = value[..second_colon].to_string();
    out.push_str(":00");
    Some(out)
}

#[cfg(feature = "vortex-local-primitives")]
fn aggregate_url_domain(value: &StatValue) -> Result<StatValue> {
    let StatValue::Utf8(value) = value else {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex aggregate URL-domain expression requires UTF-8 input; no fallback execution was attempted"
                .to_string(),
        ));
    };
    let without_scheme = value
        .strip_prefix("https://")
        .or_else(|| value.strip_prefix("http://"))
        .unwrap_or(value);
    let without_www = without_scheme
        .strip_prefix("www.")
        .unwrap_or(without_scheme);
    let domain = without_www.split('/').next().unwrap_or_default();
    Ok(StatValue::Utf8(domain.to_string()))
}

#[cfg(feature = "vortex-local-primitives")]
fn aggregate_case_search_adv_zero_referer_else_empty(
    referer: &StatValue,
    extra_values: &[&StatValue],
) -> Result<StatValue> {
    if extra_values.len() != 2 {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex aggregate CASE search/advertising expression requires SearchEngineID and AdvEngineID auxiliary columns; no fallback execution was attempted"
                .to_string(),
        ));
    }
    let search_engine_id = stat_value_to_i64(extra_values[0])?;
    let adv_engine_id = stat_value_to_i64(extra_values[1])?;
    if search_engine_id == 0 && adv_engine_id == 0 {
        match referer {
            StatValue::Utf8(value) => Ok(StatValue::Utf8(value.clone())),
            other => Err(ShardLoomError::InvalidOperation(format!(
                "local Vortex aggregate CASE referer expression requires UTF-8 referer input, got {}; no fallback execution was attempted",
                other.dtype().as_str()
            ))),
        }
    } else {
        Ok(StatValue::Utf8(String::new()))
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn aggregate_row_matches_having(
    row: &serde_json::Map<String, serde_json::Value>,
    having: &[VortexAggregateHavingExpr],
) -> Result<bool> {
    for expression in having {
        let Some(left) = row.get(&expression.column) else {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local Vortex aggregate HAVING column '{}' was not present in the result row; no fallback execution was attempted",
                expression.column
            )));
        };
        let right = aggregate_having_value_json(&expression.value);
        let ordering = compare_json_values(left, &right);
        let matches = match expression.op {
            ComparisonOp::Eq => ordering == std::cmp::Ordering::Equal,
            ComparisonOp::NotEq => ordering != std::cmp::Ordering::Equal,
            ComparisonOp::Lt => ordering == std::cmp::Ordering::Less,
            ComparisonOp::LtEq => {
                matches!(
                    ordering,
                    std::cmp::Ordering::Less | std::cmp::Ordering::Equal
                )
            }
            ComparisonOp::Gt => ordering == std::cmp::Ordering::Greater,
            ComparisonOp::GtEq => {
                matches!(
                    ordering,
                    std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
                )
            }
        };
        if !matches {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(feature = "vortex-local-primitives")]
fn aggregate_having_value_json(value: &str) -> serde_json::Value {
    if let Ok(value) = value.parse::<i64>() {
        return serde_json::Value::Number(value.into());
    }
    if let Ok(value) = value.parse::<u64>() {
        return serde_json::Value::Number(value.into());
    }
    if let Ok(value) = value.parse::<f64>()
        && let Some(number) = serde_json::Number::from_f64(value)
    {
        return serde_json::Value::Number(number);
    }
    match value {
        "true" => serde_json::Value::Bool(true),
        "false" => serde_json::Value::Bool(false),
        _ => serde_json::Value::String(value.to_string()),
    }
}

#[cfg(feature = "vortex-local-primitives")]
struct SimpleAggregateState {
    function: SimpleAggregateFunction,
    column_index: Option<usize>,
    alias: String,
    count: u64,
    sum: f64,
    min: Option<StatValue>,
    max: Option<StatValue>,
    distinct_values: std::collections::BTreeSet<String>,
    argument_offset: Option<i64>,
    value_transform: AggregateValueTransform,
}

#[cfg(feature = "vortex-local-primitives")]
impl SimpleAggregateState {
    fn new(
        function: SimpleAggregateFunction,
        column_index: Option<usize>,
        alias: String,
        argument_offset: Option<i64>,
        value_transform: AggregateValueTransform,
    ) -> Self {
        Self {
            function,
            column_index,
            alias,
            count: 0,
            sum: 0.0,
            min: None,
            max: None,
            distinct_values: std::collections::BTreeSet::new(),
            argument_offset,
            value_transform,
        }
    }

    fn update(&mut self, columns: &[Vec<StatValue>], rows: usize) -> Result<()> {
        let Some(column_index) = self.column_index else {
            self.count = self.count.checked_add(usize_to_u64(rows)?).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex simple aggregate count overflowed u64".to_string(),
                )
            })?;
            return Ok(());
        };
        let values = columns.get(column_index).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex simple aggregate column index was missing; no fallback execution was attempted"
                    .to_string(),
            )
        })?;
        if rows > values.len() {
            return Err(ShardLoomError::InvalidOperation(
                "local Vortex simple aggregate requested more rows than the materialized chunk contains; no fallback execution was attempted"
                    .to_string(),
            ));
        }
        for value in values.iter().take(rows) {
            let transformed_value = self.value_transform.apply(value)?;
            self.count = self.count.checked_add(1).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex simple aggregate count overflowed u64".to_string(),
                )
            })?;
            match self.function {
                SimpleAggregateFunction::Count => {}
                SimpleAggregateFunction::CountDistinct => {
                    self.distinct_values
                        .insert(stat_value_group_key(&transformed_value));
                }
                SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg => {
                    let numeric = self.aggregate_numeric_value(&transformed_value)?;
                    if !numeric.is_finite() {
                        return Err(ShardLoomError::InvalidOperation(
                            "local Vortex simple aggregate encountered non-finite numeric value; no fallback execution was attempted"
                                .to_string(),
                        ));
                    }
                    self.sum += numeric;
                }
                SimpleAggregateFunction::Min => {
                    self.min = Some(simple_aggregate_min_value(
                        self.min.take(),
                        &transformed_value,
                    )?);
                }
                SimpleAggregateFunction::Max => {
                    self.max = Some(simple_aggregate_max_value(
                        self.max.take(),
                        &transformed_value,
                    )?);
                }
            }
        }
        Ok(())
    }

    fn update_row(&mut self, columns: &[Vec<StatValue>], row_index: usize) -> Result<()> {
        let Some(column_index) = self.column_index else {
            self.count = self.count.checked_add(1).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local Vortex simple aggregate count overflowed u64".to_string(),
                )
            })?;
            return Ok(());
        };
        let values = columns.get(column_index).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex simple aggregate column index was missing; no fallback execution was attempted"
                    .to_string(),
            )
        })?;
        let value = values.get(row_index).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex grouped aggregate row index exceeded materialized chunk rows; no fallback execution was attempted"
                    .to_string(),
            )
        })?;
        let transformed_value = self.value_transform.apply(value)?;
        self.count = self.count.checked_add(1).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex simple aggregate count overflowed u64".to_string(),
            )
        })?;
        match self.function {
            SimpleAggregateFunction::Count => {}
            SimpleAggregateFunction::CountDistinct => {
                self.distinct_values
                    .insert(stat_value_group_key(&transformed_value));
            }
            SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg => {
                let numeric = self.aggregate_numeric_value(&transformed_value)?;
                if !numeric.is_finite() {
                    return Err(ShardLoomError::InvalidOperation(
                        "local Vortex simple aggregate encountered non-finite numeric value; no fallback execution was attempted"
                            .to_string(),
                    ));
                }
                self.sum += numeric;
            }
            SimpleAggregateFunction::Min => {
                self.min = Some(simple_aggregate_min_value(
                    self.min.take(),
                    &transformed_value,
                )?);
            }
            SimpleAggregateFunction::Max => {
                self.max = Some(simple_aggregate_max_value(
                    self.max.take(),
                    &transformed_value,
                )?);
            }
        }
        Ok(())
    }

    fn result_json(&self) -> Result<serde_json::Value> {
        match self.function {
            SimpleAggregateFunction::Count => Ok(serde_json::Value::Number(self.count.into())),
            SimpleAggregateFunction::CountDistinct => Ok(serde_json::Value::Number(
                usize_to_u64(self.distinct_values.len())?.into(),
            )),
            SimpleAggregateFunction::Sum => json_number_from_f64(self.sum),
            SimpleAggregateFunction::Avg => {
                if self.count == 0 {
                    Ok(serde_json::Value::Null)
                } else {
                    json_number_from_f64(simple_average_value(self.sum, self.count))
                }
            }
            SimpleAggregateFunction::Min => self
                .min
                .as_ref()
                .map_or(Ok(serde_json::Value::Null), stat_value_to_json_value),
            SimpleAggregateFunction::Max => self
                .max
                .as_ref()
                .map_or(Ok(serde_json::Value::Null), stat_value_to_json_value),
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn aggregate_numeric_value(&self, value: &StatValue) -> Result<f64> {
        let numeric = stat_value_to_f64(value)?;
        self.argument_offset.map_or(Ok(numeric), |offset| {
            let adjusted = numeric + offset as f64;
            if adjusted.is_finite() {
                Ok(adjusted)
            } else {
                Err(ShardLoomError::InvalidOperation(
                    "local Vortex simple aggregate argument offset produced a non-finite value; no fallback execution was attempted"
                        .to_string(),
                ))
            }
        })
    }
}

#[cfg(feature = "vortex-local-primitives")]
#[allow(clippy::cast_precision_loss)]
fn simple_average_value(sum: f64, count: u64) -> f64 {
    sum / count as f64
}

#[cfg(feature = "vortex-local-primitives")]
fn simple_aggregate_min_value(current: Option<StatValue>, value: &StatValue) -> Result<StatValue> {
    let Some(current) = current else {
        return Ok(value.clone());
    };
    match stat_value_cmp(value, &current) {
        Some(std::cmp::Ordering::Less) => Ok(value.clone()),
        Some(_) => Ok(current),
        None => Err(ShardLoomError::InvalidOperation(
            "local Vortex simple aggregate min encountered incomparable values; no fallback execution was attempted"
                .to_string(),
        )),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn simple_aggregate_max_value(current: Option<StatValue>, value: &StatValue) -> Result<StatValue> {
    let Some(current) = current else {
        return Ok(value.clone());
    };
    match stat_value_cmp(value, &current) {
        Some(std::cmp::Ordering::Greater) => Ok(value.clone()),
        Some(_) => Ok(current),
        None => Err(ShardLoomError::InvalidOperation(
            "local Vortex simple aggregate max encountered incomparable values; no fallback execution was attempted"
                .to_string(),
        )),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn json_number_from_f64(value: f64) -> Result<serde_json::Value> {
    serde_json::Number::from_f64(value)
        .map(serde_json::Value::Number)
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex simple aggregate cannot serialize non-finite numeric result; no fallback execution was attempted"
                    .to_string(),
            )
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
fn sample_scan_plan(
    dtype: &vortex::array::dtype::DType,
    request: &VortexQueryPrimitiveRequest,
) -> Result<LocalVortexScanPlan> {
    let mut projected_columns = sample_output_columns(dtype, request)?
        .iter()
        .map(ColumnRef::new)
        .collect::<Result<Vec<_>>>()?;
    if let Some(column) = request.sample_weight_column.as_ref() {
        append_unique_column(&mut projected_columns, column);
    }
    let projection = ProjectionRequest::columns(projected_columns);
    let mut plan = projection_scan_plan(dtype, &projection, request.kind)?;
    if let Some(predicate) = request.predicate.as_ref() {
        plan.filter = Some(predicate_to_vortex_expr(predicate, dtype, request.kind)?);
    }
    plan.source_order_limit = request.source_order_limit;
    Ok(plan)
}

#[cfg(feature = "vortex-local-primitives")]
fn sample_output_columns(
    dtype: &vortex::array::dtype::DType,
    request: &VortexQueryPrimitiveRequest,
) -> Result<Vec<String>> {
    projected_column_names(dtype, &request.projection, request.kind)
}

#[cfg(feature = "vortex-local-primitives")]
fn drop_duplicate_output_columns(
    dtype: &vortex::array::dtype::DType,
    request: &VortexQueryPrimitiveRequest,
) -> Result<Vec<String>> {
    projected_column_names(dtype, &request.projection, request.kind)
}

#[cfg(feature = "vortex-local-primitives")]
fn drop_duplicate_key_columns(
    dtype: &vortex::array::dtype::DType,
    request: &VortexQueryPrimitiveRequest,
) -> Result<Vec<String>> {
    let key_projection = request.deduplicate_key_projection.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "local Vortex drop_duplicates requires explicit row-key columns; no fallback execution was attempted"
                .to_string(),
        )
    })?;
    let key_columns = projected_column_names(dtype, key_projection, request.kind)?;
    if key_columns.is_empty() {
        return Err(ShardLoomError::InvalidOperation(
            "local Vortex drop_duplicates requires at least one row-key column; no fallback execution was attempted"
                .to_string(),
        ));
    }
    Ok(key_columns)
}

#[cfg(feature = "vortex-local-primitives")]
fn drop_duplicate_scan_plan(
    dtype: &vortex::array::dtype::DType,
    request: &VortexQueryPrimitiveRequest,
) -> Result<LocalVortexScanPlan> {
    let output_columns = drop_duplicate_output_columns(dtype, request)?;
    let key_columns = drop_duplicate_key_columns(dtype, request)?;
    let mut projected_columns = output_columns
        .iter()
        .map(ColumnRef::new)
        .collect::<Result<Vec<_>>>()?;
    for column in &key_columns {
        append_unique_column(&mut projected_columns, &ColumnRef::new(column)?);
    }
    let projection = ProjectionRequest::columns(projected_columns);
    let mut plan = projection_scan_plan(dtype, &projection, request.kind)?;
    if let Some(predicate) = request.predicate.as_ref() {
        plan.filter = Some(predicate_to_vortex_expr(predicate, dtype, request.kind)?);
    }
    plan.source_order_limit = request.source_order_limit;
    Ok(plan)
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
    use vortex::array::expr::{
        and_collect, eq, gt, gt_eq, is_not_null, is_null, lit, lt, lt_eq, not_eq,
    };

    match predicate {
        PredicateExpr::AlwaysTrue => Ok(lit(true)),
        PredicateExpr::AlwaysFalse => Ok(lit(false)),
        PredicateExpr::And(predicates) => {
            if predicates.is_empty() {
                return Ok(lit(true));
            }
            let expressions = predicates
                .iter()
                .map(|predicate| predicate_to_vortex_expr(predicate, dtype, primitive_kind))
                .collect::<Result<Vec<_>>>()?;
            and_collect(expressions).ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "local primitive conjunction produced no predicate expressions; no fallback execution was attempted"
                        .to_string(),
                )
            })
        }
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
        PredicateExpr::StringContains { .. } | PredicateExpr::InList { .. } => {
            Err(ShardLoomError::InvalidOperation(format!(
                "local primitive {} predicate requires ShardLoom materialized predicate evaluation; no fallback execution was attempted",
                primitive_kind.as_str()
            )))
        }
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn predicate_requires_materialized_evaluation(predicate: &PredicateExpr) -> bool {
    match predicate {
        PredicateExpr::AlwaysTrue
        | PredicateExpr::AlwaysFalse
        | PredicateExpr::IsNull { .. }
        | PredicateExpr::IsNotNull { .. }
        | PredicateExpr::Compare { .. } => false,
        PredicateExpr::StringContains { .. } | PredicateExpr::InList { .. } => true,
        PredicateExpr::And(predicates) => predicates
            .iter()
            .any(predicate_requires_materialized_evaluation),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn append_predicate_columns(predicate: &PredicateExpr, out: &mut Vec<ColumnRef>) {
    match predicate {
        PredicateExpr::AlwaysTrue | PredicateExpr::AlwaysFalse => {}
        PredicateExpr::And(predicates) => {
            for predicate in predicates {
                append_predicate_columns(predicate, out);
            }
        }
        PredicateExpr::IsNull { column }
        | PredicateExpr::IsNotNull { column }
        | PredicateExpr::Compare { column, .. }
        | PredicateExpr::StringContains { column, .. }
        | PredicateExpr::InList { column, .. } => append_unique_column(out, column),
    }
}

#[cfg(feature = "vortex-local-primitives")]
fn append_unique_column(out: &mut Vec<ColumnRef>, column: &ColumnRef) {
    if !out
        .iter()
        .any(|existing| existing.as_str() == column.as_str())
    {
        out.push(column.clone());
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
    #[cfg(feature = "universal-format-io")]
    use crate::VortexStructuredProjectionColumn;
    use crate::{
        VortexEncodedValuePredicateBatch, VortexExplodeProjectionRequest,
        VortexExpressionProjectionRequest, VortexExpressionRewrite,
        VortexReaderBackedEncodedExecutionStatus, VortexReaderGeneratedPreparedBatchStatus,
        VortexSourceBackedEncodedValuePredicateBatch,
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

    fn write_melt_struct_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{PrimitiveArray, StructArray};
        use vortex::array::dtype::FieldNames;
        use vortex::array::validity::Validity;

        let array = StructArray::try_new(
            FieldNames::from(["id", "amount_a", "amount_b"]),
            vec![
                [1_u32, 2, 3]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
                [10_i64, 20, 30]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
                [100_i64, 200, 300]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .map_err(vortex_error)?;
        write_array(path, &array.into_array())
    }

    fn write_melt_mixed_struct_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
        use vortex::array::dtype::FieldNames;
        use vortex::array::validity::Validity;

        let array = StructArray::try_new(
            FieldNames::from(["id", "amount", "label"]),
            vec![
                [1_u32, 2]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
                [10_i64, 20]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
                VarBinViewArray::from_iter_str(["paid", "trial"]).into_array(),
            ],
            2,
            Validity::NonNullable,
        )
        .map_err(vortex_error)?;
        write_array(path, &array.into_array())
    }

    fn write_pivot_struct_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{PrimitiveArray, StructArray, VarBinViewArray};
        use vortex::array::dtype::FieldNames;
        use vortex::array::validity::Validity;

        let array = StructArray::try_new(
            FieldNames::from(["id", "label", "amount"]),
            vec![
                [1_u32, 1, 2]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
                VarBinViewArray::from_iter_str(["paid", "paid", "trial"]).into_array(),
                [10_i64, 5, 7]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .map_err(vortex_error)?;
        write_array(path, &array.into_array())
    }

    fn write_large_int_sort_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{PrimitiveArray, StructArray};
        use vortex::array::dtype::FieldNames;
        use vortex::array::validity::Validity;

        let array = StructArray::try_new(
            FieldNames::from(["id", "large"]),
            vec![
                [1_u32, 2]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
                [9_007_199_254_740_993_i64, 9_007_199_254_740_992_i64]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
            ],
            2,
            Validity::NonNullable,
        )
        .map_err(vortex_error)?;
        write_array(path, &array.into_array())
    }

    fn write_explode_struct_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{ListViewArray, PrimitiveArray, StructArray};
        use vortex::array::dtype::FieldNames;
        use vortex::array::validity::Validity;

        let elements = [7_i64, 8, 9]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let offsets = [0_u32, 2, 2]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let sizes = [2_u32, 0, 1]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let items =
            ListViewArray::new(elements, offsets, sizes, Validity::NonNullable).into_array();
        let array = StructArray::try_new(
            FieldNames::from(["id", "items"]),
            vec![
                [1_u32, 2, 3]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
                items,
            ],
            3,
            Validity::NonNullable,
        )
        .map_err(vortex_error)?;
        write_array(path, &array.into_array())
    }

    fn write_multi_explode_struct_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{ListViewArray, PrimitiveArray, StructArray, VarBinViewArray};
        use vortex::array::dtype::FieldNames;
        use vortex::array::validity::Validity;

        let item_elements = [7_i64, 8, 9]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let label_elements = VarBinViewArray::from_iter_str(["red", "blue", "green"]).into_array();
        let item_offsets = [0_u32, 2, 2]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let item_sizes = [2_u32, 0, 1]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let label_offsets = [0_u32, 2, 2]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let label_sizes = [2_u32, 0, 1]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let items = ListViewArray::new(
            item_elements,
            item_offsets,
            item_sizes,
            Validity::NonNullable,
        )
        .into_array();
        let labels = ListViewArray::new(
            label_elements,
            label_offsets,
            label_sizes,
            Validity::NonNullable,
        )
        .into_array();
        let array = StructArray::try_new(
            FieldNames::from(["id", "items", "labels"]),
            vec![
                [1_u32, 2, 3]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
                items,
                labels,
            ],
            3,
            Validity::NonNullable,
        )
        .map_err(vortex_error)?;
        write_array(path, &array.into_array())
    }

    fn write_nullable_nested_explode_struct_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{ListViewArray, PrimitiveArray, StructArray, VarBinViewArray};
        use vortex::array::dtype::FieldNames;
        use vortex::array::validity::Validity;

        let element_struct = StructArray::try_new(
            FieldNames::from(["code", "label"]),
            vec![
                [7_i64, 8, 9]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
                VarBinViewArray::from_iter_str(["red", "blue", "green"]).into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .map_err(vortex_error)?
        .into_array();
        let offsets = [0_u32, 2, 2]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let sizes = [2_u32, 1, 1]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let items = ListViewArray::new(
            element_struct,
            offsets,
            sizes,
            Validity::from_iter([true, false, true]),
        )
        .into_array();
        let array = StructArray::try_new(
            FieldNames::from(["id", "items"]),
            vec![
                [1_u32, 2, 3]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
                items,
            ],
            3,
            Validity::NonNullable,
        )
        .map_err(vortex_error)?;
        write_array(path, &array.into_array())
    }

    fn write_nested_row_key_struct_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{ListViewArray, PrimitiveArray, StructArray, VarBinViewArray};
        use vortex::array::dtype::FieldNames;
        use vortex::array::validity::Validity;

        let elements = [7_i64, 8, 7, 8, 9]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let offsets = [0_u32, 2, 4]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let sizes = [2_u32, 2, 1]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let items =
            ListViewArray::new(elements, offsets, sizes, Validity::NonNullable).into_array();
        let payload = StructArray::try_new(
            FieldNames::from(["code", "label"]),
            vec![
                [1_u32, 1, 2]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
                VarBinViewArray::from_iter_str(["alpha", "alpha", "beta"]).into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .map_err(vortex_error)?
        .into_array();
        let array = StructArray::try_new(
            FieldNames::from(["id", "items", "payload", "metric"]),
            vec![
                [1_u32, 2, 3]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
                items,
                payload,
                [10_i64, 20, 30]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .map_err(vortex_error)?;
        write_array(path, &array.into_array())
    }

    fn write_nullable_parent_nested_row_key_struct_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{ListViewArray, PrimitiveArray, StructArray};
        use vortex::array::dtype::FieldNames;
        use vortex::array::validity::Validity;

        let nullable_elements = PrimitiveArray::new(
            vec![0_i64, 0, 0],
            Validity::from_iter([false, false, false]),
        )
        .into_array();
        let offsets = [0_u32, 1, 2]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let sizes = [1_u32, 1, 1]
            .into_iter()
            .collect::<PrimitiveArray>()
            .into_array();
        let items = ListViewArray::new(
            nullable_elements,
            offsets,
            sizes,
            Validity::from_iter([true, false, true]),
        )
        .into_array();
        let payload = StructArray::try_new(
            FieldNames::from(["code"]),
            vec![
                [9_u32, 9, 9]
                    .into_iter()
                    .collect::<PrimitiveArray>()
                    .into_array(),
            ],
            3,
            Validity::from_iter([true, false, true]),
        )
        .map_err(vortex_error)?
        .into_array();
        let array = StructArray::try_new(
            FieldNames::from(["items", "payload"]),
            vec![items, payload],
            3,
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

    fn write_sort_tie_struct_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{PrimitiveArray, StructArray};
        use vortex::array::dtype::FieldNames;
        use vortex::array::validity::Validity;

        let array = StructArray::try_new(
            FieldNames::from(["key", "metric"]),
            vec![
                [1_i64, 1, 2, 2, 3]
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

    fn write_string_struct_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{StructArray, VarBinViewArray};
        use vortex::array::dtype::FieldNames;
        use vortex::array::validity::Validity;

        let array = StructArray::try_new(
            FieldNames::from(["label"]),
            vec![VarBinViewArray::from_iter_str(["bad", "good", "badly"]).into_array()],
            3,
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

    fn write_nullable_duplicate_struct_fixture(path: &std::path::Path) -> Result<()> {
        use vortex::array::IntoArray as _;
        use vortex::array::arrays::{PrimitiveArray, StructArray};
        use vortex::array::dtype::FieldNames;
        use vortex::array::validity::Validity;

        let values = PrimitiveArray::new(
            vec![1_u32, 0, 0, 2, 0],
            Validity::from_iter([true, false, false, true, false]),
        )
        .into_array();
        let metric =
            PrimitiveArray::new(vec![10_i64, 20, 99, 30, 44], Validity::NonNullable).into_array();
        let array = StructArray::try_new(
            FieldNames::from(["value", "metric"]),
            vec![values, metric],
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

    fn scoped_pivot_projection(aggregate: &str) -> VortexPivotProjectionRequest {
        VortexPivotProjectionRequest::new(
            ColumnRef::new("id").expect("index column"),
            ColumnRef::new("label").expect("pivot column"),
            ColumnRef::new("amount").expect("value column"),
            aggregate,
        )
    }

    #[test]
    fn pivot_first_unique_blocks_conflicting_duplicate_cells() {
        let projection = scoped_pivot_projection("first_unique");
        let mut index_keys = std::collections::BTreeSet::new();
        let mut pivot_columns = std::collections::BTreeMap::new();
        let mut first_cells = std::collections::BTreeMap::new();
        let mut aggregate_cells = std::collections::BTreeMap::new();

        let result = update_pivot_state(
            &projection,
            "first_unique",
            &[StatValue::UInt64(1), StatValue::UInt64(1)],
            &[
                StatValue::Utf8("paid".to_string()),
                StatValue::Utf8("paid".to_string()),
            ],
            &[StatValue::Int64(10), StatValue::Int64(20)],
            &mut index_keys,
            &mut pivot_columns,
            &mut first_cells,
            &mut aggregate_cells,
        );

        let error = result.expect_err("conflicting duplicate pivot cells must fail closed");
        assert!(error.to_string().contains("multiple values"), "{error:?}");
    }

    #[test]
    fn pivot_table_sum_tracks_numeric_group_state() {
        let projection = scoped_pivot_projection("sum");
        let mut index_keys = std::collections::BTreeSet::new();
        let mut pivot_columns = std::collections::BTreeMap::new();
        let mut first_cells = std::collections::BTreeMap::new();
        let mut aggregate_cells = std::collections::BTreeMap::new();

        update_pivot_state(
            &projection,
            "sum",
            &[
                StatValue::UInt64(1),
                StatValue::UInt64(1),
                StatValue::UInt64(2),
            ],
            &[
                StatValue::Utf8("paid".to_string()),
                StatValue::Utf8("paid".to_string()),
                StatValue::Utf8("trial".to_string()),
            ],
            &[
                StatValue::Int64(10),
                StatValue::Float64(5.5),
                StatValue::UInt64(7),
            ],
            &mut index_keys,
            &mut pivot_columns,
            &mut first_cells,
            &mut aggregate_cells,
        )
        .expect("numeric pivot_table aggregate");

        let paid_cell = aggregate_cells
            .get(&(
                pivot_value_key(&StatValue::UInt64(1)),
                pivot_value_key(&StatValue::Utf8("paid".to_string())),
            ))
            .expect("paid aggregate cell");
        assert_eq!(paid_cell.count, 2);
        assert!((paid_cell.sum - 15.5).abs() < f64::EPSILON);
        assert_eq!(paid_cell.min, Some(5.5));
        assert_eq!(paid_cell.max, Some(10.0));
        assert_eq!(pivot_columns.len(), 2);
        assert!(first_cells.is_empty());
        assert_eq!(index_keys.len(), 2);
    }

    #[test]
    fn pivot_table_sum_blocks_non_numeric_value_column() {
        let projection = scoped_pivot_projection("sum");
        let mut index_keys = std::collections::BTreeSet::new();
        let mut pivot_columns = std::collections::BTreeMap::new();
        let mut first_cells = std::collections::BTreeMap::new();
        let mut aggregate_cells = std::collections::BTreeMap::new();

        let result = update_pivot_state(
            &projection,
            "sum",
            &[StatValue::UInt64(1)],
            &[StatValue::Utf8("paid".to_string())],
            &[StatValue::Utf8("ten".to_string())],
            &mut index_keys,
            &mut pivot_columns,
            &mut first_cells,
            &mut aggregate_cells,
        );

        let error = result.expect_err("non-numeric pivot_table values must fail closed");
        assert!(
            error
                .to_string()
                .contains("requires a numeric value column"),
            "{error:?}"
        );
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
    fn drop_duplicate_rows_keep_last_retains_full_rows_by_key_without_fallback() {
        let path = unique_vortex_path("drop-duplicates-keep-last");
        write_duplicate_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::drop_duplicate_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![
                ColumnRef::new("value").expect("column"),
                ColumnRef::new("metric").expect("column"),
            ]),
            ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
        )
        .with_duplicate_keep(VortexDuplicateKeepPolicy::Last)
        .with_source_order_limit(2);

        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(2).expect("policy"),
        )
        .expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::DropDuplicateRows
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_selected, Some(2));
        assert_eq!(report.rows_projected, Some(2));
        assert_eq!(
            report.projected_columns,
            vec!["value".to_string(), "metric".to_string()]
        );
        assert_eq!(report.source_order_limit_requested, Some(2));
        assert!(report.source_order_limit_applied);
        assert_eq!(report.source_order_limit_input_rows, Some(3));
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
        assert_eq!(
            report.state_budget.state_family,
            "retained_row_deduplicate_state"
        );
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
    fn sample_rows_materialize_replacement_rows_without_fallback() {
        let path = unique_vortex_path("sample-replacement-rows");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::sample_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
            Some(PredicateExpr::Compare {
                column: ColumnRef::new("value").expect("column"),
                op: ComparisonOp::GtEq,
                value: StatValue::Int64(2),
            }),
            7,
            11,
        )
        .with_sample_replacement(true);

        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(1).expect("policy"),
        )
        .expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_selected, Some(7));
        assert_eq!(report.rows_projected, Some(7));
        assert_eq!(report.projected_columns, vec!["metric".to_string()]);
        assert_eq!(
            report.result_summary.as_deref(),
            Some(
                "sample_rows=7 projected_columns=metric sample_seed=11 sample_size=7 sample_replacement=true"
            )
        );
        assert_eq!(report.source_order_limit_requested, Some(7));
        assert!(report.source_order_limit_applied);
        assert_eq!(report.source_order_limit_input_rows, Some(4));
        assert_eq!(report.source_order_limit_rows_output, Some(7));
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
    fn sample_rows_materialize_fractional_replacement_rows_without_fallback() {
        let path = unique_vortex_path("sample-fraction-replacement-rows");
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
            19,
        )
        .with_sample_replacement(true);

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
            Some(
                "sample_rows=2 projected_columns=metric sample_seed=19 sample_fraction=0.5 sample_replacement=true"
            )
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
    fn sample_rows_materialize_weighted_vortex_rows_without_fallback() {
        let path = unique_vortex_path("sample-weighted-rows");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::sample_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
            None,
            2,
            7,
        )
        .with_sample_weight_column(ColumnRef::new("metric").expect("column"));

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
        assert_eq!(report.projected_columns, vec!["value".to_string()]);
        assert_eq!(
            report.result_summary.as_deref(),
            Some(
                "sample_rows=2 projected_columns=value sample_seed=7 sample_size=2 sample_weight_column=metric"
            )
        );
        assert!(report.projection_pushdown_applied);
        assert!(report.data_read);
        assert!(report.data_decoded);
        assert!(report.data_materialized);
        assert!(report.row_read);
        assert!(!report.fallback_execution_allowed);
        assert!(report.materialization_boundary_reported);
    }

    #[test]
    fn expression_project_rows_materialize_typed_rewrites_without_fallback() {
        let path = unique_vortex_path("expression-project-rows");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::expression_project_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
            VortexExpressionProjectionRequest::new(vec![VortexExpressionRewrite::MaskScalar {
                target_column: ColumnRef::new("metric").expect("column"),
                predicate: PredicateExpr::Compare {
                    column: ColumnRef::new("metric").expect("column"),
                    op: ComparisonOp::Lt,
                    value: StatValue::Int64(30),
                },
                replacement: StatValue::Int64(0),
            }]),
        )
        .with_source_order_limit(2);

        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(1).expect("policy"),
        )
        .expect("report");
        let certificate =
            local_primitive_native_io_certificate(&request, &report).expect("certificate");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::ExpressionProjectRows
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_selected, Some(2));
        assert_eq!(report.rows_projected, Some(2));
        assert_eq!(report.projected_columns, vec!["metric".to_string()]);
        assert_eq!(
            report.result_summary.as_deref(),
            Some(
                "expression_project_rows=2 projected_columns=metric expression_rewrites=mask_scalar"
            )
        );
        assert!(report.projection_pushdown_applied);
        assert_eq!(report.source_order_limit_requested, Some(2));
        assert!(report.source_order_limit_applied);
        assert_eq!(report.source_order_limit_input_rows, Some(5));
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
        assert!(certificate.is_certified());
        assert_eq!(
            certificate
                .source_pushdown_report
                .accepted_operation_order(),
            "project,expression_project"
        );
        assert_eq!(
            certificate.representation_transition_order(),
            "vortex_encoded->decoded_columnar,decoded_columnar->materialized_rows"
        );
        assert_eq!(
            certificate.materialization_boundary_order(),
            "local_primitive.expression_project_rows.bounded_materialization"
        );
        assert!(!certificate.side_effects.fallback_attempted);
    }

    #[test]
    fn rolling_window_rows_materialize_source_order_sums_without_fallback() {
        let path = unique_vortex_path("rolling-window-rows");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::rolling_window_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexRollingWindowRequest::new(
                ColumnRef::new("metric").expect("column"),
                "rolling_metric".to_string(),
                3,
                3,
                "sum".to_string(),
            ),
        )
        .with_source_order_limit(2);

        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(1).expect("policy"),
        )
        .expect("report");
        let certificate =
            local_primitive_native_io_certificate(&request, &report).expect("certificate");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::RollingWindowRows
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_selected, Some(2));
        assert_eq!(report.rows_projected, Some(2));
        assert_eq!(report.projected_columns, vec!["rolling_metric".to_string()]);
        assert_eq!(
            report.result_summary.as_deref(),
            Some(
                "rolling_window_rows=2 projected_columns=rolling_metric source_column=metric;output_column=rolling_metric;window_size=3;min_periods=3;aggregate=sum;center=false"
            )
        );
        assert!(report.projection_pushdown_applied);
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
        assert!(report.state_budget.state_budget_required);
        assert_eq!(report.state_budget.state_family, "rolling_window_state");
        assert_eq!(report.state_budget.observed_state_items, 3);
        assert!(
            report
                .state_budget
                .capillary_work_units
                .contains(&"rolling_window_state_fragment".to_string())
        );
        assert!(
            report
                .state_budget
                .pulseweave_pressure_signals
                .contains(&"window_state_memory".to_string())
        );
        assert!(report.state_budget.fail_closed_if_spill_required);
        assert!(certificate.is_certified());
        assert_eq!(
            certificate
                .source_pushdown_report
                .accepted_operation_order(),
            "project,rolling_window"
        );
        assert_eq!(
            certificate.representation_transition_order(),
            "vortex_encoded->decoded_columnar,decoded_columnar->materialized_rows"
        );
        assert!(!certificate.side_effects.fallback_attempted);
    }

    #[test]
    fn rolling_window_rows_materialize_min_max_without_fallback() {
        let min_request = VortexRollingWindowRequest::new(
            ColumnRef::new("metric").expect("column"),
            "rolling_metric_min".to_string(),
            3,
            2,
            "min".to_string(),
        );
        let max_request = VortexRollingWindowRequest::new(
            ColumnRef::new("metric").expect("column"),
            "rolling_metric_max".to_string(),
            3,
            2,
            "max".to_string(),
        );
        let values = vec![vec![
            StatValue::Int64(10),
            StatValue::Int64(30),
            StatValue::Int64(20),
            StatValue::Int64(50),
        ]];

        let min_values = rolling_window_values(
            &values,
            &min_request,
            &mut RollingWindowState::new(min_request.window_size),
            4,
            false,
        )
        .expect("rolling min");
        let max_values = rolling_window_values(
            &values,
            &max_request,
            &mut RollingWindowState::new(max_request.window_size),
            4,
            false,
        )
        .expect("rolling max");

        assert_eq!(
            min_values,
            vec![
                StatValue::Float64(10.0),
                StatValue::Float64(10.0),
                StatValue::Float64(20.0)
            ]
        );
        assert_eq!(
            max_values,
            vec![
                StatValue::Float64(30.0),
                StatValue::Float64(30.0),
                StatValue::Float64(50.0)
            ]
        );
    }

    #[test]
    fn rolling_window_values_use_valid_observation_min_periods_without_fallback() {
        let sum_request = VortexRollingWindowRequest::new(
            ColumnRef::new("metric").expect("column"),
            "rolling_metric_sum".to_string(),
            3,
            2,
            "sum".to_string(),
        );
        let count_request = VortexRollingWindowRequest::new(
            ColumnRef::new("metric").expect("column"),
            "rolling_metric_count".to_string(),
            3,
            2,
            "count".to_string(),
        );
        let values = vec![vec![
            StatValue::Int64(10),
            StatValue::Null,
            StatValue::Int64(20),
            StatValue::Null,
            StatValue::Int64(50),
        ]];

        let sum_values = rolling_window_values(
            &values,
            &sum_request,
            &mut RollingWindowState::new(sum_request.window_size),
            5,
            false,
        )
        .expect("rolling sum with nulls");
        let count_values = rolling_window_values(
            &values,
            &count_request,
            &mut RollingWindowState::new(count_request.window_size),
            5,
            false,
        )
        .expect("rolling count with nulls");

        assert_eq!(
            sum_values,
            vec![StatValue::Float64(30.0), StatValue::Float64(70.0)]
        );
        assert_eq!(
            count_values,
            vec![StatValue::UInt64(2), StatValue::UInt64(2)]
        );
    }

    #[test]
    fn centered_rolling_window_values_use_bounded_lookahead_without_fallback() {
        let request = VortexRollingWindowRequest::new(
            ColumnRef::new("metric").expect("column"),
            "rolling_metric_centered_sum".to_string(),
            3,
            2,
            "sum".to_string(),
        )
        .with_center(true);
        let mut state = RollingWindowState::new(request.window_size);
        let first_values = rolling_window_values(
            &[vec![StatValue::Int64(10), StatValue::Int64(20)]],
            &request,
            &mut state,
            2,
            false,
        )
        .expect("centered rolling first chunk");
        let second_values = rolling_window_values(
            &[vec![
                StatValue::Int64(30),
                StatValue::Int64(40),
                StatValue::Int64(50),
            ]],
            &request,
            &mut state,
            3,
            true,
        )
        .expect("centered rolling second chunk");

        assert_eq!(first_values, vec![StatValue::Float64(30.0)]);
        assert_eq!(
            second_values,
            vec![
                StatValue::Float64(60.0),
                StatValue::Float64(90.0),
                StatValue::Float64(120.0),
                StatValue::Float64(90.0)
            ]
        );
    }

    #[test]
    fn rolling_window_row_export_writes_mean_rows_without_fallback() {
        let path = unique_vortex_path("rolling-window-mean-row-export");
        let output_path =
            unique_vortex_path("rolling-window-mean-row-export-output").with_extension("jsonl");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::rolling_window_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexRollingWindowRequest::new(
                ColumnRef::new("metric").expect("column"),
                "rolling_metric_mean".to_string(),
                2,
                2,
                "mean".to_string(),
            ),
        )
        .with_source_order_limit(3);

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
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::RollingWindowRows
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 3);
        assert_eq!(report.pre_limit_result_row_count, 4);
        assert_eq!(
            report.projected_columns,
            vec!["rolling_metric_mean".to_string()]
        );
        assert_eq!(
            rows,
            "{\"rolling_metric_mean\":15.0}\n{\"rolling_metric_mean\":25.0}\n{\"rolling_metric_mean\":35.0}\n"
        );
        assert!(report.evidence.side_effects.data_read);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(report.evidence.side_effects.data_materialized);
        assert!(report.evidence.side_effects.row_read);
        assert!(report.evidence.side_effects.write_io);
        assert!(!report.evidence.side_effects.external_effects_executed);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(report.state_budget.state_budget_required);
        assert_eq!(report.state_budget.state_family, "rolling_window_state");
        assert_eq!(report.state_budget.observed_state_items, 2);
        assert!(report.state_budget.fail_closed_if_spill_required);
    }

    #[test]
    fn rolling_window_row_export_writes_count_rows_without_fallback() {
        let path = unique_vortex_path("rolling-window-count-row-export");
        let output_path =
            unique_vortex_path("rolling-window-count-row-export-output").with_extension("jsonl");
        write_string_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::rolling_window_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexRollingWindowRequest::new(
                ColumnRef::new("label").expect("column"),
                "rolling_label_count".to_string(),
                3,
                1,
                "count".to_string(),
            ),
        )
        .with_source_order_limit(3);

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
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::RollingWindowRows
        );
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_written, 3);
        assert_eq!(report.pre_limit_result_row_count, 3);
        assert_eq!(
            report.projected_columns,
            vec!["rolling_label_count".to_string()]
        );
        assert_eq!(
            rows,
            "{\"rolling_label_count\":1}\n{\"rolling_label_count\":2}\n{\"rolling_label_count\":3}\n"
        );
        assert!(report.evidence.side_effects.data_read);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(report.evidence.side_effects.data_materialized);
        assert!(report.evidence.side_effects.row_read);
        assert!(report.evidence.side_effects.write_io);
        assert!(!report.evidence.side_effects.external_effects_executed);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(report.state_budget.state_budget_required);
        assert_eq!(report.state_budget.state_family, "rolling_window_state");
        assert_eq!(report.state_budget.observed_state_items, 3);
        assert!(report.state_budget.fail_closed_if_spill_required);
    }

    #[test]
    fn centered_rolling_window_row_export_writes_bounded_rows_without_fallback() {
        let path = unique_vortex_path("rolling-window-centered-row-export");
        let output_path =
            unique_vortex_path("rolling-window-centered-row-export-output").with_extension("jsonl");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::rolling_window_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexRollingWindowRequest::new(
                ColumnRef::new("metric").expect("column"),
                "rolling_metric_centered_sum".to_string(),
                3,
                3,
                "sum".to_string(),
            )
            .with_center(true),
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
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::RollingWindowRows
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 3);
        assert_eq!(report.pre_limit_result_row_count, 3);
        assert_eq!(
            report.projected_columns,
            vec!["rolling_metric_centered_sum".to_string()]
        );
        assert_eq!(
            rows,
            "{\"rolling_metric_centered_sum\":60.0}\n{\"rolling_metric_centered_sum\":90.0}\n{\"rolling_metric_centered_sum\":120.0}\n"
        );
        assert!(report.evidence.side_effects.data_read);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(report.evidence.side_effects.data_materialized);
        assert!(report.evidence.side_effects.row_read);
        assert!(report.evidence.side_effects.write_io);
        assert!(!report.evidence.side_effects.external_effects_executed);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(report.state_budget.state_budget_required);
        assert_eq!(report.state_budget.state_family, "rolling_window_state");
        assert_eq!(report.state_budget.observed_state_items, 3);
        assert!(report.state_budget.fail_closed_if_spill_required);
    }

    #[test]
    fn explode_rows_materialize_list_elements_without_fallback() {
        let path = unique_vortex_path("explode-rows");
        write_explode_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::explode_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![
                ColumnRef::new("id").expect("column"),
                ColumnRef::new("items").expect("column"),
            ]),
            VortexExplodeProjectionRequest::new(ColumnRef::new("items").expect("column")),
        )
        .with_source_order_limit(2);

        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(1).expect("policy"),
        )
        .expect("report");
        let certificate =
            local_primitive_native_io_certificate(&request, &report).expect("certificate");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::ExplodeRows);
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_selected, Some(2));
        assert_eq!(report.rows_projected, Some(2));
        assert_eq!(
            report.projected_columns,
            vec!["id".to_string(), "items".to_string()]
        );
        assert_eq!(
            report.result_summary.as_deref(),
            Some("explode_rows=2 projected_columns=id,items column=items")
        );
        assert!(report.projection_pushdown_applied);
        assert_eq!(report.source_order_limit_requested, Some(2));
        assert!(report.source_order_limit_applied);
        assert_eq!(report.source_order_limit_input_rows, Some(3));
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
        assert!(certificate.is_certified());
        assert_eq!(
            certificate
                .source_pushdown_report
                .accepted_operation_order(),
            "project,explode"
        );
        assert_eq!(
            certificate.representation_transition_order(),
            "vortex_encoded->decoded_columnar,decoded_columnar->materialized_rows"
        );
        assert_eq!(
            certificate.materialization_boundary_order(),
            "local_primitive.explode_rows.bounded_materialization"
        );
        assert!(!certificate.side_effects.fallback_attempted);
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
        assert_eq!(report.rows_written, 3);
        assert_eq!(report.pre_limit_result_row_count, 3);
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
    fn drop_duplicate_row_export_keep_false_writes_only_unique_key_rows_without_fallback() {
        let path = unique_vortex_path("drop-duplicates-row-export");
        let output_path =
            unique_vortex_path("drop-duplicates-row-export-output").with_extension("jsonl");
        write_duplicate_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::drop_duplicate_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![
                ColumnRef::new("value").expect("column"),
                ColumnRef::new("metric").expect("column"),
            ]),
            ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
        )
        .with_duplicate_keep(VortexDuplicateKeepPolicy::AllDuplicates);

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
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::DropDuplicateRows
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 1);
        assert_eq!(report.pre_limit_result_row_count, 1);
        assert_eq!(
            report.projected_columns,
            vec!["value".to_string(), "metric".to_string()]
        );
        let emitted_rows: Vec<serde_json::Value> = rows
            .lines()
            .map(|line| serde_json::from_str(line).expect("jsonl row"))
            .collect();
        assert_eq!(
            emitted_rows,
            vec![serde_json::json!({"value": 3, "metric": 30})]
        );
        assert!(report.evidence.pushdown.projection_pushdown_applied);
        assert!(!report.evidence.pushdown.source_order_limit_applied);
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
    fn drop_duplicate_row_export_keep_last_retains_nullable_scalar_key_without_fallback() {
        let path = unique_vortex_path("drop-duplicates-nullable-row-export");
        let output_path = unique_vortex_path("drop-duplicates-nullable-row-export-output")
            .with_extension("jsonl");
        write_nullable_duplicate_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::drop_duplicate_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![
                ColumnRef::new("value").expect("column"),
                ColumnRef::new("metric").expect("column"),
            ]),
            ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
        )
        .with_duplicate_keep(VortexDuplicateKeepPolicy::Last);

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
        let emitted_rows: Vec<serde_json::Value> = rows
            .lines()
            .map(|line| serde_json::from_str(line).expect("jsonl row"))
            .collect();

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::DropDuplicateRows
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 3);
        assert_eq!(report.pre_limit_result_row_count, 3);
        assert_eq!(
            emitted_rows,
            vec![
                serde_json::json!({"value": 1, "metric": 10}),
                serde_json::json!({"value": 2, "metric": 30}),
                serde_json::json!({"value": null, "metric": 44}),
            ]
        );
        assert!(report.evidence.upstream_scan_called);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(!report.evidence.side_effects.external_effects_executed);
    }

    #[test]
    fn drop_duplicate_row_export_retains_rows_by_list_key_without_fallback() {
        let path = unique_vortex_path("drop-duplicates-list-key-row-export");
        let output_path = unique_vortex_path("drop-duplicates-list-key-row-export-output")
            .with_extension("jsonl");
        write_nested_row_key_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::drop_duplicate_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
            ProjectionRequest::columns(vec![ColumnRef::new("items").expect("column")]),
        )
        .with_duplicate_keep(VortexDuplicateKeepPolicy::Last);

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
        let emitted_rows: Vec<serde_json::Value> = rows
            .lines()
            .map(|line| serde_json::from_str(line).expect("jsonl row"))
            .collect();

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::DropDuplicateRows
        );
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_written, 2);
        assert_eq!(report.pre_limit_result_row_count, 2);
        assert_eq!(report.projected_columns, vec!["metric".to_string()]);
        assert_eq!(
            emitted_rows,
            vec![
                serde_json::json!({"metric": 20}),
                serde_json::json!({"metric": 30}),
            ]
        );
        assert!(report.evidence.upstream_scan_called);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(!report.evidence.side_effects.external_effects_executed);
    }

    #[test]
    fn duplicate_mask_row_export_writes_boolean_mask_without_fallback() {
        let path = unique_vortex_path("duplicate-mask-row-export");
        let output_path =
            unique_vortex_path("duplicate-mask-row-export-output").with_extension("jsonl");
        write_duplicate_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::duplicate_mask_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
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
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::DuplicateMaskRows
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 5);
        assert_eq!(report.pre_limit_result_row_count, 5);
        assert_eq!(report.projected_columns, vec!["duplicated".to_string()]);
        assert_eq!(
            rows,
            "{\"duplicated\":false}\n{\"duplicated\":true}\n{\"duplicated\":false}\n{\"duplicated\":true}\n{\"duplicated\":false}\n"
        );
        assert!(report.evidence.pushdown.projection_pushdown_applied);
        assert!(!report.evidence.pushdown.source_order_limit_applied);
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
    fn duplicate_mask_row_export_marks_struct_key_duplicates_without_fallback() {
        let path = unique_vortex_path("duplicate-mask-struct-key-row-export");
        let output_path = unique_vortex_path("duplicate-mask-struct-key-row-export-output")
            .with_extension("jsonl");
        write_nested_row_key_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::duplicate_mask_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("payload").expect("column")]),
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
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::DuplicateMaskRows
        );
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_written, 3);
        assert_eq!(report.pre_limit_result_row_count, 3);
        assert_eq!(
            rows,
            "{\"duplicated\":false}\n{\"duplicated\":true}\n{\"duplicated\":false}\n"
        );
        assert!(report.evidence.upstream_scan_called);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(!report.evidence.side_effects.external_effects_executed);
    }

    #[test]
    fn duplicate_mask_row_export_preserves_list_parent_null_key_identity() {
        let path = unique_vortex_path("duplicate-mask-nullable-list-parent-key-row-export");
        let output_path =
            unique_vortex_path("duplicate-mask-nullable-list-parent-key-row-export-output")
                .with_extension("jsonl");
        write_nullable_parent_nested_row_key_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::duplicate_mask_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("items").expect("column")]),
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
        assert_eq!(
            rows,
            "{\"duplicated\":false}\n{\"duplicated\":false}\n{\"duplicated\":true}\n"
        );
        assert!(report.evidence.upstream_scan_called);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(!report.evidence.side_effects.external_effects_executed);
    }

    #[test]
    fn duplicate_mask_row_export_preserves_struct_parent_null_key_identity() {
        let path = unique_vortex_path("duplicate-mask-nullable-struct-parent-key-row-export");
        let output_path =
            unique_vortex_path("duplicate-mask-nullable-struct-parent-key-row-export-output")
                .with_extension("jsonl");
        write_nullable_parent_nested_row_key_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::duplicate_mask_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("payload").expect("column")]),
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
        assert_eq!(
            rows,
            "{\"duplicated\":false}\n{\"duplicated\":false}\n{\"duplicated\":true}\n"
        );
        assert!(report.evidence.upstream_scan_called);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(!report.evidence.side_effects.external_effects_executed);
    }

    #[test]
    fn duplicate_mask_row_export_writes_keep_last_mask_without_fallback() {
        let path = unique_vortex_path("duplicate-mask-row-export-last");
        let output_path =
            unique_vortex_path("duplicate-mask-row-export-last-output").with_extension("jsonl");
        write_duplicate_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::duplicate_mask_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
        )
        .with_source_order_limit(2)
        .with_duplicate_keep(VortexDuplicateKeepPolicy::Last);

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
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 2);
        assert_eq!(report.pre_limit_result_row_count, 5);
        assert_eq!(report.source_order_limit_requested, Some(2));
        assert_eq!(rows, "{\"duplicated\":true}\n{\"duplicated\":false}\n");
        assert!(report.evidence.upstream_scan_called);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(!report.evidence.side_effects.external_effects_executed);
    }

    #[test]
    fn duplicate_mask_row_export_marks_repeated_null_scalar_key_without_fallback() {
        let path = unique_vortex_path("duplicate-mask-nullable-row-export");
        let output_path =
            unique_vortex_path("duplicate-mask-nullable-row-export-output").with_extension("jsonl");
        write_nullable_duplicate_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::duplicate_mask_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
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
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::DuplicateMaskRows
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 5);
        assert_eq!(report.pre_limit_result_row_count, 5);
        assert_eq!(
            rows,
            "{\"duplicated\":false}\n{\"duplicated\":false}\n{\"duplicated\":true}\n{\"duplicated\":false}\n{\"duplicated\":true}\n"
        );
        assert!(report.evidence.upstream_scan_called);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(!report.evidence.side_effects.external_effects_executed);
    }

    #[test]
    fn duplicate_mask_row_export_writes_all_duplicate_mask_without_fallback() {
        let path = unique_vortex_path("duplicate-mask-row-export-all");
        let output_path =
            unique_vortex_path("duplicate-mask-row-export-all-output").with_extension("jsonl");
        write_duplicate_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::duplicate_mask_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
        )
        .with_duplicate_keep(VortexDuplicateKeepPolicy::AllDuplicates);

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
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 5);
        assert_eq!(report.pre_limit_result_row_count, 5);
        assert_eq!(
            rows,
            "{\"duplicated\":true}\n{\"duplicated\":true}\n{\"duplicated\":true}\n{\"duplicated\":true}\n{\"duplicated\":false}\n"
        );
        assert!(report.evidence.upstream_scan_called);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(!report.evidence.side_effects.external_effects_executed);
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
    fn sample_row_export_writes_weighted_rows_without_exposing_weight_column() {
        let path = unique_vortex_path("sample-row-export-weighted");
        let output_path =
            unique_vortex_path("sample-row-export-weighted-output").with_extension("jsonl");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::sample_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
            None,
            2,
            7,
        )
        .with_sample_weight_column(ColumnRef::new("metric").expect("column"));

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

        let values = [1_i64, 2, 3, 4, 5];
        let weights = [10.0_f64, 20.0, 30.0, 40.0, 50.0];
        let mut expected_indices = weights
            .iter()
            .enumerate()
            .map(|(index, weight)| {
                (
                    deterministic_weighted_sample_score(7, index, *weight),
                    index,
                )
            })
            .collect::<Vec<_>>();
        expected_indices.sort_by(|(left_score, _left), (right_score, _right)| {
            right_score
                .partial_cmp(left_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        expected_indices.truncate(2);
        expected_indices.sort_by_key(|(_score, index)| *index);
        let expected = expected_indices
            .iter()
            .map(|(_score, index)| format!("{{\"value\":{}}}", values[*index]))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 2);
        assert_eq!(report.pre_limit_result_row_count, 5);
        assert_eq!(report.projected_columns, vec!["value".to_string()]);
        assert_eq!(report.source_order_limit_requested, Some(2));
        assert_eq!(rows, expected);
        assert!(!rows.contains("metric"));
        assert!(report.evidence.pushdown.projection_pushdown_applied);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(report.evidence.side_effects.data_materialized);
        assert!(report.evidence.side_effects.row_read);
        assert!(report.evidence.side_effects.write_io);
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
    fn sample_row_export_writes_replacement_rows_without_fallback() {
        let path = unique_vortex_path("sample-row-export-replacement");
        let output_path =
            unique_vortex_path("sample-row-export-replacement-output").with_extension("jsonl");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::sample_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
            None,
            7,
            11,
        )
        .with_sample_replacement(true);

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
        let expected = (0..7)
            .map(|draw_index| {
                let row_index =
                    deterministic_sample_replacement_index(11, draw_index, metrics.len());
                format!("{{\"metric\":{}}}", metrics[row_index])
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SampleRows);
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 7);
        assert_eq!(report.pre_limit_result_row_count, 5);
        assert_eq!(report.projected_columns, vec!["metric".to_string()]);
        assert_eq!(report.source_order_limit_requested, Some(7));
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
    fn sample_row_export_writes_fractional_replacement_rows_without_fallback() {
        let path = unique_vortex_path("sample-row-export-fraction-replacement");
        let output_path = unique_vortex_path("sample-row-export-fraction-replacement-output")
            .with_extension("jsonl");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::sample_fraction_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
            None,
            0.4,
            23,
        )
        .with_sample_replacement(true);

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
        let expected = (0..2)
            .map(|draw_index| {
                let row_index =
                    deterministic_sample_replacement_index(23, draw_index, metrics.len());
                format!("{{\"metric\":{}}}", metrics[row_index])
            })
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
        assert!(!report.evidence.side_effects.external_effects_executed);
        assert!(!report.evidence.side_effects.fallback_attempted);
    }

    #[test]
    fn expression_project_row_export_writes_rewritten_rows_without_fallback() {
        let path = unique_vortex_path("expression-project-row-export");
        let output_path =
            unique_vortex_path("expression-project-row-export-output").with_extension("jsonl");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::expression_project_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
            VortexExpressionProjectionRequest::new(vec![VortexExpressionRewrite::ReplaceScalar {
                target_column: ColumnRef::new("metric").expect("column"),
                to_replace: StatValue::Int64(20),
                replacement: StatValue::Int64(99),
            }]),
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
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::ExpressionProjectRows
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 5);
        assert_eq!(report.pre_limit_result_row_count, 5);
        assert_eq!(report.projected_columns, vec!["metric".to_string()]);
        assert_eq!(
            rows,
            "{\"metric\":10}\n{\"metric\":99}\n{\"metric\":30}\n{\"metric\":40}\n{\"metric\":50}\n"
        );
        assert!(report.evidence.pushdown.projection_pushdown_applied);
        assert!(!report.evidence.pushdown.source_order_limit_applied);
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
    fn expression_project_row_export_writes_null_rewrite_without_fallback() {
        let path = unique_vortex_path("expression-project-null-row-export");
        let output_path =
            unique_vortex_path("expression-project-null-row-export-output").with_extension("jsonl");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::expression_project_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
            VortexExpressionProjectionRequest::new(vec![VortexExpressionRewrite::ReplaceScalar {
                target_column: ColumnRef::new("metric").expect("column"),
                to_replace: StatValue::Int64(20),
                replacement: StatValue::Null,
            }]),
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
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::ExpressionProjectRows
        );
        assert_eq!(report.rows_written, 5);
        assert_eq!(
            rows,
            "{\"metric\":10}\n{\"metric\":null}\n{\"metric\":30}\n{\"metric\":40}\n{\"metric\":50}\n"
        );
        assert!(report.evidence.upstream_scan_called);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(report.evidence.side_effects.data_materialized);
        assert!(report.evidence.side_effects.write_io);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(!report.evidence.side_effects.external_effects_executed);
    }

    #[test]
    fn expression_project_row_export_forward_fills_nulls_without_fallback() {
        let path = unique_vortex_path("expression-project-forward-fill-row-export");
        let output_path = unique_vortex_path("expression-project-forward-fill-row-export-output")
            .with_extension("jsonl");
        write_nullable_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::expression_project_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("value").expect("column")]),
            VortexExpressionProjectionRequest::new(vec![
                VortexExpressionRewrite::ForwardFillNull {
                    target_column: ColumnRef::new("value").expect("column"),
                    limit: Some(1),
                },
            ]),
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
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::ExpressionProjectRows
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 5);
        assert_eq!(report.pre_limit_result_row_count, 5);
        assert_eq!(report.projected_columns, vec!["value".to_string()]);
        assert_eq!(
            rows,
            "{\"value\":1}\n{\"value\":1}\n{\"value\":3}\n{\"value\":4}\n{\"value\":5}\n"
        );
        assert!(report.evidence.upstream_scan_called);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(report.evidence.side_effects.data_materialized);
        assert!(report.evidence.side_effects.write_io);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(!report.evidence.side_effects.external_effects_executed);
    }

    #[cfg(feature = "universal-format-io")]
    #[test]
    fn structured_expression_project_row_export_writes_parquet_without_fallback() {
        let path = unique_vortex_path("structured-expression-project-row-export");
        let output_path = unique_vortex_path("structured-expression-project-row-export-output")
            .with_extension("parquet");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::structured_project_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexStructuredProjectionRequest::new(vec![
                VortexStructuredProjectionColumn::new(
                    "value".to_string(),
                    VortexStructuredProjectionExpr::SourceColumn(
                        ColumnRef::new("value").expect("column"),
                    ),
                ),
                VortexStructuredProjectionColumn::new(
                    "values".to_string(),
                    VortexStructuredProjectionExpr::ArrayLiteral(vec![
                        ScalarValue::Int64(1),
                        ScalarValue::Int64(2),
                        ScalarValue::Null,
                    ]),
                ),
                VortexStructuredProjectionColumn::new(
                    "payload".to_string(),
                    VortexStructuredProjectionExpr::StructColumns(vec![
                        ColumnRef::new("value").expect("column"),
                        ColumnRef::new("metric").expect("column"),
                    ]),
                ),
            ]),
        )
        .with_source_order_limit(2);

        let report = execute_vortex_local_primitive_row_export_with_policy(
            &request,
            &output_path,
            VortexLocalPrimitiveRowExportFormat::Parquet,
            false,
            VortexLocalPrimitiveExecutionPolicy::new(1).expect("policy"),
        )
        .expect("report");
        let table = crate::read_flat_parquet_source(&output_path, 10).expect("parquet rows");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&output_path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::ExpressionProjectRows
        );
        assert_eq!(report.output_format, "parquet");
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 2);
        assert_eq!(report.pre_limit_result_row_count, 5);
        assert_eq!(
            report.projected_columns,
            vec![
                "value".to_string(),
                "values".to_string(),
                "payload".to_string()
            ]
        );
        assert_eq!(report.source_order_limit_requested, Some(2));
        assert!(report.evidence.upstream_scan_called);
        assert!(report.evidence.pushdown.projection_pushdown_applied);
        assert!(report.evidence.pushdown.source_order_limit_applied);
        assert!(report.evidence.materialization_boundary_reported);
        assert!(report.evidence.side_effects.data_read);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(report.evidence.side_effects.data_materialized);
        assert!(report.evidence.side_effects.row_read);
        assert!(report.evidence.side_effects.write_io);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(!report.evidence.side_effects.external_effects_executed);
        assert_eq!(
            table.header,
            vec![
                "value".to_string(),
                "values".to_string(),
                "payload".to_string()
            ]
        );
        assert_eq!(table.rows.len(), 2);
        assert_eq!(table.rows[0]["value"], ScalarValue::UInt64(1));
        assert_eq!(
            table.rows[0]["values"],
            ScalarValue::List(vec![
                ScalarValue::Int64(1),
                ScalarValue::Int64(2),
                ScalarValue::Null
            ])
        );
        assert_eq!(
            table.rows[0]["payload"],
            ScalarValue::Struct(vec![
                ("value".to_string(), ScalarValue::UInt64(1)),
                ("metric".to_string(), ScalarValue::Int64(10))
            ])
        );
    }

    #[cfg(all(feature = "universal-format-io", feature = "vortex-write"))]
    #[test]
    fn structured_expression_project_row_export_writes_vortex_without_fallback() {
        let path = unique_vortex_path("structured-expression-project-vortex-row-export");
        let output_path =
            unique_vortex_path("structured-expression-project-vortex-row-export-output")
                .with_extension("vortex");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::structured_project_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexStructuredProjectionRequest::new(vec![
                VortexStructuredProjectionColumn::new(
                    "value".to_string(),
                    VortexStructuredProjectionExpr::SourceColumn(
                        ColumnRef::new("value").expect("column"),
                    ),
                ),
                VortexStructuredProjectionColumn::new(
                    "values".to_string(),
                    VortexStructuredProjectionExpr::ArrayLiteral(vec![
                        ScalarValue::Int64(1),
                        ScalarValue::Int64(2),
                        ScalarValue::Null,
                    ]),
                ),
                VortexStructuredProjectionColumn::new(
                    "payload".to_string(),
                    VortexStructuredProjectionExpr::StructColumns(vec![
                        ColumnRef::new("value").expect("column"),
                        ColumnRef::new("metric").expect("column"),
                    ]),
                ),
            ]),
        )
        .with_source_order_limit(2);

        let report = execute_vortex_local_primitive_row_export_with_policy(
            &request,
            &output_path,
            VortexLocalPrimitiveRowExportFormat::Vortex,
            false,
            VortexLocalPrimitiveExecutionPolicy::new(1).expect("policy"),
        )
        .expect("report");
        let count_report = execute_vortex_local_primitive(&VortexQueryPrimitiveRequest::count_all(
            DatasetUri::new(output_path.display().to_string()).expect("output uri"),
        ))
        .expect("count output");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&output_path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::ExpressionProjectRows
        );
        assert_eq!(report.output_format, "vortex");
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 2);
        assert_eq!(report.pre_limit_result_row_count, 5);
        assert_eq!(
            report.projected_columns,
            vec![
                "value".to_string(),
                "values".to_string(),
                "payload".to_string()
            ]
        );
        assert_eq!(report.source_order_limit_requested, Some(2));
        assert!(report.evidence.upstream_scan_called);
        assert!(report.evidence.pushdown.projection_pushdown_applied);
        assert!(report.evidence.pushdown.source_order_limit_applied);
        assert!(report.evidence.materialization_boundary_reported);
        assert!(report.evidence.side_effects.data_read);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(report.evidence.side_effects.data_materialized);
        assert!(report.evidence.side_effects.row_read);
        assert!(report.evidence.side_effects.write_io);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(!report.evidence.side_effects.external_effects_executed);
        assert_eq!(
            count_report.status,
            VortexLocalPrimitiveExecutionStatus::Executed
        );
        assert_eq!(count_report.rows_scanned, 2);
        assert!(count_report.upstream_scan_called);
        assert!(!count_report.fallback_execution_allowed);
    }

    #[test]
    fn expression_project_row_export_writes_string_replacement_rows_without_fallback() {
        let path = unique_vortex_path("expression-project-string-replace");
        let output_path =
            unique_vortex_path("expression-project-string-replace-output").with_extension("jsonl");
        write_string_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::expression_project_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("label").expect("column")]),
            VortexExpressionProjectionRequest::new(vec![
                VortexExpressionRewrite::StringReplaceScalar {
                    target_column: ColumnRef::new("label").expect("column"),
                    needle: "bad".to_string(),
                    replacement: "ok".to_string(),
                },
            ]),
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
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::ExpressionProjectRows
        );
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_written, 3);
        assert_eq!(report.pre_limit_result_row_count, 3);
        assert_eq!(report.projected_columns, vec!["label".to_string()]);
        assert_eq!(
            rows,
            "{\"label\":\"ok\"}\n{\"label\":\"good\"}\n{\"label\":\"okly\"}\n"
        );
        assert!(report.evidence.pushdown.projection_pushdown_applied);
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
    fn expression_project_regex_replacement_translates_python_backrefs() {
        assert_eq!(
            python_regex_replacement_for_rust_regex(r"\1foo$\g<name>\g<2>"),
            "${1}foo$$${name}${2}"
        );
        assert_eq!(
            python_regex_replacement_for_rust_regex(r"\g<unterminated"),
            r"\g<unterminated"
        );
    }

    #[test]
    fn expression_project_row_export_writes_regex_replacement_rows_without_fallback() {
        let path = unique_vortex_path("expression-project-regex-replace");
        let output_path =
            unique_vortex_path("expression-project-regex-replace-output").with_extension("jsonl");
        write_string_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::expression_project_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("label").expect("column")]),
            VortexExpressionProjectionRequest::new(vec![
                VortexExpressionRewrite::RegexReplaceScalar {
                    target_column: ColumnRef::new("label").expect("column"),
                    pattern: "^(bad)(.*)$".to_string(),
                    replacement: "\\1-fixed\\2".to_string(),
                },
            ]),
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
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::ExpressionProjectRows
        );
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_written, 3);
        assert_eq!(report.pre_limit_result_row_count, 3);
        assert_eq!(report.projected_columns, vec!["label".to_string()]);
        assert_eq!(
            rows,
            "{\"label\":\"bad-fixed\"}\n{\"label\":\"good\"}\n{\"label\":\"bad-fixedly\"}\n"
        );
        assert!(report.evidence.pushdown.projection_pushdown_applied);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(!report.evidence.side_effects.external_effects_executed);
    }

    #[test]
    fn expression_project_row_export_writes_numeric_scalar_assignment_without_fallback() {
        let path = unique_vortex_path("expression-project-numeric-assignment");
        let output_path = unique_vortex_path("expression-project-numeric-assignment-output")
            .with_extension("jsonl");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::expression_project_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
            VortexExpressionProjectionRequest::new(vec![
                VortexExpressionRewrite::NumericScalarArithmetic {
                    target_column: ColumnRef::new("metric").expect("column"),
                    operator: "+".to_string(),
                    operand: StatValue::Int64(5),
                },
            ]),
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
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::ExpressionProjectRows
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 5);
        assert_eq!(report.pre_limit_result_row_count, 5);
        assert_eq!(report.projected_columns, vec!["metric".to_string()]);
        assert_eq!(
            rows,
            "{\"metric\":15}\n{\"metric\":25}\n{\"metric\":35}\n{\"metric\":45}\n{\"metric\":55}\n"
        );
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
    fn expression_project_row_export_adds_source_order_row_number_without_fallback() {
        let path = unique_vortex_path("expression-project-row-number");
        let output_path =
            unique_vortex_path("expression-project-row-number-output").with_extension("jsonl");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::expression_project_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
            VortexExpressionProjectionRequest::new(vec![VortexExpressionRewrite::RowNumber {
                target_column: ColumnRef::new("index").expect("column"),
                start: 0,
            }]),
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
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::ExpressionProjectRows
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 5);
        assert_eq!(report.pre_limit_result_row_count, 5);
        assert_eq!(
            report.projected_columns,
            vec!["metric".to_string(), "index".to_string()]
        );
        assert_eq!(
            rows,
            "{\"index\":0,\"metric\":10}\n{\"index\":1,\"metric\":20}\n{\"index\":2,\"metric\":30}\n{\"index\":3,\"metric\":40}\n{\"index\":4,\"metric\":50}\n"
        );
        assert!(report.evidence.upstream_scan_called);
        assert!(report.evidence.pushdown.projection_pushdown_applied);
        assert!(report.evidence.materialization_boundary_reported);
        assert!(!report.evidence.side_effects.external_effects_executed);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(!report.evidence.side_effects.fallback_execution_allowed);
    }

    #[test]
    fn melt_row_export_writes_expanded_rows_without_fallback() {
        let path = unique_vortex_path("melt-row-export");
        let output_path = unique_vortex_path("melt-row-export-output").with_extension("jsonl");
        write_melt_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::melt_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexMeltProjectionRequest::new(
                vec![ColumnRef::new("id").expect("column")],
                vec![
                    ColumnRef::new("amount_a").expect("column"),
                    ColumnRef::new("amount_b").expect("column"),
                ],
                "measure".to_string(),
                "amount".to_string(),
            ),
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
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::MeltRows);
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_written, 6);
        assert_eq!(report.pre_limit_result_row_count, 6);
        assert_eq!(
            report.projected_columns,
            vec![
                "id".to_string(),
                "measure".to_string(),
                "amount".to_string()
            ]
        );
        assert_eq!(
            rows,
            "{\"amount\":10,\"id\":1,\"measure\":\"amount_a\"}\n{\"amount\":100,\"id\":1,\"measure\":\"amount_b\"}\n{\"amount\":20,\"id\":2,\"measure\":\"amount_a\"}\n{\"amount\":200,\"id\":2,\"measure\":\"amount_b\"}\n{\"amount\":30,\"id\":3,\"measure\":\"amount_a\"}\n{\"amount\":300,\"id\":3,\"measure\":\"amount_b\"}\n"
        );
        assert!(report.evidence.pushdown.projection_pushdown_applied);
        assert!(!report.evidence.pushdown.source_order_limit_applied);
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
    fn melt_row_export_writes_mixed_scalar_values_without_fallback() {
        let path = unique_vortex_path("melt-mixed-row-export");
        let output_path =
            unique_vortex_path("melt-mixed-row-export-output").with_extension("jsonl");
        write_melt_mixed_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::melt_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexMeltProjectionRequest::new(
                vec![ColumnRef::new("id").expect("column")],
                vec![
                    ColumnRef::new("amount").expect("column"),
                    ColumnRef::new("label").expect("column"),
                ],
                "field".to_string(),
                "value".to_string(),
            ),
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
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::MeltRows);
        assert_eq!(report.rows_scanned, 2);
        assert_eq!(report.rows_written, 4);
        assert_eq!(report.pre_limit_result_row_count, 4);
        assert_eq!(
            report.projected_columns,
            vec!["id".to_string(), "field".to_string(), "value".to_string()]
        );
        assert_eq!(
            rows,
            "{\"field\":\"amount\",\"id\":1,\"value\":10}\n{\"field\":\"label\",\"id\":1,\"value\":\"paid\"}\n{\"field\":\"amount\",\"id\":2,\"value\":20}\n{\"field\":\"label\",\"id\":2,\"value\":\"trial\"}\n"
        );
        assert!(report.evidence.side_effects.data_read);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(report.evidence.side_effects.data_materialized);
        assert!(!report.evidence.side_effects.external_effects_executed);
        assert!(!report.evidence.side_effects.fallback_attempted);

        let limited_path = unique_vortex_path("pivot-fill-margins-limited-row-export");
        let limited_output_path =
            unique_vortex_path("pivot-fill-margins-limited-row-export-output")
                .with_extension("jsonl");
        write_pivot_struct_fixture(&limited_path).expect("fixture");
        let limited_request = VortexQueryPrimitiveRequest::pivot_rows(
            DatasetUri::new(limited_path.display().to_string()).expect("uri"),
            VortexPivotProjectionRequest::new(
                ColumnRef::new("id").expect("column"),
                ColumnRef::new("label").expect("column"),
                ColumnRef::new("amount").expect("column"),
                "sum",
            )
            .with_output_policy(Some(StatValue::Int64(0)), false, true, "total"),
        )
        .with_source_order_limit(2);

        let limited_report = execute_vortex_local_primitive_row_export_with_policy(
            &limited_request,
            &limited_output_path,
            VortexLocalPrimitiveRowExportFormat::Jsonl,
            false,
            VortexLocalPrimitiveExecutionPolicy::new(1).expect("policy"),
        )
        .expect("limited report");
        let limited_rows = std::fs::read_to_string(&limited_output_path).expect("limited output");
        let limited_parsed_rows = limited_rows
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
            .collect::<Vec<_>>();
        let _ = std::fs::remove_file(&limited_path);
        let _ = std::fs::remove_file(&limited_output_path);

        assert_eq!(
            limited_report.status,
            VortexLocalPrimitiveExecutionStatus::Executed
        );
        assert_eq!(limited_report.rows_written, 2);
        assert_eq!(limited_report.pre_limit_result_row_count, 3);
        assert_eq!(limited_report.source_order_limit_requested, Some(2));
        assert_eq!(limited_parsed_rows.len(), 2);
        assert_eq!(limited_parsed_rows[0]["id"], serde_json::json!(1));
        assert_eq!(limited_parsed_rows[1]["id"], serde_json::json!("total"));
        assert!(limited_report.evidence.pushdown.source_order_limit_applied);
        assert!(!limited_report.evidence.side_effects.fallback_attempted);
    }

    #[test]
    fn pivot_row_export_writes_sparse_wide_rows_without_fallback() {
        let path = unique_vortex_path("pivot-row-export");
        let output_path = unique_vortex_path("pivot-row-export-output").with_extension("jsonl");
        write_pivot_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::pivot_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexPivotProjectionRequest::new(
                ColumnRef::new("id").expect("column"),
                ColumnRef::new("label").expect("column"),
                ColumnRef::new("amount").expect("column"),
                "sum",
            ),
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
        let parsed_rows = rows
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
            .collect::<Vec<_>>();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&output_path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::PivotRows);
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_written, 2);
        assert_eq!(report.pre_limit_result_row_count, 2);
        assert_eq!(
            report.projected_columns,
            vec![
                "id".to_string(),
                "pivot_paid".to_string(),
                "pivot_trial".to_string()
            ]
        );
        assert_eq!(parsed_rows.len(), 2);
        assert_eq!(parsed_rows[0]["id"], serde_json::json!(1));
        assert_eq!(parsed_rows[0]["pivot_paid"].as_f64(), Some(15.0));
        assert!(parsed_rows[0]["pivot_trial"].is_null());
        assert_eq!(parsed_rows[1]["id"], serde_json::json!(2));
        assert!(parsed_rows[1]["pivot_paid"].is_null());
        assert_eq!(parsed_rows[1]["pivot_trial"].as_f64(), Some(7.0));
        assert!(report.evidence.pushdown.projection_pushdown_applied);
        assert!(!report.evidence.pushdown.source_order_limit_applied);
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
    fn pivot_row_export_writes_fill_and_margins_without_fallback() {
        let path = unique_vortex_path("pivot-fill-margins-row-export");
        let output_path =
            unique_vortex_path("pivot-fill-margins-row-export-output").with_extension("jsonl");
        write_pivot_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::pivot_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexPivotProjectionRequest::new(
                ColumnRef::new("id").expect("column"),
                ColumnRef::new("label").expect("column"),
                ColumnRef::new("amount").expect("column"),
                "sum",
            )
            .with_output_policy(Some(StatValue::Int64(0)), false, true, "total"),
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
        let parsed_rows = rows
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
            .collect::<Vec<_>>();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&output_path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::PivotRows);
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(
            report.projected_columns,
            vec![
                "id".to_string(),
                "pivot_paid".to_string(),
                "pivot_trial".to_string(),
                "pivot_total".to_string(),
            ]
        );
        assert_eq!(report.rows_written, 3);
        assert_eq!(report.pre_limit_result_row_count, 3);
        assert_eq!(parsed_rows.len(), 3);
        assert_eq!(parsed_rows[0]["id"], serde_json::json!(1));
        assert_eq!(parsed_rows[0]["pivot_paid"].as_f64(), Some(15.0));
        assert_eq!(parsed_rows[0]["pivot_trial"], serde_json::json!(0));
        assert_eq!(parsed_rows[0]["pivot_total"].as_f64(), Some(15.0));
        assert_eq!(parsed_rows[1]["id"], serde_json::json!(2));
        assert_eq!(parsed_rows[1]["pivot_paid"], serde_json::json!(0));
        assert_eq!(parsed_rows[1]["pivot_trial"].as_f64(), Some(7.0));
        assert_eq!(parsed_rows[1]["pivot_total"].as_f64(), Some(7.0));
        assert_eq!(parsed_rows[2]["id"], serde_json::json!("total"));
        assert_eq!(parsed_rows[2]["pivot_paid"].as_f64(), Some(15.0));
        assert_eq!(parsed_rows[2]["pivot_trial"].as_f64(), Some(7.0));
        assert_eq!(parsed_rows[2]["pivot_total"].as_f64(), Some(22.0));
        assert!(report.evidence.upstream_scan_called);
        assert!(report.evidence.materialization_boundary_reported);
        assert!(report.evidence.side_effects.data_read);
        assert!(report.evidence.side_effects.data_decoded);
        assert!(report.evidence.side_effects.data_materialized);
        assert!(!report.evidence.side_effects.external_effects_executed);
        assert!(!report.evidence.side_effects.fallback_attempted);
    }

    #[test]
    fn pivot_row_export_writes_min_max_aggregates_without_fallback() {
        let path = unique_vortex_path("pivot-min-max-row-export");
        let max_output_path =
            unique_vortex_path("pivot-max-row-export-output").with_extension("jsonl");
        write_pivot_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::pivot_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexPivotProjectionRequest::new(
                ColumnRef::new("id").expect("column"),
                ColumnRef::new("label").expect("column"),
                ColumnRef::new("amount").expect("column"),
                "max",
            ),
        );

        let report = execute_vortex_local_primitive_row_export_with_policy(
            &request,
            &max_output_path,
            VortexLocalPrimitiveRowExportFormat::Jsonl,
            false,
            VortexLocalPrimitiveExecutionPolicy::new(1).expect("policy"),
        )
        .expect("report");
        let rows = std::fs::read_to_string(&max_output_path).expect("output");
        let parsed_rows = rows
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
            .collect::<Vec<_>>();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&max_output_path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::PivotRows);
        assert_eq!(parsed_rows.len(), 2);
        assert_eq!(parsed_rows[0]["pivot_paid"].as_f64(), Some(10.0));
        assert!(parsed_rows[0]["pivot_trial"].is_null());
        assert!(parsed_rows[1]["pivot_paid"].is_null());
        assert_eq!(parsed_rows[1]["pivot_trial"].as_f64(), Some(7.0));
        assert!(!report.evidence.side_effects.fallback_attempted);
    }

    #[test]
    fn explode_row_export_writes_expanded_rows_without_fallback() {
        let path = unique_vortex_path("explode-row-export");
        let output_path = unique_vortex_path("explode-row-export-output").with_extension("jsonl");
        write_explode_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::explode_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![
                ColumnRef::new("id").expect("column"),
                ColumnRef::new("items").expect("column"),
            ]),
            VortexExplodeProjectionRequest::new(ColumnRef::new("items").expect("column")),
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
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::ExplodeRows);
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_written, 3);
        assert_eq!(report.pre_limit_result_row_count, 3);
        assert_eq!(
            report.projected_columns,
            vec!["id".to_string(), "items".to_string()]
        );
        assert_eq!(
            rows,
            "{\"id\":1,\"items\":7}\n{\"id\":1,\"items\":8}\n{\"id\":3,\"items\":9}\n"
        );
        assert!(report.evidence.pushdown.projection_pushdown_applied);
        assert!(!report.evidence.pushdown.source_order_limit_applied);
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
    fn explode_row_export_writes_multi_column_expanded_rows_without_fallback() {
        let path = unique_vortex_path("multi-explode-row-export");
        let output_path =
            unique_vortex_path("multi-explode-row-export-output").with_extension("jsonl");
        write_multi_explode_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::explode_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![
                ColumnRef::new("id").expect("column"),
                ColumnRef::new("items").expect("column"),
                ColumnRef::new("labels").expect("column"),
            ]),
            VortexExplodeProjectionRequest::new(ColumnRef::new("items").expect("column"))
                .with_columns(vec![
                    ColumnRef::new("items").expect("column"),
                    ColumnRef::new("labels").expect("column"),
                ]),
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
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::ExplodeRows);
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_written, 3);
        assert_eq!(report.pre_limit_result_row_count, 3);
        assert_eq!(
            report.projected_columns,
            vec!["id".to_string(), "items".to_string(), "labels".to_string()]
        );
        assert_eq!(
            rows,
            "{\"id\":1,\"items\":7,\"labels\":\"red\"}\n{\"id\":1,\"items\":8,\"labels\":\"blue\"}\n{\"id\":3,\"items\":9,\"labels\":\"green\"}\n"
        );
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
    fn explode_row_export_writes_nullable_nested_rows_without_fallback() {
        let path = unique_vortex_path("nullable-nested-explode-row-export");
        let output_path =
            unique_vortex_path("nullable-nested-explode-row-export-output").with_extension("jsonl");
        write_nullable_nested_explode_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::explode_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![
                ColumnRef::new("id").expect("column"),
                ColumnRef::new("items").expect("column"),
            ]),
            VortexExplodeProjectionRequest::new(ColumnRef::new("items").expect("column")),
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
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::ExplodeRows);
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_written, 4);
        assert_eq!(report.pre_limit_result_row_count, 4);
        assert_eq!(
            report.projected_columns,
            vec!["id".to_string(), "items".to_string()]
        );
        assert_eq!(
            rows,
            "{\"id\":1,\"items\":{\"code\":7,\"label\":\"red\"}}\n{\"id\":1,\"items\":{\"code\":8,\"label\":\"blue\"}}\n{\"id\":2,\"items\":null}\n{\"id\":3,\"items\":{\"code\":9,\"label\":\"green\"}}\n"
        );
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
    fn explode_row_export_writes_dotted_struct_field_rows_without_fallback() {
        let path = unique_vortex_path("dotted-field-explode-row-export");
        let output_path =
            unique_vortex_path("dotted-field-explode-row-export-output").with_extension("jsonl");
        write_nullable_nested_explode_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::explode_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![
                ColumnRef::new("id").expect("column"),
                ColumnRef::new("items").expect("column"),
            ]),
            VortexExplodeProjectionRequest::new(ColumnRef::new("items").expect("column"))
                .with_element_field("code".to_string(), "code".to_string()),
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
        let parsed_rows = rows
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
            .collect::<Vec<_>>();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&output_path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::ExplodeRows);
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_written, 4);
        assert_eq!(report.pre_limit_result_row_count, 4);
        assert_eq!(
            report.projected_columns,
            vec!["id".to_string(), "code".to_string()]
        );
        assert_eq!(
            parsed_rows,
            vec![
                serde_json::json!({"id": 1, "code": 7}),
                serde_json::json!({"id": 1, "code": 8}),
                serde_json::json!({"id": 2, "code": null}),
                serde_json::json!({"id": 3, "code": 9}),
            ]
        );
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
    fn simple_aggregate_accumulates_scalar_measures_without_fallback() {
        let path = unique_vortex_path("simple-aggregate");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexSimpleAggregateRequest::new(vec![
                crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
                crate::VortexSimpleAggregateMeasure::new(
                    "sum",
                    Some(ColumnRef::new("metric").expect("column")),
                    "sum_metric".to_string(),
                ),
                crate::VortexSimpleAggregateMeasure::new(
                    "avg",
                    Some(ColumnRef::new("metric").expect("column")),
                    "avg_metric".to_string(),
                ),
                crate::VortexSimpleAggregateMeasure::new(
                    "min",
                    Some(ColumnRef::new("value").expect("column")),
                    "min_value".to_string(),
                ),
                crate::VortexSimpleAggregateMeasure::new(
                    "max",
                    Some(ColumnRef::new("value").expect("column")),
                    "max_value".to_string(),
                ),
            ]),
        );

        let report = execute_vortex_local_primitive(&request).expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::SimpleAggregate
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_selected, Some(5));
        assert_eq!(report.rows_projected, Some(1));
        assert!(report.upstream_scan_called);
        assert!(report.data_read);
        assert!(report.data_decoded);
        assert!(report.data_materialized);
        assert!(report.row_read);
        assert!(!report.external_effects_executed);
        assert!(!report.fallback_execution_allowed);
        assert!(report.state_budget.state_budget_required);
        assert_eq!(report.state_budget.state_family, "scalar_aggregate_state");
        assert!(
            report
                .state_budget
                .capillary_work_units
                .contains(&"aggregate_state".to_string())
        );
        assert!(
            report
                .state_budget
                .pulseweave_pressure_signals
                .contains(&"aggregate_input_rows".to_string())
        );
        assert_eq!(
            report.state_budget.spill_policy,
            "fail_closed_before_uncertified_spill"
        );
        let summary = report.result_summary.expect("summary");
        assert!(summary.contains("\"rows\":5"));
        assert!(summary.contains("\"sum_metric\":150.0"));
        assert!(summary.contains("\"avg_metric\":30.0"));
        assert!(summary.contains("\"min_value\":1"));
        assert!(summary.contains("\"max_value\":5"));
    }

    #[test]
    fn simple_aggregate_applies_sum_argument_offsets_without_fallback() {
        let path = unique_vortex_path("simple-aggregate-offset");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexSimpleAggregateRequest::new(vec![
                crate::VortexSimpleAggregateMeasure::new(
                    "sum",
                    Some(ColumnRef::new("metric").expect("column")),
                    "sum_metric_plus_2".to_string(),
                )
                .with_argument_offset(2),
                crate::VortexSimpleAggregateMeasure::new(
                    "sum",
                    Some(ColumnRef::new("metric").expect("column")),
                    "sum_metric_minus_1".to_string(),
                )
                .with_argument_offset(-1),
            ]),
        );

        let report = execute_vortex_local_primitive(&request).expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_selected, Some(5));
        assert_eq!(report.rows_projected, Some(1));
        assert!(report.upstream_scan_called);
        assert!(!report.external_effects_executed);
        assert!(!report.fallback_execution_allowed);
        let summary = report.result_summary.expect("summary");
        assert!(summary.contains("\"sum_metric_plus_2\":160.0"));
        assert!(summary.contains("\"sum_metric_minus_1\":145.0"));
    }

    #[test]
    fn grouped_aggregate_accumulates_group_rows_without_fallback() {
        let path = unique_vortex_path("grouped-aggregate");
        write_pivot_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexSimpleAggregateRequest::grouped(
                vec![ColumnRef::new("label").expect("column")],
                vec![
                    crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
                    crate::VortexSimpleAggregateMeasure::new(
                        "sum",
                        Some(ColumnRef::new("amount").expect("column")),
                        "total_amount".to_string(),
                    ),
                ],
            ),
        );

        let report = execute_vortex_local_primitive(&request).expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::SimpleAggregate
        );
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_selected, Some(3));
        assert_eq!(report.rows_projected, Some(2));
        assert!(report.upstream_scan_called);
        assert!(report.data_read);
        assert!(report.data_decoded);
        assert!(report.data_materialized);
        assert!(!report.external_effects_executed);
        assert!(!report.fallback_execution_allowed);
        assert!(report.state_budget.state_budget_required);
        assert!(
            report
                .state_budget
                .state_family
                .contains("grouped_aggregate_state")
        );
        assert!(
            report
                .state_budget
                .pulseweave_pressure_signals
                .contains(&"group_cardinality".to_string())
        );
        let summary = report.result_summary.expect("summary");
        assert!(summary.contains("\"group_by\":\"label\""));
        assert!(summary.contains("\"label\":\"paid\""));
        assert!(summary.contains("\"label\":\"trial\""));
        assert!(summary.contains("\"total_amount\":15.0"));
        assert!(summary.contains("\"total_amount\":7.0"));
    }

    #[test]
    fn grouped_aggregate_filters_orders_and_limits_rows_without_fallback() {
        let path = unique_vortex_path("grouped-aggregate-filtered-topk");
        write_pivot_struct_fixture(&path).expect("fixture");
        let mut request = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexSimpleAggregateRequest::grouped(
                vec![ColumnRef::new("label").expect("column")],
                vec![
                    crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
                    crate::VortexSimpleAggregateMeasure::new(
                        "sum",
                        Some(ColumnRef::new("amount").expect("column")),
                        "total_amount".to_string(),
                    ),
                ],
            )
            .with_order_by(vec![crate::VortexAggregateOrderExpr::new(
                "total_amount",
                true,
            )]),
        )
        .with_source_order_limit(1);
        request.predicate = Some(PredicateExpr::Compare {
            column: ColumnRef::new("amount").expect("column"),
            op: ComparisonOp::GtEq,
            value: StatValue::Int64(7),
        });

        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(1).expect("policy"),
        )
        .expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::SimpleAggregate
        );
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_selected, Some(2));
        assert_eq!(report.rows_projected, Some(1));
        assert_eq!(report.source_order_limit_requested, Some(1));
        assert!(report.source_order_limit_applied);
        assert_eq!(report.source_order_limit_input_rows, Some(2));
        assert_eq!(report.source_order_limit_rows_output, Some(1));
        assert!(report.filter_pushdown_applied);
        assert!(report.upstream_filter_expression_used);
        assert!(report.projection_pushdown_applied);
        assert!(report.upstream_scan_called);
        assert!(report.data_read);
        assert!(report.data_decoded);
        assert!(report.data_materialized);
        assert!(report.row_read);
        assert!(!report.external_effects_executed);
        assert!(!report.fallback_execution_allowed);
        assert!(report.state_budget.state_budget_required);
        assert_eq!(
            report.state_budget.state_family,
            "grouped_aggregate_state+topk"
        );
        assert!(
            report
                .state_budget
                .capillary_work_units
                .contains(&"grouped_topk_heap".to_string())
        );
        assert!(
            report
                .state_budget
                .pulseweave_pressure_signals
                .contains(&"topk_heap_rows".to_string())
        );
        let summary = report.result_summary.expect("summary");
        assert!(summary.contains("\"rows\":1"));
        assert!(summary.contains("\"label\":\"paid\""));
        assert!(summary.contains("\"total_amount\":10.0"));
        assert!(!summary.contains("\"label\":\"trial\""));
    }

    #[test]
    fn sort_rows_reports_topk_offset_state_budget_without_fallback() {
        let path = unique_vortex_path("sort-rows-state-budget");
        write_pivot_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::sort_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![
                ColumnRef::new("id").expect("column"),
                ColumnRef::new("label").expect("column"),
                ColumnRef::new("amount").expect("column"),
            ]),
            None,
            VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("amount", true)])
                .with_offset(1),
            1,
        );

        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(1).expect("policy"),
        )
        .expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SortRows);
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_selected, Some(1));
        assert!(report.full_stream_collected);
        assert!(report.data_decoded);
        assert!(report.data_materialized);
        assert!(!report.external_effects_executed);
        assert!(!report.fallback_execution_allowed);
        assert!(report.state_budget.state_budget_required);
        assert_eq!(
            report.state_budget.state_family,
            "raw_row_topk_sort_state+offset"
        );
        assert!(
            report
                .state_budget
                .capillary_work_units
                .contains(&"offset_drain".to_string())
        );
        assert!(
            report
                .state_budget
                .pulseweave_pressure_signals
                .contains(&"materialized_sort_rows".to_string())
        );
        assert!(report.state_budget.fail_closed_if_spill_required);
    }

    #[test]
    fn sort_rows_keep_last_uses_source_order_tie_reversal_without_fallback() {
        let path = unique_vortex_path("sort-rows-keep-last");
        write_sort_tie_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::sort_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![
                ColumnRef::new("key").expect("column"),
                ColumnRef::new("metric").expect("column"),
            ]),
            None,
            VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("key", false)])
                .with_tie_policy(VortexSortTiePolicy::Last),
            1,
        );

        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(1).expect("policy"),
        )
        .expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SortRows);
        assert!(report.state_budget.state_family.contains("tie_last"));
        assert!(
            report
                .state_budget
                .capillary_work_units
                .contains(&"source_order_tie_reversal".to_string())
        );
        let summary = report.result_summary.expect("summary");
        assert!(summary.contains("\"tie_policy\":\"last\""));
        assert!(summary.contains("\"metric\":20"));
        assert!(!summary.contains("\"metric\":10"));
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn sort_rows_keep_all_expands_cutoff_ties_without_fallback() {
        let path = unique_vortex_path("sort-rows-keep-all");
        write_sort_tie_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::sort_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![
                ColumnRef::new("key").expect("column"),
                ColumnRef::new("metric").expect("column"),
            ]),
            None,
            VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("key", false)])
                .with_tie_policy(VortexSortTiePolicy::All),
            3,
        );

        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(1).expect("policy"),
        )
        .expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SortRows);
        assert_eq!(report.rows_projected, Some(4));
        assert!(report.state_budget.state_family.contains("tie_all"));
        assert!(
            report
                .state_budget
                .capillary_work_units
                .contains(&"cutoff_tie_expansion".to_string())
        );
        let summary = report.result_summary.expect("summary");
        assert!(summary.contains("\"tie_policy\":\"all\""));
        assert!(summary.contains("\"rows\":4"));
        assert!(summary.contains("\"metric\":40"));
        assert!(!summary.contains("\"metric\":50"));
        assert!(!report.fallback_execution_allowed);
    }

    #[test]
    fn sort_rows_preserves_exact_integer_ordering_without_fallback() {
        let path = unique_vortex_path("sort-rows-exact-integer");
        write_large_int_sort_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::sort_rows(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            ProjectionRequest::columns(vec![
                ColumnRef::new("id").expect("column"),
                ColumnRef::new("large").expect("column"),
            ]),
            None,
            VortexSortRowsRequest::new(vec![crate::VortexAggregateOrderExpr::new("large", false)]),
            1,
        );

        let report = execute_vortex_local_primitive_with_policy(
            &request,
            VortexLocalPrimitiveExecutionPolicy::new(1).expect("policy"),
        )
        .expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.primitive_kind, VortexQueryPrimitiveKind::SortRows);
        assert_eq!(report.rows_scanned, 2);
        assert_eq!(report.rows_projected, Some(1));
        assert!(!report.external_effects_executed);
        assert!(!report.fallback_execution_allowed);
        let summary = report.result_summary.expect("summary");
        assert!(summary.contains("\"id\":2"));
        assert!(summary.contains("9007199254740992"));
        assert!(!summary.contains("9007199254740993"));
    }

    #[test]
    fn grouped_aggregate_applies_expression_groups_value_transforms_and_having_without_fallback() {
        let path = unique_vortex_path("grouped-aggregate-expressions");
        write_pivot_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexSimpleAggregateRequest::grouped(
                vec![ColumnRef::new("label").expect("column")],
                vec![
                    crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
                    crate::VortexSimpleAggregateMeasure::new(
                        "avg",
                        Some(ColumnRef::new("label").expect("column")),
                        "avg_label_len".to_string(),
                    )
                    .with_value_transform("length"),
                ],
            )
            .with_group_expressions(vec![
                crate::VortexAggregateExpression::new(
                    "amount_minus_1".to_string(),
                    ColumnRef::new("amount").expect("column"),
                    "add_offset",
                )
                .with_argument_offset(-1),
            ])
            .with_having(vec![crate::VortexAggregateHavingExpr::new(
                "rows",
                ComparisonOp::GtEq,
                "1",
            )]),
        );

        let report = execute_vortex_local_primitive(&request).expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_selected, Some(3));
        assert_eq!(report.rows_projected, Some(3));
        assert!(report.data_decoded);
        assert!(report.data_materialized);
        assert!(report.row_read);
        assert!(!report.external_effects_executed);
        assert!(!report.fallback_execution_allowed);
        let summary = report.result_summary.expect("summary");
        assert!(summary.contains("\"amount_minus_1\":9"));
        assert!(summary.contains("\"amount_minus_1\":4"));
        assert!(summary.contains("\"amount_minus_1\":6"));
        assert!(summary.contains("\"avg_label_len\":4.0"));
        assert!(summary.contains("\"avg_label_len\":5.0"));
    }

    #[test]
    fn grouped_aggregate_uses_expression_groups_without_column_group_by() {
        let path = unique_vortex_path("grouped-aggregate-expression-only");
        write_pivot_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexSimpleAggregateRequest::grouped(
                Vec::new(),
                vec![
                    crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
                    crate::VortexSimpleAggregateMeasure::new(
                        "sum",
                        Some(ColumnRef::new("amount").expect("column")),
                        "total_amount".to_string(),
                    ),
                ],
            )
            .with_group_expressions(vec![
                crate::VortexAggregateExpression::new(
                    "amount_bucket".to_string(),
                    ColumnRef::new("amount").expect("column"),
                    "add_offset",
                )
                .with_argument_offset(-5),
            ]),
        );

        let report = execute_vortex_local_primitive(&request).expect("report");
        let _ = std::fs::remove_file(&path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::SimpleAggregate
        );
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_projected, Some(3));
        assert!(!report.external_effects_executed);
        assert!(!report.fallback_execution_allowed);
        assert!(
            report
                .state_budget
                .state_family
                .contains("grouped_aggregate_state")
        );
        let summary = report.result_summary.expect("summary");
        assert!(summary.contains("\"amount_bucket\":5"));
        assert!(summary.contains("\"amount_bucket\":0"));
        assert!(summary.contains("\"amount_bucket\":2"));
        assert!(summary.contains("\"total_amount\":10.0"));
        assert!(summary.contains("\"total_amount\":5.0"));
        assert!(summary.contains("\"total_amount\":7.0"));
    }

    #[test]
    fn aggregate_expression_transforms_cover_datetime_domain_and_case_without_fallback() {
        let extract_minute =
            AggregateValueTransform::from_parts("extract_minute", None).expect("extract");
        let date_trunc =
            AggregateValueTransform::from_parts("date_trunc_minute", None).expect("date trunc");
        let domain = AggregateValueTransform::from_parts("url_domain", None).expect("domain");
        let case =
            AggregateValueTransform::from_parts("case_search_adv_zero_referer_else_empty", None)
                .expect("case");

        assert_eq!(
            extract_minute
                .apply(&StatValue::Utf8("2026-06-18 12:34:56".to_string()))
                .expect("minute"),
            StatValue::UInt64(34)
        );
        assert_eq!(
            date_trunc
                .apply(&StatValue::Utf8("2026-06-18T12:34:56".to_string()))
                .expect("trunc"),
            StatValue::Utf8("2026-06-18T12:34:00".to_string())
        );
        assert_eq!(
            domain
                .apply(&StatValue::Utf8(
                    "https://www.example.com/path?q=1".to_string()
                ))
                .expect("domain"),
            StatValue::Utf8("example.com".to_string())
        );
        assert_eq!(
            case.apply_values(
                &StatValue::Utf8("https://source.example/path".to_string()),
                &[&StatValue::Int64(0), &StatValue::Int64(0)]
            )
            .expect("case true"),
            StatValue::Utf8("https://source.example/path".to_string())
        );
        assert_eq!(
            case.apply_values(
                &StatValue::Utf8("https://source.example/path".to_string()),
                &[&StatValue::Int64(1), &StatValue::Int64(0)]
            )
            .expect("case false"),
            StatValue::Utf8(String::new())
        );
    }

    #[test]
    fn aggregate_count_distinct_accumulates_scalar_and_grouped_state_without_fallback() {
        let scalar_path = unique_vortex_path("count-distinct-scalar");
        let grouped_path = unique_vortex_path("count-distinct-grouped");
        write_duplicate_struct_fixture(&scalar_path).expect("scalar fixture");
        write_pivot_struct_fixture(&grouped_path).expect("grouped fixture");
        let scalar_request = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(scalar_path.display().to_string()).expect("uri"),
            VortexSimpleAggregateRequest::new(vec![crate::VortexSimpleAggregateMeasure::new(
                "count_distinct",
                Some(ColumnRef::new("value").expect("column")),
                "unique_values".to_string(),
            )]),
        );
        let grouped_request = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(grouped_path.display().to_string()).expect("uri"),
            VortexSimpleAggregateRequest::grouped(
                vec![ColumnRef::new("label").expect("column")],
                vec![crate::VortexSimpleAggregateMeasure::new(
                    "count_distinct",
                    Some(ColumnRef::new("id").expect("column")),
                    "unique_ids".to_string(),
                )],
            ),
        );

        let scalar_report = execute_vortex_local_primitive(&scalar_request).expect("scalar report");
        let grouped_report =
            execute_vortex_local_primitive(&grouped_request).expect("grouped report");
        let _ = std::fs::remove_file(&scalar_path);
        let _ = std::fs::remove_file(&grouped_path);

        assert_eq!(
            scalar_report.status,
            VortexLocalPrimitiveExecutionStatus::Executed
        );
        assert_eq!(scalar_report.rows_scanned, 5);
        assert_eq!(scalar_report.rows_projected, Some(1));
        assert_eq!(
            scalar_report.state_budget.state_family,
            "scalar_count_distinct_state"
        );
        assert!(
            scalar_report
                .state_budget
                .capillary_work_units
                .contains(&"count_distinct_set".to_string())
        );
        let scalar_summary = scalar_report.result_summary.expect("scalar summary");
        assert!(scalar_summary.contains("\"unique_values\":3"));
        assert!(!scalar_report.external_effects_executed);
        assert!(!scalar_report.fallback_execution_allowed);

        assert_eq!(
            grouped_report.status,
            VortexLocalPrimitiveExecutionStatus::Executed
        );
        assert_eq!(grouped_report.rows_scanned, 3);
        assert_eq!(grouped_report.rows_projected, Some(2));
        assert!(
            grouped_report
                .state_budget
                .state_family
                .contains("count_distinct")
        );
        assert!(
            grouped_report
                .state_budget
                .pulseweave_pressure_signals
                .contains(&"distinct_value_cardinality".to_string())
        );
        let grouped_summary = grouped_report.result_summary.expect("grouped summary");
        assert!(grouped_summary.contains("\"label\":\"paid\""));
        assert!(grouped_summary.contains("\"label\":\"trial\""));
        assert!(grouped_summary.contains("\"unique_ids\":1"));
        assert!(!grouped_report.external_effects_executed);
        assert!(!grouped_report.fallback_execution_allowed);
    }

    #[test]
    fn simple_aggregate_row_export_writes_scalar_result_without_fallback() {
        let path = unique_vortex_path("simple-aggregate-row-export");
        let output_path =
            unique_vortex_path("simple-aggregate-row-export-output").with_extension("jsonl");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexSimpleAggregateRequest::new(vec![
                crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
                crate::VortexSimpleAggregateMeasure::new(
                    "sum",
                    Some(ColumnRef::new("metric").expect("column")),
                    "sum_metric".to_string(),
                ),
            ]),
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
        let parsed = serde_json::from_str::<serde_json::Value>(rows.trim()).expect("json row");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&output_path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::SimpleAggregate
        );
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_written, 1);
        assert_eq!(report.pre_limit_result_row_count, 1);
        assert_eq!(
            report.projected_columns,
            vec!["rows".to_string(), "sum_metric".to_string()]
        );
        assert_eq!(parsed["rows"], serde_json::json!(5));
        assert_eq!(parsed["sum_metric"].as_f64(), Some(150.0));
        assert!(report.evidence.pushdown.projection_pushdown_applied);
        assert!(!report.evidence.pushdown.source_order_limit_applied);
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
    fn simple_aggregate_having_can_filter_scalar_result_without_fallback() {
        let path = unique_vortex_path("simple-aggregate-having-filtered");
        let output_path =
            unique_vortex_path("simple-aggregate-having-filtered-output").with_extension("jsonl");
        write_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexSimpleAggregateRequest::new(vec![crate::VortexSimpleAggregateMeasure::new(
                "count",
                None,
                "rows".to_string(),
            )])
            .with_having(vec![crate::VortexAggregateHavingExpr::new(
                "rows",
                ComparisonOp::Gt,
                "10",
            )]),
        );

        let report = execute_vortex_local_primitive(&request).expect("report");
        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(report.rows_scanned, 5);
        assert_eq!(report.rows_projected, Some(0));
        assert!(!report.external_effects_executed);
        assert!(!report.fallback_execution_allowed);
        let summary = report.result_summary.as_deref().expect("summary");
        assert!(summary.contains("\"rows\":0"));
        assert!(summary.contains("\"values\":{}"));

        let export_report = execute_vortex_local_primitive_row_export_with_policy(
            &request,
            &output_path,
            VortexLocalPrimitiveRowExportFormat::Jsonl,
            false,
            VortexLocalPrimitiveExecutionPolicy::new(1).expect("policy"),
        )
        .expect("row export report");
        let rows = std::fs::read_to_string(&output_path).expect("output");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&output_path);

        assert_eq!(
            export_report.status,
            VortexLocalPrimitiveExecutionStatus::Executed
        );
        assert_eq!(export_report.rows_written, 0);
        assert_eq!(export_report.pre_limit_result_row_count, 0);
        assert!(rows.trim().is_empty());
        assert!(!export_report.evidence.side_effects.fallback_attempted);
        assert!(
            !export_report
                .evidence
                .side_effects
                .fallback_execution_allowed
        );
    }

    #[test]
    fn grouped_aggregate_row_export_writes_group_rows_without_fallback() {
        let path = unique_vortex_path("grouped-aggregate-row-export");
        let output_path =
            unique_vortex_path("grouped-aggregate-row-export-output").with_extension("jsonl");
        write_pivot_struct_fixture(&path).expect("fixture");
        let request = VortexQueryPrimitiveRequest::simple_aggregate(
            DatasetUri::new(path.display().to_string()).expect("uri"),
            VortexSimpleAggregateRequest::grouped(
                vec![ColumnRef::new("label").expect("column")],
                vec![
                    crate::VortexSimpleAggregateMeasure::new("count", None, "rows".to_string()),
                    crate::VortexSimpleAggregateMeasure::new(
                        "sum",
                        Some(ColumnRef::new("amount").expect("column")),
                        "total_amount".to_string(),
                    ),
                ],
            ),
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
        let parsed_rows = rows
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("json row"))
            .collect::<Vec<_>>();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&output_path);

        assert_eq!(report.status, VortexLocalPrimitiveExecutionStatus::Executed);
        assert_eq!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::SimpleAggregate
        );
        assert_eq!(report.rows_scanned, 3);
        assert_eq!(report.rows_written, 2);
        assert_eq!(report.pre_limit_result_row_count, 2);
        assert_eq!(
            report.projected_columns,
            vec![
                "label".to_string(),
                "rows".to_string(),
                "total_amount".to_string()
            ]
        );
        assert_eq!(parsed_rows.len(), 2);
        assert_eq!(parsed_rows[0]["label"], serde_json::json!("paid"));
        assert_eq!(parsed_rows[0]["rows"], serde_json::json!(2));
        assert_eq!(parsed_rows[0]["total_amount"].as_f64(), Some(15.0));
        assert_eq!(parsed_rows[1]["label"], serde_json::json!("trial"));
        assert_eq!(parsed_rows[1]["rows"], serde_json::json!(1));
        assert_eq!(parsed_rows[1]["total_amount"].as_f64(), Some(7.0));
        assert!(report.evidence.pushdown.projection_pushdown_applied);
        assert!(report.evidence.upstream_scan_called);
        assert!(report.evidence.materialization_boundary_reported);
        assert!(!report.evidence.side_effects.external_effects_executed);
        assert!(!report.evidence.side_effects.fallback_attempted);
        assert!(!report.evidence.side_effects.fallback_execution_allowed);
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
