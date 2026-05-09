#![allow(clippy::must_use_candidate)]

use std::fmt::Write as _;

#[cfg(feature = "vortex-local-primitives")]
use shardloom_core::UriScheme;
#[cfg(feature = "vortex-local-primitives")]
use shardloom_core::{ComparisonOp, DatasetUri, StatValue};
use shardloom_core::{
    CorrectnessFixture, Diagnostic, DiagnosticCode, DiagnosticSeverity, ExecutionCertificate,
    ExecutionCertificateInput, ExpectedOutcome, NativeIoAdapterFidelityReport, NativeIoCertificate,
    NativeIoRepresentationTransition, NativeIoSideEffectReport, NativeIoSinkRequirementReport,
    NativeIoSourceCapabilityReport, NativeIoSourcePushdownReport, PredicateExpr,
    RepresentationState, Result, ShardLoomError,
};
#[cfg(feature = "vortex-local-primitives")]
use shardloom_plan::ProjectionRequest;

use crate::{VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest};

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
    pub max_chunk_rows: usize,
    pub streaming_scan_used: bool,
    pub full_stream_collected: bool,
    pub max_parallelism_requested: usize,
    pub scan_concurrency_per_worker: usize,
    pub filter_pushdown_applied: bool,
    pub projection_pushdown_applied: bool,
    pub upstream_filter_expression_used: bool,
    pub upstream_projection_expression_used: bool,
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
            max_chunk_rows: 0,
            streaming_scan_used: false,
            full_stream_collected: false,
            max_parallelism_requested: 1,
            scan_concurrency_per_worker: 1,
            filter_pushdown_applied: false,
            projection_pushdown_applied: false,
            upstream_filter_expression_used: false,
            upstream_projection_expression_used: false,
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
    input.correctness_passed = local_primitive_correctness_passed(
        &fixture.expected,
        request,
        report,
        input.actual_outcome.as_ref(),
    );
    input.diagnostics.extend(request.diagnostics.clone());
    input.diagnostics.extend(report.diagnostics.clone());
    Ok(ExecutionCertificate::evaluate(input))
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
        Vec::new(),
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
        && !report.full_stream_collected
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
            report.projection_pushdown_applied
                && report.upstream_projection_expression_used
                && !report.projected_columns.is_empty()
        }
        VortexQueryPrimitiveKind::FilterAndProject => {
            report.filter_pushdown_applied
                && report.projection_pushdown_applied
                && report.upstream_filter_expression_used
                && report.upstream_projection_expression_used
                && !report.projected_columns.is_empty()
        }
        VortexQueryPrimitiveKind::SimpleAggregate | VortexQueryPrimitiveKind::Unsupported => false,
    }
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
            "local primitive native I/O certificate requires a successful local Vortex scan-pushdown report with no decode, materialization, row reads, Arrow conversion, object-store IO, writes, spill, external effects, or fallback",
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
    NativeIoSourcePushdownReport {
        accepted_operations: if safe {
            local_primitive_accepted_operations(report)
        } else {
            Vec::new()
        },
        rejected_operations: if safe {
            Vec::new()
        } else {
            vec![report.primitive_kind.as_str().to_string()]
        },
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
            None
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
            "exact_filter_project_from_single_vortex_scan_pushdown"
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
            "local Vortex scan applied filter and projection pushdown in one scan without row reads"
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
    let to_state = match report.primitive_kind {
        VortexQueryPrimitiveKind::CountWhere
        | VortexQueryPrimitiveKind::FilterPredicate
        | VortexQueryPrimitiveKind::FilterAndProject => RepresentationState::SelectionVectorEncoded,
        VortexQueryPrimitiveKind::CountAll | VortexQueryPrimitiveKind::ProjectColumns => {
            RepresentationState::VortexEncoded
        }
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
    NativeIoSinkRequirementReport {
        target_format: local_primitive_sink_target_format(report).to_string(),
        accepts_encoded: safe,
        requires_decoded_columnar: false,
        requires_rows: false,
        preserves_metadata: !matches!(
            report.primitive_kind,
            VortexQueryPrimitiveKind::CountAll | VortexQueryPrimitiveKind::CountWhere
        ),
        requires_ordering: false,
        requires_partitioning: false,
        requires_commit: false,
        supports_streaming: safe && report.streaming_scan_used,
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
        VortexQueryPrimitiveKind::SimpleAggregate | VortexQueryPrimitiveKind::Unsupported => {
            "unsupported_result"
        }
    }
}

fn local_primitive_adapter_fidelity_report(
    safe: bool,
    report: &VortexLocalPrimitiveExecutionReport,
) -> NativeIoAdapterFidelityReport {
    NativeIoAdapterFidelityReport {
        adapter_id: "shardloom.adapter.vortex.local_primitive.v1".to_string(),
        source_kind: "vortex".to_string(),
        sink_kind: local_primitive_sink_target_format(report).to_string(),
        metadata_preserved: safe,
        statistics_preserved: safe,
        encoded_representation_preserved: safe,
        materialization_required: false,
        fidelity_loss: if safe {
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
                    "rows_selected={rows};rows_projected={rows};projected_columns={}",
                    report.projected_columns.join(",")
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
        VortexQueryPrimitiveKind::FilterAndProject => {
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
    effects
}

fn local_primitive_unsafe_effect_detected(report: &VortexLocalPrimitiveExecutionReport) -> bool {
    report.has_errors()
        || report.diagnostics.iter().any(|d| {
            matches!(
                d.severity,
                DiagnosticSeverity::Error | DiagnosticSeverity::Fatal
            )
        })
        || report.data_decoded
        || report.data_materialized
        || report.row_read
        || report.arrow_converted
        || report.object_store_io
        || report.write_io
        || report.spill_io_performed
        || report.external_effects_executed
        || report.fallback_execution_allowed
}

fn local_primitive_correctness_passed(
    expected: &ExpectedOutcome,
    request: &VortexQueryPrimitiveRequest,
    report: &VortexLocalPrimitiveExecutionReport,
    actual: Option<&ExpectedOutcome>,
) -> bool {
    report.status == VortexLocalPrimitiveExecutionStatus::Executed
        && request.kind == report.primitive_kind
        && Some(expected) == actual
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

#[cfg(feature = "vortex-local-primitives")]
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
            let scan = read_local_vortex_scan(&path, request.kind, policy, |_| {
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
            let scan = read_local_vortex_scan(&path, request.kind, policy, |dtype| {
                Ok(LocalVortexScanPlan::filter(predicate_to_vortex_expr(
                    predicate,
                    dtype,
                    request.kind,
                )?))
            })?;
            predicate_report(request.kind, &scan, predicate)
        }
        VortexQueryPrimitiveKind::ProjectColumns => {
            let scan = read_local_vortex_scan(&path, request.kind, policy, |dtype| {
                projection_scan_plan(dtype, &request.projection, request.kind)
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
            let scan = read_local_vortex_scan(&path, request.kind, policy, |dtype| {
                let mut plan = projection_scan_plan(dtype, &request.projection, request.kind)?;
                plan.filter = Some(predicate_to_vortex_expr(predicate, dtype, request.kind)?);
                Ok(plan)
            })?;
            filter_and_project_report(request.kind, &scan)
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
    arrays_read_count: usize,
    max_chunk_rows: usize,
    max_parallelism_requested: usize,
    scan_concurrency_per_worker: usize,
    projected_columns: Vec<String>,
    filter_pushdown_applied: bool,
    projection_pushdown_applied: bool,
}

#[cfg(feature = "vortex-local-primitives")]
struct LocalVortexScanPlan {
    filter: Option<vortex::array::expr::Expression>,
    projection: Option<vortex::array::expr::Expression>,
    projected_columns: Vec<String>,
}
#[cfg(feature = "vortex-local-primitives")]
impl LocalVortexScanPlan {
    fn passthrough() -> Self {
        Self {
            filter: None,
            projection: None,
            projected_columns: Vec::new(),
        }
    }

    fn filter(filter: vortex::array::expr::Expression) -> Self {
        Self {
            filter: Some(filter),
            projection: None,
            projected_columns: Vec::new(),
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
fn read_local_vortex_scan(
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
    let mut result_row_count = 0usize;
    let mut arrays_read_count = 0usize;
    let mut max_chunk_rows = 0usize;
    for chunk in scan.into_array_iter(&runtime).map_err(vortex_error)? {
        let chunk = chunk.map_err(vortex_error)?;
        let rows = chunk.len();
        result_row_count = result_row_count.checked_add(rows).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "local Vortex primitive result row count overflowed usize".to_string(),
            )
        })?;
        max_chunk_rows = max_chunk_rows.max(rows);
        arrays_read_count += 1;
    }
    Ok(LocalVortexScan {
        source_row_count,
        result_row_count,
        arrays_read_count,
        max_chunk_rows,
        max_parallelism_requested: policy.max_parallelism,
        scan_concurrency_per_worker: policy.scan_concurrency_per_worker(),
        projected_columns: plan.projected_columns,
        filter_pushdown_applied,
        projection_pushdown_applied,
    })
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
        max_chunk_rows: scan.max_chunk_rows,
        streaming_scan_used: true,
        full_stream_collected: false,
        max_parallelism_requested: scan.max_parallelism_requested,
        scan_concurrency_per_worker: scan.scan_concurrency_per_worker,
        filter_pushdown_applied: scan.filter_pushdown_applied,
        projection_pushdown_applied: scan.projection_pushdown_applied,
        upstream_filter_expression_used: scan.filter_pushdown_applied,
        upstream_projection_expression_used: scan.projection_pushdown_applied,
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
        max_chunk_rows: scan.max_chunk_rows,
        streaming_scan_used: true,
        full_stream_collected: false,
        max_parallelism_requested: scan.max_parallelism_requested,
        scan_concurrency_per_worker: scan.scan_concurrency_per_worker,
        filter_pushdown_applied: scan.filter_pushdown_applied,
        projection_pushdown_applied: scan.projection_pushdown_applied,
        upstream_filter_expression_used: scan.filter_pushdown_applied,
        upstream_projection_expression_used: scan.projection_pushdown_applied,
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
        max_chunk_rows: scan.max_chunk_rows,
        streaming_scan_used: true,
        full_stream_collected: false,
        max_parallelism_requested: scan.max_parallelism_requested,
        scan_concurrency_per_worker: scan.scan_concurrency_per_worker,
        filter_pushdown_applied: scan.filter_pushdown_applied,
        projection_pushdown_applied: scan.projection_pushdown_applied,
        upstream_filter_expression_used: scan.filter_pushdown_applied,
        upstream_projection_expression_used: scan.projection_pushdown_applied,
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
        max_chunk_rows: scan.max_chunk_rows,
        streaming_scan_used: true,
        full_stream_collected: false,
        max_parallelism_requested: scan.max_parallelism_requested,
        scan_concurrency_per_worker: scan.scan_concurrency_per_worker,
        filter_pushdown_applied: scan.filter_pushdown_applied,
        projection_pushdown_applied: scan.projection_pushdown_applied,
        upstream_filter_expression_used: scan.filter_pushdown_applied,
        upstream_projection_expression_used: scan.projection_pushdown_applied,
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
    use shardloom_core::{
        ColumnRef, CorrectnessFixture, CorrectnessValidationPlan, ExecutionCertificateStatus,
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
        std::fs::write(path, bytes).map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to write test Vortex file '{}': {error}",
                path.display()
            ))
        })
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

    fn correctness_fixture(id: &str) -> CorrectnessFixture {
        CorrectnessValidationPlan::default_foundation_plan()
            .fixtures
            .into_iter()
            .find(|fixture| fixture.id.as_str() == id)
            .expect("fixture")
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
                "count-where-certificate",
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
                "project-certificate",
                "vortex-local-project-struct-five",
                VortexQueryPrimitiveRequest::project(
                    DatasetUri::new("placeholder.vortex").expect("uri"),
                    ProjectionRequest::columns(vec![ColumnRef::new("metric").expect("column")]),
                ),
            ),
            (
                "filter-project-certificate",
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

        for (name, fixture_id, mut request) in cases {
            let path = unique_vortex_path(name);
            write_struct_fixture(&path).expect("fixture");
            request.source_uri = Some(DatasetUri::new(path.display().to_string()).expect("uri"));

            let report = execute_vortex_local_primitive(&request).expect("report");
            let certificate = local_primitive_execution_certificate(
                &correctness_fixture(fixture_id),
                &request,
                &report,
            )
            .expect("certificate");
            let _ = std::fs::remove_file(&path);

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
