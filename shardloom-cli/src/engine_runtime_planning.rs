//! Engine runtime, streaming, sizing, and scheduling planning handlers.
//!
//! These handlers emit report-only runtime planning surfaces. They do not read
//! datasets, execute tasks, collect runtime profiles, write data, materialize
//! outputs, invoke external engines, or provide fallback execution.

use std::process::ExitCode;

use shardloom_core::{
    CommandStatus, DatasetRef, DatasetUri, Diagnostic, OutputFormat, OutputTarget, ShardLoomError,
};
use shardloom_exec::{
    AdaptiveSizer, AdaptiveSizingPolicy, BackpressurePlanInput, BackpressurePlanReport,
    BoundedMemoryPolicy, ByteSize, DynamicRuntimePromotionGateReport, DynamicSizingFeedbackInput,
    DynamicSizingFeedbackReport, DynamicWorkShapingReport, EncodedStreamingBatchPlanInput,
    EncodedStreamingBatchPlanReport, ParallelismLimit, ParallelismPlan, RuntimePlanSkeleton,
    SizeEstimate, SizingFeedbackSignal, SizingFeedbackSignalKind, SizingInput, SizingPlan,
    StreamingCapabilityMatrixReport, StreamingCapabilityMatrixRow, StreamingPlanSkeleton,
    plan_backpressure, plan_dynamic_runtime_promotion_gate, plan_dynamic_sizing_feedback,
    plan_dynamic_work_shaping, plan_encoded_streaming_batches,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
};

const STREAMING_BATCH_COMMAND: &str = "streaming-batch-plan";
const STREAMING_BATCH_SUMMARY: &str = "encoded streaming-batch planning failed";
const STREAMING_BATCH_USAGE: &str = "usage: shardloom streaming-batch-plan <dataset_uri> <target_uri> <memory_gb> <max_parallelism> [batch_mib]";

struct StreamingBatchArgs {
    dataset_ref: DatasetRef,
    output_target: OutputTarget,
    memory_gb: u64,
    max_parallelism: usize,
    batch_mib: Option<u64>,
}

pub(crate) fn handle_streaming_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom streaming-plan <dataset_uri> <target_uri>");
        return ExitCode::from(2);
    };
    let Some(target_uri) = args.next() else {
        eprintln!("usage: shardloom streaming-plan <dataset_uri> <target_uri>");
        return ExitCode::from(2);
    };
    let dataset_uri = match DatasetUri::new(dataset_uri) {
        Ok(uri) => uri,
        Err(error) => {
            eprintln!("invalid dataset uri: {error}");
            return ExitCode::from(2);
        }
    };
    let dataset_ref = match DatasetRef::from_uri(dataset_uri) {
        Ok(dataset_ref) => dataset_ref,
        Err(error) => {
            eprintln!("failed to create dataset reference: {error}");
            return ExitCode::from(2);
        }
    };
    let target_uri = match DatasetUri::new(target_uri) {
        Ok(uri) => uri,
        Err(error) => {
            eprintln!("invalid target uri: {error}");
            return ExitCode::from(2);
        }
    };
    let output_target = OutputTarget::from_uri(target_uri);
    let plan = StreamingPlanSkeleton::for_vortex_to_target(dataset_ref, output_target);
    let matrix = StreamingCapabilityMatrixReport::gar0013_current();
    emit(
        "streaming-plan",
        format,
        if plan.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "streaming plan".to_string(),
        plan.to_human_text(),
        streaming_plan_diagnostics(&plan, &matrix),
        streaming_plan_fields(&plan, &matrix),
    );
    if plan.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_streaming_batch_plan(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let parsed = match parse_streaming_batch_args(args, format) {
        Ok(parsed) => parsed,
        Err(exit_code) => return exit_code,
    };
    let memory = BoundedMemoryPolicy::required(ByteSize::from_gib(parsed.memory_gb));
    let mut input = match EncodedStreamingBatchPlanInput::for_vortex_to_target(
        parsed.dataset_ref,
        parsed.output_target,
        memory,
        parsed.max_parallelism,
    ) {
        Ok(input) => input,
        Err(error) => {
            return emit_error(
                STREAMING_BATCH_COMMAND,
                format,
                STREAMING_BATCH_SUMMARY,
                &error,
            );
        }
    };
    if let Some(batch_mib) = parsed.batch_mib {
        input = input.with_estimated_batch_bytes(ByteSize::from_mib(batch_mib));
    }
    let report = match plan_encoded_streaming_batches(input) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                STREAMING_BATCH_COMMAND,
                format,
                STREAMING_BATCH_SUMMARY,
                &error,
            );
        }
    };
    let matrix = StreamingCapabilityMatrixReport::gar0013_current();
    emit(
        STREAMING_BATCH_COMMAND,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "encoded streaming-batch plan".to_string(),
        report.to_human_text(),
        streaming_batch_diagnostics(&report, &matrix),
        encoded_streaming_batch_plan_fields(&report, parsed.memory_gb, parsed.batch_mib, &matrix),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn parse_streaming_batch_args(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> Result<StreamingBatchArgs, ExitCode> {
    let dataset_uri = next_streaming_batch_arg(&mut args)?;
    let target_uri = next_streaming_batch_arg(&mut args)?;
    let memory_gb_text = next_streaming_batch_arg(&mut args)?;
    let max_parallelism_text = next_streaming_batch_arg(&mut args)?;
    let batch_mib = parse_streaming_batch_mib(&mut args, format)?;
    let dataset_ref = parse_streaming_batch_dataset_ref(dataset_uri, format)?;
    let output_target = parse_streaming_batch_output_target(target_uri, format)?;
    let memory_gb = parse_positive_streaming_batch_u64(&memory_gb_text, "memory_gb", format)?;
    let max_parallelism =
        parse_positive_streaming_batch_usize(&max_parallelism_text, "max_parallelism", format)?;
    Ok(StreamingBatchArgs {
        dataset_ref,
        output_target,
        memory_gb,
        max_parallelism,
        batch_mib,
    })
}

fn next_streaming_batch_arg(args: &mut impl Iterator<Item = String>) -> Result<String, ExitCode> {
    if let Some(value) = args.next() {
        Ok(value)
    } else {
        eprintln!("{STREAMING_BATCH_USAGE}");
        Err(ExitCode::from(2))
    }
}

fn parse_streaming_batch_mib(
    args: &mut impl Iterator<Item = String>,
    format: OutputFormat,
) -> Result<Option<u64>, ExitCode> {
    let batch_mib = match args.next() {
        Some(value) => match value.parse::<u64>() {
            Ok(parsed) if parsed > 0 => Some(parsed),
            _ => {
                return Err(emit_error(
                    STREAMING_BATCH_COMMAND,
                    format,
                    STREAMING_BATCH_SUMMARY,
                    &ShardLoomError::InvalidOperation(
                        "batch_mib must be a positive integer".to_string(),
                    ),
                ));
            }
        },
        None => None,
    };
    if let Some(extra) = args.next() {
        return Err(emit_error(
            STREAMING_BATCH_COMMAND,
            format,
            STREAMING_BATCH_SUMMARY,
            &cli_unknown_arg_error(STREAMING_BATCH_COMMAND, &extra),
        ));
    }
    Ok(batch_mib)
}

fn parse_streaming_batch_dataset_ref(
    dataset_uri: String,
    format: OutputFormat,
) -> Result<DatasetRef, ExitCode> {
    let dataset_uri = match DatasetUri::new(dataset_uri) {
        Ok(uri) => uri,
        Err(error) => {
            return Err(emit_error(
                STREAMING_BATCH_COMMAND,
                format,
                "invalid dataset uri",
                &ShardLoomError::InvalidOperation(format!("invalid dataset uri: {error}")),
            ));
        }
    };
    let dataset_ref = match DatasetRef::from_uri(dataset_uri) {
        Ok(dataset_ref) => dataset_ref,
        Err(error) => {
            return Err(emit_error(
                STREAMING_BATCH_COMMAND,
                format,
                "failed to create dataset reference",
                &error,
            ));
        }
    };
    Ok(dataset_ref)
}

fn parse_streaming_batch_output_target(
    target_uri: String,
    format: OutputFormat,
) -> Result<OutputTarget, ExitCode> {
    let target_uri = match DatasetUri::new(target_uri) {
        Ok(uri) => uri,
        Err(error) => {
            return Err(emit_error(
                STREAMING_BATCH_COMMAND,
                format,
                "invalid target uri",
                &ShardLoomError::InvalidOperation(format!("invalid target uri: {error}")),
            ));
        }
    };
    Ok(OutputTarget::from_uri(target_uri))
}

fn parse_positive_streaming_batch_u64(
    value: &str,
    arg_name: &str,
    format: OutputFormat,
) -> Result<u64, ExitCode> {
    match value.parse() {
        Ok(parsed) if parsed > 0 => Ok(parsed),
        _ => Err(positive_streaming_batch_arg_error(arg_name, format)),
    }
}

fn parse_positive_streaming_batch_usize(
    value: &str,
    arg_name: &str,
    format: OutputFormat,
) -> Result<usize, ExitCode> {
    match value.parse() {
        Ok(parsed) if parsed > 0 => Ok(parsed),
        _ => Err(positive_streaming_batch_arg_error(arg_name, format)),
    }
}

fn positive_streaming_batch_arg_error(arg_name: &str, format: OutputFormat) -> ExitCode {
    emit_error(
        STREAMING_BATCH_COMMAND,
        format,
        STREAMING_BATCH_SUMMARY,
        &ShardLoomError::InvalidOperation(format!("{arg_name} must be a positive integer")),
    )
}

fn streaming_plan_diagnostics(
    plan: &StreamingPlanSkeleton,
    matrix: &StreamingCapabilityMatrixReport,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    diagnostics.extend(plan.source.diagnostics.clone());
    diagnostics.extend(plan.sink.diagnostics.clone());
    diagnostics.extend(
        plan.operators
            .iter()
            .flat_map(|operator| operator.diagnostics.iter().cloned()),
    );
    diagnostics.extend(
        plan.stages
            .iter()
            .flat_map(|stage| stage.diagnostics.iter().cloned()),
    );
    diagnostics.extend(plan.diagnostics.clone());
    diagnostics.extend(matrix.diagnostics());
    diagnostics
}

fn streaming_batch_diagnostics(
    report: &EncodedStreamingBatchPlanReport,
    matrix: &StreamingCapabilityMatrixReport,
) -> Vec<Diagnostic> {
    let mut diagnostics = report.diagnostics.clone();
    diagnostics.extend(matrix.diagnostics());
    diagnostics
}

fn backpressure_plan_diagnostics(
    report: &BackpressurePlanReport,
    matrix: &StreamingCapabilityMatrixReport,
) -> Vec<Diagnostic> {
    let mut diagnostics = report.diagnostics.clone();
    diagnostics.extend(matrix.diagnostics());
    diagnostics
}

fn streaming_plan_fields(
    plan: &StreamingPlanSkeleton,
    matrix: &StreamingCapabilityMatrixReport,
) -> Vec<(String, String)> {
    let mut fields = vec![
        ("mode".to_string(), plan.mode.as_str().to_string()),
        ("status".to_string(), plan.status.as_str().to_string()),
        (
            "source_kind".to_string(),
            plan.source.kind.as_str().to_string(),
        ),
        (
            "source_capability".to_string(),
            plan.source.capability.as_str().to_string(),
        ),
        (
            "source_zero_decode".to_string(),
            plan.source.zero_decode.as_str().to_string(),
        ),
        ("sink_kind".to_string(), plan.sink.kind.as_str().to_string()),
        (
            "sink_capability".to_string(),
            plan.sink.capability.as_str().to_string(),
        ),
        (
            "sink_accepts_encoded".to_string(),
            plan.sink.requirement.accepts_encoded.to_string(),
        ),
        (
            "sink_requires_materialization".to_string(),
            plan.sink.requirement.requires_materialization.to_string(),
        ),
        (
            "sink_preserves_metadata".to_string(),
            plan.sink.requirement.preserves_metadata.to_string(),
        ),
        (
            "backpressure_enabled".to_string(),
            plan.backpressure.enabled.to_string(),
        ),
        (
            "backpressure_bounded".to_string(),
            plan.backpressure.is_bounded().to_string(),
        ),
        (
            "memory_policy_required".to_string(),
            plan.memory.required.to_string(),
        ),
        (
            "memory_policy_allow_spill".to_string(),
            plan.memory.allow_spill.to_string(),
        ),
        (
            "materialization_required".to_string(),
            plan.requires_materialization().to_string(),
        ),
        (
            "best_data_work_level".to_string(),
            plan.best_data_work_level().as_str().to_string(),
        ),
        ("stage_count".to_string(), plan.stages.len().to_string()),
        (
            "operator_count".to_string(),
            plan.operators.len().to_string(),
        ),
        ("runtime_execution".to_string(), "false".to_string()),
        (
            "fallback_execution_allowed".to_string(),
            "false".to_string(),
        ),
        ("fallback_attempted".to_string(), "false".to_string()),
        ("external_engine_invoked".to_string(), "false".to_string()),
    ];
    append_streaming_capability_matrix_fields(&mut fields, matrix);
    fields
}

fn encoded_streaming_batch_plan_fields(
    report: &EncodedStreamingBatchPlanReport,
    memory_gb: u64,
    estimated_batch_mib: Option<u64>,
    matrix: &StreamingCapabilityMatrixReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "streaming_batch_plan");
    push_field(
        &mut fields,
        "encoded_streaming_batch_status",
        report.status.as_str(),
    );
    push_field(&mut fields, "streaming_mode", report.mode.as_str());
    push_field(
        &mut fields,
        "source_kind",
        report.input.source.kind.as_str(),
    );
    push_field(
        &mut fields,
        "source_capability",
        report.input.source.capability.as_str(),
    );
    push_field(&mut fields, "sink_kind", report.input.sink.kind.as_str());
    push_field(
        &mut fields,
        "sink_capability",
        report.input.sink.capability.as_str(),
    );
    push_field(
        &mut fields,
        "representation",
        report.representation.as_str(),
    );
    push_field(&mut fields, "zero_decode", report.zero_decode.as_str());
    push_bool_field(
        &mut fields,
        "encoded_representation_preserved",
        report.encoded_representation_preserved,
    );
    push_bool_field(
        &mut fields,
        "selection_vector_preserved",
        report.selection_vector_preserved,
    );
    push_bool_field(
        &mut fields,
        "bounded_parallelism",
        report.bounded_parallelism,
    );
    push_count_field(&mut fields, "max_parallelism", report.input.max_parallelism);
    push_bool_field(&mut fields, "bounded_memory", report.bounded_memory);
    push_field(&mut fields, "memory_gb", &memory_gb.to_string());
    push_bool_field(
        &mut fields,
        "backpressure_bounded",
        report.backpressure_bounded,
    );
    push_bool_field(
        &mut fields,
        "materialization_required",
        report.materialization_boundary.required,
    );
    push_field(
        &mut fields,
        "materialization_boundary",
        report.materialization_boundary.canonical_label(),
    );
    push_field(
        &mut fields,
        "estimated_batch_count",
        &report
            .estimated_batch_count
            .map_or("unknown".to_string(), |value| value.to_string()),
    );
    push_field(
        &mut fields,
        "estimated_batch_mib",
        &estimated_batch_mib.map_or("unknown".to_string(), |value| value.to_string()),
    );
    push_field(
        &mut fields,
        "estimated_batch_bytes",
        &report
            .estimated_batch_bytes
            .map_or("unknown".to_string(), |value| value.as_bytes().to_string()),
    );
    push_bool_field(&mut fields, "streams_executed", report.streams_executed);
    push_bool_field(&mut fields, "tasks_executed", report.tasks_executed);
    push_bool_field(&mut fields, "data_read", report.data_read);
    push_bool_field(&mut fields, "data_decoded", report.data_decoded);
    push_bool_field(&mut fields, "data_materialized", report.data_materialized);
    push_bool_field(&mut fields, "row_read", report.row_read);
    push_bool_field(&mut fields, "arrow_converted", report.arrow_converted);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(&mut fields, "spill_io_performed", report.spill_io_performed);
    push_field(&mut fields, "execution", "not_performed");
    push_bool_field(&mut fields, "fallback_attempted", false);
    push_bool_field(&mut fields, "external_engine_invoked", false);
    append_streaming_capability_matrix_fields(&mut fields, matrix);
    fields
}

fn backpressure_plan_fields(
    report: &BackpressurePlanReport,
    matrix: &StreamingCapabilityMatrixReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "backpressure_plan");
    push_field(&mut fields, "backpressure_status", report.status.as_str());
    push_field(&mut fields, "backpressure_mode", report.mode.as_str());
    push_bool_field(&mut fields, "bounded", report.bounded);
    push_bool_field(&mut fields, "memory_required", report.memory_required);
    push_bool_field(&mut fields, "spill_allowed", report.spill_allowed);
    push_count_field(&mut fields, "max_parallelism", report.input.max_parallelism);
    push_field(
        &mut fields,
        "max_in_flight_chunks",
        &report
            .max_in_flight_chunks
            .map_or("none".to_string(), |value| value.to_string()),
    );
    push_field(
        &mut fields,
        "max_buffered_bytes",
        &report
            .max_buffered_bytes
            .map_or("none".to_string(), |value| value.as_bytes().to_string()),
    );
    push_field(
        &mut fields,
        "estimated_chunk_bytes",
        &report
            .estimated_chunk_bytes
            .map_or("unknown".to_string(), |value| value.as_bytes().to_string()),
    );
    push_bool_field(&mut fields, "streams_executed", report.streams_executed);
    push_bool_field(&mut fields, "tasks_executed", report.tasks_executed);
    push_bool_field(&mut fields, "data_read", report.data_read);
    push_bool_field(&mut fields, "data_materialized", report.data_materialized);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(&mut fields, "spill_io_performed", report.spill_io_performed);
    push_field(&mut fields, "execution", "not_performed");
    push_bool_field(&mut fields, "fallback_attempted", false);
    push_bool_field(&mut fields, "external_engine_invoked", false);
    append_streaming_capability_matrix_fields(&mut fields, matrix);
    fields
}

pub(crate) fn append_streaming_capability_matrix_fields(
    fields: &mut Vec<(String, String)>,
    matrix: &StreamingCapabilityMatrixReport,
) {
    append_streaming_capability_matrix_summary_fields(fields, matrix);
    for row in &matrix.rows {
        append_streaming_capability_matrix_row_fields(fields, row);
    }
}

pub(crate) fn append_streaming_capability_matrix_summary_fields(
    fields: &mut Vec<(String, String)>,
    matrix: &StreamingCapabilityMatrixReport,
) {
    append_streaming_capability_matrix_identity_fields(fields, matrix);
    append_streaming_capability_matrix_count_fields(fields, matrix);
    append_streaming_capability_matrix_order_fields(fields, matrix);
    append_streaming_capability_matrix_policy_fields(fields, matrix);
}

fn append_streaming_capability_matrix_identity_fields(
    fields: &mut Vec<(String, String)>,
    matrix: &StreamingCapabilityMatrixReport,
) {
    push_field(
        fields,
        "streaming_capability_matrix_schema_version",
        matrix.schema_version,
    );
    push_field(
        fields,
        "streaming_capability_matrix_report_id",
        matrix.report_id,
    );
    push_field(
        fields,
        "streaming_capability_matrix_status",
        matrix.matrix_status,
    );
    push_field(
        fields,
        "streaming_capability_matrix_claim_gate_status",
        matrix.claim_gate_status,
    );
}

fn append_streaming_capability_matrix_count_fields(
    fields: &mut Vec<(String, String)>,
    matrix: &StreamingCapabilityMatrixReport,
) {
    push_count_field(
        fields,
        "streaming_capability_matrix_row_count",
        matrix.rows.len(),
    );
    push_count_field(
        fields,
        "streaming_capability_matrix_blocked_row_count",
        matrix.blocked_row_count(),
    );
    push_count_field(
        fields,
        "streaming_capability_matrix_report_only_row_count",
        matrix.report_only_row_count(),
    );
    push_count_field(
        fields,
        "streaming_capability_matrix_fixture_smoke_row_count",
        matrix.fixture_smoke_row_count(),
    );
    push_count_field(
        fields,
        "streaming_capability_matrix_materialization_row_count",
        matrix.materialization_row_count(),
    );
}

fn append_streaming_capability_matrix_order_fields(
    fields: &mut Vec<(String, String)>,
    matrix: &StreamingCapabilityMatrixReport,
) {
    push_field(
        fields,
        "streaming_capability_matrix_row_order",
        &matrix.row_order().join(","),
    );
    push_field(
        fields,
        "streaming_capability_matrix_family_order",
        &matrix.family_order().join(","),
    );
    push_field(
        fields,
        "streaming_capability_matrix_diagnostic_code_order",
        &matrix.diagnostic_code_order().join(","),
    );
}

fn append_streaming_capability_matrix_policy_fields(
    fields: &mut Vec<(String, String)>,
    matrix: &StreamingCapabilityMatrixReport,
) {
    push_bool_field(
        fields,
        "streaming_capability_matrix_all_rows_have_support_status",
        matrix.all_rows_have_support_status(),
    );
    push_bool_field(
        fields,
        "streaming_capability_matrix_all_blocked_rows_have_diagnostics",
        matrix.all_blocked_rows_have_diagnostics(),
    );
    push_bool_field(
        fields,
        "streaming_capability_matrix_all_rows_no_fallback_no_external_engine",
        matrix.all_rows_no_fallback_no_external_engine(),
    );
    push_bool_field(
        fields,
        "streaming_capability_matrix_runtime_execution",
        matrix.runtime_execution,
    );
    push_bool_field(
        fields,
        "streaming_capability_matrix_data_read",
        matrix.data_read,
    );
    push_bool_field(
        fields,
        "streaming_capability_matrix_object_store_io",
        matrix.object_store_io,
    );
    push_bool_field(
        fields,
        "streaming_capability_matrix_write_io",
        matrix.write_io,
    );
    push_bool_field(
        fields,
        "streaming_capability_matrix_fallback_attempted",
        matrix.fallback_attempted,
    );
    push_bool_field(
        fields,
        "streaming_capability_matrix_external_engine_invoked",
        matrix.external_engine_invoked,
    );
}

fn append_streaming_capability_matrix_row_fields(
    fields: &mut Vec<(String, String)>,
    row: &StreamingCapabilityMatrixRow,
) {
    let prefix = format!("streaming_capability_matrix_row_{}", row.id);
    push_field(fields, &format!("{prefix}_family"), row.family);
    push_field(fields, &format!("{prefix}_surface"), row.surface);
    push_field(
        fields,
        &format!("{prefix}_support_status"),
        row.support_status.as_str(),
    );
    push_field(fields, &format!("{prefix}_source_kind"), row.source_kind);
    push_field(fields, &format!("{prefix}_sink_kind"), row.sink_kind);
    push_field(
        fields,
        &format!("{prefix}_zero_decode"),
        row.zero_decode.as_str(),
    );
    push_field(
        fields,
        &format!("{prefix}_zero_copy"),
        row.zero_copy.as_str(),
    );
    push_field(
        fields,
        &format!("{prefix}_backpressure_status"),
        row.backpressure_status,
    );
    push_field(
        fields,
        &format!("{prefix}_materialization_boundary"),
        row.materialization_boundary,
    );
    push_field(
        fields,
        &format!("{prefix}_diagnostic_code"),
        row.diagnostic_code_text(),
    );
    push_field(
        fields,
        &format!("{prefix}_diagnostic_category"),
        row.diagnostic_category_text(),
    );
    push_field(fields, &format!("{prefix}_blocker_id"), row.blocker_id);
    push_field(
        fields,
        &format!("{prefix}_evidence_refs"),
        row.evidence_refs,
    );
    push_field(
        fields,
        &format!("{prefix}_required_future_evidence"),
        row.required_future_evidence,
    );
    push_field(
        fields,
        &format!("{prefix}_claim_gate_status"),
        row.claim_gate_status,
    );
    push_field(
        fields,
        &format!("{prefix}_claim_boundary"),
        row.claim_boundary,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_runtime_execution"),
        row.runtime_execution,
    );
    push_bool_field(fields, &format!("{prefix}_data_read"), row.data_read);
    push_bool_field(
        fields,
        &format!("{prefix}_object_store_io"),
        row.object_store_io,
    );
    push_bool_field(fields, &format!("{prefix}_write_io"), row.write_io);
    push_bool_field(
        fields,
        &format!("{prefix}_fallback_attempted"),
        row.fallback_attempted,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_external_engine_invoked"),
        row.external_engine_invoked,
    );
}

pub(crate) fn parse_sizing_feedback_signals(
    value: &str,
) -> Result<Vec<SizingFeedbackSignal>, ShardLoomError> {
    let mut signals = Vec::new();
    for token in value
        .split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
    {
        let kind = match token {
            "stable" => SizingFeedbackSignalKind::Stable,
            "task-too-large" | "task_too_large" => SizingFeedbackSignalKind::TaskTooLarge,
            "task-too-small" | "task_too_small" => SizingFeedbackSignalKind::TaskTooSmall,
            "memory-pressure-high" | "memory_pressure_high" => {
                SizingFeedbackSignalKind::MemoryPressureHigh
            }
            "object-store-throttled" | "object_store_throttled" => {
                SizingFeedbackSignalKind::ObjectStoreThrottled
            }
            _ => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "invalid sizing feedback signal token: {token}"
                )));
            }
        };
        if !signals
            .iter()
            .any(|signal: &SizingFeedbackSignal| signal.kind == kind)
        {
            signals.push(SizingFeedbackSignal::new(
                kind,
                format!("observed sizing feedback signal: {}", kind.as_str()),
            ));
        }
    }
    Ok(signals)
}

fn dynamic_sizing_feedback_fields(
    report: &DynamicSizingFeedbackReport,
    memory_gb: u64,
    signals_raw: &str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "fallback_execution_allowed", "false");
    push_field(&mut fields, "mode", "sizing_feedback_plan");
    push_field(
        &mut fields,
        "dynamic_sizing_feedback_status",
        report.status.as_str(),
    );
    push_field(
        &mut fields,
        "dynamic_sizing_feedback_mode",
        report.mode.as_str(),
    );
    push_field(&mut fields, "memory_gb", &memory_gb.to_string());
    push_field(&mut fields, "signals", signals_raw);
    push_count_field(&mut fields, "signal_count", report.signal_count);
    push_count_field(
        &mut fields,
        "reduce_signal_count",
        report.reduce_signal_count,
    );
    push_count_field(
        &mut fields,
        "increase_signal_count",
        report.increase_signal_count,
    );
    push_count_field(
        &mut fields,
        "stable_signal_count",
        report.stable_signal_count,
    );
    push_field(
        &mut fields,
        "current_target_task_bytes",
        &report.current_target_task_bytes.as_bytes().to_string(),
    );
    push_field(
        &mut fields,
        "recommended_target_task_bytes",
        &report.recommended_target_task_bytes.as_bytes().to_string(),
    );
    push_bool_field(
        &mut fields,
        "target_task_bytes_changed",
        report.current_target_task_bytes != report.recommended_target_task_bytes,
    );
    push_bool_field(
        &mut fields,
        "adaptive_splitting_allowed",
        report.recommended_policy.allow_splitting,
    );
    push_bool_field(
        &mut fields,
        "adaptive_coalescing_allowed",
        report.recommended_policy.allow_coalescing,
    );
    push_bool_field(&mut fields, "tasks_executed", report.tasks_executed);
    push_bool_field(&mut fields, "data_read", report.data_read);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(&mut fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(&mut fields, "feedback_applied", report.feedback_applied);
    push_field(&mut fields, "execution", "not_performed");
    fields
}

fn dynamic_work_shaping_profile(
    profile: Option<&str>,
) -> shardloom_core::Result<(String, u64, usize, &'static str, ByteSize)> {
    match profile {
        None | Some("balanced") => Ok((
            "balanced".to_string(),
            8,
            4,
            "stable",
            ByteSize::from_mib(256),
        )),
        Some("memory-pressure" | "memory_pressure") => Ok((
            "memory-pressure".to_string(),
            8,
            4,
            "memory-pressure-high",
            ByteSize::from_mib(256),
        )),
        Some("object-store-throttled" | "object_store_throttled") => Ok((
            "object-store-throttled".to_string(),
            8,
            4,
            "object-store-throttled",
            ByteSize::from_mib(64),
        )),
        Some("small-tasks" | "small_tasks") => Ok((
            "small-tasks".to_string(),
            8,
            8,
            "task-too-small",
            ByteSize::from_mib(32),
        )),
        Some(other) => Err(ShardLoomError::InvalidOperation(format!(
            "unknown dynamic work shaping profile: {other}"
        ))),
    }
}

fn dynamic_work_shaping_report_for_profile(
    profile: Option<&str>,
) -> shardloom_core::Result<DynamicWorkShapingReport> {
    let (profile_label, memory_gb, max_parallelism, signals_raw, estimated_chunk_bytes) =
        dynamic_work_shaping_profile(profile)?;
    let policy = AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb));
    let mut feedback_input = DynamicSizingFeedbackInput::new(policy);
    for signal in parse_sizing_feedback_signals(signals_raw)? {
        feedback_input.add_signal(signal);
    }
    let feedback = plan_dynamic_sizing_feedback(feedback_input);
    let backpressure = plan_backpressure(
        BackpressurePlanInput::new(
            BoundedMemoryPolicy::required(ByteSize::from_gib(memory_gb)).with_spill(true),
            max_parallelism,
        )?
        .with_estimated_chunk_bytes(estimated_chunk_bytes),
    )?;
    Ok(plan_dynamic_work_shaping(
        profile_label,
        &feedback,
        &backpressure,
    ))
}

#[allow(clippy::too_many_lines)]
fn dynamic_work_shaping_fields(report: &DynamicWorkShapingReport) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    push_field(&mut fields, "mode", "dynamic_work_shaping_plan");
    push_field(&mut fields, "schema_version", report.schema_version);
    push_field(&mut fields, "report_id", report.report_id);
    push_field(&mut fields, "profile", &report.profile);
    push_field(
        &mut fields,
        "dynamic_work_shaping_status",
        report.status.as_str(),
    );
    push_field(
        &mut fields,
        "surface_order",
        &DynamicWorkShapingReport::surface_order().join(","),
    );
    push_count_field(
        &mut fields,
        "surface_count",
        DynamicWorkShapingReport::surface_order().len(),
    );
    push_count_field(
        &mut fields,
        "planned_surface_count",
        report.planned_surface_count,
    );
    push_count_field(
        &mut fields,
        "blocked_surface_count",
        report.blocked_surface_count,
    );
    push_field(
        &mut fields,
        "blocked_surface_order",
        &report.blocked_surface_order.join(","),
    );
    push_field(
        &mut fields,
        "feedback_status",
        report.feedback_status.as_str(),
    );
    push_field(&mut fields, "feedback_mode", report.feedback_mode.as_str());
    push_count_field(&mut fields, "signal_count", report.signal_count);
    push_count_field(
        &mut fields,
        "reduce_signal_count",
        report.reduce_signal_count,
    );
    push_count_field(
        &mut fields,
        "increase_signal_count",
        report.increase_signal_count,
    );
    push_count_field(
        &mut fields,
        "stable_signal_count",
        report.stable_signal_count,
    );
    push_bool_field(
        &mut fields,
        "target_task_bytes_changed",
        report.target_task_bytes_changed,
    );
    push_field(
        &mut fields,
        "current_target_task_bytes",
        &report.current_target_task_bytes.as_bytes().to_string(),
    );
    push_field(
        &mut fields,
        "recommended_target_task_bytes",
        &report.recommended_target_task_bytes.as_bytes().to_string(),
    );
    push_bool_field(
        &mut fields,
        "adaptive_splitting_allowed",
        report.adaptive_splitting_allowed,
    );
    push_bool_field(
        &mut fields,
        "adaptive_coalescing_allowed",
        report.adaptive_coalescing_allowed,
    );
    push_field(
        &mut fields,
        "backpressure_status",
        report.backpressure_status.as_str(),
    );
    push_field(
        &mut fields,
        "backpressure_mode",
        report.backpressure_mode.as_str(),
    );
    push_bool_field(
        &mut fields,
        "bounded_backpressure",
        report.bounded_backpressure,
    );
    push_count_field(&mut fields, "max_parallelism", report.max_parallelism);
    push_field(
        &mut fields,
        "max_in_flight_chunks",
        &report
            .max_in_flight_chunks
            .map_or("none".to_string(), |value| value.to_string()),
    );
    push_field(
        &mut fields,
        "max_buffered_bytes",
        &report
            .max_buffered_bytes
            .map_or("none".to_string(), |value| value.as_bytes().to_string()),
    );
    push_field(
        &mut fields,
        "estimated_chunk_bytes",
        &report
            .estimated_chunk_bytes
            .map_or("unknown".to_string(), |value| value.as_bytes().to_string()),
    );
    push_bool_field(
        &mut fields,
        "bounded_memory_required",
        report.bounded_memory_required,
    );
    push_bool_field(&mut fields, "spill_allowed", report.spill_allowed);
    push_bool_field(
        &mut fields,
        "runtime_feedback_loop_ready",
        report.runtime_feedback_loop_ready,
    );
    push_bool_field(
        &mut fields,
        "policy_application_ready",
        report.policy_application_ready,
    );
    push_bool_field(
        &mut fields,
        "benchmark_evidence_ready",
        report.benchmark_evidence_ready,
    );
    push_bool_field(&mut fields, "streams_executed", report.streams_executed);
    push_bool_field(&mut fields, "tasks_executed", report.tasks_executed);
    push_bool_field(&mut fields, "feedback_applied", report.feedback_applied);
    push_bool_field(&mut fields, "policy_mutated", report.policy_mutated);
    push_bool_field(&mut fields, "data_read", report.data_read);
    push_bool_field(&mut fields, "data_materialized", report.data_materialized);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(&mut fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        &mut fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(&mut fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(
        &mut fields,
        "side_effect_free",
        report.is_side_effect_free(),
    );
    push_count_field(&mut fields, "diagnostic_count", report.diagnostics.len());
    fields
}

#[allow(clippy::too_many_lines)]
fn dynamic_runtime_promotion_gate_fields(
    report: &DynamicRuntimePromotionGateReport,
) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    push_field(&mut fields, "mode", "cg8_runtime_promotion_gate");
    push_field(&mut fields, "schema_version", report.schema_version);
    push_field(&mut fields, "report_id", report.report_id);
    push_count_field(&mut fields, "surface_count", report.surface_count());
    push_count_field(
        &mut fields,
        "existing_limited_surface_count",
        report.existing_limited_surface_count(),
    );
    push_count_field(
        &mut fields,
        "blocked_surface_count",
        report.blocked_surface_count(),
    );
    push_count_field(
        &mut fields,
        "runtime_ready_surface_count",
        report.runtime_ready_surface_count(),
    );
    push_field(
        &mut fields,
        "surface_order",
        &report.surface_order().join(","),
    );
    push_bool_field(
        &mut fields,
        "existing_local_streaming_scan_evidence_present",
        report.existing_local_streaming_scan_evidence_present,
    );
    push_bool_field(
        &mut fields,
        "existing_local_bounded_metadata_noop_evidence_present",
        report.existing_local_bounded_metadata_noop_evidence_present,
    );
    push_bool_field(
        &mut fields,
        "existing_local_filter_project_bounded_scan_evidence_present",
        report.existing_local_filter_project_bounded_scan_evidence_present,
    );
    push_bool_field(
        &mut fields,
        "runtime_promotions_blocked",
        report.runtime_promotions_blocked(),
    );
    push_bool_field(&mut fields, "claim_blocked", report.claim_blocked());
    push_bool_field(&mut fields, "side_effect_free", report.side_effect_free());
    push_bool_field(
        &mut fields,
        "dynamic_feedback_application_allowed",
        report.dynamic_feedback_application_allowed,
    );
    push_bool_field(
        &mut fields,
        "bounded_parallel_encoded_read_allowed",
        report.bounded_parallel_encoded_read_allowed,
    );
    push_bool_field(
        &mut fields,
        "source_backed_parallel_reader_allowed",
        report.source_backed_parallel_reader_allowed,
    );
    push_bool_field(
        &mut fields,
        "scheduler_requeue_allowed",
        report.scheduler_requeue_allowed,
    );
    push_bool_field(
        &mut fields,
        "bounded_backpressure_runtime_allowed",
        report.bounded_backpressure_runtime_allowed,
    );
    push_bool_field(
        &mut fields,
        "memory_spill_reservation_runtime_allowed",
        report.memory_spill_reservation_runtime_allowed,
    );
    push_bool_field(
        &mut fields,
        "object_store_request_budget_runtime_allowed",
        report.object_store_request_budget_runtime_allowed,
    );
    push_bool_field(
        &mut fields,
        "runtime_policy_mutation_allowed",
        report.runtime_policy_mutation_allowed,
    );
    push_bool_field(
        &mut fields,
        "large_workload_claim_allowed",
        report.large_workload_claim_allowed,
    );
    push_bool_field(
        &mut fields,
        "runtime_metrics_required",
        report.runtime_metrics_required,
    );
    push_bool_field(
        &mut fields,
        "target_task_policy_required",
        report.target_task_policy_required,
    );
    push_bool_field(
        &mut fields,
        "scheduler_queue_policy_required",
        report.scheduler_queue_policy_required,
    );
    push_bool_field(
        &mut fields,
        "memory_reservation_evidence_required",
        report.memory_reservation_evidence_required,
    );
    push_bool_field(
        &mut fields,
        "spill_policy_evidence_required",
        report.spill_policy_evidence_required,
    );
    push_bool_field(
        &mut fields,
        "backpressure_evidence_required",
        report.backpressure_evidence_required,
    );
    push_bool_field(
        &mut fields,
        "cancellation_retry_evidence_required",
        report.cancellation_retry_evidence_required,
    );
    push_bool_field(
        &mut fields,
        "execution_certificate_required",
        report.execution_certificate_required,
    );
    push_bool_field(
        &mut fields,
        "native_io_certificate_required",
        report.native_io_certificate_required,
    );
    push_bool_field(
        &mut fields,
        "benchmark_evidence_required",
        report.benchmark_evidence_required,
    );
    push_bool_field(
        &mut fields,
        "runtime_execution",
        report.runtime_execution_performed,
    );
    push_bool_field(&mut fields, "tasks_executed", report.tasks_executed);
    push_bool_field(&mut fields, "data_read", report.data_read);
    push_bool_field(&mut fields, "data_materialized", report.data_materialized);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(&mut fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(&mut fields, "feedback_applied", report.feedback_applied);
    push_bool_field(&mut fields, "policy_mutated", report.policy_mutated);
    push_bool_field(
        &mut fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(&mut fields, "fallback_attempted", report.fallback_attempted);
    push_count_field(&mut fields, "diagnostic_count", report.diagnostics.len());
    for (idx, entry) in report.entries.iter().enumerate() {
        let prefix = format!("cg8_runtime_surface_{idx}");
        push_field(
            &mut fields,
            &format!("{prefix}_name"),
            entry.surface.as_str(),
        );
        push_field(
            &mut fields,
            &format!("{prefix}_status"),
            entry.status.as_str(),
        );
        push_field(
            &mut fields,
            &format!("{prefix}_required_evidence"),
            entry.required_evidence,
        );
        push_bool_field(
            &mut fields,
            &format!("{prefix}_existing_limited_local_evidence"),
            entry.existing_limited_local_evidence,
        );
        push_bool_field(
            &mut fields,
            &format!("{prefix}_requires_runtime_metrics"),
            entry.requires_runtime_metrics,
        );
        push_bool_field(
            &mut fields,
            &format!("{prefix}_requires_execution_certificate"),
            entry.requires_execution_certificate,
        );
        push_bool_field(
            &mut fields,
            &format!("{prefix}_requires_native_io_certificate"),
            entry.requires_native_io_certificate,
        );
        push_bool_field(
            &mut fields,
            &format!("{prefix}_requires_benchmark_evidence"),
            entry.requires_benchmark_evidence,
        );
        push_bool_field(
            &mut fields,
            &format!("{prefix}_runtime_promotion_allowed"),
            entry.runtime_promotion_allowed,
        );
    }
    fields
}

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    fields.push((key.to_string(), value.to_string()));
}

pub(crate) fn handle_backpressure_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(memory_gb_text) = args.next() else {
        eprintln!("usage: shardloom backpressure-plan <memory_gb> <max_parallelism> [chunk_mib]");
        return ExitCode::from(2);
    };
    let Some(max_parallelism_text) = args.next() else {
        eprintln!("usage: shardloom backpressure-plan <memory_gb> <max_parallelism> [chunk_mib]");
        return ExitCode::from(2);
    };
    let memory_gb: u64 = match memory_gb_text.parse() {
        Ok(value) => value,
        Err(_) => {
            return emit_error(
                "backpressure-plan",
                format,
                "backpressure planning failed",
                &shardloom_core::ShardLoomError::InvalidOperation(
                    "memory_gb must be an unsigned integer".to_string(),
                ),
            );
        }
    };
    let max_parallelism: usize = match max_parallelism_text.parse() {
        Ok(value) => value,
        Err(_) => {
            return emit_error(
                "backpressure-plan",
                format,
                "backpressure planning failed",
                &shardloom_core::ShardLoomError::InvalidOperation(
                    "max_parallelism must be an unsigned integer".to_string(),
                ),
            );
        }
    };
    let chunk_mib = match args.next() {
        Some(value) => match value.parse::<u64>() {
            Ok(parsed) => Some(parsed),
            Err(_) => {
                return emit_error(
                    "backpressure-plan",
                    format,
                    "backpressure planning failed",
                    &shardloom_core::ShardLoomError::InvalidOperation(
                        "chunk_mib must be an unsigned integer".to_string(),
                    ),
                );
            }
        },
        None => None,
    };
    let memory = BoundedMemoryPolicy::required(ByteSize::from_gib(memory_gb));
    let mut input = match BackpressurePlanInput::new(memory, max_parallelism) {
        Ok(input) => input,
        Err(error) => {
            return emit_error(
                "backpressure-plan",
                format,
                "backpressure planning failed",
                &error,
            );
        }
    };
    if let Some(chunk_mib) = chunk_mib {
        input = input.with_estimated_chunk_bytes(ByteSize::from_mib(chunk_mib));
    }
    let report = match plan_backpressure(input) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "backpressure-plan",
                format,
                "backpressure planning failed",
                &error,
            );
        }
    };
    let matrix = StreamingCapabilityMatrixReport::gar0013_current();
    emit(
        "backpressure-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "backpressure plan".to_string(),
        report.to_human_text(),
        backpressure_plan_diagnostics(&report, &matrix),
        backpressure_plan_fields(&report, &matrix),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_runtime_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom runtime-plan <dataset_uri>");
        return ExitCode::from(2);
    };
    emit_runtime_or_task_plan("runtime-plan", "runtime plan", dataset_uri, format, false)
}

pub(crate) fn handle_task_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom task-plan <dataset_uri>");
        return ExitCode::from(2);
    };
    emit_runtime_or_task_plan("task-plan", "task plan", dataset_uri, format, true)
}

fn emit_runtime_or_task_plan(
    command: &str,
    summary: &str,
    dataset_uri: String,
    format: OutputFormat,
    graph_only: bool,
) -> ExitCode {
    let uri = match DatasetUri::new(dataset_uri) {
        Ok(uri) => uri,
        Err(error) => {
            eprintln!("invalid dataset uri: {error}");
            return ExitCode::from(2);
        }
    };
    let dataset = match DatasetRef::from_uri(uri) {
        Ok(dataset) => dataset,
        Err(error) => {
            eprintln!("failed to create dataset reference: {error}");
            return ExitCode::from(2);
        }
    };
    let plan = match RuntimePlanSkeleton::for_dataset(dataset) {
        Ok(plan) => plan,
        Err(error) => {
            eprintln!("failed to build {summary}: {error}");
            return ExitCode::from(2);
        }
    };
    emit(
        command,
        format,
        CommandStatus::Success,
        summary.to_string(),
        if graph_only {
            plan.graph.summary()
        } else {
            plan.to_human_text()
        },
        if graph_only {
            vec![]
        } else {
            plan.diagnostics.clone()
        },
        vec![],
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_sizing_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(dataset_uri) = args.next() else {
        eprintln!("usage: shardloom sizing-plan <dataset_uri> --memory-gb <gb>");
        return ExitCode::from(2);
    };
    let Some(memory_flag) = args.next() else {
        eprintln!("usage: shardloom sizing-plan <dataset_uri> --memory-gb <gb>");
        return ExitCode::from(2);
    };
    if memory_flag != "--memory-gb" {
        eprintln!("usage: shardloom sizing-plan <dataset_uri> --memory-gb <gb>");
        return ExitCode::from(2);
    }
    let Some(memory_gb_raw) = args.next() else {
        eprintln!("usage: shardloom sizing-plan <dataset_uri> --memory-gb <gb>");
        return ExitCode::from(2);
    };
    let memory_gb = match memory_gb_raw.parse::<u64>() {
        Ok(value) if value > 0 => value,
        _ => {
            return emit_error(
                "sizing-plan",
                format,
                "invalid memory setting",
                &shardloom_core::ShardLoomError::InvalidOperation(
                    "memory-gb must be a positive integer".to_string(),
                ),
            );
        }
    };
    let uri = match DatasetUri::new(dataset_uri) {
        Ok(uri) => uri,
        Err(error) => {
            return emit_error(
                "sizing-plan",
                format,
                "invalid dataset uri",
                &shardloom_core::ShardLoomError::InvalidOperation(format!(
                    "invalid dataset uri: {error}"
                )),
            );
        }
    };
    let dataset = match DatasetRef::from_uri(uri) {
        Ok(dataset) => dataset,
        Err(error) => {
            eprintln!("failed to create dataset reference: {error}");
            return ExitCode::from(2);
        }
    };
    let policy = AdaptiveSizingPolicy::memory_limited(ByteSize::from_gib(memory_gb));
    let sizer = AdaptiveSizer::new(policy.clone());
    let input = SizingInput::new(
        shardloom_core::SegmentId::new("placeholder-segment").expect("valid segment id"),
        SizeEstimate::unknown(),
    );
    let decision = sizer.decide_for_segment(&input);
    let parallelism = ParallelismPlan::new(ParallelismLimit::auto(), 1, 1, "planning skeleton");
    let mut plan = SizingPlan::new(policy, parallelism);
    plan.add_decision(input.segment_id.clone(), decision);
    emit(
        "sizing-plan",
        format,
        CommandStatus::Success,
        "sizing plan".to_string(),
        format!("dataset: {}\n{}", dataset.summary(), plan.to_human_text()),
        vec![],
        vec![],
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_sizing_feedback_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let Some(memory_gb_text) = args.next() else {
        eprintln!("usage: shardloom sizing-feedback-plan <memory_gb> <signals>");
        return ExitCode::from(2);
    };
    let Some(signals_raw) = args.next() else {
        eprintln!("usage: shardloom sizing-feedback-plan <memory_gb> <signals>");
        return ExitCode::from(2);
    };
    if let Some(extra) = args.next() {
        return emit_error(
            "sizing-feedback-plan",
            format,
            "dynamic sizing feedback planning failed",
            &cli_unknown_arg_error("sizing-feedback-plan", &extra),
        );
    }
    let memory_gb: u64 = match memory_gb_text.parse() {
        Ok(value) if value > 0 => value,
        _ => {
            return emit_error(
                "sizing-feedback-plan",
                format,
                "dynamic sizing feedback planning failed",
                &shardloom_core::ShardLoomError::InvalidOperation(
                    "memory_gb must be a positive integer".to_string(),
                ),
            );
        }
    };
    let signals = match parse_sizing_feedback_signals(&signals_raw) {
        Ok(signals) => signals,
        Err(error) => {
            return emit_error(
                "sizing-feedback-plan",
                format,
                "dynamic sizing feedback planning failed",
                &error,
            );
        }
    };
    let mut input = DynamicSizingFeedbackInput::new(AdaptiveSizingPolicy::memory_limited(
        ByteSize::from_gib(memory_gb),
    ));
    for signal in signals {
        input.add_signal(signal);
    }
    let report = plan_dynamic_sizing_feedback(input);
    emit(
        "sizing-feedback-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "dynamic sizing feedback plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        dynamic_sizing_feedback_fields(&report, memory_gb, &signals_raw),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_dynamic_work_shaping_plan(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let profile = args.next();
    if let Some(extra) = args.next() {
        return emit_error(
            "dynamic-work-shaping-plan",
            format,
            "dynamic work shaping planning failed",
            &shardloom_core::ShardLoomError::InvalidOperation(format!(
                "unknown extra dynamic-work-shaping-plan argument: {extra}"
            )),
        );
    }
    let report = match dynamic_work_shaping_report_for_profile(profile.as_deref()) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                "dynamic-work-shaping-plan",
                format,
                "dynamic work shaping planning failed",
                &error,
            );
        }
    };
    emit(
        "dynamic-work-shaping-plan",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "dynamic work shaping plan".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        dynamic_work_shaping_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_dynamic_runtime_gate(format: OutputFormat) -> ExitCode {
    let report = plan_dynamic_runtime_promotion_gate();
    emit(
        "cg8-runtime-promotion-gate",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "CG-8 runtime promotion gate".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        dynamic_runtime_promotion_gate_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}
