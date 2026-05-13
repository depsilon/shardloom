//! CG-22 engine fabric planning handlers.
//!
//! These handlers are report-only contract surfaces. They do not run engines,
//! read sources, write outputs, create checkpoints, probe external systems, or
//! provide fallback execution.

use std::{process::ExitCode, vec::IntoIter};

use shardloom_core::{
    Boundedness, CommandStatus, EngineCapabilityMatrixReport, EngineCapabilityRow, EngineMode,
    EngineSelectionReport, EngineSelectionRequest, OutputFormat, OutputMode, ShardLoomError,
    UpdateMode, boundedness_vocabulary, engine_mode_vocabulary, output_mode_vocabulary,
    update_mode_vocabulary,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
};

const ENGINE_SELECTION_COMMAND: &str = "engine-selection-plan";
const ENGINE_SELECTION_SUMMARY: &str = "engine selection planning failed";
const ENGINE_SELECTION_USAGE: &str = "usage: shardloom engine-selection-plan [auto|batch|live|hybrid] [bounded|unbounded|snapshot|unknown] [snapshot|append-only|upsert|delete|retract|tombstone|changelog] [snapshot|append|update|complete|changelog|continuous-view]";

const ENGINE_CAPABILITY_MATRIX_COMMAND: &str = "engine-capability-matrix";

pub(crate) fn handle_engine_selection_plan(
    args: IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    let request = match parse_engine_selection_args(args, format) {
        Ok(request) => request,
        Err(exit_code) => return exit_code,
    };
    let report = EngineSelectionReport::evaluate(request);
    emit(
        ENGINE_SELECTION_COMMAND,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "engine selection plan".to_string(),
        report.to_human_text(),
        report.diagnostics(),
        engine_selection_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

pub(crate) fn handle_engine_capability_matrix(
    mut args: IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    if let Some(extra) = args.next() {
        return emit_error(
            ENGINE_CAPABILITY_MATRIX_COMMAND,
            format,
            "engine capability matrix failed",
            &cli_unknown_arg_error(ENGINE_CAPABILITY_MATRIX_COMMAND, &extra),
        );
    }
    let report = EngineCapabilityMatrixReport::cg22_contract();
    emit(
        ENGINE_CAPABILITY_MATRIX_COMMAND,
        format,
        CommandStatus::Success,
        "engine capability matrix".to_string(),
        report.to_human_text(),
        vec![],
        engine_capability_matrix_fields(&report),
    );
    ExitCode::SUCCESS
}

fn parse_engine_selection_args(
    mut args: IntoIter<String>,
    format: OutputFormat,
) -> Result<EngineSelectionRequest, ExitCode> {
    let requested = match args.next() {
        Some(value) => parse_arg(
            EngineMode::parse,
            &value,
            "engine mode",
            ENGINE_SELECTION_COMMAND,
            format,
        )?,
        None => EngineMode::Auto,
    };
    let boundedness = match args.next() {
        Some(value) => parse_arg(
            Boundedness::parse,
            &value,
            "boundedness",
            ENGINE_SELECTION_COMMAND,
            format,
        )?,
        None => Boundedness::Snapshot,
    };
    let update_mode = match args.next() {
        Some(value) => parse_arg(
            UpdateMode::parse,
            &value,
            "update mode",
            ENGINE_SELECTION_COMMAND,
            format,
        )?,
        None => UpdateMode::Snapshot,
    };
    let output_mode = match args.next() {
        Some(value) => parse_arg(
            OutputMode::parse,
            &value,
            "output mode",
            ENGINE_SELECTION_COMMAND,
            format,
        )?,
        None => OutputMode::Snapshot,
    };
    if let Some(extra) = args.next() {
        return Err(emit_error(
            ENGINE_SELECTION_COMMAND,
            format,
            ENGINE_SELECTION_SUMMARY,
            &cli_unknown_arg_error(ENGINE_SELECTION_COMMAND, &extra),
        ));
    }
    Ok(EngineSelectionRequest::new(
        requested,
        boundedness,
        update_mode,
        output_mode,
    ))
}

fn parse_arg<T>(
    parser: impl FnOnce(&str) -> shardloom_core::Result<T>,
    value: &str,
    label: &str,
    command: &str,
    format: OutputFormat,
) -> Result<T, ExitCode> {
    parser(value).map_err(|error| {
        emit_error(
            command,
            format,
            ENGINE_SELECTION_SUMMARY,
            &ShardLoomError::InvalidOperation(format!(
                "{label} parse error: {}; {ENGINE_SELECTION_USAGE}",
                error.message()
            )),
        )
    })
}

fn engine_selection_fields(report: &EngineSelectionReport) -> Vec<(String, String)> {
    let mut fields = common_engine_contract_fields(
        report.schema_version,
        report.report_id,
        report.fallback_attempted(),
        report.external_engine_invoked,
        report.runtime_execution,
        report.data_read,
        report.write_io,
    );
    push_field(
        &mut fields,
        "requested_engine_mode",
        report.request.requested.as_str(),
    );
    push_field(
        &mut fields,
        "requested_boundedness",
        report.request.boundedness.as_str(),
    );
    push_field(
        &mut fields,
        "requested_update_mode",
        report.request.update_mode.as_str(),
    );
    push_field(
        &mut fields,
        "requested_output_mode",
        report.request.output_mode.as_str(),
    );
    push_field(&mut fields, "selection_status", report.status.as_str());
    push_field(&mut fields, "selected_engine_mode", report.selected_text());
    push_field(
        &mut fields,
        "allowed_engine_modes",
        &report.allowed_modes_text(),
    );
    push_field(
        &mut fields,
        "rejected_engine_modes",
        &report.rejected_modes_text(),
    );
    push_field(
        &mut fields,
        "rejection_reasons",
        &report.rejection_reason_text(),
    );
    push_bool_field(
        &mut fields,
        "requested_contract_batch_compatible",
        report.request.batch_compatible(),
    );
    fields
}

fn engine_capability_matrix_fields(report: &EngineCapabilityMatrixReport) -> Vec<(String, String)> {
    let mut fields = common_engine_contract_fields(
        report.schema_version,
        report.report_id,
        report.fallback_attempted(),
        report.external_engine_invoked,
        report.runtime_execution,
        report.data_read,
        report.write_io,
    );
    push_count_field(&mut fields, "engine_mode_count", report.rows.len());
    push_field(&mut fields, "engine_modes", "batch,live,hybrid");
    push_count_field(
        &mut fields,
        "partially_supported_engine_count",
        report.partially_supported_count(),
    );
    push_count_field(&mut fields, "planned_engine_count", report.planned_count());
    push_count_field(&mut fields, "blocked_engine_count", report.blocked_count());
    push_count_field(
        &mut fields,
        "live_hybrid_claim_blocked_count",
        report.live_hybrid_claim_blocked_count(),
    );
    for row in &report.rows {
        append_engine_capability_row_fields(&mut fields, row);
    }
    fields
}

#[allow(clippy::fn_params_excessive_bools)]
fn common_engine_contract_fields(
    schema_version: &str,
    report_id: &str,
    fallback_attempted: bool,
    external_engine_invoked: bool,
    runtime_execution: bool,
    data_read: bool,
    write_io: bool,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "schema_version", schema_version);
    push_field(&mut fields, "report_id", report_id);
    push_field(
        &mut fields,
        "engine_mode_vocabulary",
        &engine_mode_vocabulary(),
    );
    push_field(
        &mut fields,
        "boundedness_vocabulary",
        &boundedness_vocabulary(),
    );
    push_field(
        &mut fields,
        "update_mode_vocabulary",
        &update_mode_vocabulary(),
    );
    push_field(
        &mut fields,
        "output_mode_vocabulary",
        &output_mode_vocabulary(),
    );
    push_bool_field(&mut fields, "fallback_execution_allowed", false);
    push_bool_field(&mut fields, "fallback_attempted", fallback_attempted);
    push_bool_field(
        &mut fields,
        "external_engine_invoked",
        external_engine_invoked,
    );
    push_bool_field(&mut fields, "runtime_execution", runtime_execution);
    push_bool_field(&mut fields, "data_read", data_read);
    push_bool_field(&mut fields, "write_io", write_io);
    fields
}

fn append_engine_capability_row_fields(
    fields: &mut Vec<(String, String)>,
    row: &EngineCapabilityRow,
) {
    let prefix = row.engine_mode.as_str();
    push_field(
        fields,
        &format!("{prefix}_support_status"),
        row.support_status.as_str(),
    );
    push_field(
        fields,
        &format!("{prefix}_operator_support"),
        &row.operator_support.join(","),
    );
    push_field(
        fields,
        &format!("{prefix}_function_support"),
        &row.function_support.join(","),
    );
    push_field(
        fields,
        &format!("{prefix}_source_support"),
        &row.source_support.join(","),
    );
    push_field(
        fields,
        &format!("{prefix}_sink_support"),
        &row.sink_support.join(","),
    );
    push_bool_field(
        fields,
        &format!("{prefix}_bounded_snapshot_support"),
        row.bounded_snapshot_support,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_append_only_stream_support"),
        row.append_only_stream_support,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_upsert_delete_tombstone_support"),
        row.upsert_delete_tombstone_support,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_changelog_support"),
        row.changelog_support,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_continuous_materialized_view_support"),
        row.continuous_materialized_view_support,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_global_sort_supported"),
        row.global_sort_supported,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_unbounded_join_supported"),
        row.unbounded_join_supported,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_state_required"),
        row.state_required,
    );
    push_bool_field(
        fields,
        &format!("{prefix}_checkpoint_required"),
        row.checkpoint_required,
    );
    push_field(
        fields,
        &format!("{prefix}_output_modes"),
        &row.output_modes_text(),
    );
    push_bool_field(
        fields,
        &format!("{prefix}_production_claim_allowed"),
        row.production_claim_allowed,
    );
    push_field(
        fields,
        &format!("{prefix}_blockers"),
        &row.blockers.join(","),
    );
}

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    push_field(fields, key, &value.to_string());
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    push_field(fields, key, if value { "true" } else { "false" });
}
