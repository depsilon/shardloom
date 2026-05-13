//! CG-22 engine fabric planning handlers.
//!
//! These handlers are report-only contract surfaces. They do not run engines,
//! read sources, write outputs, create checkpoints, probe external systems, or
//! provide fallback execution.

use std::{process::ExitCode, vec::IntoIter};

use shardloom_core::{
    Boundedness, CommandStatus, ContinuousViewCertificate, EngineCapabilityMatrixReport,
    EngineCapabilityRow, EngineMode, EngineSelectionReport, EngineSelectionRequest,
    FreshnessCertificate, LiveChangeContractReport, LiveFixtureOperator, LiveFixtureRunInput,
    LiveFixtureRunReport, OutputFormat, OutputMode, ShardLoomError, StateCertificate, UpdateMode,
    boundedness_vocabulary, engine_mode_vocabulary, output_mode_vocabulary,
    plan_live_change_contract, run_live_fixture, update_mode_vocabulary,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error,
};

const ENGINE_SELECTION_COMMAND: &str = "engine-selection-plan";
const ENGINE_SELECTION_SUMMARY: &str = "engine selection planning failed";
const ENGINE_SELECTION_USAGE: &str = "usage: shardloom engine-selection-plan [auto|batch|live|hybrid] [bounded|unbounded|snapshot|unknown] [snapshot|append-only|upsert|delete|retract|tombstone|changelog] [snapshot|append|update|complete|changelog|continuous-view]";

const ENGINE_CAPABILITY_MATRIX_COMMAND: &str = "engine-capability-matrix";
const LIVE_CHANGE_CONTRACT_COMMAND: &str = "live-change-contract-plan";
const LIVE_FIXTURE_RUN_COMMAND: &str = "live-fixture-run";
const LIVE_FIXTURE_RUN_SUMMARY: &str = "live fixture run failed";
const LIVE_FIXTURE_RUN_USAGE: &str = "usage: shardloom live-fixture-run [filter|project|count|count-where|group-count] [predicate|columns|group-column]";

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

pub(crate) fn handle_live_change_contract_plan(
    mut args: IntoIter<String>,
    format: OutputFormat,
) -> ExitCode {
    if let Some(extra) = args.next() {
        return emit_error(
            LIVE_CHANGE_CONTRACT_COMMAND,
            format,
            "live change contract failed",
            &cli_unknown_arg_error(LIVE_CHANGE_CONTRACT_COMMAND, &extra),
        );
    }
    let report = plan_live_change_contract();
    emit(
        LIVE_CHANGE_CONTRACT_COMMAND,
        format,
        CommandStatus::Success,
        "live change contract plan".to_string(),
        report.to_human_text(),
        vec![],
        live_change_contract_fields(&report),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_live_fixture_run(args: IntoIter<String>, format: OutputFormat) -> ExitCode {
    let input = match parse_live_fixture_run_args(args, format) {
        Ok(input) => input,
        Err(exit_code) => return exit_code,
    };
    let report = match run_live_fixture(input) {
        Ok(report) => report,
        Err(error) => {
            return emit_error(
                LIVE_FIXTURE_RUN_COMMAND,
                format,
                LIVE_FIXTURE_RUN_SUMMARY,
                &error,
            );
        }
    };
    emit(
        LIVE_FIXTURE_RUN_COMMAND,
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "live fixture run".to_string(),
        report.to_human_text(),
        vec![],
        live_fixture_run_fields(&report),
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
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

fn parse_live_fixture_run_args(
    mut args: IntoIter<String>,
    format: OutputFormat,
) -> Result<LiveFixtureRunInput, ExitCode> {
    let operator = match args.next() {
        Some(value) => parse_live_fixture_operator(&value, format)?,
        None => LiveFixtureOperator::Filter,
    };
    let argument = args.next();
    if let Some(extra) = args.next() {
        return Err(emit_error(
            LIVE_FIXTURE_RUN_COMMAND,
            format,
            LIVE_FIXTURE_RUN_SUMMARY,
            &cli_unknown_arg_error(LIVE_FIXTURE_RUN_COMMAND, &extra),
        ));
    }
    LiveFixtureRunInput::new(operator)
        .with_argument(argument.as_deref())
        .map_err(|error| {
            emit_error(
                LIVE_FIXTURE_RUN_COMMAND,
                format,
                LIVE_FIXTURE_RUN_SUMMARY,
                &ShardLoomError::InvalidOperation(format!(
                    "{}; {LIVE_FIXTURE_RUN_USAGE}",
                    error.message()
                )),
            )
        })
}

fn parse_live_fixture_operator(
    value: &str,
    format: OutputFormat,
) -> Result<LiveFixtureOperator, ExitCode> {
    LiveFixtureOperator::parse(value).map_err(|error| {
        emit_error(
            LIVE_FIXTURE_RUN_COMMAND,
            format,
            LIVE_FIXTURE_RUN_SUMMARY,
            &ShardLoomError::InvalidOperation(format!(
                "{}; {LIVE_FIXTURE_RUN_USAGE}",
                error.message()
            )),
        )
    })
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

fn live_change_contract_fields(report: &LiveChangeContractReport) -> Vec<(String, String)> {
    let mut fields = common_engine_contract_fields(
        report.schema_version,
        report.report_id,
        report.fallback_attempted(),
        report.external_engine_invoked,
        report.runtime_execution,
        report.data_read,
        report.write_io,
    );
    push_field(&mut fields, "mode", "live_change_contract_plan");
    push_bool_field(&mut fields, "plan_only", true);
    push_field(&mut fields, "execution", "not_performed");
    push_field(
        &mut fields,
        "change_record_field_order",
        &report.change_field_order(),
    );
    push_field(
        &mut fields,
        "change_operation_vocabulary",
        &report.operation_vocabulary(),
    );
    push_field(
        &mut fields,
        "watermark_policy",
        report.watermark_policy.as_str(),
    );
    push_field(
        &mut fields,
        "late_data_policy",
        report.late_data_policy.as_str(),
    );
    push_field(
        &mut fields,
        "state_ttl_policy",
        report.state_ttl_policy.as_str(),
    );
    push_field(
        &mut fields,
        "checkpoint_policy",
        report.checkpoint_policy.as_str(),
    );
    push_field(
        &mut fields,
        "output_changelog_vocabulary",
        &report.output_changelog_vocabulary(),
    );
    push_field(
        &mut fields,
        "fixture_operator_vocabulary",
        &report.fixture_operator_vocabulary(),
    );
    push_bool_field(
        &mut fields,
        "broker_integrations_deferred",
        report.broker_integrations_deferred,
    );
    push_bool_field(
        &mut fields,
        "runtime_integrations_deferred",
        report.runtime_integrations_deferred,
    );
    push_bool_field(
        &mut fields,
        "production_claim_allowed",
        report.production_claim_allowed,
    );
    fields
}

fn live_fixture_run_fields(report: &LiveFixtureRunReport) -> Vec<(String, String)> {
    let mut fields = common_engine_contract_fields(
        report.schema_version,
        &report.report_id,
        report.fallback_attempted(),
        report.external_engine_invoked,
        report.runtime_execution,
        report.data_read,
        report.write_io,
    );
    push_field(&mut fields, "mode", "live_fixture_run");
    push_field(&mut fields, "fixture_id", report.fixture_id);
    push_field(
        &mut fields,
        "fixture_operator",
        report.input.operator.as_str(),
    );
    push_field(&mut fields, "predicate", &report.input.predicate);
    push_field(
        &mut fields,
        "projection_columns",
        &report.input.projection_columns_text(),
    );
    push_field(&mut fields, "group_column", &report.input.group_column);
    push_bool_field(&mut fields, "fixture_in_memory", report.fixture_in_memory);
    push_bool_field(&mut fields, "broker_io", report.broker_io);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(
        &mut fields,
        "production_claim_allowed",
        report.production_claim_allowed,
    );
    push_count_field(
        &mut fields,
        "input_change_record_count",
        report.input_change_records.len(),
    );
    push_field(&mut fields, "sequence_range", &report.sequence_range());
    push_field(
        &mut fields,
        "input_operation_order",
        &report.input_operation_order(),
    );
    push_count_field(
        &mut fields,
        "active_state_key_count",
        report.active_state_key_count(),
    );
    push_count_field(&mut fields, "output_row_count", report.output_row_count());
    push_field(&mut fields, "output_rows", &report.output_rows_text());
    push_count_field(
        &mut fields,
        "output_changelog_record_count",
        report.output_changelog.len(),
    );
    push_field(
        &mut fields,
        "output_changelog_order",
        &report.output_changelog_order(),
    );
    append_freshness_certificate_fields(&mut fields, &report.freshness_certificate);
    append_state_certificate_fields(&mut fields, &report.state_certificate);
    append_continuous_view_certificate_fields(&mut fields, &report.continuous_view_certificate);
    append_execution_certificate_fields(&mut fields, report);
    append_native_io_certificate_fields(&mut fields, report);
    fields
}

fn append_freshness_certificate_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &FreshnessCertificate,
) {
    push_bool_field(fields, "freshness_certificate_emitted", true);
    push_field(
        fields,
        "freshness_certificate_schema_version",
        certificate.schema_version,
    );
    push_field(
        fields,
        "freshness_certificate_id",
        &certificate.certificate_id,
    );
    push_field(
        fields,
        "freshness_certificate_status",
        certificate.status.as_str(),
    );
    push_field(
        fields,
        "watermark_policy",
        certificate.watermark_policy.as_str(),
    );
    push_field(
        fields,
        "late_data_policy",
        certificate.late_data_policy.as_str(),
    );
    push_u64_field(fields, "watermark_ms", certificate.watermark_ms);
    push_u64_field(fields, "freshness_lag_ms", certificate.freshness_lag_ms);
    push_count_field(fields, "late_record_count", certificate.late_record_count);
}

fn append_state_certificate_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &StateCertificate,
) {
    push_bool_field(fields, "state_certificate_emitted", true);
    push_field(
        fields,
        "state_certificate_schema_version",
        certificate.schema_version,
    );
    push_field(fields, "state_certificate_id", &certificate.certificate_id);
    push_field(
        fields,
        "state_certificate_status",
        certificate.status.as_str(),
    );
    push_field(
        fields,
        "state_ttl_policy",
        certificate.state_ttl_policy.as_str(),
    );
    push_field(
        fields,
        "checkpoint_policy",
        certificate.checkpoint_policy.as_str(),
    );
    push_field(fields, "checkpoint_ref", &certificate.checkpoint_ref);
    push_count_field(
        fields,
        "checkpoint_record_count",
        certificate.checkpoint_record_count,
    );
    push_count_field(fields, "append_count", certificate.append_count);
    push_count_field(fields, "upsert_count", certificate.upsert_count);
    push_count_field(fields, "delete_count", certificate.delete_count);
    push_count_field(fields, "retract_count", certificate.retract_count);
    push_count_field(fields, "tombstone_count", certificate.tombstone_count);
    push_bool_field(
        fields,
        "checkpoint_write_performed",
        certificate.checkpoint_write_performed,
    );
}

fn append_continuous_view_certificate_fields(
    fields: &mut Vec<(String, String)>,
    certificate: &ContinuousViewCertificate,
) {
    push_bool_field(fields, "continuous_view_certificate_emitted", true);
    push_field(
        fields,
        "continuous_view_certificate_schema_version",
        certificate.schema_version,
    );
    push_field(
        fields,
        "continuous_view_certificate_id",
        &certificate.certificate_id,
    );
    push_field(
        fields,
        "continuous_view_certificate_status",
        certificate.status.as_str(),
    );
    push_field(fields, "output_mode", certificate.output_mode.as_str());
    push_field(fields, "result_ref", &certificate.result_ref);
    push_count_field(
        fields,
        "continuous_view_row_count",
        certificate.continuous_view_row_count,
    );
    push_bool_field(
        fields,
        "continuous_view_deterministic_order",
        certificate.deterministic_order,
    );
}

fn append_execution_certificate_fields(
    fields: &mut Vec<(String, String)>,
    report: &LiveFixtureRunReport,
) {
    let certificate = &report.execution_certificate;
    push_bool_field(fields, "execution_certificate_emitted", true);
    push_field(
        fields,
        "execution_certificate_id",
        &certificate.certificate_id,
    );
    push_field(
        fields,
        "execution_certificate_status",
        certificate.status.as_str(),
    );
    push_field(
        fields,
        "execution_certificate_fixture_id",
        certificate
            .correctness_fixture_id
            .as_deref()
            .unwrap_or("none"),
    );
    push_bool_field(
        fields,
        "execution_certificate_correctness_passed",
        certificate.correctness_passed,
    );
    push_bool_field(
        fields,
        "execution_certificate_fallback_attempted",
        certificate.fallback_attempted,
    );
    push_bool_field(
        fields,
        "execution_certificate_external_query_engine_invoked",
        certificate.external_query_engine_invoked,
    );
}

fn append_native_io_certificate_fields(
    fields: &mut Vec<(String, String)>,
    report: &LiveFixtureRunReport,
) {
    let certificate = &report.native_io_certificate;
    push_bool_field(fields, "native_io_certificate_emitted", true);
    push_field(
        fields,
        "native_io_certificate_id",
        &certificate.certificate_id,
    );
    push_field(fields, "native_io_certificate_status", certificate.status());
    push_field(
        fields,
        "native_io_certificate_path_id",
        &certificate.path_id,
    );
    push_bool_field(
        fields,
        "native_io_certificate_fallback_attempted",
        certificate.fallback_attempted,
    );
    push_bool_field(
        fields,
        "native_io_certificate_source_streaming_capability",
        certificate.source_capability_report.streaming_capability,
    );
    push_bool_field(
        fields,
        "native_io_certificate_object_store_io",
        certificate.side_effects.object_store_io,
    );
    push_bool_field(
        fields,
        "native_io_certificate_write_io",
        certificate.side_effects.write_io,
    );
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

fn push_u64_field(fields: &mut Vec<(String, String)>, key: &str, value: u64) {
    push_field(fields, key, &value.to_string());
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    push_field(fields, key, if value { "true" } else { "false" });
}
