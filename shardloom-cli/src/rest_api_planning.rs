//! REST/API planning CLI handlers.
//!
//! This module owns the current report-only API protocol planning command. It
//! does not start a server, open sockets, or authorize remote execution.

use std::process::ExitCode;

use shardloom_core::{CliApiJsonProtocolReport, OutputFormat, ReleasePlan};

use crate::cli_output::emit;

pub(crate) fn handle_api_compat_plan(format: OutputFormat) -> ExitCode {
    let plan = ReleasePlan::default_foundation_plan();
    let protocol = CliApiJsonProtocolReport::contract_only();
    let mut diagnostics = plan.diagnostics.clone();
    diagnostics.extend(protocol.diagnostics.clone());
    emit(
        "api-compat-plan",
        format,
        protocol.status(),
        "api compatibility and cli json protocol foundation".to_string(),
        format!("{}\n\n{}", plan.to_human_text(), protocol.to_human_text()),
        diagnostics,
        api_protocol_fields(&protocol),
    );
    ExitCode::SUCCESS
}

pub(crate) fn api_protocol_fields(report: &CliApiJsonProtocolReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "mode", "api_compat_plan");
    push_field(&mut fields, "publish_allowed", "false");
    push_field(&mut fields, "published", "false");
    push_field(&mut fields, "execution", "not_performed");
    push_field(&mut fields, "plan_only", "true");
    push_field(&mut fields, "schema_version", report.schema_version);
    push_field(&mut fields, "protocol_id", report.protocol_id);
    push_field(&mut fields, "protocol_stability", report.protocol_stability);
    push_field(
        &mut fields,
        "output_envelope_schema_version",
        report.output_envelope_schema_version,
    );
    push_field(
        &mut fields,
        "required_envelope_fields",
        &report.required_envelope_fields.join(","),
    );
    push_field(
        &mut fields,
        "required_fallback_fields",
        &report.required_fallback_fields.join(","),
    );
    push_field(
        &mut fields,
        "required_diagnostic_fields",
        &report.required_diagnostic_fields.join(","),
    );
    push_field(
        &mut fields,
        "required_field_entry_fields",
        &report.required_field_entry_fields.join(","),
    );
    push_field(
        &mut fields,
        "required_typed_payload_fields",
        &report.required_typed_payload_fields.join(","),
    );
    push_bool_field(
        &mut fields,
        "legacy_fields_mirror_present",
        report.legacy_fields_mirror_present,
    );
    push_bool_field(
        &mut fields,
        "flat_fields_primary_payload_allowed",
        report.flat_fields_primary_payload_allowed,
    );
    push_field(
        &mut fields,
        "command_status_values",
        &report.command_status_values.join(","),
    );
    append_api_protocol_compatibility_lock_fields(&mut fields, report);
    push_field(
        &mut fields,
        "output_formats",
        &report.output_formats.join(","),
    );
    push_field(
        &mut fields,
        "thin_python_wrapper_boundary",
        report.thin_python_wrapper_boundary,
    );
    push_bool_field(
        &mut fields,
        "pyo3_maturin_allowed",
        report.pyo3_maturin_allowed,
    );
    push_bool_field(&mut fields, "foundry_required", report.foundry_required);
    push_bool_field(
        &mut fields,
        "dataframe_api_implemented",
        report.dataframe_api_implemented,
    );
    push_bool_field(&mut fields, "side_effect_free", report.side_effect_free);
    push_bool_field(&mut fields, "filesystem_probe", report.filesystem_probe);
    push_bool_field(&mut fields, "network_probe", report.network_probe);
    push_bool_field(&mut fields, "catalog_probe", report.catalog_probe);
    push_bool_field(&mut fields, "adapter_probe", report.adapter_probe);
    push_bool_field(&mut fields, "parser_executed", report.parser_executed);
    push_bool_field(&mut fields, "runtime_execution", report.runtime_execution);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_field(&mut fields, "external_publish", "not_performed");
    push_bool_field(
        &mut fields,
        "external_publish_performed",
        report.external_publish,
    );
    push_bool_field(
        &mut fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(&mut fields, "fallback_attempted", report.fallback_attempted);
    push_count_field(&mut fields, "diagnostic_count", report.diagnostics.len());
    fields
}

fn append_api_protocol_compatibility_lock_fields(
    fields: &mut Vec<(String, String)>,
    report: &CliApiJsonProtocolReport,
) {
    push_field(
        fields,
        "compatibility_lock_status",
        report.compatibility_lock_status,
    );
    push_field(
        fields,
        "compatibility_lock_fixture_statuses",
        &report.compatibility_lock_fixture_statuses.join(","),
    );
    push_bool_field(
        fields,
        "json_error_paths_enveloped",
        report.json_error_paths_enveloped,
    );
    push_bool_field(
        fields,
        "unknown_command_json_enveloped",
        report.unknown_command_json_enveloped,
    );
    push_bool_field(
        fields,
        "missing_binary_error_payload_shaped",
        report.missing_binary_error_payload_shaped,
    );
}

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    fields.push((key.to_string(), value.to_string()));
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    push_field(fields, key, &value.to_string());
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    push_field(fields, key, &value.to_string());
}
