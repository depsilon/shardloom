//! REST/API planning CLI handlers.
//!
//! This module owns the current report-only API protocol planning command. It
//! does not start a server, open sockets, or authorize remote execution.

use std::process::ExitCode;

use shardloom_core::{
    CliApiJsonProtocolReport, OutputFormat, ReleasePlan, RestApiContractReport,
    RestApiDiscoveryModeReport, ShardLoomError,
};

use crate::cli_output::{emit, emit_error};

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

pub(crate) fn handle_rest_api_contract_plan(format: OutputFormat) -> ExitCode {
    let report = RestApiContractReport::contract_only();
    emit(
        "rest-api-contract-plan",
        format,
        report.status(),
        "rest api contract and discovery surface".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        rest_api_contract_fields(&report, "rest_api_contract_plan"),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_serve_command(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let mut mode: Option<String> = None;
    let mut bind = "127.0.0.1:8787".to_string();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--mode" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        "serve",
                        format,
                        "serve argument parsing failed",
                        &ShardLoomError::InvalidOperation(
                            "missing value for --mode; expected discovery".to_string(),
                        ),
                    );
                };
                mode = Some(value);
            }
            "--bind" => {
                let Some(value) = args.next() else {
                    return emit_error(
                        "serve",
                        format,
                        "serve argument parsing failed",
                        &ShardLoomError::InvalidOperation(
                            "missing value for --bind; expected host:port".to_string(),
                        ),
                    );
                };
                bind = value;
            }
            _ => {
                return emit_error(
                    "serve",
                    format,
                    "serve argument parsing failed",
                    &ShardLoomError::InvalidOperation(format!(
                        "unknown serve argument: {arg}; only --mode discovery and optional --bind are supported"
                    )),
                );
            }
        }
    }

    match mode.as_deref() {
        Some("discovery") => {
            let report = RestApiDiscoveryModeReport::contract_only(bind);
            emit(
                "serve",
                format,
                report.status(),
                "rest discovery mode contract".to_string(),
                report.to_human_text(),
                report.diagnostics.clone(),
                rest_api_discovery_fields(&report),
            );
            ExitCode::SUCCESS
        }
        Some(other) => emit_error(
            "serve",
            format,
            "serve mode is not available",
            &ShardLoomError::InvalidOperation(format!(
                "unsupported serve mode: {other}; only discovery contract mode is available"
            )),
        ),
        None => emit_error(
            "serve",
            format,
            "serve mode is required",
            &ShardLoomError::InvalidOperation(
                "serve requires --mode discovery; no server is started by this contract surface"
                    .to_string(),
            ),
        ),
    }
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

fn rest_api_contract_fields(
    report: &RestApiContractReport,
    mode: &'static str,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    push_field(&mut fields, "mode", mode);
    push_field(&mut fields, "schema_version", report.schema_version);
    push_field(&mut fields, "report_id", report.report_id);
    push_field(&mut fields, "api_version", report.api_version);
    push_field(&mut fields, "openapi_version", report.openapi_version);
    push_field(
        &mut fields,
        "openapi_contract_path",
        report.openapi_contract_path,
    );
    push_bool_field(
        &mut fields,
        "openapi_contract_artifact_checked_in",
        report.contract_artifact_checked_in,
    );
    push_field(
        &mut fields,
        "problem_details_media_type",
        report.problem_details_media_type,
    );
    push_field(
        &mut fields,
        "represented_resources",
        &report.represented_resources.join(","),
    );
    push_count_field(
        &mut fields,
        "represented_resource_count",
        report.represented_resources.len(),
    );
    push_field(
        &mut fields,
        "execution_policy_fields",
        &report.execution_policy_fields.join(","),
    );
    push_field(
        &mut fields,
        "result_policy_modes",
        &report.result_policy_modes.join(","),
    );
    push_field(
        &mut fields,
        "api_maturity_stage_statuses",
        &report.maturity_stage_summary(),
    );
    push_field(
        &mut fields,
        "discovery_endpoint_paths",
        &report.endpoint_paths().join(","),
    );
    push_count_field(
        &mut fields,
        "discovery_endpoint_count",
        report.discovery_endpoints.len(),
    );
    push_bool_field(
        &mut fields,
        "discovery_endpoints_side_effect_free",
        report.discovery_endpoints_side_effect_free(),
    );
    push_bool_field(&mut fields, "server_started", report.server_started);
    push_bool_field(
        &mut fields,
        "network_listener_opened",
        report.network_listener_opened,
    );
    push_bool_field(&mut fields, "network_probe", report.network_listener_opened);
    push_bool_field(&mut fields, "dataset_probe", report.dataset_probe);
    push_bool_field(&mut fields, "object_store_io", report.object_store_io);
    push_bool_field(&mut fields, "catalog_probe", report.catalog_probe);
    push_bool_field(
        &mut fields,
        "credential_resolution",
        report.credential_resolution,
    );
    push_bool_field(&mut fields, "query_execution", report.query_execution);
    push_bool_field(&mut fields, "runtime_execution", report.runtime_execution);
    push_bool_field(&mut fields, "write_io", report.write_io);
    push_bool_field(
        &mut fields,
        "external_engine_invoked",
        report.external_engine_invoked,
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

fn rest_api_discovery_fields(report: &RestApiDiscoveryModeReport) -> Vec<(String, String)> {
    let mut fields = rest_api_contract_fields(&report.contract_report, "rest_api_discovery_mode");
    push_field(
        &mut fields,
        "discovery_schema_version",
        report.schema_version,
    );
    push_field(&mut fields, "discovery_report_id", report.report_id);
    push_field(&mut fields, "server_mode", report.mode);
    push_field(&mut fields, "bind", &report.bind);
    push_field(&mut fields, "health_endpoint", report.health_endpoint);
    push_field(&mut fields, "version_endpoint", report.version_endpoint);
    push_field(
        &mut fields,
        "capabilities_endpoint",
        report.capabilities_endpoint,
    );
    push_field(&mut fields, "adapters_endpoint", report.adapters_endpoint);
    push_bool_field(
        &mut fields,
        "serve_command_contract_only",
        !report.server_started,
    );
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
