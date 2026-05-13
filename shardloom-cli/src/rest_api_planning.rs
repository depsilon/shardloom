//! REST/API planning CLI handlers.
//!
//! This module owns the current report-only API protocol planning command. It
//! does not start a server, open sockets, or authorize remote execution.

use std::process::ExitCode;

use shardloom_core::{
    CliApiJsonProtocolReport, OutputFormat, ReleasePlan, RestApiContractReport,
    RestApiDataPlaneReport, RestApiDataPlaneScenario, RestApiDiscoveryModeReport,
    RestApiEventStreamReport, RestApiEventStreamScenario, RestApiLocalLifecycleReport,
    RestApiLocalLifecycleScenario, RestApiPlanPreviewReport, RestApiPlanPreviewScenario,
    RestApiSecurityGovernanceReport, RestApiSecurityGovernanceScenario, ShardLoomError,
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

pub(crate) fn handle_rest_api_plan_preview(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let scenario = match args.next() {
        Some(token) => {
            let Some(parsed) = RestApiPlanPreviewScenario::parse(&token) else {
                return emit_error(
                    "rest-api-plan-preview",
                    format,
                    "rest api plan preview argument parsing failed",
                    &ShardLoomError::InvalidOperation(format!(
                        "unknown plan preview scenario: {token}; expected one of {}",
                        RestApiPlanPreviewScenario::all()
                            .iter()
                            .map(|scenario| scenario.as_str())
                            .collect::<Vec<_>>()
                            .join(",")
                    )),
                );
            };
            parsed
        }
        None => RestApiPlanPreviewScenario::CertifiedLocalBatch,
    };

    if let Some(extra) = args.next() {
        return emit_error(
            "rest-api-plan-preview",
            format,
            "rest api plan preview argument parsing failed",
            &ShardLoomError::InvalidOperation(format!(
                "unexpected rest-api-plan-preview argument: {extra}; pass at most one scenario"
            )),
        );
    }

    let report = RestApiPlanPreviewReport::for_scenario(scenario);
    emit(
        "rest-api-plan-preview",
        format,
        report.status(),
        "rest api plan/explain/validate/certification preview".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        rest_api_plan_preview_fields(&report),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_rest_api_local_lifecycle(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let scenario = match args.next() {
        Some(token) => {
            let Some(parsed) = RestApiLocalLifecycleScenario::parse(&token) else {
                return emit_error(
                    "rest-api-local-lifecycle",
                    format,
                    "rest api local lifecycle argument parsing failed",
                    &ShardLoomError::InvalidOperation(format!(
                        "unknown local lifecycle scenario: {token}; expected one of {}",
                        RestApiLocalLifecycleScenario::all()
                            .iter()
                            .map(|scenario| scenario.as_str())
                            .collect::<Vec<_>>()
                            .join(",")
                    )),
                );
            };
            parsed
        }
        None => RestApiLocalLifecycleScenario::CertifiedLocalBatch,
    };

    if let Some(extra) = args.next() {
        return emit_error(
            "rest-api-local-lifecycle",
            format,
            "rest api local lifecycle argument parsing failed",
            &ShardLoomError::InvalidOperation(format!(
                "unexpected rest-api-local-lifecycle argument: {extra}; pass at most one scenario"
            )),
        );
    }

    let report = RestApiLocalLifecycleReport::for_scenario(scenario);
    emit(
        "rest-api-local-lifecycle",
        format,
        report.status(),
        "rest api certified local lifecycle and result delivery".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        rest_api_local_lifecycle_fields(&report),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_rest_api_event_stream(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let scenario = match args.next() {
        Some(token) => {
            let Some(parsed) = RestApiEventStreamScenario::parse(&token) else {
                return emit_error(
                    "rest-api-event-stream",
                    format,
                    "rest api event stream argument parsing failed",
                    &ShardLoomError::InvalidOperation(format!(
                        "unknown event stream scenario: {token}; expected one of {}",
                        RestApiEventStreamScenario::all()
                            .iter()
                            .map(|scenario| scenario.as_str())
                            .collect::<Vec<_>>()
                            .join(",")
                    )),
                );
            };
            parsed
        }
        None => RestApiEventStreamScenario::CertifiedLiveFixture,
    };

    if let Some(extra) = args.next() {
        return emit_error(
            "rest-api-event-stream",
            format,
            "rest api event stream argument parsing failed",
            &ShardLoomError::InvalidOperation(format!(
                "unexpected rest-api-event-stream argument: {extra}; pass at most one scenario"
            )),
        );
    }

    let report = RestApiEventStreamReport::for_scenario(scenario);
    emit(
        "rest-api-event-stream",
        format,
        report.status(),
        "rest api live/hybrid event stream contract".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        rest_api_event_stream_fields(&report),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_rest_api_security_governance(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let scenario = match args.next() {
        Some(token) => {
            let Some(parsed) = RestApiSecurityGovernanceScenario::parse(&token) else {
                return emit_error(
                    "rest-api-security-governance",
                    format,
                    "rest api security governance argument parsing failed",
                    &ShardLoomError::InvalidOperation(format!(
                        "unknown security governance scenario: {token}; expected one of {}",
                        RestApiSecurityGovernanceScenario::all()
                            .iter()
                            .map(|scenario| scenario.as_str())
                            .collect::<Vec<_>>()
                            .join(",")
                    )),
                );
            };
            parsed
        }
        None => RestApiSecurityGovernanceScenario::SafeLocalDefault,
    };

    if let Some(extra) = args.next() {
        return emit_error(
            "rest-api-security-governance",
            format,
            "rest api security governance argument parsing failed",
            &ShardLoomError::InvalidOperation(format!(
                "unexpected rest-api-security-governance argument: {extra}; pass at most one scenario"
            )),
        );
    }

    let report = RestApiSecurityGovernanceReport::for_scenario(scenario);
    emit(
        "rest-api-security-governance",
        format,
        report.status(),
        "rest api security, governance, observability, and agent contract".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        rest_api_security_governance_fields(&report),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_rest_api_data_plane(
    mut args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let scenario = match args.next() {
        Some(token) => {
            let Some(parsed) = RestApiDataPlaneScenario::parse(&token) else {
                return emit_error(
                    "rest-api-data-plane",
                    format,
                    "rest api data plane argument parsing failed",
                    &ShardLoomError::InvalidOperation(format!(
                        "unknown data plane scenario: {token}; expected one of {}",
                        RestApiDataPlaneScenario::all()
                            .iter()
                            .map(|scenario| scenario.as_str())
                            .collect::<Vec<_>>()
                            .join(",")
                    )),
                );
            };
            parsed
        }
        None => RestApiDataPlaneScenario::ArtifactReferenceDefault,
    };

    if let Some(extra) = args.next() {
        return emit_error(
            "rest-api-data-plane",
            format,
            "rest api data plane argument parsing failed",
            &ShardLoomError::InvalidOperation(format!(
                "unexpected rest-api-data-plane argument: {extra}; pass at most one scenario"
            )),
        );
    }

    let report = RestApiDataPlaneReport::for_scenario(scenario);
    emit(
        "rest-api-data-plane",
        format,
        report.status(),
        "rest api columnar data-plane and standards boundary contract".to_string(),
        report.to_human_text(),
        report.diagnostics.clone(),
        rest_api_data_plane_fields(&report),
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

#[allow(clippy::too_many_lines)]
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
    append_rest_no_effect_parity_fields(
        &mut fields,
        report.runtime_execution,
        false,
        report.write_io,
        report.fallback_attempted,
        false,
        report.external_publish,
    );
    push_count_field(&mut fields, "diagnostic_count", report.diagnostics.len());
    fields
}

#[allow(clippy::too_many_lines)]
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
    append_rest_no_effect_parity_fields(
        &mut fields,
        report.runtime_execution,
        false,
        report.write_io,
        report.fallback_attempted,
        report.external_engine_invoked,
        false,
    );
    push_count_field(&mut fields, "diagnostic_count", report.diagnostics.len());
    fields
}

fn rest_api_discovery_fields(report: &RestApiDiscoveryModeReport) -> Vec<(String, String)> {
    let mut fields = rest_api_contract_fields(&report.contract_report, "rest_api_discovery_mode");
    set_field(&mut fields, "schema_version", report.schema_version);
    set_field(&mut fields, "report_id", report.report_id);
    push_field(
        &mut fields,
        "contract_schema_version",
        report.contract_report.schema_version,
    );
    push_field(
        &mut fields,
        "contract_report_id",
        report.contract_report.report_id,
    );
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

fn rest_api_plan_preview_fields(report: &RestApiPlanPreviewReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_plan_preview_identity_fields(&mut fields, report);
    append_plan_preview_stage_fields(&mut fields, report);
    append_plan_preview_problem_fields(&mut fields, report);
    append_plan_preview_effect_fields(&mut fields, report);
    push_count_field(&mut fields, "diagnostic_count", report.diagnostics.len());
    fields
}

fn append_plan_preview_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiPlanPreviewReport,
) {
    push_field(fields, "mode", "rest_api_plan_preview");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", report.report_id);
    push_field(fields, "api_version", report.api_version);
    push_field(fields, "scenario", report.scenario.as_str());
    push_field(fields, "preview_status", report.preview_status.as_str());
    push_field(fields, "plan_handle", report.plan_handle);
    push_field(fields, "endpoint_path", report.endpoint_path);
    push_field(fields, "endpoint_paths", &report.endpoint_paths.join(","));
    push_field(
        fields,
        "preview_operations",
        &report.preview_operations.join(","),
    );
    push_field(
        fields,
        "execution_policy_fields",
        &report.execution_policy_fields.join(","),
    );
}

fn append_plan_preview_stage_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiPlanPreviewReport,
) {
    push_field(fields, "stage_order", &report.stage_order().join(","));
    push_field(fields, "stage_statuses", &report.stage_status_summary());
    push_field(
        fields,
        "stage_diagnostics",
        &report.stage_diagnostic_summary(),
    );
    for stage in &report.stages {
        push_field(
            fields,
            &format!("{}_stage_status", stage.stage_id),
            stage.status.as_str(),
        );
        push_field(
            fields,
            &format!("{}_stage_summary", stage.stage_id),
            stage.summary,
        );
        push_field(
            fields,
            &format!("{}_stage_diagnostic_code", stage.stage_id),
            stage.diagnostic_code.unwrap_or("none"),
        );
    }
}

fn append_plan_preview_problem_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiPlanPreviewReport,
) {
    push_bool_field(
        fields,
        "problem_details_emitted",
        report.problem_details_emitted(),
    );
    if let Some(problem) = &report.problem_details {
        push_field(fields, "problem_details_type", problem.problem_type);
        push_field(fields, "problem_details_title", problem.title);
        push_field(
            fields,
            "problem_details_status",
            &problem.http_status.to_string(),
        );
        push_field(fields, "problem_details_detail", problem.detail);
        push_field(
            fields,
            "problem_details_diagnostic_code",
            problem.diagnostic_code,
        );
        push_field(
            fields,
            "unsupported_reason",
            problem.unsupported_reason.unwrap_or("none"),
        );
    } else {
        push_field(fields, "problem_details_type", "none");
        push_field(fields, "problem_details_title", "none");
        push_field(fields, "problem_details_status", "none");
        push_field(fields, "problem_details_detail", "none");
        push_field(fields, "problem_details_diagnostic_code", "none");
        push_field(fields, "unsupported_reason", "none");
    }
}

fn append_plan_preview_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiPlanPreviewReport,
) {
    push_bool_field(fields, "server_started", report.server_started);
    push_bool_field(
        fields,
        "network_listener_opened",
        report.network_listener_opened,
    );
    push_bool_field(fields, "network_probe", report.network_listener_opened);
    push_bool_field(fields, "dataset_probe", report.dataset_probe);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "catalog_probe", report.catalog_probe);
    push_bool_field(
        fields,
        "credential_resolution",
        report.credential_resolution,
    );
    push_bool_field(fields, "parser_executed", report.parser_executed);
    push_bool_field(fields, "binder_executed", report.binder_executed);
    push_bool_field(
        fields,
        "native_logical_planned",
        report.native_logical_planned,
    );
    push_bool_field(
        fields,
        "native_physical_planned",
        report.native_physical_planned,
    );
    push_bool_field(fields, "query_execution", report.query_execution);
    push_bool_field(fields, "runtime_execution", report.runtime_execution);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(
        fields,
        "external_engine_invoked",
        report.external_engine_invoked,
    );
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(fields, "execution_delegated", report.execution_delegated);
    push_bool_field(
        fields,
        "effect_policy_violated",
        report.effect_policy_violated(),
    );
    append_rest_no_effect_parity_fields(
        fields,
        report.runtime_execution,
        false,
        report.write_io,
        report.fallback_attempted,
        report.external_engine_invoked,
        report.effect_policy_violated(),
    );
}

fn rest_api_local_lifecycle_fields(report: &RestApiLocalLifecycleReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_local_lifecycle_identity_fields(&mut fields, report);
    append_local_lifecycle_result_fields(&mut fields, report);
    append_local_lifecycle_evidence_fields(&mut fields, report);
    append_local_lifecycle_control_fields(&mut fields, report);
    append_local_lifecycle_effect_fields(&mut fields, report);
    push_count_field(&mut fields, "diagnostic_count", report.diagnostics.len());
    fields
}

fn append_local_lifecycle_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiLocalLifecycleReport,
) {
    push_field(fields, "mode", "rest_api_local_lifecycle");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", report.report_id);
    push_field(fields, "api_version", report.api_version);
    push_field(fields, "scenario", report.scenario.as_str());
    push_field(fields, "lifecycle_status", report.lifecycle_status.as_str());
    push_field(fields, "query_id", report.query_id);
    push_field(fields, "plan_handle", report.plan_handle);
    push_field(fields, "endpoint_paths", &report.endpoint_paths.join(","));
    push_field(
        fields,
        "lifecycle_operations",
        &report.lifecycle_operations.join(","),
    );
    push_field(
        fields,
        "lifecycle_events",
        &report.lifecycle_event_summary(),
    );
}

fn append_local_lifecycle_result_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiLocalLifecycleReport,
) {
    push_field(fields, "result_id", report.result_id);
    push_field(fields, "result_ref", report.result_ref);
    push_field(fields, "result_artifact_ref", report.result_artifact_ref);
    push_field(fields, "result_policies", &report.result_policy_summary());
    push_bool_field(
        fields,
        "inline_json_available",
        report.inline_json_available,
    );
    push_bool_field(fields, "paged_json_available", report.paged_json_available);
    push_bool_field(
        fields,
        "jsonl_ndjson_available",
        report.jsonl_ndjson_available,
    );
    push_bool_field(
        fields,
        "vortex_artifact_available",
        report.vortex_artifact_available,
    );
    push_bool_field(
        fields,
        "object_reference_available",
        report.object_reference_available,
    );
    push_bool_field(fields, "arrow_ipc_available", report.arrow_ipc_available);
    push_field(
        fields,
        "arrow_ipc_materialization",
        report.arrow_ipc_materialization,
    );
    push_bool_field(
        fields,
        "arrow_ipc_certified_native",
        report.arrow_ipc_certified_native,
    );
    push_field(
        fields,
        "preferred_high_fidelity_result_modes",
        &report.preferred_high_fidelity_result_modes.join(","),
    );
    push_field(
        fields,
        "result_ttl_seconds",
        &report.result_ttl_seconds.to_string(),
    );
    push_field(fields, "retention_policy", report.retention_policy);
    push_bool_field(fields, "cleanup_required", report.cleanup_required);
    push_field(fields, "cleanup_endpoint", report.cleanup_endpoint);
}

fn append_local_lifecycle_evidence_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiLocalLifecycleReport,
) {
    push_field(
        fields,
        "execution_certificate_ref",
        report.execution_certificate_ref,
    );
    push_field(
        fields,
        "native_io_certificate_ref",
        report.native_io_certificate_ref,
    );
    push_field(
        fields,
        "materialization_boundary_report_ref",
        report.materialization_boundary_report_ref,
    );
    push_field(fields, "profile_artifact_ref", report.profile_artifact_ref);
    push_field(fields, "lineage_artifact_ref", report.lineage_artifact_ref);
    push_field(
        fields,
        "no_fallback_evidence_artifact_ref",
        report.no_fallback_evidence_artifact_ref,
    );
}

fn append_local_lifecycle_control_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiLocalLifecycleReport,
) {
    push_bool_field(
        fields,
        "non_certified_path_blocked",
        report.non_certified_path_blocked,
    );
    push_bool_field(
        fields,
        "cancellation_requested",
        report.cancellation_requested,
    );
    push_field(fields, "cancellation_status", report.cancellation_status);
    push_field(
        fields,
        "cancel_diagnostic_code",
        report.cancel_diagnostic_code,
    );
    push_bool_field(fields, "retry_requested", report.retry_requested);
    push_field(fields, "retry_status", report.retry_status);
    push_field(
        fields,
        "retry_diagnostic_code",
        report.retry_diagnostic_code,
    );
}

fn append_local_lifecycle_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiLocalLifecycleReport,
) {
    push_bool_field(fields, "server_started", report.server_started);
    push_bool_field(
        fields,
        "network_listener_opened",
        report.network_listener_opened,
    );
    push_bool_field(fields, "network_probe", report.network_listener_opened);
    push_bool_field(fields, "dataset_probe", report.dataset_probe);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "catalog_probe", report.catalog_probe);
    push_bool_field(
        fields,
        "credential_resolution",
        report.credential_resolution,
    );
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "data_materialized", report.data_materialized);
    push_bool_field(fields, "query_execution", report.query_execution);
    push_bool_field(fields, "runtime_execution", report.runtime_execution);
    push_bool_field(
        fields,
        "local_execution_performed",
        report.local_execution_performed,
    );
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(
        fields,
        "external_engine_invoked",
        report.external_engine_invoked,
    );
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(fields, "execution_delegated", report.execution_delegated);
    push_bool_field(
        fields,
        "effect_policy_violated",
        report.effect_policy_violated(),
    );
    append_rest_no_effect_parity_fields(
        fields,
        report.runtime_execution,
        report.data_read,
        report.write_io,
        report.fallback_attempted,
        report.external_engine_invoked,
        report.effect_policy_violated(),
    );
}

fn rest_api_event_stream_fields(report: &RestApiEventStreamReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_event_stream_identity_fields(&mut fields, report);
    append_event_stream_protocol_fields(&mut fields, report);
    append_event_stream_evidence_fields(&mut fields, report);
    append_event_stream_certification_fields(&mut fields, report);
    append_event_stream_effect_fields(&mut fields, report);
    push_count_field(&mut fields, "diagnostic_count", report.diagnostics.len());
    fields
}

fn append_event_stream_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiEventStreamReport,
) {
    push_field(fields, "mode", "rest_api_event_stream");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", report.report_id);
    push_field(fields, "api_version", report.api_version);
    push_field(fields, "scenario", report.scenario.as_str());
    push_field(
        fields,
        "event_stream_status",
        report.event_stream_status.as_str(),
    );
    push_field(fields, "stream_id", report.stream_id);
    push_field(fields, "stream_ref", report.stream_ref);
    push_field(fields, "engine_mode", report.engine_mode);
    push_field(fields, "workload_ref", report.workload_ref);
    push_field(fields, "endpoint_paths", &report.endpoint_paths.join(","));
    push_field(
        fields,
        "event_operations",
        &report.event_operations.join(","),
    );
}

fn append_event_stream_protocol_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiEventStreamReport,
) {
    push_field(
        fields,
        "delivery_protocols",
        &report.delivery_protocols.join(","),
    );
    push_bool_field(fields, "sse_first", report.sse_first);
    push_field(fields, "sse_media_type", report.sse_media_type);
    push_bool_field(fields, "websocket_supported", report.websocket_supported);
    push_bool_field(fields, "websocket_required", report.websocket_required);
    push_bool_field(
        fields,
        "bidirectional_interaction_required",
        report.bidirectional_interaction_required,
    );
    push_field(
        fields,
        "openapi_contract_path",
        report.openapi_contract_path,
    );
    push_field(fields, "asyncapi_version", report.asyncapi_version);
    push_field(
        fields,
        "asyncapi_contract_path",
        report.asyncapi_contract_path,
    );
    push_field(
        fields,
        "cloudevents_spec_version",
        report.cloudevents_spec_version,
    );
    push_field(
        fields,
        "cloudevents_required_fields",
        &report.cloudevents_required_field_summary(),
    );
    push_field(fields, "event_types", &report.event_type_summary());
    push_field(fields, "event_contracts", &report.event_contract_summary());
    push_field(fields, "event_count", &report.event_count.to_string());
    push_field(
        fields,
        "progress_event_count",
        &report.progress_event_count.to_string(),
    );
    push_field(
        fields,
        "state_event_count",
        &report.state_event_count.to_string(),
    );
    push_field(
        fields,
        "checkpoint_event_count",
        &report.checkpoint_event_count.to_string(),
    );
    push_field(
        fields,
        "watermark_event_count",
        &report.watermark_event_count.to_string(),
    );
    push_field(
        fields,
        "certificate_event_count",
        &report.certificate_event_count.to_string(),
    );
    push_field(
        fields,
        "lineage_event_count",
        &report.lineage_event_count.to_string(),
    );
    push_field(
        fields,
        "benchmark_event_count",
        &report.benchmark_event_count.to_string(),
    );
    push_field(
        fields,
        "hot_cold_contribution_event_count",
        &report.hot_cold_contribution_event_count.to_string(),
    );
}

fn append_event_stream_evidence_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiEventStreamReport,
) {
    push_field(
        fields,
        "freshness_certificate_ref",
        report.freshness_certificate_ref,
    );
    push_field(
        fields,
        "state_certificate_ref",
        report.state_certificate_ref,
    );
    push_field(
        fields,
        "continuous_view_certificate_ref",
        report.continuous_view_certificate_ref,
    );
    push_field(
        fields,
        "delta_overlay_certificate_ref",
        report.delta_overlay_certificate_ref,
    );
    push_field(
        fields,
        "micro_segment_flush_evidence_ref",
        report.micro_segment_flush_evidence_ref,
    );
    push_field(
        fields,
        "hot_cold_contribution_report_ref",
        report.hot_cold_contribution_report_ref,
    );
    push_field(
        fields,
        "execution_certificate_ref",
        report.execution_certificate_ref,
    );
    push_field(
        fields,
        "native_io_certificate_ref",
        report.native_io_certificate_ref,
    );
    push_field(fields, "lineage_artifact_ref", report.lineage_artifact_ref);
    push_field(fields, "benchmark_event_ref", report.benchmark_event_ref);
    push_field(
        fields,
        "no_fallback_evidence_artifact_ref",
        report.no_fallback_evidence_artifact_ref,
    );
    push_field(
        fields,
        "certificate_ref_summary",
        &report.certificate_ref_summary(),
    );
}

fn append_event_stream_certification_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiEventStreamReport,
) {
    push_bool_field(
        fields,
        "live_fixture_certified",
        report.live_fixture_certified,
    );
    push_bool_field(
        fields,
        "hybrid_fixture_certified",
        report.hybrid_fixture_certified,
    );
    push_bool_field(fields, "workload_certified", report.workload_certified);
    push_bool_field(
        fields,
        "cg22_workload_evidence_present",
        report.cg22_workload_evidence_present,
    );
    push_bool_field(
        fields,
        "cg8_runtime_evidence_present",
        report.cg8_runtime_evidence_present,
    );
    push_bool_field(
        fields,
        "cg4_checkpoint_evidence_present",
        report.cg4_checkpoint_evidence_present,
    );
    push_bool_field(
        fields,
        "cg16_execution_certificate_present",
        report.cg16_execution_certificate_present,
    );
    push_bool_field(
        fields,
        "production_claim_allowed",
        report.production_claim_allowed,
    );
    push_bool_field(fields, "broker_requested", report.broker_requested);
    push_bool_field(fields, "broker_required", report.broker_required);
    push_bool_field(
        fields,
        "object_store_required",
        report.object_store_required,
    );
}

fn append_event_stream_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiEventStreamReport,
) {
    push_bool_field(fields, "server_started", report.server_started);
    push_bool_field(
        fields,
        "network_listener_opened",
        report.network_listener_opened,
    );
    push_bool_field(fields, "network_probe", report.network_listener_opened);
    push_bool_field(fields, "broker_io", report.broker_io);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "dataset_probe", report.dataset_probe);
    push_bool_field(fields, "catalog_probe", report.catalog_probe);
    push_bool_field(
        fields,
        "credential_resolution",
        report.credential_resolution,
    );
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "data_materialized", report.data_materialized);
    push_bool_field(fields, "query_execution", report.query_execution);
    push_bool_field(fields, "runtime_execution", report.runtime_execution);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(
        fields,
        "external_engine_invoked",
        report.external_engine_invoked,
    );
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(fields, "execution_delegated", report.execution_delegated);
    push_bool_field(
        fields,
        "effect_policy_violated",
        report.effect_policy_violated(),
    );
    append_rest_no_effect_parity_fields(
        fields,
        report.runtime_execution,
        report.data_read,
        report.write_io,
        report.fallback_attempted,
        report.external_engine_invoked,
        report.effect_policy_violated(),
    );
}

fn rest_api_security_governance_fields(
    report: &RestApiSecurityGovernanceReport,
) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_security_governance_identity_fields(&mut fields, report);
    append_security_governance_auth_scope_fields(&mut fields, report);
    append_security_governance_agent_evidence_fields(&mut fields, report);
    append_security_governance_problem_fields(&mut fields, report);
    append_security_governance_effect_fields(&mut fields, report);
    push_count_field(&mut fields, "diagnostic_count", report.diagnostics.len());
    fields
}

fn append_security_governance_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiSecurityGovernanceReport,
) {
    push_field(fields, "mode", "rest_api_security_governance");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", report.report_id);
    push_field(fields, "api_version", report.api_version);
    push_field(fields, "scenario", report.scenario.as_str());
    push_field(
        fields,
        "governance_status",
        report.governance_status.as_str(),
    );
    push_field(fields, "endpoint_paths", &report.endpoint_paths.join(","));
    push_field(
        fields,
        "governance_operations",
        &report.governance_operations.join(","),
    );
    push_field(
        fields,
        "openapi_contract_path",
        report.openapi_contract_path,
    );
    push_field(
        fields,
        "problem_details_media_type",
        report.problem_details_media_type,
    );
}

fn append_security_governance_auth_scope_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiSecurityGovernanceReport,
) {
    push_bool_field(fields, "local_only_default", report.local_only_default);
    push_field(fields, "auth_postures", &report.auth_posture_summary());
    push_field(fields, "api_scopes", &report.scope_summary());
    push_count_field(fields, "auth_posture_count", report.auth_postures.len());
    push_count_field(fields, "api_scope_count", report.scopes.len());
    push_bool_field(
        fields,
        "credential_references_only",
        report.credential_references_only,
    );
    push_bool_field(fields, "credentials_resolved", report.credentials_resolved);
    push_field(fields, "token_secret_ref", report.token_secret_ref);
    push_field(fields, "mtls_certificate_ref", report.mtls_certificate_ref);
    push_field(fields, "oidc_issuer_ref", report.oidc_issuer_ref);
    push_field(fields, "service_account_ref", report.service_account_ref);
    push_bool_field(
        fields,
        "raw_secret_values_present",
        report.raw_secret_values_present,
    );
    push_bool_field(fields, "secrets_redacted", report.secrets_redacted);
    push_field(fields, "redaction_policy", report.redaction_policy);
    push_bool_field(
        fields,
        "destructive_operation_requested",
        report.destructive_operation_requested,
    );
    push_bool_field(
        fields,
        "destructive_policy_required",
        report.destructive_policy_required,
    );
    push_bool_field(
        fields,
        "destructive_policy_present",
        report.destructive_policy_present,
    );
    push_bool_field(
        fields,
        "destructive_operations_allowed",
        report.destructive_operations_allowed,
    );
    push_bool_field(fields, "audit_required", report.audit_required);
    push_field(fields, "audit_policies", &report.audit_policy_summary());
    push_field(fields, "audit_evidence_ref", report.audit_evidence_ref);
}

fn append_security_governance_agent_evidence_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiSecurityGovernanceReport,
) {
    push_field(fields, "mcp_resources", &report.mcp_resource_summary());
    push_field(fields, "mcp_tools", &report.mcp_tool_summary());
    push_count_field(fields, "mcp_resource_count", report.mcp_resources.len());
    push_count_field(fields, "mcp_tool_count", report.mcp_tools.len());
    push_bool_field(fields, "mcp_dry_run_default", report.mcp_dry_run_default);
    push_bool_field(
        fields,
        "mcp_effectful_tools_allowed",
        report.mcp_effectful_tools_allowed,
    );
    push_bool_field(
        fields,
        "mcp_discovery_side_effect_free",
        report.mcp_discovery_side_effect_free,
    );
    push_field(
        fields,
        "evidence_model_signals",
        &report.evidence_signal_summary(),
    );
    push_count_field(fields, "evidence_signal_count", report.evidence_model.len());
    push_bool_field(
        fields,
        "opentelemetry_exporter_enabled",
        report.opentelemetry_exporter_enabled,
    );
    push_bool_field(
        fields,
        "runtime_collection_enabled",
        report.runtime_collection_enabled,
    );
    push_bool_field(
        fields,
        "openlineage_facets_mapped",
        report.openlineage_facets_mapped,
    );
    push_bool_field(
        fields,
        "problem_details_mapped",
        report.problem_details_mapped,
    );
    push_bool_field(fields, "cloudevents_mapped", report.cloudevents_mapped);
    push_bool_field(
        fields,
        "certificate_refs_mapped",
        report.certificate_refs_mapped,
    );
}

fn append_security_governance_problem_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiSecurityGovernanceReport,
) {
    push_bool_field(
        fields,
        "problem_details_emitted",
        report.problem_details_emitted(),
    );
    if let Some(problem) = &report.problem_details {
        push_field(fields, "problem_details_type", problem.problem_type);
        push_field(fields, "problem_details_title", problem.title);
        push_field(
            fields,
            "problem_details_status",
            &problem.http_status.to_string(),
        );
        push_field(fields, "problem_details_detail", problem.detail);
        push_field(
            fields,
            "problem_details_diagnostic_code",
            problem.diagnostic_code,
        );
        push_field(
            fields,
            "unsupported_reason",
            problem.unsupported_reason.unwrap_or("none"),
        );
    } else {
        push_field(fields, "problem_details_type", "none");
        push_field(fields, "problem_details_title", "none");
        push_field(fields, "problem_details_status", "none");
        push_field(fields, "problem_details_detail", "none");
        push_field(fields, "problem_details_diagnostic_code", "none");
        push_field(fields, "unsupported_reason", "none");
    }
}

fn append_security_governance_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiSecurityGovernanceReport,
) {
    push_bool_field(fields, "server_started", report.server_started);
    push_bool_field(
        fields,
        "network_listener_opened",
        report.network_listener_opened,
    );
    push_bool_field(fields, "network_probe", report.network_listener_opened);
    push_bool_field(fields, "dataset_probe", report.dataset_probe);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "catalog_probe", report.catalog_probe);
    push_bool_field(
        fields,
        "credential_resolution",
        report.credential_resolution,
    );
    push_bool_field(fields, "secret_resolution", report.secret_resolution);
    push_bool_field(fields, "raw_secret_emitted", report.raw_secret_emitted);
    push_bool_field(fields, "audit_write_io", report.audit_write_io);
    push_bool_field(fields, "mcp_tool_execution", report.mcp_tool_execution);
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "data_materialized", report.data_materialized);
    push_bool_field(fields, "query_execution", report.query_execution);
    push_bool_field(fields, "runtime_execution", report.runtime_execution);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(
        fields,
        "external_engine_invoked",
        report.external_engine_invoked,
    );
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(fields, "execution_delegated", report.execution_delegated);
    push_bool_field(
        fields,
        "effect_policy_violated",
        report.effect_policy_violated(),
    );
    append_rest_no_effect_parity_fields(
        fields,
        report.runtime_execution,
        report.data_read,
        report.write_io,
        report.fallback_attempted,
        report.external_engine_invoked,
        report.effect_policy_violated(),
    );
}

fn rest_api_data_plane_fields(report: &RestApiDataPlaneReport) -> Vec<(String, String)> {
    let mut fields = vec![];
    append_data_plane_identity_fields(&mut fields, report);
    append_data_plane_transfer_fields(&mut fields, report);
    append_data_plane_standards_fields(&mut fields, report);
    append_data_plane_effect_fields(&mut fields, report);
    push_count_field(&mut fields, "diagnostic_count", report.diagnostics.len());
    fields
}

fn append_data_plane_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiDataPlaneReport,
) {
    push_field(fields, "mode", "rest_api_data_plane");
    push_field(fields, "schema_version", report.schema_version);
    push_field(fields, "report_id", report.report_id);
    push_field(fields, "api_version", report.api_version);
    push_field(fields, "scenario", report.scenario.as_str());
    push_field(
        fields,
        "data_plane_status",
        report.data_plane_status.as_str(),
    );
    push_field(fields, "endpoint_paths", &report.endpoint_paths.join(","));
    push_field(
        fields,
        "data_plane_operations",
        &report.data_plane_operations.join(","),
    );
    push_field(
        fields,
        "openapi_contract_path",
        report.openapi_contract_path,
    );
    push_bool_field(
        fields,
        "rest_control_plane_required",
        report.rest_control_plane_required,
    );
    push_bool_field(
        fields,
        "rest_control_plane_sufficient_for_local_use",
        report.rest_control_plane_sufficient_for_local_use,
    );
    push_bool_field(
        fields,
        "flight_adbc_required_for_basic_local_use",
        report.flight_adbc_required_for_basic_local_use,
    );
}

fn append_data_plane_transfer_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiDataPlaneReport,
) {
    push_field(fields, "transfer_modes", &report.transfer_mode_summary());
    push_count_field(
        fields,
        "transfer_mode_count",
        report.transfer_contracts.len(),
    );
    push_field(
        fields,
        "optional_transports",
        &report.optional_transport_summary(),
    );
    append_data_plane_optional_transport_fields(fields, report);
    append_data_plane_payload_policy_fields(fields, report);
    push_field(
        fields,
        "no_fallback_evidence_artifact_ref",
        report.no_fallback_evidence_artifact_ref,
    );
    push_field(
        fields,
        "security_governance_policy_ref",
        report.security_governance_policy_ref,
    );
}

fn append_data_plane_optional_transport_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiDataPlaneReport,
) {
    push_bool_field(
        fields,
        "flight_ticket_requested",
        report.flight_ticket_requested,
    );
    push_bool_field(
        fields,
        "flight_ticket_supported",
        report.flight_ticket_supported,
    );
    push_bool_field(
        fields,
        "adbc_endpoint_requested",
        report.adbc_endpoint_requested,
    );
    push_bool_field(
        fields,
        "adbc_endpoint_supported",
        report.adbc_endpoint_supported,
    );
    push_bool_field(
        fields,
        "optional_transport_required",
        report.optional_transport_required,
    );
}

fn append_data_plane_payload_policy_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiDataPlaneReport,
) {
    push_field(
        fields,
        "large_payload_threshold_bytes",
        &report.large_payload_threshold_bytes.to_string(),
    );
    push_field(
        fields,
        "preferred_large_payload_modes",
        &report.preferred_large_payload_modes.join(","),
    );
    push_field(
        fields,
        "inline_json_max_bytes",
        &report.inline_json_max_bytes.to_string(),
    );
    push_bool_field(fields, "paged_json_available", report.paged_json_available);
    push_bool_field(
        fields,
        "jsonl_ndjson_available",
        report.jsonl_ndjson_available,
    );
    push_bool_field(
        fields,
        "vortex_artifact_available",
        report.vortex_artifact_available,
    );
    push_bool_field(
        fields,
        "object_reference_available",
        report.object_reference_available,
    );
    push_bool_field(
        fields,
        "arrow_ipc_decoded_boundary_available",
        report.arrow_ipc_decoded_boundary_available,
    );
    push_bool_field(
        fields,
        "arrow_ipc_certified_native",
        report.arrow_ipc_certified_native,
    );
    push_bool_field(
        fields,
        "decoded_columnar_boundary_declared",
        report.decoded_columnar_boundary_declared,
    );
    push_bool_field(
        fields,
        "materialization_declared",
        report.materialization_declared,
    );
    push_bool_field(fields, "fidelity_declared", report.fidelity_declared);
    push_bool_field(
        fields,
        "result_policy_declared",
        report.result_policy_declared,
    );
}

fn append_data_plane_standards_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiDataPlaneReport,
) {
    push_bool_field(
        fields,
        "standards_matrix_requested",
        report.standards_matrix_requested,
    );
    push_count_field(
        fields,
        "standards_matrix_count",
        report.standards_matrix_count,
    );
    push_field(fields, "standards", &report.standards_summary());
    push_field(fields, "standards_names", &report.standards_name_summary());
}

fn append_data_plane_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &RestApiDataPlaneReport,
) {
    push_bool_field(fields, "server_started", report.server_started);
    push_bool_field(
        fields,
        "network_listener_opened",
        report.network_listener_opened,
    );
    push_bool_field(fields, "network_probe", report.network_listener_opened);
    push_bool_field(
        fields,
        "flight_server_started",
        report.flight_server_started,
    );
    push_bool_field(fields, "adbc_endpoint_opened", report.adbc_endpoint_opened);
    push_bool_field(fields, "broker_io", report.broker_io);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "catalog_probe", report.catalog_probe);
    push_bool_field(fields, "dataset_probe", report.dataset_probe);
    push_bool_field(
        fields,
        "credential_resolution",
        report.credential_resolution,
    );
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "data_materialized", report.data_materialized);
    push_bool_field(fields, "query_execution", report.query_execution);
    push_bool_field(fields, "runtime_execution", report.runtime_execution);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(
        fields,
        "external_engine_invoked",
        report.external_engine_invoked,
    );
    push_bool_field(
        fields,
        "fallback_execution_allowed",
        report.fallback_execution_allowed,
    );
    push_bool_field(fields, "fallback_attempted", report.fallback_attempted);
    push_bool_field(fields, "execution_delegated", report.execution_delegated);
    push_bool_field(
        fields,
        "effect_policy_violated",
        report.effect_policy_violated(),
    );
    append_rest_no_effect_parity_fields(
        fields,
        report.runtime_execution,
        report.data_read,
        report.write_io,
        report.fallback_attempted,
        report.external_engine_invoked,
        report.effect_policy_violated(),
    );
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

fn set_field(fields: &mut Vec<(String, String)>, key: &str, value: &str) {
    if let Some((_, existing_value)) = fields.iter_mut().find(|(name, _)| name == key) {
        *existing_value = value.to_string();
    } else {
        push_field(fields, key, value);
    }
}

fn push_count_field(fields: &mut Vec<(String, String)>, key: &str, value: usize) {
    push_field(fields, key, &value.to_string());
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: &str, value: bool) {
    push_field(fields, key, &value.to_string());
}

#[allow(clippy::fn_params_excessive_bools)]
fn append_rest_no_effect_parity_fields(
    fields: &mut Vec<(String, String)>,
    runtime_execution: bool,
    data_read: bool,
    write_io: bool,
    fallback_attempted: bool,
    external_engine_invoked: bool,
    effect_policy_violated: bool,
) {
    push_bool_field(fields, "external_effects_executed", effect_policy_violated);
    push_bool_field(fields, "no_runtime", !runtime_execution);
    push_bool_field(
        fields,
        "no_fallback",
        !fallback_attempted && !external_engine_invoked,
    );
    push_bool_field(
        fields,
        "no_effects",
        !data_read && !write_io && !effect_policy_violated,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rest_api_discovery_fields_keep_discovery_identity_canonical() {
        let report = RestApiDiscoveryModeReport::contract_only("127.0.0.1:8787");
        let fields = rest_api_discovery_fields(&report);

        assert_eq!(
            field_value(&fields, "schema_version"),
            report.schema_version
        );
        assert_eq!(field_value(&fields, "report_id"), report.report_id);
        assert_eq!(
            field_value(&fields, "contract_schema_version"),
            report.contract_report.schema_version
        );
        assert_eq!(
            field_value(&fields, "contract_report_id"),
            report.contract_report.report_id
        );
        assert_eq!(
            field_value(&fields, "discovery_schema_version"),
            report.schema_version
        );
        assert_eq!(
            field_value(&fields, "discovery_report_id"),
            report.report_id
        );
    }

    fn field_value<'a>(fields: &'a [(String, String)], key: &str) -> &'a str {
        fields.iter().find(|(name, _)| name == key).map_or_else(
            || panic!("missing output field {key}"),
            |(_, value)| value.as_str(),
        )
    }
}
