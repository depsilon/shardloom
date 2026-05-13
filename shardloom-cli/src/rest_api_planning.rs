//! REST/API planning CLI handlers.
//!
//! This module owns the current report-only API protocol planning command. It
//! does not start a server, open sockets, or authorize remote execution.

use std::process::ExitCode;

use shardloom_core::{
    CliApiJsonProtocolReport, OutputFormat, ReleasePlan, RestApiContractReport,
    RestApiDiscoveryModeReport, RestApiLocalLifecycleReport, RestApiLocalLifecycleScenario,
    RestApiPlanPreviewReport, RestApiPlanPreviewScenario, ShardLoomError,
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
