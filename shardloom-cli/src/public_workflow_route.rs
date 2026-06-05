//! Public workflow route facade.
//!
//! This module is the side-effect-free narrow waist for user-facing route
//! inspection. It resolves a declared SQL/Python/DataFrame/CLI workflow into a
//! ShardLoom-internal command route or a deterministic blocker before any
//! runtime command is allowed to execute.

use std::process::ExitCode;

use shardloom_core::{
    CommandStatus, Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity,
    FallbackStatus, OutputFormat, ShardLoomError,
};

use crate::{
    cli_output::{emit, emit_error},
    cli_unknown_arg_error, generated_source_runtime, sql_local_source_runtime,
};

const ROUTE_SCHEMA_VERSION: &str = "shardloom.public_workflow_route.v1";
const FACADE_SCHEMA_VERSION: &str = "shardloom.public_workflow_execution_facade.v1";
const ROUTE_REPORT_ID: &str = "gar-runtime-impl-6d.public_workflow_route_facade";
const ROUTE_DOCS_REF: &str = "docs/status/cli-command-registry.md#public-route-facade-command";
const FALLBACK_BOUNDARY: &str =
    "route inspection is side-effect-free and never invokes fallback or external engines";
const CLAIM_BOUNDARY: &str = "simplified public facade over admitted ShardLoom routes only; not broad SQL/DataFrame support, production readiness, or performance superiority";

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicWorkflowRouteRequest {
    surface: String,
    input_uri: Option<String>,
    input_format: Option<String>,
    sql_statement: Option<String>,
    plan_summary: Option<String>,
    requested_output: String,
    output_ref: Option<String>,
    execution_policy: String,
    materialization_policy: String,
    evidence_level: String,
    bounded: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicWorkflowRoutePlan {
    status: CommandStatus,
    route_status: &'static str,
    route_id: &'static str,
    resolved_internal_command: &'static str,
    start_state: &'static str,
    vortex_normalization_point: &'static str,
    execution_mode: &'static str,
    preparation_included: bool,
    query_timing_starts_after_preparation: bool,
    blocker_id: &'static str,
    blocker_reason: &'static str,
    diagnostics: Vec<Diagnostic>,
}

pub(crate) fn handle_public_workflow_route(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let request = match PublicWorkflowRouteRequest::parse(args) {
        Ok(request) => request,
        Err(error) => {
            return emit_error("route", format, "public workflow route failed", &error);
        }
    };
    let plan = plan_public_workflow_route(&request);
    let human_text = route_human_text(&request, &plan);
    emit(
        "route",
        format,
        plan.status,
        "public workflow route".to_string(),
        human_text,
        plan.diagnostics.clone(),
        route_fields(&request, &plan),
    );
    ExitCode::SUCCESS
}

pub(crate) fn handle_public_workflow_run(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let request = match PublicWorkflowRouteRequest::parse(args) {
        Ok(request) => request,
        Err(error) => {
            return emit_error("run", format, "public workflow run failed", &error);
        }
    };
    let plan = plan_public_workflow_route(&request);
    if plan.status != CommandStatus::Success {
        return emit_blocked_facade("run", format, &request, &plan);
    }

    match plan.route_id {
        "local_file_direct_query" | "local_file_direct_sink" => {
            let Some(statement) = request.sql_statement.clone() else {
                let blocked = executable_sql_required_route("run");
                return emit_blocked_facade("run", format, &request, &blocked);
            };
            let runtime_args = match sql_local_source_runtime_args(&request, statement) {
                Ok(args) => args,
                Err(error) => {
                    return emit_error("run", format, "public workflow run failed", &error);
                }
            };
            sql_local_source_runtime::handle_sql_local_source_smoke_with_facade(
                runtime_args.into_iter(),
                format,
                "run",
                execution_attachment_fields("run", &request, &plan),
            )
        }
        "source_free_generated_output" => {
            let Some(statement) = request.sql_statement.clone() else {
                let blocked = executable_sql_required_route("run");
                return emit_blocked_facade("run", format, &request, &blocked);
            };
            let Some(output_ref) = request.output_ref.clone() else {
                let blocked = output_required_route("run", "source-free SQL run");
                return emit_blocked_facade("run", format, &request, &blocked);
            };
            let runtime_args = generated_source_runtime_args(&request, output_ref, statement);
            generated_source_runtime::handle_generated_source_sql_smoke_with_facade(
                runtime_args.into_iter(),
                format,
                "run",
                execution_attachment_fields("run", &request, &plan),
            )
        }
        _ => {
            let blocked = run_route_not_executable_yet(&plan);
            emit_blocked_facade("run", format, &request, &blocked)
        }
    }
}

pub(crate) fn handle_public_workflow_prepare(
    args: impl Iterator<Item = String>,
    format: OutputFormat,
) -> ExitCode {
    let mut request = match PublicWorkflowRouteRequest::parse(args) {
        Ok(request) => request,
        Err(error) => {
            return emit_error("prepare", format, "public workflow prepare failed", &error);
        }
    };
    request.requested_output = "prepare".to_string();
    request.execution_policy = "prepare_once".to_string();
    request.bounded = true;

    let plan = plan_public_workflow_route(&request);
    if plan.status != CommandStatus::Success {
        return emit_blocked_facade("prepare", format, &request, &plan);
    }
    let Some(input_uri) = request.input_uri.clone() else {
        let blocked = input_not_declared_route();
        return emit_blocked_facade("prepare", format, &request, &blocked);
    };
    let Some(output_ref) = request.output_ref.clone() else {
        let blocked = output_required_route("prepare", "prepared Vortex target");
        return emit_blocked_facade("prepare", format, &request, &blocked);
    };
    if request.input_format.as_deref() == Some("vortex") {
        let blocked = already_native_vortex_prepare_route();
        return emit_blocked_facade("prepare", format, &request, &blocked);
    }

    let extra_fields = execution_attachment_fields("prepare", &request, &plan);
    sql_local_source_runtime::handle_vortex_ingest_smoke_with_facade(
        [input_uri, output_ref].into_iter(),
        format,
        "prepare",
        &extra_fields,
    )
}

impl PublicWorkflowRouteRequest {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, ShardLoomError> {
        let mut args = args.peekable();
        let Some(surface) = args.next() else {
            return Err(ShardLoomError::InvalidOperation(
                "usage: shardloom route <sql|python|dataframe|cli> [--input <uri>] [--input-format <format>] [--sql <statement>] [--plan <summary>] [--request <collect|prepare|write_vortex|write_parquet|write_csv|write_jsonl|explain|route|evidence>] [--output <ref>] [--execution-policy <auto|direct|native_vortex|prepare_once>] [--materialization-policy <bounded|materialized|zero_decode|explicit>] [--evidence-level <report_only|runtime_smoke|claim_grade>] [--bounded true|false]"
                    .to_string(),
            ));
        };
        let surface = normalize_surface(&surface)?;
        let mut request = Self {
            surface,
            input_uri: None,
            input_format: None,
            sql_statement: None,
            plan_summary: None,
            requested_output: "collect".to_string(),
            output_ref: None,
            execution_policy: "auto".to_string(),
            materialization_policy: "bounded".to_string(),
            evidence_level: "runtime_smoke".to_string(),
            bounded: false,
        };

        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--input" => request.input_uri = Some(required_value(&mut args, "--input")?),
                "--input-format" => {
                    request.input_format = Some(normalize_input_format(&required_value(
                        &mut args,
                        "--input-format",
                    )?)?);
                }
                "--sql" => request.sql_statement = Some(required_value(&mut args, "--sql")?),
                "--plan" => request.plan_summary = Some(required_value(&mut args, "--plan")?),
                "--request" | "--requested-output" => {
                    request.requested_output =
                        normalize_requested_output(&required_value(&mut args, &flag)?)?;
                }
                "--output" => request.output_ref = Some(required_value(&mut args, "--output")?),
                "--execution-policy" => {
                    request.execution_policy = normalize_execution_policy(&required_value(
                        &mut args,
                        "--execution-policy",
                    )?)?;
                }
                "--materialization-policy" | "--decode-policy" => {
                    request.materialization_policy =
                        normalize_materialization_policy(&required_value(&mut args, &flag)?)?;
                }
                "--evidence-level" => {
                    request.evidence_level =
                        normalize_evidence_level(&required_value(&mut args, "--evidence-level")?)?;
                }
                "--bounded" => {
                    request.bounded =
                        parse_bool_flag("--bounded", &required_value(&mut args, "--bounded")?)?;
                }
                extra => {
                    return Err(cli_unknown_arg_error("route", extra));
                }
            }
        }

        if request.sql_statement.is_none() && request.plan_summary.is_none() {
            request.plan_summary = Some(format!("{} workflow", request.surface));
        }
        if request.input_uri.is_none() {
            request.input_uri = request
                .sql_statement
                .as_deref()
                .and_then(extract_first_quoted_source_ref);
        }
        if request.input_format.is_none() {
            request.input_format = request
                .input_uri
                .as_deref()
                .and_then(infer_input_format_from_ref)
                .map(str::to_string);
        }
        if !request.bounded {
            request.bounded = request
                .sql_statement
                .as_deref()
                .is_some_and(sql_statement_has_limit)
                || request
                    .plan_summary
                    .as_deref()
                    .is_some_and(plan_summary_has_limit)
                || !matches!(request.requested_output.as_str(), "collect");
        }
        Ok(request)
    }
}

fn plan_public_workflow_route(request: &PublicWorkflowRouteRequest) -> PublicWorkflowRoutePlan {
    if matches!(request.requested_output.as_str(), "collect") && !request.bounded {
        return unbounded_collect_blocked_route();
    }

    if is_native_vortex_route(request) {
        return native_vortex_route();
    }

    match request.input_format.as_deref() {
        Some(format) if is_local_file_format(format) => local_file_route(request),
        None if is_source_free_sql_write_request(request) => source_free_generated_output_route(),
        Some(other) => input_format_not_admitted_route(other),
        None => input_not_declared_route(),
    }
}

fn unbounded_collect_blocked_route() -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.unbounded_collect_blocked",
        "bounded collect requires an explicit LIMIT, bounded=true, or a write/materialized output request",
        Diagnostic::materialization_required(
            "public_workflow_route.collect",
            "unbounded public collect is not admitted through the facade",
            "add LIMIT, pass bounded=true for a proven bounded request, or use an explicit write route",
        ),
    )
}

fn is_native_vortex_route(request: &PublicWorkflowRouteRequest) -> bool {
    request.input_format.as_deref() == Some("vortex") || request.execution_policy == "native_vortex"
}

fn native_vortex_route() -> PublicWorkflowRoutePlan {
    admitted_route(
        "native_vortex_direct_query",
        "vortex-run",
        "native_vortex_file",
        "native_vortex_boundary",
        "native_vortex",
        false,
        true,
    )
}

fn is_local_file_format(format: &str) -> bool {
    matches!(
        format,
        "csv" | "json" | "jsonl" | "ndjson" | "parquet" | "arrow-ipc" | "avro" | "orc"
    )
}

fn local_file_route(request: &PublicWorkflowRouteRequest) -> PublicWorkflowRoutePlan {
    if request.requested_output == "prepare" {
        admitted_route(
            "local_file_prepare_once",
            "vortex-ingest-smoke",
            "compatibility_local_source",
            "VortexPreparedState",
            "prepared_vortex",
            true,
            true,
        )
    } else if request.execution_policy == "prepare_once" {
        admitted_route(
            "local_file_prepare_once_first_query",
            "vortex-ingest-smoke->traditional-analytics-vortex-run",
            "compatibility_local_source",
            "VortexPreparedState",
            "prepared_vortex",
            true,
            true,
        )
    } else if is_write_request(request) {
        admitted_route(
            "local_file_direct_sink",
            "sql-local-source-smoke",
            "compatibility_local_source",
            "direct_transient",
            "direct",
            false,
            false,
        )
    } else {
        admitted_route(
            "local_file_direct_query",
            "sql-local-source-smoke",
            "compatibility_local_source",
            "direct_transient",
            "direct",
            false,
            false,
        )
    }
}

fn is_write_request(request: &PublicWorkflowRouteRequest) -> bool {
    matches!(
        request.requested_output.as_str(),
        "write_vortex" | "write_parquet" | "write_csv" | "write_jsonl"
    )
}

fn is_source_free_sql_write_request(request: &PublicWorkflowRouteRequest) -> bool {
    request
        .sql_statement
        .as_deref()
        .is_some_and(is_source_free_sql_statement)
        && is_write_request(request)
}

fn source_free_generated_output_route() -> PublicWorkflowRoutePlan {
    admitted_route(
        "source_free_generated_output",
        "generated-source-sql-smoke",
        "source_free_sql_statement",
        "generated_rows_boundary",
        "source_free_generated_output",
        false,
        false,
    )
}

fn input_format_not_admitted_route(format: &str) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.input_format_not_admitted",
        "input format is not admitted by the public route facade",
        Diagnostic::unsupported(
            DiagnosticCode::UnsupportedEncoding,
            "public_workflow_route.input_format",
            format!("input format {format:?} is not admitted by the public route facade"),
            Some("use csv, jsonl, parquet, arrow-ipc, avro, orc, or vortex".to_string()),
        ),
    )
}

fn input_not_declared_route() -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.input_not_declared",
        "route requires a declared input, inferred SQL source, or source-free SQL write request",
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            "public workflow route requires a declared input boundary",
            Some("public_workflow_route.input".to_string()),
            Some("no input URI or inferable SQL source was provided".to_string()),
            Some(
                "pass --input with --input-format or route a source-free SQL write request"
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn admitted_route(
    route_id: &'static str,
    resolved_internal_command: &'static str,
    start_state: &'static str,
    vortex_normalization_point: &'static str,
    execution_mode: &'static str,
    preparation_included: bool,
    query_timing_starts_after_preparation: bool,
) -> PublicWorkflowRoutePlan {
    PublicWorkflowRoutePlan {
        status: CommandStatus::Success,
        route_status: "admitted",
        route_id,
        resolved_internal_command,
        start_state,
        vortex_normalization_point,
        execution_mode,
        preparation_included,
        query_timing_starts_after_preparation,
        blocker_id: "none",
        blocker_reason: "none",
        diagnostics: Vec::new(),
    }
}

fn blocked_route(
    blocker_id: &'static str,
    blocker_reason: &'static str,
    diagnostic: Diagnostic,
) -> PublicWorkflowRoutePlan {
    PublicWorkflowRoutePlan {
        status: CommandStatus::Unsupported,
        route_status: "blocked",
        route_id: "blocked",
        resolved_internal_command: "not_resolved",
        start_state: "blocked",
        vortex_normalization_point: "not_applicable",
        execution_mode: "blocked",
        preparation_included: false,
        query_timing_starts_after_preparation: false,
        blocker_id,
        blocker_reason,
        diagnostics: vec![diagnostic],
    }
}

fn route_fields(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> Vec<(String, String)> {
    let mut fields = Vec::with_capacity(40);
    add_route_identity_fields(&mut fields, request, plan);
    add_route_request_fields(&mut fields, request);
    add_route_execution_fields(&mut fields, plan);
    add_route_boundary_fields(&mut fields, plan);
    fields
}

fn add_route_identity_fields(
    fields: &mut Vec<(String, String)>,
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) {
    push_field(
        fields,
        "public_workflow_route_schema_version",
        ROUTE_SCHEMA_VERSION,
    );
    push_field(fields, "public_workflow_route_report_id", ROUTE_REPORT_ID);
    push_field(fields, "public_workflow_route_docs_ref", ROUTE_DOCS_REF);
    push_field(fields, "route_id", plan.route_id);
    push_field(fields, "route_status", plan.route_status);
    push_field(
        fields,
        "resolved_internal_command",
        plan.resolved_internal_command,
    );
    push_field(fields, "surface", request.surface.clone());
}

fn add_route_request_fields(
    fields: &mut Vec<(String, String)>,
    request: &PublicWorkflowRouteRequest,
) {
    push_field(
        fields,
        "declared_inputs",
        optional_or_none(request.input_uri.as_ref()),
    );
    push_field(
        fields,
        "primary_input",
        optional_or_none(request.input_uri.as_ref()),
    );
    push_field(
        fields,
        "source_format",
        request
            .input_format
            .clone()
            .unwrap_or_else(|| "not_declared".to_string()),
    );
    push_field(
        fields,
        "sql_statement_present",
        request.sql_statement.is_some().to_string(),
    );
    push_field(
        fields,
        "sql_statement",
        optional_or_none(request.sql_statement.as_ref()),
    );
    push_field(
        fields,
        "plan_summary",
        optional_or_none(request.plan_summary.as_ref()),
    );
    push_field(fields, "requested_output", request.requested_output.clone());
    push_field(
        fields,
        "output_ref",
        optional_or_none(request.output_ref.as_ref()),
    );
    push_field(fields, "execution_policy", request.execution_policy.clone());
    push_field(
        fields,
        "materialization_decode_policy",
        request.materialization_policy.clone(),
    );
    push_field(fields, "evidence_level", request.evidence_level.clone());
    push_field(fields, "bounded_request", request.bounded.to_string());
}

fn add_route_execution_fields(fields: &mut Vec<(String, String)>, plan: &PublicWorkflowRoutePlan) {
    push_field(fields, "start_state", plan.start_state);
    push_field(
        fields,
        "vortex_normalization_point",
        plan.vortex_normalization_point,
    );
    push_field(fields, "execution_mode", plan.execution_mode);
    push_field(
        fields,
        "preparation_included",
        plan.preparation_included.to_string(),
    );
    push_field(
        fields,
        "query_timing_starts_after_preparation",
        plan.query_timing_starts_after_preparation.to_string(),
    );
    for (key, value) in [
        ("fallback_attempted", "false"),
        ("external_engine_invoked", "false"),
        ("runtime_execution", "false"),
        ("route_side_effect_free", "true"),
        ("side_effect_free", "true"),
        ("source_io_performed", "false"),
        ("output_io_performed", "false"),
        ("execution", "not_performed"),
        ("plan_only", "true"),
    ] {
        push_field(fields, key, value);
    }
}

fn add_route_boundary_fields(fields: &mut Vec<(String, String)>, plan: &PublicWorkflowRoutePlan) {
    push_field(fields, "blocker_id", plan.blocker_id);
    push_field(fields, "blocker_reason", plan.blocker_reason);
    push_field(fields, "claim_boundary", CLAIM_BOUNDARY);
    push_field(fields, "fallback_boundary", FALLBACK_BOUNDARY);
    push_field(fields, "claim_gate_status", "route_inspection_only");
}

fn push_field(fields: &mut Vec<(String, String)>, key: &str, value: impl Into<String>) {
    fields.push((key.to_string(), value.into()));
}

fn optional_or_none(value: Option<&String>) -> String {
    value.cloned().unwrap_or_else(|| "none".to_string())
}

fn route_human_text(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> String {
    format!(
        "public workflow route\nsurface: {}\nroute_id: {}\nresolved_internal_command: {}\nstatus: {}\nexecution: not_performed\nfallback_attempted: false\nexternal_engine_invoked: false",
        request.surface, plan.route_id, plan.resolved_internal_command, plan.route_status
    )
}

fn emit_blocked_facade(
    command: &'static str,
    format: OutputFormat,
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> ExitCode {
    emit(
        command,
        format,
        CommandStatus::Unsupported,
        format!("public workflow {command} blocked before execution"),
        facade_human_text(command, request, plan, false),
        plan.diagnostics.clone(),
        blocked_facade_fields(command, request, plan),
    );
    ExitCode::from(1)
}

fn facade_human_text(
    command: &str,
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    runtime_execution: bool,
) -> String {
    format!(
        "public workflow {command}\nsurface: {}\nroute_id: {}\nresolved_internal_command: {}\nstatus: {}\nruntime_execution: {}\nfallback_attempted: false\nexternal_engine_invoked: false",
        request.surface,
        plan.route_id,
        plan.resolved_internal_command,
        plan.route_status,
        runtime_execution
    )
}

fn blocked_facade_fields(
    command: &str,
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> Vec<(String, String)> {
    let mut fields = execution_attachment_fields(command, request, plan);
    fields.extend([
        ("runtime_execution".to_string(), "false".to_string()),
        ("source_io_performed".to_string(), "false".to_string()),
        ("output_io_performed".to_string(), "false".to_string()),
        (
            "execution".to_string(),
            "blocked_before_execution".to_string(),
        ),
        ("fallback_attempted".to_string(), "false".to_string()),
        ("external_engine_invoked".to_string(), "false".to_string()),
        (
            "claim_gate_status".to_string(),
            "not_claim_grade".to_string(),
        ),
    ]);
    fields
}

#[allow(clippy::too_many_lines)]
fn execution_attachment_fields(
    command: &str,
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> Vec<(String, String)> {
    vec![
        (
            "public_workflow_facade_schema_version".to_string(),
            FACADE_SCHEMA_VERSION.to_string(),
        ),
        (
            "public_workflow_route_schema_version".to_string(),
            ROUTE_SCHEMA_VERSION.to_string(),
        ),
        (
            "public_workflow_route_report_id".to_string(),
            ROUTE_REPORT_ID.to_string(),
        ),
        (
            "public_workflow_route_docs_ref".to_string(),
            ROUTE_DOCS_REF.to_string(),
        ),
        (
            "public_workflow_facade_command".to_string(),
            command.to_string(),
        ),
        (
            "public_workflow_route_attached".to_string(),
            "true".to_string(),
        ),
        (
            "public_workflow_surface".to_string(),
            request.surface.clone(),
        ),
        (
            "public_workflow_route_id".to_string(),
            plan.route_id.to_string(),
        ),
        (
            "public_workflow_route_status".to_string(),
            plan.route_status.to_string(),
        ),
        (
            "public_workflow_resolved_internal_command".to_string(),
            plan.resolved_internal_command.to_string(),
        ),
        (
            "public_workflow_start_state".to_string(),
            plan.start_state.to_string(),
        ),
        (
            "public_workflow_vortex_normalization_point".to_string(),
            plan.vortex_normalization_point.to_string(),
        ),
        (
            "public_workflow_execution_mode".to_string(),
            plan.execution_mode.to_string(),
        ),
        (
            "public_workflow_preparation_included".to_string(),
            plan.preparation_included.to_string(),
        ),
        (
            "public_workflow_query_timing_starts_after_preparation".to_string(),
            plan.query_timing_starts_after_preparation.to_string(),
        ),
        (
            "public_workflow_requested_output".to_string(),
            request.requested_output.clone(),
        ),
        (
            "public_workflow_output_ref".to_string(),
            optional_or_none(request.output_ref.as_ref()),
        ),
        (
            "public_workflow_evidence_level".to_string(),
            request.evidence_level.clone(),
        ),
        (
            "public_workflow_bounded_request".to_string(),
            request.bounded.to_string(),
        ),
        (
            "public_workflow_blocker_id".to_string(),
            plan.blocker_id.to_string(),
        ),
        (
            "public_workflow_blocker_reason".to_string(),
            plan.blocker_reason.to_string(),
        ),
        (
            "public_workflow_fallback_attempted".to_string(),
            "false".to_string(),
        ),
        (
            "public_workflow_external_engine_invoked".to_string(),
            "false".to_string(),
        ),
        (
            "public_workflow_claim_boundary".to_string(),
            CLAIM_BOUNDARY.to_string(),
        ),
        (
            "public_workflow_fallback_boundary".to_string(),
            FALLBACK_BOUNDARY.to_string(),
        ),
    ]
}

fn sql_local_source_runtime_args(
    request: &PublicWorkflowRouteRequest,
    statement: String,
) -> Result<Vec<String>, ShardLoomError> {
    let mut args = vec![statement];
    if let Some(output_ref) = request.output_ref.as_ref() {
        args.extend([
            "--output".to_string(),
            output_ref.clone(),
            "--output-format".to_string(),
            local_output_format_for_request(request)?.to_string(),
        ]);
    }
    Ok(args)
}

fn generated_source_runtime_args(
    request: &PublicWorkflowRouteRequest,
    output_ref: String,
    statement: String,
) -> Vec<String> {
    vec![
        output_ref,
        statement,
        "--output-format".to_string(),
        local_output_format_for_request(request)
            .unwrap_or("jsonl")
            .to_string(),
    ]
}

fn local_output_format_for_request(
    request: &PublicWorkflowRouteRequest,
) -> Result<&'static str, ShardLoomError> {
    match request.requested_output.as_str() {
        "collect" => Ok("inline-jsonl"),
        "write_vortex" => Ok("vortex"),
        "write_parquet" => Ok("parquet"),
        "write_csv" => Ok("csv"),
        "write_jsonl" => Ok("jsonl"),
        other => Err(ShardLoomError::InvalidOperation(format!(
            "public workflow output {other:?} is not executable by this facade"
        ))),
    }
}

fn executable_sql_required_route(command: &'static str) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.run.executable_sql_required",
        "public workflow execution requires an executable SQL statement for this facade slice",
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            format!("public workflow {command} requires executable SQL"),
            Some(format!("public_workflow_{command}.sql_statement")),
            Some("route metadata alone is not executable".to_string()),
            Some(
                "pass --sql or use the Python/DataFrame facade that renders admitted SQL"
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn output_required_route(
    command: &'static str,
    target_name: &'static str,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.facade.output_required",
        "public workflow facade requires an explicit output target for this operation",
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            format!("public workflow {command} requires an explicit {target_name}"),
            Some(format!("public_workflow_{command}.output")),
            Some("no output target was provided".to_string()),
            Some("pass --output with a caller-owned local target".to_string()),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn already_native_vortex_prepare_route() -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.prepare.already_native_vortex",
        "native Vortex input is already prepared and should use a native run route",
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            "public workflow prepare received native Vortex input".to_string(),
            Some("public_workflow_prepare.input_format".to_string()),
            Some("input_format=vortex does not need compatibility preparation".to_string()),
            Some("use route/run with --execution-policy native_vortex when an operator facade exists".to_string()),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn run_route_not_executable_yet(plan: &PublicWorkflowRoutePlan) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.run.route_not_executable_yet",
        "this admitted route still requires a dedicated execution wrapper before public run can execute it",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "public workflow run route is not executable by this facade slice".to_string(),
            Some("public_workflow_run.route_id".to_string()),
            Some(format!(
                "route_id={} resolved_internal_command={}",
                plan.route_id, plan.resolved_internal_command
            )),
            Some(
                "use the lower-level explicit runtime command until this wrapper is promoted"
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn required_value(
    args: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    flag: &str,
) -> Result<String, ShardLoomError> {
    let Some(value) = args.next() else {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{flag} requires a value"
        )));
    };
    if value.starts_with("--") {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{flag} requires a value"
        )));
    }
    Ok(value)
}

fn normalize_surface(value: &str) -> Result<String, ShardLoomError> {
    let normalized = value.trim().to_ascii_lowercase().replace('_', "-");
    match normalized.as_str() {
        "sql" | "python" | "dataframe" | "cli" => Ok(normalized),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "unsupported public workflow surface: {value}"
        ))),
    }
}

fn normalize_input_format(value: &str) -> Result<String, ShardLoomError> {
    let normalized = value.trim().to_ascii_lowercase().replace('_', "-");
    match normalized.as_str() {
        "csv" | "json" | "jsonl" | "ndjson" | "parquet" | "arrow-ipc" | "avro" | "orc"
        | "vortex" => Ok(normalized),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "unsupported route input format: {value}"
        ))),
    }
}

fn normalize_requested_output(value: &str) -> Result<String, ShardLoomError> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "collect" | "prepare" | "write_vortex" | "write_parquet" | "write_csv" | "write_jsonl"
        | "explain" | "route" | "evidence" | "profile" => Ok(normalized),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "unsupported route requested output: {value}"
        ))),
    }
}

fn normalize_execution_policy(value: &str) -> Result<String, ShardLoomError> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "auto" | "direct" | "native_vortex" | "prepare_once" => Ok(normalized),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "unsupported route execution policy: {value}"
        ))),
    }
}

fn normalize_materialization_policy(value: &str) -> Result<String, ShardLoomError> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "bounded" | "materialized" | "zero_decode" | "explicit" => Ok(normalized),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "unsupported route materialization/decode policy: {value}"
        ))),
    }
}

fn normalize_evidence_level(value: &str) -> Result<String, ShardLoomError> {
    let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "report_only" | "runtime_smoke" | "claim_grade" => Ok(normalized),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "unsupported route evidence level: {value}"
        ))),
    }
}

fn parse_bool_flag(flag: &str, value: &str) -> Result<bool, ShardLoomError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "{flag} expects true or false"
        ))),
    }
}

fn infer_input_format_from_ref(value: &str) -> Option<&'static str> {
    let trimmed = value.trim();
    let source_ref = trimmed.split(['?', '#']).next().unwrap_or(trimmed);
    let extension = std::path::Path::new(source_ref)
        .extension()
        .and_then(|extension| extension.to_str())?;
    match extension.to_ascii_lowercase().as_str() {
        "csv" => Some("csv"),
        "jsonl" | "ndjson" => Some("jsonl"),
        "json" => Some("json"),
        "parquet" => Some("parquet"),
        "arrow" | "arrow-ipc" | "feather" => Some("arrow-ipc"),
        "avro" => Some("avro"),
        "orc" => Some("orc"),
        "vortex" => Some("vortex"),
        _ => None,
    }
}

fn extract_first_quoted_source_ref(statement: &str) -> Option<String> {
    let mut quote_char: Option<char> = None;
    let mut start = 0usize;
    for (index, char) in statement.char_indices() {
        if char == '\'' || char == '"' {
            if quote_char == Some(char) {
                let candidate = statement[start..index].trim();
                if infer_input_format_from_ref(candidate).is_some() {
                    return Some(candidate.to_string());
                }
                quote_char = None;
            } else if quote_char.is_none() {
                quote_char = Some(char);
                start = index + char.len_utf8();
            }
        }
    }
    None
}

fn sql_statement_has_limit(statement: &str) -> bool {
    let mut token = String::new();
    let mut quote_char: Option<char> = None;
    for char in statement.chars() {
        if char == '\'' || char == '"' {
            if quote_char == Some(char) {
                quote_char = None;
            } else if quote_char.is_none() {
                quote_char = Some(char);
            }
            token.clear();
            continue;
        }
        if quote_char.is_some() {
            continue;
        }
        if char.is_ascii_alphanumeric() || char == '_' {
            token.push(char.to_ascii_lowercase());
            continue;
        }
        if token == "limit" {
            return true;
        }
        token.clear();
    }
    token == "limit"
}

fn plan_summary_has_limit(summary: &str) -> bool {
    summary.to_ascii_lowercase().contains("limit(")
}

fn is_source_free_sql_statement(statement: &str) -> bool {
    let lower = statement.trim().to_ascii_lowercase();
    lower.starts_with("select ") && !lower.contains(" from ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(fields: &[(String, String)], key: &str) -> String {
        fields
            .iter()
            .find_map(|(field_key, value)| (field_key == key).then(|| value.clone()))
            .unwrap_or_else(|| panic!("missing route field: {key}"))
    }

    #[test]
    fn route_planner_admits_equivalent_sql_and_dataframe_local_file_routes() {
        let sql = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT id FROM 'target/input.csv' LIMIT 10",
                "--request",
                "collect",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("sql route request");
        let dataframe = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "target/input.csv",
                "--input-format",
                "csv",
                "--plan",
                "read_csv(target/input.csv) -> select(id) -> limit(10)",
                "--request",
                "collect",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("dataframe route request");

        let sql_plan = plan_public_workflow_route(&sql);
        let dataframe_plan = plan_public_workflow_route(&dataframe);

        assert_eq!(sql_plan.route_id, "local_file_direct_query");
        assert_eq!(dataframe_plan.route_id, sql_plan.route_id);
        assert_eq!(
            dataframe_plan.resolved_internal_command,
            "sql-local-source-smoke"
        );
        assert!(!dataframe_plan.preparation_included);
        assert!(!dataframe_plan.query_timing_starts_after_preparation);
    }

    #[test]
    fn route_planner_blocks_unbounded_collect_before_execution() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "python",
                "--input",
                "target/input.csv",
                "--input-format",
                "csv",
                "--plan",
                "read_csv(target/input.csv)",
                "--request",
                "collect",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);

        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(plan.route_id, "blocked");
        assert_eq!(
            field(&fields, "blocker_id"),
            "cg21.route.unbounded_collect_blocked"
        );
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
        assert_eq!(field(&fields, "runtime_execution"), "false");
    }

    #[test]
    fn route_planner_detects_sql_limit_token_outside_quoted_literals() {
        let limited = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT id FROM 'target/input.csv'\nLIMIT\n10",
                "--request",
                "collect",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("limited sql route request");
        let blocked = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT 'limit' AS label FROM 'target/input.csv'",
                "--request",
                "collect",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("quoted sql route request");

        assert_eq!(
            plan_public_workflow_route(&limited).route_id,
            "local_file_direct_query"
        );
        assert_eq!(plan_public_workflow_route(&blocked).route_id, "blocked");
    }

    #[test]
    fn route_planner_admits_prepare_once_policy_without_execution() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "cli",
                "--input",
                "target/input.parquet",
                "--input-format",
                "parquet",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "prepare-once",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("prepare route request");
        let plan = plan_public_workflow_route(&request);

        assert_eq!(plan.route_id, "local_file_prepare_once_first_query");
        assert_eq!(plan.vortex_normalization_point, "VortexPreparedState");
        assert!(plan.preparation_included);
        assert!(plan.query_timing_starts_after_preparation);
    }
}
