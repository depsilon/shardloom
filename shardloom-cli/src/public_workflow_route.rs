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
    benchmark_runtime,
    cli_output::{emit, emit_error},
    cli_unknown_arg_error, generated_source_runtime, sql_local_source_runtime,
    vortex_primitive_execution,
};

const ROUTE_SCHEMA_VERSION: &str = "shardloom.public_workflow_route.v1";
const FACADE_SCHEMA_VERSION: &str = "shardloom.public_workflow_execution_facade.v1";
const NATIVE_VORTEX_USER_ROUTE_CONTRACT_SCHEMA_VERSION: &str =
    "shardloom.native_vortex_user_route_contract.v1";
const TYPED_RESULT_SINK_CONTRACT_SCHEMA_VERSION: &str = "shardloom.typed_result_sink_contract.v1";
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
    allow_overwrite: bool,
    generated_source_kind: Option<String>,
    generated_schema: Option<String>,
    generated_rows: Option<String>,
    generated_range_start: Option<String>,
    generated_range_end: Option<String>,
    generated_range_step: Option<String>,
    generated_range_column: Option<String>,
    fanout_outputs: Vec<String>,
    native_vortex_operation_family: Option<String>,
    native_vortex_provider_scenario: Option<String>,
    native_vortex_right_input: Option<String>,
    vortex_primitive: Option<String>,
    vortex_predicate: Option<String>,
    vortex_columns: Option<String>,
    vortex_source_order_limit: Option<String>,
    memory_gb: Option<String>,
    max_parallelism: Option<String>,
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
    let request = effective_public_workflow_request(&request);
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
    let request = effective_public_workflow_request(&request);
    let plan = plan_public_workflow_route(&request);
    if plan.status != CommandStatus::Success {
        return emit_blocked_facade("run", format, &request, &plan);
    }

    match plan.route_id {
        "local_file_product_query" | "local_file_product_sink" => {
            let Some(statement) = request.sql_statement.clone() else {
                let blocked = executable_sql_required_route("run");
                return emit_blocked_facade("run", format, &request, &blocked);
            };
            let runtime_args = match sql_local_source_runtime_args(&request, &plan, statement) {
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
        "source_free_generated_output"
        | "generated_user_rows_direct_output"
        | "generated_range_direct_output"
        | "generated_sequence_direct_output" => {
            execute_generated_source_run(&request, &plan, format)
        }
        "native_vortex_count_all"
        | "native_vortex_count_where"
        | "native_vortex_filter"
        | "native_vortex_project"
        | "native_vortex_filter_project" => {
            execute_native_vortex_primitive_run(&request, &plan, format)
        }
        "native_vortex_user_aggregate"
        | "native_vortex_user_join"
        | "native_vortex_user_top_n"
        | "native_vortex_user_cast"
        | "native_vortex_user_contains"
        | "native_vortex_user_sink" => execute_native_vortex_provider_run(&request, &plan, format),
        _ => {
            let blocked = run_route_not_executable_yet(&plan);
            emit_blocked_facade("run", format, &request, &blocked)
        }
    }
}

fn execute_native_vortex_provider_run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
) -> ExitCode {
    let Some(scenario) = request.native_vortex_provider_scenario.clone() else {
        let blocked = native_vortex_payload_blocked_route(
            "public_workflow_route.native_vortex_provider_scenario",
            "native Vortex provider route requires a scenario payload",
            "pass --native-vortex-provider-scenario for the exact admitted provider-backed shape",
        );
        return emit_blocked_facade("run", format, request, &blocked);
    };
    let Some(fact_vortex) = request.input_uri.clone() else {
        let blocked = input_not_declared_route();
        return emit_blocked_facade("run", format, request, &blocked);
    };
    let dim_vortex = request
        .native_vortex_right_input
        .clone()
        .unwrap_or_else(|| fact_vortex.clone());
    let mut runtime_args = vec![scenario, fact_vortex, dim_vortex];
    if is_write_request(request) {
        let Some(output_ref) = request.output_ref.clone() else {
            let blocked = output_required_route("run", "native Vortex result sink");
            return emit_blocked_facade("run", format, request, &blocked);
        };
        runtime_args.extend(["--workspace".to_string(), output_ref]);
        runtime_args.push("--write-result-vortex".to_string());
    }
    runtime_args.extend(["--execution-mode".to_string(), "native_vortex".to_string()]);
    if let Some(memory_gb) = request.memory_gb.clone() {
        runtime_args.extend(["--memory-gb".to_string(), memory_gb]);
    }
    if let Some(max_parallelism) = request.max_parallelism.clone() {
        runtime_args.extend(["--max-parallelism".to_string(), max_parallelism]);
    }
    benchmark_runtime::handle_traditional_analytics_vortex_run_with_facade(
        runtime_args.into_iter(),
        format,
        "run",
        execution_attachment_fields("run", request, plan),
    )
}

fn execute_native_vortex_primitive_run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
) -> ExitCode {
    let Some(primitive) = normalized_vortex_primitive(request) else {
        let blocked = native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_primitive",
            "public native Vortex run requires a primitive payload",
            "pass --vortex-primitive with count, count_where, filter, project, or filter_project",
        );
        return emit_blocked_facade("run", format, request, &blocked);
    };
    let runtime_args = match native_vortex_primitive_runtime_args(request, primitive) {
        Ok(args) => args,
        Err(error) => {
            return emit_error("run", format, "public native Vortex run failed", &error);
        }
    };
    let extra_fields = execution_attachment_fields("run", request, plan);
    match primitive {
        PublicVortexPrimitive::Count => vortex_primitive_execution::handle_vortex_run_with_facade(
            runtime_args.into_iter(),
            format,
            "run",
            extra_fields,
        ),
        PublicVortexPrimitive::CountWhere => {
            vortex_primitive_execution::handle_vortex_count_where_with_facade(
                runtime_args.into_iter(),
                format,
                "run",
                extra_fields,
            )
        }
        PublicVortexPrimitive::Filter => {
            vortex_primitive_execution::handle_vortex_filter_with_facade(
                runtime_args.into_iter(),
                format,
                "run",
                extra_fields,
            )
        }
        PublicVortexPrimitive::Project => {
            vortex_primitive_execution::handle_vortex_project_with_facade(
                runtime_args.into_iter(),
                format,
                "run",
                extra_fields,
            )
        }
        PublicVortexPrimitive::FilterProject => {
            vortex_primitive_execution::handle_vortex_filter_project_with_facade(
                runtime_args.into_iter(),
                format,
                "run",
                extra_fields,
            )
        }
    }
}

fn execute_generated_source_run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
) -> ExitCode {
    match plan.route_id {
        "source_free_generated_output" => {
            execute_source_free_generated_sql_run(request, plan, format)
        }
        "generated_user_rows_direct_output" => {
            execute_generated_user_rows_run(request, plan, format)
        }
        "generated_range_direct_output" => {
            execute_generated_range_run(request, plan, format, false)
        }
        "generated_sequence_direct_output" => {
            execute_generated_range_run(request, plan, format, true)
        }
        _ => {
            let blocked = run_route_not_executable_yet(plan);
            emit_blocked_facade("run", format, request, &blocked)
        }
    }
}

fn execute_source_free_generated_sql_run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
) -> ExitCode {
    let Some(statement) = request.sql_statement.clone() else {
        let blocked = executable_sql_required_route("run");
        return emit_blocked_facade("run", format, request, &blocked);
    };
    let Some(output_ref) = request.output_ref.clone() else {
        let blocked = output_required_route("run", "source-free SQL run");
        return emit_blocked_facade("run", format, request, &blocked);
    };
    let runtime_args = generated_source_runtime_args(request, output_ref, statement);
    generated_source_runtime::handle_generated_source_sql_smoke_with_facade(
        runtime_args.into_iter(),
        format,
        "run",
        execution_attachment_fields("run", request, plan),
    )
}

fn execute_generated_user_rows_run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
) -> ExitCode {
    let Some(output_ref) = request.output_ref.clone() else {
        let blocked = output_required_route("run", "generated-source run");
        return emit_blocked_facade("run", format, request, &blocked);
    };
    let runtime_args = match generated_user_rows_runtime_args(request, output_ref) {
        Ok(args) => args,
        Err(error) => {
            return emit_error("run", format, "public workflow run failed", &error);
        }
    };
    generated_source_runtime::handle_generated_source_user_rows_smoke_with_facade(
        runtime_args.into_iter(),
        format,
        "run",
        execution_attachment_fields("run", request, plan),
    )
}

fn execute_generated_range_run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    sequence: bool,
) -> ExitCode {
    let Some(output_ref) = request.output_ref.clone() else {
        let blocked = output_required_route("run", "generated-source run");
        return emit_blocked_facade("run", format, request, &blocked);
    };
    let runtime_args = match generated_range_runtime_args(request, output_ref) {
        Ok(args) => args,
        Err(error) => {
            return emit_error("run", format, "public workflow run failed", &error);
        }
    };
    if sequence {
        generated_source_runtime::handle_generated_source_sequence_smoke_with_facade(
            runtime_args.into_iter(),
            format,
            "run",
            execution_attachment_fields("run", request, plan),
        )
    } else {
        generated_source_runtime::handle_generated_source_range_smoke_with_facade(
            runtime_args.into_iter(),
            format,
            "run",
            execution_attachment_fields("run", request, plan),
        )
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

    let mut runtime_args = vec![input_uri, output_ref];
    append_declared_local_input_format_args(&mut runtime_args, &request);
    if request.allow_overwrite {
        runtime_args.push("--allow-overwrite".to_string());
    }

    let extra_fields = execution_attachment_fields("prepare", &request, &plan);
    sql_local_source_runtime::handle_vortex_ingest_smoke_with_facade(
        runtime_args.into_iter(),
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
                "usage: shardloom route <sql|python|dataframe|cli> [--input <uri>] [--input-format <format>] [--sql <statement>] [--plan <summary>] [--request <collect|prepare|write_vortex|write_parquet|write_arrow_ipc|write_avro|write_orc|write_csv|write_jsonl|explain|route|evidence>] [--output <ref>] [--fanout-output <format=local-path>]... [--execution-policy <auto|direct|native_vortex|prepare_once>] [--materialization-policy <bounded|materialized|zero_decode|explicit>] [--evidence-level <report_only|runtime_smoke|production_admitted_local_workflow|claim_grade>] [--bounded true|false] [--allow-overwrite] [--generated-source-kind <kind>] [--generated-schema <schema>] [--generated-rows <rows>] [--generated-range-start <int>] [--generated-range-end <int>] [--generated-range-step <int>] [--generated-range-column <name>] [--native-vortex-operation-family <family>] [--vortex-primitive <count|count_where|filter|project|filter_project>] [--vortex-predicate <tiny-predicate>] [--vortex-columns <columns>] [--vortex-source-order-limit <rows>] [--memory-gb <n>] [--max-parallelism <n>]"
                    .to_string(),
            ));
        };
        let surface = normalize_surface(&surface)?;
        let mut request = Self::new(surface);

        while let Some(flag) = args.next() {
            request.parse_flag(&flag, &mut args)?;
        }

        request.infer_defaults();
        Ok(request)
    }

    fn new(surface: String) -> Self {
        Self {
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
            allow_overwrite: false,
            generated_source_kind: None,
            generated_schema: None,
            generated_rows: None,
            generated_range_start: None,
            generated_range_end: None,
            generated_range_step: None,
            generated_range_column: None,
            fanout_outputs: Vec::new(),
            native_vortex_operation_family: None,
            native_vortex_provider_scenario: None,
            native_vortex_right_input: None,
            vortex_primitive: None,
            vortex_predicate: None,
            vortex_columns: None,
            vortex_source_order_limit: None,
            memory_gb: None,
            max_parallelism: None,
        }
    }

    fn parse_flag(
        &mut self,
        flag: &str,
        args: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    ) -> Result<(), ShardLoomError> {
        match flag {
            "--input" => self.input_uri = Some(required_value(args, "--input")?),
            "--input-format" => {
                self.input_format = Some(normalize_input_format(&required_value(
                    args,
                    "--input-format",
                )?)?);
            }
            "--sql" => self.sql_statement = Some(required_value(args, "--sql")?),
            "--plan" => self.plan_summary = Some(required_value(args, "--plan")?),
            "--request" | "--requested-output" => {
                self.requested_output = normalize_requested_output(&required_value(args, flag)?)?;
            }
            "--output" => self.output_ref = Some(required_value(args, "--output")?),
            "--execution-policy" => {
                self.execution_policy =
                    normalize_execution_policy(&required_value(args, "--execution-policy")?)?;
            }
            "--materialization-policy" | "--decode-policy" => {
                self.materialization_policy =
                    normalize_materialization_policy(&required_value(args, flag)?)?;
            }
            "--evidence-level" => {
                self.evidence_level =
                    normalize_evidence_level(&required_value(args, "--evidence-level")?)?;
            }
            "--bounded" => {
                self.bounded = parse_bool_flag("--bounded", &required_value(args, "--bounded")?)?;
            }
            "--allow-overwrite" => self.allow_overwrite = true,
            "--fanout-output" => self
                .fanout_outputs
                .push(required_value(args, "--fanout-output")?),
            "--generated-source-kind" => {
                self.generated_source_kind = Some(required_value(args, "--generated-source-kind")?);
            }
            "--generated-schema" => {
                self.generated_schema = Some(required_value(args, "--generated-schema")?);
            }
            "--generated-rows" => {
                self.generated_rows = Some(required_value(args, "--generated-rows")?);
            }
            "--generated-range-start" => {
                self.generated_range_start = Some(required_value(args, "--generated-range-start")?);
            }
            "--generated-range-end" => {
                self.generated_range_end = Some(required_value(args, "--generated-range-end")?);
            }
            "--generated-range-step" => {
                self.generated_range_step = Some(required_value(args, "--generated-range-step")?);
            }
            "--generated-range-column" => {
                self.generated_range_column =
                    Some(required_value(args, "--generated-range-column")?);
            }
            "--native-vortex-operation-family" | "--operation-family" => {
                self.native_vortex_operation_family = Some(required_value(args, flag)?);
            }
            "--native-vortex-provider-scenario" | "--provider-scenario" => {
                self.native_vortex_provider_scenario = Some(required_value(args, flag)?);
            }
            "--native-vortex-right-input" | "--right-input" => {
                self.native_vortex_right_input = Some(required_value(args, flag)?);
            }
            "--vortex-primitive" => {
                self.vortex_primitive = Some(required_value(args, "--vortex-primitive")?);
            }
            "--vortex-predicate" => {
                self.vortex_predicate = Some(required_value(args, "--vortex-predicate")?);
            }
            "--vortex-columns" => {
                self.vortex_columns = Some(required_value(args, "--vortex-columns")?);
            }
            "--vortex-source-order-limit" => {
                self.vortex_source_order_limit =
                    Some(required_value(args, "--vortex-source-order-limit")?);
            }
            "--memory-gb" => {
                self.memory_gb = Some(required_value(args, "--memory-gb")?);
            }
            "--max-parallelism" => {
                self.max_parallelism = Some(required_value(args, "--max-parallelism")?);
            }
            extra => return Err(cli_unknown_arg_error("route", extra)),
        }
        Ok(())
    }

    fn infer_defaults(&mut self) {
        if self.sql_statement.is_none() && self.plan_summary.is_none() {
            self.plan_summary = Some(format!("{} workflow", self.surface));
        }
        if self.input_uri.is_none() {
            self.input_uri = self
                .sql_statement
                .as_deref()
                .and_then(extract_first_quoted_source_ref);
        }
        if self.input_format.is_none() {
            self.input_format = self
                .input_uri
                .as_deref()
                .and_then(infer_input_format_from_ref)
                .map(str::to_string);
        }
        if !self.bounded {
            self.bounded = self
                .sql_statement
                .as_deref()
                .is_some_and(sql_statement_has_limit)
                || self
                    .plan_summary
                    .as_deref()
                    .is_some_and(plan_summary_has_limit)
                || !matches!(self.requested_output.as_str(), "collect");
        }
    }
}

fn plan_public_workflow_route(request: &PublicWorkflowRouteRequest) -> PublicWorkflowRoutePlan {
    if matches!(request.requested_output.as_str(), "collect") && !request.fanout_outputs.is_empty()
    {
        return collect_fanout_blocked_route();
    }
    if matches!(request.requested_output.as_str(), "collect") && !request.bounded {
        return unbounded_collect_blocked_route();
    }

    if request.execution_policy == "native_vortex"
        && request.input_format.as_deref() != Some("vortex")
    {
        return native_vortex_input_required_route();
    }

    if is_native_vortex_route(request) {
        return native_vortex_route(request);
    }

    match request.input_format.as_deref() {
        Some(format) if is_local_file_format(format) => local_file_route(request),
        None if is_generated_source_write_request(request) => {
            generated_source_output_route(request)
        }
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

fn collect_fanout_blocked_route() -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.collect_fanout_blocked",
        "collect routes cannot carry fanout output payloads",
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            "public collect routes do not admit fanout outputs".to_string(),
            Some("public_workflow_route.fanout_outputs".to_string()),
            Some("fanout outputs require an explicit write request".to_string()),
            Some(
                "use --request write_jsonl/write_csv/etc. with --output and --fanout-output"
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn is_native_vortex_route(request: &PublicWorkflowRouteRequest) -> bool {
    request.input_format.as_deref() == Some("vortex")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublicVortexPrimitive {
    Count,
    CountWhere,
    Filter,
    Project,
    FilterProject,
}

impl PublicVortexPrimitive {
    fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "count" | "count_all" => Some(Self::Count),
            "count_where" | "count-where" | "filter_count" | "filtered_count" => {
                Some(Self::CountWhere)
            }
            "filter" | "filter_predicate" => Some(Self::Filter),
            "project" | "project_columns" => Some(Self::Project),
            "filter_project" | "filter-project" | "filter_and_project" => Some(Self::FilterProject),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::CountWhere => "count_where",
            Self::Filter => "filter",
            Self::Project => "project",
            Self::FilterProject => "filter_project",
        }
    }

    const fn route_id(self) -> &'static str {
        match self {
            Self::Count => "native_vortex_count_all",
            Self::CountWhere => "native_vortex_count_where",
            Self::Filter => "native_vortex_filter",
            Self::Project => "native_vortex_project",
            Self::FilterProject => "native_vortex_filter_project",
        }
    }

    const fn resolved_internal_command(self) -> &'static str {
        match self {
            Self::Count => "vortex-run",
            Self::CountWhere => "vortex-count-where",
            Self::Filter => "vortex-filter",
            Self::Project => "vortex-project",
            Self::FilterProject => "vortex-filter-project",
        }
    }

    const fn requires_predicate(self) -> bool {
        matches!(self, Self::CountWhere | Self::Filter | Self::FilterProject)
    }

    const fn requires_columns(self) -> bool {
        matches!(self, Self::Project | Self::FilterProject)
    }

    const fn allows_source_order_limit(self) -> bool {
        matches!(self, Self::Filter | Self::Project | Self::FilterProject)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeVortexOperationFamily {
    Count,
    FilterProjectLimit,
    Aggregate,
    Join,
    TopN,
    Cast,
    Contains,
    Sink,
    GeneralQuery,
}

impl NativeVortexOperationFamily {
    fn parse(value: &str) -> Option<Self> {
        let normalized = value.trim().to_ascii_lowercase().replace('-', "_");
        match normalized.as_str() {
            "count" | "count_where" | "filter_count" | "primitive_count" => Some(Self::Count),
            "filter"
            | "project"
            | "filter_project"
            | "filter_project_limit"
            | "filter_project_limit_collect"
            | "primitive_filter_project" => Some(Self::FilterProjectLimit),
            "aggregate"
            | "aggregation"
            | "group_by"
            | "grouped_count_sum"
            | "null_heavy_aggregate" => Some(Self::Aggregate),
            "join" | "hash_join" | "join_state" | "multi_input_join" => Some(Self::Join),
            "top_n" | "topn" | "global_top_n" | "nlargest" | "nsmallest" => Some(Self::TopN),
            "cast" | "try_cast" | "trycast" => Some(Self::Cast),
            "contains" | "substring_contains" | "string_contains" => Some(Self::Contains),
            "sink" | "write" | "write_vortex" | "write_jsonl" | "write_csv" | "write_parquet"
            | "write_arrow_ipc" => Some(Self::Sink),
            "query" | "general" | "general_query" | "unshaped_query" => Some(Self::GeneralQuery),
            _ => None,
        }
    }

    const fn from_primitive(primitive: PublicVortexPrimitive) -> Self {
        match primitive {
            PublicVortexPrimitive::Count | PublicVortexPrimitive::CountWhere => Self::Count,
            PublicVortexPrimitive::Filter
            | PublicVortexPrimitive::Project
            | PublicVortexPrimitive::FilterProject => Self::FilterProjectLimit,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::FilterProjectLimit => "filter_project_limit",
            Self::Aggregate => "aggregate",
            Self::Join => "join",
            Self::TopN => "top_n",
            Self::Cast => "cast",
            Self::Contains => "contains",
            Self::Sink => "sink",
            Self::GeneralQuery => "general_query",
        }
    }

    const fn allows_primitive(self, primitive: PublicVortexPrimitive) -> bool {
        matches!(
            (self, primitive),
            (
                Self::Count,
                PublicVortexPrimitive::Count | PublicVortexPrimitive::CountWhere
            ) | (
                Self::FilterProjectLimit,
                PublicVortexPrimitive::Filter
                    | PublicVortexPrimitive::Project
                    | PublicVortexPrimitive::FilterProject
            )
        )
    }

    const fn blocker_id(self) -> &'static str {
        match self {
            Self::Aggregate => "py-vortex-route-unify-1.native_vortex_aggregate_route_missing",
            Self::Join => "py-vortex-route-unify-1.native_vortex_join_state_missing",
            Self::TopN => "py-vortex-route-unify-1.native_vortex_top_n_route_missing",
            Self::Cast => "py-vortex-route-unify-1.native_vortex_cast_route_missing",
            Self::Contains => "py-vortex-route-unify-1.native_vortex_contains_route_missing",
            Self::Sink => "py-vortex-route-unify-1.native_vortex_sink_contract_missing",
            Self::Count | Self::FilterProjectLimit | Self::GeneralQuery => {
                "py-vortex-route-unify-1.native_vortex_general_route_missing"
            }
        }
    }

    const fn blocker_reason(self) -> &'static str {
        match self {
            Self::Aggregate => "native Vortex aggregate user route is not admitted yet",
            Self::Join => "native Vortex multi-input join state is not admitted yet",
            Self::TopN => "native Vortex global top-N user route is not admitted yet",
            Self::Cast => "native Vortex cast/try-cast user route is not admitted yet",
            Self::Contains => "native Vortex substring contains user route is not admitted yet",
            Self::Sink => "native Vortex typed sink contract is not admitted yet",
            Self::Count | Self::FilterProjectLimit | Self::GeneralQuery => {
                "native Vortex user route requires an admitted primitive payload or promoted operator route"
            }
        }
    }

    const fn required_evidence(self) -> &'static str {
        match self {
            Self::Count | Self::FilterProjectLimit => {
                "native_vortex_input;public_facade;operator_result;decode_materialization_boundary;fallback_disabled"
            }
            Self::Aggregate => {
                "python_expression_lowering;native_vortex_group_state;decoded_reference_correctness;route_certificate"
            }
            Self::Join => {
                "multi_input_binding;native_vortex_build_probe_state;decoded_reference_correctness;route_certificate"
            }
            Self::TopN => {
                "native_vortex_ordering_semantics;bounded_result_contract;decoded_reference_correctness;route_certificate"
            }
            Self::Cast => {
                "native_vortex_cast_semantics;fail_closed_diagnostics;decoded_reference_correctness;route_certificate"
            }
            Self::Contains => {
                "native_vortex_string_contains_kernel;utf8_semantics;decoded_reference_correctness;route_certificate"
            }
            Self::Sink => {
                "typed_sink_contract;native_vortex_output_or_compatibility_sink_evidence;decode_materialization_boundary;route_certificate"
            }
            Self::GeneralQuery => {
                "declared_operation_family;native_vortex_operator_route;decoded_reference_correctness;route_certificate"
            }
        }
    }

    const fn next_action(self) -> &'static str {
        match self {
            Self::Count | Self::FilterProjectLimit => {
                "pass the supported --vortex-primitive payload for this operation family"
            }
            Self::Aggregate => {
                "implement and certify native Vortex grouped count/sum route before admitting this family"
            }
            Self::Join => {
                "implement and certify native Vortex multi-input build/probe state before admitting this family"
            }
            Self::TopN => {
                "implement and certify native Vortex global top-N route before admitting this family"
            }
            Self::Cast => {
                "implement and certify native Vortex cast/try-cast semantics before admitting this family"
            }
            Self::Contains => {
                "implement and certify native Vortex substring contains kernel before admitting this family"
            }
            Self::Sink => {
                "implement and certify typed native Vortex sink contract before admitting this family"
            }
            Self::GeneralQuery => {
                "declare an operation family and route only admitted native Vortex primitive or promoted operator families"
            }
        }
    }
}

fn native_vortex_route(request: &PublicWorkflowRouteRequest) -> PublicWorkflowRoutePlan {
    let mut effective_request = request.clone();
    if let Some(payload) = infer_native_vortex_route_payload(&effective_request) {
        payload.apply(&mut effective_request);
    }
    if effective_request.input_uri.is_none() {
        return input_not_declared_route();
    }
    let Ok(requested_family) = normalized_native_vortex_operation_family(&effective_request) else {
        return native_vortex_payload_blocked_route(
            "public_workflow_route.native_vortex_operation_family",
            "unsupported native Vortex operation family",
            "use count, filter_project_limit, aggregate, join, top_n, cast, contains, or sink",
        );
    };
    if effective_request.native_vortex_provider_scenario.is_some() {
        return native_vortex_provider_route(&effective_request, requested_family);
    }
    if is_write_request(&effective_request)
        || effective_request.output_ref.is_some()
        || !effective_request.fanout_outputs.is_empty()
    {
        return native_vortex_operation_blocked_route(NativeVortexOperationFamily::Sink);
    }
    let Some(primitive) = normalized_vortex_primitive(&effective_request) else {
        if effective_request.vortex_primitive.is_some() {
            return native_vortex_payload_blocked_route(
                "public_workflow_route.vortex_primitive",
                "unsupported native Vortex primitive",
                "use count, count_where, filter, project, or filter_project",
            );
        }
        return native_vortex_operation_blocked_route(
            requested_family.unwrap_or(NativeVortexOperationFamily::GeneralQuery),
        );
    };
    let operation_family =
        requested_family.unwrap_or_else(|| NativeVortexOperationFamily::from_primitive(primitive));
    if !operation_family.allows_primitive(primitive) {
        return native_vortex_operation_blocked_route(operation_family);
    }
    if primitive.requires_predicate() && effective_request.vortex_predicate.is_none() {
        return native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_predicate",
            "native Vortex primitive requires a predicate payload",
            "pass --vortex-predicate with the scoped tiny predicate expression",
        );
    }
    if primitive.requires_columns() && effective_request.vortex_columns.is_none() {
        return native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_columns",
            "native Vortex primitive requires a projection payload",
            "pass --vortex-columns with comma-separated projected columns",
        );
    }
    if effective_request.vortex_source_order_limit.is_some()
        && !primitive.allows_source_order_limit()
    {
        return native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_source_order_limit",
            "native Vortex primitive does not admit a source-order limit",
            "use --vortex-source-order-limit only with filter, project, or filter_project",
        );
    }
    if let Some(plan) = native_vortex_resource_hint_blocker(&effective_request) {
        return plan;
    }
    admitted_route(
        primitive.route_id(),
        primitive.resolved_internal_command(),
        "native_vortex_file",
        "native_vortex_boundary",
        "native_vortex",
        false,
        true,
    )
}

fn native_vortex_resource_hint_blocker(
    request: &PublicWorkflowRouteRequest,
) -> Option<PublicWorkflowRoutePlan> {
    if let Some(error) = positive_u64_option_error("--memory-gb", request.memory_gb.as_deref()) {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.memory_gb",
            error,
            "pass --memory-gb with an integer >= 1",
        ));
    }
    if let Some(error) =
        positive_usize_option_error("--max-parallelism", request.max_parallelism.as_deref())
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.max_parallelism",
            error,
            "pass --max-parallelism with an integer >= 1",
        ));
    }
    if let Some(error) = positive_usize_option_error(
        "--vortex-source-order-limit",
        request.vortex_source_order_limit.as_deref(),
    ) {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_source_order_limit",
            error,
            "pass --vortex-source-order-limit with an integer >= 1",
        ));
    }
    None
}

fn native_vortex_provider_route(
    request: &PublicWorkflowRouteRequest,
    requested_family: Option<NativeVortexOperationFamily>,
) -> PublicWorkflowRoutePlan {
    if !cfg!(feature = "vortex-traditional-analytics-benchmark") {
        return native_vortex_provider_feature_gated_route();
    }
    if request.vortex_primitive.is_some() {
        return native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_primitive",
            "native Vortex provider routes do not accept primitive payloads",
            "use primitive payloads for count/filter/project, or provider scenario payloads for promoted operator families",
        );
    }
    if request.output_ref.is_some() && !is_write_request(request) {
        return native_vortex_operation_blocked_route(NativeVortexOperationFamily::Sink);
    }
    if is_write_request(request) && request.requested_output != "write_vortex" {
        return native_vortex_payload_blocked_route(
            "public_workflow_route.requested_output",
            "native Vortex provider sinks currently admit only write_vortex",
            "use write_vortex for native result-sink proof or keep compatibility exports on product local routes",
        );
    }
    let Some(scenario_text) = request.native_vortex_provider_scenario.as_ref() else {
        return native_vortex_payload_blocked_route(
            "public_workflow_route.native_vortex_provider_scenario",
            "native Vortex provider route requires a scenario payload",
            "pass --native-vortex-provider-scenario for an exact admitted provider-backed route",
        );
    };
    let Ok(scenario) = shardloom_vortex::TraditionalAnalyticsScenario::parse(scenario_text) else {
        return native_vortex_payload_blocked_route(
            "public_workflow_route.native_vortex_provider_scenario",
            "unknown native Vortex provider scenario",
            "use group-by-aggregation, null-heavy-aggregate, hash-join, sort-and-top-k, clean-cast-filter-write, malformed-timestamp-dirty-csv, or nested-json-field-scan",
        );
    };
    let scenario_family = native_vortex_provider_scenario_family(scenario);
    if scenario_family == NativeVortexOperationFamily::GeneralQuery {
        return native_vortex_operation_blocked_route(NativeVortexOperationFamily::GeneralQuery);
    }
    let effective_family = if is_write_request(request) {
        NativeVortexOperationFamily::Sink
    } else {
        requested_family.unwrap_or(scenario_family)
    };
    if !matches!(effective_family, NativeVortexOperationFamily::Sink)
        && effective_family != scenario_family
    {
        return native_vortex_payload_blocked_route(
            "public_workflow_route.native_vortex_operation_family",
            "native Vortex operation family does not match the provider scenario",
            "send the provider scenario only for its matching aggregate, join, top_n, cast, or contains family",
        );
    }
    if native_vortex_provider_scenario_requires_right_input(scenario)
        && request.native_vortex_right_input.is_none()
    {
        return native_vortex_payload_blocked_route(
            "public_workflow_route.native_vortex_right_input",
            "native Vortex provider scenario requires a second Vortex input",
            "pass --native-vortex-right-input for join/build state routes",
        );
    }
    admitted_route(
        native_vortex_provider_route_id(effective_family),
        "traditional-analytics-vortex-run",
        "native_vortex_file",
        "native_vortex_user_operator_provider",
        "native_vortex",
        false,
        true,
    )
}

fn native_vortex_provider_feature_gated_route() -> PublicWorkflowRoutePlan {
    blocked_route(
        "py-vortex-route-unify-1.native_vortex_provider_feature_gated",
        "native Vortex provider route requires the vortex-traditional-analytics-benchmark feature",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "native Vortex provider-backed user routes are feature-gated in this binary".to_string(),
            Some("public_workflow_route.native_vortex_provider_scenario".to_string()),
            Some("compiled_without=vortex-traditional-analytics-benchmark".to_string()),
            Some("build the release binary with --features vortex-traditional-analytics-benchmark to execute promoted native Vortex operator routes".to_string()),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn native_vortex_provider_scenario_family(
    scenario: shardloom_vortex::TraditionalAnalyticsScenario,
) -> NativeVortexOperationFamily {
    match scenario {
        shardloom_vortex::TraditionalAnalyticsScenario::GroupByAggregation
        | shardloom_vortex::TraditionalAnalyticsScenario::NullHeavyAggregate => {
            NativeVortexOperationFamily::Aggregate
        }
        shardloom_vortex::TraditionalAnalyticsScenario::HashJoin
        | shardloom_vortex::TraditionalAnalyticsScenario::JoinAggregate => {
            NativeVortexOperationFamily::Join
        }
        shardloom_vortex::TraditionalAnalyticsScenario::SortAndTopK
        | shardloom_vortex::TraditionalAnalyticsScenario::TopNPerGroup => {
            NativeVortexOperationFamily::TopN
        }
        shardloom_vortex::TraditionalAnalyticsScenario::CleanCastFilterWrite
        | shardloom_vortex::TraditionalAnalyticsScenario::MalformedTimestampDirtyCsv => {
            NativeVortexOperationFamily::Cast
        }
        shardloom_vortex::TraditionalAnalyticsScenario::NestedJsonFieldScan => {
            NativeVortexOperationFamily::Contains
        }
        _ => NativeVortexOperationFamily::GeneralQuery,
    }
}

fn native_vortex_provider_scenario_requires_right_input(
    scenario: shardloom_vortex::TraditionalAnalyticsScenario,
) -> bool {
    matches!(
        scenario,
        shardloom_vortex::TraditionalAnalyticsScenario::HashJoin
            | shardloom_vortex::TraditionalAnalyticsScenario::JoinAggregate
    )
}

fn native_vortex_provider_route_id(family: NativeVortexOperationFamily) -> &'static str {
    match family {
        NativeVortexOperationFamily::Aggregate => "native_vortex_user_aggregate",
        NativeVortexOperationFamily::Join => "native_vortex_user_join",
        NativeVortexOperationFamily::TopN => "native_vortex_user_top_n",
        NativeVortexOperationFamily::Cast => "native_vortex_user_cast",
        NativeVortexOperationFamily::Contains => "native_vortex_user_contains",
        NativeVortexOperationFamily::Sink => "native_vortex_user_sink",
        NativeVortexOperationFamily::Count
        | NativeVortexOperationFamily::FilterProjectLimit
        | NativeVortexOperationFamily::GeneralQuery => "native_vortex_user_general_query",
    }
}

fn native_vortex_input_required_route() -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.native_vortex_input_required",
        "native Vortex execution policy requires declared Vortex input",
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            "native Vortex public routes require input_format=vortex".to_string(),
            Some("public_workflow_route.input_format".to_string()),
            Some("execution_policy=native_vortex was requested for a non-Vortex input".to_string()),
            Some("prepare compatibility input first or use a direct compatibility route without native_vortex policy".to_string()),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn native_vortex_operation_blocked_route(
    family: NativeVortexOperationFamily,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        family.blocker_id(),
        family.blocker_reason(),
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            family.blocker_reason().to_string(),
            Some("public_workflow_route.native_vortex_operation_family".to_string()),
            Some(format!(
                "operation_family={} required_evidence={}",
                family.as_str(),
                family.required_evidence()
            )),
            Some(family.next_action().to_string()),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn normalized_native_vortex_operation_family(
    request: &PublicWorkflowRouteRequest,
) -> Result<Option<NativeVortexOperationFamily>, ()> {
    match request.native_vortex_operation_family.as_deref() {
        Some(value) => NativeVortexOperationFamily::parse(value)
            .map(Some)
            .ok_or(()),
        None => Ok(None),
    }
}

fn normalized_vortex_primitive(
    request: &PublicWorkflowRouteRequest,
) -> Option<PublicVortexPrimitive> {
    request
        .vortex_primitive
        .as_deref()
        .and_then(PublicVortexPrimitive::parse)
}

#[derive(Debug, Clone)]
struct InferredNativeVortexRoutePayload {
    family: NativeVortexOperationFamily,
    provider_scenario: Option<&'static str>,
    primitive: Option<PublicVortexPrimitive>,
    predicate: Option<String>,
    columns: Option<String>,
    source_order_limit: Option<String>,
    right_input: Option<String>,
}

impl InferredNativeVortexRoutePayload {
    fn apply(self, request: &mut PublicWorkflowRouteRequest) {
        if request.native_vortex_operation_family.is_none() {
            request.native_vortex_operation_family = Some(self.family.as_str().to_string());
        }
        if request.native_vortex_provider_scenario.is_none() {
            request.native_vortex_provider_scenario = self.provider_scenario.map(str::to_string);
        }
        if request.vortex_primitive.is_none() {
            request.vortex_primitive = self
                .primitive
                .map(|primitive| primitive.as_str().to_string());
        }
        if request.vortex_predicate.is_none() {
            request.vortex_predicate = self.predicate;
        }
        if request.vortex_columns.is_none() {
            request.vortex_columns = self.columns;
        }
        if request.vortex_source_order_limit.is_none() {
            request.vortex_source_order_limit = self.source_order_limit;
        }
        if request.native_vortex_right_input.is_none() {
            request.native_vortex_right_input = self.right_input;
        }
    }
}

fn infer_native_vortex_route_payload(
    request: &PublicWorkflowRouteRequest,
) -> Option<InferredNativeVortexRoutePayload> {
    if request.input_format.as_deref() != Some("vortex") {
        return None;
    }
    if request.surface == "sql" || request.sql_statement.is_some() {
        return None;
    }
    if request.native_vortex_provider_scenario.is_some() || request.vortex_primitive.is_some() {
        return None;
    }
    infer_native_vortex_provider_payload(request)
        .or_else(|| infer_native_vortex_primitive_payload(request))
}

fn effective_public_workflow_request(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRouteRequest {
    let mut effective_request = request.clone();
    if let Some(payload) = infer_native_vortex_route_payload(&effective_request) {
        payload.apply(&mut effective_request);
    }
    effective_request
}

fn infer_native_vortex_provider_payload(
    request: &PublicWorkflowRouteRequest,
) -> Option<InferredNativeVortexRoutePayload> {
    if request.surface == "sql" || request.sql_statement.is_some() {
        return None;
    }
    if !matches!(
        request.requested_output.as_str(),
        "collect" | "write_vortex"
    ) {
        return None;
    }
    let operations = parse_plan_summary_operations(request.plan_summary.as_deref()?)?;
    if !summary_read_vortex_matches_input(&operations, request.input_uri.as_deref()?) {
        return None;
    }
    let write_vortex = request.requested_output == "write_vortex";

    let (family, provider_scenario) = if summary_matches_group_by_aggregation(&operations) {
        (
            NativeVortexOperationFamily::Aggregate,
            "group-by-aggregation",
        )
    } else if summary_matches_null_heavy_aggregate(&operations) {
        (
            NativeVortexOperationFamily::Aggregate,
            "null-heavy-aggregate",
        )
    } else if summary_matches_hash_join(&operations) {
        (NativeVortexOperationFamily::Join, "hash-join")
    } else if summary_matches_global_top_n(&operations) {
        (NativeVortexOperationFamily::TopN, "sort-and-top-k")
    } else if summary_matches_clean_cast(&operations) {
        (NativeVortexOperationFamily::Cast, "clean-cast-filter-write")
    } else if summary_matches_malformed_timestamp(&operations) {
        (
            NativeVortexOperationFamily::Cast,
            "malformed-timestamp-dirty-csv",
        )
    } else if summary_matches_nested_json_contains(&operations) {
        (
            NativeVortexOperationFamily::Contains,
            "nested-json-field-scan",
        )
    } else {
        return None;
    };

    Some(InferredNativeVortexRoutePayload {
        family: if write_vortex {
            NativeVortexOperationFamily::Sink
        } else {
            family
        },
        provider_scenario: Some(provider_scenario),
        primitive: None,
        predicate: None,
        columns: None,
        source_order_limit: None,
        right_input: infer_native_vortex_right_input(request),
    })
}

#[derive(Debug, Clone, Copy)]
struct SummaryOperation<'a> {
    kind: &'a str,
    arg: &'a str,
}

fn parse_plan_summary_operations(summary: &str) -> Option<Vec<SummaryOperation<'_>>> {
    let mut operations = Vec::new();
    for segment in summary.split(" -> ") {
        let open = segment.find('(')?;
        if !segment.ends_with(')') || open == 0 {
            return None;
        }
        let kind = &segment[..open];
        if !kind
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch == '_' || ch.is_ascii_digit())
        {
            return None;
        }
        operations.push(SummaryOperation {
            kind,
            arg: &segment[open + 1..segment.len() - 1],
        });
    }
    (!operations.is_empty()).then_some(operations)
}

fn summary_read_vortex_matches_input(operations: &[SummaryOperation<'_>], input_uri: &str) -> bool {
    operations.first().is_some_and(|operation| {
        operation.kind == "read_vortex" && operation.arg.trim() == input_uri
    })
}

fn summary_matches_group_by_aggregation(operations: &[SummaryOperation<'_>]) -> bool {
    matches_summary_kinds(
        operations,
        &["read_vortex", "group_by", "aggregate", "limit"],
    ) && summary_arg_eq(operations[1].arg, "group_key")
        && summary_arg_eq(
            operations[2].arg,
            "count(*) AS rows,sum(metric) AS total_metric",
        )
        && summary_limit_eq(operations[3].arg, 100)
}

fn summary_matches_null_heavy_aggregate(operations: &[SummaryOperation<'_>]) -> bool {
    matches_summary_kinds(
        operations,
        &["read_vortex", "filter", "group_by", "aggregate", "limit"],
    ) && summary_arg_eq(operations[1].arg, "nullable_metric_00 IS NOT NULL")
        && summary_arg_eq(operations[2].arg, "group_key")
        && summary_arg_eq(
            operations[3].arg,
            "count(*) AS rows,sum(nullable_metric_00) AS total_nullable_metric",
        )
        && summary_positive_limit(operations[4].arg)
}

fn summary_matches_hash_join(operations: &[SummaryOperation<'_>]) -> bool {
    matches_summary_kinds(operations, &["read_vortex", "join", "select", "limit"])
        && summary_arg_matches_hash_join(operations[1].arg)
        && summary_arg_eq(operations[2].arg, "f.id,d.dim_label,f.metric")
        && summary_positive_limit(operations[3].arg)
}

fn summary_matches_global_top_n(operations: &[SummaryOperation<'_>]) -> bool {
    matches_summary_kinds(operations, &["read_vortex", "select", "sort", "limit"])
        && summary_arg_eq(operations[1].arg, "id,group_key,metric")
        && summary_arg_eq(operations[2].arg, "desc,metric")
        && summary_limit_eq(operations[3].arg, 10)
}

fn summary_matches_clean_cast(operations: &[SummaryOperation<'_>]) -> bool {
    matches_summary_kinds(
        operations,
        &["read_vortex", "with_column", "filter", "limit"],
    ) && summary_arg_eq(
        operations[1].arg,
        "amount_float,CAST(dirty_numeric AS float64)",
    ) && summary_arg_eq(operations[2].arg, "amount_float >= 0")
        && summary_positive_limit(operations[3].arg)
}

fn summary_matches_malformed_timestamp(operations: &[SummaryOperation<'_>]) -> bool {
    matches_summary_kinds(operations, &["read_vortex", "with_column", "limit"])
        && summary_arg_eq(
            operations[1].arg,
            "event_day,CAST(raw_event_time AS date32)",
        )
        && summary_positive_limit(operations[2].arg)
}

fn summary_matches_nested_json_contains(operations: &[SummaryOperation<'_>]) -> bool {
    matches_summary_kinds(operations, &["read_vortex", "filter", "select", "limit"])
        && summary_arg_eq(operations[1].arg, "nested_payload LIKE '%target%'")
        && summary_arg_eq(operations[2].arg, "id,nested_payload")
        && summary_positive_limit(operations[3].arg)
}

fn matches_summary_kinds(operations: &[SummaryOperation<'_>], kinds: &[&str]) -> bool {
    operations.len() == kinds.len()
        && operations
            .iter()
            .zip(kinds)
            .all(|(operation, kind)| operation.kind == *kind)
}

fn summary_arg_eq(actual: &str, expected: &str) -> bool {
    compact_ascii_lower(actual) == compact_ascii_lower(expected)
}

fn summary_positive_limit(value: &str) -> bool {
    value.trim().parse::<usize>().is_ok_and(|parsed| parsed > 0)
}

fn summary_limit_eq(value: &str, expected: usize) -> bool {
    value.trim().parse::<usize>() == Ok(expected)
}

fn summary_arg_matches_hash_join(value: &str) -> bool {
    let parts: Vec<_> = value.split(',').map(str::trim).collect();
    parts.len() == 7
        && parts[0].ends_with(".vortex")
        && parts[1] == "dim_key"
        && parts[2] == "dim_key"
        && parts[3] == "inner"
        && parts[4] == "f"
        && parts[5] == "d"
        && parts[6].is_empty()
}

fn infer_native_vortex_primitive_payload(
    request: &PublicWorkflowRouteRequest,
) -> Option<InferredNativeVortexRoutePayload> {
    if request.requested_output != "collect" {
        return None;
    }
    let operations = parse_plan_summary_operations(request.plan_summary.as_deref()?)?;
    if !summary_read_vortex_matches_input(&operations, request.input_uri.as_deref()?) {
        return None;
    }
    let (primitive, predicate, columns, source_order_limit) =
        if matches_summary_kinds(&operations, &["read_vortex", "filter", "select", "limit"])
            && summary_positive_limit(operations[3].arg)
        {
            (
                PublicVortexPrimitive::FilterProject,
                Some(operations[1].arg.trim().to_string()),
                Some(operations[2].arg.trim().to_string()),
                Some(operations[3].arg.trim().to_string()),
            )
        } else if matches_summary_kinds(&operations, &["read_vortex", "filter", "select"]) {
            (
                PublicVortexPrimitive::FilterProject,
                Some(operations[1].arg.trim().to_string()),
                Some(operations[2].arg.trim().to_string()),
                None,
            )
        } else if matches_summary_kinds(&operations, &["read_vortex", "filter", "limit"])
            && summary_positive_limit(operations[2].arg)
        {
            (
                PublicVortexPrimitive::Filter,
                Some(operations[1].arg.trim().to_string()),
                None,
                Some(operations[2].arg.trim().to_string()),
            )
        } else if matches_summary_kinds(&operations, &["read_vortex", "filter"]) {
            (
                PublicVortexPrimitive::Filter,
                Some(operations[1].arg.trim().to_string()),
                None,
                None,
            )
        } else if matches_summary_kinds(&operations, &["read_vortex", "select", "limit"])
            && summary_positive_limit(operations[2].arg)
        {
            (
                PublicVortexPrimitive::Project,
                None,
                Some(operations[1].arg.trim().to_string()),
                Some(operations[2].arg.trim().to_string()),
            )
        } else if matches_summary_kinds(&operations, &["read_vortex", "select"]) {
            (
                PublicVortexPrimitive::Project,
                None,
                Some(operations[1].arg.trim().to_string()),
                None,
            )
        } else {
            return None;
        };
    Some(InferredNativeVortexRoutePayload {
        family: NativeVortexOperationFamily::from_primitive(primitive),
        provider_scenario: None,
        primitive: Some(primitive),
        predicate,
        columns,
        source_order_limit,
        right_input: None,
    })
}

fn infer_native_vortex_right_input(request: &PublicWorkflowRouteRequest) -> Option<String> {
    if let Some(value) = request.native_vortex_right_input.clone() {
        return Some(value);
    }
    request
        .plan_summary
        .as_deref()
        .and_then(|summary| summary_operation_arg(summary, "join"))
        .and_then(|join| join.split(',').next().map(str::to_string))
        .or_else(|| {
            request.sql_statement.as_deref().and_then(|sql| {
                let refs = quoted_source_refs(sql);
                refs.get(1).cloned()
            })
        })
}

fn summary_operation_arg(summary: &str, operation: &str) -> Option<String> {
    let marker = format!(" -> {operation}(");
    let start = summary.find(&marker)? + marker.len();
    let rest = &summary[start..];
    let end = rest.find(')')?;
    let value = rest[..end].trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn quoted_source_refs(value: &str) -> Vec<String> {
    let mut refs = Vec::new();
    let mut chars = value.char_indices().peekable();
    while let Some((_, ch)) = chars.next() {
        if ch != '\'' {
            continue;
        }
        let mut current = String::new();
        while let Some((_, inner)) = chars.next() {
            if inner == '\'' {
                if matches!(chars.peek(), Some((_, '\''))) {
                    current.push('\'');
                    chars.next();
                    continue;
                }
                break;
            }
            current.push(inner);
        }
        if !current.is_empty() {
            refs.push(current);
        }
    }
    refs
}

fn compact_ascii_lower(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect()
}

fn positive_u64_option_error<'a>(flag: &str, value: Option<&str>) -> Option<&'a str> {
    let value = value?;
    match value.parse::<u64>() {
        Ok(parsed) if parsed >= 1 => None,
        Ok(_) => Some(match flag {
            "--memory-gb" => "memory_gb must be >= 1",
            _ => "value must be >= 1",
        }),
        Err(_) => Some(match flag {
            "--memory-gb" => "memory_gb must be an unsigned integer",
            _ => "value must be an unsigned integer",
        }),
    }
}

fn positive_usize_option_error<'a>(flag: &str, value: Option<&str>) -> Option<&'a str> {
    let value = value?;
    match value.parse::<usize>() {
        Ok(parsed) if parsed >= 1 => None,
        Ok(_) => Some(match flag {
            "--max-parallelism" => "max_parallelism must be >= 1",
            "--vortex-source-order-limit" => "source-order limit must be >= 1",
            _ => "value must be >= 1",
        }),
        Err(_) => Some(match flag {
            "--max-parallelism" => "max_parallelism must be an unsigned integer",
            "--vortex-source-order-limit" => "source-order limit must be an unsigned integer",
            _ => "value must be an unsigned integer",
        }),
    }
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
            "local_file_product_sink",
            "local-workflow-run",
            "compatibility_local_source",
            "compatibility_source_state_pre_vortex_unification",
            "product_local",
            false,
            false,
        )
    } else {
        admitted_route(
            "local_file_product_query",
            "local-workflow-run",
            "compatibility_local_source",
            "compatibility_source_state_pre_vortex_unification",
            "product_local",
            false,
            false,
        )
    }
}

fn is_write_request(request: &PublicWorkflowRouteRequest) -> bool {
    matches!(
        request.requested_output.as_str(),
        "write_vortex"
            | "write_parquet"
            | "write_arrow_ipc"
            | "write_avro"
            | "write_orc"
            | "write_csv"
            | "write_jsonl"
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

fn is_generated_source_write_request(request: &PublicWorkflowRouteRequest) -> bool {
    request.generated_source_kind.is_some() && is_write_request(request)
}

fn generated_source_output_route(request: &PublicWorkflowRouteRequest) -> PublicWorkflowRoutePlan {
    let Some(kind) = normalized_generated_source_kind(request) else {
        return generated_source_payload_blocked_route(
            "public_workflow_route.generated_source_kind",
            "unsupported generated source kind",
            "use user_rows, literal_table, calendar, dataframe_source_free_projection, dataframe_generated_with_column, range, or sequence",
        );
    };
    match kind {
        "user_rows"
        | "literal_table"
        | "calendar"
        | "dataframe_source_free_projection"
        | "dataframe_generated_with_column" => {
            if request.generated_schema.is_none() || request.generated_rows.is_none() {
                return generated_source_payload_blocked_route(
                    "public_workflow_route.generated_rows",
                    "generated rows require schema and row payload",
                    "pass --generated-schema and --generated-rows",
                );
            }
            admitted_route(
                "generated_user_rows_direct_output",
                "generated-source-user-rows-smoke",
                "generated_user_rows",
                "generated_rows_boundary",
                "generated_source_output",
                false,
                false,
            )
        }
        "range" | "sequence" => {
            if request.generated_range_start.is_none() || request.generated_range_end.is_none() {
                return generated_source_payload_blocked_route(
                    "public_workflow_route.generated_range",
                    "generated range requires start and end",
                    "pass --generated-range-start and --generated-range-end",
                );
            }
            admitted_route(
                if kind == "sequence" {
                    "generated_sequence_direct_output"
                } else {
                    "generated_range_direct_output"
                },
                if kind == "sequence" {
                    "generated-source-sequence-smoke"
                } else {
                    "generated-source-range-smoke"
                },
                "engine_native_generated_source",
                "generated_rows_boundary",
                "generated_source_output",
                false,
                false,
            )
        }
        _ => unreachable!("normalized generated source kind is exhaustive"),
    }
}

fn normalized_generated_source_kind(request: &PublicWorkflowRouteRequest) -> Option<&'static str> {
    match request
        .generated_source_kind
        .as_deref()?
        .trim()
        .to_ascii_lowercase()
        .replace('-', "_")
        .as_str()
    {
        "user_rows" | "rows" => Some("user_rows"),
        "literal_table" | "literal" => Some("literal_table"),
        "calendar" | "date_dimension" => Some("calendar"),
        "dataframe_projection" | "dataframe_source_free_projection" => {
            Some("dataframe_source_free_projection")
        }
        "dataframe_generated_with_column" | "generated_with_column" => {
            Some("dataframe_generated_with_column")
        }
        "range" => Some("range"),
        "sequence" => Some("sequence"),
        _ => None,
    }
}

fn generated_source_payload_blocked_route(
    field: &'static str,
    reason: &'static str,
    remediation: &'static str,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.generated_source_payload_invalid",
        reason,
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            reason,
            Some(field.to_string()),
            Some(reason.to_string()),
            Some(remediation.to_string()),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn native_vortex_payload_blocked_route(
    field: &'static str,
    reason: &'static str,
    remediation: &'static str,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.native_vortex_payload_invalid",
        reason,
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            reason,
            Some(field.to_string()),
            Some(reason.to_string()),
            Some(remediation.to_string()),
            FallbackStatus::disabled_by_policy(),
        ),
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
        "route requires a declared input, inferred SQL source, source-free SQL write request, or generated-source payload",
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            "public workflow route requires a declared input or generated-source boundary",
            Some("public_workflow_route.input".to_string()),
            Some("no input URI or inferable SQL source was provided".to_string()),
            Some(
                "pass --input with --input-format, route a source-free SQL write request, or pass explicit generated-source payload fields".to_string(),
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
    let effective_request = effective_public_workflow_request(request);
    let mut fields = Vec::with_capacity(40);
    add_route_identity_fields(&mut fields, &effective_request, plan);
    add_route_request_fields(&mut fields, &effective_request, plan);
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
    push_field(fields, "route_support_status", route_support_status(plan));
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
    plan: &PublicWorkflowRoutePlan,
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
    push_field(
        fields,
        "fanout_output_count",
        request.fanout_outputs.len().to_string(),
    );
    push_field(fields, "fanout_outputs", fanout_outputs_field(request));
    push_field(fields, "execution_policy", request.execution_policy.clone());
    push_field(
        fields,
        "materialization_decode_policy",
        request.materialization_policy.clone(),
    );
    push_field(
        fields,
        "evidence_level",
        effective_evidence_level(request, plan),
    );
    push_field(fields, "bounded_request", request.bounded.to_string());
    push_field(
        fields,
        "allow_overwrite",
        request.allow_overwrite.to_string(),
    );
    add_route_generated_source_request_fields(fields, request);
    add_route_native_vortex_request_fields(fields, request, plan);
}

fn add_route_generated_source_request_fields(
    fields: &mut Vec<(String, String)>,
    request: &PublicWorkflowRouteRequest,
) {
    push_field(
        fields,
        "generated_source_kind",
        normalized_generated_source_kind(request)
            .unwrap_or("none")
            .to_string(),
    );
    push_field(
        fields,
        "generated_source_schema_present",
        request.generated_schema.is_some().to_string(),
    );
    push_field(
        fields,
        "generated_source_rows_present",
        request.generated_rows.is_some().to_string(),
    );
    push_field(
        fields,
        "generated_source_range_start",
        request
            .generated_range_start
            .clone()
            .unwrap_or_else(|| "none".to_string()),
    );
    push_field(
        fields,
        "generated_source_range_end",
        request
            .generated_range_end
            .clone()
            .unwrap_or_else(|| "none".to_string()),
    );
    push_field(
        fields,
        "generated_source_range_step",
        request
            .generated_range_step
            .clone()
            .unwrap_or_else(|| "none".to_string()),
    );
    push_field(
        fields,
        "generated_source_range_column",
        request
            .generated_range_column
            .clone()
            .unwrap_or_else(|| "none".to_string()),
    );
}

fn add_route_native_vortex_request_fields(
    fields: &mut Vec<(String, String)>,
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) {
    push_native_vortex_contract_fields(fields, "", request, plan);
    push_field(
        fields,
        "vortex_primitive",
        normalized_vortex_primitive(request).map_or("none", PublicVortexPrimitive::as_str),
    );
    push_field(
        fields,
        "vortex_predicate",
        optional_or_none(request.vortex_predicate.as_ref()),
    );
    push_field(
        fields,
        "vortex_columns",
        optional_or_none(request.vortex_columns.as_ref()),
    );
    push_field(
        fields,
        "vortex_source_order_limit",
        optional_or_none(request.vortex_source_order_limit.as_ref()),
    );
    push_field(
        fields,
        "memory_gb",
        request.memory_gb.clone().unwrap_or_else(|| "1".to_string()),
    );
    push_field(
        fields,
        "max_parallelism",
        request
            .max_parallelism
            .clone()
            .unwrap_or_else(|| "1".to_string()),
    );
}

fn push_native_vortex_contract_fields(
    fields: &mut Vec<(String, String)>,
    prefix: &str,
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) {
    push_field(
        fields,
        format!("{prefix}native_vortex_user_route_contract_schema_version"),
        NATIVE_VORTEX_USER_ROUTE_CONTRACT_SCHEMA_VERSION,
    );
    push_field(
        fields,
        format!("{prefix}typed_result_sink_contract_schema_version"),
        TYPED_RESULT_SINK_CONTRACT_SCHEMA_VERSION,
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_operation_family"),
        native_vortex_operation_family_field(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_provider_scenario"),
        optional_or_none(request.native_vortex_provider_scenario.as_ref()),
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_right_input"),
        optional_or_none(request.native_vortex_right_input.as_ref()),
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_capability_status"),
        native_vortex_capability_status(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_required_evidence"),
        native_vortex_required_evidence(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}native_vortex_next_action"),
        native_vortex_next_action(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}typed_result_contract"),
        typed_result_contract(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}typed_sink_contract"),
        typed_sink_contract(request, plan),
    );
    push_field(
        fields,
        format!("{prefix}decode_materialization_boundary"),
        decode_materialization_boundary(request, plan),
    );
}

fn native_vortex_operation_family_field(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if matches!(
        plan.blocker_id,
        "py-vortex-route-unify-1.native_vortex_sink_contract_missing"
    ) || (is_native_vortex_route(request) && is_write_request(request))
    {
        return NativeVortexOperationFamily::Sink.as_str();
    }
    if let Ok(Some(family)) = normalized_native_vortex_operation_family(request) {
        return family.as_str();
    }
    if let Some(primitive) = normalized_vortex_primitive(request) {
        return NativeVortexOperationFamily::from_primitive(primitive).as_str();
    }
    if is_native_vortex_route(request) {
        return NativeVortexOperationFamily::GeneralQuery.as_str();
    }
    "not_applicable"
}

fn native_vortex_capability_status(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if !is_native_vortex_route(request) {
        return match plan.route_id {
            "local_file_product_query" | "local_file_product_sink" => {
                "not_applicable_compatibility_source_pending_vortex_middle_unification"
            }
            _ => "not_applicable",
        };
    }
    if plan.status == CommandStatus::Success {
        match plan.route_id {
            "native_vortex_filter" | "native_vortex_project" | "native_vortex_filter_project" => {
                "supported_with_materialization_boundary"
            }
            _ => "supported",
        }
    } else if plan.blocker_id.starts_with("py-vortex-route-unify-1.") {
        "blocked_until_native_route_admitted"
    } else {
        "blocked_by_public_route_contract"
    }
}

fn native_vortex_required_evidence(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if matches!(
        plan.blocker_id,
        "py-vortex-route-unify-1.native_vortex_sink_contract_missing"
    ) {
        return NativeVortexOperationFamily::Sink.required_evidence();
    }
    if let Ok(Some(family)) = normalized_native_vortex_operation_family(request) {
        return family.required_evidence();
    }
    if let Some(primitive) = normalized_vortex_primitive(request) {
        return NativeVortexOperationFamily::from_primitive(primitive).required_evidence();
    }
    if !is_native_vortex_route(request) {
        return match plan.route_id {
            "local_file_product_query" | "local_file_product_sink" => {
                "product_local_route_evidence;compatibility_source_state;materialization_boundary;fallback_disabled"
            }
            _ => "not_applicable",
        };
    }
    NativeVortexOperationFamily::GeneralQuery.required_evidence()
}

fn native_vortex_next_action(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if matches!(
        plan.blocker_id,
        "py-vortex-route-unify-1.native_vortex_sink_contract_missing"
    ) {
        return NativeVortexOperationFamily::Sink.next_action();
    }
    if let Ok(Some(family)) = normalized_native_vortex_operation_family(request) {
        return family.next_action();
    }
    if let Some(primitive) = normalized_vortex_primitive(request) {
        return NativeVortexOperationFamily::from_primitive(primitive).next_action();
    }
    if !is_native_vortex_route(request) {
        return match plan.route_id {
            "local_file_product_query" | "local_file_product_sink" => {
                "continue through product local workflow until native Vortex middle is unified"
            }
            _ => "not_applicable",
        };
    }
    NativeVortexOperationFamily::GeneralQuery.next_action()
}

fn typed_result_contract(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if plan.status != CommandStatus::Success {
        return "none_blocked_before_execution";
    }
    match plan.route_id {
        "native_vortex_count_all" | "native_vortex_count_where" => {
            "bounded_python_scalar_summary_with_native_vortex_evidence"
        }
        "native_vortex_filter" | "native_vortex_project" | "native_vortex_filter_project" => {
            "bounded_python_rows_with_explicit_materialization_boundary"
        }
        "native_vortex_user_aggregate"
        | "native_vortex_user_join"
        | "native_vortex_user_top_n"
        | "native_vortex_user_cast"
        | "native_vortex_user_contains" => {
            "provider_backed_native_vortex_result_summary_with_route_certificate"
        }
        "native_vortex_user_sink" => "native_vortex_result_sink_with_replay_certificate",
        "local_file_product_query" if request.requested_output == "collect" => {
            "bounded_python_rows_json_envelope_with_product_local_evidence"
        }
        "local_file_product_sink" => "declared_sink_result_with_product_local_evidence",
        _ => "route_metadata_only",
    }
}

fn typed_sink_contract(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if plan.status != CommandStatus::Success
        && matches!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_sink_contract_missing"
        )
    {
        return "blocked_until_native_vortex_typed_sink_contract";
    }
    if !is_write_request(request) {
        return "not_applicable_collect";
    }
    match plan.route_id {
        "local_file_product_sink" => "declared_compatibility_sink_with_format_specific_output",
        "native_vortex_user_sink" => "native_vortex_result_sink_with_replay_verified_artifact",
        _ => "not_admitted",
    }
}

fn decode_materialization_boundary(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if plan.status != CommandStatus::Success {
        return "not_executed";
    }
    if is_native_vortex_route(request) {
        return "native_vortex_zero_decode_runtime_with_bounded_python_materialization_boundary";
    }
    match plan.route_id {
        "local_file_product_query" | "local_file_product_sink" => {
            "compatibility_source_runtime_materialization_boundary_pre_vortex_middle_unification"
        }
        "local_file_prepare_once" | "local_file_prepare_once_first_query" => {
            "prepared_vortex_state_boundary"
        }
        _ => "not_applicable",
    }
}

fn add_route_execution_fields(fields: &mut Vec<(String, String)>, plan: &PublicWorkflowRoutePlan) {
    push_field(fields, "start_state", plan.start_state);
    push_field(
        fields,
        "vortex_normalization_point",
        plan.vortex_normalization_point,
    );
    push_field(fields, "vortex_middle_status", vortex_middle_status(plan));
    push_field(
        fields,
        "underlying_runtime_command",
        underlying_runtime_command(plan),
    );
    push_field(
        fields,
        "local_workflow_runtime_profile",
        local_workflow_runtime_profile(plan),
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
        ("route_runtime_status", route_runtime_status(plan)),
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

fn effective_evidence_level<'a>(
    request: &'a PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'a str {
    if matches!(
        plan.route_id,
        "local_file_product_query" | "local_file_product_sink"
    ) && request.evidence_level == "runtime_smoke"
    {
        "production_admitted_local_workflow"
    } else {
        request.evidence_level.as_str()
    }
}

fn add_route_boundary_fields(fields: &mut Vec<(String, String)>, plan: &PublicWorkflowRoutePlan) {
    push_field(fields, "blocker_id", plan.blocker_id);
    push_field(fields, "blocker_reason", plan.blocker_reason);
    push_field(fields, "claim_boundary", CLAIM_BOUNDARY);
    push_field(fields, "fallback_boundary", FALLBACK_BOUNDARY);
    push_field(fields, "claim_gate_status", "route_inspection_only");
}

fn route_support_status(plan: &PublicWorkflowRoutePlan) -> &'static str {
    match plan.route_id {
        "local_file_product_query" | "local_file_product_sink" => {
            "production_admitted_local_workflow"
        }
        "local_file_prepare_once"
        | "local_file_prepare_once_first_query"
        | "native_vortex_count_all"
        | "native_vortex_count_where"
        | "native_vortex_filter"
        | "native_vortex_project"
        | "native_vortex_filter_project"
        | "native_vortex_user_aggregate"
        | "native_vortex_user_join"
        | "native_vortex_user_top_n"
        | "native_vortex_user_cast"
        | "native_vortex_user_contains"
        | "native_vortex_user_sink"
        | "source_free_generated_output"
        | "generated_user_rows_direct_output"
        | "generated_range_direct_output"
        | "generated_sequence_direct_output" => "scoped_runtime_supported",
        _ => "unsupported_boundary",
    }
}

fn route_runtime_status(plan: &PublicWorkflowRoutePlan) -> &'static str {
    if plan.status == CommandStatus::Success {
        route_support_status(plan)
    } else {
        "blocked_before_execution"
    }
}

fn vortex_middle_status(plan: &PublicWorkflowRoutePlan) -> &'static str {
    match plan.route_id {
        "local_file_product_query" | "local_file_product_sink" => {
            "pending_native_vortex_middle_unification"
        }
        "local_file_prepare_once" | "local_file_prepare_once_first_query" => {
            "prepared_vortex_state"
        }
        "native_vortex_count_all"
        | "native_vortex_count_where"
        | "native_vortex_filter"
        | "native_vortex_project"
        | "native_vortex_filter_project" => "native_vortex_primitive",
        "native_vortex_user_aggregate"
        | "native_vortex_user_join"
        | "native_vortex_user_top_n"
        | "native_vortex_user_cast"
        | "native_vortex_user_contains"
        | "native_vortex_user_sink" => "native_vortex_user_operator_provider",
        "source_free_generated_output"
        | "generated_user_rows_direct_output"
        | "generated_range_direct_output"
        | "generated_sequence_direct_output" => "not_applicable_source_free",
        _ => "blocked_or_unsupported",
    }
}

fn underlying_runtime_command(plan: &PublicWorkflowRoutePlan) -> &'static str {
    match plan.route_id {
        "local_file_product_query" | "local_file_product_sink" => "sql-local-source-smoke",
        _ => plan.resolved_internal_command,
    }
}

fn local_workflow_runtime_profile(plan: &PublicWorkflowRoutePlan) -> &'static str {
    match plan.route_id {
        "local_file_product_query" | "local_file_product_sink" => "product_local_workflow",
        _ => "not_applicable",
    }
}

fn push_field(
    fields: &mut Vec<(String, String)>,
    key: impl Into<String>,
    value: impl Into<String>,
) {
    fields.push((key.into(), value.into()));
}

fn optional_or_none(value: Option<&String>) -> String {
    value.cloned().unwrap_or_else(|| "none".to_string())
}

fn fanout_outputs_field(request: &PublicWorkflowRouteRequest) -> String {
    if request.fanout_outputs.is_empty() {
        "none".to_string()
    } else {
        request.fanout_outputs.join(";")
    }
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
    let effective_request = effective_public_workflow_request(request);
    let mut fields = vec![
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
            effective_request.surface.clone(),
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
            "public_workflow_route_support_status".to_string(),
            route_support_status(plan).to_string(),
        ),
        (
            "public_workflow_route_runtime_status".to_string(),
            route_runtime_status(plan).to_string(),
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
            "public_workflow_vortex_middle_status".to_string(),
            vortex_middle_status(plan).to_string(),
        ),
        (
            "public_workflow_underlying_runtime_command".to_string(),
            underlying_runtime_command(plan).to_string(),
        ),
        (
            "public_workflow_local_workflow_runtime_profile".to_string(),
            local_workflow_runtime_profile(plan).to_string(),
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
            effective_request.requested_output.clone(),
        ),
        (
            "public_workflow_output_ref".to_string(),
            optional_or_none(effective_request.output_ref.as_ref()),
        ),
        (
            "public_workflow_fanout_output_count".to_string(),
            effective_request.fanout_outputs.len().to_string(),
        ),
        (
            "public_workflow_fanout_outputs".to_string(),
            fanout_outputs_field(&effective_request),
        ),
        (
            "public_workflow_evidence_level".to_string(),
            effective_evidence_level(&effective_request, plan).to_string(),
        ),
        (
            "public_workflow_bounded_request".to_string(),
            effective_request.bounded.to_string(),
        ),
        (
            "public_workflow_allow_overwrite".to_string(),
            effective_request.allow_overwrite.to_string(),
        ),
        (
            "public_workflow_generated_source_kind".to_string(),
            normalized_generated_source_kind(&effective_request)
                .unwrap_or("none")
                .to_string(),
        ),
        (
            "public_workflow_generated_source_schema_present".to_string(),
            effective_request.generated_schema.is_some().to_string(),
        ),
        (
            "public_workflow_generated_source_rows_present".to_string(),
            effective_request.generated_rows.is_some().to_string(),
        ),
        (
            "public_workflow_vortex_primitive".to_string(),
            normalized_vortex_primitive(&effective_request)
                .map_or("none", PublicVortexPrimitive::as_str)
                .to_string(),
        ),
        (
            "public_workflow_vortex_predicate".to_string(),
            optional_or_none(effective_request.vortex_predicate.as_ref()),
        ),
        (
            "public_workflow_vortex_columns".to_string(),
            optional_or_none(effective_request.vortex_columns.as_ref()),
        ),
        (
            "public_workflow_vortex_source_order_limit".to_string(),
            optional_or_none(effective_request.vortex_source_order_limit.as_ref()),
        ),
        (
            "public_workflow_memory_gb".to_string(),
            effective_request
                .memory_gb
                .clone()
                .unwrap_or_else(|| "1".to_string()),
        ),
        (
            "public_workflow_max_parallelism".to_string(),
            effective_request
                .max_parallelism
                .clone()
                .unwrap_or_else(|| "1".to_string()),
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
    ];
    push_native_vortex_contract_fields(&mut fields, "public_workflow_", &effective_request, plan);
    fields
}

fn sql_local_source_runtime_args(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    statement: String,
) -> Result<Vec<String>, ShardLoomError> {
    let mut args = vec![statement];
    append_declared_local_input_format_args(&mut args, request);
    if let Some(output_ref) = request.output_ref.as_ref() {
        args.extend([
            "--output".to_string(),
            output_ref.clone(),
            "--output-format".to_string(),
            local_output_format_for_request(request)?.to_string(),
        ]);
    }
    append_fanout_args(&mut args, request);
    if request.allow_overwrite {
        args.push("--allow-overwrite".to_string());
    }
    if matches!(
        plan.route_id,
        "local_file_product_query" | "local_file_product_sink"
    ) {
        args.push("--product-local-workflow".to_string());
    }
    Ok(args)
}

fn append_declared_local_input_format_args(
    args: &mut Vec<String>,
    request: &PublicWorkflowRouteRequest,
) {
    if let Some(input_format) = request
        .input_format
        .as_deref()
        .filter(|format| is_local_file_format(format))
    {
        args.extend(["--input-format".to_string(), input_format.to_string()]);
    }
}

fn generated_source_runtime_args(
    request: &PublicWorkflowRouteRequest,
    output_ref: String,
    statement: String,
) -> Vec<String> {
    let mut args = vec![
        output_ref,
        statement,
        "--output-format".to_string(),
        local_output_format_for_request(request)
            .unwrap_or("jsonl")
            .to_string(),
    ];
    append_fanout_args(&mut args, request);
    if request.allow_overwrite {
        args.push("--allow-overwrite".to_string());
    }
    args
}

fn generated_user_rows_runtime_args(
    request: &PublicWorkflowRouteRequest,
    output_ref: String,
) -> Result<Vec<String>, ShardLoomError> {
    let kind = normalized_generated_source_kind(request).ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "public workflow generated-source run requires a supported generated source kind"
                .to_string(),
        )
    })?;
    let schema = request.generated_schema.clone().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "public workflow generated-source user rows run requires --generated-schema"
                .to_string(),
        )
    })?;
    let rows = request.generated_rows.clone().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "public workflow generated-source user rows run requires --generated-rows".to_string(),
        )
    })?;
    let mut args = vec![
        output_ref,
        schema,
        rows,
        "--source-kind".to_string(),
        kind.to_string(),
        "--output-format".to_string(),
        local_output_format_for_request(request)?.to_string(),
    ];
    append_fanout_args(&mut args, request);
    if request.allow_overwrite {
        args.push("--allow-overwrite".to_string());
    }
    Ok(args)
}

fn generated_range_runtime_args(
    request: &PublicWorkflowRouteRequest,
    output_ref: String,
) -> Result<Vec<String>, ShardLoomError> {
    let start = request.generated_range_start.clone().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "public workflow generated-source range run requires --generated-range-start"
                .to_string(),
        )
    })?;
    let end = request.generated_range_end.clone().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "public workflow generated-source range run requires --generated-range-end".to_string(),
        )
    })?;
    let step = request
        .generated_range_step
        .clone()
        .unwrap_or_else(|| "1".to_string());
    let column = request
        .generated_range_column
        .clone()
        .unwrap_or_else(|| "value".to_string());
    let mut args = vec![
        output_ref,
        start,
        end,
        "--step".to_string(),
        step,
        "--column".to_string(),
        column,
        "--output-format".to_string(),
        local_output_format_for_request(request)?.to_string(),
    ];
    append_fanout_args(&mut args, request);
    if request.allow_overwrite {
        args.push("--allow-overwrite".to_string());
    }
    Ok(args)
}

fn append_fanout_args(args: &mut Vec<String>, request: &PublicWorkflowRouteRequest) {
    for fanout_output in &request.fanout_outputs {
        args.extend(["--fanout-output".to_string(), fanout_output.clone()]);
    }
}

fn native_vortex_primitive_runtime_args(
    request: &PublicWorkflowRouteRequest,
    primitive: PublicVortexPrimitive,
) -> Result<Vec<String>, ShardLoomError> {
    let input_uri = request.input_uri.clone().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "public native Vortex run requires --input with a Vortex dataset".to_string(),
        )
    })?;
    let memory_gb = positive_u64_arg("memory_gb", request.memory_gb.as_deref().unwrap_or("1"))?;
    let max_parallelism = positive_usize_arg(
        "max_parallelism",
        request.max_parallelism.as_deref().unwrap_or("1"),
    )?;
    let mut args = match primitive {
        PublicVortexPrimitive::Count => vec![
            input_uri,
            "count".to_string(),
            memory_gb.to_string(),
            max_parallelism.to_string(),
        ],
        PublicVortexPrimitive::CountWhere => vec![
            input_uri,
            required_native_vortex_payload(request.vortex_predicate.as_ref(), "vortex predicate")?,
            "--execute-local-primitive".to_string(),
            memory_gb.to_string(),
            max_parallelism.to_string(),
        ],
        PublicVortexPrimitive::Filter => vec![
            input_uri,
            required_native_vortex_payload(request.vortex_predicate.as_ref(), "vortex predicate")?,
        ],
        PublicVortexPrimitive::Project => vec![
            input_uri,
            required_native_vortex_payload(request.vortex_columns.as_ref(), "vortex columns")?,
        ],
        PublicVortexPrimitive::FilterProject => vec![
            input_uri,
            required_native_vortex_payload(request.vortex_predicate.as_ref(), "vortex predicate")?,
            required_native_vortex_payload(request.vortex_columns.as_ref(), "vortex columns")?,
        ],
    };
    if primitive.allows_source_order_limit()
        && let Some(limit) = request.vortex_source_order_limit.as_ref()
    {
        args.extend([
            "--limit".to_string(),
            positive_usize_arg("source-order limit", limit)?.to_string(),
        ]);
    }
    if !matches!(
        primitive,
        PublicVortexPrimitive::Count | PublicVortexPrimitive::CountWhere
    ) {
        args.extend([
            "--execute-local-primitive".to_string(),
            memory_gb.to_string(),
            max_parallelism.to_string(),
        ]);
    }
    Ok(args)
}

fn required_native_vortex_payload(
    value: Option<&String>,
    label: &str,
) -> Result<String, ShardLoomError> {
    value
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "public native Vortex run requires {label}; fallback execution was not attempted"
            ))
        })
}

fn positive_u64_arg(label: &str, value: &str) -> Result<u64, ShardLoomError> {
    let parsed = value.parse::<u64>().map_err(|_| {
        ShardLoomError::InvalidOperation(format!("{label} must be an unsigned integer"))
    })?;
    if parsed == 0 {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{label} must be >= 1"
        )));
    }
    Ok(parsed)
}

fn positive_usize_arg(label: &str, value: &str) -> Result<usize, ShardLoomError> {
    let parsed = value.parse::<usize>().map_err(|_| {
        ShardLoomError::InvalidOperation(format!("{label} must be an unsigned integer"))
    })?;
    if parsed == 0 {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{label} must be >= 1"
        )));
    }
    Ok(parsed)
}

fn local_output_format_for_request(
    request: &PublicWorkflowRouteRequest,
) -> Result<&'static str, ShardLoomError> {
    match request.requested_output.as_str() {
        "collect" => Ok("inline-jsonl"),
        "write_vortex" => Ok("vortex"),
        "write_parquet" => Ok("parquet"),
        "write_arrow_ipc" => Ok("arrow-ipc"),
        "write_avro" => Ok("avro"),
        "write_orc" => Ok("orc"),
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
        "collect" | "prepare" | "write_vortex" | "write_parquet" | "write_arrow_ipc"
        | "write_avro" | "write_orc" | "write_csv" | "write_jsonl" | "explain" | "route"
        | "evidence" | "profile" => Ok(normalized),
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
        "report_only" | "runtime_smoke" | "production_admitted_local_workflow" | "claim_grade" => {
            Ok(normalized)
        }
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
    for keyword in ["FROM", "JOIN"] {
        let mut search_start = 0usize;
        while search_start < statement.len() {
            let Some(relative_index) =
                find_sql_keyword_outside_quotes_and_parens(&statement[search_start..], keyword)
            else {
                break;
            };
            let index = search_start + relative_index + keyword.len();
            if let Some(candidate) = leading_quoted_sql_literal(&statement[index..]) {
                let candidate = candidate.trim();
                if infer_input_format_from_ref(candidate).is_some() {
                    return Some(candidate.to_string());
                }
            }
            search_start = index;
        }
    }
    None
}

fn sql_statement_has_limit(statement: &str) -> bool {
    find_sql_keyword_outside_quotes_and_parens(statement, "LIMIT").is_some()
}

fn plan_summary_has_limit(summary: &str) -> bool {
    summary.to_ascii_lowercase().contains("limit(")
}

fn is_source_free_sql_statement(statement: &str) -> bool {
    let normalized = statement.trim().trim_end_matches(';').trim();
    if sql_keyword_prefix(normalized, "VALUES") {
        return true;
    }
    if !sql_keyword_prefix(normalized, "SELECT") {
        return false;
    }

    let select_body = normalized["SELECT".len()..].trim();
    let Some(from_position) = find_sql_keyword_outside_quotes_and_parens(select_body, "FROM")
    else {
        return true;
    };
    let source_ref = select_body[from_position + "FROM".len()..].trim();
    is_source_free_generator_source_ref(source_ref)
}

fn is_source_free_generator_source_ref(source_ref: &str) -> bool {
    let trimmed = trim_sql_tail_clauses(source_ref).trim();
    let lower = trimmed.to_ascii_lowercase();
    (lower.starts_with("range(")
        || lower.starts_with("range (")
        || lower.starts_with("generate_series(")
        || lower.starts_with("generate_series ("))
        && lower.ends_with(')')
}

fn trim_sql_tail_clauses(raw: &str) -> &str {
    let tail_position = ["WHERE", "ORDER BY", "LIMIT"]
        .iter()
        .filter_map(|keyword| find_sql_keyword_outside_quotes_and_parens(raw, keyword))
        .min();
    tail_position.map_or(raw, |position| &raw[..position])
}

fn sql_keyword_prefix(raw: &str, keyword: &str) -> bool {
    let trimmed = trim_sql_leading_comments_and_whitespace(raw);
    let Some(prefix) = trimmed.get(..keyword.len()) else {
        return false;
    };
    prefix.eq_ignore_ascii_case(keyword)
        && trimmed
            .as_bytes()
            .get(keyword.len())
            .is_none_or(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_')
}

fn find_sql_keyword_outside_quotes_and_parens(raw: &str, keyword: &str) -> Option<usize> {
    let bytes = raw.as_bytes();
    let keyword_bytes = keyword.as_bytes();
    let mut quote_char: Option<u8> = None;
    let mut paren_depth = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        if quote_char.is_none() {
            if bytes[index] == b'-' && bytes.get(index + 1) == Some(&b'-') {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
                continue;
            }
            if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
                index += 2;
                while index + 1 < bytes.len() && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                {
                    index += 1;
                }
                index = (index + 2).min(bytes.len());
                continue;
            }
        }
        match bytes[index] {
            b'\'' | b'"' => {
                let byte = bytes[index];
                if quote_char == Some(byte) {
                    if byte == b'\'' && index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                        index += 2;
                        continue;
                    }
                    quote_char = None;
                } else if quote_char.is_none() {
                    quote_char = Some(byte);
                }
            }
            b'(' if quote_char.is_none() => paren_depth += 1,
            b')' if quote_char.is_none() && paren_depth > 0 => paren_depth -= 1,
            _ if quote_char.is_none()
                && paren_depth == 0
                && index + keyword_bytes.len() <= bytes.len()
                && bytes[index..index + keyword_bytes.len()]
                    .eq_ignore_ascii_case(keyword_bytes)
                && index
                    .checked_sub(1)
                    .and_then(|before| bytes.get(before))
                    .is_none_or(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_')
                && bytes
                    .get(index + keyword_bytes.len())
                    .is_none_or(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_') =>
            {
                return Some(index);
            }
            _ => {}
        }
        index += 1;
    }
    None
}

fn trim_sql_leading_comments_and_whitespace(mut raw: &str) -> &str {
    loop {
        let trimmed = raw.trim_start();
        if let Some(rest) = trimmed.strip_prefix("--") {
            let next_line = rest.find('\n').map_or("", |index| &rest[index + 1..]);
            raw = next_line;
            continue;
        }
        if let Some(rest) = trimmed.strip_prefix("/*") {
            let after_comment = rest.find("*/").map_or("", |index| &rest[index + 2..]);
            raw = after_comment;
            continue;
        }
        return trimmed;
    }
}

fn leading_quoted_sql_literal(raw: &str) -> Option<String> {
    let raw = trim_sql_leading_comments_and_whitespace(raw);
    let mut chars = raw.char_indices();
    let (_, quote_char) = chars.next()?;
    if quote_char != '\'' && quote_char != '"' {
        return None;
    }
    let mut literal = String::new();
    let mut last_index = quote_char.len_utf8();
    while last_index < raw.len() {
        let mut iter = raw[last_index..].char_indices();
        let (relative_index, char) = iter.next()?;
        let index = last_index + relative_index;
        if char == quote_char {
            let next_index = index + char.len_utf8();
            if quote_char == '\'' && raw[next_index..].starts_with('\'') {
                literal.push('\'');
                last_index = next_index + 1;
                continue;
            }
            return Some(literal);
        }
        literal.push(char);
        last_index = index + char.len_utf8();
    }
    None
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

        assert_eq!(sql_plan.route_id, "local_file_product_query");
        assert_eq!(dataframe_plan.route_id, sql_plan.route_id);
        assert_eq!(
            dataframe_plan.resolved_internal_command,
            "local-workflow-run"
        );
        assert_eq!(
            dataframe_plan.vortex_normalization_point,
            "compatibility_source_state_pre_vortex_unification"
        );
        assert_eq!(dataframe_plan.execution_mode, "product_local");
        assert!(!dataframe_plan.preparation_included);
        assert!(!dataframe_plan.query_timing_starts_after_preparation);
    }

    #[test]
    fn product_local_file_route_appends_product_runtime_profile_flag() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "target/input.csv",
                "--input-format",
                "csv",
                "--sql",
                "SELECT id FROM 'target/input.csv' LIMIT 10",
                "--plan",
                "read_csv(target/input.csv) -> select(id) -> limit(10)",
                "--request",
                "collect",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("dataframe run request");
        let plan = plan_public_workflow_route(&request);
        let args =
            sql_local_source_runtime_args(&request, &plan, request.sql_statement.clone().unwrap())
                .expect("runtime args");
        let fields = route_fields(&request, &plan);

        assert_eq!(plan.route_id, "local_file_product_query");
        assert_eq!(
            field(&fields, "route_support_status"),
            "production_admitted_local_workflow"
        );
        assert_eq!(
            field(&fields, "vortex_middle_status"),
            "pending_native_vortex_middle_unification"
        );
        assert_eq!(
            field(&fields, "underlying_runtime_command"),
            "sql-local-source-smoke"
        );
        assert_eq!(
            field(&fields, "local_workflow_runtime_profile"),
            "product_local_workflow"
        );
        assert!(args.iter().any(|arg| arg == "--product-local-workflow"));
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
            "local_file_product_query"
        );
        assert_eq!(plan_public_workflow_route(&blocked).route_id, "blocked");
    }

    #[test]
    fn route_planner_does_not_infer_scalar_path_literals_as_sources() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT 'target/input.csv' AS label LIMIT 1",
                "--request",
                "collect",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("scalar literal route request");

        let plan = plan_public_workflow_route(&request);

        assert_eq!(request.input_uri, None);
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(plan.blocker_id, "cg21.route.input_not_declared");
    }

    #[test]
    fn route_planner_ignores_limit_inside_sql_comments() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT id FROM 'target/input.csv' -- LIMIT 1\nWHERE id > 0",
                "--request",
                "collect",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("commented limit route request");

        let plan = plan_public_workflow_route(&request);

        assert_eq!(request.input_uri.as_deref(), Some("target/input.csv"));
        assert!(!request.bounded);
        assert_eq!(plan.blocker_id, "cg21.route.unbounded_collect_blocked");
    }

    #[test]
    fn route_planner_blocks_newline_from_source_without_declared_input() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT *\nFROM events",
                "--request",
                "write_vortex",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("newline from route request");

        let plan = plan_public_workflow_route(&request);

        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(plan.blocker_id, "cg21.route.input_not_declared");
    }

    #[test]
    fn route_planner_requires_vortex_input_for_native_vortex_policy() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "cli",
                "--input",
                "target/input.csv",
                "--input-format",
                "csv",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("native policy route request");

        let plan = plan_public_workflow_route(&request);

        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(plan.blocker_id, "cg21.route.native_vortex_input_required");
    }

    #[test]
    fn route_planner_blocks_collect_fanout_payloads() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "target/input.csv",
                "--input-format",
                "csv",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--fanout-output",
                "csv=target/out.csv",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("collect fanout route request");

        let plan = plan_public_workflow_route(&request);

        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(plan.blocker_id, "cg21.route.collect_fanout_blocked");
    }

    #[test]
    fn route_planner_blocks_native_vortex_output_payloads() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "cli",
                "--input",
                "target/input.vortex",
                "--input-format",
                "vortex",
                "--request",
                "write_jsonl",
                "--output",
                "target/out.jsonl",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
                "--vortex-primitive",
                "count",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("native output route request");

        let plan = plan_public_workflow_route(&request);

        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_sink_contract_missing"
        );
        let fields = route_fields(&request, &plan);
        assert_eq!(field(&fields, "native_vortex_operation_family"), "sink");
        assert_eq!(
            field(&fields, "typed_sink_contract"),
            "blocked_until_native_vortex_typed_sink_contract"
        );
    }

    #[test]
    fn route_planner_attaches_native_vortex_contract_for_primitive() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "target/input.vortex",
                "--input-format",
                "vortex",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
                "--native-vortex-operation-family",
                "filter_project_limit",
                "--vortex-primitive",
                "filter_project",
                "--vortex-predicate",
                "metric >= 10",
                "--vortex-columns",
                "id,metric",
                "--vortex-source-order-limit",
                "100",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("native primitive route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);
        let attachments = execution_attachment_fields("run", &request, &plan);

        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.route_id, "native_vortex_filter_project");
        assert_eq!(
            field(&fields, "native_vortex_user_route_contract_schema_version"),
            NATIVE_VORTEX_USER_ROUTE_CONTRACT_SCHEMA_VERSION
        );
        assert_eq!(
            field(&fields, "native_vortex_operation_family"),
            "filter_project_limit"
        );
        assert_eq!(
            field(&fields, "native_vortex_capability_status"),
            "supported_with_materialization_boundary"
        );
        assert_eq!(
            field(&fields, "typed_result_contract"),
            "bounded_python_rows_with_explicit_materialization_boundary"
        );
        assert_eq!(
            field(&fields, "decode_materialization_boundary"),
            "native_vortex_zero_decode_runtime_with_bounded_python_materialization_boundary"
        );
        assert_eq!(
            field(
                &attachments,
                "public_workflow_native_vortex_operation_family"
            ),
            "filter_project_limit"
        );
    }

    #[test]
    fn route_planner_infers_payloadless_native_vortex_primitive_facade_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "orders.vortex",
                "--input-format",
                "vortex",
                "--plan",
                "read_vortex(orders.vortex) -> filter(gte:value:3) -> select(metric,value) -> limit(5)",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("payloadless native primitive route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);
        let attachments = execution_attachment_fields("run", &request, &plan);

        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.route_id, "native_vortex_filter_project");
        assert_eq!(
            field(&fields, "route_runtime_status"),
            "scoped_runtime_supported"
        );
        assert_eq!(field(&fields, "vortex_primitive"), "filter_project");
        assert_eq!(
            field(&fields, "native_vortex_operation_family"),
            "filter_project_limit"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_primitive"),
            "filter_project"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_predicate"),
            "gte:value:3"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_columns"),
            "metric,value"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_source_order_limit"),
            "5"
        );
    }

    #[test]
    fn route_planner_does_not_infer_primitive_from_partial_plan_summary() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "fact.vortex",
                "--input-format",
                "vortex",
                "--plan",
                "read_vortex(fact.vortex) -> join(dim.vortex,dim_key,dim_key,inner,f,d,) -> filter(gte:value:3)",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("partial native primitive route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);

        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_general_route_missing"
        );
        assert_eq!(field(&fields, "vortex_primitive"), "none");
        assert_eq!(field(&fields, "native_vortex_provider_scenario"), "none");
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[cfg(not(feature = "vortex-traditional-analytics-benchmark"))]
    #[test]
    fn route_planner_feature_gates_native_vortex_provider_routes() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "target/fact.vortex",
                "--input-format",
                "vortex",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
                "--native-vortex-operation-family",
                "aggregate",
                "--native-vortex-provider-scenario",
                "group-by-aggregation",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("native provider route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);

        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_provider_feature_gated"
        );
        assert_eq!(
            field(&fields, "native_vortex_provider_scenario"),
            "group-by-aggregation"
        );
        assert_eq!(
            field(&fields, "native_vortex_capability_status"),
            "blocked_until_native_route_admitted"
        );
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn route_planner_admits_native_vortex_provider_operator_routes() {
        let aggregate = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "target/fact.vortex",
                "--input-format",
                "vortex",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
                "--native-vortex-operation-family",
                "aggregate",
                "--native-vortex-provider-scenario",
                "group-by-aggregation",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("aggregate provider route request");
        let join = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "target/fact.vortex",
                "--input-format",
                "vortex",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
                "--native-vortex-operation-family",
                "join",
                "--native-vortex-provider-scenario",
                "hash-join",
                "--native-vortex-right-input",
                "target/dim.vortex",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("join provider route request");
        let sink = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "target/fact.vortex",
                "--input-format",
                "vortex",
                "--request",
                "write_vortex",
                "--output",
                "target/out.vortex",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
                "--native-vortex-operation-family",
                "sink",
                "--native-vortex-provider-scenario",
                "clean-cast-filter-write",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("sink provider route request");

        let aggregate_plan = plan_public_workflow_route(&aggregate);
        let join_plan = plan_public_workflow_route(&join);
        let sink_plan = plan_public_workflow_route(&sink);
        let aggregate_fields = route_fields(&aggregate, &aggregate_plan);
        let join_fields = route_fields(&join, &join_plan);
        let sink_fields = route_fields(&sink, &sink_plan);

        assert_eq!(aggregate_plan.status, CommandStatus::Success);
        assert_eq!(aggregate_plan.route_id, "native_vortex_user_aggregate");
        assert_eq!(
            field(&aggregate_fields, "route_runtime_status"),
            "scoped_runtime_supported"
        );
        assert_eq!(
            field(&aggregate_fields, "vortex_middle_status"),
            "native_vortex_user_operator_provider"
        );
        assert_eq!(
            aggregate_plan.resolved_internal_command,
            "traditional-analytics-vortex-run"
        );
        assert_eq!(
            field(&aggregate_fields, "typed_result_contract"),
            "provider_backed_native_vortex_result_summary_with_route_certificate"
        );
        assert_eq!(join_plan.status, CommandStatus::Success);
        assert_eq!(join_plan.route_id, "native_vortex_user_join");
        assert_eq!(
            field(&join_fields, "native_vortex_right_input"),
            "target/dim.vortex"
        );
        assert_eq!(sink_plan.status, CommandStatus::Success);
        assert_eq!(sink_plan.route_id, "native_vortex_user_sink");
        assert_eq!(
            field(&sink_fields, "typed_sink_contract"),
            "native_vortex_result_sink_with_replay_verified_artifact"
        );
        assert_eq!(field(&sink_fields, "fallback_attempted"), "false");
        assert_eq!(field(&sink_fields, "external_engine_invoked"), "false");
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn route_planner_infers_payloadless_native_vortex_provider_facade_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "fact.vortex",
                "--input-format",
                "vortex",
                "--plan",
                "read_vortex(fact.vortex) -> group_by(group_key) -> aggregate(count(*) AS rows,sum(metric) AS total_metric) -> limit(100)",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("payloadless native provider route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);
        let attachments = execution_attachment_fields("run", &request, &plan);

        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.route_id, "native_vortex_user_aggregate");
        assert_eq!(
            field(&fields, "route_runtime_status"),
            "scoped_runtime_supported"
        );
        assert_eq!(
            field(&fields, "native_vortex_provider_scenario"),
            "group-by-aggregation"
        );
        assert_eq!(
            field(
                &attachments,
                "public_workflow_native_vortex_provider_scenario"
            ),
            "group-by-aggregation"
        );
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn route_planner_does_not_infer_filtered_group_by_provider_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "fact.vortex",
                "--input-format",
                "vortex",
                "--plan",
                "read_vortex(fact.vortex) -> filter(metric >= 0) -> group_by(group_key) -> aggregate(count(*) AS rows,sum(metric) AS total_metric) -> limit(100)",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("filtered group-by provider route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);

        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_general_route_missing"
        );
        assert_eq!(field(&fields, "native_vortex_provider_scenario"), "none");
        assert_eq!(field(&fields, "resolved_internal_command"), "not_resolved");
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn route_planner_does_not_infer_top_n_provider_with_noncanonical_limit() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "fact.vortex",
                "--input-format",
                "vortex",
                "--plan",
                "read_vortex(fact.vortex) -> select(id,group_key,metric) -> sort(desc,metric) -> limit(1)",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("top-N provider route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);

        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_general_route_missing"
        );
        assert_eq!(field(&fields, "native_vortex_provider_scenario"), "none");
        assert_eq!(field(&fields, "resolved_internal_command"), "not_resolved");
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn route_planner_does_not_infer_provider_from_sql_literals() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--input",
                "orders.vortex",
                "--input-format",
                "vortex",
                "--sql",
                "SELECT 'count(*) AS rows', 'sum(metric) AS total_metric', 'WHERE metric >= 0', 'GROUP BY group_key' FROM 'orders.vortex' LIMIT 1",
                "--plan",
                "read_vortex(orders.vortex) -> filter(metric >= 0) -> group_by(group_key) -> aggregate(count(*) AS rows,sum(metric) AS total_metric) -> limit(100)",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("literal bait SQL route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);

        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_general_route_missing"
        );
        assert_eq!(field(&fields, "native_vortex_provider_scenario"), "none");
        assert_eq!(field(&fields, "resolved_internal_command"), "not_resolved");
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn route_planner_does_not_infer_provider_for_report_requests() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "fact.vortex",
                "--input-format",
                "vortex",
                "--plan",
                "read_vortex(fact.vortex) -> filter(metric >= 0) -> group_by(group_key) -> aggregate(count(*) AS rows,sum(metric) AS total_metric) -> limit(100)",
                "--request",
                "explain",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("payloadless report route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);

        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_general_route_missing"
        );
        assert_eq!(field(&fields, "native_vortex_provider_scenario"), "none");
        assert_eq!(field(&fields, "resolved_internal_command"), "not_resolved");
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[cfg(feature = "vortex-traditional-analytics-benchmark")]
    #[test]
    fn route_planner_does_not_infer_provider_from_quoted_plan_arguments() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "fact.vortex",
                "--input-format",
                "vortex",
                "--plan",
                "read_vortex(fact.vortex) -> filter('metric >= 0') -> group_by(group_key) -> aggregate('count(*) AS rows','sum(metric) AS total_metric') -> limit(1)",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("quoted payloadless provider route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);

        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_general_route_missing"
        );
        assert_eq!(field(&fields, "native_vortex_provider_scenario"), "none");
        assert_eq!(field(&fields, "resolved_internal_command"), "not_resolved");
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_blocks_native_vortex_aggregate_family_with_contract() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--input",
                "target/input.vortex",
                "--input-format",
                "vortex",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
                "--native-vortex-operation-family",
                "aggregate",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("native aggregate route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);

        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_aggregate_route_missing"
        );
        assert_eq!(
            field(&fields, "native_vortex_operation_family"),
            "aggregate"
        );
        assert_eq!(
            field(&fields, "native_vortex_capability_status"),
            "blocked_until_native_route_admitted"
        );
        assert_eq!(
            field(&fields, "typed_result_contract"),
            "none_blocked_before_execution"
        );
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_blocks_unshaped_native_vortex_query_before_execution() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "target/input.vortex",
                "--input-format",
                "vortex",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("native direct query route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);

        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_general_route_missing"
        );
        assert_eq!(
            field(&fields, "native_vortex_operation_family"),
            "general_query"
        );
        assert_eq!(
            field(&fields, "native_vortex_next_action"),
            "declare an operation family and route only admitted native Vortex primitive or promoted operator families"
        );
    }

    #[test]
    fn runtime_args_forward_declared_local_input_format() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT id FROM 'target/input' LIMIT 1",
                "--input-format",
                "csv",
                "--request",
                "collect",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("declared input format route request");
        let plan = plan_public_workflow_route(&request);
        let args = sql_local_source_runtime_args(
            &request,
            &plan,
            request
                .sql_statement
                .clone()
                .expect("request carries SQL statement"),
        )
        .expect("runtime args");

        assert_eq!(
            args,
            vec![
                "SELECT id FROM 'target/input' LIMIT 1".to_string(),
                "--input-format".to_string(),
                "csv".to_string(),
                "--product-local-workflow".to_string(),
            ]
        );
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
