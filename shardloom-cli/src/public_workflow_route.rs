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
    vortex_primitive_execution,
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
    allow_overwrite: bool,
    generated_source_kind: Option<String>,
    generated_schema: Option<String>,
    generated_rows: Option<String>,
    generated_range_start: Option<String>,
    generated_range_end: Option<String>,
    generated_range_step: Option<String>,
    generated_range_column: Option<String>,
    fanout_outputs: Vec<String>,
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
        _ => {
            let blocked = run_route_not_executable_yet(&plan);
            emit_blocked_facade("run", format, &request, &blocked)
        }
    }
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
                "usage: shardloom route <sql|python|dataframe|cli> [--input <uri>] [--input-format <format>] [--sql <statement>] [--plan <summary>] [--request <collect|prepare|write_vortex|write_parquet|write_arrow_ipc|write_avro|write_orc|write_csv|write_jsonl|explain|route|evidence>] [--output <ref>] [--fanout-output <format=local-path>]... [--execution-policy <auto|direct|native_vortex|prepare_once>] [--materialization-policy <bounded|materialized|zero_decode|explicit>] [--evidence-level <report_only|runtime_smoke|claim_grade>] [--bounded true|false] [--allow-overwrite] [--generated-source-kind <kind>] [--generated-schema <schema>] [--generated-rows <rows>] [--generated-range-start <int>] [--generated-range-end <int>] [--generated-range-step <int>] [--generated-range-column <name>] [--vortex-primitive <count|count_where|filter|project|filter_project>] [--vortex-predicate <tiny-predicate>] [--vortex-columns <columns>] [--vortex-source-order-limit <rows>] [--memory-gb <n>] [--max-parallelism <n>]"
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

fn native_vortex_route(request: &PublicWorkflowRouteRequest) -> PublicWorkflowRoutePlan {
    if request.input_uri.is_none() {
        return input_not_declared_route();
    }
    if is_write_request(request)
        || request.output_ref.is_some()
        || !request.fanout_outputs.is_empty()
    {
        return native_vortex_output_blocked_route();
    }
    let Some(primitive) = normalized_vortex_primitive(request) else {
        if request.vortex_primitive.is_some() {
            return native_vortex_payload_blocked_route(
                "public_workflow_route.vortex_primitive",
                "unsupported native Vortex primitive",
                "use count, count_where, filter, project, or filter_project",
            );
        }
        return admitted_route(
            "native_vortex_direct_query",
            "vortex-run",
            "native_vortex_file",
            "native_vortex_boundary",
            "native_vortex",
            false,
            true,
        );
    };
    if primitive.requires_predicate() && request.vortex_predicate.is_none() {
        return native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_predicate",
            "native Vortex primitive requires a predicate payload",
            "pass --vortex-predicate with the scoped tiny predicate expression",
        );
    }
    if primitive.requires_columns() && request.vortex_columns.is_none() {
        return native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_columns",
            "native Vortex primitive requires a projection payload",
            "pass --vortex-columns with comma-separated projected columns",
        );
    }
    if request.vortex_source_order_limit.is_some() && !primitive.allows_source_order_limit() {
        return native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_source_order_limit",
            "native Vortex primitive does not admit a source-order limit",
            "use --vortex-source-order-limit only with filter, project, or filter_project",
        );
    }
    if let Some(error) = positive_u64_option_error("--memory-gb", request.memory_gb.as_deref()) {
        return native_vortex_payload_blocked_route(
            "public_workflow_route.memory_gb",
            error,
            "pass --memory-gb with an integer >= 1",
        );
    }
    if let Some(error) =
        positive_usize_option_error("--max-parallelism", request.max_parallelism.as_deref())
    {
        return native_vortex_payload_blocked_route(
            "public_workflow_route.max_parallelism",
            error,
            "pass --max-parallelism with an integer >= 1",
        );
    }
    if let Some(error) = positive_usize_option_error(
        "--vortex-source-order-limit",
        request.vortex_source_order_limit.as_deref(),
    ) {
        return native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_source_order_limit",
            error,
            "pass --vortex-source-order-limit with an integer >= 1",
        );
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

fn native_vortex_output_blocked_route() -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.native_vortex_output_not_admitted",
        "native Vortex primitive facade does not admit write or fanout outputs yet",
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            "native Vortex public primitive route cannot execute output payloads".to_string(),
            Some("public_workflow_route.output".to_string()),
            Some("output_ref, write requests, and fanout outputs are not wired to native Vortex primitive wrappers".to_string()),
            Some("use collect for admitted native Vortex primitives or an explicit compatibility sink route".to_string()),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn normalized_vortex_primitive(
    request: &PublicWorkflowRouteRequest,
) -> Option<PublicVortexPrimitive> {
    request
        .vortex_primitive
        .as_deref()
        .and_then(PublicVortexPrimitive::parse)
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
    push_field(fields, "evidence_level", request.evidence_level.clone());
    push_field(fields, "bounded_request", request.bounded.to_string());
    push_field(
        fields,
        "allow_overwrite",
        request.allow_overwrite.to_string(),
    );
    add_route_generated_source_request_fields(fields, request);
    add_route_native_vortex_request_fields(fields, request);
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
) {
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
            "public_workflow_fanout_output_count".to_string(),
            request.fanout_outputs.len().to_string(),
        ),
        (
            "public_workflow_fanout_outputs".to_string(),
            fanout_outputs_field(request),
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
            "public_workflow_allow_overwrite".to_string(),
            request.allow_overwrite.to_string(),
        ),
        (
            "public_workflow_generated_source_kind".to_string(),
            normalized_generated_source_kind(request)
                .unwrap_or("none")
                .to_string(),
        ),
        (
            "public_workflow_generated_source_schema_present".to_string(),
            request.generated_schema.is_some().to_string(),
        ),
        (
            "public_workflow_generated_source_rows_present".to_string(),
            request.generated_rows.is_some().to_string(),
        ),
        (
            "public_workflow_vortex_primitive".to_string(),
            normalized_vortex_primitive(request)
                .map_or("none", PublicVortexPrimitive::as_str)
                .to_string(),
        ),
        (
            "public_workflow_vortex_predicate".to_string(),
            optional_or_none(request.vortex_predicate.as_ref()),
        ),
        (
            "public_workflow_vortex_columns".to_string(),
            optional_or_none(request.vortex_columns.as_ref()),
        ),
        (
            "public_workflow_vortex_source_order_limit".to_string(),
            optional_or_none(request.vortex_source_order_limit.as_ref()),
        ),
        (
            "public_workflow_memory_gb".to_string(),
            request.memory_gb.clone().unwrap_or_else(|| "1".to_string()),
        ),
        (
            "public_workflow_max_parallelism".to_string(),
            request
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
    ]
}

fn sql_local_source_runtime_args(
    request: &PublicWorkflowRouteRequest,
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
    if primitive.allows_source_order_limit() {
        if let Some(limit) = request.vortex_source_order_limit.as_ref() {
            args.extend([
                "--limit".to_string(),
                positive_usize_arg("source-order limit", limit)?.to_string(),
            ]);
        }
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
            "cg21.route.native_vortex_output_not_admitted"
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
        let args = sql_local_source_runtime_args(
            &request,
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
