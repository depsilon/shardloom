//! Public workflow route facade.
//!
//! This module is the side-effect-free narrow waist for user-facing route
//! inspection. It resolves a declared SQL/Python/DataFrame/CLI workflow into a
//! ShardLoom-internal command route or a deterministic blocker before any
//! runtime command is allowed to execute.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    process::ExitCode,
};

use shardloom_core::{
    CommandStatus, DatasetUri, Diagnostic, DiagnosticCategory, DiagnosticCode, DiagnosticSeverity,
    FallbackStatus, OutputFormat, ShardLoomError,
};

use crate::{
    benchmark_runtime,
    cli_output::{emit, emit_error},
    cli_unknown_arg_error, generated_source_runtime, sql_local_source_runtime, vortex_planning,
    vortex_primitive_execution,
};

const ROUTE_SCHEMA_VERSION: &str = "shardloom.public_workflow_route.v1";
const FACADE_SCHEMA_VERSION: &str = "shardloom.public_workflow_execution_facade.v1";
const NATIVE_VORTEX_USER_ROUTE_CONTRACT_SCHEMA_VERSION: &str =
    "shardloom.native_vortex_user_route_contract.v1";
const TYPED_RESULT_SINK_CONTRACT_SCHEMA_VERSION: &str = "shardloom.typed_result_sink_contract.v1";
const ROUTE_REPORT_ID: &str = "gar-runtime-impl-6d.public_workflow_route_facade";
const ROUTE_DOCS_REF: &str = "docs/status/cli-command-registry.md#public-route-facade-command";
const VORTEX_PRODUCTION_RUNTIME_COMMAND: &str = "vortex-production-runtime-run";
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
    vortex_sample_seed: Option<String>,
    vortex_sample_fraction: Option<String>,
    vortex_sample_replacement: bool,
    vortex_duplicate_keep: Option<String>,
    vortex_expression_projection: Option<String>,
    vortex_melt_projection: Option<String>,
    vortex_explode_projection: Option<String>,
    vortex_pivot_projection: Option<String>,
    vortex_rolling_window: Option<String>,
    vortex_aggregate: Option<String>,
    vortex_sort_rows: Option<String>,
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

type PublicWorkflowRoutePlanResult<T> = Result<T, Box<PublicWorkflowRoutePlan>>;

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
        | "native_vortex_filter_project"
        | "native_vortex_distinct"
        | "native_vortex_duplicate_mask"
        | "native_vortex_tail"
        | "native_vortex_sample"
        | "native_vortex_expression_project"
        | "native_vortex_melt"
        | "native_vortex_explode"
        | "native_vortex_pivot"
        | "native_vortex_rolling_window"
        | "native_vortex_aggregate"
        | "native_vortex_sort_rows" => execute_native_vortex_primitive_run(&request, &plan, format),
        "native_vortex_user_aggregate"
        | "native_vortex_user_join"
        | "native_vortex_user_top_n"
        | "native_vortex_user_cast"
        | "native_vortex_user_contains"
        | "native_vortex_user_sink" => execute_native_vortex_provider_run(&request, &plan, format),
        "native_vortex_user_profile" => execute_native_vortex_profile_run(&request, &plan, format),
        "native_vortex_primitive_row_export" => {
            execute_native_vortex_primitive_row_export_run(&request, &plan, format)
        }
        "local_file_prepare_once_first_query" => {
            execute_local_file_prepare_once_first_query_run(&request, &plan, format)
        }
        _ => {
            let blocked = run_route_not_executable_yet(&plan);
            emit_blocked_facade("run", format, &request, &blocked)
        }
    }
}

fn execute_native_vortex_profile_run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
) -> ExitCode {
    execute_native_vortex_profile_run_with_extra(request, plan, format, Vec::new())
}

fn execute_native_vortex_profile_run_with_extra(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    mut extra_fields: Vec<(String, String)>,
) -> ExitCode {
    let Some(input_uri) = request.input_uri.clone() else {
        let blocked = input_not_declared_route();
        return emit_blocked_facade("run", format, request, &blocked);
    };
    let mut attachment_fields = execution_attachment_fields("run", request, plan);
    attachment_fields.extend(native_vortex_profile_projection_attachment_fields(request));
    attachment_fields.append(&mut extra_fields);
    vortex_planning::handle_vortex_metadata_summary_with_facade(
        vec![input_uri].into_iter(),
        format,
        "run",
        attachment_fields,
    )
}

fn execute_native_vortex_primitive_row_export_run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
) -> ExitCode {
    execute_native_vortex_primitive_row_export_run_with_extra(request, plan, format, Vec::new())
}

fn execute_native_vortex_primitive_row_export_run_with_extra(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    mut extra_fields: Vec<(String, String)>,
) -> ExitCode {
    let Some(input_uri) = request.input_uri.clone() else {
        let blocked = input_not_declared_route();
        return emit_blocked_facade("run", format, request, &blocked);
    };
    let targets = match native_vortex_primitive_row_export_targets(request, "run") {
        Ok(targets) => targets,
        Err(blocked) => return emit_blocked_facade("run", format, request, blocked.as_ref()),
    };
    let Some(primitive) = normalized_vortex_primitive(request) else {
        let blocked = native_vortex_operation_blocked_route(NativeVortexOperationFamily::Sink);
        return emit_blocked_facade("run", format, request, &blocked);
    };
    let execution =
        match native_vortex_primitive_row_export_execution(request, input_uri, primitive) {
            Ok(execution) => execution,
            Err(error) => {
                return emit_error(
                    "run",
                    format,
                    "native Vortex primitive row export failed",
                    &error,
                );
            }
        };
    let reports =
        match execute_native_vortex_primitive_row_export_reports(request, &targets, &execution) {
            Ok(reports) => reports,
            Err(error) => {
                return emit_error(
                    "run",
                    format,
                    "native Vortex primitive row export failed",
                    &error,
                );
            }
        };
    let mut fields = execution_attachment_fields("run", request, plan);
    fields.append(&mut extra_fields);
    let primary_report = reports
        .first()
        .expect("native Vortex primitive row export has a primary target");
    append_native_vortex_primitive_row_export_fields(&mut fields, primary_report);
    append_native_vortex_primitive_row_export_target_fields(&mut fields, &targets, &reports);
    let has_errors = reports
        .iter()
        .any(shardloom_vortex::VortexLocalPrimitiveRowExportReport::has_errors);
    let diagnostics = reports
        .iter()
        .flat_map(|report| report.diagnostics.clone())
        .collect::<Vec<_>>();
    emit(
        "run",
        format,
        if has_errors {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "native Vortex primitive row export".to_string(),
        format!(
            "native Vortex primitive row export wrote {} {} rows to {} across {} target(s)",
            primary_report.rows_written,
            primary_report.output_format,
            primary_report.output_path,
            reports.len()
        ),
        diagnostics,
        fields,
    );
    if has_errors {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

struct NativeVortexPrimitiveRowExportExecution {
    primitive_request: shardloom_vortex::VortexQueryPrimitiveRequest,
    policy: shardloom_vortex::VortexLocalPrimitiveExecutionPolicy,
}

fn native_vortex_primitive_row_export_execution(
    request: &PublicWorkflowRouteRequest,
    input_uri: String,
    primitive: PublicVortexPrimitive,
) -> Result<NativeVortexPrimitiveRowExportExecution, ShardLoomError> {
    let uri = shardloom_core::DatasetUri::new(input_uri)?;
    let primitive_arg = native_vortex_primitive_arg_for_request(request, primitive)?;
    let mut primitive_request =
        vortex_primitive_execution::parse_vortex_primitive_request(uri, &primitive_arg)?;
    if let Some(limit) = request.vortex_source_order_limit.as_deref() {
        primitive_request = primitive_request
            .with_source_order_limit(positive_usize_arg("source-order limit", limit)?);
    }
    if let Some(seed) = request.vortex_sample_seed.as_ref() {
        primitive_request =
            primitive_request.with_sample_seed(non_negative_u64_arg("sample seed", seed)?);
    }
    if let Some(fraction) = request.vortex_sample_fraction.as_ref() {
        primitive_request = primitive_request
            .with_sample_fraction(sample_fraction_arg("sample fraction", fraction)?);
    }
    primitive_request =
        primitive_request.with_sample_replacement(request.vortex_sample_replacement);
    primitive_request = primitive_request.with_duplicate_keep(duplicate_keep_policy_arg(
        request.vortex_duplicate_keep.as_deref(),
    )?);
    let policy = shardloom_vortex::VortexLocalPrimitiveExecutionPolicy::new(
        request
            .max_parallelism
            .as_deref()
            .unwrap_or("1")
            .parse::<usize>()
            .unwrap_or(0),
    )?;
    Ok(NativeVortexPrimitiveRowExportExecution {
        primitive_request,
        policy,
    })
}

fn execute_native_vortex_primitive_row_export_reports(
    request: &PublicWorkflowRouteRequest,
    targets: &[NativeVortexPrimitiveRowExportTarget],
    execution: &NativeVortexPrimitiveRowExportExecution,
) -> Result<Vec<shardloom_vortex::VortexLocalPrimitiveRowExportReport>, ShardLoomError> {
    targets
        .iter()
        .map(|target| {
            shardloom_vortex::execute_vortex_local_primitive_row_export_with_policy(
                &execution.primitive_request,
                &target.path,
                target.format,
                request.allow_overwrite,
                execution.policy,
            )
        })
        .collect()
}

#[derive(Debug, Clone)]
struct NativeVortexPrimitiveRowExportTarget {
    role: &'static str,
    format: shardloom_vortex::VortexLocalPrimitiveRowExportFormat,
    path: PathBuf,
}

fn native_vortex_primitive_row_export_targets(
    request: &PublicWorkflowRouteRequest,
    command: &'static str,
) -> PublicWorkflowRoutePlanResult<Vec<NativeVortexPrimitiveRowExportTarget>> {
    let Some(output_ref) = request.output_ref.as_deref() else {
        return Err(Box::new(output_required_route(
            command,
            "native Vortex primitive row export",
        )));
    };
    if output_ref.trim().is_empty() {
        return Err(Box::new(output_required_route(
            command,
            "native Vortex primitive row export",
        )));
    }
    let primary_format = native_vortex_row_export_format_for_output_request(request)?;
    let mut targets = vec![NativeVortexPrimitiveRowExportTarget {
        role: "primary",
        format: primary_format,
        path: PathBuf::from(output_ref),
    }];
    for fanout_output in &request.fanout_outputs {
        targets.push(native_vortex_parse_row_export_fanout_target(fanout_output)?);
    }
    native_vortex_preflight_row_export_targets(&targets, request.allow_overwrite)?;
    Ok(targets)
}

fn native_vortex_row_export_format_for_output_request(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRoutePlanResult<shardloom_vortex::VortexLocalPrimitiveRowExportFormat> {
    match request.requested_output.as_str() {
        "write_jsonl" => Ok(shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Jsonl),
        "write_csv" => Ok(shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Csv),
        _ => Err(Box::new(native_vortex_sink_format_blocked_route(request))),
    }
}

fn native_vortex_parse_row_export_fanout_target(
    value: &str,
) -> PublicWorkflowRoutePlanResult<NativeVortexPrimitiveRowExportTarget> {
    let Some((format_raw, path_raw)) = value.split_once('=') else {
        return Err(native_vortex_row_export_fanout_blocked_route(
            "py-vortex-route-unify-1.native_vortex_fanout_payload_invalid",
            "native Vortex primitive row-export fanout requires format=local-path",
            format!("fanout_payload={value}"),
            "pass --fanout-output jsonl=path or --fanout-output csv=path",
        ));
    };
    if path_raw.trim().is_empty() {
        return Err(native_vortex_row_export_fanout_blocked_route(
            "py-vortex-route-unify-1.native_vortex_fanout_payload_invalid",
            "native Vortex primitive row-export fanout path is empty",
            format!("fanout_payload={value}"),
            "pass --fanout-output jsonl=path or --fanout-output csv=path",
        ));
    }
    let format = match format_raw.trim().to_ascii_lowercase().as_str() {
        "jsonl" => shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Jsonl,
        "csv" => shardloom_vortex::VortexLocalPrimitiveRowExportFormat::Csv,
        _ => {
            return Err(native_vortex_row_export_fanout_blocked_route(
                "py-vortex-route-unify-1.native_vortex_fanout_sink_format_missing",
                "native Vortex primitive row-export fanout supports JSONL and CSV only",
                format!("fanout_format={format_raw} admitted_fanout_formats=jsonl,csv"),
                "use jsonl=path or csv=path fanout targets for native Vortex primitive row export",
            ));
        }
    };
    Ok(NativeVortexPrimitiveRowExportTarget {
        role: "fanout",
        format,
        path: PathBuf::from(path_raw),
    })
}

fn native_vortex_preflight_row_export_targets(
    targets: &[NativeVortexPrimitiveRowExportTarget],
    allow_overwrite: bool,
) -> PublicWorkflowRoutePlanResult<()> {
    let mut seen = BTreeSet::new();
    for target in targets {
        let workspace_root = shardloom_core::infer_local_output_workspace_root(&target.path)
            .map_err(|error| native_vortex_row_export_output_safety_blocked_route(&error))?;
        let write_plan = shardloom_core::plan_workspace_safe_local_output(
            workspace_root,
            &target.path,
            allow_overwrite,
        )
        .map_err(|error| native_vortex_row_export_output_safety_blocked_route(&error))?;
        let key = write_plan
            .target_path
            .to_string_lossy()
            .replace('\\', "/")
            .to_ascii_lowercase();
        if !seen.insert(key) {
            return Err(native_vortex_row_export_fanout_blocked_route(
                "py-vortex-route-unify-1.native_vortex_fanout_duplicate_output",
                "native Vortex primitive row-export fanout output path is duplicated",
                format!("duplicate_output_path={}", target.path.display()),
                "choose distinct primary and fanout output paths",
            ));
        }
    }
    Ok(())
}

fn native_vortex_row_export_fanout_blocked_route(
    blocker_id: &'static str,
    message: &'static str,
    reason: String,
    next_action: &'static str,
) -> Box<PublicWorkflowRoutePlan> {
    Box::new(blocked_route(
        blocker_id,
        message,
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            message.to_string(),
            Some("public_workflow_route.fanout_outputs".to_string()),
            Some(reason),
            Some(next_action.to_string()),
            FallbackStatus::disabled_by_policy(),
        ),
    ))
}

fn native_vortex_row_export_output_safety_blocked_route(
    error: &ShardLoomError,
) -> Box<PublicWorkflowRoutePlan> {
    Box::new(blocked_route(
        "py-vortex-route-unify-1.native_vortex_row_export_output_path_unsafe",
        "native Vortex primitive row-export output path failed workspace-safety validation",
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::InvalidInput,
            "native Vortex primitive row-export output path failed workspace-safety validation"
                .to_string(),
            Some("public_workflow_route.output_ref".to_string()),
            Some(error.to_string()),
            Some("choose a local output path within the inferred workspace and retry".to_string()),
            FallbackStatus::disabled_by_policy(),
        ),
    ))
}

#[allow(clippy::too_many_lines)]
fn native_vortex_primitive_arg_for_request(
    request: &PublicWorkflowRouteRequest,
    primitive: PublicVortexPrimitive,
) -> Result<String, ShardLoomError> {
    match primitive {
        PublicVortexPrimitive::Filter => Ok(format!(
            "filter:{}",
            required_native_vortex_payload(request.vortex_predicate.as_ref(), "vortex predicate")?
        )),
        PublicVortexPrimitive::Project => Ok(format!(
            "project:{}",
            required_native_vortex_payload(request.vortex_columns.as_ref(), "vortex columns")?
        )),
        PublicVortexPrimitive::FilterProject => Ok(format!(
            "filter-project:{}|{}",
            required_native_vortex_payload(request.vortex_predicate.as_ref(), "vortex predicate")?,
            required_native_vortex_payload(request.vortex_columns.as_ref(), "vortex columns")?
        )),
        PublicVortexPrimitive::Distinct => {
            let columns = request
                .vortex_columns
                .as_ref()
                .map_or("*", String::as_str);
            if let Some(predicate) = request.vortex_predicate.as_ref() {
                Ok(format!("distinct-filter-project:{predicate}|{columns}"))
            } else {
                Ok(format!("distinct:{columns}"))
            }
        }
        PublicVortexPrimitive::DuplicateMask => Ok(format!(
            "duplicate-mask:{}",
            required_native_vortex_payload(request.vortex_columns.as_ref(), "vortex columns")?
        )),
        PublicVortexPrimitive::Tail => {
            let columns = request
                .vortex_columns
                .as_ref()
                .map_or("*", String::as_str);
            Ok(format!("tail:{columns}"))
        }
        PublicVortexPrimitive::Sample => {
            let columns = request
                .vortex_columns
                .as_ref()
                .map_or("*", String::as_str);
            if let Some(predicate) = request.vortex_predicate.as_ref() {
                Ok(format!("sample-filter-project:{predicate}|{columns}"))
            } else {
                Ok(format!("sample:{columns}"))
            }
        }
        PublicVortexPrimitive::ExpressionProject => Ok(format!(
            "expression-project:{}",
            required_native_vortex_payload(
                request.vortex_expression_projection.as_ref(),
                "vortex expression projection"
            )?
        )),
        PublicVortexPrimitive::Melt => Ok(format!(
            "melt:{}",
            required_native_vortex_payload(
                request.vortex_melt_projection.as_ref(),
                "vortex melt projection"
            )?
        )),
        PublicVortexPrimitive::Explode => Ok(format!(
            "explode:{}",
            required_native_vortex_payload(
                request.vortex_explode_projection.as_ref(),
                "vortex explode projection"
            )?
        )),
        PublicVortexPrimitive::Pivot => Ok(format!(
            "pivot:{}",
            required_native_vortex_payload(
                request.vortex_pivot_projection.as_ref(),
                "vortex pivot projection"
            )?
        )),
        PublicVortexPrimitive::RollingWindow => Ok(format!(
            "rolling:{}",
            required_native_vortex_payload(
                request.vortex_rolling_window.as_ref(),
                "vortex rolling window"
            )?
        )),
        PublicVortexPrimitive::Aggregate => {
            let aggregate =
                required_native_vortex_payload(request.vortex_aggregate.as_ref(), "vortex aggregate")?;
            if let Some(predicate) = request.vortex_predicate.as_ref() {
                Ok(format!("aggregate-filter:{predicate}|{aggregate}"))
            } else {
                Ok(format!("aggregate:{aggregate}"))
            }
        }
        PublicVortexPrimitive::SortRows => {
            let sort_rows =
                required_native_vortex_payload(request.vortex_sort_rows.as_ref(), "vortex sort rows")?;
            let columns = request
                .vortex_columns
                .as_ref()
                .map_or("*", String::as_str);
            if let Some(predicate) = request.vortex_predicate.as_ref() {
                Ok(format!("sort-filter-project:{predicate}|{columns}|{sort_rows}"))
            } else {
                Ok(format!(
                    "sort-rows:{}",
                    sort_rows_payload_with_columns(&sort_rows, request.vortex_columns.as_deref())?
                ))
            }
        }
        PublicVortexPrimitive::Count | PublicVortexPrimitive::CountWhere => Err(
            ShardLoomError::InvalidOperation(
                "native Vortex primitive row export supports filter, project, filter-project, distinct, duplicate-mask, tail, sample, expression-project, melt, explode, pivot, rolling-window, aggregate, and sort-row primitives only"
                    .to_string(),
            ),
        ),
    }
}

fn sort_rows_payload_with_columns(
    payload: &str,
    columns: Option<&str>,
) -> Result<String, ShardLoomError> {
    let Some(columns) = columns else {
        return Ok(payload.to_string());
    };
    let mut value = serde_json::from_str::<serde_json::Value>(payload).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "native Vortex sort rows payload must be valid JSON: {error}"
        ))
    })?;
    let object = value.as_object_mut().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "native Vortex sort rows payload must be a JSON object".to_string(),
        )
    })?;
    object.insert(
        "columns".to_string(),
        serde_json::Value::String(columns.to_string()),
    );
    Ok(value.to_string())
}

fn append_native_vortex_primitive_row_export_fields(
    fields: &mut Vec<(String, String)>,
    report: &shardloom_vortex::VortexLocalPrimitiveRowExportReport,
) {
    push_field(fields, "mode", "native_vortex_primitive_row_export");
    push_field(
        fields,
        "execution",
        "native_vortex_primitive_row_export_performed",
    );
    push_field(
        fields,
        "native_vortex_result_export_kind",
        "primitive_row_stream",
    );
    push_field(
        fields,
        "native_vortex_result_export_format",
        report.output_format,
    );
    push_field(
        fields,
        "native_vortex_result_export_path",
        &report.output_path,
    );
    push_field(
        fields,
        "native_vortex_result_export_rows_written",
        report.rows_written.to_string(),
    );
    push_field(
        fields,
        "native_vortex_result_export_projected_columns",
        report.projected_columns.join(","),
    );
    push_field(
        fields,
        "typed_sink_contract",
        "native_vortex_primitive_row_stream_to_jsonl_csv_compatibility_sink",
    );
    push_field(
        fields,
        "decode_materialization_boundary",
        "native_vortex_scan_pushdown_then_selected_column_decode_at_compatibility_sink",
    );
    push_bool_field(fields, "data_read", report.evidence.side_effects.data_read);
    push_bool_field(
        fields,
        "data_decoded",
        report.evidence.side_effects.data_decoded,
    );
    push_bool_field(
        fields,
        "data_materialized",
        report.evidence.side_effects.data_materialized,
    );
    push_bool_field(
        fields,
        "upstream_vortex_scan_called",
        report.evidence.upstream_scan_called,
    );
    push_bool_field(fields, "row_read", report.evidence.side_effects.row_read);
    push_bool_field(
        fields,
        "arrow_converted",
        report.evidence.side_effects.arrow_converted,
    );
    push_bool_field(fields, "runtime_execution", true);
    push_bool_field(
        fields,
        "output_io_performed",
        report.evidence.side_effects.write_io,
    );
    push_bool_field(fields, "write_io", report.evidence.side_effects.write_io);
    push_bool_field(fields, "fallback_attempted", false);
    push_bool_field(fields, "external_engine_invoked", false);
    push_bool_field(
        fields,
        "materialization_boundary_reported",
        report.evidence.materialization_boundary_reported,
    );
    push_field(
        fields,
        "local_primitive_state_budget_schema_version",
        &report.state_budget.schema_version,
    );
    push_bool_field(
        fields,
        "local_primitive_state_budget_required",
        report.state_budget.state_budget_required,
    );
    push_field(
        fields,
        "local_primitive_state_budget_status",
        &report.state_budget.state_budget_status,
    );
    push_field(
        fields,
        "local_primitive_state_family",
        &report.state_budget.state_family,
    );
    push_field(
        fields,
        "local_primitive_capillary_work_units",
        report.state_budget.capillary_work_units.join(","),
    );
    push_field(
        fields,
        "local_primitive_pulseweave_pressure_signals",
        report.state_budget.pulseweave_pressure_signals.join(","),
    );
    push_field(
        fields,
        "local_primitive_observed_state_items",
        report.state_budget.observed_state_items.to_string(),
    );
    push_field(
        fields,
        "local_primitive_estimated_state_items",
        report
            .state_budget
            .estimated_state_items
            .map_or_else(|| "none".to_string(), |value| value.to_string()),
    );
    push_field(
        fields,
        "local_primitive_spill_policy",
        &report.state_budget.spill_policy,
    );
    push_bool_field(
        fields,
        "local_primitive_spill_required",
        report.state_budget.spill_required,
    );
    push_bool_field(
        fields,
        "local_primitive_spill_supported",
        report.state_budget.spill_supported,
    );
    push_bool_field(
        fields,
        "local_primitive_fail_closed_if_spill_required",
        report.state_budget.fail_closed_if_spill_required,
    );
    push_field(
        fields,
        "local_primitive_state_budget_diagnostic_code",
        &report.state_budget.diagnostic_code,
    );
    push_field(fields, "claim_gate_status", "not_claim_grade");
}

fn append_native_vortex_primitive_row_export_target_fields(
    fields: &mut Vec<(String, String)>,
    targets: &[NativeVortexPrimitiveRowExportTarget],
    reports: &[shardloom_vortex::VortexLocalPrimitiveRowExportReport],
) {
    push_field(
        fields,
        "native_vortex_result_export_target_count",
        targets.len().to_string(),
    );
    push_field(
        fields,
        "native_vortex_result_export_fanout_count",
        targets.len().saturating_sub(1).to_string(),
    );
    push_bool_field(
        fields,
        "native_vortex_result_export_fanout_performed",
        targets.len() > 1,
    );
    push_field(
        fields,
        "native_vortex_result_export_target_roles",
        targets
            .iter()
            .map(|target| target.role)
            .collect::<Vec<_>>()
            .join(","),
    );
    push_field(
        fields,
        "native_vortex_result_export_target_formats",
        targets
            .iter()
            .map(|target| target.format.as_str())
            .collect::<Vec<_>>()
            .join(","),
    );
    push_field(
        fields,
        "native_vortex_result_export_target_paths",
        targets
            .iter()
            .map(|target| target.path.display().to_string())
            .collect::<Vec<_>>()
            .join(","),
    );
    push_field(
        fields,
        "native_vortex_result_export_target_rows_written",
        reports
            .iter()
            .map(|report| report.rows_written.to_string())
            .collect::<Vec<_>>()
            .join(","),
    );
}

fn execute_native_vortex_provider_run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
) -> ExitCode {
    execute_native_vortex_provider_run_with_extra(request, plan, format, Vec::new())
}

fn execute_native_vortex_provider_run_with_extra(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    mut extra_fields: Vec<(String, String)>,
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
        match request.requested_output.as_str() {
            "write_vortex" => {
                runtime_args.extend(["--workspace".to_string(), output_ref]);
                runtime_args.push("--write-result-vortex".to_string());
            }
            "write_jsonl" | "write_csv" => {
                runtime_args.extend([
                    "--result-output".to_string(),
                    output_ref,
                    "--result-output-format".to_string(),
                    local_output_format_for_request(request)
                        .unwrap_or("jsonl")
                        .to_string(),
                ]);
            }
            _ => {
                let blocked = native_vortex_sink_format_blocked_route(request);
                return emit_blocked_facade("run", format, request, &blocked);
            }
        }
        for fanout_output in &request.fanout_outputs {
            runtime_args.extend(["--fanout-output".to_string(), fanout_output.clone()]);
        }
        if request.allow_overwrite {
            runtime_args.push("--allow-overwrite".to_string());
        }
    }
    runtime_args.extend(["--execution-mode".to_string(), "native_vortex".to_string()]);
    if let Some(memory_gb) = request.memory_gb.clone() {
        runtime_args.extend(["--memory-gb".to_string(), memory_gb]);
    }
    if let Some(max_parallelism) = request.max_parallelism.clone() {
        runtime_args.extend(["--max-parallelism".to_string(), max_parallelism]);
    }
    let mut attachment_fields = execution_attachment_fields("run", request, plan);
    attachment_fields.append(&mut extra_fields);
    benchmark_runtime::handle_traditional_analytics_vortex_run_with_facade(
        runtime_args.into_iter(),
        format,
        "run",
        attachment_fields,
    )
}

fn execute_native_vortex_primitive_run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
) -> ExitCode {
    execute_native_vortex_primitive_run_with_extra(request, plan, format, Vec::new())
}

fn execute_native_vortex_primitive_run_with_extra(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    mut extra_fields: Vec<(String, String)>,
) -> ExitCode {
    let Some(primitive) = normalized_vortex_primitive(request) else {
        let blocked = native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_primitive",
            "public native Vortex run requires a primitive payload",
            "pass --vortex-primitive with count, count_where, filter, project, filter_project, distinct, duplicate_mask, tail, sample, expression_project, melt, explode, pivot, rolling_window, aggregate, or sort_rows",
        );
        return emit_blocked_facade("run", format, request, &blocked);
    };
    if matches!(
        primitive,
        PublicVortexPrimitive::Distinct
            | PublicVortexPrimitive::DuplicateMask
            | PublicVortexPrimitive::Tail
            | PublicVortexPrimitive::Sample
            | PublicVortexPrimitive::ExpressionProject
            | PublicVortexPrimitive::Melt
            | PublicVortexPrimitive::Explode
            | PublicVortexPrimitive::Pivot
            | PublicVortexPrimitive::RollingWindow
            | PublicVortexPrimitive::Aggregate
            | PublicVortexPrimitive::SortRows
    ) {
        return execute_native_vortex_materializing_primitive_run_with_extra(
            request,
            plan,
            format,
            extra_fields,
            primitive,
        );
    }
    let runtime_args = match native_vortex_primitive_runtime_args(request, primitive) {
        Ok(args) => args,
        Err(error) => {
            return emit_error("run", format, "public native Vortex run failed", &error);
        }
    };
    let mut attachment_fields = execution_attachment_fields("run", request, plan);
    attachment_fields.append(&mut extra_fields);
    match primitive {
        PublicVortexPrimitive::Count => vortex_primitive_execution::handle_vortex_run_with_facade(
            runtime_args.into_iter(),
            format,
            "run",
            attachment_fields,
        ),
        PublicVortexPrimitive::CountWhere => {
            vortex_primitive_execution::handle_vortex_count_where_with_facade(
                runtime_args.into_iter(),
                format,
                "run",
                attachment_fields,
            )
        }
        PublicVortexPrimitive::Filter => {
            vortex_primitive_execution::handle_vortex_filter_with_facade(
                runtime_args.into_iter(),
                format,
                "run",
                attachment_fields,
            )
        }
        PublicVortexPrimitive::Project => {
            vortex_primitive_execution::handle_vortex_project_with_facade(
                runtime_args.into_iter(),
                format,
                "run",
                attachment_fields,
            )
        }
        PublicVortexPrimitive::FilterProject => {
            vortex_primitive_execution::handle_vortex_filter_project_with_facade(
                runtime_args.into_iter(),
                format,
                "run",
                attachment_fields,
            )
        }
        PublicVortexPrimitive::Distinct
        | PublicVortexPrimitive::DuplicateMask
        | PublicVortexPrimitive::Tail
        | PublicVortexPrimitive::Sample
        | PublicVortexPrimitive::ExpressionProject
        | PublicVortexPrimitive::Melt
        | PublicVortexPrimitive::Explode
        | PublicVortexPrimitive::Pivot
        | PublicVortexPrimitive::RollingWindow
        | PublicVortexPrimitive::Aggregate
        | PublicVortexPrimitive::SortRows => {
            unreachable!("handled before runtime args")
        }
    }
}

fn execute_native_vortex_materializing_primitive_run_with_extra(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    mut extra_fields: Vec<(String, String)>,
    primitive: PublicVortexPrimitive,
) -> ExitCode {
    let (primitive_request, primitive_arg) =
        match native_vortex_materializing_request_and_arg(request, primitive) {
            Ok(value) => value,
            Err(error) => return native_vortex_materializing_error(format, primitive, &error),
        };
    let policy = match native_vortex_materializing_policy(request) {
        Ok(policy) => policy,
        Err(error) => return native_vortex_materializing_error(format, primitive, &error),
    };
    let report = match shardloom_vortex::execute_vortex_local_primitive_with_policy(
        &primitive_request,
        policy,
    ) {
        Ok(report) => report,
        Err(error) => return native_vortex_materializing_error(format, primitive, &error),
    };
    let native_io_certificate =
        shardloom_vortex::local_primitive_native_io_certificate(&primitive_request, &report).ok();
    let execution_certificate =
        native_vortex_materializing_execution_certificate(&primitive_request, &report);
    let mut fields = execution_attachment_fields("run", request, plan);
    fields.append(&mut extra_fields);
    append_native_vortex_materializing_primitive_fields(
        &mut fields,
        &report,
        &primitive_arg,
        native_io_certificate.as_ref(),
        execution_certificate.as_ref(),
    );
    emit(
        "run",
        format,
        if report.has_errors() {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        format!("native Vortex {} primitive", primitive.as_str()),
        report.to_human_text(),
        report.diagnostics.clone(),
        fields,
    );
    if report.has_errors() {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn native_vortex_materializing_error(
    format: OutputFormat,
    primitive: PublicVortexPrimitive,
    error: &ShardLoomError,
) -> ExitCode {
    let summary = format!("public native Vortex {} failed", primitive.as_str());
    emit_error("run", format, &summary, error)
}

fn native_vortex_materializing_request_and_arg(
    request: &PublicWorkflowRouteRequest,
    primitive: PublicVortexPrimitive,
) -> Result<(shardloom_vortex::VortexQueryPrimitiveRequest, String), ShardLoomError> {
    let input_uri = request.input_uri.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!(
            "public native Vortex {} requires --input",
            primitive.as_str()
        ))
    })?;
    let uri = DatasetUri::new(input_uri.clone())?;
    let primitive_arg = native_vortex_primitive_arg_for_request(request, primitive)?;
    let mut primitive_request =
        vortex_primitive_execution::parse_vortex_primitive_request(uri, &primitive_arg)?;
    if let Some(limit) = request.vortex_source_order_limit.as_ref() {
        primitive_request = primitive_request
            .with_source_order_limit(positive_usize_arg("source-order limit", limit)?);
    }
    if let Some(seed) = request.vortex_sample_seed.as_ref() {
        primitive_request =
            primitive_request.with_sample_seed(non_negative_u64_arg("sample seed", seed)?);
    }
    if let Some(fraction) = request.vortex_sample_fraction.as_ref() {
        primitive_request = primitive_request
            .with_sample_fraction(sample_fraction_arg("sample fraction", fraction)?);
    }
    primitive_request =
        primitive_request.with_sample_replacement(request.vortex_sample_replacement);
    primitive_request = primitive_request.with_duplicate_keep(duplicate_keep_policy_arg(
        request.vortex_duplicate_keep.as_deref(),
    )?);
    Ok((primitive_request, primitive_arg))
}

fn native_vortex_materializing_policy(
    request: &PublicWorkflowRouteRequest,
) -> Result<shardloom_vortex::VortexLocalPrimitiveExecutionPolicy, ShardLoomError> {
    let max_parallelism = positive_usize_arg(
        "max_parallelism",
        request.max_parallelism.as_deref().unwrap_or("1"),
    )?;
    positive_u64_arg("memory_gb", request.memory_gb.as_deref().unwrap_or("1"))?;
    shardloom_vortex::VortexLocalPrimitiveExecutionPolicy::new(max_parallelism)
}

fn native_vortex_materializing_execution_certificate(
    primitive_request: &shardloom_vortex::VortexQueryPrimitiveRequest,
    report: &shardloom_vortex::VortexLocalPrimitiveExecutionReport,
) -> Option<shardloom_core::ExecutionCertificate> {
    vortex_primitive_execution::local_primitive_correctness_fixture_for_request(
        primitive_request,
        report,
    )
    .and_then(|fixture| {
        shardloom_vortex::local_primitive_execution_certificate(&fixture, primitive_request, report)
            .ok()
    })
}

fn append_native_vortex_materializing_primitive_fields(
    fields: &mut Vec<(String, String)>,
    report: &shardloom_vortex::VortexLocalPrimitiveExecutionReport,
    primitive_arg: &str,
    native_io_certificate: Option<&shardloom_core::NativeIoCertificate>,
    execution_certificate: Option<&shardloom_core::ExecutionCertificate>,
) {
    append_native_vortex_materializing_identity_fields(fields, report, primitive_arg);
    append_native_vortex_materializing_row_fields(fields, report);
    append_native_vortex_materializing_side_effect_fields(fields, report);
    append_native_vortex_materializing_limit_fields(fields, report);
    vortex_primitive_execution::append_vortex_local_primitive_native_io_certificate_fields(
        fields,
        native_io_certificate,
    );
    vortex_primitive_execution::append_vortex_local_primitive_execution_certificate_fields(
        fields,
        execution_certificate,
    );
}

fn append_native_vortex_materializing_identity_fields(
    fields: &mut Vec<(String, String)>,
    report: &shardloom_vortex::VortexLocalPrimitiveExecutionReport,
    primitive_arg: &str,
) {
    let primitive = native_vortex_materializing_public_primitive_name(report.primitive_kind);
    push_field(fields, "fallback_execution_allowed", "false");
    push_field(fields, "fallback_attempted", "false");
    push_field(fields, "external_engine_invoked", "false");
    push_field(fields, "mode", "native_vortex_primitive");
    push_field(fields, "primitive", primitive);
    push_field(fields, "vortex_primitive_arg", primitive_arg);
    push_field(
        fields,
        "execution",
        if report.has_errors() {
            format!("local_vortex_{primitive}_primitive_not_performed")
        } else {
            format!("local_vortex_{primitive}_primitive_performed")
        },
    );
    push_field(fields, "local_primitive_report_present", "true");
    push_field(
        fields,
        "local_primitive_status",
        report.status.as_str().to_string(),
    );
    push_field(
        fields,
        "local_primitive_mode",
        report.mode.as_str().to_string(),
    );
}

fn native_vortex_materializing_public_primitive_name(
    kind: shardloom_vortex::VortexQueryPrimitiveKind,
) -> &'static str {
    match kind {
        shardloom_vortex::VortexQueryPrimitiveKind::DistinctRows => "distinct",
        shardloom_vortex::VortexQueryPrimitiveKind::TailRows => "tail",
        shardloom_vortex::VortexQueryPrimitiveKind::ExpressionProjectRows => "expression_project",
        shardloom_vortex::VortexQueryPrimitiveKind::MeltRows => "melt",
        shardloom_vortex::VortexQueryPrimitiveKind::ExplodeRows => "explode",
        shardloom_vortex::VortexQueryPrimitiveKind::PivotRows => "pivot",
        shardloom_vortex::VortexQueryPrimitiveKind::RollingWindowRows => "rolling_window",
        shardloom_vortex::VortexQueryPrimitiveKind::SimpleAggregate => "aggregate",
        shardloom_vortex::VortexQueryPrimitiveKind::SortRows => "sort_rows",
        _ => kind.as_str(),
    }
}

fn append_native_vortex_materializing_row_fields(
    fields: &mut Vec<(String, String)>,
    report: &shardloom_vortex::VortexLocalPrimitiveExecutionReport,
) {
    push_field(
        fields,
        "local_primitive_rows_scanned",
        report.rows_scanned.to_string(),
    );
    push_field(
        fields,
        "local_primitive_rows_selected",
        report
            .rows_selected
            .map_or_else(|| "unknown".to_string(), |rows| rows.to_string()),
    );
    push_field(
        fields,
        "local_primitive_rows_projected",
        report
            .rows_projected
            .map_or_else(|| "unknown".to_string(), |rows| rows.to_string()),
    );
    push_field(
        fields,
        "rows_selected",
        report
            .rows_selected
            .map_or_else(|| "unknown".to_string(), |rows| rows.to_string()),
    );
    push_field(
        fields,
        "rows_projected",
        report
            .rows_projected
            .map_or_else(|| "unknown".to_string(), |rows| rows.to_string()),
    );
    push_field(
        fields,
        "output_row_count",
        report
            .rows_selected
            .map_or_else(|| "0".to_string(), |rows| rows.to_string()),
    );
    push_bool_field(fields, "result_known", report.rows_selected.is_some());
    push_field(
        fields,
        "local_primitive_projected_columns",
        report.projected_columns.join(","),
    );
    push_field(
        fields,
        "local_primitive_arrays_read_count",
        report.arrays_read_count.to_string(),
    );
    push_field(
        fields,
        "local_primitive_max_chunk_rows",
        report.max_chunk_rows.to_string(),
    );
    push_field(
        fields,
        "local_primitive_max_parallelism_requested",
        report.max_parallelism_requested.to_string(),
    );
    push_field(
        fields,
        "local_primitive_scan_concurrency_per_worker",
        report.scan_concurrency_per_worker.to_string(),
    );
}

fn append_native_vortex_materializing_side_effect_fields(
    fields: &mut Vec<(String, String)>,
    report: &shardloom_vortex::VortexLocalPrimitiveExecutionReport,
) {
    push_bool_field(fields, "data_read", report.data_read);
    push_bool_field(fields, "data_decoded", report.data_decoded);
    push_bool_field(fields, "data_materialized", report.data_materialized);
    push_bool_field(fields, "row_read", report.row_read);
    push_bool_field(fields, "arrow_converted", report.arrow_converted);
    push_bool_field(fields, "object_store_io", report.object_store_io);
    push_bool_field(fields, "write_io", report.write_io);
    push_bool_field(fields, "spill_io_performed", report.spill_io_performed);
    push_bool_field(
        fields,
        "upstream_vortex_scan_called",
        report.upstream_scan_called,
    );
    push_bool_field(
        fields,
        "local_primitive_filter_pushdown_applied",
        report.filter_pushdown_applied,
    );
    push_bool_field(
        fields,
        "local_primitive_projection_pushdown_applied",
        report.projection_pushdown_applied,
    );
    push_bool_field(
        fields,
        "local_primitive_materialization_boundary_reported",
        report.materialization_boundary_reported,
    );
}

fn append_native_vortex_materializing_limit_fields(
    fields: &mut Vec<(String, String)>,
    report: &shardloom_vortex::VortexLocalPrimitiveExecutionReport,
) {
    push_field(
        fields,
        "local_primitive_source_order_limit_requested",
        report
            .source_order_limit_requested
            .map_or_else(|| "none".to_string(), |limit| limit.to_string()),
    );
    push_bool_field(
        fields,
        "local_primitive_source_order_limit_applied",
        report.source_order_limit_applied,
    );
    push_field(
        fields,
        "local_primitive_source_order_limit_rows_output",
        report
            .source_order_limit_rows_output
            .map_or_else(|| "none".to_string(), |rows| rows.to_string()),
    );
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

#[derive(Debug, Clone)]
struct PreparedLocalWorkflowRun {
    request: PublicWorkflowRouteRequest,
    left_source_uri: String,
    left_source_format: String,
    left_target: PathBuf,
    right_source: Option<PreparedLocalWorkflowRightSource>,
}

#[derive(Debug, Clone)]
struct PreparedLocalWorkflowRightSource {
    source_uri: String,
    source_format: String,
    target: PathBuf,
}

fn execute_local_file_prepare_once_first_query_run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
) -> ExitCode {
    if is_write_request(request)
        && !matches!(
            request.requested_output.as_str(),
            "write_vortex" | "write_jsonl" | "write_csv"
        )
    {
        let blocked = local_file_compatibility_sink_contract_missing_route(request);
        return emit_blocked_facade("run", format, request, &blocked);
    }

    let prepared_run = match prepared_local_workflow_native_request(request) {
        Ok(prepared_run) => prepared_run,
        Err(blocked) => return emit_blocked_facade("run", format, request, &blocked),
    };
    let native_plan = plan_public_workflow_route(&prepared_run.request);
    if native_plan.status != CommandStatus::Success {
        return emit_blocked_facade("run", format, &prepared_run.request, &native_plan);
    }

    let left_preparation = match prepare_local_source_for_public_workflow(
        &prepared_run.left_source_uri,
        &prepared_run.left_source_format,
        &prepared_run.left_target,
    ) {
        Ok(preparation) => preparation,
        Err(PreparationFacadeError::FeatureGated) => {
            let blocked = local_file_vortex_ingest_feature_gated_route(request);
            return emit_blocked_facade("run", format, request, &blocked);
        }
        Err(PreparationFacadeError::Runtime(error)) => {
            return emit_error(
                "run",
                format,
                "public local Vortex preparation failed",
                &error,
            );
        }
    };

    let mut extra_fields = local_prepared_vortex_execution_attachment_fields(
        request,
        plan,
        &left_preparation,
        prepared_run.right_source.is_some(),
    );
    if let Some(right) = &prepared_run.right_source {
        let right_preparation = match prepare_local_source_for_public_workflow(
            &right.source_uri,
            &right.source_format,
            &right.target,
        ) {
            Ok(preparation) => preparation,
            Err(PreparationFacadeError::FeatureGated) => {
                let blocked = local_file_vortex_ingest_feature_gated_route(request);
                return emit_blocked_facade("run", format, request, &blocked);
            }
            Err(PreparationFacadeError::Runtime(error)) => {
                return emit_error(
                    "run",
                    format,
                    "public local Vortex right-input preparation failed",
                    &error,
                );
            }
        };
        extra_fields.extend(local_prepared_vortex_right_execution_attachment_fields(
            &right_preparation,
        ));
    }

    execute_prepared_local_native_route(&prepared_run.request, &native_plan, format, extra_fields)
}

fn execute_prepared_local_native_route(
    request: &PublicWorkflowRouteRequest,
    native_plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    extra_fields: Vec<(String, String)>,
) -> ExitCode {
    match native_plan.route_id {
        "native_vortex_count_all"
        | "native_vortex_count_where"
        | "native_vortex_filter"
        | "native_vortex_project"
        | "native_vortex_filter_project"
        | "native_vortex_distinct"
        | "native_vortex_duplicate_mask"
        | "native_vortex_tail"
        | "native_vortex_sample"
        | "native_vortex_expression_project"
        | "native_vortex_melt"
        | "native_vortex_explode"
        | "native_vortex_pivot"
        | "native_vortex_rolling_window"
        | "native_vortex_aggregate"
        | "native_vortex_sort_rows" => execute_native_vortex_primitive_run_with_extra(
            request,
            native_plan,
            format,
            extra_fields,
        ),
        "native_vortex_user_aggregate"
        | "native_vortex_user_join"
        | "native_vortex_user_top_n"
        | "native_vortex_user_cast"
        | "native_vortex_user_contains"
        | "native_vortex_user_sink" => execute_native_vortex_provider_run_with_extra(
            request,
            native_plan,
            format,
            extra_fields,
        ),
        "native_vortex_user_profile" => {
            execute_native_vortex_profile_run_with_extra(request, native_plan, format, extra_fields)
        }
        "native_vortex_primitive_row_export" => {
            execute_native_vortex_primitive_row_export_run_with_extra(
                request,
                native_plan,
                format,
                extra_fields,
            )
        }
        _ => {
            let blocked = run_route_not_executable_yet(native_plan);
            emit_blocked_facade("run", format, request, &blocked)
        }
    }
}

enum PreparationFacadeError {
    FeatureGated,
    Runtime(ShardLoomError),
}

fn prepare_local_source_for_public_workflow(
    source_uri: &str,
    source_format: &str,
    target: &Path,
) -> Result<sql_local_source_runtime::PublicWorkflowVortexPreparation, PreparationFacadeError> {
    sql_local_source_runtime::prepare_local_source_as_vortex_for_public_workflow(
        source_uri,
        target,
        Some(source_format),
        false,
    )
    .map_err(|error| match error {
        ShardLoomError::NotImplemented(feature)
            if feature == "vortex_ingest feature gate is not enabled" =>
        {
            PreparationFacadeError::FeatureGated
        }
        other => PreparationFacadeError::Runtime(other),
    })
}

fn prepared_local_workflow_native_request(
    request: &PublicWorkflowRouteRequest,
) -> Result<PreparedLocalWorkflowRun, Box<PublicWorkflowRoutePlan>> {
    let Some(input_uri) = request.input_uri.clone() else {
        return Err(Box::new(input_not_declared_route()));
    };
    let Some(source_format) = request.input_format.clone() else {
        return Err(Box::new(input_format_not_admitted_route("not_declared")));
    };
    let left_target = auto_prepared_vortex_target_path(&input_uri, &source_format);
    let right_source = prepared_local_workflow_right_source(request)?;
    let mut native_request = request.clone();
    if let Some(plan_summary) =
        prepared_local_workflow_plan_summary(request, &left_target, right_source.as_ref())
    {
        native_request.surface = "dataframe".to_string();
        native_request.input_uri = Some(left_target.display().to_string());
        native_request.input_format = Some("vortex".to_string());
        native_request.sql_statement = None;
        native_request.plan_summary = Some(plan_summary);
    } else if let Some(statement) =
        prepared_local_workflow_sql_statement(request, &left_target, right_source.as_ref())
    {
        native_request.surface = "sql".to_string();
        native_request.input_uri = Some(left_target.display().to_string());
        native_request.input_format = Some("vortex".to_string());
        native_request.sql_statement = Some(statement);
        native_request.plan_summary = Some("sql(statement)".to_string());
        if let Some(payload) = infer_native_vortex_sql_payload(&native_request) {
            payload.apply(&mut native_request);
        } else {
            let blocked = if is_write_request(request) {
                local_file_compatibility_sink_contract_missing_route(request)
            } else {
                local_file_vortex_middle_required_route(request)
            };
            return Err(Box::new(blocked));
        }
    } else {
        return Err(Box::new(local_file_vortex_middle_required_route(request)));
    }

    native_request.execution_policy = "native_vortex".to_string();
    native_request.materialization_policy = "zero_decode".to_string();
    native_request.bounded = true;
    Ok(PreparedLocalWorkflowRun {
        request: effective_public_workflow_request(&native_request),
        left_source_uri: input_uri,
        left_source_format: source_format,
        left_target,
        right_source,
    })
}

fn prepared_local_workflow_right_source(
    request: &PublicWorkflowRouteRequest,
) -> Result<Option<PreparedLocalWorkflowRightSource>, Box<PublicWorkflowRoutePlan>> {
    let right_uri = if let Some(summary) = request.plan_summary.as_deref() {
        parse_plan_summary_operations(summary).and_then(|operations| {
            operations
                .iter()
                .find(|operation| operation.kind == "join")
                .map(|operation| operation.arg)
                .and_then(|join_arg| join_arg.split(',').next())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })
    } else if let Some(statement) = request.sql_statement.as_deref() {
        quoted_source_refs(statement)
            .into_iter()
            .skip(1)
            .find(|ref_| {
                ref_ != request.input_uri.as_deref().unwrap_or("")
                    && infer_input_format_from_ref(ref_).is_some()
            })
    } else {
        None
    };
    let Some(right_uri) = right_uri else {
        return Ok(None);
    };
    if infer_input_format_from_ref(&right_uri) == Some("vortex") {
        return Ok(None);
    }
    let Some(source_format) = infer_input_format_from_ref(&right_uri) else {
        return Err(Box::new(local_file_vortex_middle_required_route(request)));
    };
    let target = auto_prepared_vortex_target_path(&right_uri, source_format);
    Ok(Some(PreparedLocalWorkflowRightSource {
        source_uri: right_uri,
        source_format: source_format.to_string(),
        target,
    }))
}

fn prepared_local_workflow_plan_summary(
    request: &PublicWorkflowRouteRequest,
    left_target: &Path,
    right_source: Option<&PreparedLocalWorkflowRightSource>,
) -> Option<String> {
    let operations = parse_plan_summary_operations(request.plan_summary.as_deref()?)?;
    let input_uri = request.input_uri.as_deref()?;
    let first = operations.first()?;
    if !first.kind.starts_with("read_") || first.arg.trim() != input_uri {
        return None;
    }
    let mut rewritten = Vec::with_capacity(operations.len());
    for (index, operation) in operations.iter().enumerate() {
        if index == 0 {
            rewritten.push(format!("read_vortex({})", left_target.display()));
        } else if operation.kind == "join" {
            let arg = prepared_local_join_arg(operation.arg, right_source);
            rewritten.push(format!("{}({arg})", operation.kind));
        } else {
            rewritten.push(format!("{}({})", operation.kind, operation.arg));
        }
    }
    Some(rewritten.join(" -> "))
}

fn prepared_local_workflow_sql_statement(
    request: &PublicWorkflowRouteRequest,
    left_target: &Path,
    right_source: Option<&PreparedLocalWorkflowRightSource>,
) -> Option<String> {
    let statement = request.sql_statement.as_deref()?;
    let input_uri = request.input_uri.as_deref()?;
    let mut rewritten =
        replace_quoted_sql_source_ref(statement, input_uri, &left_target.display().to_string());
    if let Some(right) = right_source {
        rewritten = replace_quoted_sql_source_ref(
            &rewritten,
            &right.source_uri,
            &right.target.display().to_string(),
        );
    }
    (rewritten != statement).then_some(rewritten)
}

fn replace_quoted_sql_source_ref(statement: &str, old_ref: &str, new_ref: &str) -> String {
    let old_literal = sql_string_literal(old_ref);
    let new_literal = sql_string_literal(new_ref);
    statement.replace(&old_literal, &new_literal)
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn prepared_local_join_arg(
    arg: &str,
    right_source: Option<&PreparedLocalWorkflowRightSource>,
) -> String {
    let Some(right_source) = right_source else {
        return arg.to_string();
    };
    let Some((_old_right, rest)) = arg.split_once(',') else {
        return arg.to_string();
    };
    format!("{},{}", right_source.target.display(), rest)
}

fn local_prepared_vortex_execution_attachment_fields(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    preparation: &sql_local_source_runtime::PublicWorkflowVortexPreparation,
    has_right_input: bool,
) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "public_workflow_local_source_route_id".to_string(),
            plan.route_id.to_string(),
        ),
        (
            "public_workflow_local_source_resolved_internal_command".to_string(),
            plan.resolved_internal_command.to_string(),
        ),
        (
            "public_workflow_local_source_format".to_string(),
            request
                .input_format
                .as_deref()
                .unwrap_or("not_declared")
                .to_string(),
        ),
        (
            "public_workflow_local_source_vortex_normalization_point".to_string(),
            "VortexPreparedState".to_string(),
        ),
        (
            "public_workflow_local_source_execution_mode".to_string(),
            "prepared_vortex_then_native_vortex".to_string(),
        ),
        (
            "public_workflow_local_source_preparation_included".to_string(),
            "true".to_string(),
        ),
        (
            "public_workflow_local_source_query_timing_starts_after_preparation".to_string(),
            "true".to_string(),
        ),
        (
            "public_workflow_local_source_vortex_ingest_performed".to_string(),
            "true".to_string(),
        ),
        (
            "public_workflow_local_source_prepared_vortex_path".to_string(),
            preparation.target_path.display().to_string(),
        ),
        (
            "public_workflow_local_source_right_input_prepared".to_string(),
            has_right_input.to_string(),
        ),
    ];
    fields.extend(preparation.fields.clone());
    fields
}

fn local_prepared_vortex_right_execution_attachment_fields(
    preparation: &sql_local_source_runtime::PublicWorkflowVortexPreparation,
) -> Vec<(String, String)> {
    let mut fields = vec![
        (
            "public_workflow_right_source_vortex_ingest_performed".to_string(),
            "true".to_string(),
        ),
        (
            "public_workflow_right_source_prepared_vortex_path".to_string(),
            preparation.target_path.display().to_string(),
        ),
    ];
    fields.extend(preparation.fields.iter().map(|(key, value)| {
        (
            key.replacen(
                "public_workflow_preparation_",
                "public_workflow_right_preparation_",
                1,
            ),
            value.clone(),
        )
    }));
    fields
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
                "usage: shardloom route <sql|python|dataframe|cli> [--input <uri>] [--input-format <format>] [--sql <statement>] [--plan <summary>] [--request <collect|prepare|write_vortex|write_parquet|write_arrow_ipc|write_avro|write_orc|write_csv|write_jsonl|explain|route|evidence>] [--output <ref>] [--fanout-output <format=local-path>]... [--execution-policy <auto|direct|native_vortex|prepare_once>] [--materialization-policy <bounded|materialized|zero_decode|explicit>] [--evidence-level <report_only|runtime_smoke|production_admitted_local_workflow|claim_grade>] [--bounded true|false] [--allow-overwrite] [--generated-source-kind <kind>] [--generated-schema <schema>] [--generated-rows <rows>] [--generated-range-start <int>] [--generated-range-end <int>] [--generated-range-step <int>] [--generated-range-column <name>] [--native-vortex-operation-family <family>] [--vortex-primitive <count|count_where|filter|project|filter_project|distinct|tail|sample|expression_project|melt|explode|pivot|rolling_window|aggregate|sort_rows>] [--vortex-predicate <tiny-predicate>] [--vortex-columns <columns>] [--vortex-source-order-limit <rows>] [--vortex-sample-fraction <fraction>] [--vortex-sample-seed <seed>] [--vortex-sample-replacement] [--vortex-expression-projection <json>] [--vortex-melt-projection <json>] [--vortex-explode-projection <json>] [--vortex-pivot-projection <json>] [--vortex-rolling-window <json>] [--vortex-aggregate <json>] [--vortex-sort-rows <json>] [--memory-gb <n>] [--max-parallelism <n>]"
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
            vortex_sample_seed: None,
            vortex_sample_fraction: None,
            vortex_sample_replacement: false,
            vortex_duplicate_keep: None,
            vortex_expression_projection: None,
            vortex_melt_projection: None,
            vortex_explode_projection: None,
            vortex_pivot_projection: None,
            vortex_rolling_window: None,
            vortex_aggregate: None,
            vortex_sort_rows: None,
            memory_gb: None,
            max_parallelism: None,
        }
    }

    #[allow(clippy::too_many_lines)]
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
            "--vortex-sample-seed" => {
                self.vortex_sample_seed = Some(required_value(args, "--vortex-sample-seed")?);
            }
            "--vortex-sample-fraction" => {
                self.vortex_sample_fraction =
                    Some(required_value(args, "--vortex-sample-fraction")?);
            }
            "--vortex-sample-replacement" => {
                self.vortex_sample_replacement = true;
            }
            "--vortex-duplicate-keep" => {
                self.vortex_duplicate_keep = Some(normalize_duplicate_keep(&required_value(
                    args,
                    "--vortex-duplicate-keep",
                )?)?);
            }
            "--vortex-expression-projection" => {
                self.vortex_expression_projection =
                    Some(required_value(args, "--vortex-expression-projection")?);
            }
            "--vortex-melt-projection" => {
                self.vortex_melt_projection =
                    Some(required_value(args, "--vortex-melt-projection")?);
            }
            "--vortex-explode-projection" => {
                self.vortex_explode_projection =
                    Some(required_value(args, "--vortex-explode-projection")?);
            }
            "--vortex-pivot-projection" => {
                self.vortex_pivot_projection =
                    Some(required_value(args, "--vortex-pivot-projection")?);
            }
            "--vortex-rolling-window" => {
                self.vortex_rolling_window = Some(required_value(args, "--vortex-rolling-window")?);
            }
            "--vortex-aggregate" => {
                self.vortex_aggregate = Some(required_value(args, "--vortex-aggregate")?);
            }
            "--vortex-sort-rows" => {
                self.vortex_sort_rows = Some(required_value(args, "--vortex-sort-rows")?);
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
            self.input_uri = self.sql_statement.as_deref().and_then(|statement| {
                extract_first_quoted_source_ref(statement).or_else(|| {
                    self.input_format
                        .as_deref()
                        .filter(|format| is_local_file_format(format) || *format == "vortex")
                        .and_then(|_| extract_first_declared_sql_source_ref(statement))
                })
            });
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
    Distinct,
    DuplicateMask,
    Tail,
    Sample,
    ExpressionProject,
    Melt,
    Explode,
    Pivot,
    RollingWindow,
    Aggregate,
    SortRows,
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
            "distinct" | "distinct_rows" | "deduplicate" | "drop_duplicates" | "unique" => {
                Some(Self::Distinct)
            }
            "duplicate_mask" | "duplicate-mask" | "duplicate_mask_rows" | "duplicated" => {
                Some(Self::DuplicateMask)
            }
            "tail" | "tail_rows" | "source_order_tail" => Some(Self::Tail),
            "sample" | "sample_rows" | "deterministic_sample" => Some(Self::Sample),
            "expression_project"
            | "expression-project"
            | "expression_project_rows"
            | "mask"
            | "replace" => Some(Self::ExpressionProject),
            "melt" | "melt_rows" | "melt-rows" | "unpivot" => Some(Self::Melt),
            "explode" | "explode_rows" | "explode-rows" | "list_explode" | "list-explode" => {
                Some(Self::Explode)
            }
            "pivot" | "pivot_rows" | "pivot-rows" | "pivot_table" | "pivot-table" => {
                Some(Self::Pivot)
            }
            "rolling" | "rolling_window" | "rolling-window" | "rolling_rows" | "rolling-rows"
            | "rolling_sum" | "rolling-sum" | "rolling_mean" | "rolling-mean" | "rolling_count"
            | "rolling-count" => Some(Self::RollingWindow),
            "aggregate" | "aggregation" | "simple_aggregate" | "simple-aggregate"
            | "scalar_aggregate" | "scalar-aggregate" => Some(Self::Aggregate),
            "sort" | "sort_rows" | "sort-rows" | "order_by" | "order-rows" | "order_rows" => {
                Some(Self::SortRows)
            }
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
            Self::Distinct => "distinct",
            Self::DuplicateMask => "duplicate_mask",
            Self::Tail => "tail",
            Self::Sample => "sample",
            Self::ExpressionProject => "expression_project",
            Self::Melt => "melt",
            Self::Explode => "explode",
            Self::Pivot => "pivot",
            Self::RollingWindow => "rolling_window",
            Self::Aggregate => "aggregate",
            Self::SortRows => "sort_rows",
        }
    }

    const fn route_id(self) -> &'static str {
        match self {
            Self::Count => "native_vortex_count_all",
            Self::CountWhere => "native_vortex_count_where",
            Self::Filter => "native_vortex_filter",
            Self::Project => "native_vortex_project",
            Self::FilterProject => "native_vortex_filter_project",
            Self::Distinct => "native_vortex_distinct",
            Self::DuplicateMask => "native_vortex_duplicate_mask",
            Self::Tail => "native_vortex_tail",
            Self::Sample => "native_vortex_sample",
            Self::ExpressionProject => "native_vortex_expression_project",
            Self::Melt => "native_vortex_melt",
            Self::Explode => "native_vortex_explode",
            Self::Pivot => "native_vortex_pivot",
            Self::RollingWindow => "native_vortex_rolling_window",
            Self::Aggregate => "native_vortex_aggregate",
            Self::SortRows => "native_vortex_sort_rows",
        }
    }

    const fn resolved_internal_command(self) -> &'static str {
        match self {
            Self::CountWhere => "vortex-count-where",
            Self::Filter => "vortex-filter",
            Self::Project => "vortex-project",
            Self::FilterProject => "vortex-filter-project",
            Self::Count
            | Self::Distinct
            | Self::DuplicateMask
            | Self::Tail
            | Self::Sample
            | Self::ExpressionProject
            | Self::Melt
            | Self::Explode
            | Self::Pivot
            | Self::RollingWindow
            | Self::Aggregate
            | Self::SortRows => "vortex-run",
        }
    }

    const fn requires_predicate(self) -> bool {
        matches!(self, Self::CountWhere | Self::Filter | Self::FilterProject)
    }

    const fn requires_columns(self) -> bool {
        matches!(
            self,
            Self::Project
                | Self::FilterProject
                | Self::DuplicateMask
                | Self::ExpressionProject
                | Self::Explode
        )
    }

    const fn requires_expression_projection(self) -> bool {
        matches!(self, Self::ExpressionProject)
    }

    const fn requires_melt_projection(self) -> bool {
        matches!(self, Self::Melt)
    }

    const fn requires_explode_projection(self) -> bool {
        matches!(self, Self::Explode)
    }

    const fn requires_pivot_projection(self) -> bool {
        matches!(self, Self::Pivot)
    }

    const fn requires_rolling_window(self) -> bool {
        matches!(self, Self::RollingWindow)
    }

    const fn requires_aggregate(self) -> bool {
        matches!(self, Self::Aggregate)
    }

    const fn requires_sort_rows(self) -> bool {
        matches!(self, Self::SortRows)
    }

    const fn allows_source_order_limit(self) -> bool {
        matches!(
            self,
            Self::Filter
                | Self::Project
                | Self::FilterProject
                | Self::Distinct
                | Self::DuplicateMask
                | Self::Tail
                | Self::Sample
                | Self::ExpressionProject
                | Self::Melt
                | Self::Explode
                | Self::Pivot
                | Self::RollingWindow
                | Self::Aggregate
                | Self::SortRows
        )
    }

    const fn requires_local_primitives_feature(self) -> bool {
        matches!(
            self,
            Self::Distinct
                | Self::DuplicateMask
                | Self::Tail
                | Self::Sample
                | Self::ExpressionProject
                | Self::Melt
                | Self::Explode
                | Self::Pivot
                | Self::RollingWindow
                | Self::Aggregate
                | Self::SortRows
        )
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
    Distinct,
    DuplicateMask,
    Sample,
    ExpressionProject,
    Melt,
    Explode,
    Pivot,
    RollingWindow,
    Profile,
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
            "top_n" | "topn" | "global_top_n" | "nlargest" | "nsmallest" | "tail" | "tail_rows" => {
                Some(Self::TopN)
            }
            "cast" | "try_cast" | "trycast" => Some(Self::Cast),
            "contains" | "substring_contains" | "string_contains" => Some(Self::Contains),
            "distinct" | "deduplicate" | "dedup" | "drop_duplicates" | "unique" => {
                Some(Self::Distinct)
            }
            "duplicate_mask" | "duplicate_mask_rows" | "duplicated" => Some(Self::DuplicateMask),
            "sample" | "sampling" | "sample_rows" | "deterministic_sample" => Some(Self::Sample),
            "expression_project"
            | "expression_project_rows"
            | "mask"
            | "replace"
            | "conditional_rewrite"
            | "value_rewrite" => Some(Self::ExpressionProject),
            "melt" | "melt_rows" | "reshape" | "unpivot" => Some(Self::Melt),
            "explode" | "explode_rows" | "list_explode" | "nested_expansion" => Some(Self::Explode),
            "pivot" | "pivot_rows" | "pivot_table" | "pivot_wide_reshape" => Some(Self::Pivot),
            "rolling" | "rolling_window" | "rolling_rows" | "rolling_sum" | "rolling_mean"
            | "rolling_count" | "window" => Some(Self::RollingWindow),
            "profile" | "schema_profile" | "bounded_profile" => Some(Self::Profile),
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
            PublicVortexPrimitive::Distinct => Self::Distinct,
            PublicVortexPrimitive::DuplicateMask => Self::DuplicateMask,
            PublicVortexPrimitive::Tail | PublicVortexPrimitive::SortRows => Self::TopN,
            PublicVortexPrimitive::Sample => Self::Sample,
            PublicVortexPrimitive::ExpressionProject => Self::ExpressionProject,
            PublicVortexPrimitive::Melt => Self::Melt,
            PublicVortexPrimitive::Explode => Self::Explode,
            PublicVortexPrimitive::Pivot => Self::Pivot,
            PublicVortexPrimitive::RollingWindow => Self::RollingWindow,
            PublicVortexPrimitive::Aggregate => Self::Aggregate,
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
            Self::Distinct => "distinct",
            Self::DuplicateMask => "duplicate_mask",
            Self::Sample => "sample",
            Self::ExpressionProject => "expression_project",
            Self::Melt => "melt",
            Self::Explode => "explode",
            Self::Pivot => "pivot",
            Self::RollingWindow => "rolling_window",
            Self::Profile => "profile",
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
            ) | (Self::Distinct, PublicVortexPrimitive::Distinct)
                | (Self::DuplicateMask, PublicVortexPrimitive::DuplicateMask)
                | (
                    Self::TopN,
                    PublicVortexPrimitive::Tail | PublicVortexPrimitive::SortRows
                )
                | (Self::Sample, PublicVortexPrimitive::Sample)
                | (
                    Self::ExpressionProject,
                    PublicVortexPrimitive::ExpressionProject
                )
                | (Self::Melt, PublicVortexPrimitive::Melt)
                | (Self::Explode, PublicVortexPrimitive::Explode)
                | (Self::Pivot, PublicVortexPrimitive::Pivot)
                | (Self::RollingWindow, PublicVortexPrimitive::RollingWindow)
                | (Self::Aggregate, PublicVortexPrimitive::Aggregate)
        )
    }

    const fn blocker_id(self) -> &'static str {
        match self {
            Self::Aggregate => "py-vortex-route-unify-1.native_vortex_aggregate_route_missing",
            Self::Join => "py-vortex-route-unify-1.native_vortex_join_state_missing",
            Self::TopN => "py-vortex-route-unify-1.native_vortex_top_n_route_missing",
            Self::Cast => "py-vortex-route-unify-1.native_vortex_cast_route_missing",
            Self::Contains => "py-vortex-route-unify-1.native_vortex_contains_route_missing",
            Self::Distinct => "py-vortex-route-unify-1.native_vortex_distinct_route_missing",
            Self::DuplicateMask => {
                "py-vortex-route-unify-1.native_vortex_duplicate_mask_route_missing"
            }
            Self::Sample => "py-vortex-route-unify-1.native_vortex_sample_route_missing",
            Self::ExpressionProject => {
                "py-vortex-route-unify-1.native_vortex_expression_project_route_missing"
            }
            Self::Melt => "py-vortex-route-unify-1.native_vortex_melt_route_missing",
            Self::Explode => "py-vortex-route-unify-1.native_vortex_explode_route_missing",
            Self::Pivot => "py-vortex-route-unify-1.native_vortex_pivot_route_missing",
            Self::RollingWindow => {
                "py-vortex-route-unify-1.native_vortex_rolling_window_route_missing"
            }
            Self::Profile => "py-vortex-route-unify-1.native_vortex_profile_route_missing",
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
            Self::Distinct => "native Vortex row-level distinct/dedup route is not admitted yet",
            Self::DuplicateMask => "native Vortex duplicate-mask route is not admitted yet",
            Self::Sample => "native Vortex deterministic sample route is not admitted yet",
            Self::ExpressionProject => {
                "native Vortex expression projection route is not admitted yet"
            }
            Self::Melt => "native Vortex scoped melt route is not admitted yet",
            Self::Explode => {
                "native Vortex scoped explode route requires an admitted single-column list primitive payload"
            }
            Self::Pivot => {
                "native Vortex scoped pivot route requires an admitted single-index/single-column/single-value primitive payload"
            }
            Self::RollingWindow => "native Vortex scoped rolling-window route is not admitted yet",
            Self::Profile => "native Vortex bounded profile/statistics route is not admitted yet",
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
            Self::Distinct => {
                "native_vortex_distinct_state;null_equality_semantics;bounded_result_contract;decoded_reference_correctness;route_certificate"
            }
            Self::DuplicateMask => {
                "native_vortex_duplicate_mask_state;keep_policy_semantics;bounded_result_contract;decoded_reference_correctness;route_certificate"
            }
            Self::Sample => {
                "native_vortex_sample_scan;deterministic_seed_policy;bounded_result_contract;decoded_reference_correctness;route_certificate"
            }
            Self::ExpressionProject => {
                "native_vortex_expression_projection;typed_scalar_rewrite;decode_materialization_boundary;decoded_reference_correctness;route_certificate"
            }
            Self::Melt => {
                "native_vortex_melt;typed_melt_projection;same_typed_value_columns;decode_materialization_boundary;decoded_reference_correctness;route_certificate"
            }
            Self::Explode => {
                "native_vortex_explode;typed_list_projection;list_element_scalar_contract;decode_materialization_boundary;decoded_reference_correctness;route_certificate"
            }
            Self::Pivot => {
                "native_vortex_pivot;single_index_column;single_pivot_column;single_value_column;wide_reshape_state;decode_materialization_boundary;decoded_reference_correctness;route_certificate"
            }
            Self::RollingWindow => {
                "native_vortex_rolling_window;source_order_window_state;sum_mean_count_semantics;decode_materialization_boundary;decoded_reference_correctness;route_certificate"
            }
            Self::Profile => {
                "metadata_first_profile;vortex_statistics_or_bounded_decode_contract;schema_correctness;route_certificate"
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
            Self::Distinct => {
                "implement and certify native Vortex row-level distinct/dedup state before admitting this family"
            }
            Self::DuplicateMask => {
                "route duplicated(keep='first'|'last'|False) through the admitted native Vortex duplicate-mask primitive with declared subset columns"
            }
            Self::Sample => {
                "route deterministic sample through the admitted native Vortex sample primitive with a bounded sample size and seed"
            }
            Self::ExpressionProject => {
                "route scoped mask/replace through the admitted native Vortex expression-project primitive with a typed rewrite payload"
            }
            Self::Melt => {
                "route scoped melt through the admitted native Vortex melt primitive with explicit id/value columns"
            }
            Self::Explode => {
                "route scoped explode through the admitted native Vortex explode primitive with one declared list column"
            }
            Self::Pivot => {
                "route scoped pivot/pivot_table through the admitted native Vortex pivot primitive with one index, one pivot, and one value column"
            }
            Self::RollingWindow => {
                "route scoped rolling-window sum/mean/count through the admitted native Vortex rolling-window primitive with explicit source/order/window payload"
            }
            Self::Profile => {
                "implement and certify metadata-first Vortex profile/statistics route before admitting this family"
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
            "use count, filter_project_limit, aggregate, join, top_n, cast, contains, distinct, duplicate_mask, sample, expression_project, melt, explode, rolling_window, profile, or sink",
        );
    };
    if effective_request.native_vortex_provider_scenario.is_some() {
        return native_vortex_provider_route(&effective_request, requested_family);
    }
    let primitive = normalized_vortex_primitive(&effective_request);
    if is_write_request(&effective_request)
        || effective_request.output_ref.is_some()
        || !effective_request.fanout_outputs.is_empty()
    {
        if let Some(primitive) = primitive {
            return native_vortex_primitive_row_export_route(
                &effective_request,
                requested_family,
                primitive,
            );
        }
        return native_vortex_operation_blocked_route(NativeVortexOperationFamily::Sink);
    }
    if requested_family == Some(NativeVortexOperationFamily::Profile)
        || effective_request.requested_output == "profile"
    {
        return native_vortex_profile_route(&effective_request);
    }
    let Some(primitive) = primitive else {
        if effective_request.vortex_primitive.is_some() {
            return native_vortex_payload_blocked_route(
                "public_workflow_route.vortex_primitive",
                "unsupported native Vortex primitive",
                "use count, count_where, filter, project, filter_project, distinct, duplicate_mask, tail, sample, expression_project, melt, explode, pivot, rolling_window, aggregate, or sort_rows",
            );
        }
        if let Some(plan) = native_vortex_provider_schema_shape_blocker(&effective_request) {
            return plan;
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
    if let Some(plan) = native_vortex_primitive_payload_blocker(&effective_request, primitive) {
        return plan;
    }
    if primitive.requires_local_primitives_feature() && !cfg!(feature = "vortex-local-primitives") {
        return native_vortex_materializing_primitive_feature_gated_route();
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

#[allow(clippy::too_many_lines)]
fn native_vortex_primitive_payload_blocker(
    request: &PublicWorkflowRouteRequest,
    primitive: PublicVortexPrimitive,
) -> Option<PublicWorkflowRoutePlan> {
    if primitive.requires_predicate() && request.vortex_predicate.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_predicate",
            "native Vortex primitive requires a predicate payload",
            "pass --vortex-predicate with the scoped tiny predicate expression",
        ));
    }
    if primitive.requires_columns() && request.vortex_columns.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_columns",
            "native Vortex primitive requires a projection payload",
            "pass --vortex-columns with comma-separated projected columns",
        ));
    }
    if primitive.requires_expression_projection() && request.vortex_expression_projection.is_none()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_expression_projection",
            "native Vortex expression-project primitive requires an expression projection payload",
            "pass --vortex-expression-projection with the scoped typed rewrite JSON",
        ));
    }
    if primitive.requires_melt_projection() && request.vortex_melt_projection.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_melt_projection",
            "native Vortex melt primitive requires a melt projection payload",
            "pass --vortex-melt-projection with the scoped typed melt JSON",
        ));
    }
    if primitive.requires_explode_projection() && request.vortex_explode_projection.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_explode_projection",
            "native Vortex explode primitive requires an explode projection payload",
            "pass --vortex-explode-projection with the scoped list explode JSON",
        ));
    }
    if primitive.requires_pivot_projection() && request.vortex_pivot_projection.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_pivot_projection",
            "native Vortex pivot primitive requires a pivot projection payload",
            "pass --vortex-pivot-projection with the scoped wide-reshape JSON",
        ));
    }
    if primitive.requires_rolling_window() && request.vortex_rolling_window.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_rolling_window",
            "native Vortex rolling-window primitive requires a rolling-window payload",
            "pass --vortex-rolling-window with the scoped source-order rolling JSON",
        ));
    }
    if primitive.requires_aggregate() && request.vortex_aggregate.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_aggregate",
            "native Vortex aggregate primitive requires an aggregate payload",
            "pass --vortex-aggregate with the scoped scalar aggregate JSON",
        ));
    }
    if primitive.requires_sort_rows() && request.vortex_sort_rows.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sort_rows",
            "native Vortex sort-row primitive requires a sort payload",
            "pass --vortex-sort-rows with the scoped order_by JSON",
        ));
    }
    if request.vortex_source_order_limit.is_some() && !primitive.allows_source_order_limit() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_source_order_limit",
            "native Vortex primitive does not admit a source-order limit",
            "use --vortex-source-order-limit only with filter, project, filter_project, distinct, duplicate_mask, tail, sample, expression_project, melt, explode, pivot, rolling_window, aggregate, or sort_rows",
        ));
    }
    if matches!(primitive, PublicVortexPrimitive::Tail)
        && request.vortex_source_order_limit.is_none()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_source_order_limit",
            "native Vortex tail requires a bounded row count",
            "pass --vortex-source-order-limit with the requested tail row count",
        ));
    }
    if matches!(primitive, PublicVortexPrimitive::Sample)
        && request.vortex_source_order_limit.is_none()
        && request.vortex_sample_fraction.is_none()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_shape",
            "native Vortex sample requires a bounded row count or fraction",
            "pass --vortex-source-order-limit for sample(n=...) or --vortex-sample-fraction for sample(frac=...)",
        ));
    }
    if matches!(primitive, PublicVortexPrimitive::Sample)
        && request.vortex_source_order_limit.is_some()
        && request.vortex_sample_fraction.is_some()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_shape",
            "native Vortex sample accepts either row count or fraction, not both",
            "pass only one of --vortex-source-order-limit or --vortex-sample-fraction",
        ));
    }
    if request.vortex_sample_seed.is_some() && !matches!(primitive, PublicVortexPrimitive::Sample) {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_seed",
            "native Vortex sample seed is only valid for sample primitives",
            "use --vortex-sample-seed only with --vortex-primitive sample",
        ));
    }
    if let Some(seed) = request.vortex_sample_seed.as_ref()
        && non_negative_u64_arg("sample seed", seed).is_err()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_seed",
            "native Vortex sample seed must be a non-negative integer",
            "pass --vortex-sample-seed with an unsigned integer seed",
        ));
    }
    if request.vortex_sample_fraction.is_some()
        && !matches!(primitive, PublicVortexPrimitive::Sample)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_fraction",
            "native Vortex sample fraction is only valid for sample primitives",
            "use --vortex-sample-fraction only with --vortex-primitive sample",
        ));
    }
    if request.vortex_sample_replacement && !matches!(primitive, PublicVortexPrimitive::Sample) {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_replacement",
            "native Vortex sample replacement is only valid for sample primitives",
            "use --vortex-sample-replacement only with --vortex-primitive sample",
        ));
    }
    if request.vortex_duplicate_keep.is_some()
        && !matches!(primitive, PublicVortexPrimitive::DuplicateMask)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_duplicate_keep",
            "native Vortex duplicate keep policy is only valid for duplicate-mask primitives",
            "use --vortex-duplicate-keep only with --vortex-primitive duplicate_mask",
        ));
    }
    if request.vortex_expression_projection.is_some()
        && !matches!(primitive, PublicVortexPrimitive::ExpressionProject)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_expression_projection",
            "native Vortex expression projection is only valid for expression-project primitives",
            "use --vortex-expression-projection only with --vortex-primitive expression_project",
        ));
    }
    if request.vortex_melt_projection.is_some() && !matches!(primitive, PublicVortexPrimitive::Melt)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_melt_projection",
            "native Vortex melt projection is only valid for melt primitives",
            "use --vortex-melt-projection only with --vortex-primitive melt",
        ));
    }
    if request.vortex_explode_projection.is_some()
        && !matches!(primitive, PublicVortexPrimitive::Explode)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_explode_projection",
            "native Vortex explode projection is only valid for explode primitives",
            "use --vortex-explode-projection only with --vortex-primitive explode",
        ));
    }
    if request.vortex_pivot_projection.is_some()
        && !matches!(primitive, PublicVortexPrimitive::Pivot)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_pivot_projection",
            "native Vortex pivot projection is only valid for pivot primitives",
            "use --vortex-pivot-projection only with --vortex-primitive pivot",
        ));
    }
    if request.vortex_rolling_window.is_some()
        && !matches!(primitive, PublicVortexPrimitive::RollingWindow)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_rolling_window",
            "native Vortex rolling-window payload is only valid for rolling-window primitives",
            "use --vortex-rolling-window only with --vortex-primitive rolling_window",
        ));
    }
    if request.vortex_aggregate.is_some() && !matches!(primitive, PublicVortexPrimitive::Aggregate)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_aggregate",
            "native Vortex aggregate payload is only valid for aggregate primitives",
            "use --vortex-aggregate only with --vortex-primitive aggregate",
        ));
    }
    if request.vortex_sort_rows.is_some() && !matches!(primitive, PublicVortexPrimitive::SortRows) {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sort_rows",
            "native Vortex sort-row payload is only valid for sort-row primitives",
            "use --vortex-sort-rows only with --vortex-primitive sort_rows",
        ));
    }
    if matches!(primitive, PublicVortexPrimitive::SortRows)
        && request.vortex_source_order_limit.is_none()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_source_order_limit",
            "native Vortex sort rows require a bounded row count",
            "pass --vortex-source-order-limit with the requested top row count",
        ));
    }
    if let Some(fraction) = request.vortex_sample_fraction.as_ref()
        && sample_fraction_arg("sample fraction", fraction).is_err()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_fraction",
            "native Vortex sample fraction must be finite and in the range (0, 1]",
            "pass --vortex-sample-fraction with a decimal value greater than 0 and no greater than 1",
        ));
    }
    native_vortex_resource_hint_blocker(request)
}

fn native_vortex_primitive_row_export_route(
    request: &PublicWorkflowRouteRequest,
    requested_family: Option<NativeVortexOperationFamily>,
    primitive: PublicVortexPrimitive,
) -> PublicWorkflowRoutePlan {
    if !cfg!(feature = "vortex-local-primitives") {
        return native_vortex_primitive_row_export_feature_gated_route();
    }
    if !matches!(
        request.requested_output.as_str(),
        "write_jsonl" | "write_csv"
    ) {
        return native_vortex_sink_format_blocked_route(request);
    }
    if !matches!(
        primitive,
        PublicVortexPrimitive::Filter
            | PublicVortexPrimitive::Project
            | PublicVortexPrimitive::FilterProject
            | PublicVortexPrimitive::Distinct
            | PublicVortexPrimitive::DuplicateMask
            | PublicVortexPrimitive::Tail
            | PublicVortexPrimitive::Sample
            | PublicVortexPrimitive::ExpressionProject
            | PublicVortexPrimitive::Melt
            | PublicVortexPrimitive::Pivot
            | PublicVortexPrimitive::RollingWindow
            | PublicVortexPrimitive::Aggregate
    ) {
        return native_vortex_operation_blocked_route(NativeVortexOperationFamily::Sink);
    }
    if let Err(blocked) = native_vortex_primitive_row_export_targets(request, "route") {
        return *blocked;
    }
    let operation_family =
        requested_family.unwrap_or_else(|| NativeVortexOperationFamily::from_primitive(primitive));
    if !matches!(operation_family, NativeVortexOperationFamily::Sink)
        && !operation_family.allows_primitive(primitive)
    {
        return native_vortex_operation_blocked_route(operation_family);
    }
    if let Some(plan) = native_vortex_primitive_row_export_payload_blocker(request, primitive) {
        return plan;
    }
    admitted_route(
        "native_vortex_primitive_row_export",
        "vortex-local-primitive-row-export",
        "native_vortex_file",
        "native_vortex_primitive_row_export",
        "native_vortex",
        false,
        true,
    )
}

#[allow(clippy::too_many_lines)]
fn native_vortex_primitive_row_export_payload_blocker(
    request: &PublicWorkflowRouteRequest,
    primitive: PublicVortexPrimitive,
) -> Option<PublicWorkflowRoutePlan> {
    if primitive.requires_predicate() && request.vortex_predicate.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_predicate",
            "native Vortex primitive row export requires a predicate payload",
            "pass --vortex-predicate with the scoped tiny predicate expression",
        ));
    }
    if primitive.requires_columns() && request.vortex_columns.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_columns",
            "native Vortex primitive row export requires a projection payload",
            "pass --vortex-columns with comma-separated projected columns",
        ));
    }
    if primitive.requires_expression_projection() && request.vortex_expression_projection.is_none()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_expression_projection",
            "native Vortex expression-project row export requires an expression projection payload",
            "pass --vortex-expression-projection with the scoped typed rewrite JSON",
        ));
    }
    if primitive.requires_melt_projection() && request.vortex_melt_projection.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_melt_projection",
            "native Vortex melt row export requires a melt projection payload",
            "pass --vortex-melt-projection with the scoped typed melt JSON",
        ));
    }
    if primitive.requires_explode_projection() && request.vortex_explode_projection.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_explode_projection",
            "native Vortex explode row export requires an explode projection payload",
            "pass --vortex-explode-projection with the scoped list explode JSON",
        ));
    }
    if primitive.requires_pivot_projection() && request.vortex_pivot_projection.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_pivot_projection",
            "native Vortex pivot row export requires a pivot projection payload",
            "pass --vortex-pivot-projection with the scoped wide-reshape JSON",
        ));
    }
    if primitive.requires_rolling_window() && request.vortex_rolling_window.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_rolling_window",
            "native Vortex rolling-window row export requires a rolling-window payload",
            "pass --vortex-rolling-window with the scoped source-order rolling JSON",
        ));
    }
    if primitive.requires_aggregate() && request.vortex_aggregate.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_aggregate",
            "native Vortex aggregate row export requires an aggregate payload",
            "pass --vortex-aggregate with the scoped scalar aggregate JSON",
        ));
    }
    if primitive.requires_sort_rows() && request.vortex_sort_rows.is_none() {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sort_rows",
            "native Vortex sort-row export requires a sort payload",
            "pass --vortex-sort-rows with the scoped order_by JSON",
        ));
    }
    if matches!(primitive, PublicVortexPrimitive::Tail)
        && request.vortex_source_order_limit.is_none()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_source_order_limit",
            "native Vortex primitive row export requires a bounded tail row count",
            "pass --vortex-source-order-limit with the requested tail row count",
        ));
    }
    if matches!(primitive, PublicVortexPrimitive::Sample)
        && request.vortex_source_order_limit.is_none()
        && request.vortex_sample_fraction.is_none()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_shape",
            "native Vortex sample row export requires a row count or fraction",
            "pass --vortex-source-order-limit for sample(n=...) or --vortex-sample-fraction for sample(frac=...)",
        ));
    }
    if matches!(primitive, PublicVortexPrimitive::Sample)
        && request.vortex_source_order_limit.is_some()
        && request.vortex_sample_fraction.is_some()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_shape",
            "native Vortex sample row export accepts either row count or fraction, not both",
            "pass only one of --vortex-source-order-limit or --vortex-sample-fraction",
        ));
    }
    if request.vortex_sample_seed.is_some() && !matches!(primitive, PublicVortexPrimitive::Sample) {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_seed",
            "native Vortex sample seed is only valid for sample row export",
            "use --vortex-sample-seed only with --vortex-primitive sample",
        ));
    }
    if let Some(seed) = request.vortex_sample_seed.as_ref()
        && non_negative_u64_arg("sample seed", seed).is_err()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_seed",
            "native Vortex sample seed must be a non-negative integer",
            "pass --vortex-sample-seed with an unsigned integer seed",
        ));
    }
    if request.vortex_sample_fraction.is_some()
        && !matches!(primitive, PublicVortexPrimitive::Sample)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_fraction",
            "native Vortex sample fraction is only valid for sample row export",
            "use --vortex-sample-fraction only with --vortex-primitive sample",
        ));
    }
    if request.vortex_sample_replacement && !matches!(primitive, PublicVortexPrimitive::Sample) {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_replacement",
            "native Vortex sample replacement is only valid for sample row export",
            "use --vortex-sample-replacement only with --vortex-primitive sample",
        ));
    }
    if request.vortex_duplicate_keep.is_some()
        && !matches!(primitive, PublicVortexPrimitive::DuplicateMask)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_duplicate_keep",
            "native Vortex duplicate keep policy is only valid for duplicate-mask row export",
            "use --vortex-duplicate-keep only with --vortex-primitive duplicate_mask",
        ));
    }
    if request.vortex_expression_projection.is_some()
        && !matches!(primitive, PublicVortexPrimitive::ExpressionProject)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_expression_projection",
            "native Vortex expression projection is only valid for expression-project row export",
            "use --vortex-expression-projection only with --vortex-primitive expression_project",
        ));
    }
    if request.vortex_melt_projection.is_some() && !matches!(primitive, PublicVortexPrimitive::Melt)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_melt_projection",
            "native Vortex melt projection is only valid for melt row export",
            "use --vortex-melt-projection only with --vortex-primitive melt",
        ));
    }
    if request.vortex_explode_projection.is_some()
        && !matches!(primitive, PublicVortexPrimitive::Explode)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_explode_projection",
            "native Vortex explode projection is only valid for explode row export",
            "use --vortex-explode-projection only with --vortex-primitive explode",
        ));
    }
    if request.vortex_pivot_projection.is_some()
        && !matches!(primitive, PublicVortexPrimitive::Pivot)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_pivot_projection",
            "native Vortex pivot projection is only valid for pivot row export",
            "use --vortex-pivot-projection only with --vortex-primitive pivot",
        ));
    }
    if request.vortex_rolling_window.is_some()
        && !matches!(primitive, PublicVortexPrimitive::RollingWindow)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_rolling_window",
            "native Vortex rolling-window payload is only valid for rolling-window row export",
            "use --vortex-rolling-window only with --vortex-primitive rolling_window",
        ));
    }
    if request.vortex_aggregate.is_some() && !matches!(primitive, PublicVortexPrimitive::Aggregate)
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_aggregate",
            "native Vortex aggregate payload is only valid for aggregate primitives",
            "use --vortex-aggregate only with --vortex-primitive aggregate",
        ));
    }
    if request.vortex_sort_rows.is_some() && !matches!(primitive, PublicVortexPrimitive::SortRows) {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sort_rows",
            "native Vortex sort-row payload is only valid for sort-row export",
            "use --vortex-sort-rows only with --vortex-primitive sort_rows",
        ));
    }
    if matches!(primitive, PublicVortexPrimitive::SortRows)
        && request.vortex_source_order_limit.is_none()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_source_order_limit",
            "native Vortex sort-row export requires a bounded row count",
            "pass --vortex-source-order-limit with the requested top row count",
        ));
    }
    if let Some(fraction) = request.vortex_sample_fraction.as_ref()
        && sample_fraction_arg("sample fraction", fraction).is_err()
    {
        return Some(native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_sample_fraction",
            "native Vortex sample fraction must be finite and in the range (0, 1]",
            "pass --vortex-sample-fraction with a decimal value greater than 0 and no greater than 1",
        ));
    }
    native_vortex_resource_hint_blocker(request)
}

fn native_vortex_primitive_row_export_feature_gated_route() -> PublicWorkflowRoutePlan {
    blocked_route(
        "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated",
        "native Vortex primitive row export requires the vortex-local-primitives feature",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "native Vortex primitive row export is feature-gated in this binary".to_string(),
            Some("public_workflow_route.vortex_primitive".to_string()),
            Some("compiled_without=vortex-local-primitives".to_string()),
            Some(
                "build the release binary with --features release-user-surfaces or vortex-local-primitives to execute scoped JSONL/CSV row exports".to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn native_vortex_materializing_primitive_feature_gated_route() -> PublicWorkflowRoutePlan {
    blocked_route(
        "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated",
        "native Vortex materializing primitive collect requires the vortex-local-primitives feature",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
                "native Vortex distinct/tail/sample/expression-project/melt/explode/rolling-window/aggregate/sort-row collect is feature-gated in this binary"
                .to_string(),
            Some("public_workflow_route.vortex_primitive".to_string()),
            Some("compiled_without=vortex-local-primitives".to_string()),
            Some(
                "build the release binary with --features release-user-surfaces or vortex-local-primitives to execute materializing native Vortex primitive collect routes".to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn native_vortex_profile_route(request: &PublicWorkflowRouteRequest) -> PublicWorkflowRoutePlan {
    if !native_vortex_metadata_profile_shape_admitted(request) {
        return native_vortex_operation_blocked_route(NativeVortexOperationFamily::Profile);
    }
    admitted_route(
        "native_vortex_user_profile",
        "vortex-metadata-summary",
        "native_vortex_file",
        "native_vortex_metadata_profile",
        "native_vortex",
        false,
        true,
    )
}

fn native_vortex_metadata_profile_shape_admitted(request: &PublicWorkflowRouteRequest) -> bool {
    if request.requested_output != "profile" {
        return false;
    }
    let Some(summary) = request.plan_summary.as_deref() else {
        return true;
    };
    let Some(operations) = parse_plan_summary_operations(summary) else {
        return false;
    };
    let operations = strip_index_metadata_operations(&operations);
    if !summary_read_vortex_matches_input(&operations, request.input_uri.as_deref().unwrap_or("")) {
        return false;
    }
    operations
        .iter()
        .skip(1)
        .all(|operation| matches!(operation.kind, "select" | "limit"))
}

fn native_vortex_profile_projection_attachment_fields(
    request: &PublicWorkflowRouteRequest,
) -> Vec<(String, String)> {
    let projected_columns = request
        .plan_summary
        .as_deref()
        .and_then(|summary| profile_projection_columns_from_summary(summary, request));
    let (scope, columns) = match projected_columns {
        Some(columns) => ("selected_columns", columns),
        None => ("all_columns", "all".to_string()),
    };
    vec![
        (
            "public_workflow_profile_projection_scope".to_string(),
            scope.to_string(),
        ),
        (
            "public_workflow_profile_projected_columns".to_string(),
            columns,
        ),
        (
            "metadata_summary_projection_scope".to_string(),
            scope.to_string(),
        ),
    ]
}

fn profile_projection_columns_from_summary(
    summary: &str,
    request: &PublicWorkflowRouteRequest,
) -> Option<String> {
    let operations = parse_plan_summary_operations(summary)?;
    let operations = strip_index_metadata_operations(&operations);
    if !summary_read_vortex_matches_input(&operations, request.input_uri.as_deref()?) {
        return None;
    }
    let columns = operations
        .iter()
        .rev()
        .find(|operation| operation.kind == "select")?
        .arg
        .trim();
    if columns.is_empty() || columns == "*" {
        return None;
    }
    normalize_sql_projection_columns(columns)
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
    if !cfg!(feature = "vortex-production-runtime") {
        return native_vortex_provider_feature_gated_route();
    }
    if request.vortex_primitive.is_some() {
        return native_vortex_payload_blocked_route(
            "public_workflow_route.vortex_primitive",
            "native Vortex provider routes do not accept primitive payloads",
            "use primitive payloads for count/filter/project, or provider scenario payloads for promoted operator families",
        );
    }
    if is_write_request(request) && request.output_ref.is_none() {
        return output_required_route("route", "native Vortex result sink");
    }
    if request.output_ref.is_some() && !is_write_request(request) {
        return native_vortex_operation_blocked_route(NativeVortexOperationFamily::Sink);
    }
    if is_write_request(request)
        && !matches!(
            request.requested_output.as_str(),
            "write_vortex" | "write_jsonl" | "write_csv"
        )
    {
        return native_vortex_sink_format_blocked_route(request);
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
        VORTEX_PRODUCTION_RUNTIME_COMMAND,
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
        "native Vortex provider route requires the vortex-production-runtime feature",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "native Vortex provider-backed user routes are feature-gated in this binary".to_string(),
            Some("public_workflow_route.native_vortex_provider_scenario".to_string()),
            Some("compiled_without=vortex-production-runtime".to_string()),
            Some("build the release binary with --features release-user-surfaces or vortex-production-runtime to execute promoted native Vortex operator routes".to_string()),
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
        NativeVortexOperationFamily::Distinct => "native_vortex_user_distinct",
        NativeVortexOperationFamily::Profile => "native_vortex_user_profile",
        NativeVortexOperationFamily::Sink => "native_vortex_user_sink",
        NativeVortexOperationFamily::Count
        | NativeVortexOperationFamily::FilterProjectLimit
        | NativeVortexOperationFamily::Sample
        | NativeVortexOperationFamily::DuplicateMask
        | NativeVortexOperationFamily::ExpressionProject
        | NativeVortexOperationFamily::Melt
        | NativeVortexOperationFamily::Explode
        | NativeVortexOperationFamily::Pivot
        | NativeVortexOperationFamily::RollingWindow
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
            Some("prepare compatibility input into Vortex first, or pass input_format=vortex with an admitted native Vortex route".to_string()),
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

fn native_vortex_provider_schema_shape_blocker(
    request: &PublicWorkflowRouteRequest,
) -> Option<PublicWorkflowRoutePlan> {
    if !matches!(
        request.requested_output.as_str(),
        "collect" | "write_vortex" | "profile"
    ) {
        return None;
    }
    let operations = parse_plan_summary_operations(request.plan_summary.as_deref()?)?;
    let operations = strip_index_metadata_operations(&operations);
    if !summary_read_vortex_matches_input(&operations, request.input_uri.as_deref()?) {
        return None;
    }
    let family = provider_like_operation_family(request, &operations)?;
    if family == NativeVortexOperationFamily::Profile {
        return Some(native_vortex_profile_route(request));
    }
    if family == NativeVortexOperationFamily::Distinct {
        return Some(native_vortex_operation_blocked_route(family));
    }
    let plan_shape = operations
        .iter()
        .map(|operation| operation.kind)
        .collect::<Vec<_>>()
        .join("->");
    Some(blocked_route(
        "py-vortex-route-unify-1.native_vortex_provider_schema_shape_not_admitted",
        "native Vortex provider plan shape does not match an admitted schema contract",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "native Vortex provider route requires an exact admitted schema/operator shape"
                .to_string(),
            Some("public_workflow_route.plan_summary".to_string()),
            Some(format!(
                "operation_family={} plan_shape={} required_evidence={}",
                family.as_str(),
                plan_shape,
                family.required_evidence()
            )),
            Some(
                "use an admitted provider scenario shape or prepare the workflow into a Vortex-backed route before requesting provider execution".to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    ))
}

fn provider_like_operation_family(
    request: &PublicWorkflowRouteRequest,
    operations: &[SummaryOperation<'_>],
) -> Option<NativeVortexOperationFamily> {
    if request.requested_output == "write_vortex" {
        return Some(NativeVortexOperationFamily::Sink);
    }
    if request.requested_output == "profile" {
        return Some(NativeVortexOperationFamily::Profile);
    }
    if operations.iter().any(|operation| operation.kind == "join") {
        return Some(NativeVortexOperationFamily::Join);
    }
    if operations
        .iter()
        .any(|operation| matches!(operation.kind, "group_by" | "aggregate"))
    {
        return Some(NativeVortexOperationFamily::Aggregate);
    }
    if operations
        .iter()
        .any(|operation| operation.kind == "distinct")
    {
        return Some(NativeVortexOperationFamily::Distinct);
    }
    if operations
        .iter()
        .any(|operation| operation.kind == "rolling_window")
    {
        return Some(NativeVortexOperationFamily::RollingWindow);
    }
    if operations.iter().any(|operation| operation.kind == "sort") {
        return Some(NativeVortexOperationFamily::TopN);
    }
    if operations
        .iter()
        .any(|operation| operation.kind == "with_column")
    {
        return Some(NativeVortexOperationFamily::Cast);
    }
    if operations.iter().any(|operation| {
        let arg = compact_ascii_lower(operation.arg);
        arg.contains("nested_payload") && arg.contains("target")
    }) {
        return Some(NativeVortexOperationFamily::Contains);
    }
    None
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
    sample_seed: Option<String>,
    sample_fraction: Option<String>,
    sample_replacement: bool,
    duplicate_keep: Option<String>,
    explode_projection: Option<String>,
    pivot_projection: Option<String>,
    rolling_window: Option<String>,
    aggregate: Option<String>,
    sort_rows: Option<String>,
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
        if request.vortex_sample_seed.is_none() {
            request.vortex_sample_seed = self.sample_seed;
        }
        if request.vortex_sample_fraction.is_none() {
            request.vortex_sample_fraction = self.sample_fraction;
        }
        request.vortex_sample_replacement |= self.sample_replacement;
        if request.vortex_duplicate_keep.is_none() {
            request.vortex_duplicate_keep = self.duplicate_keep;
        }
        if request.vortex_explode_projection.is_none() {
            request.vortex_explode_projection = self.explode_projection;
        }
        if request.vortex_pivot_projection.is_none() {
            request.vortex_pivot_projection = self.pivot_projection;
        }
        if request.vortex_rolling_window.is_none() {
            request.vortex_rolling_window = self.rolling_window;
        }
        if request.vortex_aggregate.is_none() {
            request.vortex_aggregate = self.aggregate;
        }
        if request.vortex_sort_rows.is_none() {
            request.vortex_sort_rows = self.sort_rows;
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
    if request.native_vortex_provider_scenario.is_some() || request.vortex_primitive.is_some() {
        return None;
    }
    if request.surface == "sql" || request.sql_statement.is_some() {
        return infer_native_vortex_sql_payload(request);
    }
    infer_native_vortex_primitive_payload(request)
        .or_else(|| infer_native_vortex_provider_payload(request))
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
        "collect" | "write_vortex" | "write_jsonl" | "write_csv"
    ) {
        return None;
    }
    let operations = parse_plan_summary_operations(request.plan_summary.as_deref()?)?;
    let operations = strip_index_metadata_operations(&operations);
    if !summary_read_vortex_matches_input(&operations, request.input_uri.as_deref()?) {
        return None;
    }
    let write_request = is_write_request(request);

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
        family: if write_request {
            NativeVortexOperationFamily::Sink
        } else {
            family
        },
        provider_scenario: Some(provider_scenario),
        primitive: None,
        predicate: None,
        columns: None,
        source_order_limit: None,
        sample_seed: None,
        sample_fraction: None,
        sample_replacement: false,
        duplicate_keep: None,
        explode_projection: None,
        pivot_projection: None,
        rolling_window: None,
        aggregate: None,
        sort_rows: None,
        right_input: infer_native_vortex_right_input(request),
    })
}

fn infer_native_vortex_sql_payload(
    request: &PublicWorkflowRouteRequest,
) -> Option<InferredNativeVortexRoutePayload> {
    let statement = request.sql_statement.as_deref()?;
    infer_native_vortex_sql_provider_payload(statement, is_write_request(request))
        .or_else(|| infer_native_vortex_sql_primitive_payload(statement, request))
}

fn infer_native_vortex_sql_provider_payload(
    statement: &str,
    write_request: bool,
) -> Option<InferredNativeVortexRoutePayload> {
    let refs = quoted_source_refs(statement)
        .into_iter()
        .filter(|ref_| infer_input_format_from_ref(ref_) == Some("vortex"))
        .collect::<Vec<_>>();
    let first_ref = refs.first()?;
    if infer_input_format_from_ref(first_ref) != Some("vortex") {
        return None;
    }
    let compact = compact_ascii_lower(statement);
    let (family, provider_scenario) = if compact.contains("nullable_metric_00isnotnull")
        && compact.contains("groupbygroup_key")
        && compact.contains("sum(nullable_metric_00)astotal_nullable_metric")
    {
        (
            NativeVortexOperationFamily::Aggregate,
            "null-heavy-aggregate",
        )
    } else if compact.contains("groupbygroup_key")
        && compact.contains("count(*)asrows")
        && compact.contains("sum(metric)astotal_metric")
    {
        (
            NativeVortexOperationFamily::Aggregate,
            "group-by-aggregation",
        )
    } else if compact.contains("join")
        && compact.contains("f.id")
        && compact.contains("d.dim_label")
        && compact.contains("f.metric")
        && compact.contains("f.dim_key=d.dim_key")
    {
        (NativeVortexOperationFamily::Join, "hash-join")
    } else if compact.contains("orderbymetricdesc") && compact.contains("limit10") {
        (NativeVortexOperationFamily::TopN, "sort-and-top-k")
    } else if compact.contains("cast(dirty_numericasfloat64)asamount_float")
        && compact.contains("amount_float>=0")
    {
        (NativeVortexOperationFamily::Cast, "clean-cast-filter-write")
    } else if compact.contains("cast(raw_event_timeasdate32)asevent_day") {
        (
            NativeVortexOperationFamily::Cast,
            "malformed-timestamp-dirty-csv",
        )
    } else if compact.contains("nested_payload")
        && (compact.contains("like'%target%'") || compact.contains("contains('target')"))
    {
        (
            NativeVortexOperationFamily::Contains,
            "nested-json-field-scan",
        )
    } else {
        return None;
    };

    Some(InferredNativeVortexRoutePayload {
        family: if write_request {
            NativeVortexOperationFamily::Sink
        } else {
            family
        },
        provider_scenario: Some(provider_scenario),
        primitive: None,
        predicate: None,
        columns: None,
        source_order_limit: None,
        sample_seed: None,
        sample_fraction: None,
        sample_replacement: false,
        duplicate_keep: None,
        explode_projection: None,
        pivot_projection: None,
        rolling_window: None,
        aggregate: None,
        sort_rows: None,
        right_input: refs.get(1).cloned(),
    })
}

#[derive(Debug, Clone)]
struct NativeVortexSqlSingleSourceShape {
    projection: String,
    source_ref: String,
    where_clause: Option<String>,
    group_by: Option<String>,
    having: Option<String>,
    order_by: Option<String>,
    limit: Option<String>,
    offset: Option<String>,
}

#[allow(clippy::too_many_lines)]
fn infer_native_vortex_sql_primitive_payload(
    statement: &str,
    request: &PublicWorkflowRouteRequest,
) -> Option<InferredNativeVortexRoutePayload> {
    if !matches!(
        request.requested_output.as_str(),
        "collect" | "write_jsonl" | "write_csv"
    ) {
        return None;
    }
    let shape = parse_native_vortex_sql_single_source_shape(statement)?;
    if infer_input_format_from_ref(&shape.source_ref) != Some("vortex") {
        return None;
    }
    let projection = compact_ascii_lower(&shape.projection);
    if projection == "count(*)" {
        if is_write_request(request) {
            return None;
        }
        let predicate = match shape.where_clause.as_deref() {
            Some(where_clause) => Some(summary_tiny_predicate_from_sql(where_clause)?),
            None => None,
        };
        if predicate
            .as_deref()
            .is_some_and(tiny_predicate_requires_materialized_eval)
        {
            return Some(InferredNativeVortexRoutePayload {
                family: NativeVortexOperationFamily::Aggregate,
                provider_scenario: None,
                primitive: Some(PublicVortexPrimitive::Aggregate),
                predicate,
                columns: None,
                source_order_limit: None,
                sample_seed: None,
                sample_fraction: None,
                sample_replacement: false,
                duplicate_keep: None,
                explode_projection: None,
                pivot_projection: None,
                rolling_window: None,
                aggregate: Some(
                    serde_json::json!({
                        "measures": [{"function": "count", "alias": "count_all_0"}]
                    })
                    .to_string(),
                ),
                sort_rows: None,
                right_input: None,
            });
        }
        return Some(InferredNativeVortexRoutePayload {
            family: NativeVortexOperationFamily::Count,
            provider_scenario: None,
            primitive: Some(if predicate.is_some() {
                PublicVortexPrimitive::CountWhere
            } else {
                PublicVortexPrimitive::Count
            }),
            predicate,
            columns: None,
            source_order_limit: None,
            sample_seed: None,
            sample_fraction: None,
            sample_replacement: false,
            duplicate_keep: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            aggregate: None,
            sort_rows: None,
            right_input: None,
        });
    }
    if shape.group_by.is_none()
        && let (Some(order_by), Some(limit)) = (shape.order_by.as_deref(), shape.limit.as_deref())
    {
        let columns = if shape.projection.trim() == "*" {
            None
        } else {
            Some(normalize_sql_projection_columns(&shape.projection)?)
        };
        let predicate = match shape.where_clause.as_deref() {
            Some(where_clause) => Some(summary_tiny_predicate_from_sql(where_clause)?),
            None => None,
        };
        return Some(InferredNativeVortexRoutePayload {
            family: NativeVortexOperationFamily::TopN,
            provider_scenario: None,
            primitive: Some(PublicVortexPrimitive::SortRows),
            predicate,
            columns,
            source_order_limit: Some(limit.to_string()),
            sample_seed: None,
            sample_fraction: None,
            sample_replacement: false,
            duplicate_keep: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            aggregate: None,
            sort_rows: Some(sort_rows_payload_from_sql_order_by(
                order_by,
                shape.offset.as_deref(),
                limit,
            )?),
            right_input: None,
        });
    }
    if let Some(aggregate_payload) = aggregate_payload_from_sql_projection(
        &shape.projection,
        shape.group_by.as_deref(),
        shape.having.as_deref(),
        shape.order_by.as_deref(),
        shape.offset.as_deref(),
    ) {
        let predicate = match shape.where_clause.as_deref() {
            Some(where_clause) => Some(summary_tiny_predicate_from_sql(where_clause)?),
            None => None,
        };
        return Some(InferredNativeVortexRoutePayload {
            family: NativeVortexOperationFamily::Aggregate,
            provider_scenario: None,
            primitive: Some(PublicVortexPrimitive::Aggregate),
            predicate,
            columns: None,
            source_order_limit: shape.limit,
            sample_seed: None,
            sample_fraction: None,
            sample_replacement: false,
            duplicate_keep: None,
            explode_projection: None,
            pivot_projection: None,
            rolling_window: None,
            aggregate: Some(aggregate_payload),
            sort_rows: None,
            right_input: None,
        });
    }
    if shape.group_by.is_some() || shape.order_by.is_some() || shape.offset.is_some() {
        return None;
    }
    let columns = normalize_sql_projection_columns(&shape.projection);
    let predicate = match shape.where_clause.as_deref() {
        Some(where_clause) => Some(summary_tiny_predicate_from_sql(where_clause)?),
        None => None,
    };
    let primitive = match (predicate.is_some(), columns.is_some()) {
        (true, true) => PublicVortexPrimitive::FilterProject,
        (true, false) => PublicVortexPrimitive::Filter,
        (false, true) => PublicVortexPrimitive::Project,
        (false, false) => return None,
    };
    Some(InferredNativeVortexRoutePayload {
        family: NativeVortexOperationFamily::from_primitive(primitive),
        provider_scenario: None,
        primitive: Some(primitive),
        predicate,
        columns,
        source_order_limit: shape.limit,
        sample_seed: None,
        sample_fraction: None,
        sample_replacement: false,
        duplicate_keep: None,
        explode_projection: None,
        pivot_projection: None,
        rolling_window: None,
        aggregate: None,
        sort_rows: None,
        right_input: None,
    })
}

fn parse_native_vortex_sql_single_source_shape(
    statement: &str,
) -> Option<NativeVortexSqlSingleSourceShape> {
    let normalized = statement.trim().trim_end_matches(';').trim();
    if !sql_keyword_prefix(normalized, "SELECT") {
        return None;
    }
    let select_body = normalized["SELECT".len()..].trim();
    let from_position = find_sql_keyword_outside_quotes_and_parens(select_body, "FROM")?;
    let projection = select_body[..from_position].trim();
    let from_tail = select_body[from_position + "FROM".len()..].trim();
    let (source_ref, consumed) = leading_quoted_sql_literal_with_consumed(from_tail)?;
    let tail = from_tail[consumed..].trim();
    let tail = parse_native_vortex_sql_allowed_tail(tail)?;
    Some(NativeVortexSqlSingleSourceShape {
        projection: projection.to_string(),
        source_ref,
        where_clause: tail.where_clause,
        group_by: tail.group_by,
        having: tail.having,
        order_by: tail.order_by,
        limit: tail.limit,
        offset: tail.offset,
    })
}

#[derive(Debug, Clone, Default)]
struct NativeVortexSqlTailClauses {
    where_clause: Option<String>,
    group_by: Option<String>,
    having: Option<String>,
    order_by: Option<String>,
    limit: Option<String>,
    offset: Option<String>,
}

fn parse_native_vortex_sql_allowed_tail(tail: &str) -> Option<NativeVortexSqlTailClauses> {
    let mut rest = tail.trim();
    let mut clauses = NativeVortexSqlTailClauses::default();
    if rest.is_empty() {
        return Some(clauses);
    }
    if sql_keyword_prefix(rest, "WHERE") {
        rest = rest["WHERE".len()..].trim();
        let (clause, tail) =
            take_sql_clause_until(rest, &["GROUP BY", "HAVING", "ORDER BY", "LIMIT", "OFFSET"]);
        if clause.is_empty() {
            return None;
        }
        clauses.where_clause = Some(clause.to_string());
        rest = tail;
    }
    if sql_keyword_prefix(rest, "GROUP BY") {
        rest = rest["GROUP BY".len()..].trim();
        let (clause, tail) =
            take_sql_clause_until(rest, &["HAVING", "ORDER BY", "LIMIT", "OFFSET"]);
        if clause.is_empty() {
            return None;
        }
        clauses.group_by = Some(clause.to_string());
        rest = tail;
    }
    if sql_keyword_prefix(rest, "HAVING") {
        rest = rest["HAVING".len()..].trim();
        let (clause, tail) = take_sql_clause_until(rest, &["ORDER BY", "LIMIT", "OFFSET"]);
        if clause.is_empty() {
            return None;
        }
        clauses.having = Some(clause.to_string());
        rest = tail;
    }
    if sql_keyword_prefix(rest, "ORDER BY") {
        rest = rest["ORDER BY".len()..].trim();
        let (clause, tail) = take_sql_clause_until(rest, &["LIMIT", "OFFSET"]);
        if clause.is_empty() {
            return None;
        }
        clauses.order_by = Some(clause.to_string());
        rest = tail;
    }
    if sql_keyword_prefix(rest, "LIMIT") {
        rest = rest["LIMIT".len()..].trim();
        let (clause, tail) = take_sql_clause_until(rest, &["OFFSET"]);
        clauses.limit = Some(parse_native_vortex_sql_limit_literal(clause)?);
        rest = tail;
    }
    if sql_keyword_prefix(rest, "OFFSET") {
        rest = rest["OFFSET".len()..].trim();
        let (clause, tail) = take_sql_clause_until(rest, &[]);
        clauses.offset = Some(parse_native_vortex_sql_limit_literal(clause)?);
        rest = tail;
    }
    rest.trim().is_empty().then_some(clauses)
}

fn take_sql_clause_until<'a>(value: &'a str, keywords: &[&str]) -> (&'a str, &'a str) {
    if keywords.is_empty() {
        return (value.trim(), "");
    }
    let Some((position, _keyword)) =
        find_first_sql_keyword_outside_quotes_and_parens(value, keywords)
    else {
        return (value.trim(), "");
    };
    (value[..position].trim(), value[position..].trim())
}

fn find_first_sql_keyword_outside_quotes_and_parens<'a>(
    value: &str,
    keywords: &'a [&str],
) -> Option<(usize, &'a str)> {
    keywords
        .iter()
        .filter_map(|keyword| {
            find_sql_keyword_outside_quotes_and_parens(value, keyword)
                .map(|position| (position, *keyword))
        })
        .min_by_key(|(position, _keyword)| *position)
}

fn parse_native_vortex_sql_limit_literal(value: &str) -> Option<String> {
    let value = value.trim();
    let digit_count = value
        .as_bytes()
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if digit_count == 0 || !value[digit_count..].trim().is_empty() {
        return None;
    }
    let limit = &value[..digit_count];
    summary_positive_limit(limit).then(|| limit.to_string())
}

fn normalize_sql_projection_columns(projection: &str) -> Option<String> {
    let projection = projection.trim();
    if projection == "*" {
        return None;
    }
    let columns = projection
        .split(',')
        .map(str::trim)
        .map(|column| column.strip_prefix("f.").unwrap_or(column))
        .collect::<Vec<_>>();
    if columns.is_empty() || !columns.iter().all(|column| is_summary_identifier(column)) {
        return None;
    }
    Some(columns.join(","))
}

fn aggregate_payload_from_sql_projection(
    projection: &str,
    group_by: Option<&str>,
    having: Option<&str>,
    order_by: Option<&str>,
    offset: Option<&str>,
) -> Option<String> {
    let group_spec = if let Some(group_by) = group_by {
        parse_sql_group_by_spec(group_by, projection)?
    } else {
        ParsedSqlGroupSpec::default()
    };
    let mut measures = Vec::new();
    let mut parsed_measures = Vec::new();
    for (index, item) in split_sql_projection_list(projection)
        .into_iter()
        .enumerate()
    {
        if let Some(measure) = scalar_aggregate_measure_from_sql(index, item) {
            measures.push(measure.payload.clone());
            parsed_measures.push(measure);
            continue;
        }
        let projected_group_raw = strip_sql_alias(item.trim()).trim();
        let projected_group = normalize_sql_group_item(projected_group_raw);
        let projected_group_alias = sql_explicit_alias(item.trim());
        let direct_group_matches = projected_group.as_ref().is_some_and(|projected_group| {
            group_spec
                .columns
                .iter()
                .any(|column| column == projected_group)
        });
        let expression_group_matches = group_spec.expressions.iter().any(|expression| {
            expression.expression_key == compact_ascii_lower(projected_group_raw)
                || projected_group_alias.is_some_and(|alias| alias == expression.alias)
        });
        if !direct_group_matches && !expression_group_matches {
            return None;
        }
    }
    if group_spec.columns.is_empty() && group_spec.expressions.is_empty() && group_by.is_some() {
        return None;
    }
    if measures.is_empty() {
        return None;
    }
    let order_by = match order_by {
        Some(order_by) => parse_sql_aggregate_order_by(order_by, &group_spec, &parsed_measures)?,
        None => Vec::new(),
    };
    let having = match having {
        Some(having) => parse_sql_aggregate_having(having, &parsed_measures)?,
        None => Vec::new(),
    };
    let mut payload = serde_json::Map::new();
    if !group_spec.columns.is_empty() {
        payload.insert(
            "group_by".to_string(),
            serde_json::Value::Array(
                group_spec
                    .columns
                    .iter()
                    .map(|column| serde_json::Value::String(column.clone()))
                    .collect(),
            ),
        );
    }
    if !group_spec.expressions.is_empty() {
        payload.insert(
            "group_expressions".to_string(),
            serde_json::Value::Array(
                group_spec
                    .expressions
                    .iter()
                    .map(|expression| expression.payload.clone())
                    .collect(),
            ),
        );
    }
    payload.insert("measures".to_string(), serde_json::Value::Array(measures));
    if !order_by.is_empty() {
        payload.insert("order_by".to_string(), serde_json::Value::Array(order_by));
    }
    if !having.is_empty() {
        payload.insert("having".to_string(), serde_json::Value::Array(having));
    }
    if let Some(offset) = offset {
        let parsed = offset.parse::<usize>().ok()?;
        if parsed > 0 {
            payload.insert(
                "offset".to_string(),
                serde_json::Value::Number(serde_json::Number::from(parsed)),
            );
        }
    }
    Some(serde_json::Value::Object(payload).to_string())
}

fn sort_rows_payload_from_sql_order_by(
    order_by: &str,
    offset: Option<&str>,
    limit: &str,
) -> Option<String> {
    let order_by = split_sql_projection_list(order_by)
        .into_iter()
        .map(|item| {
            let (expression, descending) = parse_sql_order_item(item);
            let column = normalize_sql_group_item(strip_sql_alias(expression).trim())?;
            Some(serde_json::json!({
                "column": column,
                "descending": descending
            }))
        })
        .collect::<Option<Vec<_>>>()?;
    if order_by.is_empty() {
        return None;
    }
    let parsed_limit = limit.parse::<usize>().ok()?;
    if parsed_limit == 0 {
        return None;
    }
    let mut payload = serde_json::Map::new();
    payload.insert("order_by".to_string(), serde_json::Value::Array(order_by));
    payload.insert(
        "limit".to_string(),
        serde_json::Value::Number(serde_json::Number::from(parsed_limit)),
    );
    if let Some(offset) = offset {
        let parsed = offset.parse::<usize>().ok()?;
        if parsed > 0 {
            payload.insert(
                "offset".to_string(),
                serde_json::Value::Number(serde_json::Number::from(parsed)),
            );
        }
    }
    Some(serde_json::Value::Object(payload).to_string())
}

#[derive(Debug, Clone)]
struct ParsedSqlAggregateMeasure {
    payload: serde_json::Value,
    alias: String,
    expression_key: String,
}

#[derive(Debug, Clone)]
struct ParsedSqlGroupExpression {
    payload: serde_json::Value,
    alias: String,
    expression_key: String,
}

#[derive(Debug, Clone, Default)]
struct ParsedSqlGroupSpec {
    columns: Vec<String>,
    expressions: Vec<ParsedSqlGroupExpression>,
}

fn scalar_aggregate_measure_from_sql(
    index: usize,
    item: &str,
) -> Option<ParsedSqlAggregateMeasure> {
    let raw_item = item.trim();
    let alias = sql_explicit_alias(raw_item);
    let item = strip_sql_alias(raw_item).trim();
    let open = item.find('(')?;
    if !item.ends_with(')') || open == 0 {
        return None;
    }
    let function = item[..open].trim().to_ascii_lowercase();
    if !matches!(function.as_str(), "sum" | "count" | "avg" | "min" | "max") {
        return None;
    }
    let argument = item[open + 1..item.len() - 1].trim();
    if function == "count" && argument == "*" {
        let alias = alias
            .filter(|alias| is_summary_identifier(alias))
            .map_or_else(|| format!("count_all_{index}"), str::to_string);
        return Some(ParsedSqlAggregateMeasure {
            payload: serde_json::json!({
            "function": "count",
            "alias": alias
            }),
            alias,
            expression_key: compact_ascii_lower(item),
        });
    }
    if function == "count"
        && let Some(distinct_argument) = argument
            .strip_prefix("DISTINCT ")
            .or_else(|| argument.strip_prefix("distinct "))
    {
        let column = distinct_argument
            .trim()
            .strip_prefix("f.")
            .unwrap_or(distinct_argument.trim());
        if !is_summary_identifier(column) {
            return None;
        }
        let alias = alias
            .filter(|alias| is_summary_identifier(alias))
            .map_or_else(
                || format!("count_distinct_{column}_{index}"),
                str::to_string,
            );
        return Some(ParsedSqlAggregateMeasure {
            payload: serde_json::json!({
                "function": "count_distinct",
                "column": column,
                "alias": alias
            }),
            alias,
            expression_key: compact_ascii_lower(item),
        });
    }
    let (column, argument_offset, value_transform) =
        if let Some(column) = parse_sql_length_expression(argument) {
            (column, 0, Some("length"))
        } else if function == "sum" {
            let (column, offset) = parse_sql_additive_aggregate_argument(argument)
                .unwrap_or_else(|| (argument.strip_prefix("f.").unwrap_or(argument).trim(), 0));
            (column, offset, None)
        } else {
            (
                argument.strip_prefix("f.").unwrap_or(argument).trim(),
                0,
                None,
            )
        };
    if !is_summary_identifier(column) {
        return None;
    }
    let alias = alias
        .filter(|alias| is_summary_identifier(alias))
        .map_or_else(|| format!("{function}_{column}_{index}"), str::to_string);
    let mut payload = serde_json::json!({
        "function": function,
        "column": column,
        "alias": alias
    });
    if argument_offset != 0
        && let serde_json::Value::Object(ref mut object) = payload
    {
        object.insert(
            "argument_offset".to_string(),
            serde_json::Value::Number(serde_json::Number::from(argument_offset)),
        );
    }
    if let Some(value_transform) = value_transform
        && let serde_json::Value::Object(ref mut object) = payload
    {
        object.insert(
            "value_transform".to_string(),
            serde_json::Value::String(value_transform.to_string()),
        );
    }
    Some(ParsedSqlAggregateMeasure {
        payload,
        alias,
        expression_key: compact_ascii_lower(item),
    })
}

fn parse_sql_additive_aggregate_argument(argument: &str) -> Option<(&str, i64)> {
    let argument = argument.trim();
    for operator in ['+', '-'] {
        let Some((column, literal)) = argument.rsplit_once(operator) else {
            continue;
        };
        let column = column.trim().strip_prefix("f.").unwrap_or(column.trim());
        if !is_summary_identifier(column) {
            continue;
        }
        let literal = literal.trim().parse::<i64>().ok()?;
        let offset = if operator == '-' {
            literal.checked_neg()?
        } else {
            literal
        };
        return Some((column, offset));
    }
    let column = argument.strip_prefix("f.").unwrap_or(argument).trim();
    is_summary_identifier(column).then_some((column, 0))
}

fn parse_sql_group_by_spec(group_by: &str, projection: &str) -> Option<ParsedSqlGroupSpec> {
    let mut spec = ParsedSqlGroupSpec::default();
    let projection_items = split_sql_projection_list(projection);
    let projection_expressions = split_sql_projection_list(projection)
        .into_iter()
        .filter_map(parse_sql_group_expression)
        .collect::<Vec<_>>();
    let fallback_column = split_sql_projection_list(group_by)
        .into_iter()
        .find_map(normalize_sql_group_item)
        .or_else(|| {
            projection_items
                .iter()
                .find_map(|item| normalize_sql_group_item(strip_sql_alias(item.trim()).trim()))
        });
    for item in split_sql_projection_list(group_by) {
        if let Ok(ordinal) = item.trim().parse::<usize>()
            && ordinal > 0
            && let Some(projection_item) = projection_items.get(ordinal - 1)
            && let Ok(value) = strip_sql_alias(projection_item.trim())
                .trim()
                .parse::<i64>()
        {
            let column = fallback_column.clone()?;
            let alias = format!("literal_{value}");
            spec.expressions.push(ParsedSqlGroupExpression {
                payload: serde_json::json!({
                    "alias": alias,
                    "column": column,
                    "function": "constant_int",
                    "argument_offset": value
                }),
                alias,
                expression_key: value.to_string(),
            });
            continue;
        }
        if let Some(expression) = projection_expressions
            .iter()
            .find(|expression| expression.alias.eq_ignore_ascii_case(item.trim()))
            .cloned()
        {
            if !spec
                .expressions
                .iter()
                .any(|existing| existing.alias == expression.alias)
            {
                spec.expressions.push(expression);
            }
            continue;
        }
        if let Some(column) = normalize_sql_group_item(item) {
            if !spec.columns.iter().any(|existing| existing == &column) {
                spec.columns.push(column);
            }
            continue;
        }
        let key = compact_ascii_lower(strip_sql_alias(item.trim()).trim());
        let expression = projection_expressions
            .iter()
            .find(|expression| {
                expression.expression_key == key
                    || expression.alias.eq_ignore_ascii_case(item.trim())
            })
            .cloned()
            .or_else(|| parse_sql_group_expression(item))?;
        if !spec
            .expressions
            .iter()
            .any(|existing| existing.alias == expression.alias)
        {
            spec.expressions.push(expression);
        }
    }
    Some(spec)
}

#[allow(clippy::too_many_lines)]
fn parse_sql_group_expression(item: &str) -> Option<ParsedSqlGroupExpression> {
    let alias = sql_explicit_alias(item)
        .filter(|alias| is_summary_identifier(alias))
        .map(str::to_string);
    let expression = strip_sql_alias(item.trim()).trim();
    let expression_key = compact_ascii_lower(expression);
    if let Some(alias) = alias.clone()
        && let Some(column) = normalize_sql_group_item(expression)
        && alias != column
    {
        return Some(ParsedSqlGroupExpression {
            payload: serde_json::json!({
                "alias": alias,
                "column": column,
                "function": "identity"
            }),
            alias,
            expression_key,
        });
    }
    if let Some((column, offset)) = parse_sql_additive_aggregate_argument(expression)
        && offset != 0
    {
        let alias = alias.unwrap_or_else(|| {
            format!(
                "{}_{}{}",
                column,
                if offset < 0 { "minus" } else { "plus" },
                offset.unsigned_abs()
            )
        });
        return Some(ParsedSqlGroupExpression {
            payload: serde_json::json!({
                "alias": alias,
                "column": column,
                "function": "add_offset",
                "argument_offset": offset
            }),
            alias,
            expression_key,
        });
    }
    if let Some(column) = parse_sql_length_expression(expression) {
        let alias = alias.unwrap_or_else(|| format!("length_{column}"));
        return Some(ParsedSqlGroupExpression {
            payload: serde_json::json!({
                "alias": alias,
                "column": column,
                "function": "length"
            }),
            alias,
            expression_key,
        });
    }
    if let Some(column) = parse_sql_extract_minute_expression(expression) {
        let alias = alias.unwrap_or_else(|| "minute".to_string());
        return Some(ParsedSqlGroupExpression {
            payload: serde_json::json!({
                "alias": alias,
                "column": column,
                "function": "extract_minute"
            }),
            alias,
            expression_key,
        });
    }
    if let Some(column) = parse_sql_date_trunc_minute_expression(expression) {
        let alias = alias.unwrap_or_else(|| "date_trunc_minute".to_string());
        return Some(ParsedSqlGroupExpression {
            payload: serde_json::json!({
                "alias": alias,
                "column": column,
                "function": "date_trunc_minute"
            }),
            alias,
            expression_key,
        });
    }
    if let Some(column) = parse_sql_regex_domain_expression(expression) {
        let alias = alias.unwrap_or_else(|| "domain".to_string());
        return Some(ParsedSqlGroupExpression {
            payload: serde_json::json!({
                "alias": alias,
                "column": column,
                "function": "regex_domain"
            }),
            alias,
            expression_key,
        });
    }
    if let Some((referer, search_engine, adv_engine)) =
        parse_sql_case_search_adv_zero_referer_expression(expression)
    {
        let alias = alias.unwrap_or_else(|| "traffic_source_referer".to_string());
        return Some(ParsedSqlGroupExpression {
            payload: serde_json::json!({
                "alias": alias,
                "column": referer,
                "extra_columns": [search_engine, adv_engine],
                "function": "case_search_adv_zero_referer_else_empty"
            }),
            alias,
            expression_key,
        });
    }
    None
}

fn parse_sql_length_expression(expression: &str) -> Option<&str> {
    let inner = parse_sql_function_call(expression, "length")?;
    let column = inner.trim().strip_prefix("f.").unwrap_or(inner.trim());
    is_summary_identifier(column).then_some(column)
}

fn parse_sql_extract_minute_expression(expression: &str) -> Option<&str> {
    let normalized = expression.trim();
    let inner = parse_sql_function_call(normalized, "extract")?;
    let (unit, tail) = inner
        .split_once("FROM")
        .or_else(|| inner.split_once("from"))?;
    if !unit.trim().eq_ignore_ascii_case("minute") {
        return None;
    }
    let column = tail.trim().strip_prefix("f.").unwrap_or(tail.trim());
    is_summary_identifier(column).then_some(column)
}

fn parse_sql_date_trunc_minute_expression(expression: &str) -> Option<&str> {
    let inner = parse_sql_function_call(expression, "date_trunc")?;
    let parts = split_sql_projection_list(inner);
    if parts.len() != 2 {
        return None;
    }
    if !parse_sql_single_quoted_literal(parts[0])?.eq_ignore_ascii_case("minute") {
        return None;
    }
    let column = parts[1]
        .trim()
        .strip_prefix("f.")
        .unwrap_or(parts[1].trim());
    is_summary_identifier(column).then_some(column)
}

fn parse_sql_regex_domain_expression(expression: &str) -> Option<&str> {
    let inner = parse_sql_function_call(expression, "regexp_replace")?;
    let parts = split_sql_projection_list(inner);
    if parts.len() != 3 {
        return None;
    }
    let column = parts[0]
        .trim()
        .strip_prefix("f.")
        .unwrap_or(parts[0].trim());
    let pattern = parse_sql_single_quoted_literal(parts[1])?;
    let replacement = parse_sql_single_quoted_literal(parts[2])?;
    (is_summary_identifier(column)
        && pattern.contains("https?")
        && pattern.contains("([^/]+)")
        && replacement == "\\1")
        .then_some(column)
}

fn parse_sql_case_search_adv_zero_referer_expression(
    expression: &str,
) -> Option<(&str, &str, &str)> {
    let lowered = expression.trim().to_ascii_lowercase();
    if !lowered.starts_with("case when ")
        || !lowered.contains(" then ")
        || !lowered.contains(" else ")
        || !lowered.ends_with(" end")
    {
        return None;
    }
    let when_tail = expression.trim()[4..].trim();
    let when_tail = when_tail
        .strip_prefix("WHEN")
        .or_else(|| when_tail.strip_prefix("when"))?
        .trim();
    let then_position = find_sql_keyword_outside_quotes_and_parens(when_tail, "THEN")?;
    let condition = when_tail[..then_position].trim();
    let then_tail = when_tail[then_position + "THEN".len()..].trim();
    let else_position = find_sql_keyword_outside_quotes_and_parens(then_tail, "ELSE")?;
    let then_value = then_tail[..else_position].trim();
    let else_tail = then_tail[else_position + "ELSE".len()..].trim();
    let else_value = else_tail
        .strip_suffix("END")
        .or_else(|| else_tail.strip_suffix("end"))?
        .trim();
    if !parse_sql_single_quoted_literal(else_value)?.is_empty() {
        return None;
    }
    let referer = then_value
        .trim()
        .strip_prefix("f.")
        .unwrap_or(then_value.trim());
    if !is_summary_identifier(referer) {
        return None;
    }
    let predicates = split_sql_conjunction_predicates(
        condition
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
            .unwrap_or(condition),
    );
    let mut zero_columns = Vec::new();
    for predicate in predicates {
        let (column, literal) = predicate.split_once('=')?;
        if summary_sql_literal_to_tiny_scalar(literal.trim())? != "0" {
            return None;
        }
        let column = column.trim().strip_prefix("f.").unwrap_or(column.trim());
        if !is_summary_identifier(column) {
            return None;
        }
        zero_columns.push(column);
    }
    let search_engine = zero_columns
        .iter()
        .find(|column| column.eq_ignore_ascii_case("SearchEngineID"))?;
    let adv_engine = zero_columns
        .iter()
        .find(|column| column.eq_ignore_ascii_case("AdvEngineID"))?;
    Some((referer, search_engine, adv_engine))
}

fn parse_sql_function_call<'a>(expression: &'a str, function: &str) -> Option<&'a str> {
    let trimmed = expression.trim();
    let open = trimmed.find('(')?;
    if !trimmed[..open].trim().eq_ignore_ascii_case(function) || !trimmed.ends_with(')') {
        return None;
    }
    Some(&trimmed[open + 1..trimmed.len() - 1])
}

fn parse_sql_aggregate_order_by(
    order_by: &str,
    group_spec: &ParsedSqlGroupSpec,
    measures: &[ParsedSqlAggregateMeasure],
) -> Option<Vec<serde_json::Value>> {
    split_sql_projection_list(order_by)
        .into_iter()
        .map(|item| {
            let (expression, descending) = parse_sql_order_item(item);
            let expression = strip_sql_alias(expression).trim();
            let column = normalize_sql_group_item(expression)
                .filter(|column| group_spec.columns.iter().any(|group| group == column))
                .or_else(|| {
                    group_spec
                        .expressions
                        .iter()
                        .find(|group_expression| {
                            group_expression.alias.eq_ignore_ascii_case(expression)
                                || group_expression.expression_key
                                    == compact_ascii_lower(expression)
                        })
                        .map(|group_expression| group_expression.alias.clone())
                })
                .or_else(|| {
                    measures
                        .iter()
                        .find(|measure| {
                            measure.alias.eq_ignore_ascii_case(expression)
                                || measure.expression_key == compact_ascii_lower(expression)
                        })
                        .map(|measure| measure.alias.clone())
                })?;
            Some(serde_json::json!({
                "column": column,
                "descending": descending
            }))
        })
        .collect()
}

fn parse_sql_aggregate_having(
    having: &str,
    measures: &[ParsedSqlAggregateMeasure],
) -> Option<Vec<serde_json::Value>> {
    split_sql_conjunction_predicates(having)
        .into_iter()
        .map(|predicate| {
            for (sql_op, op) in [
                (">=", "gte"),
                ("<=", "lte"),
                ("!=", "neq"),
                ("<>", "neq"),
                (">", "gt"),
                ("<", "lt"),
                ("=", "eq"),
            ] {
                let Some((left, right)) = predicate.split_once(sql_op) else {
                    continue;
                };
                let left = left.trim();
                let column = measures
                    .iter()
                    .find(|measure| {
                        measure.alias.eq_ignore_ascii_case(left)
                            || measure.expression_key == compact_ascii_lower(left)
                    })
                    .map(|measure| measure.alias.clone())?;
                let value = summary_sql_literal_to_tiny_scalar(right.trim())?;
                return Some(serde_json::json!({
                    "column": column,
                    "op": op,
                    "value": value
                }));
            }
            None
        })
        .collect()
}

fn parse_sql_order_item(item: &str) -> (&str, bool) {
    let item = item.trim();
    if let Some(position) = find_sql_trailing_keyword(item, "DESC") {
        return (item[..position].trim(), true);
    }
    if let Some(position) = find_sql_trailing_keyword(item, "ASC") {
        return (item[..position].trim(), false);
    }
    (item, false)
}

fn find_sql_trailing_keyword(value: &str, keyword: &str) -> Option<usize> {
    let trimmed = value.trim_end();
    let suffix_start = trimmed.len().checked_sub(keyword.len())?;
    let suffix = &trimmed[suffix_start..];
    if !suffix.eq_ignore_ascii_case(keyword) {
        return None;
    }
    if suffix_start == 0 {
        return Some(0);
    }
    trimmed.as_bytes()[suffix_start - 1]
        .is_ascii_whitespace()
        .then_some(suffix_start - 1)
}

fn normalize_sql_group_item(item: &str) -> Option<String> {
    let item = item.trim();
    let column = item.strip_prefix("f.").unwrap_or(item);
    is_summary_identifier(column).then(|| column.to_string())
}

fn split_sql_projection_list(projection: &str) -> Vec<&str> {
    let mut items = Vec::new();
    let mut start = 0usize;
    let mut depth = 0usize;
    let mut in_single_quote = false;
    let bytes = projection.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        match byte {
            b'\'' => {
                in_single_quote = !in_single_quote;
            }
            b'(' if !in_single_quote => depth = depth.saturating_add(1),
            b')' if !in_single_quote => depth = depth.saturating_sub(1),
            b',' if !in_single_quote && depth == 0 => {
                items.push(projection[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
        index += 1;
    }
    items.push(projection[start..].trim());
    items.into_iter().filter(|item| !item.is_empty()).collect()
}

fn strip_sql_alias(item: &str) -> &str {
    find_sql_keyword_outside_quotes_and_parens(item, "AS")
        .map_or(item, |position| item[..position].trim())
}

fn sql_explicit_alias(item: &str) -> Option<&str> {
    let position = find_sql_keyword_outside_quotes_and_parens(item, "AS")?;
    let alias = item[position + "AS".len()..].trim();
    (!alias.is_empty()).then_some(alias)
}

#[derive(Debug, Clone, Copy)]
struct SummaryOperation<'a> {
    kind: &'a str,
    arg: &'a str,
}

#[derive(Debug, Clone)]
struct SummarySampleShape {
    source_order_limit: Option<String>,
    sample_fraction: Option<String>,
    sample_seed: String,
    sample_replacement: bool,
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

fn strip_index_metadata_operations<'a>(
    operations: &[SummaryOperation<'a>],
) -> Vec<SummaryOperation<'a>> {
    operations
        .iter()
        .copied()
        .filter(|operation| operation.kind != "set_index")
        .collect()
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
    if !matches!(
        request.requested_output.as_str(),
        "collect" | "write_jsonl" | "write_csv"
    ) {
        return None;
    }
    let operations = parse_plan_summary_operations(request.plan_summary.as_deref()?)?;
    let operations = strip_index_metadata_operations(&operations);
    if !summary_read_vortex_matches_input(&operations, request.input_uri.as_deref()?) {
        return None;
    }
    infer_native_vortex_sample_primitive_payload(&operations)
        .or_else(|| infer_native_vortex_explode_primitive_payload(&operations))
        .or_else(|| infer_native_vortex_rolling_window_primitive_payload(&operations))
        .or_else(|| infer_native_vortex_sort_rows_primitive_payload(&operations))
        .or_else(|| infer_native_vortex_tail_primitive_payload(&operations))
        .or_else(|| infer_native_vortex_duplicate_mask_primitive_payload(&operations))
        .or_else(|| infer_native_vortex_distinct_primitive_payload(&operations))
        .or_else(|| infer_native_vortex_basic_primitive_payload(&operations))
}

fn native_vortex_primitive_payload(
    primitive: PublicVortexPrimitive,
    predicate: Option<String>,
    columns: Option<String>,
    source_order_limit: Option<String>,
) -> InferredNativeVortexRoutePayload {
    native_vortex_primitive_payload_with_seed(
        primitive,
        predicate,
        columns,
        source_order_limit,
        None,
    )
}

fn native_vortex_primitive_payload_with_seed(
    primitive: PublicVortexPrimitive,
    predicate: Option<String>,
    columns: Option<String>,
    source_order_limit: Option<String>,
    sample_seed: Option<String>,
) -> InferredNativeVortexRoutePayload {
    InferredNativeVortexRoutePayload {
        family: NativeVortexOperationFamily::from_primitive(primitive),
        provider_scenario: None,
        primitive: Some(primitive),
        predicate,
        columns,
        source_order_limit,
        sample_seed,
        sample_fraction: None,
        sample_replacement: false,
        duplicate_keep: None,
        explode_projection: None,
        pivot_projection: None,
        rolling_window: None,
        aggregate: None,
        sort_rows: None,
        right_input: None,
    }
}

fn native_vortex_sample_primitive_payload(
    predicate: Option<String>,
    columns: Option<String>,
    sample_shape: SummarySampleShape,
) -> InferredNativeVortexRoutePayload {
    InferredNativeVortexRoutePayload {
        family: NativeVortexOperationFamily::Sample,
        provider_scenario: None,
        primitive: Some(PublicVortexPrimitive::Sample),
        predicate,
        columns,
        source_order_limit: sample_shape.source_order_limit,
        sample_seed: Some(sample_shape.sample_seed),
        sample_fraction: sample_shape.sample_fraction,
        sample_replacement: sample_shape.sample_replacement,
        duplicate_keep: None,
        explode_projection: None,
        pivot_projection: None,
        rolling_window: None,
        aggregate: None,
        sort_rows: None,
        right_input: None,
    }
}

fn native_vortex_explode_primitive_payload(
    payload: String,
    columns: Option<String>,
    source_order_limit: Option<String>,
) -> InferredNativeVortexRoutePayload {
    InferredNativeVortexRoutePayload {
        family: NativeVortexOperationFamily::Explode,
        provider_scenario: None,
        primitive: Some(PublicVortexPrimitive::Explode),
        predicate: None,
        columns,
        source_order_limit,
        sample_seed: None,
        sample_fraction: None,
        sample_replacement: false,
        duplicate_keep: None,
        explode_projection: Some(payload),
        pivot_projection: None,
        rolling_window: None,
        aggregate: None,
        sort_rows: None,
        right_input: None,
    }
}

fn infer_native_vortex_explode_primitive_payload(
    operations: &[SummaryOperation<'_>],
) -> Option<InferredNativeVortexRoutePayload> {
    if matches_summary_kinds(operations, &["read_vortex", "explode", "limit"])
        && summary_positive_limit(operations[2].arg)
    {
        let (payload, column) = summary_explode_projection_payload(operations[1].arg)?;
        Some(native_vortex_explode_primitive_payload(
            payload,
            Some(column),
            Some(operations[2].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "explode"]) {
        let (payload, column) = summary_explode_projection_payload(operations[1].arg)?;
        Some(native_vortex_explode_primitive_payload(
            payload,
            Some(column),
            None,
        ))
    } else {
        None
    }
}

fn summary_explode_projection_payload(value: &str) -> Option<(String, String)> {
    let column = value.trim();
    if !is_summary_identifier(column) {
        return None;
    }
    let payload = serde_json::json!({ "column": column }).to_string();
    Some((payload, column.to_string()))
}

fn native_vortex_rolling_window_primitive_payload(
    payload: String,
    columns: Option<String>,
    source_order_limit: Option<String>,
) -> InferredNativeVortexRoutePayload {
    InferredNativeVortexRoutePayload {
        family: NativeVortexOperationFamily::RollingWindow,
        provider_scenario: None,
        primitive: Some(PublicVortexPrimitive::RollingWindow),
        predicate: None,
        columns,
        source_order_limit,
        sample_seed: None,
        sample_fraction: None,
        sample_replacement: false,
        duplicate_keep: None,
        explode_projection: None,
        pivot_projection: None,
        rolling_window: Some(payload),
        aggregate: None,
        sort_rows: None,
        right_input: None,
    }
}

fn infer_native_vortex_rolling_window_primitive_payload(
    operations: &[SummaryOperation<'_>],
) -> Option<InferredNativeVortexRoutePayload> {
    if matches_summary_kinds(operations, &["read_vortex", "rolling_window", "limit"])
        && summary_positive_limit(operations[2].arg)
    {
        let payload = summary_rolling_window_payload(operations[1].arg)?;
        let columns = summary_rolling_window_source_column(&payload)?;
        Some(native_vortex_rolling_window_primitive_payload(
            payload,
            Some(columns),
            Some(operations[2].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "rolling_window"]) {
        let payload = summary_rolling_window_payload(operations[1].arg)?;
        let columns = summary_rolling_window_source_column(&payload)?;
        Some(native_vortex_rolling_window_primitive_payload(
            payload,
            Some(columns),
            None,
        ))
    } else {
        None
    }
}

fn summary_rolling_window_payload(value: &str) -> Option<String> {
    let payload = value.trim();
    (!payload.is_empty()).then(|| payload.to_string())
}

fn summary_rolling_window_source_column(payload: &str) -> Option<String> {
    let value = serde_json::from_str::<serde_json::Value>(payload).ok()?;
    let column = value
        .get("source_column")
        .or_else(|| value.get("column"))
        .or_else(|| value.get("on"))
        .and_then(serde_json::Value::as_str)?
        .trim();
    is_summary_identifier(column).then(|| column.to_string())
}

fn native_vortex_sort_rows_primitive_payload(
    predicate: Option<String>,
    columns: Option<String>,
    source_order_limit: String,
    sort_rows: String,
) -> InferredNativeVortexRoutePayload {
    InferredNativeVortexRoutePayload {
        family: NativeVortexOperationFamily::TopN,
        provider_scenario: None,
        primitive: Some(PublicVortexPrimitive::SortRows),
        predicate,
        columns,
        source_order_limit: Some(source_order_limit),
        sample_seed: None,
        sample_fraction: None,
        sample_replacement: false,
        duplicate_keep: None,
        explode_projection: None,
        pivot_projection: None,
        rolling_window: None,
        aggregate: None,
        sort_rows: Some(sort_rows),
        right_input: None,
    }
}

fn infer_native_vortex_sort_rows_primitive_payload(
    operations: &[SummaryOperation<'_>],
) -> Option<InferredNativeVortexRoutePayload> {
    if matches_summary_kinds(operations, &["read_vortex", "select", "sort", "limit"])
        && summary_positive_limit(operations[3].arg)
    {
        let limit = operations[3].arg.trim().to_string();
        return Some(native_vortex_sort_rows_primitive_payload(
            None,
            Some(operations[1].arg.trim().to_string()),
            limit.clone(),
            summary_sort_rows_payload(operations[2].arg, &limit)?,
        ));
    }
    if matches_summary_kinds(
        operations,
        &["read_vortex", "filter", "select", "sort", "limit"],
    ) && summary_positive_limit(operations[4].arg)
    {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        let limit = operations[4].arg.trim().to_string();
        return Some(native_vortex_sort_rows_primitive_payload(
            Some(predicate),
            Some(operations[2].arg.trim().to_string()),
            limit.clone(),
            summary_sort_rows_payload(operations[3].arg, &limit)?,
        ));
    }
    None
}

fn summary_sort_rows_payload(sort: &str, limit: &str) -> Option<String> {
    let parts = sort.split(',').map(str::trim).collect::<Vec<_>>();
    let (descending, column) = match parts.as_slice() {
        ["desc" | "DESC", column] | [column, "desc" | "DESC"] => (true, *column),
        ["asc" | "ASC", column] | [column, "asc" | "ASC"] | [column] => (false, *column),
        _ => return None,
    };
    if !is_summary_identifier(column) {
        return None;
    }
    let parsed_limit = limit.parse::<usize>().ok()?;
    if parsed_limit == 0 {
        return None;
    }
    Some(
        serde_json::json!({
            "order_by": [{"column": column, "descending": descending}],
            "limit": parsed_limit
        })
        .to_string(),
    )
}

fn infer_native_vortex_distinct_primitive_payload(
    operations: &[SummaryOperation<'_>],
) -> Option<InferredNativeVortexRoutePayload> {
    if matches_summary_kinds(
        operations,
        &["read_vortex", "filter", "select", "distinct", "limit"],
    ) && summary_positive_limit(operations[4].arg)
    {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Distinct,
            Some(predicate),
            Some(operations[2].arg.trim().to_string()),
            Some(operations[4].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "filter", "select", "distinct"]) {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Distinct,
            Some(predicate),
            Some(operations[2].arg.trim().to_string()),
            None,
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "filter", "distinct", "limit"])
        && summary_positive_limit(operations[3].arg)
    {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Distinct,
            Some(predicate),
            None,
            Some(operations[3].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "filter", "distinct"]) {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Distinct,
            Some(predicate),
            None,
            None,
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "select", "distinct", "limit"])
        && summary_positive_limit(operations[3].arg)
    {
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Distinct,
            None,
            Some(operations[1].arg.trim().to_string()),
            Some(operations[3].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "select", "distinct"]) {
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Distinct,
            None,
            Some(operations[1].arg.trim().to_string()),
            None,
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "distinct", "limit"])
        && summary_positive_limit(operations[2].arg)
    {
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Distinct,
            None,
            None,
            Some(operations[2].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "distinct"]) {
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Distinct,
            None,
            None,
            None,
        ))
    } else {
        None
    }
}

fn infer_native_vortex_duplicate_mask_primitive_payload(
    operations: &[SummaryOperation<'_>],
) -> Option<InferredNativeVortexRoutePayload> {
    for duplicate_kind in ["duplicate_mask", "duplicated"] {
        if matches_summary_kinds(
            operations,
            &["read_vortex", "select", duplicate_kind, "limit"],
        ) && summary_positive_limit(operations[3].arg)
        {
            return Some(native_vortex_primitive_payload(
                PublicVortexPrimitive::DuplicateMask,
                None,
                Some(operations[2].arg.trim().to_string()),
                Some(operations[3].arg.trim().to_string()),
            ));
        }
        if matches_summary_kinds(operations, &["read_vortex", "select", duplicate_kind]) {
            return Some(native_vortex_primitive_payload(
                PublicVortexPrimitive::DuplicateMask,
                None,
                Some(operations[2].arg.trim().to_string()),
                None,
            ));
        }
        if matches_summary_kinds(operations, &["read_vortex", duplicate_kind, "limit"])
            && summary_positive_limit(operations[2].arg)
        {
            return Some(native_vortex_primitive_payload(
                PublicVortexPrimitive::DuplicateMask,
                None,
                Some(operations[1].arg.trim().to_string()),
                Some(operations[2].arg.trim().to_string()),
            ));
        }
        if matches_summary_kinds(operations, &["read_vortex", duplicate_kind]) {
            return Some(native_vortex_primitive_payload(
                PublicVortexPrimitive::DuplicateMask,
                None,
                Some(operations[1].arg.trim().to_string()),
                None,
            ));
        }
    }
    None
}

fn infer_native_vortex_sample_primitive_payload(
    operations: &[SummaryOperation<'_>],
) -> Option<InferredNativeVortexRoutePayload> {
    if matches_summary_kinds(operations, &["read_vortex", "filter", "select", "sample"]) {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        let sample_shape = summary_sample_shape(operations[3].arg)?;
        Some(native_vortex_sample_primitive_payload(
            Some(predicate),
            Some(operations[2].arg.trim().to_string()),
            sample_shape,
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "filter", "sample"]) {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        let sample_shape = summary_sample_shape(operations[2].arg)?;
        Some(native_vortex_sample_primitive_payload(
            Some(predicate),
            None,
            sample_shape,
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "select", "sample"]) {
        let sample_shape = summary_sample_shape(operations[2].arg)?;
        Some(native_vortex_sample_primitive_payload(
            None,
            Some(operations[1].arg.trim().to_string()),
            sample_shape,
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "sample"]) {
        let sample_shape = summary_sample_shape(operations[1].arg)?;
        Some(native_vortex_sample_primitive_payload(
            None,
            None,
            sample_shape,
        ))
    } else {
        None
    }
}

fn summary_sample_shape(value: &str) -> Option<SummarySampleShape> {
    let parts = value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.is_empty() {
        return None;
    }
    if parts.first().copied() == Some("fraction") {
        let fraction = *parts.get(1)?;
        let seed = parts.get(2).copied().unwrap_or("0");
        let replacement = matches!(parts.get(3).copied(), Some("replacement" | "replace=true"));
        let valid_replacement_marker = match parts.get(3).copied() {
            None | Some("replacement" | "replace=true") => true,
            Some(_) => false,
        };
        if parts.len() <= if replacement { 4 } else { 3 }
            && sample_fraction_arg("sample fraction", fraction).is_ok()
            && seed.parse::<u64>().is_ok()
            && valid_replacement_marker
        {
            return Some(SummarySampleShape {
                source_order_limit: None,
                sample_fraction: Some(fraction.to_string()),
                sample_seed: seed.to_string(),
                sample_replacement: replacement,
            });
        }
        return None;
    }
    let mut source_order_limit = None;
    let mut sample_fraction = None;
    let mut sample_seed = "0".to_string();
    let mut sample_replacement = false;
    let mut saw_seed = false;
    for (index, part) in parts.iter().enumerate() {
        if let Some(value) = part.strip_prefix("n=") {
            if source_order_limit.is_some() || !summary_positive_limit(value) {
                return None;
            }
            source_order_limit = Some(value.to_string());
        } else if let Some(value) = part
            .strip_prefix("frac=")
            .or_else(|| part.strip_prefix("fraction="))
        {
            if sample_fraction.is_some() || sample_fraction_arg("sample fraction", value).is_err() {
                return None;
            }
            sample_fraction = Some(value.to_string());
        } else if let Some(value) = part
            .strip_prefix("seed=")
            .or_else(|| part.strip_prefix("random_state="))
        {
            if saw_seed || value.parse::<u64>().is_err() {
                return None;
            }
            sample_seed = value.to_string();
            saw_seed = true;
        } else if *part == "replacement" || *part == "replace=true" {
            if sample_replacement {
                return None;
            }
            sample_replacement = true;
        } else if index == 0 && source_order_limit.is_none() && summary_positive_limit(part) {
            source_order_limit = Some((*part).to_string());
        } else if index == 1 && !saw_seed && part.parse::<u64>().is_ok() {
            sample_seed = (*part).to_string();
            saw_seed = true;
        } else {
            return None;
        }
    }
    if source_order_limit.is_some() == sample_fraction.is_some() {
        None
    } else {
        Some(SummarySampleShape {
            source_order_limit,
            sample_fraction,
            sample_seed,
            sample_replacement,
        })
    }
}

fn infer_native_vortex_tail_primitive_payload(
    operations: &[SummaryOperation<'_>],
) -> Option<InferredNativeVortexRoutePayload> {
    if matches_summary_kinds(operations, &["read_vortex", "select", "tail"])
        && summary_positive_limit(operations[2].arg)
    {
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Tail,
            None,
            Some(operations[1].arg.trim().to_string()),
            Some(operations[2].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "tail"])
        && summary_positive_limit(operations[1].arg)
    {
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Tail,
            None,
            None,
            Some(operations[1].arg.trim().to_string()),
        ))
    } else {
        None
    }
}

fn infer_native_vortex_basic_primitive_payload(
    operations: &[SummaryOperation<'_>],
) -> Option<InferredNativeVortexRoutePayload> {
    if matches_summary_kinds(operations, &["read_vortex", "filter", "select", "limit"])
        && summary_positive_limit(operations[3].arg)
    {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::FilterProject,
            Some(predicate),
            Some(operations[2].arg.trim().to_string()),
            Some(operations[3].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "filter", "select"]) {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::FilterProject,
            Some(predicate),
            Some(operations[2].arg.trim().to_string()),
            None,
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "filter", "limit"])
        && summary_positive_limit(operations[2].arg)
    {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Filter,
            Some(predicate),
            None,
            Some(operations[2].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "filter"]) {
        let predicate = summary_tiny_predicate_from_sql(operations[1].arg)?;
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Filter,
            Some(predicate),
            None,
            None,
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "select", "limit"])
        && summary_positive_limit(operations[2].arg)
    {
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Project,
            None,
            Some(operations[1].arg.trim().to_string()),
            Some(operations[2].arg.trim().to_string()),
        ))
    } else if matches_summary_kinds(operations, &["read_vortex", "select"]) {
        Some(native_vortex_primitive_payload(
            PublicVortexPrimitive::Project,
            None,
            Some(operations[1].arg.trim().to_string()),
            None,
        ))
    } else {
        None
    }
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

fn summary_tiny_predicate_from_sql(value: &str) -> Option<String> {
    let text = value.trim();
    if text.is_empty() {
        return None;
    }
    let conjuncts = split_sql_conjunction_predicates(text);
    if conjuncts.len() > 1 {
        let predicates = conjuncts
            .into_iter()
            .map(summary_tiny_predicate_from_sql)
            .collect::<Option<Vec<_>>>()?;
        return Some(format!("and({})", predicates.join(";")));
    }
    if is_summary_compact_tiny_predicate(text) {
        return Some(text.to_string());
    }
    if let Some((column, is_not)) = parse_summary_null_predicate(text) {
        return Some(format!(
            "{}:{column}",
            if is_not { "is_not_null" } else { "is_null" }
        ));
    }
    if let Some((column, needle, negated)) = parse_summary_like_predicate(text) {
        return Some(format!(
            "{}:{column}:{needle}",
            if negated { "not_contains" } else { "contains" }
        ));
    }
    if let Some((column, values, negated)) = parse_summary_in_predicate(text) {
        return Some(format!(
            "{}:{column}:{}",
            if negated { "not_in" } else { "in" },
            values.join(",")
        ));
    }
    for (sql_op, tiny_op) in [
        ("!=", "neq"),
        ("<>", "neq"),
        (">=", "gte"),
        ("<=", "lte"),
        (">", "gt"),
        ("<", "lt"),
        ("=", "eq"),
    ] {
        let Some((column, literal)) = text.split_once(sql_op) else {
            continue;
        };
        let column = column.trim();
        let literal = literal.trim();
        if is_summary_identifier(column)
            && let Some(literal) = summary_sql_literal_to_tiny_scalar(literal)
        {
            return Some(format!("{tiny_op}:{column}:{literal}"));
        }
    }
    None
}

fn is_summary_compact_tiny_predicate(value: &str) -> bool {
    if let Some(inner) = value
        .strip_prefix("and(")
        .and_then(|value| value.strip_suffix(')'))
    {
        return inner
            .split(';')
            .filter(|part| !part.trim().is_empty())
            .all(|part| is_summary_compact_tiny_predicate(part.trim()));
    }
    let parts = value.splitn(3, ':').collect::<Vec<_>>();
    match parts.as_slice() {
        ["is_null" | "is_not_null", column] => is_summary_identifier(column),
        [
            "contains" | "string_contains" | "not_contains" | "not_string_contains",
            column,
            needle,
        ] => is_summary_identifier(column) && !needle.is_empty(),
        ["in" | "not_in", column, literals] => {
            is_summary_identifier(column)
                && literals
                    .split(',')
                    .map(str::trim)
                    .any(|literal| !literal.is_empty())
        }
        [
            "eq" | "neq" | "not_eq" | "gt" | "gte" | "lt" | "lte",
            column,
            _literal,
        ] => is_summary_identifier(column),
        _ => false,
    }
}

fn tiny_predicate_requires_materialized_eval(value: &str) -> bool {
    if let Some(inner) = value
        .strip_prefix("and(")
        .and_then(|value| value.strip_suffix(')'))
    {
        return inner
            .split(';')
            .filter(|part| !part.trim().is_empty())
            .any(|part| tiny_predicate_requires_materialized_eval(part.trim()));
    }
    matches!(
        value.split(':').next(),
        Some(
            "contains"
                | "string_contains"
                | "not_contains"
                | "not_string_contains"
                | "in"
                | "not_in"
        )
    )
}

fn parse_summary_like_predicate(value: &str) -> Option<(&str, String, bool)> {
    for (keyword, negated) in [("NOT LIKE", true), ("LIKE", false)] {
        let Some(position) = find_sql_keyword_outside_quotes_and_parens(value, keyword) else {
            continue;
        };
        let column = value[..position].trim();
        if !is_summary_identifier(column) {
            continue;
        }
        let pattern = parse_sql_single_quoted_literal(&value[position + keyword.len()..])?;
        let needle = sql_like_contains_needle(&pattern)?;
        return Some((column, needle, negated));
    }
    None
}

fn sql_like_contains_needle(pattern: &str) -> Option<String> {
    if pattern.contains('_') {
        return None;
    }
    let inner = pattern.strip_prefix('%')?.strip_suffix('%')?;
    if inner.is_empty() || inner.contains('%') {
        return None;
    }
    Some(inner.to_string())
}

fn parse_summary_in_predicate(value: &str) -> Option<(&str, Vec<String>, bool)> {
    for (keyword, negated) in [("NOT IN", true), ("IN", false)] {
        let Some(position) = find_sql_keyword_outside_quotes_and_parens(value, keyword) else {
            continue;
        };
        let column = value[..position].trim();
        if !is_summary_identifier(column) {
            continue;
        }
        let tail = value[position + keyword.len()..].trim();
        let inner = tail.strip_prefix('(')?.strip_suffix(')')?;
        let values = split_sql_projection_list(inner)
            .into_iter()
            .map(summary_sql_literal_to_tiny_scalar)
            .collect::<Option<Vec<_>>>()?;
        if values.is_empty() {
            continue;
        }
        return Some((column, values, negated));
    }
    None
}

fn split_sql_conjunction_predicates(value: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut rest = value.trim();
    while let Some(position) = find_sql_keyword_outside_quotes_and_parens(rest, "AND") {
        let head = rest[..position].trim();
        if !head.is_empty() {
            out.push(head);
        }
        rest = rest[position + "AND".len()..].trim();
    }
    if !rest.is_empty() {
        out.push(rest);
    }
    out
}

fn summary_sql_literal_to_tiny_scalar(value: &str) -> Option<String> {
    if value.parse::<i64>().is_ok() {
        return Some(value.to_string());
    }
    parse_sql_single_quoted_literal(value)
}

fn parse_sql_single_quoted_literal(value: &str) -> Option<String> {
    let value = value.trim();
    if !value.starts_with('\'') || !value.ends_with('\'') || value.len() < 2 {
        return None;
    }
    let inner = &value[1..value.len() - 1];
    let mut out = String::new();
    let mut chars = inner.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\'' {
            if chars.peek() == Some(&'\'') {
                chars.next();
                out.push('\'');
            } else {
                return None;
            }
        } else {
            out.push(ch);
        }
    }
    Some(out)
}

fn parse_summary_null_predicate(value: &str) -> Option<(&str, bool)> {
    let mut parts = value.split_whitespace();
    let column = parts.next()?;
    if !is_summary_identifier(column) {
        return None;
    }
    let is_token = parts.next()?;
    if !is_token.eq_ignore_ascii_case("is") {
        return None;
    }
    let third = parts.next()?;
    if third.eq_ignore_ascii_case("null") && parts.next().is_none() {
        return Some((column, false));
    }
    if third.eq_ignore_ascii_case("not") {
        let fourth = parts.next()?;
        if fourth.eq_ignore_ascii_case("null") && parts.next().is_none() {
            return Some((column, true));
        }
    }
    None
}

fn is_summary_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
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
    if request.execution_policy == "direct" {
        return direct_local_file_route_blocked(request);
    }
    if is_write_request(request)
        && !matches!(
            request.requested_output.as_str(),
            "write_vortex" | "write_jsonl" | "write_csv"
        )
    {
        return local_file_compatibility_sink_contract_missing_route(request);
    }
    if !shardloom_vortex::vortex_ingest_write_feature_enabled() {
        return local_file_vortex_ingest_feature_gated_route(request);
    }
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
    } else if matches!(request.execution_policy.as_str(), "auto" | "prepare_once") {
        let prepared_run = match prepared_local_workflow_native_request(request) {
            Ok(prepared_run) => prepared_run,
            Err(blocked) => return *blocked,
        };
        let native_plan = plan_public_workflow_route(&prepared_run.request);
        if native_plan.status != CommandStatus::Success {
            return native_plan;
        }
        if matches!(
            native_plan.route_id,
            "native_vortex_count_all"
                | "native_vortex_count_where"
                | "native_vortex_filter"
                | "native_vortex_project"
                | "native_vortex_filter_project"
                | "native_vortex_distinct"
                | "native_vortex_duplicate_mask"
                | "native_vortex_tail"
                | "native_vortex_sample"
                | "native_vortex_expression_project"
                | "native_vortex_melt"
                | "native_vortex_explode"
                | "native_vortex_pivot"
                | "native_vortex_rolling_window"
                | "native_vortex_aggregate"
                | "native_vortex_sort_rows"
        ) && !cfg!(feature = "vortex-local-primitives")
        {
            return local_file_vortex_primitive_feature_gated_route(request);
        }
        admitted_route(
            "local_file_prepare_once_first_query",
            "vortex-ingest-smoke->vortex-production-runtime-run",
            "compatibility_local_source",
            "VortexPreparedState",
            "prepared_vortex",
            true,
            true,
        )
    } else {
        local_file_vortex_middle_required_route(request)
    }
}

fn direct_local_file_route_blocked(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.direct_local_file_blocked",
        "direct local-file compatibility execution is not admitted as a public workflow route",
        Diagnostic::new(
            DiagnosticCode::InvalidInput,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "public local-file workflows require a Vortex normalization or native Vortex execution layer"
                .to_string(),
            Some("public_workflow_route.execution_policy".to_string()),
            Some(format!(
                "execution_policy=direct input_format={} requested_output={} direct_compatibility_runtime=disabled_for_public_workflows",
                request.input_format.as_deref().unwrap_or("not_declared"),
                request.requested_output
            )),
            Some(
                "use prepare dataframe to create VortexPreparedState, then run an admitted prepared/native Vortex route; direct compatibility remains an internal smoke safeguard only"
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn local_file_vortex_primitive_feature_gated_route(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.local_file_vortex_primitive_feature_gated",
        "local-file public workflow requires the vortex-local-primitives feature gate before the prepared Vortex query can run",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "public local-file primitive workflows require native Vortex primitive execution, but this binary was compiled without vortex-local-primitives"
                .to_string(),
            Some("public_workflow_route.required_feature_gate".to_string()),
            Some(format!(
                "input_format={} requested_output={} required_feature_gate=vortex-local-primitives",
                request.input_format.as_deref().unwrap_or("not_declared"),
                request.requested_output
            )),
            Some(
                "use a release-user-surfaces build or rebuild shardloom-cli with --features vortex-write,vortex-local-primitives"
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn local_file_vortex_middle_required_route(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.local_file_vortex_middle_required",
        "local-file public workflow requires Vortex preparation/runtime before execution",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "public local-file workflows cannot execute through a direct decoded compatibility middle"
                .to_string(),
            Some("public_workflow_route.vortex_normalization_point".to_string()),
            Some(format!(
                "execution_policy={} input_format={} requested_output={} required_middle=VortexPreparedState_or_native_vortex",
                request.execution_policy,
                request.input_format.as_deref().unwrap_or("not_declared"),
                request.requested_output
            )),
            Some(
                "run prepare dataframe with Vortex ingest enabled or pass input_format=vortex with an admitted native Vortex route; no public workflow may execute sql-local-source-smoke as its runtime middle"
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        )
    )
}

fn local_file_vortex_ingest_feature_gated_route(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.local_file_vortex_ingest_feature_gated",
        "local-file public workflow requires the vortex-write feature gate before Vortex preparation can run",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "public local-file workflows require Vortex ingest before native execution, but this binary was compiled without vortex-write"
                .to_string(),
            Some("public_workflow_route.required_feature_gate".to_string()),
            Some(format!(
                "input_format={} requested_output={} required_feature_gate=vortex-write",
                request.input_format.as_deref().unwrap_or("not_declared"),
                request.requested_output
            )),
            Some(
                "use a release-user-surfaces build or rebuild shardloom-cli with --features vortex-write,vortex-local-primitives for local primitive routes"
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn local_file_compatibility_sink_contract_missing_route(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.local_file_compatibility_sink_contract_missing",
        "local-file compatibility sinks require a native Vortex-derived typed sink contract",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "public local-file writes cannot execute through direct decoded compatibility sinks"
                .to_string(),
            Some("public_workflow_route.requested_output".to_string()),
            Some(format!(
                "requested_output={} input_format={} direct_compatibility_sink=disabled_for_public_workflows",
                request.requested_output,
                request.input_format.as_deref().unwrap_or("not_declared")
            )),
            Some(
                "use write_vortex for an admitted native Vortex sink shape, or wait for the native compatibility export contract"
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
}

fn native_vortex_sink_format_blocked_route(
    request: &PublicWorkflowRouteRequest,
) -> PublicWorkflowRoutePlan {
    blocked_route(
        "py-vortex-route-unify-1.native_vortex_sink_format_missing",
        "native Vortex result export supports Vortex, JSONL, and CSV result sinks in this runtime slice",
        Diagnostic::new(
            DiagnosticCode::NotImplemented,
            DiagnosticSeverity::Error,
            DiagnosticCategory::UnsupportedFeature,
            "native Vortex provider result export does not yet support the requested sink format"
                .to_string(),
            Some("public_workflow_route.requested_output".to_string()),
            Some(format!(
                "requested_output={} admitted_result_sink_formats=vortex,jsonl,csv",
                request.requested_output
            )),
            Some(
                "use write_vortex, write_jsonl, or write_csv for admitted provider-backed native Vortex result sinks"
                    .to_string(),
            ),
            FallbackStatus::disabled_by_policy(),
        ),
    )
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
        "vortex_sample_seed",
        optional_or_none(request.vortex_sample_seed.as_ref()),
    );
    push_field(
        fields,
        "vortex_sample_fraction",
        optional_or_none(request.vortex_sample_fraction.as_ref()),
    );
    push_field(
        fields,
        "vortex_sample_replacement",
        request.vortex_sample_replacement.to_string(),
    );
    push_field(
        fields,
        "vortex_duplicate_keep",
        request.vortex_duplicate_keep.as_deref().unwrap_or("first"),
    );
    push_field(
        fields,
        "vortex_expression_projection_present",
        request.vortex_expression_projection.is_some().to_string(),
    );
    push_field(
        fields,
        "vortex_expression_projection_changed_columns",
        expression_projection_changed_columns(request.vortex_expression_projection.as_ref()),
    );
    push_field(
        fields,
        "vortex_melt_projection_present",
        request.vortex_melt_projection.is_some().to_string(),
    );
    push_field(
        fields,
        "vortex_explode_projection_present",
        request.vortex_explode_projection.is_some().to_string(),
    );
    push_field(
        fields,
        "vortex_pivot_projection_present",
        request.vortex_pivot_projection.is_some().to_string(),
    );
    push_field(
        fields,
        "vortex_rolling_window_present",
        request.vortex_rolling_window.is_some().to_string(),
    );
    push_field(
        fields,
        "vortex_aggregate_present",
        request.vortex_aggregate.is_some().to_string(),
    );
    push_field(
        fields,
        "vortex_sort_rows_present",
        request.vortex_sort_rows.is_some().to_string(),
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
        format!("{prefix}native_vortex_required_feature_gate"),
        native_vortex_required_feature_gate(request, plan),
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
    if let Some(family) = native_vortex_family_from_plan_blocker(plan) {
        return family.as_str();
    }
    if matches!(
        plan.blocker_id,
        "py-vortex-route-unify-1.native_vortex_sink_contract_missing"
    ) || (is_native_vortex_route(request) && is_write_request(request))
    {
        return NativeVortexOperationFamily::Sink.as_str();
    }
    if plan.route_id == "native_vortex_user_profile" || request.requested_output == "profile" {
        return NativeVortexOperationFamily::Profile.as_str();
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

fn native_vortex_family_from_plan_blocker(
    plan: &PublicWorkflowRoutePlan,
) -> Option<NativeVortexOperationFamily> {
    match plan.blocker_id {
        "py-vortex-route-unify-1.native_vortex_aggregate_route_missing" => {
            Some(NativeVortexOperationFamily::Aggregate)
        }
        "py-vortex-route-unify-1.native_vortex_join_state_missing" => {
            Some(NativeVortexOperationFamily::Join)
        }
        "py-vortex-route-unify-1.native_vortex_top_n_route_missing" => {
            Some(NativeVortexOperationFamily::TopN)
        }
        "py-vortex-route-unify-1.native_vortex_cast_route_missing" => {
            Some(NativeVortexOperationFamily::Cast)
        }
        "py-vortex-route-unify-1.native_vortex_contains_route_missing" => {
            Some(NativeVortexOperationFamily::Contains)
        }
        "py-vortex-route-unify-1.native_vortex_distinct_route_missing" => {
            Some(NativeVortexOperationFamily::Distinct)
        }
        "py-vortex-route-unify-1.native_vortex_duplicate_mask_route_missing" => {
            Some(NativeVortexOperationFamily::DuplicateMask)
        }
        "py-vortex-route-unify-1.native_vortex_sample_route_missing" => {
            Some(NativeVortexOperationFamily::Sample)
        }
        "py-vortex-route-unify-1.native_vortex_expression_project_route_missing" => {
            Some(NativeVortexOperationFamily::ExpressionProject)
        }
        "py-vortex-route-unify-1.native_vortex_melt_route_missing" => {
            Some(NativeVortexOperationFamily::Melt)
        }
        "py-vortex-route-unify-1.native_vortex_explode_route_missing" => {
            Some(NativeVortexOperationFamily::Explode)
        }
        "py-vortex-route-unify-1.native_vortex_pivot_route_missing" => {
            Some(NativeVortexOperationFamily::Pivot)
        }
        "py-vortex-route-unify-1.native_vortex_rolling_window_route_missing" => {
            Some(NativeVortexOperationFamily::RollingWindow)
        }
        "py-vortex-route-unify-1.native_vortex_profile_route_missing" => {
            Some(NativeVortexOperationFamily::Profile)
        }
        "py-vortex-route-unify-1.native_vortex_sink_contract_missing"
        | "py-vortex-route-unify-1.native_vortex_sink_format_missing" => {
            Some(NativeVortexOperationFamily::Sink)
        }
        _ => None,
    }
}

fn native_vortex_required_feature_gate(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if !is_native_vortex_route(request) {
        return "not_applicable";
    }
    if plan.route_id == "native_vortex_user_profile" {
        return "default";
    }
    if matches!(plan.route_id, "native_vortex_primitive_row_export")
        || plan.blocker_id
            == "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        || plan.blocker_id
            == "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
        || normalized_vortex_primitive(request)
            .is_some_and(PublicVortexPrimitive::requires_local_primitives_feature)
    {
        return "vortex-local-primitives";
    }
    if request.native_vortex_provider_scenario.is_some()
        || matches!(
            plan.route_id,
            "native_vortex_user_aggregate"
                | "native_vortex_user_join"
                | "native_vortex_user_top_n"
                | "native_vortex_user_cast"
                | "native_vortex_user_contains"
                | "native_vortex_user_distinct"
                | "native_vortex_user_profile"
                | "native_vortex_user_sink"
        )
        || matches!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_provider_feature_gated"
        )
    {
        return "vortex-production-runtime";
    }
    if matches!(
        plan.route_id,
        "native_vortex_count_all"
            | "native_vortex_count_where"
            | "native_vortex_filter"
            | "native_vortex_project"
            | "native_vortex_filter_project"
            | "native_vortex_distinct"
            | "native_vortex_duplicate_mask"
            | "native_vortex_tail"
            | "native_vortex_sample"
            | "native_vortex_expression_project"
            | "native_vortex_melt"
            | "native_vortex_explode"
            | "native_vortex_pivot"
            | "native_vortex_rolling_window"
    ) {
        return "default";
    }
    "not_applicable"
}

fn native_vortex_capability_status(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if plan.status == CommandStatus::Success {
        match plan.route_id {
            "native_vortex_filter"
            | "native_vortex_project"
            | "native_vortex_filter_project"
            | "native_vortex_distinct"
            | "native_vortex_duplicate_mask"
            | "native_vortex_tail"
            | "native_vortex_sample"
            | "native_vortex_expression_project"
            | "native_vortex_melt"
            | "native_vortex_explode"
            | "native_vortex_pivot"
            | "native_vortex_rolling_window"
            | "native_vortex_aggregate"
            | "native_vortex_sort_rows" => "supported_with_materialization_boundary",
            "native_vortex_primitive_row_export" => "supported_with_explicit_decode_sink_boundary",
            _ => "supported",
        }
    } else if matches!(
        plan.blocker_id,
        "py-vortex-route-unify-1.native_vortex_provider_feature_gated"
            | "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
            | "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
    ) {
        "feature_gated"
    } else if plan.blocker_id.starts_with("py-vortex-route-unify-1.") {
        "blocked_until_native_route_admitted"
    } else if !is_native_vortex_route(request) {
        "not_applicable"
    } else {
        "blocked_by_public_route_contract"
    }
}

fn native_vortex_required_evidence(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if let Some(family) = native_vortex_family_from_plan_blocker(plan) {
        return family.required_evidence();
    }
    if plan.route_id == "native_vortex_user_profile" || request.requested_output == "profile" {
        return NativeVortexOperationFamily::Profile.required_evidence();
    }
    if let Ok(Some(family)) = normalized_native_vortex_operation_family(request) {
        return family.required_evidence();
    }
    if let Some(primitive) = normalized_vortex_primitive(request) {
        return NativeVortexOperationFamily::from_primitive(primitive).required_evidence();
    }
    if !is_native_vortex_route(request) {
        return "not_applicable";
    }
    NativeVortexOperationFamily::GeneralQuery.required_evidence()
}

fn native_vortex_next_action(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if let Some(next_action) = admitted_native_vortex_next_action(request, plan) {
        return next_action;
    }
    if let Some(family) = native_vortex_family_from_plan_blocker(plan) {
        return family.next_action();
    }
    if plan.route_id == "native_vortex_user_profile" || request.requested_output == "profile" {
        return NativeVortexOperationFamily::Profile.next_action();
    }
    if let Ok(Some(family)) = normalized_native_vortex_operation_family(request) {
        return family.next_action();
    }
    if let Some(primitive) = normalized_vortex_primitive(request) {
        return NativeVortexOperationFamily::from_primitive(primitive).next_action();
    }
    if !is_native_vortex_route(request) {
        return "not_applicable";
    }
    NativeVortexOperationFamily::GeneralQuery.next_action()
}

fn admitted_native_vortex_next_action(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> Option<&'static str> {
    if plan.status != CommandStatus::Success || plan.route_status != "admitted" {
        return None;
    }
    if plan.route_id == "native_vortex_primitive_row_export" {
        return Some(
            "execute the admitted native Vortex primitive row-export route for JSONL/CSV or admitted fanout sinks",
        );
    }
    if plan.route_id == "native_vortex_user_profile" {
        return Some(
            "execute the admitted metadata-first native Vortex profile route; broader profile options remain blocked",
        );
    }
    if plan.route_id.starts_with("native_vortex_user_") {
        return Some(
            "execute the exact admitted native Vortex provider route; broader shapes remain blocked until separately certified",
        );
    }
    let primitive = normalized_vortex_primitive(request)?;
    Some(match primitive {
        PublicVortexPrimitive::Count | PublicVortexPrimitive::CountWhere => {
            "execute the admitted native Vortex count primitive route"
        }
        PublicVortexPrimitive::Filter
        | PublicVortexPrimitive::Project
        | PublicVortexPrimitive::FilterProject => {
            "execute the admitted native Vortex filter/project primitive route"
        }
        PublicVortexPrimitive::Distinct => {
            "execute the admitted native Vortex row-level distinct primitive route with explicit materialization evidence"
        }
        PublicVortexPrimitive::DuplicateMask => {
            "execute the admitted native Vortex duplicate-mask primitive route with explicit row-key state evidence"
        }
        PublicVortexPrimitive::Tail => {
            "execute the admitted native Vortex bounded source-order tail primitive route"
        }
        PublicVortexPrimitive::Sample => {
            "execute the admitted native Vortex deterministic sample primitive route with a declared seed and row-count or fractional replacement policy"
        }
        PublicVortexPrimitive::ExpressionProject => {
            "execute the admitted native Vortex expression-project primitive route with explicit rewrite materialization evidence"
        }
        PublicVortexPrimitive::Melt => {
            "execute the admitted native Vortex melt primitive route with explicit row-expansion materialization evidence"
        }
        PublicVortexPrimitive::Explode => {
            "execute the admitted native Vortex explode primitive route with explicit list row-expansion materialization evidence"
        }
        PublicVortexPrimitive::Pivot => {
            "execute the admitted native Vortex pivot primitive route with explicit wide-reshape materialization evidence"
        }
        PublicVortexPrimitive::RollingWindow => {
            "execute the admitted native Vortex rolling-window primitive route with explicit source-order window-state materialization evidence"
        }
        PublicVortexPrimitive::Aggregate => {
            "execute the admitted native Vortex scalar aggregate primitive route with explicit aggregate-state materialization evidence"
        }
        PublicVortexPrimitive::SortRows => {
            "execute the admitted native Vortex bounded sort/top-N primitive route with explicit order-state materialization evidence"
        }
    })
}

fn typed_result_contract(
    _request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
) -> &'static str {
    if plan.status != CommandStatus::Success {
        return "none_blocked_before_execution";
    }
    match plan.route_id {
        "native_vortex_count_all" | "native_vortex_count_where" => {
            "bounded_python_scalar_summary_with_native_vortex_evidence"
        }
        "native_vortex_filter"
        | "native_vortex_project"
        | "native_vortex_filter_project"
        | "native_vortex_distinct"
        | "native_vortex_duplicate_mask"
        | "native_vortex_tail"
        | "native_vortex_sample"
        | "native_vortex_expression_project"
        | "native_vortex_melt"
        | "native_vortex_explode"
        | "native_vortex_pivot"
        | "native_vortex_rolling_window"
        | "native_vortex_aggregate"
        | "native_vortex_sort_rows" => "bounded_python_rows_with_explicit_materialization_boundary",
        "native_vortex_user_aggregate"
        | "native_vortex_user_join"
        | "native_vortex_user_top_n"
        | "native_vortex_user_cast"
        | "native_vortex_user_contains" => {
            "provider_backed_native_vortex_result_summary_with_route_certificate"
        }
        "native_vortex_user_profile" => "metadata_first_native_vortex_profile_summary",
        "native_vortex_user_sink" => "native_vortex_result_sink_with_replay_certificate",
        "native_vortex_primitive_row_export" => {
            "native_vortex_primitive_row_stream_with_explicit_sink_materialization"
        }
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
        "native_vortex_user_sink" if request.requested_output == "write_vortex" => {
            "native_vortex_result_sink_with_replay_verified_artifact"
        }
        "native_vortex_user_sink" => {
            "native_vortex_provider_result_json_export_with_workspace_safe_sink"
        }
        "native_vortex_primitive_row_export" => {
            "native_vortex_primitive_row_stream_to_jsonl_csv_compatibility_sink"
        }
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
    if plan.route_id == "native_vortex_user_profile" {
        return "metadata_only_no_decode_materialization";
    }
    if plan.route_id == "native_vortex_user_sink" && request.requested_output != "write_vortex" {
        return "native_vortex_zero_decode_runtime_with_bounded_result_json_sink_materialization";
    }
    if plan.route_id == "native_vortex_primitive_row_export" {
        return "native_vortex_scan_pushdown_then_selected_column_decode_at_compatibility_sink";
    }
    if is_native_vortex_route(request) {
        return "native_vortex_zero_decode_runtime_with_bounded_python_materialization_boundary";
    }
    match plan.route_id {
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
    _plan: &PublicWorkflowRoutePlan,
) -> &'a str {
    request.evidence_level.as_str()
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
        "native_vortex_user_aggregate"
        | "native_vortex_user_join"
        | "native_vortex_user_top_n"
        | "native_vortex_user_cast"
        | "native_vortex_user_contains"
        | "native_vortex_distinct"
        | "native_vortex_tail"
        | "native_vortex_sample"
        | "native_vortex_expression_project"
        | "native_vortex_melt"
        | "native_vortex_explode"
        | "native_vortex_pivot"
        | "native_vortex_rolling_window"
        | "native_vortex_aggregate"
        | "native_vortex_sort_rows"
        | "native_vortex_user_profile"
        | "native_vortex_user_sink"
        | "native_vortex_duplicate_mask"
        | "native_vortex_primitive_row_export" => "production_admitted_local_workflow",
        "local_file_prepare_once"
        | "local_file_prepare_once_first_query"
        | "native_vortex_count_all"
        | "native_vortex_count_where"
        | "native_vortex_filter"
        | "native_vortex_project"
        | "native_vortex_filter_project"
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
        "local_file_prepare_once" | "local_file_prepare_once_first_query" => {
            "prepared_vortex_state"
        }
        "native_vortex_count_all"
        | "native_vortex_count_where"
        | "native_vortex_filter"
        | "native_vortex_project"
        | "native_vortex_filter_project"
        | "native_vortex_distinct"
        | "native_vortex_duplicate_mask"
        | "native_vortex_tail"
        | "native_vortex_sample"
        | "native_vortex_expression_project"
        | "native_vortex_melt"
        | "native_vortex_explode"
        | "native_vortex_pivot"
        | "native_vortex_rolling_window"
        | "native_vortex_aggregate"
        | "native_vortex_sort_rows"
        | "native_vortex_primitive_row_export" => "native_vortex_primitive",
        "native_vortex_user_aggregate"
        | "native_vortex_user_join"
        | "native_vortex_user_top_n"
        | "native_vortex_user_cast"
        | "native_vortex_user_contains"
        | "native_vortex_user_sink" => "native_vortex_user_operator_provider",
        "native_vortex_user_profile" => "native_vortex_metadata_profile",
        "source_free_generated_output"
        | "generated_user_rows_direct_output"
        | "generated_range_direct_output"
        | "generated_sequence_direct_output" => "not_applicable_source_free",
        _ => "blocked_or_unsupported",
    }
}

fn underlying_runtime_command(plan: &PublicWorkflowRoutePlan) -> &'static str {
    plan.resolved_internal_command
}

fn local_workflow_runtime_profile(plan: &PublicWorkflowRoutePlan) -> &'static str {
    let _ = plan;
    "not_applicable"
}

fn push_field(
    fields: &mut Vec<(String, String)>,
    key: impl Into<String>,
    value: impl Into<String>,
) {
    fields.push((key.into(), value.into()));
}

fn push_bool_field(fields: &mut Vec<(String, String)>, key: impl Into<String>, value: bool) {
    push_field(fields, key, value.to_string());
}

fn optional_or_none(value: Option<&String>) -> String {
    value.cloned().unwrap_or_else(|| "none".to_string())
}

fn expression_projection_changed_columns(value: Option<&String>) -> String {
    let Some(value) = value else {
        return "none".to_string();
    };
    let Ok(payload) = serde_json::from_str::<serde_json::Value>(value) else {
        return "invalid".to_string();
    };
    let Some(rewrites) = payload
        .get("rewrites")
        .and_then(serde_json::Value::as_array)
    else {
        return "none".to_string();
    };
    let mut columns = std::collections::BTreeSet::new();
    for rewrite in rewrites {
        let Some(object) = rewrite.as_object() else {
            continue;
        };
        let column = object
            .get("target_column")
            .or_else(|| object.get("target"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if let Some(column) = column {
            columns.insert(column.to_string());
        }
    }
    if columns.is_empty() {
        "none".to_string()
    } else {
        columns.into_iter().collect::<Vec<_>>().join(",")
    }
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
            "public_workflow_source_format".to_string(),
            effective_request
                .input_format
                .clone()
                .unwrap_or_else(|| "not_declared".to_string()),
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
            "public_workflow_vortex_sample_seed".to_string(),
            optional_or_none(effective_request.vortex_sample_seed.as_ref()),
        ),
        (
            "public_workflow_vortex_sample_fraction".to_string(),
            optional_or_none(effective_request.vortex_sample_fraction.as_ref()),
        ),
        (
            "public_workflow_vortex_sample_replacement".to_string(),
            effective_request.vortex_sample_replacement.to_string(),
        ),
        (
            "public_workflow_vortex_duplicate_keep".to_string(),
            effective_request
                .vortex_duplicate_keep
                .clone()
                .unwrap_or_else(|| "first".to_string()),
        ),
        (
            "public_workflow_vortex_expression_projection_present".to_string(),
            effective_request
                .vortex_expression_projection
                .is_some()
                .to_string(),
        ),
        (
            "public_workflow_vortex_expression_projection_changed_columns".to_string(),
            expression_projection_changed_columns(
                effective_request.vortex_expression_projection.as_ref(),
            ),
        ),
        (
            "public_workflow_vortex_melt_projection_present".to_string(),
            effective_request
                .vortex_melt_projection
                .is_some()
                .to_string(),
        ),
        (
            "public_workflow_vortex_explode_projection_present".to_string(),
            effective_request
                .vortex_explode_projection
                .is_some()
                .to_string(),
        ),
        (
            "public_workflow_vortex_pivot_projection_present".to_string(),
            effective_request
                .vortex_pivot_projection
                .is_some()
                .to_string(),
        ),
        (
            "public_workflow_vortex_rolling_window_present".to_string(),
            effective_request
                .vortex_rolling_window
                .is_some()
                .to_string(),
        ),
        (
            "public_workflow_vortex_aggregate_present".to_string(),
            effective_request.vortex_aggregate.is_some().to_string(),
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

#[cfg(test)]
fn sql_local_source_runtime_args(
    request: &PublicWorkflowRouteRequest,
    _plan: &PublicWorkflowRoutePlan,
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
        PublicVortexPrimitive::Distinct
        | PublicVortexPrimitive::DuplicateMask
        | PublicVortexPrimitive::Tail
        | PublicVortexPrimitive::Sample
        | PublicVortexPrimitive::ExpressionProject
        | PublicVortexPrimitive::Melt
        | PublicVortexPrimitive::Explode
        | PublicVortexPrimitive::Pivot
        | PublicVortexPrimitive::RollingWindow
        | PublicVortexPrimitive::Aggregate
        | PublicVortexPrimitive::SortRows => {
            return Err(ShardLoomError::InvalidOperation(
                "public native Vortex materializing primitives use direct local primitive execution; fallback execution was not attempted".to_string(),
            ));
        }
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

fn non_negative_u64_arg(label: &str, value: &str) -> Result<u64, ShardLoomError> {
    value.parse::<u64>().map_err(|_| {
        ShardLoomError::InvalidOperation(format!("{label} must be a non-negative integer"))
    })
}

fn sample_fraction_arg(label: &str, value: &str) -> Result<f64, ShardLoomError> {
    let parsed = value.parse::<f64>().map_err(|_| {
        ShardLoomError::InvalidOperation(format!("{label} must be a finite decimal"))
    })?;
    if !parsed.is_finite() || parsed <= 0.0 || parsed > 1.0 {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{label} must be finite and in the range (0, 1]"
        )));
    }
    Ok(parsed)
}

fn normalize_duplicate_keep(value: &str) -> Result<String, ShardLoomError> {
    let normalized = value.trim().to_ascii_lowercase().replace('_', "-");
    match normalized.as_str() {
        "first" => Ok("first".to_string()),
        "last" => Ok("last".to_string()),
        "false" | "all" | "none" => Ok("false".to_string()),
        _ => Err(ShardLoomError::InvalidOperation(format!(
            "duplicate keep must be first, last, or false: {value}"
        ))),
    }
}

fn duplicate_keep_policy_arg(
    value: Option<&str>,
) -> Result<shardloom_vortex::VortexDuplicateKeepPolicy, ShardLoomError> {
    match value {
        None | Some("first") => Ok(shardloom_vortex::VortexDuplicateKeepPolicy::First),
        Some("last") => Ok(shardloom_vortex::VortexDuplicateKeepPolicy::Last),
        Some("false") => Ok(shardloom_vortex::VortexDuplicateKeepPolicy::AllDuplicates),
        Some(other) => Err(ShardLoomError::InvalidOperation(format!(
            "duplicate keep must be first, last, or false: {other}"
        ))),
    }
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

fn auto_prepared_vortex_target_path(source_uri: &str, source_format: &str) -> PathBuf {
    let source_path = Path::new(source_uri);
    let source_name = source_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("source");
    let stem = source_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .map(safe_generated_vortex_stem)
        .filter(|stem| !stem.is_empty())
        .unwrap_or_else(|| safe_generated_vortex_stem(source_name));
    let digest = fnv64_digest_hex(&format!("{source_uri}|{source_format}"));
    let parent = source_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    parent
        .join(".shardloom")
        .join("prepared")
        .join(format!("{stem}-{digest}.vortex"))
}

fn safe_generated_vortex_stem(value: &str) -> String {
    let mut normalized = String::with_capacity(value.len());
    let mut previous_dash = false;
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
            previous_dash = false;
        } else if !previous_dash {
            normalized.push('-');
            previous_dash = true;
        }
    }
    let normalized = normalized.trim_matches('-').to_string();
    if normalized.is_empty() {
        "source".to_string()
    } else {
        normalized
    }
}

fn fnv64_digest_hex(value: &str) -> String {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("{hash:016x}")
}

fn extract_first_quoted_source_ref(statement: &str) -> Option<String> {
    extract_first_sql_source_ref(statement, true)
}

fn extract_first_declared_sql_source_ref(statement: &str) -> Option<String> {
    extract_first_sql_source_ref(statement, false)
}

fn extract_first_sql_source_ref(statement: &str, require_known_format: bool) -> Option<String> {
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
                if !require_known_format || infer_input_format_from_ref(candidate).is_some() {
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
    leading_quoted_sql_literal_with_consumed(raw).map(|(literal, _)| literal)
}

fn leading_quoted_sql_literal_with_consumed(raw: &str) -> Option<(String, usize)> {
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
            return Some((literal, next_index));
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

    fn assert_provider_schema_shape_blocked(plan: &PublicWorkflowRoutePlan) {
        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(
            plan.blocker_id,
            "py-vortex-route-unify-1.native_vortex_provider_schema_shape_not_admitted"
        );
        assert_eq!(
            plan.blocker_reason,
            "native Vortex provider plan shape does not match an admitted schema contract"
        );
    }

    #[test]
    fn route_planner_admits_equivalent_sql_and_dataframe_local_file_routes_through_vortex_middle() {
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

        if !shardloom_vortex::vortex_ingest_write_feature_enabled() {
            assert_eq!(sql_plan.status, CommandStatus::Unsupported);
            assert_eq!(
                sql_plan.blocker_id,
                "cg21.route.local_file_vortex_ingest_feature_gated"
            );
            assert_eq!(dataframe_plan.status, CommandStatus::Unsupported);
            assert_eq!(dataframe_plan.blocker_id, sql_plan.blocker_id);
            return;
        }

        assert_eq!(sql_plan.status, CommandStatus::Unsupported);
        assert_eq!(
            sql_plan.blocker_id,
            "cg21.route.local_file_vortex_middle_required"
        );
        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(dataframe_plan.status, CommandStatus::Success);
            assert_eq!(
                dataframe_plan.route_id,
                "local_file_prepare_once_first_query"
            );
            assert_eq!(
                dataframe_plan.resolved_internal_command,
                "vortex-ingest-smoke->vortex-production-runtime-run"
            );
            assert_eq!(
                dataframe_plan.vortex_normalization_point,
                "VortexPreparedState"
            );
            assert_eq!(dataframe_plan.execution_mode, "prepared_vortex");
            assert!(dataframe_plan.preparation_included);
            assert!(dataframe_plan.query_timing_starts_after_preparation);
        } else {
            assert_eq!(
                dataframe_plan.blocker_id,
                "cg21.route.local_file_vortex_primitive_feature_gated"
            );
        }
    }

    #[test]
    fn route_planner_blocks_explicit_direct_local_file_policy() {
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
                "--execution-policy",
                "direct",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("dataframe run request");
        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);

        assert_eq!(plan.status, CommandStatus::Unsupported);
        assert_eq!(plan.route_id, "blocked");
        assert_eq!(plan.blocker_id, "cg21.route.direct_local_file_blocked");
        assert_eq!(
            field(&fields, "route_support_status"),
            "unsupported_boundary"
        );
        assert_eq!(
            field(&fields, "vortex_middle_status"),
            "blocked_or_unsupported"
        );
        assert_eq!(field(&fields, "underlying_runtime_command"), "not_resolved");
        assert_eq!(
            field(&fields, "local_workflow_runtime_profile"),
            "not_applicable"
        );
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
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

        let limited_plan = plan_public_workflow_route(&limited);
        let blocked_plan = plan_public_workflow_route(&blocked);

        assert_ne!(
            limited_plan.blocker_id,
            "cg21.route.unbounded_collect_blocked"
        );
        assert_eq!(
            blocked_plan.blocker_id,
            "cg21.route.unbounded_collect_blocked"
        );
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
            "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
        );
        let fields = route_fields(&request, &plan);
        assert_eq!(field(&fields, "native_vortex_operation_family"), "sink");
        assert_eq!(
            field(&fields, "native_vortex_required_feature_gate"),
            "vortex-local-primitives"
        );
        assert_eq!(
            field(&fields, "native_vortex_capability_status"),
            "feature_gated"
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
            field(&fields, "native_vortex_required_feature_gate"),
            "default"
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
        assert_eq!(
            field(&attachments, "public_workflow_source_format"),
            "vortex"
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
    fn route_planner_infers_native_vortex_sql_not_equal_count_where_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--input",
                "hits.vortex",
                "--input-format",
                "vortex",
                "--sql",
                "SELECT COUNT(*) FROM 'hits.vortex' WHERE AdvEngineID <> 0",
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
        .expect("native SQL not-equal count route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);
        let attachments = execution_attachment_fields("run", &request, &plan);

        assert_eq!(plan.status, CommandStatus::Success);
        assert_eq!(plan.route_id, "native_vortex_count_where");
        assert_eq!(field(&fields, "vortex_primitive"), "count_where");
        assert_eq!(field(&fields, "native_vortex_operation_family"), "count");
        assert_eq!(
            field(&attachments, "public_workflow_vortex_predicate"),
            "neq:AdvEngineID:0"
        );
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_infers_payloadless_native_vortex_tail_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "orders.vortex",
                "--input-format",
                "vortex",
                "--plan",
                "read_vortex(orders.vortex) -> select(id,metric) -> tail(10)",
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
        .expect("payloadless native tail route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);
        let attachments = execution_attachment_fields("run", &request, &plan);

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_tail");
            assert_eq!(
                field(&fields, "route_runtime_status"),
                "production_admitted_local_workflow"
            );
            assert_eq!(
                field(&fields, "typed_result_contract"),
                "bounded_python_rows_with_explicit_materialization_boundary"
            );
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(plan.route_id, "blocked");
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
            );
            assert_eq!(
                field(&fields, "native_vortex_capability_status"),
                "feature_gated"
            );
            assert_eq!(
                field(&fields, "native_vortex_required_feature_gate"),
                "vortex-local-primitives"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "tail");
        assert_eq!(field(&fields, "native_vortex_operation_family"), "top_n");
        assert_eq!(
            field(&attachments, "public_workflow_vortex_primitive"),
            "tail"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_columns"),
            "id,metric"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_source_order_limit"),
            "10"
        );
    }

    #[test]
    fn route_planner_infers_payloadless_native_vortex_duplicate_mask_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "orders.vortex",
                "--input-format",
                "vortex",
                "--plan",
                "read_vortex(orders.vortex) -> select(id,label) -> duplicate_mask(id) -> limit(2)",
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
        .expect("payloadless native duplicate-mask route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);
        let attachments = execution_attachment_fields("run", &request, &plan);

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_duplicate_mask");
            assert_eq!(
                field(&fields, "route_runtime_status"),
                "production_admitted_local_workflow"
            );
            assert_eq!(
                field(&fields, "typed_result_contract"),
                "bounded_python_rows_with_explicit_materialization_boundary"
            );
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(plan.route_id, "blocked");
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
            );
            assert_eq!(
                field(&fields, "native_vortex_capability_status"),
                "feature_gated"
            );
            assert_eq!(
                field(&fields, "native_vortex_required_feature_gate"),
                "vortex-local-primitives"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "duplicate_mask");
        assert_eq!(
            field(&fields, "native_vortex_operation_family"),
            "duplicate_mask"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_primitive"),
            "duplicate_mask"
        );
        assert_eq!(field(&attachments, "public_workflow_vortex_columns"), "id");
        assert_eq!(
            field(&attachments, "public_workflow_vortex_source_order_limit"),
            "2"
        );
    }

    #[test]
    fn route_planner_attaches_native_vortex_expression_project_route() {
        let expression_projection = r#"{"columns":["id","amount"],"rewrites":[{"kind":"mask_scalar","target_column":"amount","predicate":"lt:amount:0","replacement":{"type":"int64","value":0}}]}"#;
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "orders.vortex",
                "--input-format",
                "vortex",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
                "--native-vortex-operation-family",
                "expression_project",
                "--vortex-primitive",
                "expression_project",
                "--vortex-columns",
                "id,amount",
                "--vortex-source-order-limit",
                "5",
                "--vortex-expression-projection",
                expression_projection,
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("native expression-project route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);
        let attachments = execution_attachment_fields("run", &request, &plan);

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_expression_project");
            assert_eq!(
                field(&fields, "route_runtime_status"),
                "production_admitted_local_workflow"
            );
            assert_eq!(
                field(&fields, "typed_result_contract"),
                "bounded_python_rows_with_explicit_materialization_boundary"
            );
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(plan.route_id, "blocked");
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
            );
            assert_eq!(
                field(&fields, "native_vortex_capability_status"),
                "feature_gated"
            );
            assert_eq!(
                field(&fields, "native_vortex_required_feature_gate"),
                "vortex-local-primitives"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "expression_project");
        assert_eq!(
            field(&fields, "native_vortex_operation_family"),
            "expression_project"
        );
        assert_eq!(
            field(&fields, "vortex_expression_projection_present"),
            "true"
        );
        assert_eq!(
            field(&fields, "vortex_expression_projection_changed_columns"),
            "amount"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_primitive"),
            "expression_project"
        );
        assert_eq!(
            field(
                &attachments,
                "public_workflow_vortex_expression_projection_present"
            ),
            "true"
        );
        assert_eq!(
            field(
                &attachments,
                "public_workflow_vortex_expression_projection_changed_columns"
            ),
            "amount"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_columns"),
            "id,amount"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_source_order_limit"),
            "5"
        );
    }

    #[test]
    fn route_planner_attaches_native_vortex_melt_route() {
        let melt_projection = r#"{"id_columns":["id"],"value_columns":["amount_a","amount_b"],"variable_column":"measure","value_column":"amount"}"#;
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "orders.vortex",
                "--input-format",
                "vortex",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
                "--native-vortex-operation-family",
                "melt",
                "--vortex-primitive",
                "melt",
                "--vortex-columns",
                "id,amount_a,amount_b",
                "--vortex-source-order-limit",
                "4",
                "--vortex-melt-projection",
                melt_projection,
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("native melt route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);
        let attachments = execution_attachment_fields("run", &request, &plan);

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_melt");
            assert_eq!(
                field(&fields, "route_runtime_status"),
                "production_admitted_local_workflow"
            );
            assert_eq!(
                field(&fields, "typed_result_contract"),
                "bounded_python_rows_with_explicit_materialization_boundary"
            );
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(plan.route_id, "blocked");
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
            );
            assert_eq!(
                field(&fields, "native_vortex_capability_status"),
                "feature_gated"
            );
            assert_eq!(
                field(&fields, "native_vortex_required_feature_gate"),
                "vortex-local-primitives"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "melt");
        assert_eq!(field(&fields, "native_vortex_operation_family"), "melt");
        assert_eq!(field(&fields, "vortex_melt_projection_present"), "true");
        assert_eq!(
            field(&attachments, "public_workflow_vortex_primitive"),
            "melt"
        );
        assert_eq!(
            field(
                &attachments,
                "public_workflow_vortex_melt_projection_present"
            ),
            "true"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_columns"),
            "id,amount_a,amount_b"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_source_order_limit"),
            "4"
        );
    }

    #[test]
    fn route_planner_attaches_native_vortex_explode_route() {
        let explode_projection = r#"{"column":"items"}"#;
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "orders.vortex",
                "--input-format",
                "vortex",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
                "--native-vortex-operation-family",
                "explode",
                "--vortex-primitive",
                "explode",
                "--vortex-columns",
                "id,items",
                "--vortex-source-order-limit",
                "4",
                "--vortex-explode-projection",
                explode_projection,
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("native explode route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);
        let attachments = execution_attachment_fields("run", &request, &plan);

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_explode");
            assert_eq!(
                field(&fields, "route_runtime_status"),
                "production_admitted_local_workflow"
            );
            assert_eq!(
                field(&fields, "typed_result_contract"),
                "bounded_python_rows_with_explicit_materialization_boundary"
            );
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(plan.route_id, "blocked");
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
            );
            assert_eq!(
                field(&fields, "native_vortex_capability_status"),
                "feature_gated"
            );
            assert_eq!(
                field(&fields, "native_vortex_required_feature_gate"),
                "vortex-local-primitives"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "explode");
        assert_eq!(field(&fields, "native_vortex_operation_family"), "explode");
        assert_eq!(field(&fields, "vortex_explode_projection_present"), "true");
        assert_eq!(
            field(&attachments, "public_workflow_vortex_primitive"),
            "explode"
        );
        assert_eq!(
            field(
                &attachments,
                "public_workflow_vortex_explode_projection_present"
            ),
            "true"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_columns"),
            "id,items"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_source_order_limit"),
            "4"
        );
    }

    #[test]
    fn route_planner_attaches_native_vortex_pivot_route() {
        let pivot_projection = r#"{"aggregate":"first_unique","index_column":"id","pivot_column":"label","value_column":"amount"}"#;
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "orders.vortex",
                "--input-format",
                "vortex",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
                "--native-vortex-operation-family",
                "pivot",
                "--vortex-primitive",
                "pivot",
                "--vortex-columns",
                "id,label,amount",
                "--vortex-source-order-limit",
                "4",
                "--vortex-pivot-projection",
                pivot_projection,
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("native pivot route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);
        let attachments = execution_attachment_fields("run", &request, &plan);

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_pivot");
            assert_eq!(
                field(&fields, "route_runtime_status"),
                "production_admitted_local_workflow"
            );
            assert_eq!(
                field(&fields, "typed_result_contract"),
                "bounded_python_rows_with_explicit_materialization_boundary"
            );
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(plan.route_id, "blocked");
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "pivot");
        assert_eq!(field(&fields, "native_vortex_operation_family"), "pivot");
        assert_eq!(field(&fields, "vortex_pivot_projection_present"), "true");
        assert_eq!(
            field(&attachments, "public_workflow_vortex_primitive"),
            "pivot"
        );
        assert_eq!(
            field(
                &attachments,
                "public_workflow_vortex_pivot_projection_present"
            ),
            "true"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_columns"),
            "id,label,amount"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_source_order_limit"),
            "4"
        );
    }

    #[test]
    fn route_planner_attaches_native_vortex_pivot_row_export_route() {
        let pivot_projection = r#"{"aggregate":"sum","index_column":"id","pivot_column":"label","value_column":"amount"}"#;
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "orders.vortex",
                "--input-format",
                "vortex",
                "--request",
                "write_jsonl",
                "--output",
                "target/pivot.jsonl",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
                "--native-vortex-operation-family",
                "sink",
                "--vortex-primitive",
                "pivot",
                "--vortex-columns",
                "id,label,amount",
                "--vortex-pivot-projection",
                pivot_projection,
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("native pivot row export route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_primitive_row_export");
            assert_eq!(
                plan.resolved_internal_command,
                "vortex-local-primitive-row-export"
            );
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(plan.route_id, "blocked");
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
            );
        }
        assert_eq!(field(&fields, "native_vortex_operation_family"), "sink");
        assert_eq!(field(&fields, "vortex_pivot_projection_present"), "true");
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_attaches_native_vortex_rolling_window_route() {
        let rolling_window = r#"{"aggregate":"sum","min_periods":3,"output_column":"rolling_amount","source_column":"amount","window_size":3}"#;
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "orders.vortex",
                "--input-format",
                "vortex",
                "--request",
                "collect",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
                "--native-vortex-operation-family",
                "rolling_window",
                "--vortex-primitive",
                "rolling_window",
                "--vortex-columns",
                "amount",
                "--vortex-source-order-limit",
                "4",
                "--vortex-rolling-window",
                rolling_window,
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("native rolling-window route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);
        let attachments = execution_attachment_fields("run", &request, &plan);

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_rolling_window");
            assert_eq!(
                field(&fields, "route_runtime_status"),
                "production_admitted_local_workflow"
            );
            assert_eq!(
                field(&fields, "typed_result_contract"),
                "bounded_python_rows_with_explicit_materialization_boundary"
            );
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(plan.route_id, "blocked");
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
            );
            assert_eq!(
                field(&fields, "native_vortex_capability_status"),
                "feature_gated"
            );
            assert_eq!(
                field(&fields, "native_vortex_required_feature_gate"),
                "vortex-local-primitives"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "rolling_window");
        assert_eq!(
            field(&fields, "native_vortex_operation_family"),
            "rolling_window"
        );
        assert_eq!(field(&fields, "vortex_rolling_window_present"), "true");
        assert_eq!(
            field(&attachments, "public_workflow_vortex_primitive"),
            "rolling_window"
        );
        assert_eq!(
            field(
                &attachments,
                "public_workflow_vortex_rolling_window_present"
            ),
            "true"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_columns"),
            "amount"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_source_order_limit"),
            "4"
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

        assert_provider_schema_shape_blocked(&plan);
        assert_eq!(field(&fields, "vortex_primitive"), "none");
        assert_eq!(field(&fields, "native_vortex_provider_scenario"), "none");
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[cfg(not(feature = "vortex-production-runtime"))]
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
            "feature_gated"
        );
        assert_eq!(
            field(&fields, "native_vortex_required_feature_gate"),
            "vortex-production-runtime"
        );
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[cfg(feature = "vortex-production-runtime")]
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
            "production_admitted_local_workflow"
        );
        assert_eq!(
            field(&aggregate_fields, "vortex_middle_status"),
            "native_vortex_user_operator_provider"
        );
        assert_eq!(
            field(&aggregate_fields, "native_vortex_required_feature_gate"),
            "vortex-production-runtime"
        );
        assert_eq!(
            aggregate_plan.resolved_internal_command,
            "vortex-production-runtime-run"
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

    #[cfg(feature = "vortex-production-runtime")]
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
            "production_admitted_local_workflow"
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

    #[cfg(feature = "vortex-production-runtime")]
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

        assert_provider_schema_shape_blocked(&plan);
        assert_eq!(field(&fields, "native_vortex_provider_scenario"), "none");
        assert_eq!(field(&fields, "resolved_internal_command"), "not_resolved");
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[cfg(feature = "vortex-production-runtime")]
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

        assert_provider_schema_shape_blocked(&plan);
        assert_eq!(field(&fields, "native_vortex_provider_scenario"), "none");
        assert_eq!(field(&fields, "resolved_internal_command"), "not_resolved");
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[cfg(feature = "vortex-production-runtime")]
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

        assert_provider_schema_shape_blocked(&plan);
        assert_eq!(field(&fields, "native_vortex_provider_scenario"), "none");
        assert_eq!(field(&fields, "resolved_internal_command"), "not_resolved");
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[cfg(feature = "vortex-production-runtime")]
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

    #[cfg(feature = "vortex-production-runtime")]
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

        assert_provider_schema_shape_blocked(&plan);
        assert_eq!(field(&fields, "native_vortex_provider_scenario"), "none");
        assert_eq!(field(&fields, "resolved_internal_command"), "not_resolved");
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_attaches_native_vortex_scalar_aggregate_route() {
        let aggregate = r#"{"measures":[{"function":"sum","column":"metric","alias":"sum_metric"},{"function":"count","alias":"rows"}]}"#;
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
                "--vortex-primitive",
                "aggregate",
                "--vortex-aggregate",
                aggregate,
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("native aggregate primitive route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);
        let attachments = execution_attachment_fields("run", &request, &plan);

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_aggregate");
            assert_eq!(
                field(&fields, "route_runtime_status"),
                "production_admitted_local_workflow"
            );
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
        assert_eq!(
            field(&fields, "native_vortex_operation_family"),
            "aggregate"
        );
        assert_eq!(field(&fields, "vortex_aggregate_present"), "true");
        assert_eq!(
            field(&attachments, "public_workflow_vortex_primitive"),
            "aggregate"
        );
        assert_eq!(
            field(&attachments, "public_workflow_vortex_aggregate_present"),
            "true"
        );
    }

    #[test]
    fn route_planner_attaches_native_vortex_scalar_aggregate_row_export_route() {
        let aggregate = r#"{"measures":[{"function":"sum","column":"metric","alias":"sum_metric"},{"function":"count","alias":"rows"}]}"#;
        let request = PublicWorkflowRouteRequest::parse(
            [
                "dataframe",
                "--input",
                "target/input.vortex",
                "--input-format",
                "vortex",
                "--request",
                "write_jsonl",
                "--output",
                "target/native-vortex-aggregate-output.jsonl",
                "--bounded",
                "true",
                "--execution-policy",
                "native_vortex",
                "--native-vortex-operation-family",
                "aggregate",
                "--vortex-primitive",
                "aggregate",
                "--vortex-aggregate",
                aggregate,
                "--allow-overwrite",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .expect("native aggregate row export route request");

        let plan = plan_public_workflow_route(&request);
        let fields = route_fields(&request, &plan);

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_primitive_row_export");
            assert_eq!(
                field(&fields, "route_runtime_status"),
                "production_admitted_local_workflow"
            );
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_primitive_row_export_feature_gated"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
        assert_eq!(field(&fields, "native_vortex_operation_family"), "sink");
        assert_eq!(field(&fields, "vortex_aggregate_present"), "true");
    }

    #[test]
    fn route_planner_infers_native_vortex_sql_scalar_aggregate_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT SUM(metric), COUNT(*), AVG(metric) FROM 'target/input.vortex'",
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
        .expect("native SQL aggregate route request");
        let effective_request = effective_public_workflow_request(&request);
        let plan = plan_public_workflow_route(&effective_request);
        let fields = route_fields(&effective_request, &plan);

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_aggregate");
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
        assert_eq!(
            field(&fields, "native_vortex_operation_family"),
            "aggregate"
        );
        assert_eq!(field(&fields, "vortex_aggregate_present"), "true");
    }

    #[test]
    fn route_planner_infers_native_vortex_sql_wide_sum_offset_aggregate_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth - 2) FROM 'hits.vortex'",
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
        .expect("native SQL wide aggregate route request");
        let effective_request = effective_public_workflow_request(&request);
        let plan = plan_public_workflow_route(&effective_request);
        let fields = route_fields(&effective_request, &plan);
        let aggregate = effective_request
            .vortex_aggregate
            .as_ref()
            .expect("aggregate payload");

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_aggregate");
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
        assert_eq!(
            field(&fields, "native_vortex_operation_family"),
            "aggregate"
        );
        assert_eq!(field(&fields, "vortex_aggregate_present"), "true");
        assert!(aggregate.contains(r#""column":"ResolutionWidth""#));
        assert!(aggregate.contains(r#""argument_offset":1"#));
        assert!(aggregate.contains(r#""argument_offset":-2"#));
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_infers_native_vortex_sql_grouped_aggregate_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT UserID, SearchPhrase, COUNT(*) FROM 'hits.vortex' GROUP BY UserID, SearchPhrase LIMIT 10",
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
        .expect("native SQL grouped aggregate route request");
        let effective_request = effective_public_workflow_request(&request);
        let plan = plan_public_workflow_route(&effective_request);
        let fields = route_fields(&effective_request, &plan);

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_aggregate");
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
        assert_eq!(
            field(&fields, "native_vortex_operation_family"),
            "aggregate"
        );
        assert_eq!(field(&fields, "vortex_aggregate_present"), "true");
        assert_eq!(field(&fields, "vortex_source_order_limit"), "10");
    }

    #[test]
    fn route_planner_infers_native_vortex_sql_grouped_count_distinct_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM 'hits.vortex' GROUP BY RegionID ORDER BY u DESC LIMIT 10",
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
        .expect("native SQL grouped count-distinct route request");
        let effective_request = effective_public_workflow_request(&request);
        let plan = plan_public_workflow_route(&effective_request);
        let fields = route_fields(&effective_request, &plan);
        let aggregate = effective_request
            .vortex_aggregate
            .as_ref()
            .expect("aggregate payload");

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_aggregate");
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
        assert_eq!(
            field(&fields, "native_vortex_operation_family"),
            "aggregate"
        );
        assert_eq!(field(&fields, "vortex_aggregate_present"), "true");
        assert_eq!(field(&fields, "vortex_source_order_limit"), "10");
        assert!(aggregate.contains(r#""function":"count_distinct""#));
        assert!(aggregate.contains(r#""column":"UserID""#));
        assert!(aggregate.contains(r#""alias":"u""#));
        assert!(aggregate.contains(r#""column":"u""#));
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_infers_native_vortex_sql_filtered_grouped_topk_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT SearchPhrase, COUNT(*) AS c FROM 'hits.vortex' WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
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
        .expect("native SQL filtered grouped top-K route request");
        let effective_request = effective_public_workflow_request(&request);
        let plan = plan_public_workflow_route(&effective_request);
        let fields = route_fields(&effective_request, &plan);
        let aggregate = effective_request
            .vortex_aggregate
            .as_ref()
            .expect("aggregate payload");

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_aggregate");
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
        assert_eq!(
            field(&fields, "native_vortex_operation_family"),
            "aggregate"
        );
        assert_eq!(field(&fields, "vortex_predicate"), "neq:SearchPhrase:");
        assert_eq!(field(&fields, "vortex_aggregate_present"), "true");
        assert_eq!(field(&fields, "vortex_source_order_limit"), "10");
        assert!(aggregate.contains(r#""group_by":["SearchPhrase"]"#));
        assert!(aggregate.contains(r#""column":"c""#));
        assert!(aggregate.contains(r#""descending":true"#));
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_infers_native_vortex_sql_like_count_as_residual_aggregate_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT COUNT(*) FROM 'hits.vortex' WHERE URL LIKE '%google%'",
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
        .expect("native SQL LIKE count route request");
        let effective_request = effective_public_workflow_request(&request);
        let plan = plan_public_workflow_route(&effective_request);
        let fields = route_fields(&effective_request, &plan);

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_aggregate");
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
        assert_eq!(field(&fields, "vortex_predicate"), "contains:URL:google");
        assert_eq!(field(&fields, "vortex_aggregate_present"), "true");
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_infers_native_vortex_sql_like_grouped_topk_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM 'hits.vortex' WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
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
        .expect("native SQL LIKE grouped route request");
        let effective_request = effective_public_workflow_request(&request);
        let plan = plan_public_workflow_route(&effective_request);
        let fields = route_fields(&effective_request, &plan);
        let aggregate = effective_request
            .vortex_aggregate
            .as_ref()
            .expect("aggregate payload");

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_aggregate");
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
        assert_eq!(
            field(&fields, "vortex_predicate"),
            "and(contains:URL:google;neq:SearchPhrase:)"
        );
        assert_eq!(field(&fields, "vortex_source_order_limit"), "10");
        assert!(aggregate.contains(r#""function":"min""#));
        assert!(aggregate.contains(r#""column":"URL""#));
        assert!(aggregate.contains(r#""group_by":["SearchPhrase"]"#));
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_infers_native_vortex_sql_in_list_grouped_topk_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM 'hits.vortex' WHERE CounterID = 62 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100",
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
        .expect("native SQL IN-list grouped route request");
        let effective_request = effective_public_workflow_request(&request);
        let plan = plan_public_workflow_route(&effective_request);
        let fields = route_fields(&effective_request, &plan);
        let aggregate = effective_request
            .vortex_aggregate
            .as_ref()
            .expect("aggregate payload");

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_aggregate");
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
        assert_eq!(
            field(&fields, "vortex_predicate"),
            "and(eq:CounterID:62;in:TraficSourceID:-1,6;eq:RefererHash:3594120000172545465)"
        );
        assert_eq!(field(&fields, "vortex_source_order_limit"), "10");
        assert!(aggregate.contains(r#""offset":100"#));
        assert!(aggregate.contains(r#""group_by":["URLHash","EventDate"]"#));
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_infers_native_vortex_sql_raw_order_topk_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT * FROM 'hits.vortex' WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10",
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
        .expect("native SQL raw top-K route request");
        let effective_request = effective_public_workflow_request(&request);
        let plan = plan_public_workflow_route(&effective_request);
        let fields = route_fields(&effective_request, &plan);
        let sort_rows = effective_request
            .vortex_sort_rows
            .as_ref()
            .expect("sort rows payload");

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_sort_rows");
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "sort_rows");
        assert_eq!(field(&fields, "native_vortex_operation_family"), "top_n");
        assert_eq!(field(&fields, "vortex_predicate"), "contains:URL:google");
        assert_eq!(field(&fields, "vortex_source_order_limit"), "10");
        assert_eq!(field(&fields, "vortex_sort_rows_present"), "true");
        assert!(sort_rows.contains(r#""column":"EventTime""#));
        assert!(sort_rows.contains(r#""limit":10"#));
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_infers_native_vortex_sql_length_having_grouped_topk_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM 'hits.vortex' WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
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
        .expect("native SQL length/HAVING route request");
        let effective_request = effective_public_workflow_request(&request);
        let plan = plan_public_workflow_route(&effective_request);
        let fields = route_fields(&effective_request, &plan);
        let aggregate = effective_request
            .vortex_aggregate
            .as_ref()
            .expect("aggregate payload");

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_aggregate");
        }
        assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
        assert_eq!(field(&fields, "vortex_source_order_limit"), "25");
        assert!(aggregate.contains(r#""value_transform":"length""#));
        assert!(aggregate.contains(r#""column":"c""#));
        assert!(aggregate.contains(r#""op":"gt""#));
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_infers_native_vortex_sql_regex_domain_grouped_topk_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                r"SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM 'hits.vortex' WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25",
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
        .expect("native SQL regex-domain route request");
        let effective_request = effective_public_workflow_request(&request);
        let plan = plan_public_workflow_route(&effective_request);
        let fields = route_fields(&effective_request, &plan);
        let aggregate = effective_request
            .vortex_aggregate
            .as_ref()
            .expect("aggregate payload");

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_aggregate");
        }
        assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
        assert!(aggregate.contains(r#""function":"regex_domain""#));
        assert!(aggregate.contains(r#""alias":"k""#));
        assert!(aggregate.contains(r#""value_transform":"length""#));
        assert!(aggregate.contains(r#""op":"gt""#));
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_infers_native_vortex_sql_extract_minute_grouped_topk_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM 'hits.vortex' GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10",
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
        .expect("native SQL extract-minute route request");
        let effective_request = effective_public_workflow_request(&request);
        let plan = plan_public_workflow_route(&effective_request);
        let fields = route_fields(&effective_request, &plan);
        let aggregate = effective_request
            .vortex_aggregate
            .as_ref()
            .expect("aggregate payload");

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_aggregate");
        }
        assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
        assert!(aggregate.contains(r#""function":"extract_minute""#));
        assert!(aggregate.contains(r#""alias":"m""#));
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_infers_native_vortex_sql_arithmetic_group_key_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM 'hits.vortex' GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10",
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
        .expect("native SQL arithmetic group route request");
        let effective_request = effective_public_workflow_request(&request);
        let plan = plan_public_workflow_route(&effective_request);
        let fields = route_fields(&effective_request, &plan);
        let aggregate = effective_request
            .vortex_aggregate
            .as_ref()
            .expect("aggregate payload");

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_aggregate");
        }
        assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
        assert!(aggregate.contains(r#""function":"add_offset""#));
        assert!(aggregate.contains(r#""argument_offset":-1"#));
        assert!(aggregate.contains(r#""argument_offset":-2"#));
        assert!(aggregate.contains(r#""argument_offset":-3"#));
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_infers_native_vortex_sql_date_trunc_grouped_topk_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM 'hits.vortex' WHERE CounterID = 62 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000",
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
        .expect("native SQL date-trunc route request");
        let effective_request = effective_public_workflow_request(&request);
        let plan = plan_public_workflow_route(&effective_request);
        let fields = route_fields(&effective_request, &plan);
        let aggregate = effective_request
            .vortex_aggregate
            .as_ref()
            .expect("aggregate payload");

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_aggregate");
        }
        assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
        assert_eq!(field(&fields, "vortex_source_order_limit"), "10");
        assert!(aggregate.contains(r#""function":"date_trunc_minute""#));
        assert!(aggregate.contains(r#""offset":1000"#));
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_infers_native_vortex_sql_case_grouped_topk_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM 'hits.vortex' WHERE CounterID = 62 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000",
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
        .expect("native SQL CASE grouped route request");
        let effective_request = effective_public_workflow_request(&request);
        let plan = plan_public_workflow_route(&effective_request);
        let fields = route_fields(&effective_request, &plan);
        let aggregate = effective_request
            .vortex_aggregate
            .as_ref()
            .expect("aggregate payload");

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_aggregate");
        }
        assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
        assert!(aggregate.contains(r#""function":"case_search_adv_zero_referer_else_empty""#));
        assert!(aggregate.contains(r#""alias":"Src""#));
        assert!(aggregate.contains(r#""alias":"Dst""#));
        assert!(aggregate.contains(r#""offset":1000"#));
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_infers_native_vortex_sql_ordinal_constant_group_key_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT 1, URL, COUNT(*) AS c FROM 'hits.vortex' GROUP BY 1, URL ORDER BY c DESC LIMIT 10",
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
        .expect("native SQL ordinal constant group route request");
        let effective_request = effective_public_workflow_request(&request);
        let plan = plan_public_workflow_route(&effective_request);
        let fields = route_fields(&effective_request, &plan);
        let aggregate = effective_request
            .vortex_aggregate
            .as_ref()
            .expect("aggregate payload");

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_aggregate");
        }
        assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
        assert!(aggregate.contains(r#""function":"constant_int""#));
        assert!(aggregate.contains(r#""argument_offset":1"#));
        assert_eq!(field(&fields, "fallback_attempted"), "false");
        assert_eq!(field(&fields, "external_engine_invoked"), "false");
    }

    #[test]
    fn route_planner_infers_native_vortex_sql_conjunctive_filtered_grouped_topk_route() {
        let request = PublicWorkflowRouteRequest::parse(
            [
                "sql",
                "--sql",
                "SELECT URL, COUNT(*) AS PageViews FROM 'hits.vortex' WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10",
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
        .expect("native SQL conjunctive filtered grouped top-K route request");
        let effective_request = effective_public_workflow_request(&request);
        let plan = plan_public_workflow_route(&effective_request);
        let fields = route_fields(&effective_request, &plan);
        let aggregate = effective_request
            .vortex_aggregate
            .as_ref()
            .expect("aggregate payload");

        if cfg!(feature = "vortex-local-primitives") {
            assert_eq!(plan.status, CommandStatus::Success);
            assert_eq!(plan.route_id, "native_vortex_aggregate");
        } else {
            assert_eq!(plan.status, CommandStatus::Unsupported);
            assert_eq!(
                plan.blocker_id,
                "py-vortex-route-unify-1.native_vortex_materializing_primitive_feature_gated"
            );
        }
        assert_eq!(field(&fields, "vortex_primitive"), "aggregate");
        assert_eq!(
            field(&fields, "native_vortex_operation_family"),
            "aggregate"
        );
        assert_eq!(
            field(&fields, "vortex_predicate"),
            "and(eq:CounterID:62;gte:EventDate:2013-07-01;lte:EventDate:2013-07-31;eq:DontCountHits:0;eq:IsRefresh:0;neq:URL:)"
        );
        assert_eq!(field(&fields, "vortex_aggregate_present"), "true");
        assert_eq!(field(&fields, "vortex_source_order_limit"), "10");
        assert!(aggregate.contains(r#""group_by":["URL"]"#));
        assert!(aggregate.contains(r#""column":"PageViews""#));
        assert!(aggregate.contains(r#""descending":true"#));
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

        assert_eq!(plan.route_id, "blocked");
        assert!(
            matches!(
                plan.blocker_id,
                "cg21.route.local_file_vortex_ingest_feature_gated"
                    | "cg21.route.local_file_vortex_middle_required"
            ),
            "unexpected blocker: {}",
            plan.blocker_id
        );
        assert!(!plan.preparation_included);
        assert!(!plan.query_timing_starts_after_preparation);
    }
}
