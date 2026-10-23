//! Schema admission selects a native strategy before any query evaluation.

use super::{
    CommandStatus, ExitCode, NativeVortexInputBinding, OutputFormat, PublicExecutionSession,
    PublicSourcePreparations, PublicVortexPrimitive, PublicWorkflowRoutePlan,
    PublicWorkflowRouteRequest, ShardLoomError, effective_public_workflow_request,
    emit_blocked_facade, execute_native_vortex_materializing_with_source,
    execute_native_vortex_owned_collect_with_source, native_vortex_bound_request_and_arg,
    native_vortex_input_binding_for_request, native_vortex_materializing_error,
    native_vortex_materializing_policy, normalized_vortex_primitive, resident_aggregate,
    resident_count_where, resident_relational, sql_local_source_runtime,
};
#[cfg(feature = "vortex-write")]
use super::{emit_native_vortex_primitive_row_export, native_vortex_primitive_row_export_targets};
use shardloom_vortex::local_primitives::prepared_dispatch;

/// SQL integer literals have signed, type-bound arithmetic semantics. The
/// primitive offset contract instead retains the input signedness. Rebind the
/// complete SQL before choosing an equivalent file provider, including for
/// floats, nullable fields and values too wide for the optimized transform.
pub(super) fn request_requires_relational(
    source: &shardloom_vortex::resident_session::PreparedVortexSource,
    request: &shardloom_vortex::VortexQueryPrimitiveRequest,
) -> Result<bool, ShardLoomError> {
    let extended = prepared_dispatch::request_requires_relational(source, request)?;
    let arithmetic = request.simple_aggregate.as_ref().is_some_and(|aggregate| {
        aggregate
            .group_expressions
            .iter()
            .any(|expression| expression.function.eq_ignore_ascii_case("add_offset"))
            || aggregate
                .measures
                .iter()
                .any(|measure| measure.argument_offset.is_some_and(|offset| offset != 0))
    });
    Ok(extended || arithmetic)
}

/// Retain names already assigned by the primitive SQL declaration when its
/// arithmetic moves to the typed binder. Only aliases are made explicit;
/// the complete expressions, clauses and source spelling are preserved.
pub(super) fn typed_statement<'a>(
    statement: &'a str,
    request: &PublicWorkflowRouteRequest,
) -> std::borrow::Cow<'a, str> {
    use std::borrow::Cow;
    if request.vortex_aggregate.is_none() {
        return Cow::Borrowed(statement);
    }
    let Some(shape) = super::parse_native_vortex_sql_single_source_shape(statement) else {
        return Cow::Borrowed(statement);
    };
    let mut arithmetic = false;
    let mut changed = false;
    let projections = super::split_sql_projection_list(&shape.projection)
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            if let Some(measure) = super::scalar_aggregate_measure_from_sql(index, item) {
                arithmetic |= measure.payload.get("argument_offset").is_some();
                if super::sql_explicit_alias(item).is_none() {
                    changed = true;
                    return format!("{item} AS {}", measure.alias);
                }
            } else if let Some(expression) = super::parse_sql_group_expression(item) {
                arithmetic |= expression.payload["function"] == "add_offset";
            }
            item.to_owned()
        })
        .collect::<Vec<_>>();
    if !arithmetic || !changed {
        return Cow::Borrowed(statement);
    }
    let body = statement.trim()["SELECT".len()..].trim_start();
    Cow::Owned(format!(
        "SELECT {}{}",
        projections.join(", "),
        &body[shape.projection.len()..]
    ))
}

pub(super) fn route(request: &PublicWorkflowRouteRequest) -> Option<PublicWorkflowRoutePlan> {
    let statement = request.sql_statement.as_deref()?;
    if request.requested_output == "prepare" {
        return None;
    }
    if request.input_format.as_deref() != Some("vortex")
        && request.execution_policy == "native_vortex"
    {
        return None;
    }
    if matches!(
        sql_local_source_runtime::native_relational::is_plain_select(statement),
        Ok(false)
    ) {
        return None;
    }
    // Specialized native primitives admit additional SELECT forms. Check them
    // before letting relational admission preserve a shared-parser diagnostic.
    let effective = effective_public_workflow_request(request);
    let request = &effective;
    if !request.fanout_outputs.is_empty() {
        return resident_relational::route_admitted_statement(request);
    }
    let optimized = matches!(
        normalized_vortex_primitive(request),
        Some(
            PublicVortexPrimitive::Count
                | PublicVortexPrimitive::CountWhere
                | PublicVortexPrimitive::Project
                | PublicVortexPrimitive::Filter
                | PublicVortexPrimitive::FilterProject
                | PublicVortexPrimitive::Aggregate
                | PublicVortexPrimitive::SortRows
        )
    );
    if !optimized {
        return resident_relational::route_admitted_statement(request);
    }
    None
}

#[cfg(feature = "vortex-write")]
pub(super) fn write_if_needed(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    extra_fields: &mut Vec<(String, String)>,
    preparations: PublicSourcePreparations,
    admission: &crate::command_context::CommandAdmission<'_>,
) -> Option<ExitCode> {
    if request.sql_statement.is_none()
        || !request.fanout_outputs.is_empty()
        || request.materialization_policy == "zero_decode"
    {
        return None;
    }
    let primitive = normalized_vortex_primitive(request)?;
    if !matches!(
        primitive,
        PublicVortexPrimitive::Count
            | PublicVortexPrimitive::CountWhere
            | PublicVortexPrimitive::Project
            | PublicVortexPrimitive::Filter
            | PublicVortexPrimitive::FilterProject
            | PublicVortexPrimitive::Aggregate
            | PublicVortexPrimitive::SortRows
    ) {
        return None;
    }
    let result = (|| {
        admission.check()?;
        let binding = native_vortex_input_binding_for_request(request, admission)?;
        if binding.mode != "single_file" {
            return Ok(Some(run_file_collection(
                request,
                format,
                extra_fields,
                &mut PublicExecutionSession::default(),
                preparations,
                &binding,
                admission,
            )?));
        }
        let targets = native_vortex_primitive_row_export_targets(request, "run").map_err(|_| {
            ShardLoomError::InvalidOperation("native SQL writer target is not admitted".into())
        })?;
        let target = targets.first().ok_or_else(|| {
            ShardLoomError::InvalidOperation("native SQL writer target is absent".into())
        })?;
        let (primitive_request, _, _) =
            native_vortex_bound_request_and_arg(request, primitive, &binding)?;
        let policy = native_vortex_materializing_policy(request)?;
        let cancellation = admission.cancellation();
        let source = prepared_dispatch::prepare_source_in_session(
            &primitive_request,
            policy,
            admission.native_session()?,
            &cancellation,
        )?;
        #[cfg(feature = "universal-format-io")]
        let (source, preparations) = (
            source.with_preparation_sources(preparations.sources)?,
            PublicSourcePreparations::default(),
        );
        if !request_requires_relational(&source, &primitive_request)?
            && let Some(report) = prepared_dispatch::try_write_source_controlled(
                &primitive_request,
                &target.path,
                target.format,
                request.allow_overwrite,
                policy,
                source.clone(),
                &cancellation,
            )?
        {
            return Ok(Some(emit_native_vortex_primitive_row_export(
                request,
                plan,
                format,
                std::mem::take(extra_fields),
                &targets,
                &[report],
            )));
        }
        let selected = resident_relational::route_admitted_statement(request).ok_or_else(|| {
            ShardLoomError::InvalidOperation("native relational writer is not admitted".into())
        })?;
        if selected.status != CommandStatus::Success {
            return Ok(Some(emit_blocked_facade("run", format, request, &selected)));
        }
        Ok(Some(resident_relational::run_with_source(
            request,
            &selected,
            format,
            &mut PublicExecutionSession::default(),
            std::mem::take(extra_fields),
            Some(source),
            preparations,
            admission,
        )))
    })();
    match result {
        Ok(exit) => exit,
        Err(error) => Some(native_vortex_materializing_error(format, primitive, &error)),
    }
}

#[allow(clippy::too_many_lines)] // Keep source admission, reuse and ownership transfer in one dispatch.
pub(super) fn run_if_needed(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    extra_fields: &mut Vec<(String, String)>,
    session: &mut PublicExecutionSession,
    preparations: PublicSourcePreparations,
    admission: &crate::command_context::CommandAdmission<'_>,
) -> Option<ExitCode> {
    if request.sql_statement.is_none() || request.requested_output != "collect" {
        return None;
    }
    let primitive = normalized_vortex_primitive(request)?;
    if !matches!(
        primitive,
        PublicVortexPrimitive::Count
            | PublicVortexPrimitive::CountWhere
            | PublicVortexPrimitive::Project
            | PublicVortexPrimitive::Filter
            | PublicVortexPrimitive::FilterProject
            | PublicVortexPrimitive::Aggregate
            | PublicVortexPrimitive::SortRows
    ) {
        return None;
    }
    if let Err(error) = admission.check() {
        session.clear();
        return Some(native_vortex_materializing_error(format, primitive, &error));
    }
    let binding = match native_vortex_input_binding_for_request(request, admission) {
        Ok(binding) => binding,
        Err(error) => {
            session.clear();
            return Some(native_vortex_materializing_error(format, primitive, &error));
        }
    };
    if binding.mode != "single_file" {
        return Some(
            match run_file_collection(
                request,
                format,
                extra_fields,
                session,
                preparations,
                &binding,
                admission,
            ) {
                Ok(exit) => exit,
                Err(error) => {
                    session.clear();
                    native_vortex_materializing_error(format, primitive, &error)
                }
            },
        );
    }
    if primitive == PublicVortexPrimitive::Count || request.materialization_policy == "zero_decode"
    {
        return None;
    }
    let relational_plan = || resident_relational::route_admitted_statement(request);
    if session
        .relational
        .as_ref()
        .is_some_and(|entry| entry.request == *request)
    {
        return Some(resident_relational::run_with_source(
            request,
            &relational_plan()?,
            format,
            session,
            std::mem::take(extra_fields),
            None,
            preparations,
            admission,
        ));
    }
    if session
        .aggregate
        .as_ref()
        .is_some_and(|entry| entry.request == *request)
        || session
            .collect
            .as_ref()
            .is_some_and(|entry| entry.request == *request)
        || session
            .count_where
            .as_ref()
            .is_some_and(|entry| entry.request == *request)
    {
        return None;
    }
    let result = (|| {
        let (primitive_request, _, _) =
            native_vortex_bound_request_and_arg(request, primitive, &binding)?;
        let policy = native_vortex_materializing_policy(request)?;
        session.clear();
        let cancellation = admission.cancellation();
        let source = prepared_dispatch::prepare_source_in_session(
            &primitive_request,
            policy,
            admission.native_session()?,
            &cancellation,
        )?;
        let extended = request_requires_relational(&source, &primitive_request)?;
        let fields = std::mem::take(extra_fields);
        let exit = if extended {
            let selected = relational_plan().ok_or_else(|| {
                ShardLoomError::InvalidOperation(
                    "native relational strategy is not admitted".into(),
                )
            })?;
            if selected.status != CommandStatus::Success {
                return Ok(Some(emit_blocked_facade("run", format, request, &selected)));
            }
            resident_relational::run_with_source(
                request,
                &selected,
                format,
                session,
                fields,
                Some(source),
                preparations,
                admission,
            )
        } else {
            #[cfg(feature = "universal-format-io")]
            let source = source.with_preparation_sources(preparations.sources)?;
            run_optimized_source(
                request, plan, format, fields, &binding, session, source, admission,
            )?
        };
        Ok(Some(exit))
    })();
    match result {
        Ok(exit) => exit,
        Err(error) => {
            session.clear();
            Some(native_vortex_materializing_error(format, primitive, &error))
        }
    }
}

fn run_file_collection(
    request: &PublicWorkflowRouteRequest,
    format: OutputFormat,
    extra_fields: &mut Vec<(String, String)>,
    session: &mut PublicExecutionSession,
    preparations: PublicSourcePreparations,
    binding: &NativeVortexInputBinding,
    admission: &crate::command_context::CommandAdmission<'_>,
) -> Result<ExitCode, ShardLoomError> {
    admission.check()?;
    let selected = resident_relational::route_admitted_statement(request).ok_or_else(|| {
        ShardLoomError::InvalidOperation("native file collection SQL is not admitted".into())
    })?;
    session.clear();
    if selected.status != CommandStatus::Success {
        return Ok(emit_blocked_facade("run", format, request, &selected));
    }
    extra_fields.extend(binding.evidence_fields());
    Ok(resident_relational::run_with_source(
        request,
        &selected,
        format,
        session,
        std::mem::take(extra_fields),
        None,
        preparations,
        admission,
    ))
}

#[allow(clippy::too_many_arguments)] // Preserve the route/report contract while borrowing its admission.
fn run_optimized_source(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    fields: Vec<(String, String)>,
    binding: &NativeVortexInputBinding,
    session: &mut PublicExecutionSession,
    source: shardloom_vortex::resident_session::PreparedVortexSource,
    admission: &crate::command_context::CommandAdmission<'_>,
) -> Result<ExitCode, ShardLoomError> {
    admission.check()?;
    let primitive = normalized_vortex_primitive(request).ok_or_else(|| {
        ShardLoomError::InvalidOperation("native source strategy is absent".into())
    })?;
    Ok(match primitive {
        PublicVortexPrimitive::Aggregate => resident_aggregate::run_with_source(
            request,
            plan,
            format,
            fields,
            session,
            Some(source),
            admission,
        ),
        PublicVortexPrimitive::CountWhere => resident_count_where::run_with_source(
            request,
            plan,
            format,
            fields,
            binding,
            session,
            Some(source),
            admission,
        ),
        PublicVortexPrimitive::SortRows => execute_native_vortex_materializing_with_source(
            request,
            plan,
            format,
            fields,
            primitive,
            Some(source),
            admission,
        ),
        _ => execute_native_vortex_owned_collect_with_source(
            request,
            plan,
            format,
            fields,
            primitive,
            binding,
            session,
            Some(source),
            admission,
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::super::{
        CommandStatus, DiagnosticCode, PublicWorkflowRouteRequest, plan_public_workflow_route,
        route_fields,
    };

    #[test]
    fn public_sql_invalid_distinct_aggregates_keep_parser_diagnostics_before_io() {
        for expression in ["SUM(DISTINCT id)", "AVG(DISTINCT id)", "COUNT(DISTINCT *)"] {
            for input_format in ["vortex", "csv"] {
                for output in [
                    "collect",
                    "write_vortex",
                    "write_parquet",
                    "write_arrow_ipc",
                    "write_avro",
                    "write_orc",
                    "write_json",
                    "write_jsonl",
                    "write_csv",
                ] {
                    let statement = format!(
                        "SELECT {expression} FROM 'target/syntax-must-not-open.{input_format}'"
                    );
                    let mut args = vec![
                        "sql",
                        "--sql",
                        &statement,
                        "--request",
                        output,
                        "--bounded",
                        "true",
                    ];
                    if output != "collect" {
                        args.extend(["--output", "target/syntax-must-not-write"]);
                    }
                    let request =
                        PublicWorkflowRouteRequest::parse(args.into_iter().map(str::to_owned))
                            .unwrap();
                    let plan = plan_public_workflow_route(&request);
                    assert_eq!(plan.status, CommandStatus::Unsupported);
                    assert_eq!(plan.diagnostics.len(), 1);
                    let diagnostic = &plan.diagnostics[0];
                    assert_eq!(diagnostic.code, DiagnosticCode::UnsupportedSql);
                    assert!(
                        diagnostic.message.contains("COUNT(DISTINCT"),
                        "{expression} {input_format} {output}: {diagnostic:?}"
                    );
                    assert!(!diagnostic.fallback.attempted);
                    assert!(!plan.preparation_included);
                    let fields = route_fields(&request, &plan);
                    for key in [
                        "runtime_execution",
                        "source_io_performed",
                        "fallback_attempted",
                        "external_engine_invoked",
                    ] {
                        assert_eq!(
                            fields
                                .iter()
                                .find(|(name, _)| name == key)
                                .map(|(_, value)| value.as_str()),
                            Some("false"),
                            "{expression} {input_format} {output} {key}",
                        );
                    }
                    assert!(
                        fields
                            .iter()
                            .any(|(key, value)| { key == "side_effect_free" && value == "true" })
                    );
                }
            }
        }
    }
}
