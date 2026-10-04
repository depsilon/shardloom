//! Schema admission selects a native strategy before any query evaluation.

use super::{
    CommandStatus, ExitCode, NativeVortexInputBinding, OutputFormat, PublicExecutionSession,
    PublicSourcePreparations, PublicVortexPrimitive, PublicWorkflowRoutePlan,
    PublicWorkflowRouteRequest, ShardLoomError, effective_public_workflow_request,
    emit_blocked_facade, execute_native_vortex_materializing_with_source,
    execute_native_vortex_owned_collect_with_source, is_write_request,
    native_vortex_bound_request_and_arg, native_vortex_input_binding_for_request,
    native_vortex_materializing_error, native_vortex_materializing_policy,
    normalized_vortex_primitive, resident_aggregate, resident_count_where, resident_relational,
    sql_local_source_runtime,
};
#[cfg(feature = "vortex-write")]
use super::{emit_native_vortex_primitive_row_export, native_vortex_primitive_row_export_targets};
use shardloom_vortex::local_primitives::prepared_dispatch;

pub(super) fn route(request: &PublicWorkflowRouteRequest) -> Option<PublicWorkflowRoutePlan> {
    let statement = request.sql_statement.as_deref()?;
    if request.input_format.as_deref() != Some("vortex")
        || request.requested_output == "prepare"
        || !request.fanout_outputs.is_empty()
        || !sql_local_source_runtime::native_relational::is_plain_select(statement).ok()?
    {
        return None;
    }
    let effective = effective_public_workflow_request(request);
    let request = &effective;
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
    let scalar_write = is_write_request(request)
        && matches!(
            normalized_vortex_primitive(request),
            Some(PublicVortexPrimitive::Count | PublicVortexPrimitive::CountWhere)
        );
    if !optimized || scalar_write {
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
        PublicVortexPrimitive::Project
            | PublicVortexPrimitive::Filter
            | PublicVortexPrimitive::FilterProject
            | PublicVortexPrimitive::Aggregate
            | PublicVortexPrimitive::SortRows
    ) {
        return None;
    }
    let result = (|| {
        let binding = native_vortex_input_binding_for_request(request)?;
        if binding.mode != "single_file" {
            return Ok(None);
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
        let source = prepared_dispatch::prepare_source(&primitive_request, policy)?;
        #[cfg(feature = "universal-format-io")]
        let (source, preparations) = (
            source.with_preparation_sources(preparations.sources)?,
            PublicSourcePreparations::default(),
        );
        if !prepared_dispatch::request_requires_relational(&source, &primitive_request)?
            && let Some(report) = prepared_dispatch::try_write_source(
                &primitive_request,
                &target.path,
                target.format,
                request.allow_overwrite,
                policy,
                source.clone(),
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
        )))
    })();
    match result {
        Ok(exit) => exit,
        Err(error) => Some(native_vortex_materializing_error(format, primitive, &error)),
    }
}

pub(super) fn run_if_needed(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    extra_fields: &mut Vec<(String, String)>,
    session: &mut PublicExecutionSession,
    preparations: PublicSourcePreparations,
) -> Option<ExitCode> {
    if request.sql_statement.is_none()
        || request.requested_output != "collect"
        || request.materialization_policy == "zero_decode"
    {
        return None;
    }
    let primitive = normalized_vortex_primitive(request)?;
    if !matches!(
        primitive,
        PublicVortexPrimitive::CountWhere
            | PublicVortexPrimitive::Project
            | PublicVortexPrimitive::Filter
            | PublicVortexPrimitive::FilterProject
            | PublicVortexPrimitive::Aggregate
            | PublicVortexPrimitive::SortRows
    ) {
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
        let binding = native_vortex_input_binding_for_request(request)?;
        if binding.mode != "single_file" {
            return Ok(None);
        }
        let (primitive_request, _, _) =
            native_vortex_bound_request_and_arg(request, primitive, &binding)?;
        let policy = native_vortex_materializing_policy(request)?;
        session.clear();
        let source = prepared_dispatch::prepare_source(&primitive_request, policy)?;
        let extended = prepared_dispatch::request_requires_relational(&source, &primitive_request)?;
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
            )
        } else {
            run_optimized_source(request, plan, format, fields, &binding, session, source)?
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

fn run_optimized_source(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    fields: Vec<(String, String)>,
    binding: &NativeVortexInputBinding,
    session: &mut PublicExecutionSession,
    source: shardloom_vortex::resident_session::PreparedVortexSource,
) -> Result<ExitCode, ShardLoomError> {
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
        ),
        PublicVortexPrimitive::CountWhere => resident_count_where::run_with_source(
            request,
            plan,
            format,
            fields,
            binding,
            session,
            Some(source),
        ),
        PublicVortexPrimitive::SortRows => execute_native_vortex_materializing_with_source(
            request,
            plan,
            format,
            fields,
            primitive,
            Some(source),
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
        ),
    })
}
