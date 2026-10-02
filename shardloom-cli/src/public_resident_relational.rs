//! Public SQL uses the same retained native relational plan for collect and sinks.

use super::{
    CommandStatus, Diagnostic, DiagnosticCode, ExitCode, OutputFormat, PublicExecutionSession,
    PublicWorkflowRoutePlan, PublicWorkflowRouteRequest, ShardLoomError, admitted_route,
    blocked_route, emit, emit_error, execution_attachment_fields, is_write_request,
    native_vortex_materializing_policy, native_vortex_row_export_format_for_output_request,
    output_required_route, sql_local_source_runtime::native_relational, vortex_primitive_execution,
};
#[cfg(feature = "vortex-write")]
use super::{
    append_native_vortex_primitive_row_export_fields,
    append_native_vortex_primitive_row_export_target_fields,
    native_vortex_primitive_row_export_targets,
};
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_vortex::local_primitives::prepared_relational::{
    ExecutedVortexRelational, PreparedVortexRelational,
};

#[path = "public_relational_sources.rs"]
mod sources;

pub(super) struct PreparedPublicRelational {
    pub(super) request: PublicWorkflowRouteRequest,
    operation: PreparedVortexRelational,
    prepared_sources: usize,
}

pub(super) fn route(request: &PublicWorkflowRouteRequest) -> Option<PublicWorkflowRoutePlan> {
    let statement = request.sql_statement.as_deref()?;
    match native_relational::is_relational(statement) {
        Ok(false) => return None,
        Ok(true) => {}
        Err(error) => return Some(denied(&error.to_string())),
    }
    if let Err(error) = sources::validate_bindings(statement, request) {
        return Some(denied(&error.to_string()));
    }
    let normalization_required = match sources::normalization_required(statement, request) {
        Ok(required) => required,
        Err(error) => return Some(denied(&error.to_string())),
    };
    if normalization_required
        && !cfg!(all(
            feature = "vortex-write",
            feature = "universal-format-io"
        ))
    {
        return Some(denied(
            "relational compatibility normalization requires vortex-write and universal-format-io",
        ));
    }
    if request.execution_policy == "direct" {
        return Some(denied(
            "relational execution requires native Vortex normalization",
        ));
    }
    if request.materialization_policy == "zero_decode" {
        return Some(denied(
            "relational keys, computed columns and result delivery require explicit materialization; choose bounded materialization",
        ));
    }
    if !request.fanout_outputs.is_empty() {
        return Some(denied(
            "relational fanout requires a shared native sink transaction",
        ));
    }
    let write = is_write_request(request);
    if write {
        if !cfg!(feature = "vortex-write") {
            return Some(denied("native relational writers require vortex-write"));
        }
        if let Err(blocked) = native_vortex_row_export_format_for_output_request(request) {
            return Some(*blocked);
        }
        if request
            .output_ref
            .as_deref()
            .is_none_or(|path| path.trim().is_empty())
        {
            return Some(output_required_route("route", "native relational write"));
        }
    } else if request.requested_output != "collect" || request.output_ref.is_some() {
        return Some(denied(
            "native relational execution requires collect or an explicit write request",
        ));
    }
    Some(admitted_route(
        if write {
            "native_vortex_relational_write"
        } else {
            "native_vortex_relational_collect"
        },
        "native-vortex-relational",
        "declared_local_sources",
        "all_sources_normalized_to_native_vortex_before_binding",
        "native_vortex",
        normalization_required,
        true,
    ))
}

fn denied(reason: &str) -> PublicWorkflowRoutePlan {
    blocked_route(
        "cg21.route.native_relational_not_admitted",
        "native relational SQL is not admitted",
        Diagnostic::unsupported(
            DiagnosticCode::UnsupportedSql,
            "public_workflow_route.sql_statement",
            reason.to_owned(),
            Some("use an admitted local native SQL shape and materialization policy; no fallback execution is available".into()),
        ),
    )
}

pub(super) fn run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    session: &mut PublicExecutionSession,
) -> ExitCode {
    match execute(request, plan, format, session) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            session.clear();
            emit_error("run", format, "native relational execution failed", &error)
        }
    }
}

fn execute(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    session: &mut PublicExecutionSession,
) -> Result<(), ShardLoomError> {
    let reused = session
        .relational
        .as_ref()
        .is_some_and(|entry| entry.request == *request);
    if !reused {
        session.clear();
        let statement = request.sql_statement.as_deref().ok_or_else(|| {
            ShardLoomError::InvalidOperation("native relational SQL is absent".into())
        })?;
        let (operation, prepared_sources) = sources::prepare(statement, request)?;
        session.relational = Some(PreparedPublicRelational {
            request: request.clone(),
            operation,
            prepared_sources,
        });
    }
    let prepared = session.relational.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation("native relational preparation did not complete".into())
    })?;
    let operation = &prepared.operation;
    let mut fields = execution_attachment_fields("run", request, plan);
    fields.extend([
        (
            "relational_normalized_source_count".into(),
            prepared.prepared_sources.to_string(),
        ),
        (
            "relational_source_normalization".into(),
            "all_source_leaves_resolved_during_native_binding".into(),
        ),
    ]);
    fields.retain(|(key, _)| key != "public_workflow_preparation_included");
    fields.push((
        "public_workflow_preparation_included".into(),
        (plan.preparation_included && !reused).to_string(),
    ));
    if is_write_request(request) {
        #[cfg(feature = "vortex-write")]
        {
            let targets =
                native_vortex_primitive_row_export_targets(request, "run").map_err(|blocked| {
                    ShardLoomError::InvalidOperation(format!(
                        "{}: {:?}",
                        blocked.blocker_reason, blocked.diagnostics
                    ))
                })?;
            let target = &targets[0];
            let written = operation.write(&target.path, target.format, request.allow_overwrite)?;
            append_native_vortex_primitive_row_export_fields(&mut fields, &written.output);
            append_native_vortex_primitive_row_export_target_fields(
                &mut fields,
                &targets,
                std::slice::from_ref(&written.output),
            );
            append_execution(&mut fields, &written.execution, reused);
            emit(
                "run",
                format,
                CommandStatus::Success,
                "native relational write".into(),
                format!(
                    "wrote {} rows to {}",
                    written.output.rows_written, written.output.output_path
                ),
                written.output.diagnostics.clone(),
                fields,
            );
            return Ok(());
        }
        #[cfg(not(feature = "vortex-write"))]
        return Err(ShardLoomError::InvalidOperation(
            "native relational writers require vortex-write".into(),
        ));
    }
    let collected = operation.collect_jsonl(&CancellationToken::default())?;
    append_execution(&mut fields, &collected.execution, reused);
    let (jsonl, _ownership) = collected.result_jsonl.into_parts();
    fields.extend([
        ("result_jsonl".into(), jsonl),
        ("result_payload_complete".into(), "true".into()),
        (
            "result_materialization_boundary".into(),
            "bounded_native_batches_to_jsonl".into(),
        ),
        ("output_io_performed".into(), "false".into()),
    ]);
    emit(
        "run",
        format,
        CommandStatus::Success,
        "native relational collection".into(),
        format!("collected {} rows", collected.execution.output_rows),
        vec![],
        fields,
    );
    Ok(())
}

fn append_execution(
    fields: &mut Vec<(String, String)>,
    result: &ExecutedVortexRelational,
    reused: bool,
) {
    vortex_primitive_execution::append_vortex_local_primitive_native_io_certificate_fields(
        fields,
        Some(&result.native_io_certificate),
    );
    let effects = &result.native_io_certificate.side_effects;
    let execution_fields = [
        ("runtime_execution".into(), "true".into()),
        (
            "execution".into(),
            "native_vortex_relational_performed".into(),
        ),
        ("source_io_performed".into(), "true".into()),
        ("fallback_attempted".into(), "false".into()),
        ("external_engine_invoked".into(), "false".into()),
        ("data_read".into(), effects.data_read.to_string()),
        ("data_decoded".into(), effects.data_decoded.to_string()),
        (
            "data_materialized".into(),
            effects.data_materialized.to_string(),
        ),
        ("output_row_count".into(), result.output_rows.to_string()),
        ("output_columns".into(), result.output_columns.join(",")),
        (
            "relational_output_batches".into(),
            result.output_batches.to_string(),
        ),
        (
            "relational_max_output_batch_rows".into(),
            result.max_output_batch_rows.to_string(),
        ),
        (
            "relational_output_buffer_bytes".into(),
            result.output_buffer_bytes.to_string(),
        ),
        (
            "relational_scan_rows_delivered".into(),
            result.scan_rows_delivered.to_string(),
        ),
        (
            "resident_source_opens".into(),
            result.runtime.prepared_source_opens.to_string(),
        ),
        (
            "public_workflow_native_vortex_plan_source_count".into(),
            result.runtime.prepared_source_opens.to_string(),
        ),
        (
            "resident_completed_executions".into(),
            result.runtime.completed_executions.to_string(),
        ),
        (
            "resident_provider_background_workers".into(),
            result.runtime.provider_background_workers.to_string(),
        ),
        (
            "resident_peak_reserved_buffer_bytes".into(),
            result.runtime.memory.peak_reserved_bytes.to_string(),
        ),
        (
            "resident_footer_open_performed_this_call".into(),
            (!reused).to_string(),
        ),
        ("resident_relational_handle_retained".into(), "true".into()),
        (
            "resident_relational_lowering_reused".into(),
            reused.to_string(),
        ),
        (
            "resident_source_generation_validation".into(),
            "all_sources_before_and_after_native_execution_and_final_consumer".into(),
        ),
        (
            "resident_decoded_byte_accounting".into(),
            "not_instrumented".into(),
        ),
        ("resident_provider_crate".into(), "vortex".into()),
        (
            "resident_provider_version".into(),
            shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION.into(),
        ),
    ];
    // Shared sink helpers also report effects; retain one unambiguous value per key.
    for (key, value) in execution_fields {
        fields.retain(|(existing, _)| existing != &key);
        fields.push((key, value));
    }
}
