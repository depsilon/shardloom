//! Public SQL uses the same retained native relational plan for collect and sinks.

use super::{
    CommandStatus, Diagnostic, DiagnosticCode, ExitCode, OutputFormat, PublicExecutionSession,
    PublicSourcePreparations, PublicWorkflowRoutePlan, PublicWorkflowRouteRequest, ShardLoomError,
    admitted_route, append_native_result_schema_fields, blocked_route, emit, emit_error,
    execution_attachment_fields, is_write_request, native_vortex_materializing_policy,
    native_vortex_row_export_format_for_output_request, output_required_route,
    sql_local_source_runtime::native_relational, vortex_primitive_execution,
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

#[path = "public_relational_batches.rs"]
mod batches;
#[path = "public_relational_sources.rs"]
mod sources;
pub(super) use batches::run as run_batches;

pub(super) struct PreparedPublicRelational {
    pub(super) request: PublicWorkflowRouteRequest,
    operation: PreparedVortexRelational,
    prepared_sources: usize,
    preparations: PublicSourcePreparations,
}

pub(super) fn route(request: &PublicWorkflowRouteRequest) -> Option<PublicWorkflowRoutePlan> {
    let statement = request.sql_statement.as_deref()?;
    match native_relational::is_relational(statement) {
        Ok(false) => return None,
        Ok(true) => {}
        Err(error) => return Some(denied(&error.to_string())),
    }
    route_admitted_statement(request)
}

pub(super) fn route_admitted_statement(
    request: &PublicWorkflowRouteRequest,
) -> Option<PublicWorkflowRoutePlan> {
    let statement = request.sql_statement.as_deref()?;
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
    run_with_source(
        request,
        plan,
        format,
        session,
        Vec::new(),
        None,
        PublicSourcePreparations::default(),
    )
}

pub(super) fn run_with_source(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    session: &mut PublicExecutionSession,
    extra_fields: Vec<(String, String)>,
    source: Option<shardloom_vortex::resident_session::PreparedVortexSource>,
    preparations: PublicSourcePreparations,
) -> ExitCode {
    match execute(
        request,
        plan,
        format,
        session,
        extra_fields,
        source,
        preparations,
    ) {
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
    extra_fields: Vec<(String, String)>,
    source: Option<shardloom_vortex::resident_session::PreparedVortexSource>,
    preparations: PublicSourcePreparations,
) -> Result<(), ShardLoomError> {
    let statement = request.sql_statement.as_deref().ok_or_else(|| {
        ShardLoomError::InvalidOperation("native relational SQL is absent".into())
    })?;
    let reused = session.relational.as_ref().is_some_and(|entry| {
        entry.request == *request && entry.preparations.same_generations(&preparations)
    }) && !sources::has_file_collection(statement, request)?;
    if !reused {
        session.clear();
        let (operation, prepared_sources) =
            sources::prepare_with_source(statement, request, source, preparations.clone())?;
        session.relational = Some(PreparedPublicRelational {
            request: request.clone(),
            operation,
            prepared_sources,
            preparations,
        });
    }
    let prepared = session.relational.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation("native relational preparation did not complete".into())
    })?;
    let operation = &prepared.operation;
    let mut fields = execution_attachment_fields("run", request, plan);
    fields.extend(extra_fields);
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
        return execute_write(request, format, operation, fields, reused);
        #[cfg(not(feature = "vortex-write"))]
        return Err(ShardLoomError::InvalidOperation(
            "native relational writers require vortex-write".into(),
        ));
    }
    execute_collect(
        format,
        operation,
        fields,
        reused,
        &CancellationToken::default(),
    )
}

fn execute_collect(
    format: OutputFormat,
    operation: &PreparedVortexRelational,
    fields: Vec<(String, String)>,
    reused: bool,
    cancellation: &CancellationToken,
) -> Result<(), ShardLoomError> {
    let collected = operation.collect_jsonl(cancellation)?;
    emit_collected(format, collected, fields, reused);
    Ok(())
}

fn emit_collected(
    format: OutputFormat,
    collected: shardloom_vortex::local_primitives::prepared_relational::CollectedVortexRelational,
    mut fields: Vec<(String, String)>,
    reused: bool,
) {
    append_execution(&mut fields, &collected.execution, reused);
    let (jsonl, _ownership) = collected.result_jsonl.into_parts();
    let (schema, _schema_ownership) = collected.result_schema_json.into_parts();
    append_native_result_schema_fields(&mut fields, schema);
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
}

#[cfg(feature = "vortex-write")]
fn execute_write(
    request: &PublicWorkflowRouteRequest,
    format: OutputFormat,
    operation: &PreparedVortexRelational,
    fields: Vec<(String, String)>,
    reused: bool,
) -> Result<(), ShardLoomError> {
    execute_write_controlled(
        request,
        format,
        operation,
        fields,
        reused,
        &CancellationToken::default(),
    )
}

#[cfg(feature = "vortex-write")]
fn execute_write_controlled(
    request: &PublicWorkflowRouteRequest,
    format: OutputFormat,
    operation: &PreparedVortexRelational,
    fields: Vec<(String, String)>,
    reused: bool,
    cancellation: &CancellationToken,
) -> Result<(), ShardLoomError> {
    let targets =
        native_vortex_primitive_row_export_targets(request, "run").map_err(|blocked| {
            ShardLoomError::InvalidOperation(format!(
                "{}: {:?}",
                blocked.blocker_reason, blocked.diagnostics
            ))
        })?;
    let written = if targets.len() == 1 {
        let target = &targets[0];
        vec![operation.write_controlled(
            &target.path,
            target.format,
            request.allow_overwrite,
            cancellation,
        )?]
    } else {
        operation.write_many_controlled(
            &targets
                .iter()
                .map(|target| (target.path.clone(), target.format))
                .collect::<Vec<_>>(),
            request.allow_overwrite,
            cancellation,
        )?
    };
    emit_written(format, &targets, written, fields, reused);
    Ok(())
}

#[cfg(feature = "vortex-write")]
fn emit_written(
    format: OutputFormat,
    targets: &[super::NativeVortexPrimitiveRowExportTarget],
    mut written: Vec<
        shardloom_vortex::local_primitives::prepared_relational::WrittenVortexRelational,
    >,
    mut fields: Vec<(String, String)>,
    reused: bool,
) {
    let final_write = written.pop().expect("at least one target");
    let reports = written
        .into_iter()
        .map(|written| written.output)
        .chain([final_write.output])
        .collect::<Vec<_>>();
    let primary = &reports[0];
    append_native_vortex_primitive_row_export_fields(&mut fields, primary);
    append_native_vortex_primitive_row_export_target_fields(&mut fields, targets, &reports);
    append_execution(&mut fields, &final_write.execution, reused);
    emit(
        "run",
        format,
        CommandStatus::Success,
        "native relational write".into(),
        format!(
            "wrote {} rows to {}",
            primary.rows_written, primary.output_path
        ),
        primary.diagnostics.clone(),
        fields,
    );
}

fn append_spill(fields: &mut Vec<(String, String)>, result: &ExecutedVortexRelational) {
    fields.push((
        "relational_spill_requested".into(),
        result.spill.is_some().to_string(),
    ));
    if let Some(spill) = &result.spill {
        fields.extend([
            (
                "relational_spill_strategy".into(),
                "stable_native_full_row_two_run_merge".into(),
            ),
            (
                "relational_spill_workspace".into(),
                spill.workspace.display().to_string(),
            ),
            (
                "relational_spill_quota_bytes".into(),
                spill.quota_bytes.to_string(),
            ),
            (
                "relational_spill_buffer_bytes".into(),
                spill.buffer_bytes.to_string(),
            ),
            (
                "relational_spill_peak_disk_bytes".into(),
                spill.peak_disk_bytes.to_string(),
            ),
            (
                "relational_spill_runs_written".into(),
                spill.runs_written.to_string(),
            ),
            (
                "relational_spill_runs_validated".into(),
                spill.runs_validated.to_string(),
            ),
            (
                "relational_spill_merge_passes".into(),
                spill.merge_passes.to_string(),
            ),
            (
                "relational_spill_max_open_runs".into(),
                spill.max_open_runs.to_string(),
            ),
            (
                "relational_spill_run_block_rows".into(),
                spill.run_block_rows.to_string(),
            ),
            (
                "relational_spill_owned_cleanup_completed".into(),
                spill.owned_cleanup_completed.to_string(),
            ),
        ]);
    }
}

#[allow(clippy::too_many_lines)]
fn append_execution(
    fields: &mut Vec<(String, String)>,
    result: &ExecutedVortexRelational,
    reused: bool,
) {
    append_spill(fields, result);
    vortex_primitive_execution::append_vortex_local_primitive_native_io_certificate_fields(
        fields,
        Some(&result.native_io_certificate),
    );
    let effects = &result.native_io_certificate.side_effects;
    let write_io = effects.write_io
        || fields
            .iter()
            .any(|(key, value)| key == "write_io_performed" && value == "true");
    let execution_fields = [
        ("runtime_execution".into(), "true".into()),
        (
            "execution".into(),
            "native_vortex_relational_performed".into(),
        ),
        (
            "source_io_performed".into(),
            (result.runtime.prepared_source_opens > 0).to_string(),
        ),
        ("fallback_attempted".into(), "false".into()),
        ("external_engine_invoked".into(), "false".into()),
        (
            "spill_io_performed".into(),
            effects.spill_io_performed.to_string(),
        ),
        ("write_io_performed".into(), write_io.to_string()),
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
            "relational_ordered_aggregate_stages".into(),
            result.ordered_aggregate_stages.to_string(),
        ),
        (
            "relational_ordered_aggregate_input_rows".into(),
            result.ordered_aggregate_input_rows.to_string(),
        ),
        (
            "relational_ordered_aggregate_distinct_rows".into(),
            result.ordered_aggregate_distinct_rows.to_string(),
        ),
        (
            "relational_ordered_join_stages".into(),
            result.ordered_join_stages.to_string(),
        ),
        (
            "relational_ordered_join_build_rows".into(),
            result.ordered_join_build_rows.to_string(),
        ),
        (
            "relational_ordered_join_probe_rows".into(),
            result.ordered_join_probe_rows.to_string(),
        ),
        (
            "relational_ordered_join_candidate_rows".into(),
            result.ordered_join_candidate_rows.to_string(),
        ),
        (
            "relational_ordered_join_match_records".into(),
            result.ordered_join_match_records.to_string(),
        ),
        (
            "relational_ordered_join_lookup_blocks".into(),
            result.ordered_join_lookup_blocks.to_string(),
        ),
        (
            "resident_source_opens".into(),
            result.runtime.prepared_source_opens.to_string(),
        ),
        (
            "public_workflow_native_vortex_plan_source_count".into(),
            result.prepared_sources.to_string(),
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
            "resident_memory_limit_bytes".into(),
            result.runtime.memory.limit_bytes.to_string(),
        ),
        (
            "resident_footer_open_performed_this_call".into(),
            (!reused && result.runtime.prepared_source_opens > 0).to_string(),
        ),
        ("resident_relational_handle_retained".into(), "true".into()),
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
    for (key, value) in execution_fields
        .into_iter()
        .chain(schema_binding_fields(result, reused))
    {
        fields.retain(|(existing, _)| existing != &key);
        fields.push((key, value));
    }
    append_batch_input(fields, result);
}

fn append_batch_input(fields: &mut Vec<(String, String)>, result: &ExecutedVortexRelational) {
    let Some(input) = &result.input else {
        return;
    };
    // These values come from the completed native operation, after explicit end
    // and owner release, rather than a transport snapshot taken during binding.
    let completed = [
        ("native_input_batches", input.payload_batches.to_string()),
        ("native_input_batch_sources", "1".into()),
        ("native_input_batch_rows", input.rows.to_string()),
        ("native_input_logical_bytes", input.input_logical_bytes.to_string()),
        ("native_input_payload_bytes_copied", input.intake_payload_bytes_copied.to_string()),
        ("native_input_max_batch_rows", input.max_batch_rows.to_string()),
        ("native_input_max_retained_batches", input.max_retained_input_batches.to_string()),
        ("native_input_max_retained_logical_bytes", input.max_retained_input_logical_bytes.to_string()),
        ("native_input_end_observed", input.end_of_input_observed.to_string()),
        ("native_input_output_ownership_detached", input.output_ownership_detached.to_string()),
        ("native_input_ordering_batches_detached", input.ordering_batches_detached.to_string()),
        ("native_input_ordering_rows_detached", input.ordering_rows_detached.to_string()),
        ("native_input_join_build_batches_detached", input.join_build_batches_detached.to_string()),
        ("native_input_join_build_rows_detached", input.join_build_rows_detached.to_string()),
        ("native_batch_input_storage", "single_current_native_batch;released_before_next_demand;no_input_spill".into()),
        ("native_input_byte_accounting", "logical_values_offsets_validity_and_names;not_allocated_capacity_or_rss".into()),
        ("resident_source_generation_validation", "declared_schema_checked_each_batch;explicit_source_end;all_input_owners_released;bound_file_generations_checked_before_and_after_final_consumer;final_consumer_completed".into()),
    ];
    for (key, value) in completed {
        fields.retain(|(existing, _)| existing != key);
        fields.push((key.into(), value));
    }
}

fn schema_binding_fields(result: &ExecutedVortexRelational, reused: bool) -> [(String, String); 4] {
    [
        (
            "resident_relational_declaration_reused".into(),
            reused.to_string(),
        ),
        (
            "resident_relational_lowering_reused".into(),
            (reused && !result.schema_binding_deferred).to_string(),
        ),
        (
            "relational_schema_binding".into(),
            if result.schema_binding_deferred {
                "during_execution"
            } else {
                "during_preparation"
            }
            .into(),
        ),
        (
            "relational_dynamic_schema_stages".into(),
            result.dynamic_schema_stages.to_string(),
        ),
    ]
}
