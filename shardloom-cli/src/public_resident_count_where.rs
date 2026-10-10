//! Matching-request reuse for actual native filtered counts, without an answer cache.

use super::{
    CommandStatus, ExitCode, NativeVortexInputBinding, OutputFormat, PreparedPublicCountWhere,
    PublicExecutionSession, PublicVortexPrimitive, PublicWorkflowRoutePlan,
    PublicWorkflowRouteRequest, ShardLoomError, emit, execution_attachment_fields,
    native_vortex_bound_request_and_arg, native_vortex_materializing_error,
    native_vortex_materializing_policy, vortex_primitive_execution,
};
use shardloom_vortex::local_primitives::prepared_count::ExecutedVortexCountWhere;

pub(super) fn run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    extra_fields: Vec<(String, String)>,
    binding: &NativeVortexInputBinding,
    execution_session: &mut PublicExecutionSession,
) -> ExitCode {
    run_with_source(
        request,
        plan,
        format,
        extra_fields,
        binding,
        execution_session,
        None,
    )
}

pub(super) fn run_with_source(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    mut extra_fields: Vec<(String, String)>,
    binding: &NativeVortexInputBinding,
    execution_session: &mut PublicExecutionSession,
    source: Option<shardloom_vortex::resident_session::PreparedVortexSource>,
) -> ExitCode {
    let (executed, rows, prepared_now) = match execute(request, binding, execution_session, source)
    {
        Ok(result) => result,
        Err(error) => {
            execution_session.clear();
            return native_vortex_materializing_error(
                format,
                PublicVortexPrimitive::CountWhere,
                &error,
            );
        }
    };
    let mut fields = execution_attachment_fields("run", request, plan);
    fields.append(&mut extra_fields);
    fields.extend(binding.evidence_fields());
    append_fields(&mut fields, request, &executed, prepared_now);
    let (jsonl, _json_ownership) = rows.result_jsonl.into_parts();
    let (schema, _schema_ownership) = rows.result_schema_json.into_parts();
    super::append_native_result_schema_fields(&mut fields, schema);
    fields.extend([
        ("result_jsonl".into(), jsonl),
        ("result_payload_complete".into(), "true".into()),
        ("output_row_count".into(), "1".into()),
    ]);
    let local = vortex_primitive_execution::VortexLocalPrimitiveCliExecutionEvidence {
        resources: executed.report.resource_envelope.declared_resources,
        report: executed.report,
        native_io_certificate: executed.native_io_certificate,
        // This call executes the source, not an independent correctness oracle.
        execution_certificate: None,
    };
    let human = local.report.to_human_text();
    let diagnostics = local.report.diagnostics.clone();
    let failed = local.runtime_has_errors();
    vortex_primitive_execution::append_vortex_count_where_local_execution_fields(
        &mut fields,
        Some(&local),
    );
    crate::execution_resources::append_resident_snapshot_fields(&mut fields, &executed.runtime);
    emit(
        "run",
        format,
        if failed {
            CommandStatus::Unsupported
        } else {
            CommandStatus::Success
        },
        "native Vortex resident filtered count".into(),
        human,
        diagnostics,
        fields,
    );
    if failed {
        execution_session.clear();
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    }
}

fn execute(
    request: &PublicWorkflowRouteRequest,
    binding: &NativeVortexInputBinding,
    execution_session: &mut PublicExecutionSession,
    source: Option<shardloom_vortex::resident_session::PreparedVortexSource>,
) -> Result<
    (
        ExecutedVortexCountWhere,
        shardloom_vortex::local_primitives::collect::CollectedCountRows,
        bool,
    ),
    ShardLoomError,
> {
    if request.vortex_columns.is_some() || request.vortex_source_order_limit.is_some() {
        return Err(ShardLoomError::InvalidOperation(
            "native filtered count does not admit projection or limit payloads; select an explicit matching primitive; no fallback execution was attempted".into()));
    }
    let prepared_now = !execution_session
        .count_where
        .as_ref()
        .is_some_and(|entry| entry.request == *request);
    if prepared_now {
        execution_session.clear();
        let (primitive, _, _) = native_vortex_bound_request_and_arg(
            request,
            PublicVortexPrimitive::CountWhere,
            binding,
        )?;
        let policy = native_vortex_materializing_policy(request)?;
        let operation = if let Some(source) = source {
            shardloom_vortex::local_primitives::prepared_count::prepare_count_where_from_source(
                &primitive, policy, source,
            )?
        } else {
            let session = shardloom_vortex::resident_session::ResidentVortexSession::new(
                policy.resource_envelope.memory_budget_bytes,
                policy.max_parallelism,
            )?;
            shardloom_vortex::local_primitives::prepared_count::prepare_count_where_in_session(
                &primitive, policy, &session,
            )?
        };
        execution_session.count_where = Some(PreparedPublicCountWhere {
            request: request.clone(),
            operation,
        });
    }
    let prepared = execution_session.count_where.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "prepared filtered count was not admitted; no fallback execution was attempted".into(),
        )
    })?;
    let (execution, rows) = prepared.operation.collect_jsonl()?;
    Ok((execution, rows, prepared_now))
}

fn append_fields(
    fields: &mut Vec<(String, String)>,
    request: &PublicWorkflowRouteRequest,
    executed: &ExecutedVortexCountWhere,
    prepared_now: bool,
) {
    fields.extend([
        ("mode".into(), "vortex_count_where".into()),
        ("primitive".into(), "count_where".into()),
        ("execution".into(), "resident_vortex_filtered_count".into()),
        ("result_known".into(), "true".into()),
        ("count".into(), executed.count.to_string()),
        (
            "predicate".into(),
            request.vortex_predicate.clone().unwrap_or_default(),
        ),
        (
            "resident_source_opens".into(),
            executed.runtime.prepared_source_opens.to_string(),
        ),
        (
            "resident_completed_executions".into(),
            executed.runtime.completed_executions.to_string(),
        ),
        (
            "resident_provider_background_workers".into(),
            executed.runtime.provider_background_workers.to_string(),
        ),
        (
            "resident_peak_reserved_buffer_bytes".into(),
            executed.runtime.memory.peak_reserved_bytes.to_string(),
        ),
        (
            "resident_footer_open_performed_this_call".into(),
            prepared_now.to_string(),
        ),
        (
            "resident_source_generation_validation".into(),
            "before_and_after_native_scan_including_metadata_pruned_result".into(),
        ),
        ("resident_provider_crate".into(), "vortex".into()),
        (
            "resident_provider_version".into(),
            shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION.into(),
        ),
        (
            "metadata_query_primitive_status".into(),
            "separate_metadata_facade_not_executed".into(),
        ),
        (
            "query_primitive_status".into(),
            executed.report.status.as_str().into(),
        ),
        ("file_io_performed".into(), "true".into()),
        (
            "file_io_scope".into(),
            "source_generation_checks_initial_footer_and_actual_native_scan_reads".into(),
        ),
        ("timing_surface".into(), "hot_runtime".into()),
        (
            "timing_claim_boundary".into(),
            "runtime_route_evidence_only_no_benchmark_or_publication_claim".into(),
        ),
    ]);
    for (key, value) in [
        ("data_read", executed.report.data_read),
        ("data_decoded", executed.report.data_decoded),
        ("data_materialized", executed.report.data_materialized),
        ("row_read", executed.report.row_read),
        ("arrow_converted", false),
        ("object_store_io", false),
        ("write_io", false),
        ("spill_io_performed", false),
        ("external_effects_executed", false),
        ("fallback_attempted", false),
        ("fallback_execution_allowed", false),
        ("external_engine_invoked", false),
        ("route_total_timing_reported", false),
        ("result_sink_timing_included_in_route_total", false),
        ("evidence_render_timing_included_in_route_total", false),
    ] {
        fields.push((key.into(), value.to_string()));
    }
}
