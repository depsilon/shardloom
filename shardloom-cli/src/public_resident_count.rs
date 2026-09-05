//! Caller-retained exact footer count. Every call executes and validates the
//! prepared source; this module stores neither answers nor rendered reports.

use super::{
    CommandStatus, ExitCode, NativeVortexInputBinding, OutputFormat, PreparedPublicCount,
    PublicExecutionSession, PublicVortexPrimitive, PublicWorkflowRoutePlan,
    PublicWorkflowRouteRequest, ShardLoomError, emit, execution_attachment_fields,
    native_vortex_bound_request_and_arg, native_vortex_materializing_error,
    native_vortex_materializing_policy, vortex_primitive_execution,
};
use shardloom_vortex::resident_session::ResidentSessionSnapshot;

pub(super) fn execute_native_vortex_resident_count(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    mut extra_fields: Vec<(String, String)>,
    binding: &NativeVortexInputBinding,
    execution_session: &mut PublicExecutionSession,
) -> ExitCode {
    let result = execute(request, binding, execution_session);
    let (count, snapshot, prepared_now) = match result {
        Ok(result) => result,
        Err(error) => {
            execution_session.clear();
            return native_vortex_materializing_error(format, PublicVortexPrimitive::Count, &error);
        }
    };
    let mut fields = execution_attachment_fields("run", request, plan);
    fields.append(&mut extra_fields);
    fields.extend(binding.evidence_fields());
    fields.extend(count_fields(count, snapshot, prepared_now));
    append_effect_fields(&mut fields);
    // A footer metadata result does not run the row primitive/report pipeline or
    // an independent correctness oracle. Preserve their explicit absence.
    vortex_primitive_execution::append_vortex_local_primitive_execution_report_fields(
        &mut fields,
        None,
    );
    vortex_primitive_execution::append_vortex_local_primitive_native_io_certificate_fields(
        &mut fields,
        None,
    );
    vortex_primitive_execution::append_vortex_local_primitive_execution_certificate_fields(
        &mut fields,
        None,
    );
    emit(
        "run",
        format,
        CommandStatus::Success,
        "native Vortex resident metadata count".into(),
        format!("result summary: {count}\n"),
        Vec::new(),
        fields,
    );
    ExitCode::SUCCESS
}

fn count_fields(
    count: u64,
    snapshot: ResidentSessionSnapshot,
    prepared_now: bool,
) -> Vec<(String, String)> {
    vec![
        ("mode".into(), "vortex_run".into()),
        ("primitive".into(), "count".into()),
        ("execution".into(), "resident_vortex_footer_count".into()),
        ("result_known".into(), "true".into()),
        ("count".into(), count.to_string()),
        (
            "resident_source_opens".into(),
            snapshot.prepared_source_opens.to_string(),
        ),
        (
            "resident_completed_executions".into(),
            snapshot.completed_executions.to_string(),
        ),
        (
            "resident_peak_reserved_buffer_bytes".into(),
            snapshot.memory.peak_reserved_bytes.to_string(),
        ),
        (
            "resident_footer_open_performed_this_call".into(),
            prepared_now.to_string(),
        ),
        (
            "resident_source_generation_validation".into(),
            "before_and_after_native_footer_count".into(),
        ),
        ("resident_provider_crate".into(), "vortex".into()),
        (
            "resident_provider_version".into(),
            shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION.into(),
        ),
        (
            "local_primitive_no_query_answer_cache".into(),
            "true".into(),
        ),
        (
            "native_result_payload".into(),
            "native_footer_count_scalar".into(),
        ),
        ("file_io_performed".into(), "true".into()),
        (
            "file_io_scope".into(),
            "source_generation_stat_checks_and_initial_footer_open".into(),
        ),
        ("metadata_open_report_present".into(), "false".into()),
        ("metadata_open_status".into(), "none".into()),
        ("metadata_open_feature_enabled".into(), "true".into()),
        ("timing_surface".into(), "hot_runtime".into()),
        ("actual_evidence_tier".into(), "metadata_sink".into()),
        (
            "timing_claim_boundary".into(),
            "runtime_route_evidence_only_no_benchmark_or_publication_claim".into(),
        ),
    ]
}

fn append_effect_fields(fields: &mut Vec<(String, String)>) {
    for key in [
        "data_io_performed",
        "object_store_io_performed",
        "write_io_performed",
        "data_read",
        "data_decoded",
        "data_materialized",
        "row_read",
        "arrow_converted",
        "object_store_io",
        "write_io",
        "spill_io_performed",
        "external_effects_executed",
        "fallback_attempted",
        "external_engine_invoked",
        "fallback_execution_allowed",
        "route_total_timing_reported",
        "result_sink_timing_included_in_route_total",
        "evidence_render_timing_included_in_route_total",
    ] {
        fields.push((key.into(), "false".into()));
    }
}

fn execute(
    request: &PublicWorkflowRouteRequest,
    binding: &NativeVortexInputBinding,
    execution_session: &mut PublicExecutionSession,
) -> Result<(u64, ResidentSessionSnapshot, bool), ShardLoomError> {
    if request.vortex_predicate.is_some()
        || request.vortex_columns.is_some()
        || request.vortex_source_order_limit.is_some()
    {
        return Err(ShardLoomError::InvalidOperation(
            "native count-all does not admit predicate, projection, or limit payloads; select an explicit matching primitive; no fallback execution was attempted".into()));
    }
    let prepared_now = !execution_session
        .count
        .as_ref()
        .is_some_and(|entry| entry.request == *request);
    if prepared_now {
        execution_session.clear();
        let (primitive_request, _, _) =
            native_vortex_bound_request_and_arg(request, PublicVortexPrimitive::Count, binding)?;
        let policy = native_vortex_materializing_policy(request)?;
        let session = shardloom_vortex::resident_session::ResidentVortexSession::new(
            policy.resource_envelope.memory_budget_bytes,
            policy.max_parallelism,
        )?;
        let operation = shardloom_vortex::local_primitives::collect::prepare_count_in_session(
            &primitive_request,
            &session,
        )?;
        execution_session.count = Some(PreparedPublicCount {
            request: request.clone(),
            operation,
            session,
        });
    }
    let prepared = execution_session.count.as_ref().ok_or_else(|| {
        ShardLoomError::InvalidOperation(
            "prepared count was not admitted; no fallback execution was attempted".into(),
        )
    })?;
    let count = prepared.operation.execute()?;
    Ok((count, prepared.session.snapshot(), prepared_now))
}
