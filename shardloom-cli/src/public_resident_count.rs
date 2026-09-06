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

struct ExecutedCount {
    count: u64,
    snapshot: ResidentSessionSnapshot,
    prepared_now: bool,
    native_io_certificate: shardloom_core::NativeIoCertificate,
}

pub(super) fn execute_native_vortex_resident_count(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    format: OutputFormat,
    mut extra_fields: Vec<(String, String)>,
    binding: &NativeVortexInputBinding,
    execution_session: &mut PublicExecutionSession,
) -> ExitCode {
    let result = execute(request, binding, execution_session);
    let ExecutedCount {
        count,
        snapshot,
        prepared_now,
        native_io_certificate,
    } = match result {
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
    fields.push((
        "resident_native_io_proof_basis".into(),
        native_io_certificate
            .source_pushdown_report
            .proof_basis
            .clone(),
    ));
    append_effect_fields(&mut fields);
    // A footer metadata result does not run the row primitive/report pipeline or
    // an independent correctness oracle. Preserve their explicit absence.
    vortex_primitive_execution::append_vortex_local_primitive_execution_report_fields(
        &mut fields,
        None,
    );
    vortex_primitive_execution::append_vortex_local_primitive_native_io_certificate_fields(
        &mut fields,
        Some(&native_io_certificate),
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
) -> Result<ExecutedCount, ShardLoomError> {
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
    let snapshot = prepared.session.snapshot();
    // Issue evidence only after the retained operation completed both generation
    // checks. Repeated calls still execute against the retained native footer.
    let native_io_certificate = count_certificate(binding, count, snapshot, prepared_now)?;
    Ok(ExecutedCount {
        count,
        snapshot,
        prepared_now,
        native_io_certificate,
    })
}

fn count_certificate(
    binding: &NativeVortexInputBinding,
    count: u64,
    snapshot: ResidentSessionSnapshot,
    prepared_now: bool,
) -> Result<shardloom_core::NativeIoCertificate, ShardLoomError> {
    use shardloom_core::{
        NativeIoAdapterFidelityReport, NativeIoCertificate, NativeIoRepresentationTransition,
        NativeIoSinkRequirementReport, NativeIoSourceCapabilityReport,
        NativeIoSourcePushdownReport, RepresentationState,
    };
    let [source] = binding.sources.as_slice() else {
        return Err(ShardLoomError::InvalidOperation(
            "resident footer count certificate requires one admitted local source; no fallback execution was attempted".into(),
        ));
    };
    NativeIoCertificate::new(
        "resident.count_all.native_io",
        "native_vortex_source_to_scalar_count_result",
        NativeIoSourceCapabilityReport {
            source_kind: "vortex".into(),
            adapter_id: "shardloom.resident_vortex.v1".into(),
            schema_discovery_status: "retained_footer_generation_validated".into(),
            statistics_availability: "exact_footer_row_count".into(),
            pushdown_capabilities: "count_all".into(),
            encoded_representation_preserved: true,
            range_read_capability: false,
            streaming_capability: false,
            object_store_capability: false,
            fallback_attempted: false,
        },
        NativeIoSourcePushdownReport {
            accepted_operations: vec!["count_all".into()],
            rejected_operations: Vec::new(),
            guarantee: "exact_retained_footer_row_count".into(),
            proof_basis: format!(
                "vortex {};feature=vortex-local-primitives,unix;source={source};provider=VortexFile::row_count;source_generation_validation=before_and_after_native_footer_count;footer_open_performed_this_call={prepared_now};completed_executions={};row_count={count};no_query_answer_cache=true;no_scan_decode_or_row_materialization",
                shardloom_vortex::UPSTREAM_VORTEX_PROVIDER_VERSION,
                snapshot.completed_executions,
            ),
            residual_expression: None,
            conservative_false_positive_policy: false,
            unsafe_rejected_reason: None,
            fallback_attempted: false,
        },
        vec![NativeIoRepresentationTransition::new(
            RepresentationState::MetadataOnly,
            RepresentationState::MetadataOnly,
            false,
        )],
        NativeIoSinkRequirementReport {
            target_format: "scalar_count_result".into(),
            accepts_encoded: true,
            requires_decoded_columnar: false,
            requires_rows: false,
            preserves_metadata: false,
            requires_ordering: false,
            requires_partitioning: false,
            requires_commit: false,
            supports_streaming: false,
            max_chunk_size: None,
            backpressure_policy: "single_exact_u64_footer_count_no_row_stream".into(),
        },
        NativeIoAdapterFidelityReport {
            adapter_id: "shardloom.resident_vortex.v1".into(),
            source_kind: "vortex".into(),
            sink_kind: "scalar_count_result".into(),
            metadata_preserved: false,
            statistics_preserved: false,
            encoded_representation_preserved: true,
            materialization_required: false,
            fidelity_loss: "none_for_exact_footer_count".into(),
            metadata_loss: "scalar_count_result_has_no_column_metadata".into(),
            fallback_attempted: false,
        },
        Vec::new(),
        count_side_effects(),
        Vec::new(),
    )
}

fn count_side_effects() -> shardloom_core::NativeIoSideEffectReport {
    shardloom_core::NativeIoSideEffectReport {
        data_read: false,
        data_decoded: false,
        data_materialized: false,
        row_read: false,
        arrow_converted: false,
        object_store_io: false,
        write_io: false,
        spill_io_performed: false,
        external_effects_executed: false,
        fallback_attempted: false,
        fallback_execution_allowed: false,
    }
}
