//! Batch transport changes input/output adapters only. Native source resolution,
//! SQL lowering, execution, writers and evidence stay with their existing owners.

#[cfg(feature = "vortex-write")]
use super::execute_write_controlled;
use super::{
    CommandStatus, ExitCode, OutputFormat, PublicSourcePreparations, PublicWorkflowRoutePlan,
    PublicWorkflowRouteRequest, ShardLoomError, append_execution, emit, emit_error,
    execute_collect, execution_attachment_fields, is_write_request, sources,
};
use crate::python_batch_protocol::Transport;

pub(in crate::public_workflow_route) fn run(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    transport: &mut Transport,
) -> ExitCode {
    match execute(request, plan, transport) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => emit_error(
            "run",
            OutputFormat::Json,
            "native batch transaction failed",
            &error,
        ),
    }
}

fn execute(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    transport: &mut Transport,
) -> Result<(), ShardLoomError> {
    if transport.stream_results && is_write_request(request) {
        return Err(ShardLoomError::InvalidOperation(
            "batch result consumption and a file sink must be separate requests".into(),
        ));
    }
    let statement = request.sql_statement.as_deref().ok_or_else(|| {
        ShardLoomError::InvalidOperation("native batch SQL declaration is absent".into())
    })?;
    // The existing compatibility preparation API has no external cancellation
    // owner. Do not start that effectful lifecycle in a cancellable transaction.
    // Users can prepare it explicitly, then consume the resulting native source.
    if sources::normalization_required(statement, request)? {
        return Err(ShardLoomError::InvalidOperation(
            "SL-NATIVE-BATCH: batch transactions require native Vortex or memory sources; prepare compatibility inputs to Vortex before batch consumption; no fallback execution was attempted".into(),
        ));
    }
    let (operation, normalized) = sources::prepare_with_input_adapter(
        statement,
        request,
        None,
        PublicSourcePreparations::default(),
        |uri, input, session| transport.build_source(uri, input, session),
    )?;
    let mut fields = execution_attachment_fields("run", request, plan);
    fields.extend(adapter_fields(transport, normalized));
    let cancellation = transport.cancellation.clone();
    cancellation.check()?;
    if is_write_request(request) {
        #[cfg(feature = "vortex-write")]
        return execute_write_controlled(
            request,
            OutputFormat::Json,
            &operation,
            fields,
            false,
            &cancellation,
        );
        #[cfg(not(feature = "vortex-write"))]
        return Err(ShardLoomError::InvalidOperation(
            "native writers require vortex-write".into(),
        ));
    }
    if !transport.stream_results {
        return execute_collect(OutputFormat::Json, &operation, fields, false, &cancellation);
    }
    let result =
        operation.for_each_json_batch(&cancellation, transport.batch_rows, 8 << 20, |batch| {
            transport.consume(&batch)
        })?;
    append_execution(&mut fields, &result, false);
    fields.extend([
        ("result_payload_complete".into(), "true".into()),
        (
            "result_materialization_boundary".into(),
            "bounded_native_batches_to_incremental_json".into(),
        ),
        (
            "native_result_batches_acknowledged".into(),
            transport.output_batches.to_string(),
        ),
        ("output_io_performed".into(), "false".into()),
    ]);
    emit(
        "run",
        OutputFormat::Json,
        CommandStatus::Success,
        "native result batch consumption".into(),
        format!("consumed {} rows", result.output_rows),
        vec![],
        fields,
    );
    Ok(())
}

fn adapter_fields(transport: &Transport, normalized: usize) -> [(String, String); 10] {
    [
        (
            "relational_normalized_source_count".into(),
            normalized.to_string(),
        ),
        (
            "relational_source_normalization".into(),
            "all_source_leaves_resolved_during_native_binding".into(),
        ),
        (
            "native_batch_protocol".into(),
            "shardloom.native_batches.v1".into(),
        ),
        (
            "native_input_batches".into(),
            transport.input_batches.to_string(),
        ),
        (
            "native_input_batch_sources".into(),
            transport.input_sources.to_string(),
        ),
        (
            "native_input_batch_rows".into(),
            transport.input_rows.to_string(),
        ),
        (
            "native_batch_input_storage".into(),
            "resident_native_arrays_under_shared_grant;no_input_spill".into(),
        ),
        (
            "native_batch_input_peak_scratch_reservation_bytes".into(),
            transport.input_scratch_peak_bytes.to_string(),
        ),
        (
            "native_batch_control_frame_bound_bytes".into(),
            (8_u64 << 20).to_string(),
        ),
        (
            "native_batch_declaration_frame_bound_bytes".into(),
            crate::python_worker_protocol::MAX_REQUEST_BYTES.to_string(),
        ),
    ]
}
