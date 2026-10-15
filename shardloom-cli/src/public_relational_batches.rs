//! Batch transport changes input/output adapters only. Native source resolution,
//! SQL lowering, execution, writers and evidence stay with their existing owners.

#[cfg(feature = "vortex-write")]
use super::execute_write_controlled;
use super::{
    CommandStatus, ExitCode, OutputFormat, PublicWorkflowRoutePlan, PublicWorkflowRouteRequest,
    ShardLoomError, append_execution, emit, emit_error, execute_collect,
    execution_attachment_fields, is_write_request, sources,
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
    let cancellation = transport.cancellation.clone();
    cancellation.check()?;
    #[cfg(feature = "vortex-write")]
    if is_write_request(request) {
        shardloom_vortex::local_primitives::output_fanout::preflight_targets(
            &write_targets(request)?
                .into_iter()
                .map(|target| (target.path, target.format))
                .collect::<Vec<_>>(),
            request.allow_overwrite,
        )?;
    }
    let adapter = std::cell::RefCell::new(&mut *transport);
    let (operation, normalized) = sources::prepare_with_input_adapter(
        statement,
        request,
        |session| adapter.borrow_mut().start_reader(session),
        |uri, input, session| adapter.borrow_mut().build_source(uri, input, session),
        &cancellation,
    )?;
    if request.source_bindings.values().any(|binding| {
        matches!(
            binding.memory_input,
            Some(crate::native_memory_input::MemoryInput::Batches {
                streaming: true,
                ..
            })
        )
    }) {
        return execute_streaming(request, plan, &operation, normalized, transport);
    }
    let mut fields = execution_attachment_fields("run", request, plan);
    fields.extend(adapter_fields(transport, normalized));
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
    emit_batches(fields, &result, transport);
    Ok(())
}

fn execute_streaming(
    request: &PublicWorkflowRouteRequest,
    plan: &PublicWorkflowRoutePlan,
    operation: &super::PreparedVortexRelational,
    normalized: usize,
    transport: &mut Transport,
) -> Result<(), ShardLoomError> {
    let cancellation = transport.cancellation.clone();
    cancellation.check()?;
    let batch_rows = transport.batch_rows;
    let stream_results = transport.stream_results;
    let transport = std::cell::RefCell::new(transport);
    let mut input =
        |uri: &shardloom_core::DatasetUri,
         ordinal: u64,
         session: &shardloom_vortex::resident_session::ResidentVortexSession| {
            let declaration = request.source_bindings.get(uri.as_str())
            .and_then(|binding| binding.memory_input.as_ref())
            .ok_or_else(|| ShardLoomError::InvalidOperation(
                "SL-NATIVE-BATCH: demanded streaming source is undeclared; no fallback execution was attempted".into(),
            ))?;
            transport
                .borrow_mut()
                .next_source_batch(uri.as_str(), ordinal, declaration, session)
        };
    let execution = operation.with_batch_inputs(&mut input)?;
    let completed_fields = || {
        let mut fields = execution_attachment_fields("run", request, plan);
        fields.extend(adapter_fields(&transport.borrow(), normalized));
        fields
    };
    if is_write_request(request) {
        #[cfg(feature = "vortex-write")]
        {
            let targets = write_targets(request)?;
            let result = execution.write_many_controlled(
                &targets
                    .iter()
                    .map(|target| (target.path.clone(), target.format))
                    .collect::<Vec<_>>(),
                request.allow_overwrite,
                &cancellation,
            )?;
            let mut fields = completed_fields();
            let primary = super::append_written_fanout(&mut fields, &targets, result, false);
            emit(
                "run",
                OutputFormat::Json,
                CommandStatus::Success,
                "native relational write".into(),
                format!(
                    "wrote {} rows to {}",
                    primary.rows_written, primary.output_path
                ),
                primary.diagnostics,
                fields,
            );
            return Ok(());
        }
        #[cfg(not(feature = "vortex-write"))]
        return Err(ShardLoomError::InvalidOperation(
            "native writers require vortex-write".into(),
        ));
    }
    if !stream_results {
        let result = execution.collect_jsonl(&cancellation)?;
        super::emit_collected(OutputFormat::Json, result, completed_fields(), false);
        return Ok(());
    }
    let result = execution.for_each_json_batch(&cancellation, batch_rows, 8 << 20, |batch| {
        transport.borrow_mut().consume(&batch)
    })?;
    emit_batches(completed_fields(), &result, &transport.borrow());
    Ok(())
}

#[cfg(feature = "vortex-write")]
fn write_targets(
    request: &PublicWorkflowRouteRequest,
) -> Result<Vec<super::super::NativeVortexPrimitiveRowExportTarget>, ShardLoomError> {
    super::native_vortex_primitive_row_export_targets(request, "run").map_err(|blocked| {
        ShardLoomError::InvalidOperation(format!(
            "{}: {:?}",
            blocked.blocker_reason, blocked.diagnostics
        ))
    })
}

fn emit_batches(
    mut fields: Vec<(String, String)>,
    result: &super::ExecutedVortexRelational,
    transport: &Transport,
) {
    append_execution(&mut fields, result, false);
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
            transport.input_scratch_peak_bytes().to_string(),
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
