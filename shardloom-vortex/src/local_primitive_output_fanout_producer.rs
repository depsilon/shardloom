//! Existing primitive providers feed the common once-only native result replay.

use super::{Format, PathBuf, Result, WrittenVortexPrimitiveFanout, failed};
use crate::local_primitives::{
    self as native, VortexLocalPrimitiveExecutionPolicy as Policy,
    VortexLocalPrimitiveExecutionReport as Execution,
    VortexLocalPrimitiveRowExportReport as Output, VortexQueryPrimitiveKind as Kind,
    VortexQueryPrimitiveRequest as Request,
};
use crate::resident_session::{NativeBatchConsumer, PreparedVortexSource};
use shardloom_exec::compute_pool::CancellationToken;

pub(super) fn write(
    request: &Request,
    targets: &[(PathBuf, Format)],
    overwrite: bool,
    policy: Policy,
    source: &PreparedVortexSource,
    cancellation: &CancellationToken,
) -> Result<WrittenVortexPrimitiveFanout> {
    let (policy, physical) = policy.with_writer_sink_physical_policy_for_request(request);
    let session = native::prepared_dispatch::source_session(source, request, Some(policy))?;
    if matches!(
        request.kind,
        Kind::ProjectColumns | Kind::FilterPredicate | Kind::FilterAndProject
    ) || (request.kind == Kind::ExpressionProjectRows && request.structured_projection.is_some())
    {
        let operation = native::prepared_dispatch::prepare_projection_from_source(
            request,
            policy,
            source.clone(),
            cancellation,
        )?;
        let written = operation.write_many_shared_controlled(targets, overwrite, cancellation)?;
        let mut outputs = written.outputs;
        for output in &mut outputs {
            output.primitive_kind = request.kind;
            output.physical_policy = physical.clone();
            native::prepared_dispatch::annotate_projection_output(
                request,
                source.file().row_count(),
                output,
            )?;
        }
        return Ok(WrittenVortexPrimitiveFanout {
            outputs,
            replay: written.replay,
        });
    }
    let written = native::prepared_relational::write_produced_many_controlled(
        &session,
        policy,
        request,
        std::slice::from_ref(source),
        targets,
        overwrite,
        cancellation,
        |consume| produce(request, policy, source, cancellation, consume),
    )?;
    let mut outputs = written.outputs;
    for output in &mut outputs {
        output.primitive_kind = request.kind;
        output.physical_policy = physical.clone();
        output.source_order_limit_requested = request
            .source_order_limit
            .map(native::usize_to_u64)
            .transpose()?;
        if let Some(execution) = &written.execution {
            annotate(output, execution);
        } else {
            // Metadata COUNT's producer reads no source payload. Keep actual
            // output-adapter work, separate from its one-row replay scan.
            output.rows_scanned = source.file().row_count();
            output.arrays_read_count = 0;
            output.max_chunk_rows = 0;
            output.evidence.upstream_scan_called = false;
            output.evidence.side_effects.data_read = false;
        }
        if request.kind == Kind::SimpleAggregate {
            native::prepared_aggregate::annotate_output_count(output, request);
        }
    }
    Ok(WrittenVortexPrimitiveFanout {
        outputs,
        replay: Some(written.replay),
    })
}

fn produce(
    request: &Request,
    policy: Policy,
    source: &PreparedVortexSource,
    cancellation: &CancellationToken,
    consume: &mut NativeBatchConsumer<'_>,
) -> Result<Option<Execution>> {
    cancellation.check()?;
    let report = match request.kind {
        Kind::CountAll => {
            let operation =
                native::collect::prepare_count_from_source(request, source, cancellation)?;
            let (_, result) = operation.execute_native_result(cancellation)?;
            result.for_each_native_batch(cancellation, consume)?;
            None
        }
        Kind::CountWhere => {
            let operation = native::prepared_count::prepare_count_where_from_source_controlled(
                request,
                policy,
                source.clone(),
                cancellation,
            )?;
            let completed = operation.execute_native_result(cancellation)?;
            completed
                .result
                .for_each_native_batch(cancellation, consume)?;
            Some(completed.execution.report)
        }
        Kind::SimpleAggregate => Some(
            native::prepared_aggregate::prepare_aggregate_from_source(
                request,
                policy,
                source.clone(),
            )?
            .for_each_batch(cancellation, consume)?
            .report,
        ),
        Kind::SortRows => Some(
            native::prepared_dispatch::for_each_sort_batch(
                request,
                policy,
                source,
                cancellation,
                consume,
            )?
            .report,
        ),
        _ if native::prepared_unary::supports(request.kind)
            && request.structured_projection.is_none() =>
        {
            Some(
                native::prepared_unary::prepare_unary_from_source(request, policy, source.clone())?
                    .for_each_batch(cancellation, consume)?
                    .report,
            )
        }
        _ => return Err(failed("native primitive fanout producer is not admitted")),
    };
    source.validate_generation()?;
    cancellation.check()?;
    Ok(report)
}

fn annotate(output: &mut Output, execution: &Execution) {
    output.rows_scanned = execution.rows_scanned;
    output.arrays_read_count = execution.arrays_read_count;
    output.max_chunk_rows = execution.max_chunk_rows;
    output.state_budget = execution.state_budget.clone();
    output.source_order_limit_requested = execution.source_order_limit_requested;
    output.evidence.upstream_scan_called = execution.upstream_scan_called;
    output.evidence.side_effects.data_read |= execution.data_read;
    output.evidence.side_effects.data_decoded |= execution.data_decoded;
    output.evidence.side_effects.data_materialized |= execution.data_materialized;
    output.evidence.side_effects.row_read |= execution.row_read;
    output.evidence.side_effects.spill_io_performed |= execution.spill_io_performed;
    output.evidence.pushdown = native::VortexLocalPrimitiveRowExportPushdownEvidence {
        filter_pushdown_applied: execution.filter_pushdown_applied,
        projection_pushdown_applied: execution.projection_pushdown_applied,
        source_order_limit_applied: execution.source_order_limit_applied,
    };
}
