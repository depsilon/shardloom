//! Late payload delivery in final order from bounded native row-index scans.
//!
//! The sort has already selected source ordinals. Reading their projected payload
//! is late materialization, not another evaluation of the query or its predicate.

use super::{
    Result, ShardLoomError, SortRowCandidate, SortRowsMaterializationResult,
    VortexQueryPrimitiveRequest, completed_result::CompletedRows, result_batch::Value,
    vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::StatValue;
use shardloom_exec::live_memory::MemoryLease;
use vortex::{
    array::{
        ArrayRef, VortexSessionExecute as _,
        arrays::{VarBinViewArray, varbinview::VarBinViewArrayExt as _},
        dtype::DType,
    },
    buffer::Buffer,
    file::VortexFile,
    io::runtime::BlockingRuntime,
    layout::scan::split_by::SplitBy,
    mask::Mask,
    scan::strict_sorted_buffer::StrictSortedBuffer,
};

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native ordered result: {reason}; no fallback execution was attempted"
    ))
}

pub(super) fn scalar_value(scalar: &vortex::array::scalar::Scalar) -> Result<Value<'static>> {
    if scalar.is_null() {
        return Ok(Value::Null);
    }
    if matches!(scalar.dtype(), DType::Utf8(_)) {
        return scalar
            .as_utf8()
            .value()
            .cloned()
            .map(Value::SharedText)
            .ok_or_else(|| failed("non-null UTF8 scalar has no value"));
    }
    super::vortex_scalar_to_stat_value(scalar)
        .map(Value::from)
        .ok_or_else(|| failed("output scalar differs from the admitted dtype"))
}

/// Reserve before the old sort kernels allocate scalar key/predicate scratch.
/// The selected keys remain accounted separately from the native payload buffers.
pub(super) fn scratch_bytes(
    array: &ArrayRef,
    columns: &[String],
    context: &mut vortex::array::ExecutionCtx,
) -> Result<u64> {
    let mut bytes = scalar_scratch_bytes(array.len(), columns.len());
    for name in columns {
        let field = super::logical_field_from_native_array(array, name)?;
        if matches!(field.dtype(), DType::Utf8(_)) {
            // Scalar access can execute a compressed array once for every row.
            // Read native view lengths after one provider decode instead. The
            // optimized Top-K path shares its already decoded columns directly.
            let values = field
                .execute::<VarBinViewArray>(context)
                .map_err(vortex_error)?;
            let valid = values
                .varbinview_validity()
                .execute_mask(values.len(), context)
                .map_err(vortex_error)?;
            bytes = add_utf8_scratch_bytes(bytes, &values, &valid)?;
        }
    }
    Ok(bytes)
}

pub(super) fn scalar_scratch_bytes(rows: usize, columns: usize) -> u64 {
    (rows as u64)
        .saturating_mul(columns as u64)
        .saturating_mul(256)
        .saturating_add(64 * 1024)
}

pub(super) fn add_utf8_scratch_bytes(
    bytes: u64,
    values: &VarBinViewArray,
    valid: &Mask,
) -> Result<u64> {
    if valid.len() != values.len() {
        return Err(failed("sort scratch validity changed length"));
    }
    // Measure native descriptors, including repeated shared views. Null payload
    // bytes are unobserved and must neither be read nor force a reservation.
    values
        .views()
        .iter()
        .enumerate()
        .try_fold(bytes, |total, (row, view)| {
            if valid.value(row) {
                total
                    .checked_add(u64::from(view.len()).saturating_mul(4))
                    .ok_or_else(|| failed("sort scratch size overflow"))
            } else {
                Ok(total)
            }
        })
}

/// Account scalar conversion while the provider continues to own its arrays.
pub(super) fn reserve_scratch(
    context: Option<&NativeExecutionContext<'_>>,
    array: &ArrayRef,
    columns: &[String],
) -> Result<Option<MemoryLease>> {
    context
        .map(|context| {
            context.check_cancelled()?;
            let bytes = scratch_bytes(
                array,
                columns,
                &mut context.native_session().create_execution_ctx(),
            )?;
            context.memory().reserve(bytes)
        })
        .transpose()
}

/// Selection maps, row descriptors and transferred child leases. Payload bytes
/// are added before each selected value is cloned into the retained rows.
pub(super) fn selection_memory(
    context: Option<&NativeExecutionContext<'_>>,
    rows: usize,
    columns: usize,
) -> Result<Option<MemoryLease>> {
    context
        .map(|context| {
            context.check_cancelled()?;
            context.memory().reserve(
                (rows as u64)
                    .saturating_mul(512)
                    .saturating_add((columns as u64).saturating_mul(512))
                    .saturating_add(64 * 1024),
            )
        })
        .transpose()
}

pub(super) fn reserve_materialized_row(
    memory: &mut Option<MemoryLease>,
    columns: &[Vec<StatValue>],
    row: usize,
    copies: usize,
) -> Result<()> {
    if let Some(memory) = memory {
        let bytes = columns.iter().try_fold(256_u64, |bytes, column| {
            let value = column
                .get(row)
                .ok_or_else(|| failed("selected row is absent"))?;
            bytes
                .checked_add(match value {
                    StatValue::Utf8(value) => 128_u64.saturating_add(value.len() as u64),
                    _ => 128,
                })
                .ok_or_else(|| failed("selected payload size overflow"))
        })?;
        // Include the temporary row and retained clones; no allocation occurs
        // before its capacity is admitted alongside the scalar scratch.
        let bytes = bytes
            .checked_mul((copies as u64).saturating_add(1))
            .and_then(|bytes| memory.bytes().checked_add(bytes))
            .ok_or_else(|| failed("selected payload size overflow"))?;
        memory.resize(bytes)?;
    }
    Ok(())
}

pub(super) fn reserve_candidates(
    memory: &mut MemoryLease,
    candidates: &[SortRowCandidate],
    scratch_bytes: u64,
    arrays_read: usize,
) -> Result<()> {
    let retained = candidates.iter().fold(0_u64, |total, row| {
        row.values
            .iter()
            .fold(total.saturating_add(256), |total, value| {
                total.saturating_add(match value {
                    StatValue::Utf8(value) => 128_u64.saturating_add(value.len() as u64),
                    _ => 128,
                })
            })
    });
    // A truncated candidate vector retains capacity. Keep the high-water grant
    // until the operator retires instead of shrinking it to the current length.
    let bytes = retained
        .saturating_add(scratch_bytes)
        .saturating_mul(2)
        .saturating_add((arrays_read as u64 + 1).saturating_mul(8192));
    memory.resize(memory.bytes().max(bytes))
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)] // Keep each bounded payload delivery inside the same cancellation and output scope.
pub(super) fn deliver(
    file: &VortexFile,
    runtime: &impl BlockingRuntime,
    context: &crate::resident_session::NativeExecutionContext<'_>,
    request: &VortexQueryPrimitiveRequest,
    columns: &[String],
    selected: &[&SortRowCandidate],
    output: &mut CompletedRows<'_>,
) -> Result<SortRowsMaterializationResult> {
    let projection = super::ProjectionRequest::columns(
        columns
            .iter()
            .map(shardloom_core::ColumnRef::new)
            .collect::<Result<Vec<_>>>()?,
    );
    let plan = super::projection_scan_plan(file.dtype(), &projection, request.kind)?;
    let projection = plan
        .projection
        .as_ref()
        .map(|expr| super::bind_vortex_scan_expr(file, expr))
        .transpose()?;
    let mut execution = context.native_session().create_execution_ctx();
    let mut chunks_scanned = 0;
    // Selection maps and per-column array descriptors, never all output values.
    let _selection = context
        .memory()
        .reserve(512 * 256 + (columns.len() as u64) * 512 * 32 + 64 * 1024)?;
    if selected.is_empty() {
        output.push_values(columns, 0, |_, _| Err(failed("empty result was accessed")))?;
    }
    for window in selected.chunks(512) {
        context.check_cancelled()?;
        let mut ordinals = window
            .iter()
            .map(|row| row.source_ordinal as u64)
            .collect::<Vec<_>>();
        ordinals.sort_unstable();
        ordinals.dedup();
        if ordinals
            .last()
            .is_some_and(|ordinal| *ordinal >= file.row_count())
        {
            return Err(failed("selected source ordinal exceeds the source"));
        }
        let indices = StrictSortedBuffer::try_new(Buffer::from_iter(ordinals.iter().copied()))
            .map_err(vortex_error)?;
        let mut scan = file
            .scan()
            .map_err(vortex_error)?
            .with_ordered(true)
            .with_row_indices(indices)
            .with_split_by(SplitBy::RowCount(512))
            .with_concurrency(1);
        if let Some(projection) = &projection {
            scan = scan.with_projection(projection.clone());
        }
        let mut arrays = Vec::new();
        let mut ends = Vec::new();
        let mut rows = 0;
        let mut iter = scan.into_array_iter(runtime).map_err(vortex_error)?;
        loop {
            context.check_cancelled()?;
            let Some(array) = iter.next() else {
                break;
            };
            context.check_cancelled()?;
            let array = array.map_err(vortex_error)?;
            chunks_scanned += 1;
            if array.is_empty() {
                continue;
            }
            rows += array.len();
            if rows > ordinals.len() {
                return Err(failed("selected read returned extra rows"));
            }
            arrays.push(
                columns
                    .iter()
                    .map(|name| super::logical_field_from_native_array(&array, name))
                    .collect::<Result<Vec<_>>>()?,
            );
            ends.push(rows);
        }
        if rows != ordinals.len() {
            return Err(failed("selected read did not return every row"));
        }
        output.push_values(columns, window.len(), |row, column| {
            let sorted = ordinals
                .binary_search(&(window[row].source_ordinal as u64))
                .map_err(|_| failed("selected row identity disappeared"))?;
            let batch = ends.partition_point(|end| *end <= sorted);
            let start = if batch == 0 { 0 } else { ends[batch - 1] };
            scalar_value(
                &arrays[batch][column]
                    .execute_scalar(sorted - start, &mut execution)
                    .map_err(vortex_error)?,
            )
        })?;
    }
    output.finish_stream()?;
    Ok(SortRowsMaterializationResult {
        rows: Vec::new(),
        chunks_scanned,
        early_stop_applied: false,
        row_index_selection_applied: true,
        requested_row_indices: selected.len(),
        min_selected_source_ordinal: selected.iter().map(|row| row.source_ordinal).min(),
        max_selected_source_ordinal: selected.iter().map(|row| row.source_ordinal).max(),
        selected_row_materialization_used: !selected.is_empty(),
        memory: Vec::new(),
    })
}
