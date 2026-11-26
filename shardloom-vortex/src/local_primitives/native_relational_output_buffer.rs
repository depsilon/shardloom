//! Bounded native record compaction shared by expanding and stored operators.

use super::{
    native_capacity::ReservedVec,
    native_payload,
    native_relational_batch::{failed, index_array},
    vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use shardloom_exec::adaptive_work::{AdaptiveWorkSizer, WorkCost, WorkDecision, WorkLimits};
use vortex::array::{ArrayRef, IntoArray as _, arrays::ChunkedArray, dtype::DType};

#[cfg(test)]
#[path = "native_relational_output_buffer_tests.rs"]
mod tests;

// Capacity estimates govern batching only. Every actual owner still reserves
// against the query grant; one oversized row is admitted or denied explicitly.
const PENDING_BYTES: u64 = 256 * 1024;
const PENDING_CHUNKS: usize = 128;

pub(super) struct OutputBuffer {
    dtype: DType,
    pending: ReservedVec<ArrayRef>,
    pending_bytes: u64,
    pending_payload_bytes: u64,
    pending_rows: usize,
    batch_rows: usize,
    chunk_metadata_bytes: u64,
    pending_metadata_bytes: u64,
    gather_scratch_per_row: u64,
    copy_policy: native_payload::CopyPolicy,
    sizing: AdaptiveWorkSizer,
    copy_counts: Option<(u64, u64)>,
}

impl OutputBuffer {
    pub(super) fn new(
        dtype: DType,
        batch_rows: usize,
        copy_policy: native_payload::CopyPolicy,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        if batch_rows == 0 {
            return Err(failed(
                "native output compaction requires positive batch capacity",
            ));
        }
        let chunk_metadata_bytes = crate::native_payload_schema::schema_bytes(&dtype, context)?
            .checked_mul(2)
            .ok_or_else(|| failed("completed record metadata capacity overflow"))?;
        let gather_scratch_per_row =
            native_payload::gather_scratch_per_row(&dtype, context.memory())?;
        Ok(Self {
            dtype,
            pending: ReservedVec::new(context.memory())?,
            pending_bytes: 0,
            pending_payload_bytes: 0,
            pending_rows: 0,
            batch_rows,
            chunk_metadata_bytes,
            pending_metadata_bytes: chunk_metadata_bytes,
            gather_scratch_per_row,
            copy_policy,
            sizing: AdaptiveWorkSizer::default(),
            copy_counts: None,
        })
    }

    #[cfg(all(feature = "vortex-write", unix))]
    pub(super) fn native_writer(
        dtype: DType,
        batch_rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let mut output = Self::new(
            dtype,
            batch_rows,
            native_payload::CopyPolicy::PreserveUnobserved,
            context,
        )?;
        // Preserve the sink's established one-schema-per-fragment byte ceiling.
        // Construction still admits the full shared structure/gather overlap;
        // this batching estimate grants no credit for replacement storage.
        output.pending_metadata_bytes = output.chunk_metadata_bytes / 2;
        output.copy_counts = Some((0, 0));
        Ok(output)
    }

    #[cfg(all(feature = "vortex-write", unix))]
    pub(super) fn copy_counts(&self) -> (u64, u64) {
        self.copy_counts.unwrap_or_default()
    }

    pub(super) fn push(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        context.check_cancelled()?;
        if array.dtype() != &self.dtype {
            return Err(failed("completed native record changed its dtype"));
        }
        if array.is_empty() {
            return Ok(());
        }
        let bytes = array
            .nbytes()
            .checked_add(self.pending_metadata_bytes)
            .ok_or_else(|| failed("completed record capacity overflow"))?;
        if bytes > PENDING_BYTES.saturating_sub(self.pending_bytes)
            || self.pending.values.len() >= PENDING_CHUNKS
            || array.len() > self.batch_rows.saturating_sub(self.pending_rows)
        {
            self.flush(context, consume)?;
        }
        // Already bounded ordinary batches remain independently owned and do
        // not need another payload copy just to pass through this buffer.
        if bytes > PENDING_BYTES || array.len() >= self.batch_rows {
            consume(array)?;
            return context.check_cancelled();
        }
        if !self.pending.values.is_empty() {
            let rows = self
                .pending_rows
                .checked_add(array.len())
                .ok_or_else(capacity_error)?;
            let payload = self
                .pending_payload_bytes
                .checked_add(array.nbytes())
                .ok_or_else(capacity_error)?;
            let cost = self.cost(rows, payload, self.pending.values.len() + 1, true)?;
            match self.decide_compaction(context, cost, limits(self.batch_rows))? {
                WorkDecision::Selected(shape) if rows as u64 > shape.units() => {
                    self.flush(context, consume)?;
                }
                WorkDecision::Declined(denial) => {
                    // Compaction is optional. Existing independently owned
                    // records can flow in order without allocating replacements.
                    context.record_adaptive_work_decline(
                        crate::native_work_sizing::Owner::Output,
                        denial,
                    );
                    let pending = self.take_pending(context)?;
                    forward(pending, context, consume)?;
                    consume(array)?;
                    return context.check_cancelled();
                }
                WorkDecision::Selected(_) => {}
            }
        }
        self.pending.reserve_one()?;
        self.pending_rows += array.len();
        self.pending_payload_bytes = self
            .pending_payload_bytes
            .checked_add(array.nbytes())
            .ok_or_else(capacity_error)?;
        self.pending.values.push(array);
        self.pending_bytes += bytes;
        Ok(())
    }

    pub(super) fn flush(
        &mut self,
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        context.check_cancelled()?;
        if self.pending.values.is_empty() {
            return Ok(());
        }
        let rows = self.pending_rows;
        let payload = self.pending_payload_bytes;
        let mut pending = self.take_pending(context)?;
        if pending.values.len() == 1 {
            let array = pending
                .values
                .pop()
                .ok_or_else(|| failed("completed output is absent"))?;
            drop(pending);
            consume(array)?;
        } else {
            let initial_cost = self.cost(rows, payload, pending.values.len(), true)?;
            let mut shape = match self.decide_compaction(context, initial_cost, limits(rows))? {
                WorkDecision::Selected(shape) => shape,
                WorkDecision::Declined(denial) => {
                    context.record_adaptive_work_decline(
                        crate::native_work_sizing::Owner::Output,
                        denial,
                    );
                    return forward(pending, context, consume);
                }
            };
            // Keep pending records available in order if a downstream consumer
            // retains earlier compacted batches and leaves no compaction headroom.
            let structure_credit = context
                .memory()
                .reserve(self.structure_bytes(pending.values.len())?)?;
            let mut slots = Vec::new();
            slots
                .try_reserve_exact(pending.values.len())
                .map_err(vortex_error)?;
            slots.extend(pending.values.iter().cloned());
            let chunked = ChunkedArray::try_new(slots, self.dtype.clone())
                .map_err(vortex_error)?
                .into_array();
            let cost = self.cost(rows, payload, 0, false)?;
            let mut start = 0;
            while start < rows {
                context.check_cancelled()?;
                let count = usize::try_from(shape.units()).map_err(|_| capacity_error())?;
                let indices = index_array(count, false, context, |row| Ok(Some(start + row)))?;
                let compact = if let Some((bytes, scalars)) = &mut self.copy_counts {
                    let (compact, count) = native_payload::take_record_with_scalar_count(
                        &chunked,
                        &indices,
                        &self.dtype,
                        self.copy_policy,
                        context,
                    )?;
                    *bytes = bytes
                        .checked_add(compact.nbytes())
                        .ok_or_else(capacity_error)?;
                    *scalars = scalars.checked_add(count).ok_or_else(capacity_error)?;
                    compact
                } else {
                    native_payload::take_record_with_policy(
                        &chunked,
                        &indices,
                        &self.dtype,
                        self.copy_policy,
                        context,
                    )?
                };
                let observed = compact
                    .nbytes()
                    .div_ceil(count as u64)
                    .saturating_mul(2)
                    .saturating_add(self.gather_scratch_per_row)
                    .saturating_add(64);
                drop(indices);
                self.sizing.complete(shape, Some(observed));
                context.record_adaptive_work(crate::native_work_sizing::Owner::Output, shape);
                consume(compact)?;
                start += count;
                if start < rows {
                    match self.decide_compaction(context, cost, limits(rows - start))? {
                        WorkDecision::Selected(next) => shape = next,
                        WorkDecision::Declined(denial) => {
                            context.record_adaptive_work_decline(
                                crate::native_work_sizing::Owner::Output,
                                denial,
                            );
                            drop((chunked, structure_credit));
                            return forward_from(pending, start, context, consume);
                        }
                    }
                }
            }
            drop((chunked, pending, structure_credit));
        }
        context.check_cancelled()
    }

    fn decide_compaction(
        &self,
        context: &NativeExecutionContext<'_>,
        cost: WorkCost,
        limits: WorkLimits,
    ) -> Result<WorkDecision> {
        let decision = self.sizing.decide(
            "native_output_compaction",
            context.memory().snapshot(),
            cost,
            limits,
        )?;
        if matches!(decision, WorkDecision::Declined(_)) {
            context.memory().reclaim_optional_memory();
            return self.sizing.decide(
                "native_output_compaction",
                context.memory().snapshot(),
                cost,
                limits,
            );
        }
        Ok(decision)
    }

    fn take_pending(
        &mut self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ReservedVec<ArrayRef>> {
        let pending = std::mem::replace(&mut self.pending, ReservedVec::new(context.memory())?);
        self.pending_rows = 0;
        self.pending_bytes = 0;
        self.pending_payload_bytes = 0;
        Ok(pending)
    }

    fn structure_bytes(&self, chunks: usize) -> Result<u64> {
        // Pinned ChunkedArray builds slot tables, usize offsets and a native
        // u64 offsets buffer while the input reference table remains live.
        (chunks as u64)
            .checked_add(1)
            .and_then(|count| count.checked_mul(128))
            .and_then(|bytes| bytes.checked_add(self.chunk_metadata_bytes))
            .ok_or_else(capacity_error)
    }

    fn cost(
        &self,
        rows: usize,
        payload: u64,
        chunks: usize,
        include_structure: bool,
    ) -> Result<WorkCost> {
        let extra = if include_structure {
            self.structure_bytes(chunks)?
        } else {
            0
        };
        Ok(WorkCost {
            fixed_bytes: (self.chunk_metadata_bytes / 2)
                .checked_add(extra)
                .and_then(|bytes| bytes.checked_add(4096))
                .ok_or_else(capacity_error)?,
            // Pending payloads are already held. Charge new output/builders and
            // simultaneous index/selection storage, using actual input widths.
            bytes_per_unit: payload
                .div_ceil(rows as u64)
                .checked_mul(2)
                .and_then(|bytes| bytes.checked_add(self.gather_scratch_per_row))
                .and_then(|bytes| bytes.checked_add(64))
                .ok_or_else(capacity_error)?,
        })
    }
}

fn limits(rows: usize) -> WorkLimits {
    WorkLimits {
        min_units: 1,
        max_units: rows as u64,
        max_in_flight: 1,
    }
}

fn forward(
    pending: ReservedVec<ArrayRef>,
    context: &NativeExecutionContext<'_>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    forward_from(pending, 0, context, consume)
}

fn forward_from(
    mut pending: ReservedVec<ArrayRef>,
    start: usize,
    context: &NativeExecutionContext<'_>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    let mut row_offset = 0usize;
    for array in pending.values.drain(..) {
        let chunk_start = row_offset;
        let len = array.len();
        row_offset = row_offset.checked_add(len).ok_or_else(capacity_error)?;
        if row_offset <= start {
            drop(array);
            continue;
        }
        context.check_cancelled()?;
        if chunk_start >= start {
            consume(array)?;
        } else {
            let tail = array
                .slice((start - chunk_start)..len)
                .map_err(vortex_error)?;
            drop(array);
            consume(tail)?;
        }
    }
    context.check_cancelled()
}

fn capacity_error() -> shardloom_core::ShardLoomError {
    failed("completed native output workspace capacity overflow")
}
