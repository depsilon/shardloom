//! Physical run framing beneath the existing payload-validation windows.

use super::{NativeExecutionContext, Result, Spec, failed, run_metadata_bytes};
use crate::native_work_sizing::{Owner, fields_cost};
use shardloom_exec::{
    adaptive_work::{AdaptiveWorkSizer, WorkCost, WorkLimits, WorkShape},
    live_memory::MemoryLease,
};
use std::cell::Cell;
use vortex::array::{ArrayRef, IntoArray, arrays::ChunkedArray};

// Two input references, chunk slots and offsets, plus their bounded facades.
// Field/name/nested dtype metadata is charged separately from this base.
const SLICE_STRUCTURE_BYTES: u64 = 1024;

pub(super) struct RunWork {
    sizing: AdaptiveWorkSizer,
    cost: WorkCost,
    gather_scratch_per_row: u64,
    validation_rows: usize,
}

impl RunWork {
    pub(super) fn new(spec: &Spec, validation_rows: usize) -> Result<Self> {
        Ok(Self {
            sizing: AdaptiveWorkSizer::default(),
            cost: fields_cost(&spec.fields, 2)?,
            gather_scratch_per_row: spec.fields.iter().try_fold(0_u64, |bytes, (_, dtype)| {
                bytes
                    .checked_add(super::super::native_payload::gather_scratch_per_row(dtype)?)
                    .ok_or_else(capacity_error)
            })?,
            validation_rows,
        })
    }

    /// Gather/validation order remains fixed. Only physical write blocks change;
    /// reducing them cannot make the complete gather window cease to exist.
    pub(super) fn select(
        &self,
        spec: &Spec,
        rows: u64,
        other_workspace: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<WorkShape> {
        context.check_cancelled()?;
        let maximum = rows.min(self.validation_rows as u64);
        self.sizing.select_candidates(
            "native_spill_write",
            context.memory().snapshot(),
            WorkLimits {
                min_units: 1,
                max_units: maximum,
                max_in_flight: 1,
            },
            (1..=maximum)
                .map(|block| Ok((block, self.block_cost(spec, rows, block, other_workspace)?))),
        )
    }

    pub(super) fn block_cost(
        &self,
        spec: &Spec,
        rows: u64,
        block_rows: u64,
        other_workspace: u64,
    ) -> Result<WorkCost> {
        let gather_rows = rows.min(self.validation_rows as u64);
        let metadata_bytes = run_metadata_bytes(spec, rows, block_rows)?;
        // A physical block crossing a gather boundary retains two windows.
        // A divisor of the validation window drains that window before asking
        // for another, so only one gathered payload is additional. The cost's
        // two-copy row estimate still covers writer/queued block overlap below.
        // Keep per-row framing, newer-window scratch and all schema structures;
        // already held input/work leases remain in the pool snapshot.
        let gather_bytes_per_row = if (self.validation_rows as u64).is_multiple_of(block_rows) {
            self.cost.bytes_per_unit.div_ceil(2).saturating_add(8)
        } else {
            self.cost.bytes_per_unit
        };
        let fixed_bytes = gather_bytes_per_row
            .checked_add(self.gather_scratch_per_row)
            .and_then(|bytes| bytes.checked_add(128))
            .and_then(|bytes| bytes.checked_mul(gather_rows))
            .and_then(|bytes| bytes.checked_add(self.cost.fixed_bytes.checked_mul(4)?))
            .and_then(|bytes| bytes.checked_add(SLICE_STRUCTURE_BYTES))
            .and_then(|bytes| bytes.checked_add(other_workspace))
            .and_then(|bytes| bytes.checked_add(metadata_bytes))
            .ok_or_else(capacity_error)?;
        Ok(WorkCost {
            fixed_bytes,
            bytes_per_unit: self.cost.bytes_per_unit,
        })
    }

    pub(super) fn slice_credit(&self, context: &NativeExecutionContext<'_>) -> Result<MemoryLease> {
        context.memory().reserve(
            self.cost
                .fixed_bytes
                .checked_mul(2)
                .and_then(|bytes| bytes.checked_add(SLICE_STRUCTURE_BYTES))
                .ok_or_else(capacity_error)?,
        )
    }

    pub(super) fn complete(
        &mut self,
        shape: WorkShape,
        payload_bytes_per_row: u64,
        context: &NativeExecutionContext<'_>,
    ) {
        // Compact completed gather windows are observations. Borrowed source
        // slices and compressed file lengths are not value-size observations.
        self.cost.bytes_per_unit = self
            .cost
            .bytes_per_unit
            .max(payload_bytes_per_row.saturating_mul(2).saturating_add(16));
        self.sizing.complete(shape, None);
        context.record_adaptive_work(Owner::Spill, shape);
    }
}

/// Slice only after the original window has fully gathered/validated. The
/// sequential native flat writer drains each leaf before taking another one;
/// this iterator and its facade credit remain alive through writer finish.
pub(super) struct Blocks<'a, I> {
    input: I,
    current: Option<ArrayRef>,
    start: usize,
    block_rows: usize,
    validation_rows: usize,
    last_partial: bool,
    payload_bytes_per_row: &'a Cell<u64>,
    maximum_block_bytes: &'a Cell<u64>,
    context: &'a NativeExecutionContext<'a>,
    finished: bool,
    _slice_credit: MemoryLease,
}

impl<'a, I> Blocks<'a, I> {
    pub(super) fn new(
        input: I,
        block_rows: usize,
        validation_rows: usize,
        payload_bytes_per_row: &'a Cell<u64>,
        maximum_block_bytes: &'a Cell<u64>,
        slice_credit: MemoryLease,
        context: &'a NativeExecutionContext<'a>,
    ) -> Self {
        Self {
            input,
            current: None,
            start: 0,
            block_rows,
            validation_rows,
            last_partial: false,
            payload_bytes_per_row,
            maximum_block_bytes,
            context,
            finished: false,
            _slice_credit: slice_credit,
        }
    }
}

impl<I: Iterator<Item = Result<ArrayRef>>> Blocks<'_, I> {
    fn part(&mut self, count: usize) -> Result<Option<(ArrayRef, bool)>> {
        if self.finished {
            return Ok(None);
        }
        if self.current.is_none() {
            let Some(array) = self.input.next() else {
                self.finished = true;
                return Ok(None);
            };
            let array = array?;
            if self.last_partial || array.is_empty() || array.len() > self.validation_rows {
                return Err(failed("spill validation window geometry changed"));
            }
            self.last_partial = array.len() < self.validation_rows;
            self.payload_bytes_per_row.set(
                self.payload_bytes_per_row
                    .get()
                    .max(array.nbytes().div_ceil(array.len() as u64)),
            );
            self.current = Some(array);
            self.start = 0;
        }
        let array = self
            .current
            .as_ref()
            .ok_or_else(|| failed("spill window is absent"))?;
        let end = self.start.saturating_add(count).min(array.len());
        let complete_window = self.start == 0 && end == array.len();
        let result = if complete_window {
            Ok(array.clone())
        } else {
            array
                .slice(self.start..end)
                .map_err(super::super::vortex_error)
        };
        self.start = end;
        if end == array.len() {
            self.current = None;
        }
        result.map(|array| Some((array, complete_window)))
    }

    fn next_block(&mut self) -> Result<Option<ArrayRef>> {
        let Some((first, complete_window)) = self.part(self.block_rows)? else {
            return Ok(None);
        };
        let (output, compact) = if first.len() < self.block_rows {
            match self.part(self.block_rows - first.len())? {
                Some((second, _)) => {
                    let dtype = first.dtype().clone();
                    (
                        ChunkedArray::try_new(vec![first, second], dtype)
                            .map(IntoArray::into_array)
                            .map_err(super::super::vortex_error)?,
                        true,
                    )
                }
                None => (first, !complete_window),
            }
        } else {
            (first, !complete_window)
        };
        let output = if compact {
            // A sliced byte view still owns its entire gathered payload. Store
            // only this block's selected values, or small physical blocks would
            // repeat that backing buffer and amplify the next merge's reads.
            // The original gather already applied its validation policy; this
            // storage-only copy preserves unobserved values and floating bits.
            super::super::native_payload::detach_with_policy(
                &output,
                super::super::native_payload::CopyPolicy::PreserveUnobserved,
                self.context,
            )?
        } else {
            output
        };
        self.maximum_block_bytes
            .set(self.maximum_block_bytes.get().max(output.nbytes()));
        Ok(Some(output))
    }
}

/// A fixed gather window can cross several smaller physical input blocks.
/// Charge the largest joint overlap across its possible left/right selections.
/// Bound it by actual run extents and subtract only the two payloads already
/// held in the shared pool. A short run cannot supply an invented full window.
pub(super) fn merge_reader_overlap(
    rows: usize,
    left: &super::Run,
    right: &super::Run,
    held_payload_bytes: u64,
) -> Result<u64> {
    if left.native.block_rows == 0 || right.native.block_rows == 0 {
        return Err(failed("spill merge input block rows must be positive"));
    }
    let total = left
        .native
        .rows
        .checked_add(right.native.rows)
        .ok_or_else(capacity_error)?;
    let rows = rows.min(usize::try_from(total).unwrap_or(usize::MAX));
    let retained = |selected: usize, run: &super::Run| {
        // A partly consumed head plus every replacement needed by this gather.
        // Include an unselected head, but never more blocks than the file owns.
        let blocks = (selected as u64)
            .div_ceil(run.native.block_rows as u64)
            .saturating_add(1)
            .min(run.native.rows.div_ceil(run.native.block_rows as u64));
        blocks
            .checked_mul(run.maximum_block_bytes)
            .ok_or_else(capacity_error)
    };
    let peak = (0..=rows).try_fold(0, |peak, selected| {
        let bytes = retained(selected, left)?
            .checked_add(retained(rows - selected, right)?)
            .ok_or_else(capacity_error)?;
        Ok::<_, shardloom_core::ShardLoomError>(peak.max(bytes))
    })?;
    Ok(peak.saturating_sub(held_payload_bytes))
}

impl<I: Iterator<Item = Result<ArrayRef>>> Iterator for Blocks<'_, I> {
    type Item = Result<ArrayRef>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_block() {
            Ok(block) => block.map(Ok),
            Err(error) => {
                self.finished = true;
                Some(Err(error))
            }
        }
    }
}

fn capacity_error() -> shardloom_core::ShardLoomError {
    failed("native spill adaptive workspace size overflow")
}
