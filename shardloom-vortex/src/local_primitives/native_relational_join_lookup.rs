//! Exact hash-range lookup with one held native block and compact candidates.

use super::records::{DATA, HASH};
use super::{Batch, NativeExecutionContext, ReservedVec, Result, Spec, Table, failed};
use crate::local_primitives::{
    logical_field_from_native_array, native_payload,
    native_relational_batch::index_array,
    native_relational_expression::keys,
    native_relational_keys::{Cell, KeyColumn},
    native_relational_spill::{StoredBlock, StoredOrder},
};

#[cfg(test)]
#[path = "native_relational_join_lookup_tests.rs"]
mod tests;

struct Held {
    payload: Option<Batch>,
    hash: KeyColumn,
    block: StoredBlock,
    start: u64,
}

pub(super) struct Search<'a, 's> {
    source: &'a StoredOrder<'s>,
    spec: &'a Spec,
    held: Option<Held>,
    pub(super) blocks: u64,
}

pub(super) struct Candidates {
    pub(super) table: Table,
    pub(super) positions: ReservedVec<u64>,
}

impl<'a, 's> Search<'a, 's> {
    pub(super) fn new(source: &'a StoredOrder<'s>, spec: &'a Spec) -> Self {
        Self {
            source,
            spec,
            held: None,
            blocks: 0,
        }
    }

    fn load(&mut self, row: u64, context: &NativeExecutionContext<'_>) -> Result<&mut Held> {
        context.check_cancelled()?;
        self.source.validate()?;
        if row >= self.source.rows() {
            return Err(failed("ordered join lookup exceeds build row count"));
        }
        let start = row / self.source.block_rows() as u64 * self.source.block_rows() as u64;
        if self.held.as_ref().is_none_or(|held| held.start != start) {
            // Drop all views/keys and the old block before allocating another.
            self.held = None;
            let block = self
                .source
                .read_block_at(start, context)?
                .ok_or_else(|| failed("ordered join lookup block is absent"))?;
            let hash = keys(
                &logical_field_from_native_array(block.array(), HASH)?,
                context,
            )?;
            self.blocks = self
                .blocks
                .checked_add(1)
                .ok_or_else(|| failed("join lookup count overflow"))?;
            self.held = Some(Held {
                payload: None,
                hash,
                block,
                start,
            });
        }
        self.held
            .as_mut()
            .ok_or_else(|| failed("ordered join held block is absent"))
    }

    fn bound(
        &mut self,
        hash: u64,
        upper: bool,
        context: &NativeExecutionContext<'_>,
    ) -> Result<u64> {
        let (mut low, mut high) = (0, self.source.rows());
        while low < high {
            let middle = low + (high - low) / 2;
            let held = self.load(middle, context)?;
            let local = usize::try_from(middle - held.start)
                .map_err(|_| failed("ordered join lookup block offset overflow"))?;
            let value = match held.hash.cell(local)? {
                Cell::Null => None,
                Cell::NonnegativeInteger(value) => Some(value),
                _ => return Err(failed("ordered join hash has the wrong dtype")),
            };
            // The shared private ordering uses ascending NULLS FIRST.
            if value < Some(hash) || (upper && value == Some(hash)) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(low)
    }

    pub(super) fn range(
        &mut self,
        left: &Batch,
        row: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<std::ops::Range<u64>> {
        if self.spec.left_keys.is_empty() {
            return Ok(0..self.source.rows());
        }
        let Some(hash) = left.hash(row, false)? else {
            return Ok(0..0);
        };
        Ok(self.bound(hash, false, context)?..self.bound(hash, true, context)?)
    }

    /// Fill one exact-key candidate batch across native block boundaries. ON
    /// sees the same candidate packing/order as the resident index strategy.
    pub(super) fn candidates(
        &mut self,
        left: &Batch,
        left_row: usize,
        range: &mut std::ops::Range<u64>,
        limit: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Candidates> {
        let mut output = Candidates {
            table: Table::new(context.memory())?,
            positions: ReservedVec::new(context.memory())?,
        };
        output.positions.reserve(limit)?;
        let mut selected = ReservedVec::new(context.memory())?;
        selected.reserve(limit.min(self.source.block_rows()))?;
        let spec = self.spec;
        while range.start < range.end && output.positions.values.len() < limit {
            let held = self.load(range.start, context)?;
            if held.payload.is_none() {
                held.payload = Some(Batch::new(
                    logical_field_from_native_array(held.block.array(), DATA)?,
                    &spec.right_keys,
                    context,
                )?);
            }
            let payload = held
                .payload
                .as_ref()
                .ok_or_else(|| failed("join payload keys are absent"))?;
            let end = range.end.min(held.start + payload.array.len() as u64);
            selected.values.clear();
            while range.start < end && output.positions.values.len() < limit {
                let local = usize::try_from(range.start - held.start)
                    .map_err(|_| failed("ordered join candidate block offset overflow"))?;
                if spec.left_keys.is_empty() || payload.equal(local, left, left_row, false)? {
                    selected.values.push(local);
                    output.positions.values.push(range.start);
                }
                range.start += 1;
            }
            if !selected.values.is_empty() {
                let indices = index_array(selected.values.len(), false, context, |row| {
                    Ok(Some(selected.values[row]))
                })?;
                let compact = native_payload::take_record(
                    &payload.array,
                    &indices,
                    payload.array.dtype(),
                    context,
                )?;
                output.table.push(Batch::new(compact, &[], context)?)?;
            }
        }
        context.check_cancelled()?;
        Ok(output)
    }
}
