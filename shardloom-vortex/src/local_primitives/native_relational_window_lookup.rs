//! Bounded held-block access and compact gathering from native window stores.

use super::{NativeExecutionContext, Result, failed};
use crate::local_primitives::{
    logical_field_from_native_array,
    native_capacity::ReservedVec,
    native_payload,
    native_relational_batch::{Batch, index_array},
    native_relational_keys::Cell,
    native_relational_spill::{StoredBlock, StoredOrder},
    native_relational_window_frame::Input,
    vortex_error,
};
use shardloom_exec::live_memory::MemoryLease;
use std::{cmp::Ordering, ops::Range};
use vortex::array::{ArrayRef, IntoArray as _, arrays::ChunkedArray, dtype::DType};

struct Held {
    batch: Batch,
    _block: StoredBlock,
    start: usize,
}

/// N is a fixed lookup-block count, not a separate memory grant. Every block,
/// prepared key and descriptor overlaps under the calling query's live pool.
pub(super) struct Lookup<'a, 's, const N: usize = 2> {
    source: &'a StoredOrder<'s>,
    names: &'a [String],
    held: [Option<Held>; N],
    next: usize,
    pub(super) blocks: u64,
    _metadata: MemoryLease,
}

impl<'a, 's, const N: usize> Lookup<'a, 's, N> {
    pub(super) fn new(
        source: &'a StoredOrder<'s>,
        names: &'a [String],
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        if N == 0 {
            return Err(failed("stored window lookup requires a held-block slot"));
        }
        let metadata = context.memory().reserve(
            u64::try_from(std::mem::size_of::<Self>())
                .map_err(|_| failed("window lookup metadata overflow"))?,
        )?;
        Ok(Self {
            source,
            names,
            held: std::array::from_fn(|_| None),
            next: 0,
            blocks: 0,
            _metadata: metadata,
        })
    }

    pub(super) fn rows(&self) -> Result<usize> {
        usize::try_from(self.source.rows()).map_err(|_| failed("window row count overflow"))
    }

    fn load(
        &mut self,
        row: usize,
        keep: Option<usize>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<usize> {
        context.check_cancelled()?;
        #[cfg(test)]
        if self.held.iter().any(|held| {
            held.as_ref()
                .is_some_and(|held| row >= held.start && row - held.start < held.batch.array.len())
        }) && let Some(path) = self.source.run_path()
        {
            let hook = super::BEFORE_CACHE_HIT.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                hook(path);
            }
        }
        self.source.validate()?;
        if row >= self.rows()? {
            return Err(failed("stored window lookup exceeds its row count"));
        }
        let start = row / self.source.block_rows() * self.source.block_rows();
        if let Some(slot) = self
            .held
            .iter()
            .position(|held| held.as_ref().is_some_and(|held| held.start == start))
        {
            return Ok(slot);
        }
        let slot = (0..N)
            .map(|offset| (self.next + offset) % N)
            .find(|slot| Some(*slot) != keep)
            .ok_or_else(|| failed("window key comparison requires two held-block slots"))?;
        // Release all old native views and key owners before the new read.
        self.held[slot] = None;
        let block = self
            .source
            .read_block_at(start as u64, context)?
            .ok_or_else(|| failed("stored window block is absent"))?;
        let batch = Batch::new(block.array().clone(), self.names, context)?;
        self.held[slot] = Some(Held {
            batch,
            _block: block,
            start,
        });
        self.next = (slot + 1) % N;
        self.blocks = self
            .blocks
            .checked_add(1)
            .ok_or_else(|| failed("window lookup count overflow"))?;
        Ok(slot)
    }

    fn held(&self, slot: usize) -> Result<&Held> {
        self.held[slot]
            .as_ref()
            .ok_or_else(|| failed("stored window held block is absent"))
    }

    pub(super) fn raw(
        &mut self,
        row: usize,
        key: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Cell> {
        let slot = self.load(row, None, context)?;
        let held = self.held(slot)?;
        held.batch.raw_cell(row - held.start, key)
    }

    pub(super) fn unsigned(
        &mut self,
        row: usize,
        key: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<usize> {
        let Cell::NonnegativeInteger(value) = self.raw(row, key, context)? else {
            return Err(failed(
                "stored window ordinal has the wrong dtype or validity",
            ));
        };
        usize::try_from(value).map_err(|_| failed("stored window ordinal exceeds addressable rows"))
    }

    pub(super) fn nullable_unsigned(
        &mut self,
        row: usize,
        key: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<usize>> {
        match self.raw(row, key, context)? {
            Cell::Null => Ok(None),
            Cell::NonnegativeInteger(value) => usize::try_from(value)
                .map(Some)
                .map_err(|_| failed("stored window ordinal exceeds addressable rows")),
            _ => Err(failed("stored window ordinal has the wrong dtype")),
        }
    }

    pub(super) fn compare(
        &mut self,
        left: usize,
        right: usize,
        key: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Ordering> {
        let left_slot = self.load(left, None, context)?;
        let right_slot = self.load(right, Some(left_slot), context)?;
        let left_held = self.held(left_slot)?;
        let right_held = self.held(right_slot)?;
        left_held.batch.compare_key(
            left - left_held.start,
            &right_held.batch,
            right - right_held.start,
            key,
        )
    }

    pub(super) fn is_null(
        &mut self,
        row: usize,
        key: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<bool> {
        let slot = self.load(row, None, context)?;
        let held = self.held(slot)?;
        held.batch.key_is_null(row - held.start, key)
    }

    pub(super) fn hash(
        &mut self,
        row: usize,
        key: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<u64>> {
        let slot = self.load(row, None, context)?;
        let held = self.held(slot)?;
        held.batch.hash_key(row - held.start, key)
    }

    /// Compact each touched block before loading another. Sorting this bounded
    /// selection only groups I/O; the final take restores requested row order.
    pub(super) fn gather(
        &mut self,
        rows: &[Option<usize>],
        parent: Option<&str>,
        name: &str,
        dtype: &DType,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let mut ordered = ReservedVec::new(context.memory())?;
        ordered.reserve(rows.len())?;
        ordered
            .values
            .extend(rows.iter().copied().zip(0..rows.len()));
        ordered.values.sort_unstable();
        let mut restore = ReservedVec::new(context.memory())?;
        restore.reserve(rows.len())?;
        restore.values.resize(rows.len(), 0);
        for (position, &(_, original)) in ordered.values.iter().enumerate() {
            restore.values[original] = position;
        }
        let mut chunks = ReservedVec::new(context.memory())?;
        chunks.reserve(rows.len())?;
        let mut begin = 0;
        while begin < ordered.values.len() {
            context.check_cancelled()?;
            let mut end = begin + 1;
            if let Some(first) = ordered.values[begin].0 {
                let slot = self.load(first, None, context)?;
                let held = self.held(slot)?;
                let limit = held.start + held.batch.array.len();
                while end < ordered.values.len()
                    && ordered.values[end].0.is_some_and(|row| row < limit)
                {
                    end += 1;
                }
                let indices = index_array(end - begin, false, context, |row| {
                    ordered.values[begin + row]
                        .0
                        .map(|row| {
                            row.checked_sub(held.start)
                                .ok_or_else(|| failed("stored window gather block moved backwards"))
                        })
                        .transpose()
                })?;
                let source = if let Some(parent) = parent {
                    logical_field_from_native_array(&held.batch.array, parent)?
                } else {
                    held.batch.array.clone()
                };
                let column = logical_field_from_native_array(&source, name)?;
                // These pieces are private movement. Validate only after the
                // final take restores the caller's selection and field order.
                chunks.values.push(native_payload::take_with_policy(
                    &column,
                    &indices,
                    dtype,
                    native_payload::CopyPolicy::PreserveUnobserved,
                    context,
                )?);
            } else {
                while end < ordered.values.len() && ordered.values[end].0.is_none() {
                    end += 1;
                }
                if !dtype.is_nullable() {
                    return Err(failed("missing window value requires a nullable output"));
                }
                chunks
                    .values
                    .push(native_payload::defaults(dtype, end - begin, context)?);
            }
            begin = end;
        }
        if chunks.values.is_empty() {
            return native_payload::defaults(dtype, 0, context);
        }
        let (chunks, ownership) = chunks.into_parts();
        let chunked = ChunkedArray::try_new(chunks, dtype.clone())
            .map_err(vortex_error)?
            .into_array();
        let indices = index_array(rows.len(), false, context, |row| {
            Ok(Some(restore.values[row]))
        })?;
        let output = native_payload::take(&chunked, &indices, dtype, context)?;
        drop((chunked, ownership));
        Ok(output)
    }
}

pub(super) struct Partition<'a, 's> {
    pub(super) lookup: Lookup<'a, 's>,
    pub(super) range: Range<usize>,
}

impl Partition<'_, '_> {
    fn row(&self, position: usize) -> Result<usize> {
        if position >= self.range.len() {
            return Err(failed("window position exceeds its stored partition"));
        }
        self.range
            .start
            .checked_add(position)
            .ok_or_else(|| failed("stored window partition position overflow"))
    }
}

impl Input for Partition<'_, '_> {
    fn key_is_null(
        &mut self,
        position: usize,
        key: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<bool> {
        self.lookup.is_null(self.row(position)?, key, context)
    }

    fn raw_cell(
        &mut self,
        position: usize,
        key: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Cell> {
        self.lookup.raw(self.row(position)?, key, context)
    }

    fn hash_key(
        &mut self,
        position: usize,
        key: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<u64>> {
        self.lookup.hash(self.row(position)?, key, context)
    }

    fn compare_key(
        &mut self,
        left: usize,
        right: usize,
        key: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Ordering> {
        self.lookup
            .compare(self.row(left)?, self.row(right)?, key, context)
    }
}
