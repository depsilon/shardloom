//! Null-equal row membership retaining only first-occurrence native payloads.

use super::{
    native_capacity::ReservedVec,
    native_relational_batch::{Batch, Gather, Table, failed, take_batch},
    native_relational_index::RowIndex,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::array::{ArrayRef, dtype::DType};

pub(super) struct RowSet<'a> {
    fields: &'a [(String, DType)],
    names: &'a [String],
    table: Table,
    index: RowIndex,
}

impl<'a> RowSet<'a> {
    pub(super) fn new(
        fields: &'a [(String, DType)],
        names: &'a [String],
        memory: &LiveMemoryPool,
    ) -> Result<Self> {
        Ok(Self {
            fields,
            names,
            table: Table::new(memory)?,
            index: RowIndex::new(memory)?,
        })
    }

    pub(super) fn contains(
        &self,
        batch: &Batch,
        row: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<bool> {
        let hash = batch
            .hash(row, true)?
            .ok_or_else(|| failed("null-equal row hash is absent"))?;
        Ok(self
            .index
            .find(hash, context.cancellation(), |old| {
                self.table.equal_batch(old, batch, row, true)
            })?
            .is_some())
    }

    /// The index addresses compact retained ordinals. During this input batch,
    /// new ordinals refer through the reserved selection until native payloads
    /// are gathered. No duplicate input payload is retained across calls.
    pub(super) fn insert_batch(
        &mut self,
        array: ArrayRef,
        other: Option<(&Self, bool)>,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: Option<&mut dyn FnMut(ArrayRef) -> Result<()>>,
    ) -> Result<u64> {
        self.ingest(array, other, context, batch_rows, consume, None)
    }

    /// Map each input row to a stable first-occurrence group ordinal, retaining
    /// only unique native key payload. No per-input duplicate chain is added.
    pub(super) fn intern_batch(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
    ) -> Result<ReservedVec<usize>> {
        let mut mapping = ReservedVec::new(context.memory())?;
        mapping.reserve(array.len())?;
        self.ingest(array, None, context, batch_rows, None, Some(&mut mapping))?;
        Ok(mapping)
    }

    pub(super) fn rows(&self) -> usize {
        self.table.rows()
    }

    pub(super) fn gather<'b>(
        &'b self,
        rows: &[Option<usize>],
        context: &NativeExecutionContext<'_>,
    ) -> Result<Gather<'b>> {
        self.table.gather(rows, false, context)
    }

    fn ingest(
        &mut self,
        array: ArrayRef,
        other: Option<(&Self, bool)>,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        mut consume: Option<&mut dyn FnMut(ArrayRef) -> Result<()>>,
        mut mapping: Option<&mut ReservedVec<usize>>,
    ) -> Result<u64> {
        if batch_rows == 0 {
            return Err(failed("set output batch rows must be positive"));
        }
        let batch = Batch::new(array, self.names, context)?;
        let mut selected = ReservedVec::new(context.memory())?;
        selected.reserve(batch.array.len())?;
        let prior_rows = self.table.rows();
        for row in 0..batch.array.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if let Some((other, want_match)) = other
                && other.contains(&batch, row, context)? != want_match
            {
                continue;
            }
            let hash = batch
                .hash(row, true)?
                .ok_or_else(|| failed("null-equal row hash is absent"))?;
            let equal = |old| {
                if old < prior_rows {
                    self.table.equal_batch(old, &batch, row, true)
                } else {
                    let selected_row = selected
                        .values
                        .get(old - prior_rows)
                        .ok_or_else(|| failed("pending set ordinal is absent"))?;
                    batch.equal(*selected_row, &batch, row, true)
                }
            };
            let group = if let Some(group) = self.index.find(hash, context.cancellation(), equal)? {
                group
            } else {
                let ordinal = prior_rows
                    .checked_add(selected.values.len())
                    .ok_or_else(|| failed("set cardinality overflow"))?;
                let group = self
                    .index
                    .insert(hash, ordinal, context.cancellation(), equal)?;
                if group != ordinal {
                    return Err(failed(
                        "interned group ordinal differs from insertion order",
                    ));
                }
                selected.values.push(row);
                group
            };
            if let Some(mapping) = &mut mapping {
                mapping.values.push(group);
            }
        }
        for rows in selected.values.chunks(batch_rows) {
            context.check_cancelled()?;
            let array = take_batch(&batch.array, self.fields, rows, context)?;
            self.table
                .push(Batch::new(array.clone(), self.names, context)?)?;
            if let Some(consume) = consume.as_mut() {
                consume(array)?;
            }
        }
        if self.table.rows() != self.index.unique_rows() {
            return Err(failed("set index and retained native payload disagree"));
        }
        context.check_cancelled()?;
        Ok(selected.values.len() as u64)
    }
}
