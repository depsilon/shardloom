//! Exact row membership retaining only first-occurrence native payloads.

use super::{
    native_capacity::ReservedVec,
    native_relational_batch::{Batch, Gather, Table, failed},
    native_relational_index::RowIndex,
    native_relational_rows::Rows,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::array::{ArrayRef, dtype::DType};

#[cfg(test)]
#[path = "native_relational_set_cache_tests.rs"]
mod cache_tests;

#[cfg(test)]
#[path = "native_relational_set_inline_cache_tests.rs"]
mod inline_cache_tests;

pub(super) enum SharedRef<'a, T: ?Sized + 'static> {
    Borrowed(&'a T),
    Shared(std::sync::Arc<T>),
}

impl<T: ?Sized + 'static> Clone for SharedRef<'_, T> {
    fn clone(&self) -> Self {
        match self {
            Self::Borrowed(value) => Self::Borrowed(value),
            Self::Shared(arc) => Self::Shared(std::sync::Arc::clone(arc)),
        }
    }
}

impl<T: ?Sized + 'static> std::ops::Deref for SharedRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(value) => value,
            Self::Shared(arc) => arc,
        }
    }
}

pub(super) struct RowSet<'a> {
    fields: SharedRef<'a, [(String, DType)]>,
    names: SharedRef<'a, [String]>,
    nulls_equal: bool,
    table: Table,
    index: RowIndex,
}

impl<'a> RowSet<'a> {
    pub(super) fn new(
        fields: &'a [(String, DType)],
        names: &'a [String],
        memory: &LiveMemoryPool,
    ) -> Result<Self> {
        Self::with_nulls_equal(fields, names, true, memory)
    }

    pub(super) fn new_shared(
        fields: std::sync::Arc<[(String, DType)]>,
        names: std::sync::Arc<[String]>,
        memory: &LiveMemoryPool,
    ) -> Result<RowSet<'static>> {
        Ok(RowSet {
            fields: SharedRef::Shared(fields),
            names: SharedRef::Shared(names),
            nulls_equal: true,
            table: Table::new(memory)?,
            index: RowIndex::new(memory)?,
        })
    }

    pub(super) fn with_nulls_equal(
        fields: &'a [(String, DType)],
        names: &'a [String],
        nulls_equal: bool,
        memory: &LiveMemoryPool,
    ) -> Result<Self> {
        Ok(Self {
            fields: SharedRef::Borrowed(fields),
            names: SharedRef::Borrowed(names),
            nulls_equal,
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
        self.contains_rows(batch, row, context)
    }

    fn contains_rows(
        &self,
        batch: &impl Rows,
        row: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<bool> {
        let Some(hash) = batch.hash(row, self.nulls_equal)? else {
            return Ok(false);
        };
        Ok(self
            .index
            .find(hash, context.cancellation(), |old| {
                let (old_batch, old_row) = self.table.locate_batch(old)?;
                batch.equal_batch(row, old_batch, old_row, self.nulls_equal)
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
        #[cfg(test)]
        let _cost = super::native_relational_grouped_cost::scope(
            super::native_relational_grouped_cost::Phase::GroupInterning,
        );
        if !self.nulls_equal {
            return Err(failed("group ordinals require null-equal membership"));
        }
        let mut mapping = ReservedVec::new(context.memory())?;
        mapping.reserve(array.len())?;
        #[cfg(all(test, debug_assertions))]
        super::native_relational_observation::record(|counts| {
            counts.group_ordinal_rows += array.len() as u64;
            counts.group_input_batches += 1;
        });
        self.ingest(array, None, context, batch_rows, None, Some(&mut mapping))?;
        Ok(mapping)
    }

    pub(super) fn intern_rows(
        &mut self,
        rows: &impl Rows,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
    ) -> Result<ReservedVec<usize>> {
        if !self.nulls_equal {
            return Err(failed("group ordinals require null-equal membership"));
        }
        let mut mapping = ReservedVec::new(context.memory())?;
        mapping.reserve(rows.len())?;
        self.ingest_rows(rows, None, context, batch_rows, None, Some(&mut mapping))?;
        Ok(mapping)
    }

    pub(super) fn rows(&self) -> usize {
        self.table.rows()
    }

    pub(super) fn seal_join(&mut self, context: &NativeExecutionContext<'_>) -> Result<()> {
        self.index.seal_join(false, context.cancellation())
    }

    pub(super) fn join_work(&self) -> Result<shardloom_core::native_join_work::NativeJoinWork> {
        self.index.join_work()
    }

    pub(super) fn gather<'b>(
        &'b self,
        rows: &[Option<usize>],
        context: &NativeExecutionContext<'_>,
    ) -> Result<Gather<'b>> {
        self.table.gather(rows, false, context)
    }

    #[allow(clippy::too_many_lines)] // Keep key admission, exact insertion and batch publication together.
    fn ingest(
        &mut self,
        array: ArrayRef,
        other: Option<(&Self, bool)>,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: Option<&mut dyn FnMut(ArrayRef) -> Result<()>>,
        mapping: Option<&mut ReservedVec<usize>>,
    ) -> Result<u64> {
        self.index.ensure_building()?;
        if batch_rows == 0 {
            return Err(failed("set output batch rows must be positive"));
        }
        #[cfg(test)]
        let preparation_cost = mapping.as_ref().map(|_| {
            super::native_relational_grouped_cost::scope(
                super::native_relational_grouped_cost::Phase::GroupPreparation,
            )
        });
        let batch = if self.nulls_equal && other.is_none() {
            Batch::for_membership(array, &self.names, context)?
        } else {
            Batch::new(array, &self.names, context)?
        };
        #[cfg(test)]
        drop(preparation_cost);
        self.ingest_rows(&batch, other, context, batch_rows, consume, mapping)
    }

    fn ingest_rows(
        &mut self,
        batch: &impl Rows,
        other: Option<(&Self, bool)>,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        mut consume: Option<&mut dyn FnMut(ArrayRef) -> Result<()>>,
        mut mapping: Option<&mut ReservedVec<usize>>,
    ) -> Result<u64> {
        self.index.ensure_building()?;
        if batch_rows == 0 {
            return Err(failed("set output batch rows must be positive"));
        }
        let mut selected = ReservedVec::new(context.memory())?;
        selected.reserve(batch.len())?;
        let mut cache = batch.key_cache(context)?;
        let prior_rows = self.table.rows();
        for row in 0..batch.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if let Some((other, want_match)) = other
                && other.contains_rows(batch, row, context)? != want_match
            {
                continue;
            }
            let probe = cache
                .as_ref()
                .map(|cache| cache.probe(row))
                .transpose()?
                .flatten();
            if let Some(probe) = probe
                && let Some(group) = probe.group()
            {
                #[cfg(all(test, debug_assertions))]
                super::native_relational_observation::record(|counts| {
                    counts.key_cache_hits += 1;
                });
                if let Some(mapping) = &mut mapping {
                    mapping.values.push(group);
                }
                continue;
            }
            let Some(hash) = batch.hash(row, self.nulls_equal)? else {
                continue;
            };
            let equal = |old| {
                if old < prior_rows {
                    let (old_batch, old_row) = self.table.locate_batch(old)?;
                    batch.equal_batch(row, old_batch, old_row, self.nulls_equal)
                } else {
                    let selected_row = selected
                        .values
                        .get(old - prior_rows)
                        .ok_or_else(|| failed("pending set ordinal is absent"))?;
                    batch.equal(*selected_row, row, self.nulls_equal)
                }
            };
            let ordinal = prior_rows
                .checked_add(selected.values.len())
                .ok_or_else(|| failed("set cardinality overflow"))?;
            #[cfg(all(test, debug_assertions))]
            super::native_relational_observation::record(|counts| {
                counts.key_exact_lookups += 1;
            });
            let (group, inserted) =
                self.index
                    .insert_unique(hash, ordinal, context.cancellation(), equal)?;
            if inserted {
                if group != ordinal {
                    return Err(failed(
                        "interned group ordinal differs from insertion order",
                    ));
                }
                selected.values.push(row);
            }
            if let Some(mapping) = &mut mapping {
                mapping.values.push(group);
            }
            if let (Some(cache), Some(probe)) = (&mut cache, probe) {
                cache.insert(probe, group);
            }
        }
        drop(cache);
        for rows in selected.values.chunks(batch_rows) {
            context.check_cancelled()?;
            let array = batch.gather(&self.fields, rows, context)?;
            self.table
                .push(Batch::new(array.clone(), &self.names, context)?)?;
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

    pub(super) fn table(&self) -> &Table {
        &self.table
    }

    pub(super) fn index(&self) -> &RowIndex {
        &self.index
    }
}
