//! Exact spilled right-match tracking and original-order outer completion.

use super::{
    Batch, Kind, NativeExecutionContext, Pairs, ReservedVec, Result, Spec, Table, failed,
    records::{DATA, Layout},
};
use crate::local_primitives::{
    logical_field_from_native_array,
    native_relational_batch::take_batch,
    native_relational_expression::keys,
    native_relational_keys::{Cell, KeyColumn},
    native_relational_records::{self as records, ORDINAL},
    native_relational_spill::{Ordering, State, StoredBlock, StoredOrder},
};
use vortex::array::ArrayRef;

pub(super) struct Matches<'a> {
    order: Ordering<'a>,
    layout: &'a Layout,
    rows: ReservedVec<u64>,
    limit: usize,
    pub(super) records: u64,
}

impl<'a> Matches<'a> {
    pub(super) fn new(
        spec: &Spec,
        layout: &'a Layout,
        spill: &'a State,
        batch_rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<Self>> {
        if !matches!(spec.kind, Kind::Right | Kind::Full) {
            return Ok(None);
        }
        let mut rows = ReservedVec::new(context.memory())?;
        rows.reserve(batch_rows)?;
        Ok(Some(Self {
            order: Ordering::new(&layout.matches, spill, batch_rows, context)?,
            layout,
            rows,
            limit: batch_rows,
            records: 0,
        }))
    }

    pub(super) fn push(&mut self, row: u64, context: &NativeExecutionContext<'_>) -> Result<()> {
        self.records = self
            .records
            .checked_add(1)
            .ok_or_else(|| failed("join match count overflow"))?;
        self.rows.values.push(row);
        if self.rows.values.len() == self.limit {
            self.flush(context)?;
        }
        Ok(())
    }

    fn flush(&mut self, context: &NativeExecutionContext<'_>) -> Result<()> {
        if self.rows.values.is_empty() {
            return Ok(());
        }
        let mut columns = ReservedVec::new(context.memory())?;
        columns.push(records::unsigned(self.rows.values.len(), context, |row| {
            Ok(self.rows.values[row])
        })?)?;
        self.order.build(
            records::structure(&self.layout.matches.fields, columns, self.rows.values.len())?,
            context,
        )?;
        self.rows.values.clear();
        Ok(())
    }

    pub(super) fn complete(
        mut self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<StoredOrder<'a>> {
        self.flush(context)?;
        self.order.retain(context)
    }
}

#[derive(Default)]
struct MatchCursor {
    column: Option<KeyColumn>,
    block: Option<StoredBlock>,
    next_block: u64,
    row: usize,
}

impl MatchCursor {
    fn next(
        &mut self,
        source: &StoredOrder<'_>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<u64>> {
        if self
            .block
            .as_ref()
            .is_none_or(|block| self.row == block.array().len())
        {
            self.column = None;
            self.block = None;
            self.row = 0;
            let Some(block) = source.read_block_at(self.next_block, context)? else {
                return Ok(None);
            };
            self.next_block = self
                .next_block
                .checked_add(block.array().len() as u64)
                .ok_or_else(|| failed("join match cursor overflow"))?;
            self.column = Some(keys(
                &logical_field_from_native_array(block.array(), ORDINAL)?,
                context,
            )?);
            self.block = Some(block);
        }
        let value = self
            .column
            .as_ref()
            .ok_or_else(|| failed("join match column is absent"))?
            .cell(self.row)?;
        self.row += 1;
        match value {
            Cell::NonnegativeInteger(value) => Ok(Some(value)),
            _ => Err(failed("join match ordinal has the wrong dtype")),
        }
    }
}

#[allow(clippy::too_many_arguments)] // Shares the same query owners and join output contract.
pub(super) fn finish(
    matches: Matches<'_>,
    right: StoredOrder<'_>,
    spec: &Spec,
    layout: &Layout,
    spill: &State,
    context: &NativeExecutionContext<'_>,
    batch_rows: usize,
    written: &mut u64,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    let matches = matches.complete(context)?;
    let mut cursor = MatchCursor::default();
    let mut matched = cursor.next(&matches, context)?;
    let mut unmatched = Ordering::new(&layout.unmatched, spill, batch_rows, context)?;
    let mut selected = ReservedVec::new(context.memory())?;
    selected.reserve(right.block_rows())?;
    let mut start = 0;
    while let Some(block) = right.read_block_at(start, context)? {
        selected.values.clear();
        for local in 0..block.array().len() {
            context.check_cancelled()?;
            let position = start + local as u64;
            while matched.is_some_and(|value| value < position) {
                matched = cursor.next(&matches, context)?;
            }
            if matched != Some(position) {
                selected.values.push(local);
            }
        }
        if !selected.values.is_empty() {
            unmatched.build(
                take_batch(
                    block.array(),
                    &layout.build.fields,
                    &selected.values,
                    context,
                )?,
                context,
            )?;
        }
        start += block.array().len() as u64;
    }
    while let Some(position) = matched {
        if position >= right.rows() {
            return Err(failed("join match ordinal exceeds completed build"));
        }
        matched = cursor.next(&matches, context)?;
    }
    drop(cursor);
    matches.finish(context)?;
    right.finish(context)?;
    drop(selected);
    let mut pairs = Pairs::new(batch_rows, context.memory())?;
    unmatched.finish(context, batch_rows, &mut |array| {
        let mut table = Table::new(context.memory())?;
        table.push(Batch::new(
            logical_field_from_native_array(&array, DATA)?,
            &[],
            context,
        )?)?;
        for row in 0..array.len() {
            pairs.left.values.push(None);
            pairs.right.values.push(Some(row));
        }
        spec.flush(&table, None, &mut pairs, written, context, consume)
    })
}
