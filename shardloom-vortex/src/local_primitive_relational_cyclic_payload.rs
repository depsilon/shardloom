//! Preserve an inner join's selected payload checks without its duplicate product.

use super::{NativeCyclicExistenceWork, NativeExecutionContext, ReservedVec, Result, RowIndex};
use super::{Side, Spec, Table, add_work};
use std::ops::Range;

pub(super) struct Validation<'a> {
    spec: &'a Spec,
    middle_keys: [&'a str; 2],
    last_keys: [&'a str; 2],
    index: RowIndex,
    checked_right: ReservedVec<bool>,
    batch_rows: usize,
}

impl<'a> Validation<'a> {
    pub(super) fn new(
        spec: &'a Spec,
        middle_keys: [&'a str; 2],
        last_keys: [&'a str; 2],
        last: &Table,
        batch_rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<Self>> {
        // Both integer key columns have already passed their native key owner.
        // Other declared payloads still owe the ordinary inner match domain.
        if spec.columns.iter().all(|(side, name)| {
            match side {
                Side::Left => middle_keys,
                Side::Right => last_keys,
            }
            .contains(&name.as_str())
        }) {
            return Ok(None);
        }
        let mut index = RowIndex::new(context.memory())?;
        for row in 0..last.rows() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if let Some(hash) = last.hash_key(row, 1)? {
                index.insert(hash, row, context.cancellation(), |other| {
                    let (left, left_row) = last.key(row, 1)?;
                    let (right, right_row) = last.key(other, 1)?;
                    left.equals_at(left_row, right, right_row, false)
                })?;
            }
        }
        let mut checked_right = ReservedVec::new(context.memory())?;
        checked_right.reserve(index.unique_rows())?;
        checked_right.values.resize(index.unique_rows(), false);
        Ok(Some(Self {
            spec,
            middle_keys,
            last_keys,
            index,
            checked_right,
            batch_rows,
        }))
    }

    pub(super) fn consume(
        &mut self,
        middle: &Table,
        last: &Table,
        rows: Range<usize>,
        context: &NativeExecutionContext<'_>,
        work: &mut NativeCyclicExistenceWork,
    ) -> Result<()> {
        let mut left_rows = ReservedVec::new(context.memory())?;
        left_rows.reserve(self.batch_rows.min(rows.len()))?;
        let mut right_rows = ReservedVec::new(context.memory())?;
        right_rows.reserve(self.batch_rows.min(last.rows()))?;
        for row in rows {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            let Some(hash) = middle.hash_key(row, 1)? else {
                continue;
            };
            let entry = self.index.find(hash, context.cancellation(), |candidate| {
                let (left, left_row) = middle.key(row, 1)?;
                let (right, right_row) = last.key(candidate, 1)?;
                left.equals_at(left_row, right, right_row, false)
            })?;
            let Some(entry) = entry else {
                continue;
            };
            left_rows.values.push(Some(row));
            if left_rows.values.len() == self.batch_rows {
                self.flush(middle, Side::Left, &mut left_rows, context, work)?;
            }
            if self.checked_right.values[entry] {
                continue;
            }
            for (visited, right) in self.index.rows(entry)?.enumerate() {
                if visited.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                right_rows.values.push(Some(right));
                if right_rows.values.len() == self.batch_rows {
                    self.flush(last, Side::Right, &mut right_rows, context, work)?;
                }
            }
            // Any pending rows stay in this bounded batch until validated below.
            // A failure never publishes this operation's work or result.
            self.checked_right.values[entry] = true;
        }
        self.flush(middle, Side::Left, &mut left_rows, context, work)?;
        self.flush(last, Side::Right, &mut right_rows, context, work)?;
        context.check_cancelled()
    }

    fn flush(
        &self,
        table: &Table,
        side: Side,
        rows: &mut ReservedVec<Option<usize>>,
        context: &NativeExecutionContext<'_>,
        work: &mut NativeCyclicExistenceWork,
    ) -> Result<()> {
        if rows.values.is_empty() {
            return Ok(());
        }
        let keys = match side {
            Side::Left => self.middle_keys,
            Side::Right => self.last_keys,
        };
        let fields =
            || {
                self.spec.columns.iter().zip(&self.spec.fields).filter(
                    |((column_side, name), _)| {
                        *column_side == side && !keys.contains(&name.as_str())
                    },
                )
            };
        if fields().next().is_some() {
            let gather = table.gather(&rows.values, false, context)?;
            for ((_, name), (_, dtype)) in fields() {
                context.check_cancelled()?;
                gather.validate_column(name, dtype, context)?;
                add_work(&mut work.payload_cells_validated, rows.values.len())?;
            }
            add_work(
                match side {
                    Side::Left => &mut work.middle_payload_rows_validated,
                    Side::Right => &mut work.last_payload_rows_validated,
                },
                rows.values.len(),
            )?;
        }
        rows.values.clear();
        Ok(())
    }
}
