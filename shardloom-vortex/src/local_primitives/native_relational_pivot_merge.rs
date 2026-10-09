//! Two bounded passes preserve exact latest-state cardinality and native payloads.

use super::{Arc, BLOCK_ROWS, Held, NativeExecutionContext, Result, Row, Run, Spec, failed, load};
use crate::local_primitives::{
    native_capacity::ReservedVec,
    native_relational_batch::{Batch, Table},
    native_relational_sort,
};
use vortex::array::ArrayRef;

struct Cursor<'a, 's> {
    run: &'a Run<'s>,
    held: Option<Arc<Held>>,
    position: u64,
}

impl Cursor<'_, '_> {
    fn head(&mut self, spec: &Spec, context: &NativeExecutionContext<'_>) -> Result<Option<Row>> {
        context.check_cancelled()?;
        self.run.reader.validate()?;
        if self.position == self.run.native.rows {
            return Ok(None);
        }
        let start =
            self.position / self.run.native.block_rows as u64 * self.run.native.block_rows as u64;
        if self.held.as_ref().is_none_or(|held| held.start != start) {
            self.held = None;
            self.held = Some(load(&self.run.reader, start, spec, context)?);
        }
        Ok(Some(Row {
            held: Arc::clone(
                self.held
                    .as_ref()
                    .ok_or_else(|| failed("pivot merge head is absent"))?,
            ),
            row: usize::try_from(self.position - start)
                .map_err(|_| failed("pivot merge row offset exceeds addressable memory"))?,
        }))
    }
}

pub(super) struct Merge<'a, 's> {
    spec: &'a Spec,
    cursors: [Cursor<'a, 's>; 2],
}

impl<'a, 's> Merge<'a, 's> {
    pub(super) fn new(spec: &'a Spec, left: &'a Run<'s>, right: &'a Run<'s>) -> Self {
        Self {
            spec,
            cursors: [left, right].map(|run| Cursor {
                run,
                held: None,
                position: 0,
            }),
        }
    }

    pub(super) fn validate(&self) -> Result<()> {
        for cursor in &self.cursors {
            cursor.run.reader.validate()?;
        }
        Ok(())
    }

    fn next_row(&mut self, context: &NativeExecutionContext<'_>) -> Result<Option<Row>> {
        let left = self.cursors[0].head(self.spec, context)?;
        let right = self.cursors[1].head(self.spec, context)?;
        let (winner, duplicate) = match (&left, &right) {
            (Some(left), Some(right)) => match left.compare_row(right)? {
                std::cmp::Ordering::Less => (0, false),
                std::cmp::Ordering::Greater => (1, false),
                // Adjacent chronological lineage: the newer complete state wins.
                std::cmp::Ordering::Equal => (1, true),
            },
            (Some(_), None) => (0, false),
            (None, Some(_)) => (1, false),
            (None, None) => {
                self.validate()?;
                return Ok(None);
            }
        };
        self.cursors[winner].position += 1;
        if duplicate {
            self.cursors[0].position += 1;
        }
        Ok(if winner == 0 { left } else { right })
    }

    pub(super) fn next(
        &mut self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<ArrayRef>> {
        let mut table = Table::new(context.memory())?;
        let mut owners = ReservedVec::<(Arc<Held>, usize)>::new(context.memory())?;
        let mut rows = ReservedVec::new(context.memory())?;
        rows.reserve(BLOCK_ROWS)?;
        while rows.values.len() < BLOCK_ROWS {
            let Some(row) = self.next_row(context)? else {
                break;
            };
            let offset = if let Some((_, offset)) = owners
                .values
                .iter()
                .find(|(held, _)| Arc::ptr_eq(held, &row.held))
            {
                *offset
            } else {
                let offset = table.rows();
                owners.push((Arc::clone(&row.held), offset))?;
                table.push(Batch::new(row.array().clone(), &[], context)?)?;
                offset
            };
            rows.values.push(Some(
                offset
                    .checked_add(row.row)
                    .ok_or_else(|| failed("pivot merge selection overflow"))?,
            ));
        }
        if rows.values.is_empty() {
            return Ok(None);
        }
        let output = native_relational_sort::gather(
            &table,
            &rows.values,
            &self.spec.fields,
            self.spec.copy_policy,
            context,
        )?;
        self.validate()?;
        #[cfg(test)]
        if let Some(hook) = super::super::AFTER_MERGE_BLOCK.with(|hook| hook.borrow_mut().take()) {
            hook(output.len());
        }
        context.check_cancelled()?;
        Ok(Some(output))
    }
}

pub(super) fn count(
    spec: &Spec,
    left: &Run<'_>,
    right: &Run<'_>,
    context: &NativeExecutionContext<'_>,
) -> Result<u64> {
    let mut merge = Merge::new(spec, left, right);
    let mut rows = 0_u64;
    while merge.next_row(context)?.is_some() {
        rows = rows
            .checked_add(1)
            .ok_or_else(|| failed("pivot merged row count overflow"))?;
    }
    merge.validate()?;
    Ok(rows)
}
