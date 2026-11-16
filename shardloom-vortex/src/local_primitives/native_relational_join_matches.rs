//! Exact unique matches from the existing completed join index. The consumer
//! owns validation barriers; no joined payload arrays are produced here.

use super::{Batch, Join, Kind, NativeExecutionContext, ReservedVec, Result, Table, failed};

type MatchConsumer<'a> = dyn FnMut(&[usize], &[usize]) -> Result<()> + 'a;

impl Join<'_> {
    pub(in crate::local_primitives) fn unique_inner(&self) -> bool {
        self.build_finished
            && self.spec.kind == Kind::Inner
            && self.spec.asof.is_none()
            && self.spec.condition.is_none()
            && !self.spec.left_keys.is_empty()
            && self.membership.is_none()
            && !self.index.has_duplicates()
    }

    pub(in crate::local_primitives) fn right_table(&self) -> &Table {
        &self.right
    }

    pub(in crate::local_primitives) fn match_unique(
        &mut self,
        left: &Batch,
        selected: Option<&[usize]>,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut MatchConsumer<'_>,
    ) -> Result<()> {
        if !self.unique_inner() || batch_rows == 0 {
            return Err(failed("unique join match contract is absent"));
        }
        let mut parents = ReservedVec::new(context.memory())?;
        let mut right = ReservedVec::new(context.memory())?;
        parents.reserve(batch_rows)?;
        right.reserve(batch_rows)?;
        // Match the ordinary admission order: mandatory keys/coordinates first.
        self.integer
            .prepare_once(self.spec, &self.right, &self.index, left, context)?;
        let integer = self.integer.probe(left)?;
        for position in 0..selected.map_or(left.array.len(), <[usize]>::len) {
            if position.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            let row = selected.map_or(position, |selected| selected[position]);
            let entry = if let Some(integer) = &integer {
                integer.entry(row)
            } else {
                left.hash(row, false)?
                    .map(|hash| {
                        self.index.find(hash, context.cancellation(), |right| {
                            self.right.equal_batch(right, left, row, false)
                        })
                    })
                    .transpose()?
                    .flatten()
            };
            if let Some(entry) = entry {
                let ordinal = self
                    .index
                    .rows(entry)?
                    .next()
                    .ok_or_else(|| failed("unique join entry has no row"))?;
                parents.values.push(position);
                right.values.push(ordinal);
                if parents.values.len() == batch_rows {
                    consume(&parents.values, &right.values)?;
                    parents.values.clear();
                    right.values.clear();
                }
            }
        }
        if !parents.values.is_empty() {
            consume(&parents.values, &right.values)?;
        }
        context.check_cancelled()
    }
}
