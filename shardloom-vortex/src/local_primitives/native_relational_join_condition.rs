//! Native ON evaluation over bounded candidate pairs before outer null extension.

use super::super::{
    native_relational_batch::Table,
    native_relational_expression::keys,
    native_relational_index::Rows,
    native_relational_keys::{Cell, KeyColumn},
};
use super::{
    ArrayRef, Batch, Condition, FieldNames, Join, NativeExecutionContext, Pairs, ReservedVec,
    Result, Side, Spec, StructArray, Validity, index_array, logical_field_from_native_array,
    take_column, vortex_error,
};
use vortex::array::IntoArray as _;

enum Candidates<'a> {
    All(std::ops::Range<usize>),
    Key(Rows<'a>),
    Empty,
}
impl Iterator for Candidates<'_> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        match self {
            Self::All(rows) => rows.next(),
            Self::Key(rows) => rows.next(),
            Self::Empty => None,
        }
    }
}

impl Join<'_> {
    pub(super) fn consume_condition(
        &mut self,
        left: &Batch,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<u64> {
        let mut pairs = Pairs::new(batch_rows, context.memory())?;
        let mut rights = ReservedVec::new(context.memory())?;
        rights.reserve(batch_rows.min(self.right.rows()))?;
        let mut written = 0u64;
        for row in 0..left.array.len() {
            context.check_cancelled()?;
            let mut candidates = if self.spec.left_keys.is_empty() {
                Candidates::All(0..self.right.rows())
            } else {
                let entry = left
                    .hash(row, false)?
                    .map(|hash| {
                        self.index.find(hash, context.cancellation(), |right| {
                            self.right.equal_batch(right, left, row, false)
                        })
                    })
                    .transpose()?
                    .flatten();
                match entry {
                    Some(entry) => Candidates::Key(self.index.rows(entry)?),
                    None => Candidates::Empty,
                }
            };
            let mut matched = false;
            loop {
                context.check_cancelled()?;
                rights.values.clear();
                rights.values.extend(candidates.by_ref().take(batch_rows));
                if rights.values.is_empty() {
                    break;
                }
                let selected = self.spec.select_candidates(
                    &left.array,
                    &self.right,
                    row,
                    &rights.values,
                    context,
                )?;
                for &right in &selected.values {
                    matched = true;
                    if self.spec.short_circuit() {
                        break;
                    }
                    if !self.matched.values.is_empty() {
                        self.matched.values[right] = true;
                    }
                    self.push_pair(
                        Some(&left.array),
                        Some(row),
                        Some(right),
                        &mut pairs,
                        &mut written,
                        context,
                        consume,
                    )?;
                }
                if matched && self.spec.short_circuit() {
                    break;
                }
            }
            if self.spec.keep_unpaired_left(matched) {
                self.push_pair(
                    Some(&left.array),
                    Some(row),
                    None,
                    &mut pairs,
                    &mut written,
                    context,
                    consume,
                )?;
            }
        }
        self.flush(
            Some(&left.array),
            &mut pairs,
            &mut written,
            context,
            consume,
        )?;
        context.check_cancelled()?;
        Ok(written)
    }
}

impl Spec {
    /// Evaluate the complete bounded candidate batch before semi/anti
    /// short-circuiting. Both resident and ordered joins use this boundary.
    pub(super) fn select_candidates(
        &self,
        left: &ArrayRef,
        right: &Table,
        row: usize,
        rows: &[usize],
        context: &NativeExecutionContext<'_>,
    ) -> Result<ReservedVec<usize>> {
        let selected = self
            .condition
            .as_ref()
            .map(|condition| condition.evaluate(left, right, row, rows, context))
            .transpose()?;
        let mut output = ReservedVec::new(context.memory())?;
        output.reserve(if self.short_circuit() {
            rows.len().min(1)
        } else {
            rows.len()
        })?;
        for (position, &right) in rows.iter().enumerate() {
            if position.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if let Some(selected) = &selected
                && selected.cell(position)? != Cell::Boolean(true)
            {
                continue;
            }
            output.values.push(right);
            if self.short_circuit() {
                break;
            }
        }
        Ok(output)
    }
}

impl Condition {
    fn evaluate(
        &self,
        left: &ArrayRef,
        right: &Table,
        row: usize,
        rows: &[usize],
        context: &NativeExecutionContext<'_>,
    ) -> Result<KeyColumn> {
        let mut ordinals = ReservedVec::new(context.memory())?;
        ordinals.reserve(rows.len())?;
        ordinals.values.extend(rows.iter().map(|row| Some(*row)));
        let gather = right.gather(&ordinals.values, false, context)?;
        let left_indices = index_array(rows.len(), false, context, |_| Ok(Some(row)))?;
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(self.fields.len())?;
        for ((side, name), (_, dtype)) in self.columns.iter().zip(&self.fields) {
            columns.values.push(match side {
                Side::Left => take_column(
                    &logical_field_from_native_array(left, name)?,
                    &left_indices,
                    dtype,
                    context,
                )?,
                Side::Right => gather.column(name, dtype, context)?,
            });
        }
        let (columns, _ownership) = columns.into_parts();
        let input = StructArray::try_new(
            self.fields
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<FieldNames>(),
            columns,
            rows.len(),
            Validity::NonNullable,
        )
        .map_err(vortex_error)?
        .into_array();
        keys(&self.expression.evaluate(&input, context)?, context)
    }
}
