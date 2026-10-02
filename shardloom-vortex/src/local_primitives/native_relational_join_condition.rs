//! Native ON evaluation over bounded candidate pairs before outer null extension.

use super::super::{
    native_relational_batch::{Table, failed},
    native_relational_expression::keys,
    native_relational_index::Rows,
    native_relational_keys::{Cell, KeyColumn},
};
use super::{
    ArrayRef, Batch, Condition, FieldNames, Join, Kind, NativeExecutionContext, Pairs, ReservedVec,
    Result, Side, StructArray, Validity, index_array, logical_field_from_native_array, take_column,
    vortex_error,
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
        let condition = self
            .spec
            .condition
            .as_ref()
            .ok_or_else(|| failed("ON predicate binding is absent"))?;
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
                let selected =
                    condition.evaluate(&left.array, &self.right, row, &rights.values, context)?;
                for (position, &right) in rights.values.iter().enumerate() {
                    if position.is_multiple_of(1024) {
                        context.check_cancelled()?;
                    }
                    if selected.cell(position)? != Cell::Boolean(true) {
                        continue;
                    }
                    matched = true;
                    if matches!(self.spec.kind, Kind::LeftSemi | Kind::LeftAnti) {
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
                if matched && matches!(self.spec.kind, Kind::LeftSemi | Kind::LeftAnti) {
                    break;
                }
            }
            let keep_left = match self.spec.kind {
                Kind::LeftSemi => matched,
                Kind::LeftAnti | Kind::Left | Kind::Full => !matched,
                _ => false,
            };
            if keep_left {
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
