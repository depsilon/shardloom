//! SQL subquery predicates and cardinality-checked values over native row owners.

use super::{
    logical_field_from_native_array,
    native_capacity::ReservedVec,
    native_relational_batch::{Batch, Table, failed, take_batch},
    native_relational_expression::{Expression, keys},
    native_relational_index::RowIndex,
    native_relational_keys::Cell,
    result_batch, vortex_error,
};
use crate::{
    relational_query::VortexRelationalQuantifier as Quantifier,
    resident_session::NativeExecutionContext,
};
use shardloom_core::{ComparisonOp, Result};
use shardloom_exec::live_memory::LiveMemoryPool;
use std::cmp::Ordering;
use vortex::array::{
    ArrayRef,
    arrays::StructArray,
    dtype::{DType, FieldNames},
    memory::MemorySessionExt as _,
    validity::Validity,
};

pub(super) enum Kind {
    In,
    Quantified {
        comparison: ComparisonOp,
        quantifier: Quantifier,
    },
    Exists,
    Scalar {
        column: String,
    },
}

pub(super) struct Spec {
    pub(super) kind: Kind,
    pub(super) guard: Option<Expression>,
    pub(super) fields: Vec<(String, DType)>,
    pub(super) left_keys: Vec<String>,
    pub(super) right_keys: Vec<String>,
    pub(super) correlation: usize,
    pub(super) negated: bool,
}

pub(super) struct Subquery<'a> {
    spec: &'a Spec,
    right: Table,
    correlation: RowIndex,
    matches: RowIndex,
    nullable: ReservedVec<usize>,
}

/// Per-outer-batch results. Scalar values remain native columns in outer order;
/// the existing retained-payload owner compacts them for bounded delivery.
pub(super) enum Results {
    Predicate(ReservedVec<Option<bool>>),
    Scalar(ReservedVec<Option<ArrayRef>>),
}

impl Results {
    pub(super) fn new(spec: &Spec, rows: usize, memory: &LiveMemoryPool) -> Result<Self> {
        if matches!(spec.kind, Kind::Scalar { .. }) {
            let mut values = ReservedVec::new(memory)?;
            values.reserve(rows)?;
            values.values.resize(rows, None);
            Ok(Self::Scalar(values))
        } else {
            let mut values = ReservedVec::new(memory)?;
            values.reserve(rows)?;
            values.values.resize(rows, spec.inactive_value());
            Ok(Self::Predicate(values))
        }
    }

    pub(super) fn push(
        &mut self,
        subquery: &Subquery<'_>,
        input: &Batch,
        row: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        match self {
            Self::Predicate(values) => {
                values.values[row] = subquery.result(input, row, context)?;
            }
            Self::Scalar(values) => {
                values.values[row] = subquery.scalar_value(context)?;
            }
        }
        Ok(())
    }

    pub(super) fn finish(
        self,
        spec: &Spec,
        input: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        match self {
            Self::Predicate(values) => spec.output(input, &values.values, context),
            Self::Scalar(mut values) => {
                let column = super::native_payload::retained_column(
                    spec.output_dtype()?,
                    input.len(),
                    context,
                    |row| Ok(values.values[row].take()),
                )?;
                spec.append(input, column, context)
            }
        }
    }
}

impl<'a> Subquery<'a> {
    pub(super) fn new(spec: &'a Spec, memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            spec,
            right: Table::new(memory)?,
            correlation: RowIndex::new(memory)?,
            matches: RowIndex::new(memory)?,
            nullable: ReservedVec::new(memory)?,
        })
    }

    pub(super) fn build(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        if matches!(self.spec.kind, Kind::Scalar { .. }) {
            context.check_cancelled()?;
            if array.len() > 1 || (array.len() == 1 && self.right.rows() != 0) {
                return Err(failed(
                    "scalar subquery cardinality violation: more than one row",
                ));
            }
            if array.is_empty() {
                return Ok(());
            }
            self.right.push(Batch::new(array, &[], context)?)?;
            return context.check_cancelled();
        }
        let batch = Batch::new(array, &self.spec.right_keys, context)?;
        let rows = self.right.push(batch)?;
        for row in rows {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            self.right.hash(row, true)?; // Reject nonfinite keys independent of a short circuit.
            if self.spec.correlation > 0 {
                let Some(hash) = self.right.hash_prefix(row, self.spec.correlation, false)? else {
                    continue;
                };
                self.correlation
                    .insert(hash, row, context.cancellation(), |old| {
                        self.right
                            .equal_prefix(row, old, self.spec.correlation, false)
                    })?;
            }
            if matches!(self.spec.kind, Kind::In) {
                if let Some(hash) = self.right.hash(row, false)? {
                    self.matches
                        .insert(hash, row, context.cancellation(), |old| {
                            self.right.equal(row, old, false)
                        })?;
                } else {
                    self.nullable.push(row)?;
                }
            }
        }
        context.check_cancelled()
    }

    pub(super) fn consume(
        &self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let selected = self.spec.selected_rows(&array, context)?;
        self.consume_selected(array, &selected.values, context, consume)
    }

    pub(super) fn consume_selected(
        &self,
        array: ArrayRef,
        selected: &[usize],
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        if let Kind::Scalar { column } = &self.spec.kind {
            let mut rows = ReservedVec::new(context.memory())?;
            rows.reserve(array.len())?;
            rows.values.resize(array.len(), None);
            if self.right.rows() == 1 {
                for &row in selected {
                    rows.values[row] = Some(0);
                }
            }
            let value = self.right.gather(&rows.values, true, context)?.column(
                column,
                self.spec.output_dtype()?,
                context,
            )?;
            consume(self.spec.append(&array, value, context)?)?;
            return context.check_cancelled();
        }
        let left = Batch::new(array, &self.spec.left_keys, context)?;
        let mut values = ReservedVec::new(context.memory())?;
        values.reserve(left.array.len())?;
        values
            .values
            .resize(left.array.len(), self.spec.inactive_value());
        for &row in selected {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            values.values[row] = self.result(&left, row, context)?;
        }
        let output = self.spec.output(&left.array, &values.values, context)?;
        consume(output)?;
        context.check_cancelled()
    }

    /// Compact a selected scalar into an owned one-value native column so its
    /// inner execution can release all unrelated buffers before the next row.
    pub(super) fn scalar_value(
        &self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<ArrayRef>> {
        let Kind::Scalar { column } = &self.spec.kind else {
            return Err(failed("scalar value requested from a predicate subquery"));
        };
        if self.right.rows() == 0 {
            return Ok(None);
        }
        self.right
            .gather(&[Some(0)], true, context)?
            .column(column, self.spec.output_dtype()?, context)
            .map(Some)
    }

    pub(super) fn result(
        &self,
        left: &Batch,
        row: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<bool>> {
        context.check_cancelled()?;
        if matches!(self.spec.kind, Kind::Scalar { .. }) {
            return Err(failed("scalar subquery cannot produce a predicate value"));
        }
        left.hash(row, true)?;
        let value = self.evaluate(left, row, context)?;
        Ok(if self.spec.negated {
            value.map(|value| !value)
        } else {
            value
        })
    }

    fn evaluate(
        &self,
        left: &Batch,
        row: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<bool>> {
        let correlation = if self.spec.correlation > 0 {
            let Some(hash) = left.hash_prefix(row, self.spec.correlation, false)? else {
                return Ok(Some(self.empty()));
            };
            let entry = self
                .correlation
                .find(hash, context.cancellation(), |right| {
                    self.right
                        .equal_prefix_batch(right, left, row, self.spec.correlation, false)
                })?;
            let Some(entry) = entry else {
                return Ok(Some(self.empty()));
            };
            Some(entry)
        } else {
            None
        };
        if matches!(self.spec.kind, Kind::Exists) {
            return Ok(Some(correlation.is_some() || self.right.rows() > 0));
        }
        let exact_hash = left.hash(row, false)?;
        if matches!(self.spec.kind, Kind::In)
            && let Some(hash) = exact_hash
            && self
                .matches
                .find(hash, context.cancellation(), |right| {
                    self.right.equal_batch(right, left, row, false)
                })?
                .is_some()
        {
            return Ok(Some(true));
        }
        // An unmatched complete IN key can only become UNKNOWN through a null
        // candidate; avoid scanning every nonnull row after the indexed miss.
        let only_nullable = matches!(self.spec.kind, Kind::In) && exact_hash.is_some();
        if let Some(entry) = correlation {
            self.reduce(
                left,
                row,
                self.correlation.rows(entry)?,
                only_nullable,
                context,
            )
        } else if only_nullable {
            self.reduce(
                left,
                row,
                self.nullable.values.iter().copied(),
                false,
                context,
            )
        } else {
            self.reduce(left, row, 0..self.right.rows(), false, context)
        }
    }

    fn empty(&self) -> bool {
        matches!(
            self.spec.kind,
            Kind::Quantified {
                quantifier: Quantifier::All,
                ..
            }
        )
    }

    fn reduce(
        &self,
        left: &Batch,
        row: usize,
        candidates: impl Iterator<Item = usize>,
        only_nullable: bool,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<bool>> {
        let all = self.empty();
        let mut unknown = false;
        for (visited, right) in candidates.enumerate() {
            if visited.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if only_nullable && self.right.hash(right, false)?.is_some() {
                continue;
            }
            let value = self.compare(left, row, right)?;
            if value == Some(!all) {
                return Ok(value);
            }
            unknown |= value.is_none();
        }
        Ok(if unknown { None } else { Some(all) })
    }

    fn compare(&self, left: &Batch, row: usize, right: usize) -> Result<Option<bool>> {
        let mut unknown = false;
        for key in self.spec.correlation..self.spec.left_keys.len() {
            if left.key_is_null(row, key)? || self.right.key_is_null(right, key)? {
                unknown = true;
                continue;
            }
            let order = self
                .right
                .compare_batch_key(right, left, row, key)?
                .reverse();
            let comparison = match self.spec.kind {
                Kind::Quantified { comparison, .. } => comparison,
                _ => ComparisonOp::Eq,
            };
            if !compare(order, comparison) {
                return Ok(Some(false));
            }
        }
        Ok(if unknown { None } else { Some(true) })
    }
}

impl Spec {
    pub(super) fn selected_rows(
        &self,
        array: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ReservedVec<usize>> {
        let guard = self
            .guard
            .as_ref()
            .map(|guard| keys(&guard.evaluate(array, context)?, context))
            .transpose()?;
        let mut selected = ReservedVec::new(context.memory())?;
        selected.reserve(array.len())?;
        for row in 0..array.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if guard
                .as_ref()
                .map(|guard| guard.cell(row))
                .transpose()?
                .is_none_or(|value| value == Cell::Boolean(true))
            {
                selected.values.push(row);
            }
        }
        Ok(selected)
    }

    pub(super) fn inactive_value(&self) -> Option<bool> {
        matches!(self.kind, Kind::Exists).then_some(false)
    }

    pub(super) fn output_dtype(&self) -> Result<&DType> {
        self.fields
            .last()
            .map(|(_, dtype)| dtype)
            .ok_or_else(|| failed("subquery output field is absent"))
    }

    pub(super) fn output(
        &self,
        array: &ArrayRef,
        values: &[Option<bool>],
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let column = result_batch::build_column(
            self.output_dtype()?,
            values.len(),
            &context.native_session().allocator(),
            |row| Ok(values[row].map_or(result_batch::Value::Null, result_batch::Value::Bool)),
        )?;
        self.append(array, column, context)
    }

    pub(super) fn append(
        &self,
        array: &ArrayRef,
        column: ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        if column.len() != array.len() || column.dtype() != self.output_dtype()? {
            return Err(failed(
                "subquery result differs from its bound schema or cardinality",
            ));
        }
        let mut rows = ReservedVec::new(context.memory())?;
        rows.reserve(array.len())?;
        rows.values.extend(0..array.len());
        let fields = &self.fields[..self.fields.len() - 1];
        let input = take_batch(array, fields, &rows.values, context)?;
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(self.fields.len())?;
        for (name, _) in fields {
            columns
                .values
                .push(logical_field_from_native_array(&input, name)?);
        }
        columns.values.push(column);
        let (columns, _ownership) = columns.into_parts();
        StructArray::try_new(
            self.fields
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<FieldNames>(),
            columns,
            array.len(),
            Validity::NonNullable,
        )
        .map_err(vortex_error)
        .map(vortex::array::IntoArray::into_array)
    }
}

fn compare(order: Ordering, op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Eq => order == Ordering::Equal,
        ComparisonOp::NotEq => order != Ordering::Equal,
        ComparisonOp::Lt => order == Ordering::Less,
        ComparisonOp::LtEq => order != Ordering::Greater,
        ComparisonOp::Gt => order == Ordering::Greater,
        ComparisonOp::GtEq => order != Ordering::Less,
    }
}
