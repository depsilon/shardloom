//! SQL three-valued subquery predicates over retained native keys.

use super::{
    logical_field_from_native_array,
    native_capacity::ReservedVec,
    native_relational_batch::{Batch, Table, failed, take_batch},
    native_relational_index::RowIndex,
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
}

pub(super) struct Spec {
    pub(super) kind: Kind,
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
        let left = Batch::new(array, &self.spec.left_keys, context)?;
        let mut values = ReservedVec::new(context.memory())?;
        values.reserve(left.array.len())?;
        for row in 0..left.array.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            values.values.push(self.result(&left, row, context)?);
        }
        let output = self.spec.output(&left.array, &values.values, context)?;
        consume(output)?;
        context.check_cancelled()
    }

    pub(super) fn result(
        &self,
        left: &Batch,
        row: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<bool>> {
        context.check_cancelled()?;
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
    pub(super) fn output(
        &self,
        array: &ArrayRef,
        values: &[Option<bool>],
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
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
        columns.values.push(result_batch::build_column(
            &self
                .fields
                .last()
                .ok_or_else(|| failed("subquery output field is absent"))?
                .1,
            values.len(),
            &context.native_session().allocator(),
            |row| Ok(values[row].map_or(result_batch::Value::Null, result_batch::Value::Bool)),
        )?);
        let (columns, _ownership) = columns.into_parts();
        StructArray::try_new(
            self.fields
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<FieldNames>(),
            columns,
            values.len(),
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
