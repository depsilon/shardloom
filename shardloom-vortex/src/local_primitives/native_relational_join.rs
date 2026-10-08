//! Duplicate-preserving native joins with a retained build side and bounded output.

use super::{
    logical_field_from_native_array,
    native_capacity::ReservedVec,
    native_relational_batch::{Batch, Table, failed, index_array, take_column},
    native_relational_expression::Expression,
    native_relational_index::RowIndex,
    result_batch, vortex_error,
};
use crate::{
    relational_query::{VortexRelationalJoinKind as Kind, VortexRelationalSide as Side},
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::array::{
    ArrayRef, IntoArray as _,
    arrays::StructArray,
    dtype::{DType, FieldNames},
    memory::MemorySessionExt as _,
    validity::Validity,
};

pub(super) struct Spec {
    pub(super) kind: Kind,
    pub(super) left_keys: Vec<String>,
    pub(super) right_keys: Vec<String>,
    pub(super) fields: Vec<(String, DType)>,
    pub(super) columns: Vec<(Side, String)>,
    pub(super) condition: Option<Condition>,
}

pub(super) struct Condition {
    pub(super) expression: Expression,
    pub(super) fields: Vec<(String, DType)>,
    pub(super) columns: Vec<(Side, String)>,
}

#[path = "native_relational_join_condition.rs"]
mod condition;
#[cfg(feature = "vortex-write")]
#[path = "native_relational_join_spill.rs"]
pub(super) mod spill;

pub(super) struct Join<'a> {
    spec: &'a Spec,
    right: Table,
    index: RowIndex,
    matched: ReservedVec<bool>,
}

impl<'a> Join<'a> {
    pub(super) fn new(spec: &'a Spec, memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            spec,
            right: Table::new(memory)?,
            index: RowIndex::new(memory)?,
            matched: ReservedVec::new(memory)?,
        })
    }

    pub(super) fn build(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        context.check_cancelled()?;
        let batch = Batch::new(array, &self.spec.right_keys, context)?;
        let keep_matches = matches!(self.spec.kind, Kind::Right | Kind::Full);
        if keep_matches {
            self.matched.reserve(batch.array.len())?;
        }
        let rows = self.right.push(batch)?;
        if keep_matches {
            self.matched.values.resize(rows.end, false);
        }
        if !self.spec.right_keys.is_empty() {
            for row in rows {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                if let Some(hash) = self.right.hash(row, false)? {
                    self.index
                        .insert(hash, row, context.cancellation(), |old| {
                            self.right.equal(row, old, false)
                        })?;
                }
            }
        }
        context.check_cancelled()
    }

    pub(super) fn consume(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<u64> {
        if batch_rows == 0 {
            return Err(failed("output batch rows must be positive"));
        }
        let left = Batch::new(array, &self.spec.left_keys, context)?;
        if self.spec.condition.is_some() {
            return self.consume_condition(&left, context, batch_rows, consume);
        }
        let mut pairs = Pairs::new(batch_rows, context.memory())?;
        let mut written = 0u64;
        for row in 0..left.array.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            let entry = left
                .hash(row, false)?
                .map(|hash| {
                    self.index.find(hash, context.cancellation(), |right| {
                        self.right.equal_batch(right, &left, row, false)
                    })
                })
                .transpose()?
                .flatten();
            let has_match = if self.spec.kind == Kind::Cross {
                self.right.rows() > 0
            } else {
                entry.is_some()
            };
            if matches!(self.spec.kind, Kind::LeftSemi | Kind::LeftAnti) {
                if has_match == (self.spec.kind == Kind::LeftSemi) {
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
                continue;
            }
            if self.spec.kind == Kind::Cross {
                for right in 0..self.right.rows() {
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
            } else if let Some(entry) = entry {
                for right in self.index.rows(entry)? {
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
            } else if matches!(self.spec.kind, Kind::Left | Kind::Full) {
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

    pub(super) fn finish(
        &self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<u64> {
        if !matches!(self.spec.kind, Kind::Right | Kind::Full) {
            return Ok(0);
        }
        let mut pairs = Pairs::new(batch_rows, context.memory())?;
        let mut written = 0;
        for (row, matched) in self.matched.values.iter().enumerate() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if !matched {
                self.push_pair(
                    None,
                    None,
                    Some(row),
                    &mut pairs,
                    &mut written,
                    context,
                    consume,
                )?;
            }
        }
        self.flush(None, &mut pairs, &mut written, context, consume)?;
        Ok(written)
    }

    #[allow(clippy::too_many_arguments)]
    fn push_pair(
        &self,
        left: Option<&ArrayRef>,
        left_row: Option<usize>,
        right_row: Option<usize>,
        pairs: &mut Pairs,
        written: &mut u64,
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        pairs.left.values.push(left_row);
        pairs.right.values.push(right_row);
        if pairs.left.values.len() == pairs.limit {
            self.flush(left, pairs, written, context, consume)?;
        }
        Ok(())
    }

    fn flush(
        &self,
        left: Option<&ArrayRef>,
        pairs: &mut Pairs,
        written: &mut u64,
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        self.spec
            .flush(&self.right, left, pairs, written, context, consume)
    }
}

impl Spec {
    fn keep_unpaired_left(&self, matched: bool) -> bool {
        match self.kind {
            Kind::LeftSemi => matched,
            Kind::LeftAnti | Kind::Left | Kind::Full => !matched,
            _ => false,
        }
    }

    fn short_circuit(&self) -> bool {
        matches!(self.kind, Kind::LeftSemi | Kind::LeftAnti)
    }

    #[allow(clippy::too_many_arguments)] // Both join strategies share the exact payload builder.
    fn flush(
        &self,
        right_table: &Table,
        left: Option<&ArrayRef>,
        pairs: &mut Pairs,
        written: &mut u64,
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let rows = pairs.left.values.len();
        if rows == 0 {
            return Ok(());
        }
        context.check_cancelled()?;
        let next = written
            .checked_add(rows as u64)
            .ok_or_else(|| failed("join cardinality overflow"))?;
        let right = self
            .columns
            .iter()
            .any(|(side, _)| *side == Side::Right)
            .then(|| {
                right_table.gather(
                    &pairs.right.values,
                    matches!(self.kind, Kind::Left | Kind::Full),
                    context,
                )
            })
            .transpose()?;
        let left_indices = left
            .map(|_| {
                index_array(
                    rows,
                    matches!(self.kind, Kind::Right | Kind::Full),
                    context,
                    |row| Ok(pairs.left.values[row]),
                )
            })
            .transpose()?;
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(self.fields.len())?;
        for ((side, name), (_, dtype)) in self.columns.iter().zip(&self.fields) {
            let array = match side {
                Side::Left => match (left, &left_indices) {
                    (Some(left), Some(indices)) => take_column(
                        &logical_field_from_native_array(left, name)?,
                        indices,
                        dtype,
                        context,
                    )?,
                    _ if super::native_payload::is_nested(dtype) => {
                        super::native_payload::defaults(dtype, rows, context)?
                    }
                    _ => result_batch::build_column(
                        dtype,
                        rows,
                        &context.native_session().allocator(),
                        |_| Ok(result_batch::Value::Null),
                    )?,
                },
                Side::Right => right
                    .as_ref()
                    .ok_or_else(|| failed("right gather is absent"))?
                    .column(name, dtype, context)?,
            };
            columns.values.push(array);
        }
        let (columns, _ownership) = columns.into_parts();
        let array = StructArray::try_new(
            self.fields
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<FieldNames>(),
            columns,
            rows,
            Validity::NonNullable,
        )
        .map_err(vortex_error)?
        .into_array();
        consume(array)?;
        context.check_cancelled()?;
        *written = next;
        pairs.left.values.clear();
        pairs.right.values.clear();
        Ok(())
    }
}

struct Pairs {
    left: ReservedVec<Option<usize>>,
    right: ReservedVec<Option<usize>>,
    limit: usize,
}
impl Pairs {
    fn new(limit: usize, memory: &LiveMemoryPool) -> Result<Self> {
        if limit == 0 {
            return Err(failed("output batch rows must be positive"));
        }
        let mut left = ReservedVec::new(memory)?;
        let mut right = ReservedVec::new(memory)?;
        left.reserve(limit)?;
        right.reserve(limit)?;
        Ok(Self { left, right, limit })
    }
}

#[cfg(test)]
#[path = "native_relational_join_tests.rs"]
mod tests;
