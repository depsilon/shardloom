//! Whole-partition windows over retained native payload and reserved ordinals.

use super::{
    native_capacity::ReservedVec,
    native_relational_batch::{Batch, Table, failed},
    native_relational_order, result_batch, vortex_error,
};
use crate::{
    relational_query::{
        VortexRelationalNullOrder as NullOrder, VortexRelationalWindowFunction as Function,
    },
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::cmp::Ordering;
use vortex::array::{
    ArrayRef,
    arrays::StructArray,
    dtype::{DType, FieldNames},
    memory::MemorySessionExt as _,
    validity::Validity,
};

#[derive(Clone, PartialEq, Eq)]
pub(super) struct OrderKey {
    pub(super) key: usize,
    pub(super) descending: bool,
    pub(super) nulls: Option<NullOrder>,
}

pub(super) struct Group {
    pub(super) partition: Vec<usize>,
    pub(super) order: Vec<OrderKey>,
    pub(super) functions: Vec<usize>,
}

pub(super) struct Spec {
    pub(super) fields: Vec<(String, DType)>,
    pub(super) columns: Vec<String>,
    pub(super) keys: Vec<String>,
    pub(super) functions: Vec<Function>,
    pub(super) groups: Vec<Group>,
}

pub(super) struct Window<'a> {
    spec: &'a Spec,
    table: Table,
}

impl<'a> Window<'a> {
    pub(super) fn new(spec: &'a Spec, memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            spec,
            table: Table::new(memory)?,
        })
    }

    pub(super) fn build(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let batch = Batch::new(array, &self.spec.keys, context)?;
        for row in 0..batch.array.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            // Validate every key even for singleton partitions or already sorted
            // data, where a comparator might never inspect a nonfinite value.
            batch.hash(row, true)?;
        }
        self.table.push(batch)?;
        Ok(())
    }

    pub(super) fn finish(
        &self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let mut values = ReservedVec::new(context.memory())?;
        values.reserve(self.spec.functions.len())?;
        for function in &self.spec.functions {
            values
                .values
                .push(Values::new(function, self.table.rows(), context.memory())?);
        }
        for group in &self.spec.groups {
            self.evaluate_group(group, &mut values.values, context)?;
        }
        for start in (0..self.table.rows()).step_by(batch_rows) {
            context.check_cancelled()?;
            let end = self.table.rows().min(start.saturating_add(batch_rows));
            consume(self.output(start..end, &values.values, context)?)?;
            context.check_cancelled()?;
        }
        Ok(())
    }

    fn evaluate_group(
        &self,
        group: &Group,
        values: &mut [Values],
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let mut ordinals = ReservedVec::new(context.memory())?;
        ordinals.reserve(self.table.rows())?;
        for row in 0..self.table.rows() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            for key in &group.order {
                if key.nulls.is_none() && self.table.key_is_null(row, key.key)? {
                    return Err(failed(
                        "window ORDER BY requires explicit NULLS FIRST or NULLS LAST for null values",
                    ));
                }
            }
            ordinals.values.push(row);
        }
        native_relational_order::sort(
            &mut ordinals.values,
            context.cancellation(),
            |left, right| {
                let partition = self.compare_partition(group, left, right)?;
                if partition != Ordering::Equal {
                    return Ok(partition);
                }
                Ok(self
                    .compare_order(group, left, right)?
                    .then(left.cmp(&right)))
            },
        )?;
        let mut start = 0;
        while start < ordinals.values.len() {
            context.check_cancelled()?;
            let mut end = start + 1;
            while end < ordinals.values.len()
                && self.compare_partition(group, ordinals.values[start], ordinals.values[end])?
                    == Ordering::Equal
            {
                if end.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                end += 1;
            }
            self.evaluate_partition(group, &ordinals.values[start..end], values, context)?;
            start = end;
        }
        Ok(())
    }

    fn compare_partition(&self, group: &Group, left: usize, right: usize) -> Result<Ordering> {
        for &key in &group.partition {
            let order = self.table.compare_key(left, right, key)?;
            if order != Ordering::Equal {
                return Ok(order);
            }
        }
        Ok(Ordering::Equal)
    }

    fn compare_order(&self, group: &Group, left: usize, right: usize) -> Result<Ordering> {
        for key in &group.order {
            let nulls = (
                self.table.key_is_null(left, key.key)?,
                self.table.key_is_null(right, key.key)?,
            );
            let order = match nulls {
                (true, true) => Ordering::Equal,
                (true, false) => {
                    if key.nulls == Some(NullOrder::First) {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (false, true) => {
                    if key.nulls == Some(NullOrder::First) {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                (false, false) => {
                    let order = self.table.compare_key(left, right, key.key)?;
                    if key.descending {
                        order.reverse()
                    } else {
                        order
                    }
                }
            };
            if order != Ordering::Equal {
                return Ok(order);
            }
        }
        Ok(Ordering::Equal)
    }

    fn evaluate_partition(
        &self,
        group: &Group,
        rows: &[usize],
        values: &mut [Values],
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let mut peer_start = 0;
        let mut dense_rank = 0u64;
        while peer_start < rows.len() {
            context.check_cancelled()?;
            let mut peer_end = peer_start + 1;
            while peer_end < rows.len()
                && self.compare_order(group, rows[peer_start], rows[peer_end])? == Ordering::Equal
            {
                if peer_end.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                peer_end += 1;
            }
            dense_rank = dense_rank
                .checked_add(1)
                .ok_or_else(|| failed("window dense rank overflow"))?;
            for position in peer_start..peer_end {
                if position.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                for &function in &group.functions {
                    values[function].set(
                        &self.spec.functions[function],
                        PeerRow {
                            rows,
                            position,
                            peer_start,
                            peer_end,
                            dense_rank,
                        },
                    )?;
                }
            }
            peer_start = peer_end;
        }
        Ok(())
    }

    fn output(
        &self,
        range: std::ops::Range<usize>,
        values: &[Values],
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let mut rows = ReservedVec::new(context.memory())?;
        rows.reserve(range.len())?;
        rows.values.extend(range.clone().map(Some));
        let gather = self.table.gather(&rows.values, false, context)?;
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(self.spec.fields.len())?;
        for (name, (_, dtype)) in self.spec.columns.iter().zip(&self.spec.fields) {
            columns.values.push(gather.column(name, dtype, context)?);
        }
        for (index, (function, values)) in self.spec.functions.iter().zip(values).enumerate() {
            context.check_cancelled()?;
            let dtype = &self.spec.fields[self.spec.columns.len() + index].1;
            columns.values.push(match (function, values) {
                (
                    Function::Lag { column, .. } | Function::Lead { column, .. },
                    Values::Source(source),
                ) => {
                    rows.values.clear();
                    rows.values
                        .extend(range.clone().map(|row| source.values[row]));
                    self.table.gather(&rows.values, true, context)?.column(
                        column.as_str(),
                        dtype,
                        context,
                    )?
                }
                (_, Values::Integer(values)) => result_batch::build_column(
                    dtype,
                    range.len(),
                    &context.native_session().allocator(),
                    |row| {
                        Ok(result_batch::Value::Int(
                            i64::try_from(values.values[range.start + row])
                                .map_err(vortex_error)?,
                        ))
                    },
                )?,
                (_, Values::Fraction(values)) => result_batch::build_column(
                    dtype,
                    range.len(),
                    &context.native_session().allocator(),
                    |row| Ok(result_batch::Value::Float(values.values[range.start + row])),
                )?,
                _ => return Err(failed("window value storage disagrees with bound function")),
            });
        }
        let (columns, _ownership) = columns.into_parts();
        StructArray::try_new(
            self.spec
                .fields
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<FieldNames>(),
            columns,
            range.len(),
            Validity::NonNullable,
        )
        .map_err(vortex_error)
        .map(vortex::array::IntoArray::into_array)
    }
}

enum Values {
    Integer(ReservedVec<u64>),
    Fraction(ReservedVec<f64>),
    Source(ReservedVec<Option<usize>>),
}

#[derive(Clone, Copy)]
struct PeerRow<'a> {
    rows: &'a [usize],
    position: usize,
    peer_start: usize,
    peer_end: usize,
    dense_rank: u64,
}

impl Values {
    fn new(function: &Function, rows: usize, memory: &LiveMemoryPool) -> Result<Self> {
        Ok(match function {
            Function::Lag { .. } | Function::Lead { .. } => {
                Self::Source(filled(rows, None, memory)?)
            }
            Function::PercentRank | Function::CumeDist => {
                Self::Fraction(filled(rows, 0.0, memory)?)
            }
            _ => Self::Integer(filled(rows, 0, memory)?),
        })
    }

    #[allow(clippy::cast_precision_loss)] // SQL distribution output is an explicit F64 ratio.
    fn set(&mut self, function: &Function, at: PeerRow<'_>) -> Result<()> {
        let row = at.rows[at.position];
        match (self, function) {
            (Self::Integer(values), function) => {
                values.values[row] = match function {
                    Function::RowNumber => u64::try_from(at.position + 1).map_err(vortex_error)?,
                    Function::Rank => u64::try_from(at.peer_start + 1).map_err(vortex_error)?,
                    Function::DenseRank => at.dense_rank,
                    Function::Ntile { buckets } => {
                        u64::try_from(ntile(at.position, at.rows.len(), *buckets)?)
                            .map_err(vortex_error)?
                    }
                    _ => return Err(failed("noninteger window function used integer storage")),
                };
            }
            (Self::Fraction(values), Function::PercentRank) => {
                values.values[row] = if at.rows.len() == 1 {
                    0.0
                } else {
                    at.peer_start as f64 / (at.rows.len() - 1) as f64
                };
            }
            (Self::Fraction(values), Function::CumeDist) => {
                values.values[row] = at.peer_end as f64 / at.rows.len() as f64;
            }
            (Self::Source(values), Function::Lag { offset, .. }) => {
                values.values[row] = at
                    .position
                    .checked_sub(*offset)
                    .map(|position| at.rows[position]);
            }
            (Self::Source(values), Function::Lead { offset, .. }) => {
                values.values[row] = at
                    .position
                    .checked_add(*offset)
                    .and_then(|position| at.rows.get(position).copied());
            }
            _ => return Err(failed("window function storage mismatch")),
        }
        Ok(())
    }
}

fn filled<T: Clone>(rows: usize, value: T, memory: &LiveMemoryPool) -> Result<ReservedVec<T>> {
    let mut values = ReservedVec::new(memory)?;
    values.reserve(rows)?;
    values.values.resize(rows, value);
    Ok(values)
}

fn ntile(position: usize, rows: usize, buckets: usize) -> Result<usize> {
    if buckets == 0 {
        return Err(failed("NTILE bucket count must be positive"));
    }
    let width = rows / buckets;
    if width == 0 {
        return Ok(position + 1);
    }
    let larger = rows % buckets;
    let wider = width
        .checked_add(1)
        .ok_or_else(|| failed("NTILE width overflow"))?;
    let boundary = larger
        .checked_mul(wider)
        .ok_or_else(|| failed("NTILE boundary overflow"))?;
    Ok(if position < boundary {
        position / wider + 1
    } else {
        larger + (position - boundary) / width + 1
    })
}
