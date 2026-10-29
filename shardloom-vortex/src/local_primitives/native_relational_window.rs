//! Analytic windows over retained native payload and shared reserved ordinals.

#[path = "native_relational_window_fill.rs"]
pub(super) mod fill;
#[cfg(feature = "vortex-write")]
#[path = "native_relational_window_spill.rs"]
pub(in crate::local_primitives) mod spill;

use super::{
    native_capacity::ReservedVec,
    native_collection_ordinals,
    native_relational_aggregate::Function as Aggregate,
    native_relational_batch::{Batch, Table, failed},
    native_relational_order, native_relational_window_frame as frame, result_batch, vortex_error,
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
    ArrayRef, VortexSessionExecute as _,
    arrays::StructArray,
    dtype::{DType, FieldNames},
    memory::HostMemorySessionExt as _,
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
    pub(super) parent_validity_keys: Vec<usize>,
    pub(super) functions: Vec<Function>,
    pub(super) groups: Vec<Group>,
    pub(super) frames: Vec<Option<frame::Spec>>,
    pub(super) fills: Vec<Option<fill::Spec>>,
    pub(super) ordering_keys: usize,
}

pub(super) fn input_key(
    spec: &Spec,
    array: &ArrayRef,
    index: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let column = super::logical_field_from_native_array(array, &spec.keys[index])?;
    if !spec.parent_validity_keys.contains(&index) {
        return Ok(column);
    }
    let valid = super::native_relational_expression::parent_validity(&column, context)?;
    result_batch::build_column_with_float_policy(
        &DType::Bool(vortex::array::dtype::Nullability::Nullable),
        array.len(),
        &context.native_session().host_allocator(),
        context.float_policy(),
        |row| {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            Ok(if valid.value(row) {
                result_batch::Value::Bool(true)
            } else {
                result_batch::Value::Null
            })
        },
    )
}

pub(super) fn input_batch(
    spec: &Spec,
    array: ArrayRef,
    context: &NativeExecutionContext<'_>,
) -> Result<Batch> {
    let mut keys = ReservedVec::new(context.memory())?;
    keys.reserve(spec.keys.len())?;
    let mut execution = context.native_session().create_execution_ctx();
    for index in 0..spec.keys.len() {
        context.check_cancelled()?;
        let column = input_key(spec, &array, index, context)?;
        keys.values.push(
            super::native_relational_keys::KeyColumn::new_with_float_policy(
                &column,
                &mut execution,
                context.memory(),
                context.cancellation(),
                context.float_policy(),
            )?,
        );
    }
    Ok(Batch::from_prepared_keys(array, keys))
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
        let batch = input_batch(self.spec, array, context)?;
        for row in 0..batch.array.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            // Validate every key even for singleton partitions or already sorted
            // data, where a comparator might never inspect a nonfinite value.
            batch.hash_prefix(row, self.spec.ordering_keys, true)?;
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
        for (index, function) in self.spec.functions.iter().enumerate() {
            values.values.push(Values::new(
                function,
                self.spec.frames[index].as_ref(),
                self.table.rows(),
                context.memory(),
            )?);
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
        let has_frames = group
            .functions
            .iter()
            .any(|&index| self.spec.frames[index].is_some());
        let mut peers = ReservedVec::new(context.memory())?;
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
            if has_frames {
                peers.push(peer_start)?;
            }
            for position in peer_start..peer_end {
                if position.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                for &function in &group.functions {
                    if self.spec.frames[function].is_some() || self.spec.fills[function].is_some() {
                        continue;
                    }
                    values[function].set(
                        &self.spec.functions[function],
                        PeerRow {
                            rows: rows.len(),
                            position,
                            peer_start,
                            peer_end,
                            dense_rank,
                        },
                        rows,
                        context,
                    )?;
                }
            }
            peer_start = peer_end;
        }
        if has_frames {
            peers.push(rows.len())?;
            for &function in &group.functions {
                if let Some(frame) = &self.spec.frames[function] {
                    self.evaluate_frame(
                        frame,
                        rows,
                        &peers.values,
                        &mut values[function],
                        context,
                    )?;
                }
            }
        }
        for &function in &group.functions {
            if let Some(spec) = &self.spec.fills[function] {
                let mut input = frame::Resident {
                    rows,
                    table: &self.table,
                };
                fill::run(spec, rows.len(), &mut input, context, |position, value| {
                    values[function].set_frame(
                        rows[position],
                        value.map_source(
                            |selected| {
                                rows.get(selected)
                                    .copied()
                                    .ok_or_else(|| failed("fill selection exceeds its partition"))
                            },
                            context,
                        )?,
                    )
                })?;
            }
        }
        Ok(())
    }

    fn evaluate_frame(
        &self,
        spec: &frame::Spec,
        rows: &[usize],
        peers: &[usize],
        values: &mut Values,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let mut cursor = frame::Cursor::default();
        let mut state = frame::State::new(spec, context.memory())?;
        let mut input = frame::Resident {
            rows,
            table: &self.table,
        };
        let mut peer_edge = |index| Ok(peers.get(index).copied().unwrap_or(rows.len()));
        let mut peer = 0;
        for position in 0..rows.len() {
            if position.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            while peers[peer + 1] <= position {
                peer += 1;
            }
            let at = frame::Position {
                rows: rows.len(),
                peer_start: peers[peer],
                peer_end: peers[peer + 1],
                peer,
                row: position,
            };
            let range = cursor.advance(&spec.frame, &at, &mut input, &mut peer_edge, context)?;
            let ranges = frame::intervals(range, &at, spec.frame.exclusion);
            values.set_frame(
                rows[position],
                state
                    .advance(ranges, spec, rows.len(), &mut input, context)?
                    .map_source(
                        |position| {
                            rows.get(position).copied().ok_or_else(|| {
                                failed("selected window position exceeds its partition")
                            })
                        },
                        context,
                    )?,
            )?;
        }
        Ok(())
    }

    fn collection_output(
        &self,
        index: usize,
        source: &native_collection_ordinals::Resident,
        range: std::ops::Range<usize>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let frame = self.spec.frames[index]
            .as_ref()
            .ok_or_else(|| failed("collection window frame is absent"))?;
        let column = frame
            .column
            .as_deref()
            .ok_or_else(|| failed("collection window column is absent"))?;
        let ordinals = source.output(range, context)?;
        native_collection_ordinals::gather(
            &ordinals,
            &self.spec.fields[self.spec.columns.len() + index].1,
            frame.separator.as_deref(),
            |rows, dtype| {
                self.table
                    .gather(rows, false, context)?
                    .column(column, dtype, context)
            },
            context,
        )
    }

    #[allow(clippy::too_many_lines)] // One typed output dispatch preserves the shared row mapping and column ownership.
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
                (_, Values::Sources(source)) => {
                    self.collection_output(index, source, range.clone(), context)?
                }
                (_, Values::Source(source)) => {
                    let column = match function {
                        Function::Lag { column, .. }
                        | Function::Lead { column, .. }
                        | Function::Fill { column, .. } => column.as_str(),
                        Function::Framed(_) => self.spec.frames[index]
                            .as_ref()
                            .and_then(|frame| frame.column.as_deref())
                            .ok_or_else(|| failed("framed value selection has no source column"))?,
                        _ => return Err(failed("window selection storage has no source function")),
                    };
                    rows.values.clear();
                    rows.values
                        .extend(range.clone().map(|row| source.values[row]));
                    self.table
                        .gather(&rows.values, true, context)?
                        .column(column, dtype, context)?
                }
                (_, Values::Integer(values)) => result_batch::build_column_with_float_policy(
                    dtype,
                    range.len(),
                    &context.native_session().host_allocator(),
                    context.float_policy(),
                    |row| {
                        Ok(result_batch::Value::Int(
                            i64::try_from(values.values[range.start + row])
                                .map_err(vortex_error)?,
                        ))
                    },
                )?,
                (_, Values::Fraction(values)) => result_batch::build_column_with_float_policy(
                    dtype,
                    range.len(),
                    &context.native_session().host_allocator(),
                    context.float_policy(),
                    |row| Ok(result_batch::Value::Float(values.values[range.start + row])),
                )?,
                (_, Values::Count(values)) => result_batch::build_column_with_float_policy(
                    dtype,
                    range.len(),
                    &context.native_session().host_allocator(),
                    context.float_policy(),
                    |row| Ok(result_batch::Value::UInt(values.values[range.start + row])),
                )?,
                (_, Values::NullableFraction(values)) => {
                    result_batch::build_column_with_float_policy(
                        dtype,
                        range.len(),
                        &context.native_session().host_allocator(),
                        context.float_policy(),
                        |row| {
                            Ok(values.values[range.start + row]
                                .map_or(result_batch::Value::Null, result_batch::Value::Float))
                        },
                    )?
                }
                (_, Values::Decimal(values)) => {
                    let DType::Decimal(decimal, _) = dtype else {
                        return Err(failed("window decimal output lost its metadata"));
                    };
                    result_batch::build_column_with_float_policy(
                        dtype,
                        range.len(),
                        &context.native_session().host_allocator(),
                        context.float_policy(),
                        |row| {
                            Ok(values.values[range.start + row]
                                .map_or(result_batch::Value::Null, |value| {
                                    result_batch::Value::Decimal(value, *decimal)
                                }))
                        },
                    )?
                }
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
    Sources(native_collection_ordinals::Resident),
    Count(ReservedVec<u64>),
    NullableFraction(ReservedVec<Option<f64>>),
    Decimal(ReservedVec<Option<i128>>),
}

#[derive(Clone, Copy)]
pub(super) struct PeerRow {
    pub(super) rows: usize,
    pub(super) position: usize,
    pub(super) peer_start: usize,
    pub(super) peer_end: usize,
    pub(super) dense_rank: u64,
}

impl Values {
    fn new(
        function: &Function,
        frame: Option<&frame::Spec>,
        rows: usize,
        memory: &LiveMemoryPool,
    ) -> Result<Self> {
        Ok(match function {
            Function::Framed(_) => {
                let frame = frame.ok_or_else(|| failed("framed function has no bound frame"))?;
                match frame.function {
                    frame::Function::Aggregate(function) if function.is_collection() => {
                        Self::Sources(native_collection_ordinals::Resident::new(rows, memory)?)
                    }
                    frame::Function::Aggregate(function) if function.is_moment() => {
                        Self::NullableFraction(filled(rows, None, memory)?)
                    }
                    frame::Function::Aggregate(Aggregate::Count | Aggregate::CountDistinct) => {
                        Self::Count(filled(rows, 0, memory)?)
                    }
                    frame::Function::Aggregate(
                        Aggregate::Sum
                        | Aggregate::Avg
                        | Aggregate::SumDistinct
                        | Aggregate::AvgDistinct
                        | Aggregate::Median
                        | Aggregate::QuantileCont,
                    ) if frame.decimal.is_some() => Self::Decimal(filled(rows, None, memory)?),
                    frame::Function::Aggregate(
                        Aggregate::Sum
                        | Aggregate::Avg
                        | Aggregate::SumDistinct
                        | Aggregate::AvgDistinct
                        | Aggregate::Median
                        | Aggregate::QuantileCont,
                    ) => Self::NullableFraction(filled(rows, None, memory)?),
                    _ => Self::Source(filled(rows, None, memory)?),
                }
            }
            Function::Fill {
                method: crate::relational_query::VortexRelationalFillMethod::Linear,
                ..
            } => Self::NullableFraction(filled(rows, None, memory)?),
            Function::Lag { .. } | Function::Lead { .. } | Function::Fill { .. } => {
                Self::Source(filled(rows, None, memory)?)
            }
            Function::PercentRank | Function::CumeDist => {
                Self::Fraction(filled(rows, 0.0, memory)?)
            }
            _ => Self::Integer(filled(rows, 0, memory)?),
        })
    }

    fn set_frame(&mut self, row: usize, value: frame::Value) -> Result<()> {
        match (self, value) {
            (Self::Integer(values), frame::Value::Integer(value))
            | (Self::Count(values), frame::Value::Count(value)) => values.values[row] = value,
            (Self::Fraction(values), frame::Value::Float(Some(value))) => {
                values.values[row] = value;
            }
            (Self::NullableFraction(values), frame::Value::Float(value)) => {
                values.values[row] = value;
            }
            (Self::Decimal(values), frame::Value::Decimal(value)) => values.values[row] = value,
            (Self::Source(values), frame::Value::Source(value)) => values.values[row] = value,
            (Self::Sources(values), frame::Value::Sources(value)) => {
                values.set(row, &value.values)?;
            }
            _ => {
                return Err(failed(
                    "framed value disagrees with its bound output storage",
                ));
            }
        }
        Ok(())
    }

    fn set(
        &mut self,
        function: &Function,
        at: PeerRow,
        rows: &[usize],
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let value = ranking_value(function, at)?.map_source(
            |position| {
                rows.get(position)
                    .copied()
                    .ok_or_else(|| failed("selected window position exceeds its partition"))
            },
            context,
        )?;
        self.set_frame(rows[at.position], value)
    }
}

#[allow(clippy::cast_precision_loss)] // SQL distribution output is an explicit F64 ratio.
pub(super) fn ranking_value(function: &Function, at: PeerRow) -> Result<frame::Value> {
    Ok(match function {
        Function::RowNumber => {
            frame::Value::Integer(u64::try_from(at.position + 1).map_err(vortex_error)?)
        }
        Function::Rank => {
            frame::Value::Integer(u64::try_from(at.peer_start + 1).map_err(vortex_error)?)
        }
        Function::DenseRank => frame::Value::Integer(at.dense_rank),
        Function::Ntile { buckets } => frame::Value::Integer(
            u64::try_from(ntile(at.position, at.rows, *buckets)?).map_err(vortex_error)?,
        ),
        Function::PercentRank => frame::Value::Float(Some(if at.rows == 1 {
            0.0
        } else {
            at.peer_start as f64 / (at.rows - 1) as f64
        })),
        Function::CumeDist => frame::Value::Float(Some(at.peer_end as f64 / at.rows as f64)),
        Function::Lag { offset, .. } => frame::Value::Source(at.position.checked_sub(*offset)),
        Function::Lead { offset, .. } => frame::Value::Source(
            at.position
                .checked_add(*offset)
                .filter(|&position| position < at.rows),
        ),
        Function::Framed(_) => return Err(failed("framed function used ranking semantics")),
        Function::Fill { .. } => return Err(failed("fill function used ranking semantics")),
    })
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
