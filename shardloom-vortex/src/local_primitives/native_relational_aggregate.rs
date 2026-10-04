//! Exact grouping and distinct membership over native keys. Additive measures
//! retain the existing ordered floating accumulation policy across input batches.

use super::{
    SimpleAggregateFunction as Function, logical_field_from_native_array,
    native_capacity::ReservedVec,
    native_relational_batch::{failed, index_array},
    native_relational_expression::keys,
    native_relational_keys::{Cell, KeyColumn, compare_cells},
    native_relational_set::RowSet,
    result_batch::{self, Value},
    vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::borrow::Cow;
use vortex::{
    array::{
        ArrayRef, IntoArray as _,
        arrays::StructArray,
        dtype::{DType, FieldNames, PType},
        memory::MemorySessionExt as _,
        validity::Validity,
    },
    buffer::Alignment,
};

pub(super) struct Measure {
    pub(super) function: Function,
    pub(super) column: Option<String>,
    pub(super) dtype: DType,
    pub(super) distinct_fields: Vec<(String, DType)>,
    pub(super) distinct_names: Vec<String>,
}
pub(super) struct Spec {
    pub(super) fields: Vec<(String, DType)>,
    pub(super) groups: Vec<(String, DType)>,
    pub(super) group_names: Vec<String>,
    pub(super) measures: Vec<Measure>,
}

#[derive(Default)]
struct State {
    count: u64,
    sum: f64,
    extreme: Option<Cell>,
}

pub(super) struct Aggregate<'a> {
    spec: &'a Spec,
    groups: Option<RowSet<'a>>,
    states: ReservedVec<State>,
    distinct: ReservedVec<Option<RowSet<'a>>>,
}

impl<'a> Aggregate<'a> {
    pub(super) fn new(spec: &'a Spec, memory: &LiveMemoryPool) -> Result<Self> {
        let mut states = ReservedVec::new(memory)?;
        let groups = if spec.group_names.is_empty() {
            states.reserve(spec.measures.len())?;
            for _ in &spec.measures {
                states.values.push(State::default());
            }
            None
        } else {
            Some(RowSet::new(&spec.groups, &spec.group_names, memory)?)
        };
        let mut distinct = ReservedVec::new(memory)?;
        distinct.reserve(spec.measures.len())?;
        for measure in &spec.measures {
            distinct
                .values
                .push(if measure.function == Function::CountDistinct {
                    Some(RowSet::new(
                        &measure.distinct_fields,
                        &measure.distinct_names,
                        memory,
                    )?)
                } else {
                    None
                });
        }
        Ok(Self {
            spec,
            groups,
            states,
            distinct,
        })
    }

    pub(super) fn consume(
        &mut self,
        array: &ArrayRef,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
    ) -> Result<()> {
        context.check_cancelled()?;
        let mut ordinals = ReservedVec::new(context.memory())?;
        if let Some(groups) = &mut self.groups {
            ordinals = groups.intern_batch(array.clone(), context, batch_rows)?;
            let count = groups
                .rows()
                .checked_mul(self.spec.measures.len())
                .ok_or_else(|| failed("aggregate state cardinality overflow"))?;
            self.states.reserve(count - self.states.values.len())?;
            self.states.values.resize_with(count, State::default);
        } else {
            ordinals.reserve(array.len())?;
            ordinals.values.resize(array.len(), 0);
        }
        for (measure_index, measure) in self.spec.measures.iter().enumerate() {
            context.check_cancelled()?;
            let column = measure
                .column
                .as_ref()
                .map(|name| logical_field_from_native_array(array, name))
                .transpose()?;
            let values = column
                .as_ref()
                .map(|array| keys(array, context))
                .transpose()?;
            if let Some(distinct) = self.distinct.values[measure_index].as_mut() {
                accumulate_distinct(
                    distinct,
                    &ordinals.values,
                    column.as_ref().expect("bound distinct column"),
                    values.as_ref().expect("bound distinct values"),
                    &mut self.states.values,
                    self.spec.measures.len(),
                    measure_index,
                    context,
                    batch_rows,
                )?;
                continue;
            }
            for (row, &group) in ordinals.values.iter().enumerate() {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                let value = values
                    .as_ref()
                    .map(|values| values.raw_cell(row))
                    .transpose()?;
                let state =
                    &mut self.states.values[group * self.spec.measures.len() + measure_index];
                update(state, measure, value, context)?;
            }
        }
        context.check_cancelled()
    }

    pub(super) fn finish(
        &self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let count = self.groups.as_ref().map_or(1, RowSet::rows);
        let mut rows = ReservedVec::new(context.memory())?;
        rows.reserve(count.min(batch_rows))?;
        for start in (0..count).step_by(batch_rows) {
            context.check_cancelled()?;
            let end = count.min(start.saturating_add(batch_rows));
            rows.values.clear();
            rows.values.extend((start..end).map(Some));
            let mut columns = ReservedVec::new(context.memory())?;
            columns.reserve(self.spec.fields.len())?;
            if let Some(groups) = &self.groups {
                let gather = groups.gather(&rows.values, context)?;
                for (name, dtype) in &self.spec.groups {
                    columns.values.push(gather.column(name, dtype, context)?);
                }
            }
            for (index, measure) in self.spec.measures.iter().enumerate() {
                columns.values.push(result_batch::build_column(
                    &measure.dtype,
                    end - start,
                    &context.native_session().allocator(),
                    |row| {
                        if row.is_multiple_of(1024) {
                            context.check_cancelled()?;
                        }
                        final_value(
                            &self.states.values[(start + row) * self.spec.measures.len() + index],
                            measure,
                        )
                    },
                )?);
            }
            let (columns, _ownership) = columns.into_parts();
            let output = StructArray::try_new(
                self.spec
                    .fields
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<FieldNames>(),
                columns,
                end - start,
                Validity::NonNullable,
            )
            .map_err(vortex_error)?
            .into_array();
            consume(output)?;
            context.check_cancelled()?;
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)] // A bounded batch updates one measure's state through the shared exact set.
fn accumulate_distinct(
    distinct: &mut RowSet<'_>,
    groups: &[usize],
    column: &ArrayRef,
    values: &KeyColumn,
    states: &mut [State],
    width: usize,
    measure: usize,
    context: &NativeExecutionContext<'_>,
    batch_rows: usize,
) -> Result<()> {
    let mut rows = ReservedVec::new(context.memory())?;
    rows.reserve(groups.len())?;
    for row in 0..groups.len() {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if !values.is_null(row)? {
            rows.values.push(row);
        }
    }
    if rows.values.is_empty() {
        return Ok(());
    }
    let ordinals = index_array(rows.values.len(), false, context, |row| {
        Ok(Some(groups[rows.values[row]]))
    })?;
    let selection = index_array(rows.values.len(), false, context, |row| {
        Ok(Some(rows.values[row]))
    })?;
    let column = column.take(selection).map_err(vortex_error)?;
    let input = StructArray::try_new(
        FieldNames::from(["group", "value"]),
        vec![ordinals, column],
        rows.values.len(),
        Validity::NonNullable,
    )
    .map_err(vortex_error)?
    .into_array();
    distinct.insert_batch(
        input,
        None,
        context,
        batch_rows,
        Some(&mut |array| {
            let groups = keys(&logical_field_from_native_array(&array, "group")?, context)?;
            for row in 0..array.len() {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                let Cell::NonnegativeInteger(group) = groups.cell(row)? else {
                    return Err(failed("distinct group ordinal is invalid"));
                };
                let index = usize::try_from(group)
                    .map_err(vortex_error)?
                    .checked_mul(width)
                    .and_then(|index| index.checked_add(measure))
                    .ok_or_else(|| failed("distinct state index overflow"))?;
                let state = states
                    .get_mut(index)
                    .ok_or_else(|| failed("distinct group has no accumulator"))?;
                state.count = state
                    .count
                    .checked_add(1)
                    .ok_or_else(|| failed("distinct count overflow"))?;
            }
            Ok(())
        }),
    )?;
    Ok(())
}

fn update(
    state: &mut State,
    measure: &Measure,
    value: Option<Cell>,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    if value == Some(Cell::Null) {
        return Ok(());
    }
    state.count = state
        .count
        .checked_add(1)
        .ok_or_else(|| failed("aggregate count overflow"))?;
    match measure.function {
        Function::Count => Ok(()),
        Function::Sum | Function::Avg => {
            state.sum += number(
                value
                    .as_ref()
                    .ok_or_else(|| failed("numeric argument absent"))?,
            )?;
            if !state.sum.is_finite() {
                return Err(failed("aggregate sum became nonfinite"));
            }
            Ok(())
        }
        Function::Min | Function::Max => {
            let value = value.ok_or_else(|| failed("extremum argument absent"))?;
            let replace = match &state.extreme {
                None => true,
                Some(prior) => {
                    let order = compare_cells(value.clone(), prior.clone())?;
                    if measure.function == Function::Min {
                        order.is_lt()
                    } else {
                        order.is_gt()
                    }
                }
            };
            if replace {
                state.extreme = Some(owned_extreme(value, context)?);
            }
            Ok(())
        }
        Function::CountDistinct => Err(failed("distinct state must use the exact native set")),
    }
}

fn owned_extreme(value: Cell, context: &NativeExecutionContext<'_>) -> Result<Cell> {
    let (Cell::Utf8(bytes) | Cell::Binary(bytes)) = &value else {
        return Ok(value);
    };
    let mut owned = context
        .native_session()
        .allocator()
        .allocate(bytes.len(), Alignment::new(1))
        .map_err(vortex_error)?;
    owned.as_mut_slice().copy_from_slice(bytes.as_slice());
    Ok(if matches!(value, Cell::Binary(_)) {
        Cell::Binary(owned.freeze())
    } else {
        Cell::Utf8(owned.freeze())
    })
}

#[allow(clippy::cast_precision_loss)] // Same explicit ordered floating SUM/AVG policy as the existing aggregate runner.
fn number(value: &Cell) -> Result<f64> {
    match value {
        Cell::NegativeInteger(value) => Ok(*value as f64),
        Cell::NonnegativeInteger(value) => Ok(*value as f64),
        Cell::Float(bits) => Ok(f64::from_bits(*bits)),
        _ => Err(failed(
            "numeric aggregate requires an integer or floating value",
        )),
    }
}

fn final_value<'a>(state: &'a State, measure: &Measure) -> Result<Value<'a>> {
    Ok(match measure.function {
        Function::Count | Function::CountDistinct => Value::UInt(state.count),
        Function::Sum | Function::Avg if state.count == 0 => Value::Null,
        Function::Sum => Value::Float(state.sum),
        Function::Avg => Value::Float(super::simple_average_value(state.sum, state.count)),
        Function::Min | Function::Max => match &state.extreme {
            None | Some(Cell::Null) => Value::Null,
            Some(Cell::NegativeInteger(value) | Cell::Timestamp(value)) => Value::Int(*value),
            Some(Cell::NonnegativeInteger(value)) => {
                if matches!(
                    measure.dtype,
                    DType::Primitive(PType::I8 | PType::I16 | PType::I32 | PType::I64, _)
                ) {
                    Value::Int(i64::try_from(*value).map_err(vortex_error)?)
                } else {
                    Value::UInt(*value)
                }
            }
            Some(Cell::Float(bits)) => Value::Float(f64::from_bits(*bits)),
            Some(Cell::Boolean(value)) => Value::Bool(*value),
            Some(Cell::Utf8(bytes)) => Value::Text(Cow::Borrowed(
                std::str::from_utf8(bytes.as_slice()).map_err(vortex_error)?,
            )),
            Some(Cell::Binary(bytes)) => Value::Binary(Cow::Borrowed(bytes.as_slice())),
            Some(Cell::Decimal(value, dtype)) => Value::Decimal(*value, *dtype),
            Some(Cell::Date(value)) => Value::Int(i64::from(*value)),
        },
    })
}

#[cfg(test)]
mod ownership_tests {
    use super::*;
    use crate::resident_session::ResidentVortexSession;
    use shardloom_exec::compute_pool::CancellationToken;
    use vortex::buffer::ByteBuffer;

    #[test]
    fn native_typed_keys_binary_extremum_owns_only_selected_bytes_and_credits() {
        let source = ByteBuffer::from(vec![255u8; 2 << 20]);
        let session = ResidentVortexSession::new(1 << 20, 1).unwrap();
        let memory = session.memory().clone();
        let owned = session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                owned_extreme(Cell::Binary(source.slice(2..6)), context)
            })
            .unwrap();
        drop((source, session));
        assert!(memory.snapshot().reserved_bytes > 0);
        assert!(memory.snapshot().reserved_bytes < 1024);
        let Cell::Binary(bytes) = &owned else {
            panic!("binary logical identity changed")
        };
        assert_eq!(bytes.as_slice(), &[255; 4]);
        let retained = owned.clone();
        drop(owned);
        assert!(memory.snapshot().reserved_bytes > 0);
        drop(retained);
        assert_eq!(memory.snapshot().reserved_bytes, 0);
    }
}
