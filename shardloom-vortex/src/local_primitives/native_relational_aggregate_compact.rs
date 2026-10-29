//! Paged additive state for large grouped reductions. Only completed state is
//! migrated; key ordinals and each measure's ordered update semantics stay intact.

use super::{
    Function, GroupOrdinals, Measure, NumericAggregateSum, Spec, State, Value, failed,
    numeric_reuse, update_count, update_numeric_parts, update_sum_parts,
};
use crate::{
    local_primitives::{
        aggregate_dense_pages::DensePages,
        native_capacity::ReservedVec,
        native_relational_keys::{Cell, KeyColumn},
    },
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use vortex::{array::ArrayRef, mask::Mask};

#[cfg(test)]
#[path = "native_relational_aggregate_compact_state_tests.rs"]
mod tests;

// Small reductions keep the existing contiguous representation and consumers.
// Beyond this charged generic payload, page growth avoids whole-state overlap.
const MIN_GENERIC_BYTES: usize = 1 << 20;

#[derive(Clone, Copy, Default)]
struct Numeric {
    count: u64,
    sum: NumericAggregateSum,
    specials: super::super::native_float_total::Specials,
}

#[derive(Clone, Copy)]
enum Slot {
    Count(usize),
    Numeric(usize),
}

pub(super) struct Table {
    slots: ReservedVec<Slot>,
    count_width: usize,
    numeric_width: usize,
    counts: DensePages<u64>,
    numbers: DensePages<Numeric>,
    groups: usize,
}

impl Table {
    pub(super) fn promote(
        spec: &Spec,
        states: &[State],
        groups: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<Self>> {
        let count = groups
            .checked_mul(spec.measures.len())
            .ok_or_else(|| failed("aggregate state cardinality overflow"))?;
        let bytes = count
            .checked_mul(size_of::<State>())
            .ok_or_else(|| failed("aggregate state capacity overflow"))?;
        if bytes <= MIN_GENERIC_BYTES
            || spec.groups.is_empty()
            || spec.measures.is_empty()
            || spec.measures.iter().any(|measure| {
                measure.decimal_source.is_some()
                    || !matches!(
                        measure.function,
                        Function::Count | Function::Sum | Function::Avg
                    )
            })
        {
            return Ok(None);
        }
        if !states.len().is_multiple_of(spec.measures.len()) || states.len() > count {
            return Err(failed(
                "compact aggregate migration has inconsistent groups",
            ));
        }
        let mut slots = ReservedVec::new(context.memory())?;
        slots.reserve(spec.measures.len())?;
        let mut count_width = 0;
        let mut numeric_width = 0;
        for measure in &spec.measures {
            slots.values.push(if measure.function == Function::Count {
                let slot = Slot::Count(count_width);
                count_width += 1;
                slot
            } else {
                let slot = Slot::Numeric(numeric_width);
                numeric_width += 1;
                slot
            });
        }
        let mut table = Self {
            slots,
            count_width,
            numeric_width,
            counts: DensePages::new(context.memory())?,
            numbers: DensePages::new(context.memory())?,
            groups: 0,
        };
        // Keep every old state and its lease alive until migration completes.
        table.reserve_groups(groups, context)?;
        for (group, old) in states.chunks_exact(spec.measures.len()).enumerate() {
            if group.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            for (slot, old) in table.slots.values.iter().zip(old) {
                match *slot {
                    Slot::Count(index) => table.counts[group * count_width + index] = old.count,
                    Slot::Numeric(index) => {
                        table.numbers[group * numeric_width + index] = Numeric {
                            count: old.count,
                            sum: old.sum,
                            specials: old.specials,
                        }
                    }
                }
            }
        }
        context.check_cancelled()?;
        Ok(Some(table))
    }

    pub(super) fn reserve_groups(
        &mut self,
        groups: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        if groups < self.groups {
            return Err(failed("compact aggregate group count decreased"));
        }
        grow(&mut self.counts, groups, self.count_width, context)?;
        grow(&mut self.numbers, groups, self.numeric_width, context)?;
        self.groups = groups;
        Ok(())
    }

    pub(super) fn admit_next(
        &self,
        spec: &Spec,
        index: usize,
        array: &ArrayRef,
        column: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<bool> {
        if !numeric_reuse::same_input(spec, index, array, column, self.groups) {
            return Ok(false);
        }
        let (Slot::Numeric(current), Slot::Numeric(next)) =
            (self.slots.values[index], self.slots.values[index + 1])
        else {
            return Ok(false);
        };
        for group in 0..self.groups {
            if group.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            #[cfg(all(test, debug_assertions))]
            super::super::native_relational_observation::record(|counts| {
                counts.aggregate_numeric_reuse_comparison_groups += 1;
            });
            let current = self.numbers[group * self.numeric_width + current];
            let next = self.numbers[group * self.numeric_width + next];
            if current.count != next.count
                || current.specials != next.specials
                || !numeric_reuse::same_sum(current.sum, next.sum)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn copy_completed(
        &mut self,
        source: usize,
        target: usize,
        rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let (Slot::Numeric(source), Slot::Numeric(target)) =
            (self.slots.values[source], self.slots.values[target])
        else {
            return Err(failed("compact numeric reuse has incompatible slots"));
        };
        for group in 0..self.groups {
            if group.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            self.numbers[group * self.numeric_width + target] =
                self.numbers[group * self.numeric_width + source];
        }
        numeric_reuse::record_copy(rows, context)
    }

    pub(super) fn consume(
        &mut self,
        index: usize,
        measure: &Measure,
        ordinals: &GroupOrdinals,
        values: Option<&KeyColumn>,
        validity: Option<&Mask>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        match self.slots.values[index] {
            Slot::Count(index) => {
                for (row, group) in ordinals.iter() {
                    if row.is_multiple_of(1024) {
                        context.check_cancelled()?;
                    }
                    if measure.column.is_some() {
                        if !validity
                            .ok_or_else(|| failed("COUNT validity absent"))?
                            .value(row)
                        {
                            continue;
                        }
                    } else {
                        #[cfg(all(test, debug_assertions))]
                        super::super::native_relational_observation::record(|counts| {
                            counts.aggregate_row_updates += 1;
                        });
                    }
                    update_count(&mut self.counts[group * self.count_width + index], 1)?;
                }
            }
            Slot::Numeric(index) => {
                if let Some(KeyColumn::Numeric(owner)) = values {
                    if owner.len() != ordinals.len() {
                        return Err(failed("numeric group ordinals changed row count"));
                    }
                    if owner.len() != 0 {
                        #[cfg(all(test, debug_assertions))]
                        super::super::native_relational_observation::record(|counts| {
                            counts.aggregate_numeric_bound_batches += 1;
                            counts.aggregate_numeric_bound_rows += owner.len() as u64;
                        });
                    }
                    owner.visit_numeric(|row, value, integer| {
                        if row.is_multiple_of(1024) {
                            context.check_cancelled()?;
                        }
                        if let Some(value) = value {
                            let state =
                                &mut self.numbers[ordinals.at(row) * self.numeric_width + index];
                            update_numeric_parts(
                                &mut state.count,
                                &mut state.sum,
                                &mut state.specials,
                                value,
                                integer,
                                context.float_policy(),
                            )?;
                        }
                        Ok(())
                    })?;
                } else {
                    for (row, group) in ordinals.iter() {
                        if row.is_multiple_of(1024) {
                            context.check_cancelled()?;
                        }
                        let value = values.map(|values| values.raw_cell(row)).transpose()?;
                        #[cfg(all(test, debug_assertions))]
                        super::super::native_relational_observation::record(|counts| {
                            counts.aggregate_row_updates += 1;
                        });
                        if value == Some(Cell::Null) {
                            continue;
                        }
                        let state = &mut self.numbers[group * self.numeric_width + index];
                        update_count(&mut state.count, 1)?;
                        update_sum_parts(
                            &mut state.sum,
                            &mut state.specials,
                            value.as_ref(),
                            context.float_policy(),
                        )?;
                    }
                }
            }
        }
        context.check_cancelled()
    }

    pub(super) fn finish(
        &self,
        index: usize,
        group: usize,
        function: Function,
    ) -> Result<Value<'static>> {
        match self.slots.values[index] {
            Slot::Count(index) => Ok(Value::UInt(self.counts[group * self.count_width + index])),
            Slot::Numeric(index) => {
                let state = &self.numbers[group * self.numeric_width + index];
                if state.count == 0 {
                    return Ok(Value::Null);
                }
                if let Some(value) = state.specials.result() {
                    return Ok(Value::Float(value));
                }
                match function {
                    Function::Sum => Ok(Value::Float(state.sum.total())),
                    Function::Avg => Ok(Value::Float(super::super::simple_average_value(
                        state.sum.total(),
                        state.count,
                    ))),
                    _ => Err(failed("compact aggregate result has incompatible measure")),
                }
            }
        }
    }
}

fn grow<T: Copy + Default>(
    values: &mut DensePages<T>,
    groups: usize,
    width: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    let count = groups
        .checked_mul(width)
        .ok_or_else(|| failed("compact aggregate state cardinality overflow"))?;
    while values.len() < count {
        if values.len().is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if !values.reserve_one(context.memory(), failed)? {
            return Err(failed(
                "compact grouped aggregate state capacity could not be admitted",
            ));
        }
        values.push(T::default());
    }
    context.check_cancelled()
}
