//! Ordered updates bind the state representation outside the native typed loop.

use super::super::super::{NativeNumericOwner, update_count};
use super::{
    DensePages, Extreme, FloatPolicy, Function, GroupOrdinals, KeyColumn, Kind, Mask, Measure,
    NativeExecutionContext, NumericAggregateSum, Result, Table, failed,
};
use crate::local_primitives::native_relational_keys::validate_float_scalar;
use shardloom_core::{StatValue, float_policy::float_key_bits};
use std::cmp::Ordering;

pub(super) fn consume(
    table: &mut Table,
    index: usize,
    measure: &Measure,
    ordinals: &GroupOrdinals,
    values: Option<&KeyColumn>,
    validity: Option<&Mask>,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    let slot = table.slots[index];
    let width = table.widths[slot.kind as usize];
    if slot.kind == Kind::Count {
        return count(
            &mut table.counts,
            width,
            slot.index,
            measure.column.is_some(),
            ordinals,
            validity,
            context,
        );
    }
    let Some(KeyColumn::Numeric(owner)) = values else {
        return Err(failed("bound compact aggregate input is not numeric"));
    };
    if owner.len() != ordinals.len() {
        return Err(failed("numeric group ordinals changed row count"));
    }
    if Some(owner.ptype()) != measure.input_ptype || owner.float_policy() != table.policy {
        return Err(failed("bound compact aggregate input domain changed"));
    }
    #[cfg(all(test, debug_assertions))]
    if owner.len() != 0 {
        crate::local_primitives::native_relational_observation::record(|counts| {
            if matches!(
                slot.kind,
                Kind::Integer | Kind::StrictFloat | Kind::ExtendedFloat
            ) {
                counts.aggregate_numeric_bound_batches += 1;
                counts.aggregate_numeric_bound_rows += owner.len() as u64;
            } else {
                counts.aggregate_extrema_bound_batches += 1;
                counts.aggregate_extrema_bound_rows += owner.len() as u64;
            }
        });
    }
    match slot.kind {
        Kind::Integer => numeric(owner, ordinals, context, |group, value| {
            let state = &mut table.integers[group * width + slot.index];
            update_count(&mut state.count, 1)?;
            NumericAggregateSum::add_integer_parts(&mut state.sum, &mut state.correction, value);
            finite_sum(state.sum + state.correction)
        })?,
        Kind::StrictFloat => numeric(owner, ordinals, context, |group, value| {
            validate_float_scalar(value, FloatPolicy::Strict)?;
            let state = &mut table.strict[group * width + slot.index];
            update_count(&mut state.count, 1)?;
            state.sum += value;
            finite_sum(state.sum)
        })?,
        Kind::ExtendedFloat => numeric(owner, ordinals, context, |group, value| {
            let state = &mut table.extended[group * width + slot.index];
            update_count(&mut state.numeric.count, 1)?;
            if !state.specials.add(value)? {
                state.numeric.sum += value;
                finite_sum(state.numeric.sum)?;
            }
            Ok(())
        })?,
        Kind::SignedExtreme => extremum(owner, ordinals, context, |group, value| {
            let StatValue::Int64(value) = value else {
                return Err(failed("signed extremum domain changed"));
            };
            update_extreme(
                &mut table.signed_extrema[group * width + slot.index],
                value,
                measure.function == Function::Min,
                |left, right| left.cmp(&right),
            )
        })?,
        Kind::UnsignedExtreme => extremum(owner, ordinals, context, |group, value| {
            let StatValue::UInt64(value) = value else {
                return Err(failed("unsigned extremum domain changed"));
            };
            update_extreme(
                &mut table.unsigned_extrema[group * width + slot.index],
                value,
                measure.function == Function::Min,
                |left, right| left.cmp(&right),
            )
        })?,
        Kind::FloatExtreme => extremum(owner, ordinals, context, |group, value| {
            let StatValue::Float64(value) = value else {
                return Err(failed("floating extremum domain changed"));
            };
            validate_float_scalar(value, table.policy)?;
            update_extreme(
                &mut table.float_extrema[group * width + slot.index],
                value,
                measure.function == Function::Min,
                compare_float,
            )
        })?,
        Kind::Count => unreachable!("COUNT was consumed before numeric binding"),
    }
    context.check_cancelled()
}

fn count(
    pages: &mut DensePages<u64>,
    width: usize,
    index: usize,
    column: bool,
    ordinals: &GroupOrdinals,
    validity: Option<&Mask>,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    for (row, group) in ordinals.iter() {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if column {
            if !validity
                .ok_or_else(|| failed("COUNT validity absent"))?
                .value(row)
            {
                continue;
            }
        } else {
            #[cfg(all(test, debug_assertions))]
            crate::local_primitives::native_relational_observation::record(|counts| {
                counts.aggregate_row_updates += 1;
            });
        }
        update_count(&mut pages[group * width + index], 1)?;
    }
    context.check_cancelled()
}

fn finite_sum(value: f64) -> Result<()> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(failed("aggregate sum became nonfinite"))
    }
}

fn numeric(
    owner: &NativeNumericOwner,
    ordinals: &GroupOrdinals,
    context: &NativeExecutionContext<'_>,
    mut update: impl FnMut(usize, f64) -> Result<()>,
) -> Result<()> {
    owner.visit_numeric(|row, value, _| {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if let Some(value) = value {
            update(ordinals.at(row), value)?;
        }
        Ok(())
    })
}

fn extremum(
    owner: &NativeNumericOwner,
    ordinals: &GroupOrdinals,
    context: &NativeExecutionContext<'_>,
    mut update: impl FnMut(usize, StatValue) -> Result<()>,
) -> Result<()> {
    owner.visit_values(|row, value| {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if !matches!(value, StatValue::Null) {
            update(ordinals.at(row), value)?;
        }
        Ok(())
    })
}

pub(super) fn compare_float(left: f64, right: f64) -> Ordering {
    f64::from_bits(float_key_bits(left)).total_cmp(&f64::from_bits(float_key_bits(right)))
}

pub(super) fn update_extreme<T: Copy>(
    state: &mut Extreme<T>,
    value: T,
    minimum: bool,
    compare: impl FnOnce(T, T) -> Ordering,
) -> Result<()> {
    let empty = state.count == 0;
    update_count(&mut state.count, 1)?;
    let replace = empty || {
        let order = compare(value, state.value);
        if minimum {
            order.is_lt()
        } else {
            order.is_gt()
        }
    };
    if replace {
        state.value = value;
    }
    Ok(())
}
