//! Ordered updates bind the state representation outside the native typed loop.

use super::super::super::{NativeNumericOwner, update_count};
use super::{
    DensePages, Extreme, FloatPolicy, Function, GroupOrdinals, KeyColumn, Kind, Mask, Measure,
    NativeExecutionContext, Result, Table, failed,
};
use crate::local_primitives::CompactI128;
use crate::local_primitives::native_relational_keys::validate_float_scalar;
use shardloom_core::{StatValue, float_policy::float_key_bits};
use std::cmp::Ordering;

#[allow(clippy::too_many_arguments, clippy::too_many_lines)] // Keep all ordered typed updates in one dispatch.
pub(super) fn consume(
    table: &mut Table,
    index: usize,
    measure: &Measure,
    ordinals: &GroupOrdinals,
    values: Option<&KeyColumn>,
    validity: Option<&Mask>,
    rows: Option<&[usize]>,
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
            rows,
            context,
        );
    }
    let Some(KeyColumn::Numeric(owner)) = values else {
        return Err(failed("bound compact aggregate input is not numeric"));
    };
    if rows.map_or(owner.len(), <[usize]>::len) != ordinals.len() {
        return Err(failed("numeric group ordinals changed row count"));
    }
    if Some(owner.ptype()) != measure.input_ptype || owner.float_policy() != table.policy {
        return Err(failed("bound compact aggregate input domain changed"));
    }
    #[cfg(all(test, debug_assertions))]
    record_bound(slot.kind, ordinals.len());
    match slot.kind {
        Kind::Integer => bind_states!(table.integers, states, {
            integer(owner, rows, ordinals, context, |group, value| {
                let state = &mut states[group * width + slot.index];
                update_count(&mut state.count, 1)?;
                let next = state
                    .sum
                    .into_i128()
                    .checked_add(value)
                    .ok_or_else(|| failed("aggregate sum overflowed i128"))?;
                state.sum = CompactI128::from_i128(next);
                Ok(())
            })
        })?,
        Kind::StrictFloat => bind_states!(table.strict, states, {
            numeric(owner, rows, ordinals, context, |group, value| {
                validate_float_scalar(value, FloatPolicy::Strict)?;
                let state = &mut states[group * width + slot.index];
                update_count(&mut state.count, 1)?;
                state.sum += value;
                finite_sum(state.sum)
            })
        })?,
        Kind::ExtendedFloat => bind_states!(table.extended, states, {
            numeric(owner, rows, ordinals, context, |group, value| {
                let state = &mut states[group * width + slot.index];
                update_count(&mut state.numeric.count, 1)?;
                if !state.specials.add(value)? {
                    state.numeric.sum += value;
                    finite_sum(state.numeric.sum)?;
                }
                Ok(())
            })
        })?,
        Kind::SignedExtreme => bind_states!(table.signed_extrema, states, {
            extremum(owner, rows, ordinals, context, |group, value| {
                let StatValue::Int64(value) = value else {
                    return Err(failed("signed extremum domain changed"));
                };
                update_extreme(
                    &mut states[group * width + slot.index],
                    value,
                    measure.function == Function::Min,
                    |left, right| left.cmp(&right),
                )
            })
        })?,
        Kind::UnsignedExtreme => bind_states!(table.unsigned_extrema, states, {
            extremum(owner, rows, ordinals, context, |group, value| {
                let StatValue::UInt64(value) = value else {
                    return Err(failed("unsigned extremum domain changed"));
                };
                update_extreme(
                    &mut states[group * width + slot.index],
                    value,
                    measure.function == Function::Min,
                    |left, right| left.cmp(&right),
                )
            })
        })?,
        Kind::FloatExtreme => bind_states!(table.float_extrema, states, {
            extremum(owner, rows, ordinals, context, |group, value| {
                let StatValue::Float64(value) = value else {
                    return Err(failed("floating extremum domain changed"));
                };
                validate_float_scalar(value, table.policy)?;
                update_extreme(
                    &mut states[group * width + slot.index],
                    value,
                    measure.function == Function::Min,
                    compare_float,
                )
            })
        })?,
        Kind::Count => unreachable!("COUNT was consumed before numeric binding"),
    }
    context.check_cancelled()
}

#[cfg(all(test, debug_assertions))]
fn record_bound(kind: Kind, rows: usize) {
    if rows != 0 {
        crate::local_primitives::native_relational_observation::record(|counts| {
            if matches!(
                kind,
                Kind::Integer | Kind::StrictFloat | Kind::ExtendedFloat
            ) {
                counts.aggregate_numeric_bound_batches += 1;
                counts.aggregate_numeric_bound_rows += rows as u64;
            } else {
                counts.aggregate_extrema_bound_batches += 1;
                counts.aggregate_extrema_bound_rows += rows as u64;
            }
        });
    }
}

#[allow(clippy::too_many_arguments)]
fn count(
    pages: &mut DensePages<u64>,
    width: usize,
    index: usize,
    column: bool,
    ordinals: &GroupOrdinals,
    validity: Option<&Mask>,
    rows: Option<&[usize]>,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    bind_states!(*pages, states, {
        for (row, group) in ordinals.iter() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if column {
                if !validity
                    .ok_or_else(|| failed("COUNT validity absent"))?
                    .value(rows.map_or(row, |rows| rows[row]))
                {
                    continue;
                }
            } else {
                #[cfg(all(test, debug_assertions))]
                crate::local_primitives::native_relational_observation::record(|counts| {
                    counts.aggregate_row_updates += 1;
                });
            }
            update_count(&mut states[group * width + index], 1)?;
            #[cfg(all(test, debug_assertions))]
            crate::local_primitives::native_relational_observation::record_count_update(1);
        }
    });
    context.check_cancelled()
}

fn finite_sum(value: f64) -> Result<()> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(failed("aggregate sum became nonfinite"))
    }
}

fn integer(
    owner: &NativeNumericOwner,
    rows: Option<&[usize]>,
    ordinals: &GroupOrdinals,
    context: &NativeExecutionContext<'_>,
    mut update: impl FnMut(usize, i128) -> Result<()>,
) -> Result<()> {
    let visit = |row: usize, value| {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if let Some(value) = value {
            update(ordinals.at(row), value)?;
        }
        Ok(())
    };
    match rows {
        Some(rows) => owner.visit_selected_integer(rows, visit),
        None => owner.visit_integer(visit),
    }
}

fn numeric(
    owner: &NativeNumericOwner,
    rows: Option<&[usize]>,
    ordinals: &GroupOrdinals,
    context: &NativeExecutionContext<'_>,
    mut update: impl FnMut(usize, f64) -> Result<()>,
) -> Result<()> {
    let visit = |row: usize, value, _| {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if let Some(value) = value {
            update(ordinals.at(row), value)?;
        }
        Ok(())
    };
    match rows {
        Some(rows) => owner.visit_selected_numeric(rows, visit),
        None => owner.visit_numeric(visit),
    }
}

fn extremum(
    owner: &NativeNumericOwner,
    rows: Option<&[usize]>,
    ordinals: &GroupOrdinals,
    context: &NativeExecutionContext<'_>,
    mut update: impl FnMut(usize, StatValue) -> Result<()>,
) -> Result<()> {
    let visit = |row: usize, value| {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if !matches!(value, StatValue::Null) {
            update(ordinals.at(row), value)?;
        }
        Ok(())
    };
    match rows {
        Some(rows) => owner.visit_selected_values(rows, visit),
        None => owner.visit_values(visit),
    }
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
    update_count(&mut state.count, 1)?;
    set_extreme_at_count(state, value, minimum, compare, state.count);
    Ok(())
}

/// The caller has either checked this increment or proved complete-batch
/// headroom and equal paired counts. The next count is therefore nonzero.
pub(super) fn set_extreme_at_count<T: Copy>(
    state: &mut Extreme<T>,
    value: T,
    minimum: bool,
    compare: impl FnOnce(T, T) -> Ordering,
    count: u64,
) {
    state.count = count;
    let replace = count == 1 || {
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
}
