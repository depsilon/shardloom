//! Ordered numeric MIN/MAX over the existing original-width value owner.
//! Only a replacement constructs a retained scalar cell. Integer comparisons
//! stay exact; equal floating zeros preserve the first observed value's bits.

use super::super::native_relational_keys::validate_float_scalar;
use super::{
    Cell, GroupOrdinals, NativeExecutionContext, NativeNumericOwner, Result, State, add_count,
    compare_cells, failed,
};
use shardloom_core::{FloatPolicy, StatValue, float_policy::float_key_bits};
use std::cmp::Ordering;

#[cfg(test)]
#[path = "native_relational_aggregate_extrema_tests.rs"]
mod tests;

pub(super) fn accumulate(
    states: &mut [State],
    ordinals: &GroupOrdinals,
    width: usize,
    measure_index: usize,
    minimum: bool,
    owner: &NativeNumericOwner,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    if owner.len() != ordinals.len() {
        return Err(failed("numeric group ordinals changed row count"));
    }
    #[cfg(all(test, debug_assertions))]
    if owner.len() != 0 {
        super::super::native_relational_observation::record(|counts| {
            counts.aggregate_extrema_bound_batches += 1;
            counts.aggregate_extrema_bound_rows += owner.len() as u64;
        });
    }
    owner.visit_values(|row, value| {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        update(
            &mut states[ordinals.at(row) * width + measure_index],
            &value,
            minimum,
            context.float_policy(),
        )
    })
}

fn update(state: &mut State, value: &StatValue, minimum: bool, policy: FloatPolicy) -> Result<()> {
    if matches!(value, StatValue::Null) {
        return Ok(());
    }
    if let StatValue::Float64(value) = value {
        // Preserve raw_cell's validation before changing the count or extremum.
        validate_float_scalar(*value, policy)?;
    }
    add_count(state, 1)?;
    update_extreme(state, value, minimum)
}

/// Compare and replace a present value after the caller establishes its domain.
/// The paired integer path shares this exact comparator without recounting it.
pub(super) fn update_extreme(state: &mut State, value: &StatValue, minimum: bool) -> Result<()> {
    let replace = match &state.extreme {
        None => true,
        Some(prior) => {
            let order = compare(value, prior)?;
            if minimum {
                order.is_lt()
            } else {
                order.is_gt()
            }
        }
    };
    if replace {
        state.extreme = Some(cell(value)?);
    }
    Ok(())
}

fn compare(value: &StatValue, prior: &Cell) -> Result<Ordering> {
    // The common physical domains compare registers without cloning generic
    // cells. The existing exact mixed-domain comparator remains authoritative
    // when a later admitted batch uses a different numeric representation.
    Ok(match (value, prior) {
        (StatValue::Int64(value), Cell::NegativeInteger(prior)) => value.cmp(prior),
        (StatValue::Int64(value), Cell::NonnegativeInteger(prior)) => {
            i128::from(*value).cmp(&i128::from(*prior))
        }
        (StatValue::UInt64(value), Cell::NegativeInteger(prior)) => {
            i128::from(*value).cmp(&i128::from(*prior))
        }
        (StatValue::UInt64(value), Cell::NonnegativeInteger(prior)) => value.cmp(prior),
        (StatValue::Float64(value), Cell::Float(prior)) => {
            let value = f64::from_bits(float_key_bits(*value));
            let prior = f64::from_bits(float_key_bits(f64::from_bits(*prior)));
            value.total_cmp(&prior)
        }
        _ => compare_cells(cell(value)?, prior.clone())?,
    })
}

fn cell(value: &StatValue) -> Result<Cell> {
    Ok(match value {
        StatValue::Int64(value) if *value < 0 => Cell::NegativeInteger(*value),
        StatValue::Int64(value) => Cell::NonnegativeInteger(value.cast_unsigned()),
        StatValue::UInt64(value) => Cell::NonnegativeInteger(*value),
        StatValue::Float64(value) => Cell::Float(value.to_bits()),
        _ => return Err(failed("numeric extremum requires a numeric value")),
    })
}
