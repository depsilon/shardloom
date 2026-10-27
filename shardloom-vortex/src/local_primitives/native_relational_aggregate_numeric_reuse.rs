//! Reuse only a completed adjacent numeric update with an identical start.
//! Copy at the later measure's original position, preserving error precedence
//! and the untouched state of measures following a failed update.

use super::{Function, NumericAggregateSum, Spec, State};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use vortex::array::{
    ArrayRef,
    arrays::{Struct, struct_::StructArrayExt as _},
    dtype::Nullability,
};

pub(super) fn admit_next(
    spec: &Spec,
    index: usize,
    array: &ArrayRef,
    column: &ArrayRef,
    states: &[State],
    context: &NativeExecutionContext<'_>,
) -> Result<bool> {
    let Some(next) = spec.measures.get(index + 1) else {
        return Ok(false);
    };
    let current = &spec.measures[index];
    if !matches!(next.function, Function::Sum | Function::Avg)
        || next.decimal_source.is_some()
        || current.filter_column != next.filter_column
    {
        return Ok(false);
    }
    // Both admission and copying traverse retained groups. A growing group
    // table must not turn small incoming batches into repeated full-state work.
    if !spec.groups.is_empty() && states.len() / spec.measures.len() > array.len() / 2 {
        return Ok(false);
    }
    if current.column != next.column {
        // Only inspect already-materialized logical fields. Do not construct
        // or execute a future measure's view to prove an alias. Unavailable
        // fields retain their ordinary error at that measure's own position.
        let same_owner = array.dtype().nullability() == Nullability::NonNullable
            && array.as_opt::<Struct>().is_some_and(|fields| {
                next.column.as_deref().is_some_and(|name| {
                    fields
                        .unmasked_field_by_name(name)
                        .is_ok_and(|next| ArrayRef::ptr_eq(column, next))
                })
            });
        if !same_owner {
            return Ok(false);
        }
    }
    for (group, states) in states.chunks_exact(spec.measures.len()).enumerate() {
        if group.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        #[cfg(all(test, debug_assertions))]
        super::super::native_relational_observation::record(|counts| {
            counts.aggregate_numeric_reuse_comparison_groups += 1;
        });
        let (current, next) = (&states[index], &states[index + 1]);
        if current.count != next.count || !same_sum(current.sum, next.sum) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn same_sum(left: NumericAggregateSum, right: NumericAggregateSum) -> bool {
    match (left, right) {
        (NumericAggregateSum::Ordered(left), NumericAggregateSum::Ordered(right)) => {
            left.to_bits() == right.to_bits()
        }
        (
            NumericAggregateSum::Integer {
                sum: left,
                correction: left_correction,
            },
            NumericAggregateSum::Integer {
                sum: right,
                correction: right_correction,
            },
        ) => {
            left.to_bits() == right.to_bits()
                && left_correction.to_bits() == right_correction.to_bits()
        }
        _ => false,
    }
}

pub(super) fn copy_completed(
    states: &mut [State],
    width: usize,
    source: usize,
    target: usize,
    rows: usize,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    debug_assert_eq!(source + 1, target);
    for (group, states) in states.chunks_exact_mut(width).enumerate() {
        if group.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        states[target].count = states[source].count;
        states[target].sum = states[source].sum;
    }
    #[cfg(all(test, debug_assertions))]
    super::super::native_relational_observation::record(|counts| {
        counts.aggregate_numeric_state_reuses += 1;
        counts.aggregate_numeric_reused_rows += rows as u64;
    });
    #[cfg(not(all(test, debug_assertions)))]
    let _ = rows;
    context.check_cancelled()
}
