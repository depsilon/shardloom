//! Pair only already-prepared, infallibly comparable extrema with count headroom.
//! The ordinary measure loop retains every decline and its original error order.

use super::super::super::{NativeNumericOwner, extrema_pair};
use super::{
    ArrayRef, Extreme, FloatPolicy, GroupOrdinals, Kind, NativeExecutionContext, Result, Spec,
    Table, failed, updates,
};
use shardloom_core::StatValue;
use std::ops::IndexMut;

#[allow(clippy::too_many_arguments)] // Existing prepared owner, proof and group state have distinct lifetimes.
pub(super) fn accumulate(
    table: &mut Table,
    spec: &Spec,
    index: usize,
    ordinals: &GroupOrdinals,
    source: &ArrayRef,
    owner: &NativeNumericOwner,
    completed_policy: Option<FloatPolicy>,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<usize>> {
    context.check_cancelled()?;
    if table.groups > owner.len() / 2
        || table.policy != context.float_policy()
        || owner.float_policy() != table.policy
    {
        return Ok(None);
    }
    let Some(pair) =
        extrema_pair::admit_inputs(spec, index, ordinals, source, owner, completed_policy)
    else {
        return Ok(None);
    };
    let (first, second) = (table.slots[index], table.slots[index + 1]);
    if first.kind != second.kind
        || !matches!(
            first.kind,
            Kind::SignedExtreme | Kind::UnsignedExtreme | Kind::FloatExtreme
        )
    {
        return Ok(None);
    }
    let Ok(rows) = u64::try_from(owner.len()) else {
        return Ok(None);
    };
    let max_start = u64::MAX - rows;
    for group in 0..table.groups {
        if group.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        #[cfg(all(test, debug_assertions))]
        crate::local_primitives::native_relational_observation::record(|counts| {
            if owner.is_integer() {
                counts.aggregate_integer_extrema_admission_groups += 1;
            } else {
                counts.aggregate_float_extrema_admission_groups += 1;
            }
        });
        let left = table.state(index, group).count;
        if left > max_start
            || left != table.state(index + 1, group).count
            || (pair.count && left != table.state(index + 2, group).count)
        {
            return Ok(None);
        }
    }
    // Bound type/policy and completed proof rule out value errors. All counts
    // have room, with no allocation or future mask execution after admission.
    #[cfg(all(test, debug_assertions))]
    record_pair(owner.is_integer(), rows, pair.count);
    let width = table.widths[first.kind as usize];
    let count_width = table.widths[Kind::Count as usize];
    let count = pair.count.then(|| table.slots[index + 2].index);
    macro_rules! visit {
        ($pages:expr, $variant:ident, $compare:expr) => {
            bind_states!($pages, states, {
                bind_states!(table.counts, counts, {
                    owner.visit_values(|row, value| {
                        if row.is_multiple_of(1024) {
                            context.check_cancelled()?;
                        }
                        if matches!(value, StatValue::Null) {
                            return Ok(());
                        }
                        let StatValue::$variant(value) = value else {
                            return Err(failed("bound paired extremum input domain changed"));
                        };
                        let group = ordinals.at(row);
                        let total = update_pair(
                            states,
                            group * width + first.index,
                            group * width + second.index,
                            value,
                            pair.first_is_minimum,
                            $compare,
                        );
                        if let Some(count) = count {
                            counts[group * count_width + count] = total;
                        }
                        #[cfg(all(test, debug_assertions))]
                        crate::local_primitives::native_relational_observation::record(|counts| {
                            counts.aggregate_typed_paired_count_proof_rows += 1;
                        });
                        Ok(())
                    })
                })
            })
        };
    }
    match first.kind {
        Kind::SignedExtreme => visit!(table.signed_extrema, Int64, |left, right| left.cmp(&right)),
        Kind::UnsignedExtreme => {
            visit!(table.unsigned_extrema, UInt64, |left, right| left
                .cmp(&right))
        }
        Kind::FloatExtreme => visit!(table.float_extrema, Float64, updates::compare_float),
        _ => unreachable!("paired extrema kinds admitted before traversal"),
    }?;
    context.check_cancelled()?;
    Ok(Some(pair.end))
}

fn update_pair<T: Copy, S: IndexMut<usize, Output = Extreme<T>> + ?Sized>(
    states: &mut S,
    first: usize,
    second: usize,
    value: T,
    minimum: bool,
    compare: impl Fn(T, T) -> std::cmp::Ordering,
) -> u64 {
    // Admission proved equal starting counts and room for every incoming row.
    // This traversal touches each input row once; NULL rows return before here.
    let total = states[first].count + 1;
    updates::set_extreme_at_count(&mut states[first], value, minimum, &compare, total);
    updates::set_extreme_at_count(&mut states[second], value, !minimum, compare, total);
    total
}

#[cfg(all(test, debug_assertions))]
fn record_pair(integer: bool, rows: u64, count: bool) {
    crate::local_primitives::native_relational_observation::record(|counts| {
        let saved = rows * if count { 2 } else { 1 };
        if integer {
            counts.aggregate_integer_extrema_fused_batches += 1;
            counts.aggregate_integer_extrema_fused_rows += rows;
            counts.aggregate_integer_extrema_saved_measure_rows += saved;
        } else {
            counts.aggregate_float_extrema_fused_batches += 1;
            counts.aggregate_float_extrema_fused_rows += rows;
            counts.aggregate_float_extrema_saved_measure_rows += saved;
        }
    });
}
