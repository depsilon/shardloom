//! Physical gather continuation for a completely validated native relation.
//! Unvalidated output keeps its original field-major error/validation windows.

use super::{ArrayRef, DType, NativeExecutionContext, Result, Sort, failed};
use crate::{local_primitives::native_payload, native_work_sizing::Owner};
use shardloom_exec::adaptive_work::{AdaptiveWorkSizer, WorkCost, WorkLimits};

pub(super) fn finish(
    sort: &Sort<'_>,
    ordinals: &[usize],
    maximum: usize,
    context: &NativeExecutionContext<'_>,
    consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
) -> Result<()> {
    if ordinals.is_empty() {
        return context.check_cancelled();
    }
    let cost = cost(&sort.spec.fields)?;
    let mut sizing = AdaptiveWorkSizer::default();
    let mut start = 0;
    while start < ordinals.len() {
        context.check_cancelled()?;
        let shape = sizing.select(
            "native_sort_output",
            context.memory().snapshot(),
            cost,
            WorkLimits {
                min_units: 1,
                max_units: (ordinals.len() - start).min(maximum) as u64,
                max_in_flight: 1,
            },
        )?;
        let rows = usize::try_from(shape.units()).map_err(|_| overflow())?;
        let output = sort.gather_rows(&ordinals[start..start + rows], context)?;
        let bytes = output.nbytes();
        #[cfg(all(test, debug_assertions))]
        super::super::native_relational_observation::record(|counts| {
            counts.sort_output_rows_constructed += rows as u64;
            counts.sort_payload_rows_validated += rows as u64;
        });
        consume(output)?;
        context.check_cancelled()?;
        context.record_adaptive_work(Owner::SortOutput, shape);
        sizing.complete(shape, Some(bytes.div_ceil(rows as u64).saturating_mul(2)));
        start += rows;
    }
    Ok(())
}

fn cost(fields: &[(String, DType)]) -> Result<WorkCost> {
    let mut cost = crate::native_work_sizing::fields_cost(fields, 2)?;
    // Selected ordinals, touched segments, offsets, chunk references and their
    // replacement overlap are new work. The full sorted ordinal vector and
    // retained source payloads already have credits in the live snapshot.
    let scratch = fields.iter().try_fold(128_u64, |bytes, (_, dtype)| {
        bytes
            .checked_add(native_payload::gather_scratch_per_row(dtype)?)
            .and_then(|bytes| bytes.checked_add(32))
            .ok_or_else(overflow)
    })?;
    cost.fixed_bytes = cost.fixed_bytes.checked_add(4096).ok_or_else(overflow)?;
    cost.bytes_per_unit = cost
        .bytes_per_unit
        .checked_add(scratch)
        .ok_or_else(overflow)?;
    Ok(cost)
}

fn overflow() -> shardloom_core::ShardLoomError {
    failed("sort output workspace size overflow")
}

#[cfg(test)]
#[path = "native_relational_sort_output_tests.rs"]
mod tests;
