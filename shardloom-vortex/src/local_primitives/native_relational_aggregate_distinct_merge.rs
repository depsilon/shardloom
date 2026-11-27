//! Exact union of range-local COUNT(DISTINCT) state. Group remapping precedes
//! membership probes; partial cardinalities are never added together.

use super::{
    Aggregate, Cell, DType, Function, Measure, NativeExecutionContext, Nullability, PType,
    ReservedVec, Result, RowSet, add_count, failed, keys, logical_field_from_native_array,
    native_payload, vortex_error,
};
use crate::local_primitives::native_relational_rows::{DistinctMeasureRows, Rows as _};

pub(super) fn admitted(measure: &Measure) -> bool {
    measure.function == Function::CountDistinct
        && measure.column.is_some()
        && measure.decimal_source.is_none()
        && measure.decimal_output.is_none()
        && measure.distinct_names == ["group", "value"]
        && matches!(
            measure.distinct_fields.as_slice(),
            [(group, DType::Primitive(PType::U64, Nullability::NonNullable)), (value, dtype)]
                if group == "group" && value == "value" && !native_payload::is_nested(dtype)
        )
}

pub(super) fn merge_scalar(
    target: &mut RowSet<'_>,
    source: &RowSet<'_>,
    context: &NativeExecutionContext<'_>,
    batch_rows: usize,
) -> Result<u64> {
    context.check_cancelled()?;
    target.release_transient_caches();
    let prior_rows = target.rows();
    let mut mapping = ReservedVec::new(context.memory())?;
    // The scalar group ordinal remains zero, so the stored native key hash is
    // still valid. Reuse the ordinary set merge and copy only new payloads.
    for (batch_start, batch) in source.table().batches() {
        target.intern_merged_batch(
            batch,
            source.index(),
            batch_start,
            context,
            batch_rows,
            &mut mapping,
        )?;
    }
    context.check_cancelled()?;
    u64::try_from(target.rows() - prior_rows).map_err(vortex_error)
}

pub(super) fn merge_grouped(
    target: &mut Aggregate<'_>,
    source: &Aggregate<'_>,
    group_mapping: &[usize],
    context: &NativeExecutionContext<'_>,
    batch_rows: usize,
) -> Result<()> {
    let width = target.spec.measures.len();
    for measure_index in 0..width {
        if target.spec.measures[measure_index].function != Function::CountDistinct
            || target.is_canonical_follower(measure_index)
        {
            continue;
        }
        context.check_cancelled()?;
        let source_set = source.distinct.values[measure_index]
            .as_ref()
            .ok_or_else(|| failed("distinct source set absent during merge"))?;
        let target_set = target.distinct.values[measure_index]
            .as_mut()
            .ok_or_else(|| failed("distinct target set absent during merge"))?;
        target_set.release_transient_caches();
        let states = &mut target.states.values;
        let mut ordinals = ReservedVec::new(context.memory())?;
        let mut rows = ReservedVec::new(context.memory())?;
        for (_, batch) in source_set.table().batches() {
            context.check_cancelled()?;
            ordinals.values.clear();
            rows.values.clear();
            ordinals.reserve(batch.len())?;
            rows.reserve(batch.len())?;
            let groups = batch
                .key_column(0)
                .ok_or_else(|| failed("distinct source group column absent during merge"))?;
            let values = batch
                .key_column(1)
                .ok_or_else(|| failed("distinct source value column absent during merge"))?;
            for row in 0..batch.len() {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                let Cell::NonnegativeInteger(group) = groups.cell(row)? else {
                    return Err(failed("distinct source group ordinal is invalid"));
                };
                let group = usize::try_from(group).map_err(vortex_error)?;
                let mapped = group_mapping
                    .get(group)
                    .ok_or_else(|| failed("distinct source group ordinal exceeds merge map"))?;
                ordinals.values.push(*mapped);
                rows.values.push(row);
            }
            let column = logical_field_from_native_array(&batch.array, "value")?;
            // Rehash only unique partial pairs after ordinal translation. The
            // borrowed row adapter retains neither duplicates nor source leases.
            let distinct_rows =
                DistinctMeasureRows::new(Some(&ordinals.values), &column, values, &rows.values);
            target_set.insert_rows(
                &distinct_rows,
                context,
                batch_rows,
                Some(&mut |array| {
                    let groups = keys(&logical_field_from_native_array(&array, "group")?, context)?;
                    for row in 0..array.len() {
                        if row.is_multiple_of(1024) {
                            context.check_cancelled()?;
                        }
                        let Cell::NonnegativeInteger(group) = groups.cell(row)? else {
                            return Err(failed("distinct merged group ordinal is invalid"));
                        };
                        let index = usize::try_from(group)
                            .map_err(vortex_error)?
                            .checked_mul(width)
                            .and_then(|index| index.checked_add(measure_index))
                            .ok_or_else(|| failed("distinct merged accumulator index overflow"))?;
                        let state = states
                            .get_mut(index)
                            .ok_or_else(|| failed("distinct merged group has no accumulator"))?;
                        add_count(state, 1)?;
                    }
                    Ok(())
                }),
            )?;
        }
    }
    context.check_cancelled()
}
