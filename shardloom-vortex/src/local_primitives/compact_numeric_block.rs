//! One exact AVG with COUNT(*) block recipe over native numeric owners.
//! Dispatch is outside the row loop; repeated counts keep their declared order.

use super::{
    AggregateDirectColumnAccessor, AggregateValueTransform, GroupedAggregateStates,
    NativeNumericOwner, Result, SimpleAggregateFunction, usize_to_u64,
};

fn inputs<'a>(
    states: &GroupedAggregateStates<'_>,
    accessors: &'a [AggregateDirectColumnAccessor],
    chunk_rows: usize,
) -> Option<(&'a NativeNumericOwner, usize)> {
    let [group] = states.group_columns.as_slice() else {
        return None;
    };
    if states.group_key_indices.as_slice() != [0]
        || !group.extra_column_indices.is_empty()
        || !matches!(group.transform, AggregateValueTransform::Identity)
        || !matches!(accessors.get(group.column_index), Some(AggregateDirectColumnAccessor::NativeNumeric(owner)) if owner.is_integer() && owner.len() == chunk_rows)
    {
        return None;
    }
    let specs = states.compact_measure_specs.as_deref()?;
    if specs.len() < 2 {
        return None;
    }
    let mut average_index = None;
    for (index, spec) in specs.iter().enumerate() {
        if !matches!(spec.value_transform, AggregateValueTransform::Identity) {
            return None;
        }
        match spec.function {
            SimpleAggregateFunction::Avg if average_index.is_none() => {
                average_index = Some(index);
            }
            SimpleAggregateFunction::Count if spec.column_index.is_none() => {}
            _ => return None,
        }
    }
    let average_index = average_index?;
    let AggregateDirectColumnAccessor::NativeNumeric(owner) =
        accessors.get(specs[average_index].column_index?)?
    else {
        return None;
    };
    (owner.len() == chunk_rows).then_some((owner, average_index))
}

pub(super) fn update(
    states: &mut GroupedAggregateStates<'_>,
    accessors: &[AggregateDirectColumnAccessor],
    rows: Option<&[usize]>,
    chunk_rows: usize,
) -> Result<bool> {
    let Some((owner, average_index)) = inputs(states, accessors, chunk_rows) else {
        return Ok(false);
    };
    let chunks = states.compact_numeric_block_chunks.checked_add(1);
    let input_rows = states
        .compact_numeric_block_input_rows
        .checked_add(usize_to_u64(rows.map_or(chunk_rows, <[usize]>::len))?);
    let (Some(chunks), Some(input_rows)) = (chunks, input_rows) else {
        return Err(super::ShardLoomError::InvalidOperation(
            "local Vortex compact numeric block work overflowed u64; no fallback execution was attempted"
                .to_owned(),
        ));
    };
    owner.update_compact_avg_count_block(states, accessors, rows, average_index)?;
    states.compact_numeric_block_chunks = chunks;
    states.compact_numeric_block_input_rows = input_rows;
    Ok(true)
}

#[cfg(test)]
#[path = "compact_numeric_block_tests.rs"]
mod tests;
