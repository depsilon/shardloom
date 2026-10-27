//! Select a measure's Boolean domain before preparing any of its value owners.

use super::{GroupOrdinals, Measure};
use crate::{
    local_primitives::{
        logical_field_from_native_array,
        native_capacity::ReservedVec,
        native_relational_batch::{failed, index_array},
        native_relational_expression::keys,
        native_relational_keys::Cell,
        vortex_error,
    },
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::MemoryLease;
use vortex::array::ArrayRef;

pub(super) struct Selected {
    pub(super) array: ArrayRef,
    pub(super) groups: GroupOrdinals,
    _metadata: MemoryLease,
}

pub(in crate::local_primitives) fn rows(
    array: &ArrayRef,
    filter: Option<&str>,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<ReservedVec<usize>>> {
    let Some(filter) = filter else {
        return Ok(None);
    };
    let values = keys(&logical_field_from_native_array(array, filter)?, context)?;
    let mut rows = ReservedVec::new(context.memory())?;
    rows.reserve(array.len())?;
    for row in 0..array.len() {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        match values.raw_cell(row)? {
            Cell::Boolean(true) => rows.values.push(row),
            Cell::Boolean(false) | Cell::Null => {}
            _ => return Err(failed("aggregate FILTER requires a Boolean input column")),
        }
    }
    Ok(Some(rows))
}

pub(super) fn select(
    array: &ArrayRef,
    groups: &GroupOrdinals,
    measure: &Measure,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<Selected>> {
    let Some(rows) = rows(array, measure.filter_column.as_deref(), context)? else {
        return Ok(None);
    };
    if rows.values.len() == array.len() {
        return Ok(None);
    }
    let metadata = context.memory().reserve(2048)?;
    let selected_groups = match groups {
        GroupOrdinals::Scalar(_) => GroupOrdinals::Scalar(rows.values.len()),
        GroupOrdinals::Grouped(_) => {
            let mut selected = ReservedVec::new(context.memory())?;
            selected.reserve(rows.values.len())?;
            for (offset, &row) in rows.values.iter().enumerate() {
                if offset.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                selected.values.push(groups.at(row));
            }
            GroupOrdinals::Grouped(selected)
        }
    };
    let indices = index_array(rows.values.len(), false, context, |row| {
        Ok(Some(rows.values[row]))
    })?;
    Ok(Some(Selected {
        array: array.take(indices).map_err(vortex_error)?,
        groups: selected_groups,
        _metadata: metadata,
    }))
}
