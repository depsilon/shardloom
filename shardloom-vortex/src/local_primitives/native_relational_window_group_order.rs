//! Stable native partition membership before partition-local ordering.

use super::{Group, Window, native_relational_order};
use crate::{
    local_primitives::{native_capacity::ReservedVec, native_relational_index::RowIndex},
    resident_session::NativeExecutionContext,
};
use shardloom_core::{Result, ShardLoomError};
use std::{cmp::Ordering, hash::Hasher};

#[cfg(test)]
#[path = "native_relational_window_partition_tests.rs"]
mod tests;

#[derive(Clone, Copy)]
struct Partition {
    entry: usize,
    first: usize,
}

struct Partitions {
    index: RowIndex,
    groups: ReservedVec<Partition>,
}

/// The caller has validated all keys and this group's complete NULL policy.
/// Temporary membership is retired before the caller starts frame evaluation.
pub(super) fn order(
    window: &Window<'_>,
    group: &Group,
    ordinals: &mut ReservedVec<usize>,
    context: &NativeExecutionContext<'_>,
) -> Result<bool> {
    if group.partition.is_empty() || ordinals.values.len() < 2 {
        return Ok(false);
    }
    if native_relational_order::preserve_order_with_evidence(
        &ordinals.values,
        context,
        |left, right| window.compare_row(group, left, right),
    )? {
        return Ok(true);
    }
    let denied_before = context.memory().snapshot().denied_reservations;
    let mut partitions = match collect(window, group, &ordinals.values, context) {
        Ok(partitions) => partitions,
        Err(ShardLoomError::InvalidOperation(message))
            if message.starts_with("memory reservation denied:")
                && context.memory().snapshot().denied_reservations > denied_before =>
        {
            // Collection has not changed the original ordinal workspace. Its
            // temporary leases drop before the original native sort is used.
            context.check_cancelled()?;
            return Ok(false);
        }
        Err(error) => return Err(error),
    };
    native_relational_order::sort_with_order_evidence(
        &mut partitions.groups.values,
        context,
        true,
        |left, right| {
            Ok(window
                .compare_partition(group, left.first, right.first)?
                .then(left.first.cmp(&right.first)))
        },
    )?;
    let expected = ordinals.values.len();
    ordinals.values.clear();
    for partition in &partitions.groups.values {
        context.check_cancelled()?;
        let start = ordinals.values.len();
        for row in partitions.index.rows(partition.entry)? {
            if ordinals.values.len().is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            // The original workspace already reserves every input ordinal.
            // Exact membership emits each one once, preserving source ties.
            ordinals.values.push(row);
        }
        if ordinals.values.len() - start > 1 {
            native_relational_order::sort_with_order_evidence(
                &mut ordinals.values[start..],
                context,
                true,
                |left, right| {
                    Ok(window
                        .compare_order(group, left, right)?
                        .then(left.cmp(&right)))
                },
            )?;
        }
    }
    debug_assert_eq!(ordinals.values.len(), expected);
    context.check_cancelled()?;
    Ok(true)
}

fn collect(
    window: &Window<'_>,
    group: &Group,
    ordinals: &[usize],
    context: &NativeExecutionContext<'_>,
) -> Result<Partitions> {
    let mut index = RowIndex::new(context.memory())?;
    let mut groups = ReservedVec::new(context.memory())?;
    for &row in ordinals {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        let mut hash = rustc_hash::FxHasher::default();
        for &key in &group.partition {
            if let Some(value) = window.table.hash_key(row, key)? {
                hash.write_u8(1);
                hash.write_u64(value);
            } else {
                hash.write_u8(0);
            }
        }
        let entry = index.insert(hash.finish(), row, context.cancellation(), |old| {
            Ok(window.compare_partition(group, old, row)? == Ordering::Equal)
        })?;
        if entry == groups.values.len() {
            groups.push(Partition { entry, first: row })?;
        }
    }
    Ok(Partitions { index, groups })
}
