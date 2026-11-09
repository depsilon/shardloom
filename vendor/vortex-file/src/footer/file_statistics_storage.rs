// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 Dylan Justin Heinrich
//! Shared, admitted statistics tables. Scalar and schema payloads retain their
//! own owners; copying an individual entry creates caller-owned data.

use std::alloc::Layout;
use std::sync::Arc;

use vortex_array::dtype::{DType, Field, FieldPath};
use vortex_array::memory::{HostMemoryReservation, HostMemorySessionExt};
use vortex_array::stats::StatsSet;
use vortex_error::{VortexResult, vortex_err};
use vortex_layout::layouts::file_stats::{AggregateStat, AggregateStats};
use vortex_session::VortexSession;

#[derive(Clone, Debug)]
pub(super) enum StatisticsStorage {
    CallerOwned {
        aggregates: Arc<[AggregateStats]>,
        dtypes: Arc<[DType]>,
        paths: Arc<[FieldPath]>,
        legacy: Arc<[StatsSet]>,
    },
    Admitted(Arc<OwnedStatistics>),
}

#[derive(Debug)]
pub(super) struct OwnedStatistics {
    pub aggregates: Vec<AggregateStats>,
    pub dtypes: Vec<DType>,
    pub paths: Vec<FieldPath>,
    pub legacy: Vec<StatsSet>,
    // Tables, path vectors and aggregate vectors drop before their admission.
    pub _reservation: Option<HostMemoryReservation>,
}

impl StatisticsStorage {
    pub fn aggregates(&self) -> &[AggregateStats] {
        match self {
            Self::CallerOwned { aggregates, .. } => aggregates,
            Self::Admitted(owner) => &owner.aggregates,
        }
    }

    pub fn dtypes(&self) -> &[DType] {
        match self {
            Self::CallerOwned { dtypes, .. } => dtypes,
            Self::Admitted(owner) => &owner.dtypes,
        }
    }

    pub fn paths(&self) -> &[FieldPath] {
        match self {
            Self::CallerOwned { paths, .. } => paths,
            Self::Admitted(owner) => &owner.paths,
        }
    }

    pub fn legacy(&self) -> &[StatsSet] {
        match self {
            Self::CallerOwned { legacy, .. } => legacy,
            Self::Admitted(owner) => &owner.legacy,
        }
    }
}

fn bytes<T>(count: usize) -> VortexResult<usize> {
    Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| vortex_err!("File statistics allocation size overflow"))
}

fn add(left: usize, right: usize) -> VortexResult<usize> {
    left.checked_add(right)
        .filter(|total| *total <= isize::MAX as usize)
        .ok_or_else(|| vortex_err!("File statistics allocation size overflow"))
}

fn multiply(left: usize, right: usize) -> VortexResult<usize> {
    left.checked_mul(right)
        .ok_or_else(|| vortex_err!("File statistics allocation size overflow"))
}

/// Conservative capacity for collections built by upstream filtered iterators.
/// A positive collection can start at capacity four and grow geometrically
/// before reaching its final size, including fallible outer-table collection.
pub(super) fn collection_capacity(count: usize) -> VortexResult<usize> {
    if count == 0 {
        return Ok(0);
    }
    count
        .max(4)
        .checked_next_power_of_two()
        .ok_or_else(|| vortex_err!("File statistics allocation size overflow"))
}

/// Reserve before any table producer or parser runs. The bound includes both
/// retained capacity and construction overlap; it is deliberately conservative,
/// not a claim about the allocator's exact RSS. No reservation is made for empty
/// tables. Native consumers cannot export any of these shared table allocations
/// without retaining the complete `FileStatistics` handle.
pub(super) fn reserve_tables(
    entries: usize,
    legacy_entries: usize,
    max_aggregates: usize,
    path_parts: usize,
    temporary_bytes: usize,
    session: &VortexSession,
) -> VortexResult<Option<HostMemoryReservation>> {
    // Writer snapshots and fallible collections may grow. Parsers with exact
    // capacities use reserve_parsed_tables instead.
    let entries = collection_capacity(entries)?;
    let legacy_entries = collection_capacity(legacy_entries)?;
    let mut tables = bytes::<AggregateStats>(entries)?;
    tables = add(tables, bytes::<DType>(entries)?)?;
    tables = add(tables, bytes::<FieldPath>(entries)?)?;
    tables = add(tables, bytes::<StatsSet>(legacy_entries)?)?;
    tables = add(tables, bytes::<Field>(path_parts)?)?;
    // Outer vectors and paths can reallocate while growing. Their old and new
    // tables may coexist, but the aggregate vectors they reference are moved,
    // never copied. Do not double every field's aggregate payload for that move.
    let mut total = add(tables, tables)?;
    let aggregate_capacity = collection_capacity(max_aggregates)?;
    total = add(
        total,
        bytes::<AggregateStat>(multiply(entries, aggregate_capacity)?)?,
    )?;
    // Per-field snapshots are produced serially, so at most one inner vector
    // can overlap its previous capacity during growth. Nested writer snapshots
    // supply their additional legacy table explicitly as temporary storage.
    if entries != 0 {
        total = add(total, bytes::<AggregateStat>(aggregate_capacity)?)?;
    }
    total = add(total, temporary_bytes)?;
    if total == 0 {
        return Ok(None);
    }
    session.host_allocator().reserve_external(total).map(Some)
}

/// Parsers preallocate their outer and aggregate vectors exactly. Their
/// borrowed footer describes every possible slot before any scalar is decoded;
/// no filtered-iterator growth or legacy aggregate snapshot is involved.
pub(super) fn reserve_parsed_tables(
    entries: usize,
    legacy_entries: usize,
    aggregate_slots: usize,
    path_parts: usize,
    temporary_bytes: usize,
    session: &VortexSession,
) -> VortexResult<Option<HostMemoryReservation>> {
    let mut total = bytes::<AggregateStats>(entries)?;
    total = add(total, bytes::<DType>(entries)?)?;
    total = add(total, bytes::<FieldPath>(entries)?)?;
    total = add(total, bytes::<StatsSet>(legacy_entries)?)?;
    total = add(total, bytes::<AggregateStat>(aggregate_slots)?)?;
    // Path traversal can clone and grow an intermediate path. Its payload
    // bound includes both copies; only one final copy moves into each entry.
    total = add(total, bytes::<Field>(multiply(path_parts, 2)?)?)?;
    total = add(total, temporary_bytes)?;
    if total == 0 {
        Ok(None)
    } else {
        session.host_allocator().reserve_external(total).map(Some)
    }
}

/// Inspect schema shape without allocating a layout, path vector, or lookup
/// table. The accounting follows the provider's post-order struct traversal.
pub(super) fn layout_shape(dtype: &DType) -> VortexResult<(usize, usize)> {
    fn walk(dtype: &DType, depth: usize) -> VortexResult<(usize, usize)> {
        match dtype.as_struct_fields_opt() {
            Some(fields) => {
                let mut count = 0;
                // The upstream traversal clones intermediate paths even when
                // a descendant contributes no statistics entry.
                let mut parts = path_capacity(depth)?;
                for field in fields.try_field_dtypes() {
                    let (child_count, child_parts) = walk(field?, add(depth, 1)?)?;
                    count = add(count, child_count)?;
                    parts = add(parts, child_parts)?;
                }
                if dtype.is_nullable() {
                    count = add(count, 1)?;
                }
                Ok((count, parts))
            }
            None if contains_variant(dtype)? => Ok((0, path_capacity(depth)?)),
            None => Ok((1, path_capacity(depth)?)),
        }
    }
    let (count, parts) = walk(dtype, 0)?;
    Ok((count, if count == 0 { 0 } else { parts }))
}

fn path_capacity(depth: usize) -> VortexResult<usize> {
    // Cloning a length-five path then pushing can allocate capacity ten; a
    // power-of-two bound on the final length alone would undercount that case.
    Ok(collection_capacity(depth)?.max(multiply(depth, 2)?))
}

fn contains_variant(dtype: &DType) -> VortexResult<bool> {
    Ok(match dtype {
        DType::Variant(_) => true,
        DType::Struct(fields, _) => {
            for field in fields.try_field_dtypes() {
                if contains_variant(field?)? {
                    return Ok(true);
                }
            }
            false
        }
        DType::List(element, _) | DType::FixedSizeList(element, ..) => contains_variant(element)?,
        DType::Map(map, _) => {
            contains_variant(&map.key_dtype())? || contains_variant(&map.value_dtype())?
        }
        DType::Extension(extension) => contains_variant(extension.storage_dtype())?,
        _ => false,
    })
}
