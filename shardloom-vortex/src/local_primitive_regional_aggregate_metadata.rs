//! Read exact typed partials from native auxiliary zone tables.

use super::{
    Column, MAX_DEPTH, MAX_TABLES, MAX_ZONES, Plan, Result, SegmentStats, Zone, check, failed,
    vortex_error,
};
use crate::local_primitives::{stat_value_cmp, vortex_scalar_to_stat_value};
use shardloom_core::StatisticsExactness;
use shardloom_exec::compute_pool::CancellationToken;
use vortex::{
    array::{
        VortexSessionExecute as _,
        aggregate_fn::fns::{max::Max, min::Min, null_count::NullCount},
        arrays::{PrimitiveArray, StructArray, struct_::StructArrayExt as _},
        dtype::{DType, Nullability, PType},
        expr::{root, select},
    },
    file::VortexFile,
    io::runtime::BlockingRuntime,
    layout::{
        LayoutRef,
        layouts::{
            chunked::Chunked,
            struct_::Struct,
            zoned::{Zoned, ZonedLayout},
        },
        scan::scan_builder::ScanBuilder,
    },
    session::VortexSession,
};

pub(super) fn count(
    file: &VortexFile,
    name: &str,
    total_zones: &mut usize,
    total_tables: &mut usize,
) -> Result<Option<usize>> {
    let mut count = 0_usize;
    let complete = walk(file.footer().layout(), Some(name), 0, 0, &mut |zoned, _| {
        let zones = zoned.nzones();
        let functions = zoned.aggregate_functions();
        if functions.len() > 16 {
            return Ok(false);
        }
        count = count
            .checked_add(zones)
            .ok_or_else(|| failed("zone count overflow"))?;
        *total_zones = total_zones
            .checked_add(zones)
            .ok_or_else(|| failed("zone count overflow"))?;
        *total_tables = total_tables
            .checked_add(1)
            .ok_or_else(|| failed("table count overflow"))?;
        Ok(*total_zones <= MAX_ZONES && *total_tables <= MAX_TABLES)
    })?;
    Ok((complete && count != 0).then_some(count))
}

/// A false result is unsupported shape; contradictory lengths are errors.
fn walk(
    layout: &LayoutRef,
    field: Option<&str>,
    start: u64,
    depth: usize,
    visit: &mut impl FnMut(&ZonedLayout, u64) -> Result<bool>,
) -> Result<bool> {
    if depth > MAX_DEPTH {
        return Ok(false);
    }
    if layout.is::<Chunked>() {
        if layout.nslots() > MAX_TABLES {
            return Ok(false);
        }
        let end = start
            .checked_add(layout.row_count())
            .ok_or_else(|| failed("layout offset overflow"))?;
        let mut offset = start;
        for index in 0..layout.nslots() {
            let child = layout
                .slot(index)
                .map_err(vortex_error)?
                .ok_or_else(|| failed("chunked layout has an absent child"))?;
            let next = offset
                .checked_add(child.row_count())
                .ok_or_else(|| failed("chunk offset overflow"))?;
            if next > end {
                return Err(failed("chunk coverage exceeds its parent"));
            }
            if child.row_count() != 0 && !walk(&child, field, offset, depth + 1, visit)? {
                return Ok(false);
            }
            offset = next;
        }
        if offset != end {
            return Err(failed("chunk coverage leaves a gap"));
        }
        return Ok(true);
    }
    if let Some(field) = field {
        if let Some(structure) = layout.as_opt::<Struct>() {
            if structure.dtype().is_nullable() {
                return Ok(false);
            }
            let Some(index) = structure.struct_fields().find(field) else {
                return Err(failed("struct layout omits a bound field"));
            };
            let child = layout
                .slot(index + 1)
                .map_err(vortex_error)?
                .ok_or_else(|| failed("struct layout has an absent field"))?;
            if child.row_count() != layout.row_count() {
                return Err(failed("struct field coverage differs from its parent"));
            }
            return walk(&child, None, start, depth + 1, visit);
        }
        // Root zoning may wrap a struct; its nested partial schema is outside
        // this integer-column contract, but its transparent data is still native.
        if let Some(zoned) = layout.as_opt::<Zoned>() {
            let child = zoned
                .slot(0)
                .map_err(vortex_error)?
                .ok_or_else(|| failed("zoned layout has no data child"))?;
            if child.row_count() != layout.row_count() {
                return Err(failed("zoned data coverage differs from its parent"));
            }
            return walk(&child, Some(field), start, depth + 1, visit);
        }
        return Ok(false);
    }
    let Some(zoned) = layout.as_opt::<Zoned>() else {
        return Ok(false);
    };
    if zoned.zone_len() == 0 {
        return Ok(false);
    }
    let data = zoned
        .slot(0)
        .map_err(vortex_error)?
        .ok_or_else(|| failed("zoned layout has no data child"))?;
    if data.row_count() != zoned.row_count()
        || zoned.nzones() as u64 != zoned.row_count().div_ceil(zoned.zone_len() as u64)
    {
        return Err(failed("zoned row coverage or zone count is inconsistent"));
    }
    visit(zoned, start)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn read(
    file: &VortexFile,
    column: &mut Column<'_>,
    session: &VortexSession,
    runtime: &impl BlockingRuntime,
    cancellation: Option<&CancellationToken>,
    plan: &mut Plan,
) -> Result<()> {
    let complete = walk(
        file.footer().layout(),
        Some(column.name),
        0,
        0,
        &mut |zoned, start| {
            read_table(
                file,
                column,
                zoned,
                start,
                session,
                runtime,
                cancellation,
                plan,
            )?;
            Ok(true)
        },
    )?;
    if !complete {
        return Err(failed("layout changed after regional admission"));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn read_table(
    file: &VortexFile,
    column: &mut Column<'_>,
    zoned: &ZonedLayout,
    start: u64,
    session: &VortexSession,
    runtime: &impl BlockingRuntime,
    cancellation: Option<&CancellationToken>,
    plan: &mut Plan,
) -> Result<()> {
    check(cancellation)?;
    if zoned.dtype() != &column.dtype {
        return Err(failed("zone dtype differs from bound input"));
    }
    let table = zoned
        .slot(1)
        .map_err(vortex_error)?
        .ok_or_else(|| failed("zoned layout has no statistics child"))?;
    let DType::Struct(fields, Nullability::NonNullable) = table.dtype() else {
        return Err(failed("zone statistics require a non-nullable struct"));
    };
    if table.row_count() != zoned.nzones() as u64 {
        return Err(failed("zone table cardinality differs"));
    }
    // At most three bounded scalar fields are selected. Typed declarations, not
    // aggregate display names, establish the exact semantic domain.
    let mut names: [Option<String>; 3] = std::array::from_fn(|_| None);
    for function in zoned.aggregate_functions().iter() {
        let kind = if function.is::<NullCount>() {
            0
        } else if function.is::<Min>() {
            1
        } else if function.is::<Max>() {
            2
        } else {
            continue;
        };
        if names[kind].is_some() {
            continue;
        }
        let name = function.to_string();
        let expected = if kind == 0 {
            DType::Primitive(PType::U64, Nullability::Nullable)
        } else {
            column.dtype.as_nullable()
        };
        if fields.try_field(&name).map_err(vortex_error)? != Some(expected) {
            return Err(failed(
                "typed zone partial differs from its stored field dtype",
            ));
        }
        names[kind] = Some(name);
    }
    let mut index = 0_usize;
    if names.iter().all(Option::is_none) {
        while index < zoned.nzones() {
            append(column, zoned, start, index, [None, None, None], plan)?;
            index += 1;
        }
        return Ok(());
    }
    let projection = select(
        names
            .iter()
            .flatten()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        root(),
    )
    .bind(table.dtype())
    .map_err(vortex_error)?;
    let reader = table
        .new_reader(
            "regional_aggregate.zones".into(),
            file.segment_source(),
            session,
            &vortex::layout::LayoutReaderContext::default(),
        )
        .map_err(vortex_error)?;
    let scan = ScanBuilder::new(session.clone(), reader)
        .with_projection(projection)
        .with_concurrency(1);
    plan.tables_read += 1;
    let mut ctx = session.create_execution_ctx();
    for chunk in scan.into_array_iter(runtime).map_err(vortex_error)? {
        check(cancellation)?;
        let chunk = chunk
            .map_err(vortex_error)?
            .execute::<StructArray>(&mut ctx)
            .map_err(vortex_error)?;
        plan.metadata_arrays += 1;
        let mut arrays: [Option<PrimitiveArray>; 3] = std::array::from_fn(|_| None);
        for (kind, name) in names.iter().enumerate() {
            if let Some(name) = name {
                arrays[kind] = Some(
                    chunk
                        .unmasked_field_by_name(name)
                        .map_err(vortex_error)?
                        .clone()
                        .execute::<PrimitiveArray>(&mut ctx)
                        .map_err(vortex_error)?,
                );
            }
        }
        for row in 0..chunk.len() {
            check(cancellation)?;
            if index >= zoned.nzones() {
                return Err(failed("zone table delivered extra rows"));
            }
            let mut values = [None, None, None];
            for (kind, array) in arrays.iter().enumerate() {
                if let Some(array) = array {
                    let scalar = array.execute_scalar(row, &mut ctx).map_err(vortex_error)?;
                    if !scalar.is_null() {
                        values[kind] = Some(
                            vortex_scalar_to_stat_value(&scalar)
                                .ok_or_else(|| failed("zone partial is not a native integer"))?,
                        );
                    }
                }
            }
            append(column, zoned, start, index, values, plan)?;
            index += 1;
            plan.metadata_rows_read += 1;
        }
    }
    if index != zoned.nzones() {
        return Err(failed("zone table omitted required rows"));
    }
    Ok(())
}

fn append(
    column: &mut Column<'_>,
    zoned: &ZonedLayout,
    start: u64,
    index: usize,
    [nulls, min, max]: [Option<super::StatValue>; 3],
    _plan: &Plan,
) -> Result<()> {
    let offset = (index as u64)
        .checked_mul(zoned.zone_len() as u64)
        .ok_or_else(|| failed("zone offset overflow"))?;
    let rows = zoned
        .row_count()
        .checked_sub(offset)
        .ok_or_else(|| failed("zone offset exceeds source"))?
        .min(zoned.zone_len() as u64);
    let start = start
        .checked_add(offset)
        .ok_or_else(|| failed("zone start overflow"))?;
    let end = start
        .checked_add(rows)
        .ok_or_else(|| failed("zone end overflow"))?;
    let nulls = match nulls {
        Some(super::StatValue::UInt64(value)) => Some(value),
        Some(_) => return Err(failed("zone null count is not u64")),
        None if !column.dtype.is_nullable() => Some(0),
        None => None,
    };
    if nulls.is_some_and(|nulls| nulls > rows || (!column.dtype.is_nullable() && nulls != 0))
        || ((rows == 0 || nulls == Some(rows)) && (min.is_some() || max.is_some()))
    {
        return Err(failed("zone null count or extrema contradict row domain"));
    }
    if let (Some(min), Some(max)) = (&min, &max)
        && !matches!(
            stat_value_cmp(min, max),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        )
    {
        return Err(failed("zone extrema are inverted"));
    }
    if column.zones.values.len() == column.zones.values.capacity() {
        return Err(failed("zone count exceeds admitted capacity"));
    }
    column.zones.values.push(Zone {
        range: start..end,
        stats: SegmentStats {
            null_count: nulls,
            min_value: min,
            max_value: max,
            exactness: StatisticsExactness::Exact,
            ..SegmentStats::with_row_count(rows)
        },
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{num::NonZeroUsize, sync::Arc};
    use vortex::{
        array::dtype::StructFields,
        layout::{layouts::flat::FlatLayout, segments::SegmentId},
        session::registry::ReadContext,
    };

    #[test]
    fn regional_aggregate_malformed_native_zone_count_fails_before_metadata_io() {
        let dtype = DType::Primitive(PType::I64, Nullability::NonNullable);
        for count in [0, 1, 2, 3] {
            let data = FlatLayout::new(
                10,
                dtype.clone(),
                SegmentId::from(0_u32),
                ReadContext::new([]),
            )
            .into_layout();
            let table = FlatLayout::new(
                count,
                DType::Struct(
                    StructFields::from_iter(std::iter::empty::<(String, DType)>()),
                    Nullability::NonNullable,
                ),
                SegmentId::from(1_u32),
                ReadContext::new([]),
            )
            .into_layout();
            let layout =
                ZonedLayout::try_new(data, table, NonZeroUsize::new(8).unwrap(), Arc::new([]))
                    .unwrap()
                    .into_layout();
            let mut visits = 0;
            let result = walk(&layout, None, 0, 0, &mut |_, _| {
                visits += 1;
                Ok(true)
            });
            if count == 2 {
                assert!(result.unwrap());
                assert_eq!(visits, 1);
            } else {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("zone count is inconsistent")
                );
                assert_eq!(visits, 0);
            }
        }
    }
}
