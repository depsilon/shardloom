//! Borrowed geometry checks after the complete ancestor visibility walk.

use super::Kind;
use crate::native_spatial::values::{failed, point, sequence};
use arrow_array::{
    Array, GenericListArray, OffsetSizeTrait, cast::AsArray as _, types::Float64Type,
};
use shardloom_core::Result;

fn offsets<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    row: usize,
) -> Result<(&dyn Array, usize, usize)> {
    let from = array.value_offsets()[row].as_usize();
    let to = array.value_offsets()[row + 1].as_usize();
    if from > to || to > array.values().len() {
        return Err(failed("spatial offsets exceed child coordinates"));
    }
    Ok((array.values().as_ref(), from, to - from))
}
fn list(array: &dyn Array, row: usize) -> Result<(&dyn Array, usize, usize)> {
    if row >= array.len() || array.is_null(row) {
        return Err(failed(
            "present geometry requires nonnull rings and vertices",
        ));
    }
    if let Some(array) = array.as_list_opt::<i32>() {
        offsets(array, row)
    } else if let Some(array) = array.as_list_opt::<i64>() {
        offsets(array, row)
    } else {
        Err(failed("spatial vertices require List/LargeList storage"))
    }
}
fn coordinate(array: &dyn Array, row: usize) -> Result<(f64, f64)> {
    if row >= array.len() || array.is_null(row) {
        return Err(failed("spatial coordinates must be nonnull"));
    }
    let coordinates = array
        .as_struct_opt()
        .ok_or_else(|| failed("spatial coordinates require x/y struct storage"))?;
    let x = coordinates.column(0).as_primitive::<Float64Type>();
    let y = coordinates.column(1).as_primitive::<Float64Type>();
    if x.is_null(row) || y.is_null(row) {
        return Err(failed("spatial coordinates must be nonnull"));
    }
    Ok((x.value(row), y.value(row)))
}
fn vertices(
    array: &dyn Array,
    row: usize,
    ring: bool,
    check: &dyn Fn() -> Result<()>,
) -> Result<()> {
    let (array, start, count) = list(array, row)?;
    sequence(count, ring, |index| {
        if index.is_multiple_of(1024) {
            check()?;
        }
        coordinate(array, start + index)
    })
}
pub(crate) fn validate(
    kind: Kind,
    array: &dyn Array,
    start: usize,
    end: usize,
    check: &dyn Fn() -> Result<()>,
) -> Result<()> {
    for row in start..end {
        if row.is_multiple_of(1024) {
            check()?;
        }
        match kind {
            Kind::Point => {
                let (x, y) = coordinate(array, row)?;
                point(x, y, true)?;
            }
            Kind::LineString => vertices(array, row, false, check)?,
            Kind::Polygon => {
                let (rings, start, count) = list(array, row)?;
                for ring in start..start + count {
                    if ring.is_multiple_of(1024) {
                        check()?;
                    }
                    vertices(rings, ring, true, check)?;
                }
            }
        }
    }
    check()
}
