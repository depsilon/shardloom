//! Borrowed cross-field vector checks after ancestor visibility has been applied.

use super::{Kind, Metadata};
use arrow_array::{
    Array, GenericListArray, OffsetSizeTrait,
    cast::AsArray as _,
    types::{
        Float16Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
        UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_schema::DataType;
use shardloom_core::{Result, ShardLoomError};
use vortex::error::vortex_err;

fn failed(message: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "vector intake: {message}; no fallback execution was attempted"
    ))
}

fn coordinates<O: OffsetSizeTrait>(
    array: &GenericListArray<O>,
    row: usize,
) -> Result<(&dyn Array, usize, usize)> {
    let from = array.value_offsets()[row].as_usize();
    let to = array.value_offsets()[row + 1].as_usize();
    if from > to || to > array.values().len() {
        return Err(failed("list coordinates exceed values"));
    }
    Ok((array.values().as_ref(), from, to - from))
}
fn list(array: &dyn Array, row: usize) -> Result<(&dyn Array, usize, usize)> {
    if row >= array.len() || array.is_null(row) {
        return Err(failed("present vector requires nonnull list containers"));
    }
    if let Some(array) = array.as_list_opt::<i32>() {
        coordinates(array, row)
    } else if let Some(array) = array.as_list_opt::<i64>() {
        coordinates(array, row)
    } else {
        Err(failed("expected list storage"))
    }
}

fn integer(array: &dyn Array, row: usize) -> Result<i128> {
    if row >= array.len() || array.is_null(row) {
        return Err(failed("vector coordinates require nonnull integers"));
    }
    macro_rules! value {
        ($ty:ty) => {
            i128::from(array.as_primitive::<$ty>().value(row))
        };
    }
    Ok(match array.data_type() {
        DataType::Int8 => value!(Int8Type),
        DataType::Int16 => value!(Int16Type),
        DataType::Int32 => value!(Int32Type),
        DataType::Int64 => value!(Int64Type),
        DataType::UInt8 => value!(UInt8Type),
        DataType::UInt16 => value!(UInt16Type),
        DataType::UInt32 => value!(UInt32Type),
        DataType::UInt64 => value!(UInt64Type),
        _ => return Err(failed("expected integer storage")),
    })
}
fn scale(array: &dyn Array, row: usize) -> Result<()> {
    if row >= array.len() {
        return Err(failed("scale coordinate exceeds values"));
    }
    let value = if array.is_null(row) {
        None
    } else {
        Some(match array.data_type() {
            DataType::Float16 => array.as_primitive::<Float16Type>().value(row).to_f64(),
            DataType::Float32 => f64::from(array.as_primitive::<Float32Type>().value(row)),
            DataType::Float64 => array.as_primitive::<Float64Type>().value(row),
            _ => return Err(failed("expected floating scale storage")),
        })
    };
    Metadata::validate_scale(value).map_err(failed)
}

pub(crate) fn validate(
    metadata: &Metadata,
    array: &dyn Array,
    start: usize,
    end: usize,
    check: &dyn Fn() -> Result<()>,
) -> Result<()> {
    let array = array
        .as_struct_opt()
        .ok_or_else(|| failed("expected vector struct"))?;
    for row in start..end {
        if row.is_multiple_of(1024) {
            check()?;
        }
        match metadata.kind() {
            Kind::Sparse => {
                let size =
                    u64::try_from(integer(array.column(0).as_ref(), row)?).map_err(failed)?;
                let (indices, from, count) = list(array.column(1).as_ref(), row)?;
                let (_, _, values) = list(array.column(2).as_ref(), row)?;
                metadata
                    .validate_sparse(
                        size,
                        count,
                        values,
                        (from..from + count).map(|index| {
                            if index.is_multiple_of(1024) {
                                check().map_err(|e| vortex_err!("{e}"))?;
                            }
                            integer(indices, index).map_err(|e| vortex_err!("{e}"))
                        }),
                    )
                    .map_err(failed)?;
            }
            Kind::Quantized => {
                // Code buffers are not touched by cross-field validation.
                let (_, _, codes) = list(array.column(0).as_ref(), row)?;
                let expected = metadata.parameter_count(codes).map_err(failed)?;
                let (scales, from, count, zeros, zero_from, zero_count) =
                    if metadata.block_size().is_some() {
                        let (scales, from, count) = list(array.column(1).as_ref(), row)?;
                        let (zeros, zero_from, zero_count) = list(array.column(2).as_ref(), row)?;
                        (scales, from, count, zeros, zero_from, zero_count)
                    } else {
                        (
                            array.column(1).as_ref(),
                            row,
                            1,
                            array.column(2).as_ref(),
                            row,
                            1,
                        )
                    };
                if count != expected || zero_count != expected {
                    return Err(failed("quantized parameter counts differ from code blocks"));
                }
                for index in 0..count {
                    if index.is_multiple_of(1024) {
                        check()?;
                    }
                    scale(scales, from + index)?;
                    if zeros.is_null(zero_from + index) {
                        return Err(failed("quantized zero points must be nonnull"));
                    }
                }
            }
        }
    }
    check()
}
