//! Schema costs for the shared work controller at compatibility boundaries.
//! Variable lengths are initial estimates, never allocation or RSS guarantees.

use arrow_schema::{DataType, Schema};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::adaptive_work::WorkCost;

/// Logical value/offset/validity spans of completed plain arrays. This excludes
/// retained backing allocations, fixed metadata and unsupported representations.
/// It performs no value validation, so observations cannot replace or reorder
/// the existing intake checks. A malformed/unknown span supplies no feedback.
pub(super) fn observed_row_bytes(batch: &arrow_array::RecordBatch) -> Option<u64> {
    use arrow_array::{Array, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray};
    let rows = u64::try_from(batch.num_rows())
        .ok()
        .filter(|rows| *rows > 0)?;
    let bytes = batch.columns().iter().try_fold(0_u64, |total, array| {
        let value_bytes = if let Some(width) = array.data_type().primitive_width() {
            rows.checked_mul(width as u64)?
        } else {
            let span = |offsets: &[i64]| {
                u64::try_from(offsets.last()?.checked_sub(*offsets.first()?)?).ok()
            };
            match array.data_type() {
                DataType::Null | DataType::Boolean => rows,
                DataType::FixedSizeBinary(width) => {
                    rows.checked_mul(u64::try_from(*width).ok()?)?
                }
                DataType::Utf8 => {
                    let offsets = array
                        .as_any()
                        .downcast_ref::<StringArray>()?
                        .value_offsets();
                    u64::try_from(i64::from(*offsets.last()?) - i64::from(*offsets.first()?))
                        .ok()?
                        .checked_add(rows.checked_add(1)?.checked_mul(4)?)?
                }
                DataType::Binary => {
                    let offsets = array
                        .as_any()
                        .downcast_ref::<BinaryArray>()?
                        .value_offsets();
                    u64::try_from(i64::from(*offsets.last()?) - i64::from(*offsets.first()?))
                        .ok()?
                        .checked_add(rows.checked_add(1)?.checked_mul(4)?)?
                }
                DataType::LargeUtf8 => span(
                    array
                        .as_any()
                        .downcast_ref::<LargeStringArray>()?
                        .value_offsets(),
                )?
                .checked_add(rows.checked_add(1)?.checked_mul(8)?)?,
                DataType::LargeBinary => span(
                    array
                        .as_any()
                        .downcast_ref::<LargeBinaryArray>()?
                        .value_offsets(),
                )?
                .checked_add(rows.checked_add(1)?.checked_mul(8)?)?,
                _ => return None,
            }
        };
        total.checked_add(value_bytes)?.checked_add(rows)
    })?;
    Some(bytes.div_ceil(rows))
}

pub(super) fn schema_cost(schema: &Schema, base: u64, copies: u64) -> Result<WorkCost> {
    let mut fixed = base;
    let mut rows = 0_u64;
    for field in schema.fields() {
        let name_bytes = (field.name().len() as u64)
            .checked_mul(4)
            .ok_or_else(overflow)?;
        fixed = fixed
            .checked_add(1024)
            .and_then(|value| value.checked_add(name_bytes))
            .ok_or_else(overflow)?;
        rows = rows
            .checked_add(row_bytes(field.data_type(), 0)?)
            .ok_or_else(overflow)?;
    }
    Ok(WorkCost {
        fixed_bytes: fixed,
        bytes_per_unit: rows
            .checked_mul(copies)
            .and_then(|value| value.checked_add(16))
            .ok_or_else(overflow)?,
    })
}

fn row_bytes(dtype: &DataType, depth: usize) -> Result<u64> {
    if depth > 64 {
        return Err(overflow());
    }
    let value = if let Some(width) = dtype.primitive_width() {
        width as u64
    } else {
        match dtype {
            DataType::Null | DataType::Boolean => 1,
            DataType::FixedSizeBinary(width) => u64::try_from(*width).map_err(|_| overflow())?,
            DataType::FixedSizeList(child, count) => row_bytes(child.data_type(), depth + 1)?
                .checked_mul(u64::try_from(*count).map_err(|_| overflow())?)
                .ok_or_else(overflow)?,
            DataType::Struct(fields) => fields.iter().try_fold(0_u64, |sum, field| {
                sum.checked_add(row_bytes(field.data_type(), depth + 1)?)
                    .ok_or_else(overflow)
            })?,
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::ListView(child)
            | DataType::LargeListView(child)
            | DataType::Map(child, _) => row_bytes(child.data_type(), depth + 1)?
                .checked_add(16)
                .ok_or_else(overflow)?,
            DataType::Dictionary(key, value) => row_bytes(key, depth + 1)?
                .checked_add(row_bytes(value, depth + 1)?)
                .ok_or_else(overflow)?,
            DataType::RunEndEncoded(ends, values) => row_bytes(ends.data_type(), depth + 1)?
                .checked_add(row_bytes(values.data_type(), depth + 1)?)
                .ok_or_else(overflow)?,
            // Sizing does not admit a type. Variable values and other layouts
            // remain governed by the decoder, its reservations and validation.
            _ => 32,
        }
    };
    value.checked_add(1).ok_or_else(overflow)
}

fn overflow() -> ShardLoomError {
    ShardLoomError::InvalidOperation(
        "compatibility adaptive work schema size overflow; no fallback execution was attempted"
            .into(),
    )
}
