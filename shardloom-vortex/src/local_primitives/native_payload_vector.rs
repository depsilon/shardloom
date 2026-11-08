//! Selected vector relations without densifying coordinates or reconstructing codes.

use super::{
    NativeExecutionContext, NativeNumericOwner, Value, check, failed, native_list, result_batch,
    vortex_error,
};
use crate::native_vector::{Kind, Metadata};
use shardloom_core::Result;
use vortex::array::{
    ArrayRef, ExecutionCtx, VortexSessionExecute as _,
    arrays::{PrimitiveArray, StructArray, struct_::StructArrayExt as _},
    dtype::{DType, Nullability, PType},
    memory::HostAllocatorRef,
};
use vortex::mask::Mask;

fn numeric_range(
    array: &ArrayRef,
    start: usize,
    count: usize,
    execution: &mut ExecutionCtx,
    context: &NativeExecutionContext<'_>,
    visit: impl FnOnce(&NativeNumericOwner) -> Result<()>,
) -> Result<()> {
    context.check_cancelled()?;
    let end = start
        .checked_add(count)
        .filter(|end| *end <= array.len())
        .ok_or_else(|| failed("vector numeric coordinates exceed values"))?;
    let bytes = result_batch::width(array.dtype())?
        .checked_add(1)
        .and_then(|width| count.checked_mul(width))
        .and_then(|n| n.checked_add(4096))
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| failed("vector numeric admission overflow"))?;
    // The lease precedes provider execution and outlives the temporary owner.
    // This credits selected logical expansion, not every provider/RSS allocation.
    let _expansion = context.memory().reserve(bytes)?;
    let selected = array.slice(start..end).map_err(vortex_error)?;
    let primitive = selected
        .execute::<PrimitiveArray>(execution)
        .map_err(vortex_error)?;
    if primitive.len() != count || primitive.dtype() != array.dtype() {
        return Err(failed("vector numeric provider changed its domain"));
    }
    let owner = NativeNumericOwner::new(primitive, execution)?;
    visit(&owner)?;
    context.check_cancelled()
}

pub(super) fn validate_selection(
    source: Option<&ArrayRef>,
    rows: &[Option<usize>],
    metadata: &Metadata,
    context: &NativeExecutionContext<'_>,
    allocator: &HostAllocatorRef,
) -> Result<()> {
    context.check_cancelled()?;
    if !rows.iter().any(Option::is_some) {
        return Ok(());
    }
    let source = source.ok_or_else(|| failed("vector source is absent"))?;
    // Three struct fields, selected list coordinates and validity. Selection
    // indices/final output buffers acquire their own allocator reservations.
    let bytes = rows
        .len()
        .checked_mul(128)
        .and_then(|n| n.checked_add(4096))
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| failed("vector selection admission overflow"))?;
    let _selection = context.memory().reserve(bytes)?;
    let indices = result_batch::build_column(
        &DType::Primitive(
            PType::U64,
            Nullability::from(rows.iter().any(Option::is_none)),
        ),
        rows.len(),
        allocator,
        |row| Ok(rows[row].map_or(Value::Null, |index| Value::UInt(index as u64))),
    )?;
    let mut execution = context.native_session().create_execution_ctx();
    let selected = source
        .take(indices)
        .map_err(vortex_error)?
        .execute::<StructArray>(&mut execution)
        .map_err(vortex_error)?;
    let valid = selected
        .struct_validity()
        .execute_mask(rows.len(), &mut execution)
        .map_err(vortex_error)?;
    match metadata.kind() {
        Kind::Sparse => {
            validate_sparse(&selected, rows, &valid, metadata, &mut execution, context)?;
        }
        Kind::Quantized => {
            validate_quantized(&selected, rows, &valid, metadata, &mut execution, context)?;
        }
    }
    context.check_cancelled()
}

fn list(
    selected: &StructArray,
    index: usize,
    execution: &mut ExecutionCtx,
) -> Result<(ArrayRef, native_list::Coordinates)> {
    native_list::Column::new(selected.unmasked_field(index).clone(), execution)?
        .into_indexed(execution)
}

fn validate_sparse(
    selected: &StructArray,
    rows: &[Option<usize>],
    valid: &Mask,
    metadata: &Metadata,
    execution: &mut ExecutionCtx,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    let (indices, index_coordinates) = list(selected, 1, execution)?;
    let (_, value_coordinates) = list(selected, 2, execution)?;
    for (row, original) in rows.iter().enumerate() {
        check(row, context)?;
        if original.is_none() || !valid.value(row) {
            continue;
        }
        let size = selected
            .unmasked_field(0)
            .execute_scalar(row, execution)
            .map_err(vortex_error)?
            .as_primitive()
            .as_::<u64>()
            .ok_or_else(|| failed("present sparse vector requires UInt64 size"))?;
        let (from, count) = index_coordinates
            .at(row)?
            .ok_or_else(|| failed("present sparse vector requires indices"))?;
        let (_, values) = value_coordinates
            .at(row)?
            .ok_or_else(|| failed("present sparse vector requires values"))?;
        numeric_range(&indices, from, count, execution, context, |owner| {
            metadata
                .validate_sparse(
                    size,
                    count,
                    values,
                    (0..count).map(|index| {
                        check(index, context).map_err(|e| vortex::error::vortex_err!("{e}"))?;
                        owner
                            .integer_ordinal(index)
                            .map_err(|e| vortex::error::vortex_err!("{e}"))?
                            .ok_or_else(|| {
                                vortex::error::vortex_err!("sparse coordinates must be nonnull")
                            })
                    }),
                )
                .map_err(vortex_error)
        })?;
    }
    Ok(())
}

fn validate_quantized(
    selected: &StructArray,
    rows: &[Option<usize>],
    valid: &Mask,
    metadata: &Metadata,
    execution: &mut ExecutionCtx,
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    let (_, codes) = list(selected, 0, execution)?;
    let parameters = if metadata.block_size().is_some() {
        Some((list(selected, 1, execution)?, list(selected, 2, execution)?))
    } else {
        None
    };
    for (row, original) in rows.iter().enumerate() {
        check(row, context)?;
        if original.is_none() || !valid.value(row) {
            continue;
        }
        let (_, count) = codes
            .at(row)?
            .ok_or_else(|| failed("present quantized vector requires codes"))?;
        let expected = metadata.parameter_count(count).map_err(vortex_error)?;
        let (scales, from, count, zeros, zero_from, zero_count) =
            if let Some(((scales, scale_at), (zeros, zero_at))) = &parameters {
                let (from, count) = scale_at
                    .at(row)?
                    .ok_or_else(|| failed("present quantized vector requires scales"))?;
                let (zero_from, zero_count) = zero_at
                    .at(row)?
                    .ok_or_else(|| failed("present quantized vector requires zero points"))?;
                (scales, from, count, zeros, zero_from, zero_count)
            } else {
                (
                    selected.unmasked_field(1),
                    row,
                    1,
                    selected.unmasked_field(2),
                    row,
                    1,
                )
            };
        if count != expected || zero_count != expected {
            return Err(failed("quantized parameter counts differ from code blocks"));
        }
        numeric_range(scales, from, count, execution, context, |owner| {
            for index in 0..count {
                check(index, context)?;
                Metadata::validate_scale(owner.numeric_value(index)?).map_err(vortex_error)?;
            }
            Ok(())
        })?;
        numeric_range(zeros, zero_from, zero_count, execution, context, |owner| {
            for index in 0..zero_count {
                check(index, context)?;
                if owner.is_null(index)? {
                    return Err(failed("quantized zero points must be nonnull"));
                }
            }
            Ok(())
        })?;
    }
    Ok(())
}
