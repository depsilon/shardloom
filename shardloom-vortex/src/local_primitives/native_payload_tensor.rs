//! Validate row-shape relations only in the caller's observed native domain.

use super::{
    NativeExecutionContext, Value, check, failed, native_list, result_batch, vortex_error,
};
use crate::native_variable_tensor::Metadata;
use shardloom_core::Result;
use vortex::array::{
    ArrayRef, VortexSessionExecute as _,
    arrays::{StructArray, struct_::StructArrayExt as _},
    dtype::{DType, Nullability, PType},
    memory::HostAllocatorRef,
};

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
    let source = source.ok_or_else(|| failed("variable tensor source is absent"))?;
    let rank = crate::native_variable_tensor::rank(source.dtype()).map_err(vortex_error)? as usize;
    let expansion = rows
        .len()
        .checked_mul(rank)
        .and_then(|n| n.checked_mul(24))
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(|| failed("variable tensor shape expansion overflow"))?;
    let _expansion = context.memory().reserve(expansion)?;
    // Apply arbitrary order, duplicates and ancestor masks before resolving
    // child providers. Component buffers are never read by this validator.
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
    let data = native_list::Column::new(selected.unmasked_field(0).clone(), &mut execution)?;
    let shape = native_list::Column::new(selected.unmasked_field(1).clone(), &mut execution)?;
    for (row, original) in rows.iter().enumerate() {
        check(row, context)?;
        if original.is_none()
            || !selected
                .struct_validity()
                .execute_is_valid(row, &mut execution)
                .map_err(vortex_error)?
        {
            continue;
        }
        let (_, elements) = data
            .coordinates(row, &mut execution)?
            .ok_or_else(|| failed("present variable tensor requires a data list"))?;
        let (start, count) = shape
            .coordinates(row, &mut execution)?
            .ok_or_else(|| failed("present variable tensor requires a shape list"))?;
        metadata
            .validate_dimensions(
                rank,
                (start..start + count).map(|index| {
                    check(index, context).map_err(|error| vortex::error::vortex_err!("{error}"))?;
                    shape
                        .elements
                        .execute_scalar(index, &mut execution)?
                        .as_primitive()
                        .as_::<i32>()
                        .ok_or_else(|| {
                            vortex::error::vortex_err!(
                                "tensor dimensions require nonnull int32 values"
                            )
                        })
                }),
                elements,
            )
            .map_err(vortex_error)?;
    }
    context.check_cancelled()
}
