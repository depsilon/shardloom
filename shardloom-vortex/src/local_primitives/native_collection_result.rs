//! Build one admitted collection result directly in native, credited buffers.

use super::{
    native_payload,
    native_relational_batch::failed,
    native_relational_expression::keys,
    native_relational_keys::Cell,
    result_batch::{self, Value},
    vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use vortex::{
    array::{
        ArrayRef, IntoArray, VortexSessionExecute as _,
        arrays::{ListArray, VarBinArray},
        dtype::{DType, Nullability, PType},
        memory::HostMemorySessionExt as _,
        validity::Validity,
    },
    buffer::Alignment,
};

pub(super) fn build(
    values: ArrayRef,
    dtype: &DType,
    separator: Option<&str>,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    context.check_cancelled()?;
    if values.is_empty() {
        return native_payload::defaults(dtype, 1, context);
    }
    let credit = context
        .memory()
        .reserve(native_payload::metadata_bytes(dtype)?)?;
    let allocator =
        crate::owned_buffers::with_credit(context.native_session().host_allocator(), credit);
    let offsets = |end| {
        result_batch::build_column(
            &DType::Primitive(PType::U64, Nullability::NonNullable),
            2,
            &allocator,
            |row| Ok(Value::UInt(if row == 0 { 0 } else { end })),
        )
    };
    match (dtype, separator) {
        (DType::List(child, _), None) if child.as_ref() == values.dtype() => {
            let end = u64::try_from(values.len()).map_err(vortex_error)?;
            ListArray::try_new(
                values,
                offsets(end)?,
                Validity::AllValid,
                &mut context.native_session().create_execution_ctx(),
            )
            .map(IntoArray::into_array)
            .map_err(vortex_error)
        }
        (DType::Utf8(_), Some(separator)) => {
            let values = keys(&values, context)?;
            let mut bytes = separator
                .len()
                .checked_mul(values.len() - 1)
                .ok_or_else(|| failed("string aggregate output size overflow"))?;
            for row in 0..values.len() {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                let Cell::Utf8(value) = values.cell(row)? else {
                    return Err(failed("string aggregate admitted a null or non-UTF8 value"));
                };
                bytes = bytes
                    .checked_add(value.len())
                    .ok_or_else(|| failed("string aggregate output size overflow"))?;
            }
            let mut buffer = allocator
                .allocate(bytes, Alignment::new(1))
                .map_err(vortex_error)?;
            let mut offset = 0;
            for row in 0..values.len() {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                if row != 0 {
                    let end = offset + separator.len();
                    buffer.as_mut_slice()[offset..end].copy_from_slice(separator.as_bytes());
                    offset = end;
                }
                let Cell::Utf8(value) = values.cell(row)? else {
                    return Err(failed("string aggregate value changed during output"));
                };
                let end = offset + value.len();
                buffer.as_mut_slice()[offset..end].copy_from_slice(value.as_slice());
                offset = end;
            }
            VarBinArray::try_new(
                offsets(u64::try_from(bytes).map_err(vortex_error)?)?,
                buffer.freeze(),
                dtype.clone(),
                Validity::AllValid,
                &mut context.native_session().create_execution_ctx(),
            )
            .map(IntoArray::into_array)
            .map_err(vortex_error)
        }
        _ => Err(failed(
            "collection result dtype or separator is inconsistent",
        )),
    }
}
