//! Preserve streaming payload validation without retaining a copied population.

use super::{
    CopyPolicy, CopyState, NativeNumericOwner, ReservedVec, Value, check, copy_rows, result_batch,
    vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use vortex::array::{
    ArrayRef, Columnar, IntoArray, VortexSessionExecute as _, arrays::PrimitiveArray, dtype::DType,
    memory::HostMemorySessionExt as _,
};

#[path = "native_payload_validation_typed.rs"]
mod typed;

/// Visit the same complete field domains as `detach`, keeping no payload rows.
/// Rich and nullable records share the copier's parent masks and field order;
/// flat nonnullable records retain their existing direct field resolution.
pub(in crate::local_primitives) fn validate_record(
    array: &ArrayRef,
    fields: &[(String, DType)],
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    context.check_cancelled()?;
    if array.dtype().is_nullable()
        || fields
            .iter()
            .any(|(_, dtype)| super::is_nested(dtype) || matches!(dtype, DType::Decimal(..)))
    {
        let metadata =
            crate::native_payload_schema::reserve_schema(array.dtype(), context.memory())?;
        let allocator =
            crate::owned_buffers::with_credit(context.native_session().host_allocator(), metadata);
        let mut rows = ReservedVec::new(context.memory())?;
        rows.reserve(array.len())?;
        for row in 0..array.len() {
            check(row, context)?;
            rows.values.push(Some(row));
        }
        drop(copy_rows::<false>(
            Some(array),
            &rows.values,
            array.dtype(),
            CopyState::unmeasured(CopyPolicy::ValidateValues),
            context,
            &allocator,
        )?);
        return context.check_cancelled();
    }
    let mut execution = context.native_session().create_execution_ctx();
    for (name, dtype) in fields {
        context.check_cancelled()?;
        let column = super::super::logical_field_from_native_array(array, name)?
            .execute::<Columnar>(&mut execution)
            .map(IntoArray::into_array)
            .map_err(vortex_error)?;
        let numeric = if matches!(dtype, DType::Primitive(..))
            || crate::native_payload_schema::temporal_storage(dtype).is_some()
        {
            let storage = result_batch::scalar_storage(&column, &mut execution)?
                .execute::<PrimitiveArray>(&mut execution)
                .map_err(vortex_error)?;
            Some(NativeNumericOwner::new(storage, &mut execution)?)
        } else {
            None
        };
        if column.dtype() == dtype && column.len() == array.len() {
            if let Some(numeric) = &numeric {
                result_batch::validate_numeric_column(
                    dtype,
                    numeric.primitive(),
                    numeric.validity(),
                    &context.native_session().host_allocator(),
                    || context.check_cancelled(),
                )?;
                continue;
            }
            if typed::validate(&column, &mut execution, context)? {
                continue;
            }
        }
        drop(result_batch::build_column_prefix(
            dtype,
            array.len(),
            0,
            &context.native_session().host_allocator(),
            |row| {
                check(row, context)?;
                #[cfg(all(test, debug_assertions))]
                super::super::native_relational_observation::record(|counts| {
                    counts.payload_validation_scalar_calls += 1;
                });
                if let Some(numeric) = &numeric {
                    numeric.stat_value(row).map(Value::from)
                } else {
                    result_batch::scalar_value(&column, row, &mut execution)
                }
            },
        )?);
    }
    context.check_cancelled()
}
