//! Preserve streaming payload validation without retaining a copied population.

use super::{NativeNumericOwner, Value, check, detach, result_batch, vortex_error};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use vortex::array::{
    ArrayRef, Columnar, IntoArray, VortexSessionExecute as _, arrays::PrimitiveArray, dtype::DType,
    memory::HostMemorySessionExt as _,
};

/// Visit the same flat fields and typed writes as `detach`, keeping no rows.
/// The bounded result builder still checks every value, validity and text offset.
/// Nested/decimal leaves are excluded by the caller's ordering admission.
pub(in crate::local_primitives) fn validate_flat_record(
    array: &ArrayRef,
    fields: &[(String, DType)],
    context: &NativeExecutionContext<'_>,
) -> Result<()> {
    context.check_cancelled()?;
    if array.dtype().is_nullable() {
        // Nullable records use the existing parent-validity/default semantics.
        // This temporary copy is released before the bounded ordering state.
        drop(detach(array, context)?);
        return Ok(());
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
        drop(result_batch::build_column_prefix(
            dtype,
            array.len(),
            0,
            &context.native_session().host_allocator(),
            |row| {
                check(row, context)?;
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
