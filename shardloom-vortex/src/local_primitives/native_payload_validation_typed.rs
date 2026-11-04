//! Complete native-view validation after the caller proves dtype/row identity.

use super::{NativeExecutionContext, Result, check, result_batch, vortex_error};
use vortex::array::{
    ArrayRef, ExecutionCtx,
    arrays::{Bool, VarBinView, varbinview::VarBinViewArrayExt as _},
    dtype::DType,
};

pub(super) fn validate(
    column: &ArrayRef,
    execution: &mut ExecutionCtx,
    context: &NativeExecutionContext<'_>,
) -> Result<bool> {
    if !column.is_host() || !(column.is::<Bool>() || column.is::<VarBinView>()) {
        return Ok(false);
    }
    let valid = column
        .validity()
        .map_err(vortex_error)?
        .execute_mask(column.len(), execution)
        .map_err(vortex_error)?;
    // Keep the original diagnostic path for a nonnullable declaration whose
    // physical mask contradicts it. Ordinary admitted arrays cannot have this.
    if !column.dtype().is_nullable() && !valid.all_true() {
        return Ok(false);
    }
    if let Some(values) = column.as_opt::<VarBinView>() {
        let text = matches!(column.dtype(), DType::Utf8(_));
        let mut bytes = 0;
        for (row, view) in values.views().iter().enumerate() {
            check(row, context)?;
            if valid.value(row) {
                let len = if text {
                    values.utf8_slice_at(row).map_err(vortex_error)?.len()
                } else {
                    // Native construction admitted these immutable views against
                    // their buffers. Binary bytes have no additional value domain.
                    view.len() as usize
                };
                bytes = result_batch::add(bytes, len)?;
                u64::try_from(bytes).map_err(vortex_error)?;
            }
        }
        #[cfg(all(test, debug_assertions))]
        super::super::super::native_relational_observation::record(|counts| {
            counts.payload_variable_validation_columns += 1;
            counts.payload_variable_validation_rows += column.len() as u64;
        });
    } else {
        // The Boolean value domain is intrinsic to the admitted bit buffer.
        // Its mask covers every row; retain the ordinary cancellation cadence.
        for row in 0..column.len() {
            check(row, context)?;
        }
        #[cfg(all(test, debug_assertions))]
        super::super::super::native_relational_observation::record(|counts| {
            counts.payload_boolean_validation_columns += 1;
            counts.payload_boolean_validation_rows += column.len() as u64;
        });
    }
    Ok(true)
}
