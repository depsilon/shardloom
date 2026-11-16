//! Borrow selected native text/bytes while constructing independent output.
//! Selection and canonicalization remain the caller's boundary. Constants keep
//! one native value; no dictionary domain or row-sized expansion is introduced.

use super::{
    Value, add, allocate, copy_bytes, failed, finish_column, finish_validity, multiply,
    scalar_value, set_bit, vortex_error,
};
use shardloom_core::Result;
use std::borrow::Cow;
use vortex::{
    array::{
        ArrayRef, ExecutionCtx,
        arrays::{Constant, VarBinViewArray, varbinview::VarBinViewArrayExt as _},
        dtype::DType,
        memory::HostAllocatorRef,
        validity::Validity,
    },
    mask::Mask,
};

#[cfg(test)]
#[path = "local_primitive_result_variable_tests.rs"]
mod tests;

pub(in crate::local_primitives) struct VariableValues {
    rows: usize,
    kind: Kind,
}

enum Kind {
    Empty,
    Constant(Value<'static>),
    Views {
        values: VarBinViewArray,
        valid: Mask,
        binary: bool,
    },
}

impl VariableValues {
    /// Bind only an already selected `Columnar` text or binary leaf. Other
    /// logical types retain their existing native consumer and diagnostics.
    pub(in crate::local_primitives) fn try_new(
        array: &ArrayRef,
        execution: &mut ExecutionCtx,
    ) -> Result<Option<Self>> {
        if !matches!(array.dtype(), DType::Utf8(_) | DType::Binary(_)) {
            return Ok(None);
        }
        let kind = if array.is_empty() {
            Kind::Empty
        } else if array.as_opt::<Constant>().is_some() {
            Kind::Constant(scalar_value(array, 0, execution)?)
        } else {
            // Columnar has already resolved this leaf to its canonical provider.
            // Requiring that shape prevents an accessor from silently expanding
            // a dictionary or moving work ahead of its selected-input boundary.
            let values = array
                .as_opt::<vortex::array::arrays::VarBinView>()
                .ok_or_else(|| failed("variable output requires selected native columnar values"))?
                .into_owned();
            if !values.as_ref().is_host() {
                return Err(failed("variable output requires native host values"));
            }
            let valid = values
                .varbinview_validity()
                .execute_mask(values.len(), execution)
                .map_err(vortex_error)?;
            if valid.len() != array.len() {
                return Err(failed("variable output validity changed row count"));
            }
            Kind::Views {
                values,
                valid,
                binary: matches!(array.dtype(), DType::Binary(_)),
            }
        };
        #[cfg(all(test, debug_assertions))]
        super::super::native_relational_observation::record(|counts| {
            counts.variable_result_preparations += 1;
            if matches!(kind, Kind::Constant(_)) {
                counts.variable_result_constant_rows += array.len() as u64;
            }
        });
        Ok(Some(Self {
            rows: array.len(),
            kind,
        }))
    }

    pub(in crate::local_primitives) fn value(&self, row: usize) -> Result<Value<'_>> {
        if row >= self.rows {
            return Err(failed("selected scalar exceeds its source domain"));
        }
        let value = match &self.kind {
            Kind::Empty => return Err(failed("empty variable output has no selected value")),
            Kind::Constant(value) => match value {
                Value::Null => Value::Null,
                Value::SharedText(value) => Value::Text(Cow::Borrowed(value.as_str())),
                Value::SharedBinary(value) => Value::Binary(Cow::Borrowed(value.as_ref())),
                _ => {
                    return Err(failed(
                        "constant variable output changed its declared dtype",
                    ));
                }
            },
            Kind::Views {
                values,
                valid,
                binary,
            } => {
                if !valid.value(row) {
                    Value::Null
                } else if *binary {
                    Value::Binary(Cow::Borrowed(super::super::native_utf8::borrowed_bytes(
                        values, row,
                    )))
                } else {
                    Value::Text(Cow::Borrowed(
                        values.utf8_slice_at(row).map_err(vortex_error)?,
                    ))
                }
            }
        };
        #[cfg(all(test, debug_assertions))]
        super::super::native_relational_observation::record(|counts| {
            counts.variable_result_visits += 1;
        });
        Ok(value)
    }

    /// Preserve the generic builder's complete validation/sizing pass, then
    /// copy the same immutable bytes without repeating typed value access.
    /// Checked Vortex arrays already reuse their UTF-8 validation receipt.
    /// Buffer allocation order and null/type errors stay at their old boundary.
    pub(in crate::local_primitives) fn copy_column(
        &self,
        dtype: &DType,
        rows: usize,
        allocator: &HostAllocatorRef,
        mut check_cancelled: impl FnMut() -> Result<()>,
    ) -> Result<ArrayRef> {
        if !matches!(dtype, DType::Utf8(_) | DType::Binary(_)) {
            return Err(failed("variable copy requires text or binary output"));
        }
        let mut validity = dtype
            .is_nullable()
            .then(|| allocate(allocator, rows.div_ceil(8), 1))
            .transpose()?;
        let mut offsets = allocate(allocator, multiply(add(rows, 1)?, 8)?, 8)?;
        let mut bytes = 0;
        for row in 0..rows {
            if row.is_multiple_of(1024) {
                check_cancelled()?;
            }
            bytes = add(
                bytes,
                self.value(row)?.variable_bytes().map_or(0, <[u8]>::len),
            )?;
        }
        let mut payload = allocate(allocator, bytes, 1)?;
        let mut end = 0;
        for row in 0..rows {
            if row.is_multiple_of(1024) {
                check_cancelled()?;
            }
            if let Some(value) = self.bytes_after_validation(row, dtype)? {
                if let Some(validity) = &mut validity {
                    set_bit(validity, row);
                }
                end = copy_bytes(&mut payload, end, value)?;
            } else if !dtype.is_nullable() {
                return Err(failed("null in a nonnullable output column"));
            }
            let observed = u64::try_from(end).map_err(vortex_error)?;
            offsets.as_mut_slice()[(row + 1) * 8..(row + 2) * 8]
                .copy_from_slice(&observed.to_ne_bytes());
        }
        let validity = validity
            .map(|buffer| finish_validity(buffer.freeze(), rows, allocator))
            .transpose()?
            .unwrap_or(Validity::NonNullable);
        if end != bytes {
            return Err(failed(
                "completed variable payload changed during construction",
            ));
        }
        finish_column(
            dtype,
            rows,
            offsets.freeze(),
            validity,
            Some(payload.freeze()),
            allocator,
        )
    }

    // Called only after every requested row passed value(). The native array
    // and its validity/buffers are immutable and remain owned by this accessor.
    // Returning bytes needs no unchecked str construction or source retention
    // in the output; the caller copies them before this borrow ends.
    fn bytes_after_validation(&self, row: usize, dtype: &DType) -> Result<Option<&[u8]>> {
        let bytes = match &self.kind {
            Kind::Empty => return Err(failed("empty variable output has no selected value")),
            Kind::Constant(Value::Null) => None,
            Kind::Constant(Value::SharedText(value)) if matches!(dtype, DType::Utf8(_)) => {
                Some(value.as_bytes())
            }
            Kind::Constant(Value::SharedBinary(value)) if matches!(dtype, DType::Binary(_)) => {
                Some(value.as_ref())
            }
            Kind::Views {
                values,
                valid,
                binary,
            } => {
                if !valid.value(row) {
                    None
                } else if *binary == matches!(dtype, DType::Binary(_)) {
                    Some(super::super::native_utf8::borrowed_bytes(values, row))
                } else {
                    return Err(failed("value differs from the declared output dtype"));
                }
            }
            Kind::Constant(_) => {
                return Err(failed("value differs from the declared output dtype"));
            }
        };
        Ok(bytes)
    }
}
