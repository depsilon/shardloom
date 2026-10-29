//! Borrow selected native text/bytes while constructing independent output.
//! Selection and canonicalization remain the caller's boundary. Constants keep
//! one native value; no dictionary domain or row-sized expansion is introduced.

use super::{Value, failed, scalar_value, vortex_error};
use shardloom_core::Result;
use std::borrow::Cow;
use vortex::{
    array::{
        ArrayRef, ExecutionCtx,
        arrays::{Constant, VarBinViewArray, varbinview::VarBinViewArrayExt as _},
        dtype::DType,
    },
    mask::Mask,
};

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
}
