//! Bounded metadata and scalar copies used only after receiving admission.

use super::{Copier, add, memory_error, mul, native_error};
use crate::{native_temporal, resident_session::NativeExecutionContext};
use shardloom_core::Result;
use std::sync::Arc;
use vortex::{
    array::{
        dtype::{DType, FieldNames, StructFields},
        scalar::{Scalar, ScalarValue},
    },
    buffer::BufferString,
};

#[derive(Default)]
pub(super) struct Shape {
    pub(super) buffers: u64,
    pub(super) metadata_bytes: u64,
}

pub(super) fn shape(
    value: Option<&ScalarValue>,
    context: &NativeExecutionContext<'_>,
) -> Result<Shape> {
    fn visit(
        value: Option<&ScalarValue>,
        depth: usize,
        output: &mut Shape,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        context.check_cancelled()?;
        if depth > 24 {
            return Err(memory_error("native scalar transfer depth exceeds 24"));
        }
        match value {
            Some(ScalarValue::Tuple(values)) => {
                output.metadata_bytes = add(output.metadata_bytes, mul(values.len() as u64, 128)?)?;
                for value in values {
                    visit(value.as_ref(), depth + 1, output, context)?;
                }
            }
            Some(ScalarValue::Utf8(_) | ScalarValue::Binary(_)) => {
                output.buffers = add(output.buffers, 1)?;
            }
            Some(ScalarValue::Union(_) | ScalarValue::Variant(_)) => {
                return Err(memory_error(
                    "native scalar transfer requires an admitted static dtype",
                ));
            }
            _ => {}
        }
        Ok(())
    }
    let mut output = Shape::default();
    visit(value, 0, &mut output, context)?;
    Ok(output)
}

pub(super) fn copy_dtype(dtype: &DType) -> Result<DType> {
    Ok(match dtype {
        DType::Struct(fields, nullability) => {
            let names = fields
                .names()
                .iter()
                .map(|name| name.as_ref().to_owned())
                .collect::<FieldNames>();
            let types = fields
                .fields()
                .map(|dtype| copy_dtype(&dtype))
                .collect::<Result<Vec<_>>>()?;
            DType::Struct(StructFields::new(names, types), *nullability)
        }
        DType::List(child, nullability) => DType::List(Arc::new(copy_dtype(child)?), *nullability),
        DType::FixedSizeList(child, size, nullability) => {
            DType::FixedSizeList(Arc::new(copy_dtype(child)?), *size, *nullability)
        }
        DType::Extension(_) => native_temporal::vortex_dtype(
            &native_temporal::temporal_dtype(dtype)
                .ok_or_else(|| memory_error("native transfer extension is not admitted"))?,
            dtype.nullability(),
        )?,
        DType::Map(..) | DType::Union(..) | DType::Variant(..) => {
            return Err(memory_error(
                "native transfer requires an admitted static dtype",
            ));
        }
        _ => dtype.clone(),
    })
}

impl Copier<'_, '_> {
    pub(super) fn copy_scalar(&mut self, scalar: &Scalar, dtype: DType) -> Result<(Scalar, u64)> {
        let (value, bytes) = self.copy_value(scalar.value())?;
        Ok((Scalar::try_new(dtype, value).map_err(native_error)?, bytes))
    }

    fn copy_value(&mut self, value: Option<&ScalarValue>) -> Result<(Option<ScalarValue>, u64)> {
        self.context.check_cancelled()?;
        let Some(value) = value else {
            return Ok((None, 0));
        };
        let (value, bytes) = match value {
            ScalarValue::Bool(value) => (ScalarValue::Bool(*value), 1),
            ScalarValue::Primitive(value) => (
                ScalarValue::Primitive(*value),
                value.ptype().byte_width() as u64,
            ),
            ScalarValue::Decimal(value) => (
                ScalarValue::Decimal(*value),
                value.decimal_type().byte_width() as u64,
            ),
            ScalarValue::Utf8(value) => (
                ScalarValue::Utf8(
                    BufferString::try_from(self.copy_buffer(value.inner())?)
                        .map_err(native_error)?,
                ),
                value.len() as u64,
            ),
            ScalarValue::Binary(value) => (
                ScalarValue::Binary(self.copy_buffer(value)?),
                value.len() as u64,
            ),
            ScalarValue::Tuple(values) => {
                let mut copied = Vec::with_capacity(values.len());
                let mut bytes = 0;
                for value in values {
                    let (value, size) = self.copy_value(value.as_ref())?;
                    copied.push(value);
                    bytes = add(bytes, size)?;
                }
                (ScalarValue::Tuple(copied), bytes)
            }
            ScalarValue::Union(_) | ScalarValue::Variant(_) => {
                return Err(memory_error(
                    "native scalar transfer requires an admitted static dtype",
                ));
            }
        };
        Ok((Some(value), bytes))
    }
}
