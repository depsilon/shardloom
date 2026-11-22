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
    let mut output = Shape::default();
    let mut stack = vec![(value, 0_usize)];
    while let Some((node, depth)) = stack.pop() {
        context.check_cancelled()?;
        if depth > 24 {
            return Err(memory_error("native scalar transfer depth exceeds 24"));
        }
        match node {
            Some(ScalarValue::Tuple(values)) => {
                output.metadata_bytes = add(output.metadata_bytes, mul(values.len() as u64, 128)?)?;
                for child in values.iter().rev() {
                    stack.push((child.as_ref(), depth + 1));
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
    }
    Ok(output)
}

enum CopyDTypeFrame {
    List(vortex::array::dtype::Nullability),
    FixedSizeList(u32, vortex::array::dtype::Nullability),
    Struct {
        names: FieldNames,
        nullability: vortex::array::dtype::Nullability,
        pending: std::vec::IntoIter<DType>,
        built: Vec<DType>,
    },
}

pub(super) fn copy_dtype(root: &DType) -> Result<DType> {
    let mut stack = Vec::<CopyDTypeFrame>::new();
    let mut current = root.clone();
    loop {
        let mut completed = match &current {
            DType::Struct(fields, nullability) => {
                let names = fields
                    .names()
                    .iter()
                    .map(|name| name.as_ref().to_owned())
                    .collect::<FieldNames>();
                let mut pending = fields.fields().collect::<Vec<_>>().into_iter();
                if let Some(first) = pending.next() {
                    let total = pending.len() + 1;
                    stack.push(CopyDTypeFrame::Struct {
                        names,
                        nullability: *nullability,
                        pending,
                        built: Vec::with_capacity(total),
                    });
                    current = first;
                    continue;
                }
                DType::Struct(StructFields::new(names, Vec::new()), *nullability)
            }
            DType::List(child, nullability) => {
                stack.push(CopyDTypeFrame::List(*nullability));
                current = child.as_ref().clone();
                continue;
            }
            DType::FixedSizeList(child, size, nullability) => {
                stack.push(CopyDTypeFrame::FixedSizeList(*size, *nullability));
                current = child.as_ref().clone();
                continue;
            }
            #[cfg(feature = "native-binary-objects")]
            DType::Extension(_) if crate::native_binary_type::storage(&current).is_some() => {
                crate::native_binary_type::dtype(current.is_nullable()).map_err(native_error)?
            }
            DType::Extension(_) => native_temporal::vortex_dtype(
                &native_temporal::temporal_dtype(&current)
                    .ok_or_else(|| memory_error("native transfer extension is not admitted"))?,
                current.nullability(),
            )?,
            DType::Map(..) | DType::Union(..) | DType::Variant(..) => {
                return Err(memory_error(
                    "native transfer requires an admitted static dtype",
                ));
            }
            _ => current.clone(),
        };
        loop {
            let Some(frame) = stack.pop() else {
                return Ok(completed);
            };
            completed = match frame {
                CopyDTypeFrame::List(nullability) => DType::List(Arc::new(completed), nullability),
                CopyDTypeFrame::FixedSizeList(size, nullability) => {
                    DType::FixedSizeList(Arc::new(completed), size, nullability)
                }
                CopyDTypeFrame::Struct {
                    names,
                    nullability,
                    mut pending,
                    mut built,
                } => {
                    built.push(completed);
                    if let Some(next) = pending.next() {
                        stack.push(CopyDTypeFrame::Struct {
                            names,
                            nullability,
                            pending,
                            built,
                        });
                        current = next;
                        break;
                    }
                    DType::Struct(StructFields::new(names, built), nullability)
                }
            };
        }
    }
}

struct CopyTupleFrame<'a> {
    remaining: std::slice::Iter<'a, Option<ScalarValue>>,
    copied: Vec<Option<ScalarValue>>,
    bytes: u64,
}

impl Copier<'_, '_> {
    pub(super) fn copy_scalar(&mut self, scalar: &Scalar, dtype: DType) -> Result<(Scalar, u64)> {
        let (value, bytes) = self.copy_value(scalar.value())?;
        Ok((Scalar::try_new(dtype, value).map_err(native_error)?, bytes))
    }

    fn copy_value(&mut self, root: Option<&ScalarValue>) -> Result<(Option<ScalarValue>, u64)> {
        let mut stack = Vec::<CopyTupleFrame<'_>>::new();
        let mut current = root;
        loop {
            self.context.check_cancelled()?;
            let mut completed = match current {
                None => (None, 0_u64),
                Some(ScalarValue::Bool(value)) => (Some(ScalarValue::Bool(*value)), 1),
                Some(ScalarValue::Primitive(value)) => (
                    Some(ScalarValue::Primitive(*value)),
                    value.ptype().byte_width() as u64,
                ),
                Some(ScalarValue::Decimal(value)) => (
                    Some(ScalarValue::Decimal(*value)),
                    value.decimal_type().byte_width() as u64,
                ),
                Some(ScalarValue::Utf8(value)) => (
                    Some(ScalarValue::Utf8(
                        BufferString::try_from(self.copy_buffer(value.inner())?)
                            .map_err(native_error)?,
                    )),
                    value.len() as u64,
                ),
                Some(ScalarValue::Binary(value)) => (
                    Some(ScalarValue::Binary(self.copy_buffer(value)?)),
                    value.len() as u64,
                ),
                Some(ScalarValue::Tuple(values)) => {
                    let mut remaining = values.iter();
                    if let Some(first) = remaining.next() {
                        stack.push(CopyTupleFrame {
                            remaining,
                            copied: Vec::with_capacity(values.len()),
                            bytes: 0,
                        });
                        current = first.as_ref();
                        continue;
                    }
                    (Some(ScalarValue::Tuple(Vec::new())), 0)
                }
                Some(ScalarValue::Union(_) | ScalarValue::Variant(_)) => {
                    return Err(memory_error(
                        "native scalar transfer requires an admitted static dtype",
                    ));
                }
            };
            loop {
                let Some(mut frame) = stack.pop() else {
                    return Ok(completed);
                };
                frame.copied.push(completed.0);
                frame.bytes = add(frame.bytes, completed.1)?;
                if let Some(next) = frame.remaining.next() {
                    stack.push(frame);
                    current = next.as_ref();
                    break;
                }
                completed = (Some(ScalarValue::Tuple(frame.copied)), frame.bytes);
            }
        }
    }
}
