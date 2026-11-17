//! Ordered field/item cursors preserve validation and null-default semantics.

use serde_json::{Map, Value as Json};
use shardloom_core::Result;
use vortex::array::dtype::{DType, StructFields};

use super::super::{
    failed, leaf, multiply,
    result_batch::{self, Value},
};
use super::{Layout, ReservedVec, add, first_child, storage};

#[derive(Clone, Copy)]
enum Input<'a> {
    Value(Option<&'a Json>),
    Defaults(usize),
}

#[derive(Clone, Copy)]
struct Visit<'a> {
    index: Option<usize>,
    dtype: &'a DType,
    input: Input<'a>,
}

enum Fields<'a> {
    Values(&'a Map<String, Json>),
    Defaults(usize),
}

enum Frame<'a> {
    Struct {
        fields: &'a StructFields,
        values: Fields<'a>,
        field: usize,
        child: usize,
    },
    List {
        dtype: &'a DType,
        values: &'a [Json],
        next: usize,
        child: usize,
    },
    One(Option<Visit<'a>>),
}

impl<'a> Frame<'a> {
    fn next(&mut self, layout: &Layout) -> Result<Option<Visit<'a>>> {
        match self {
            Self::Struct {
                fields,
                values,
                field,
                child,
            } => {
                let Some(dtype) = fields.field_dtypes().nth(*field) else {
                    return Ok(None);
                };
                let input = match values {
                    Fields::Values(values) => Input::Value(Some(
                        values
                            .get(fields.names()[*field].as_ref())
                            .ok_or_else(|| failed("struct input is missing a declared field"))?,
                    )),
                    Fields::Defaults(count) => Input::Defaults(*count),
                };
                let index = *child;
                *child = layout.nodes.values[index].end;
                *field += 1;
                Ok(Some(Visit {
                    index: Some(index),
                    dtype,
                    input,
                }))
            }
            Self::List {
                dtype,
                values,
                next,
                child,
            } => {
                let Some(value) = values.get(*next) else {
                    return Ok(None);
                };
                *next += 1;
                Ok(Some(Visit {
                    index: Some(*child),
                    dtype,
                    input: Input::Value(Some(value)),
                }))
            }
            Self::One(visit) => Ok(visit.take()),
        }
    }
}

impl Layout {
    pub(in crate::local_primitives) fn observe(
        &mut self,
        dtype: &DType,
        value: Option<&Json>,
    ) -> Result<()> {
        let mut pending = ReservedVec::new(&self.memory)?;
        let mut current = Visit {
            index: None,
            dtype,
            input: Input::Value(value),
        };
        loop {
            if let Some(frame) = self.enter(current)? {
                pending.push(frame)?;
            }
            loop {
                let Some(frame) = pending.values.last_mut() else {
                    return Ok(());
                };
                if let Some(visit) = frame.next(self)? {
                    current = visit;
                    break;
                }
                pending.values.pop();
            }
        }
    }

    fn enter<'a>(&mut self, visit: Visit<'a>) -> Result<Option<Frame<'a>>> {
        let Visit {
            index,
            dtype,
            input,
        } = visit;
        let Input::Value(value) = input else {
            let Input::Defaults(count) = input else {
                unreachable!()
            };
            return self.defaults(index, dtype, count);
        };
        validate_rich(dtype, value)?;
        let dtype = storage(dtype);
        let Some(present) = value.filter(|value| !value.is_null()) else {
            if value.is_some() && !dtype.is_nullable() {
                return Err(failed("null in a nonnullable typed input"));
            }
            return self.defaults(index, dtype, 1);
        };
        let node = self.node_mut(index);
        node.rows = add(node.rows, 1)?;
        Ok(match dtype {
            DType::Struct(fields, _) => {
                let values = present
                    .as_object()
                    .filter(|values| values.len() == fields.nfields())
                    .ok_or_else(|| {
                        failed("struct input must match its complete declared fields")
                    })?;
                Some(Frame::Struct {
                    fields,
                    values: Fields::Values(values),
                    field: 0,
                    child: first_child(index),
                })
            }
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => {
                let values = present
                    .as_array()
                    .ok_or_else(|| failed("list input requires an array"))?;
                if let DType::FixedSizeList(_, size, _) = dtype
                    && values.len() != *size as usize
                {
                    return Err(failed("fixed-size list input has the wrong width"));
                }
                Some(Frame::List {
                    dtype: child,
                    values,
                    next: 0,
                    child: first_child(index),
                })
            }
            _ => {
                let value = leaf(dtype, Some(present))?;
                let bytes = match &value {
                    Value::Text(value) => value.len(),
                    Value::Binary(value) => value.len(),
                    Value::Bool(_) | Value::Null => 0,
                    _ => result_batch::width(dtype)?,
                };
                node.copied = add(node.copied, bytes)?;
                if matches!(dtype, DType::Utf8(_) | DType::Binary(_)) {
                    node.variable = add(node.variable, bytes)?;
                }
                None
            }
        })
    }

    fn defaults<'a>(
        &mut self,
        index: Option<usize>,
        dtype: &'a DType,
        count: usize,
    ) -> Result<Option<Frame<'a>>> {
        let dtype = storage(dtype);
        let node = self.node_mut(index);
        node.rows = add(node.rows, count)?;
        Ok(match dtype {
            DType::Struct(fields, _) => Some(Frame::Struct {
                fields,
                values: Fields::Defaults(count),
                field: 0,
                child: first_child(index),
            }),
            DType::FixedSizeList(child, size, _) => Some(Frame::One(Some(Visit {
                index: Some(first_child(index)),
                dtype: child,
                input: Input::Defaults(multiply(count, *size as usize)?),
            }))),
            _ => None,
        })
    }
}

fn validate_rich(dtype: &DType, value: Option<&Json>) -> Result<()> {
    if let Some(value) = value.filter(|value| !value.is_null())
        && let Some(metadata) = crate::native_spatial::metadata(dtype)
    {
        crate::native_spatial::values::validate_json(metadata.kind(), value)?;
    }
    if let Some(value) = value.filter(|value| !value.is_null())
        && let Some(metadata) = crate::native_vector::metadata(dtype)
    {
        metadata
            .validate_json(value)
            .map_err(super::super::super::vortex_error)?;
    }
    if let Some(value) = value.filter(|value| !value.is_null())
        && let DType::Extension(extension) = dtype
        && let Some(metadata) =
            extension.metadata_opt::<crate::native_variable_tensor::VariableShapeTensor>()
    {
        let data = value
            .get("data")
            .and_then(Json::as_array)
            .ok_or_else(|| failed("present variable tensor requires a data list"))?;
        let shape = value
            .get("shape")
            .and_then(Json::as_array)
            .ok_or_else(|| failed("present variable tensor requires a shape list"))?;
        let rank = crate::native_variable_tensor::rank(extension.storage_dtype())
            .map_err(super::super::super::vortex_error)?;
        metadata
            .validate_dimensions(
                rank as usize,
                shape.iter().map(|value| {
                    value
                        .as_i64()
                        .and_then(|n| i32::try_from(n).ok())
                        .ok_or_else(|| {
                            vortex::error::vortex_err!(
                                "tensor dimensions require nonnull int32 values"
                            )
                        })
                }),
                data.len(),
            )
            .map_err(super::super::super::vortex_error)?;
    }
    Ok(())
}
