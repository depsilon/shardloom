// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Ordered scalar reconstruction from the existing protobuf object model.

use vortex_error::{VortexExpect, VortexResult, vortex_bail, vortex_ensure_eq, vortex_err};
use vortex_session::VortexSession;

use crate::dtype::{DType, StructFields};
use crate::proto::scalar as pb;
use crate::scalar::{Scalar, ScalarValue, UnionValue};

use pb::scalar_value::Kind;

enum Children {
    Repeated(DType),
    Struct(StructFields),
}

impl Children {
    fn dtype(&self, index: usize) -> DType {
        match self {
            Self::Repeated(dtype) => dtype.clone(),
            Self::Struct(fields) => fields
                .field_by_index(index)
                .vortex_expect("struct field index must be in bounds"),
        }
    }
}

enum Parent<'a> {
    Tuple {
        values: Vec<Option<ScalarValue>>,
        children: Children,
        remaining: std::slice::Iter<'a, pb::ScalarValue>,
    },
    Union {
        type_id: u8,
        dtype: DType,
    },
    Variant(DType),
}

#[derive(Default)]
struct Pending<'a>(Vec<Parent<'a>>);

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        // Partial native values and Variant schemas unwind inside-out.
        while self.0.pop().is_some() {}
    }
}

enum Step<'a> {
    Child(&'a pb::ScalarValue, DType, Parent<'a>),
    Ready(Option<ScalarValue>),
}

pub(super) fn read(
    value: &pb::ScalarValue,
    dtype: &DType,
    session: &VortexSession,
) -> VortexResult<Option<ScalarValue>> {
    // DType's Clone shares its native child owners; it does not clone a tree.
    let mut step = enter(value, dtype, session)?;
    let mut pending = Pending::default();
    loop {
        step = match step {
            Step::Child(child, dtype, parent) => {
                pending.0.push(parent);
                enter(child, &dtype, session)?
            }
            Step::Ready(value) => match pending.0.pop() {
                None => return Ok(value),
                Some(parent) => complete(parent, value)?,
            },
        };
    }
}

fn enter<'a>(
    value: &'a pb::ScalarValue,
    dtype: &DType,
    session: &VortexSession,
) -> VortexResult<Step<'a>> {
    let kind = value
        .kind
        .as_ref()
        .ok_or_else(|| vortex_err!("Scalar value missing kind"))?;
    // Preserve exactly ONE extension unwrap per original value invocation.
    let dtype = match dtype {
        DType::Extension(extension) => extension.storage_dtype(),
        _ => dtype,
    };
    Ok(match kind {
        Kind::ListValue(value) => list(value, dtype)?,
        Kind::UnionValue(value) => union(value, dtype)?,
        Kind::VariantValue(value) => {
            if !matches!(dtype, DType::Variant(_)) {
                vortex_bail!("expected non-Variant scalar proto for dtype {dtype}");
            }
            let dtype = DType::from_proto(
                value
                    .dtype
                    .as_ref()
                    .ok_or_else(|| vortex_err!("Scalar missing dtype"))?,
                session,
            )?;
            let child = value
                .value
                .as_deref()
                .ok_or_else(|| vortex_err!("Scalar missing value"))?;
            Step::Child(child, dtype.clone(), Parent::Variant(dtype))
        }
        _ => Step::Ready(leaf(kind, dtype)?),
    })
}

fn list<'a>(value: &'a pb::ListValue, dtype: &DType) -> VortexResult<Step<'a>> {
    let children = match dtype {
        DType::List(dtype, _) | DType::FixedSizeList(dtype, ..) => {
            Children::Repeated(dtype.as_ref().clone())
        }
        DType::Struct(fields, _) => {
            vortex_ensure_eq!(
                value.values.len(),
                fields.nfields(),
                "ListValue has the wrong number of struct fields"
            );
            Children::Struct(fields.clone())
        }
        // Construct this once, even for an empty map, as in the original reader.
        DType::Map(map, _) => Children::Repeated(map.entries_dtype()),
        _ => vortex_bail!("expected a tuple-backed dtype for ListValue, got {dtype}"),
    };
    let mut remaining = value.values.iter();
    Ok(if let Some(child) = remaining.next() {
        let dtype = children.dtype(0);
        Step::Child(
            child,
            dtype,
            Parent::Tuple {
                values: Vec::new(),
                children,
                remaining,
            },
        )
    } else {
        Step::Ready(Some(ScalarValue::Tuple(Vec::new())))
    })
}

fn union<'a>(value: &'a pb::UnionValue, dtype: &DType) -> VortexResult<Step<'a>> {
    let DType::Union(variants, _) = dtype else {
        vortex_bail!("expected Union dtype for UnionValue, got {dtype}");
    };
    let type_id = u8::try_from(value.type_id)
        .map_err(|_| vortex_err!("union type ID {} is outside the u8 range", value.type_id))?;
    let child_index = variants.tag_to_child_index(type_id).ok_or_else(|| {
        vortex_err!(
            "union type ID {type_id} is not present in {:?}",
            variants.type_ids()
        )
    })?;
    let dtype = variants
        .variant_by_index(child_index)
        .ok_or_else(|| vortex_err!("union type ID {type_id} resolved out of bounds"))?;
    let child = value
        .value
        .as_deref()
        .ok_or_else(|| vortex_err!("UnionValue missing child value"))?;
    Ok(Step::Child(
        child,
        dtype.clone(),
        Parent::Union { type_id, dtype },
    ))
}

fn complete(parent: Parent<'_>, value: Option<ScalarValue>) -> VortexResult<Step<'_>> {
    Ok(match parent {
        Parent::Tuple {
            mut values,
            children,
            mut remaining,
        } => {
            values.push(value);
            if let Some(child) = remaining.next() {
                let dtype = children.dtype(values.len());
                Step::Child(
                    child,
                    dtype,
                    Parent::Tuple {
                        values,
                        children,
                        remaining,
                    },
                )
            } else {
                Step::Ready(Some(ScalarValue::Tuple(values)))
            }
        }
        Parent::Union { type_id, dtype } => {
            Scalar::validate(&dtype, value.as_ref()).map_err(|error| {
                error.with_context(format!(
                    "union type ID {type_id} has invalid child for dtype {dtype}"
                ))
            })?;
            Step::Ready(Some(ScalarValue::Union(UnionValue::new(type_id, value))))
        }
        Parent::Variant(dtype) => Step::Ready(Some(ScalarValue::Variant(Box::new(
            Scalar::try_new(dtype, value)?,
        )))),
    })
}

fn leaf(kind: &Kind, dtype: &DType) -> VortexResult<Option<ScalarValue>> {
    Ok(Some(match kind {
        Kind::NullValue(_) => return Ok(None),
        Kind::BoolValue(value) => super::bool_from_proto(*value, dtype)?,
        Kind::Int64Value(value) => super::int64_from_proto(*value, dtype)?,
        Kind::Uint64Value(value) => super::uint64_from_proto(*value, dtype)?,
        Kind::F16Value(value) => super::f16_from_proto(*value, dtype)?,
        Kind::F32Value(value) => super::f32_from_proto(*value, dtype)?,
        Kind::F64Value(value) => super::f64_from_proto(*value, dtype)?,
        Kind::StringValue(value) => super::string_from_proto(value, dtype)?,
        Kind::BytesValue(value) => super::bytes_from_proto(value, dtype)?,
        Kind::ListValue(_) | Kind::UnionValue(_) | Kind::VariantValue(_) => unreachable!(),
    }))
}
