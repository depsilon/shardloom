// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 ShardLoom contributors

//! Borrowed native values and ordered protobuf result continuations.

use num_traits::ToBytes;
use vortex_error::VortexExpect;

use crate::proto::{dtype, scalar as pb};
use crate::scalar::{DecimalValue, ScalarValue};

use pb::scalar_value::Kind;

enum Parent<'a> {
    Tuple(
        std::slice::Iter<'a, Option<ScalarValue>>,
        Vec<pb::ScalarValue>,
    ),
    Union(u8),
    Variant(dtype::DType),
}

pub(super) fn write(mut value: Option<&ScalarValue>) -> pb::ScalarValue {
    let mut pending = Vec::new();
    'descend: loop {
        let mut result = match value {
            None => pb::ScalarValue {
                kind: Some(Kind::NullValue(0)),
            },
            Some(ScalarValue::Tuple(values)) => {
                let mut children = values.iter();
                let partial = Vec::with_capacity(values.len());
                if let Some(child) = children.next() {
                    pending.push(Parent::Tuple(children, partial));
                    value = child.as_ref();
                    continue;
                }
                tuple(partial)
            }
            Some(ScalarValue::Union(union)) => {
                pending.push(Parent::Union(union.type_id()));
                value = union.child_value();
                continue;
            }
            Some(ScalarValue::Variant(scalar)) => {
                let dtype = scalar
                    .dtype()
                    .try_into()
                    .vortex_expect("Failed to convert DType to proto");
                pending.push(Parent::Variant(dtype));
                value = scalar.value();
                continue;
            }
            Some(value) => leaf(value),
        };
        loop {
            result = match pending.pop() {
                None => return result,
                Some(Parent::Tuple(mut children, mut values)) => {
                    values.push(result);
                    if let Some(child) = children.next() {
                        pending.push(Parent::Tuple(children, values));
                        value = child.as_ref();
                        continue 'descend;
                    }
                    tuple(values)
                }
                Some(Parent::Union(type_id)) => pb::ScalarValue {
                    kind: Some(Kind::UnionValue(Box::new(pb::UnionValue {
                        type_id: u32::from(type_id),
                        value: Some(Box::new(result)),
                    }))),
                },
                Some(Parent::Variant(dtype)) => pb::ScalarValue {
                    kind: Some(Kind::VariantValue(Box::new(pb::Scalar {
                        dtype: Some(dtype),
                        value: Some(Box::new(result)),
                    }))),
                },
            };
        }
    }
}

fn tuple(values: Vec<pb::ScalarValue>) -> pb::ScalarValue {
    pb::ScalarValue {
        kind: Some(Kind::ListValue(pb::ListValue { values })),
    }
}

fn leaf(value: &ScalarValue) -> pb::ScalarValue {
    let kind = match value {
        ScalarValue::Bool(value) => Kind::BoolValue(*value),
        ScalarValue::Primitive(value) => return pb::ScalarValue::from(value),
        ScalarValue::Decimal(value) => Kind::BytesValue(match value {
            DecimalValue::I8(value) => value.to_le_bytes().to_vec(),
            DecimalValue::I16(value) => value.to_le_bytes().to_vec(),
            DecimalValue::I32(value) => value.to_le_bytes().to_vec(),
            DecimalValue::I64(value) => value.to_le_bytes().to_vec(),
            DecimalValue::I128(value) => value.to_le_bytes().to_vec(),
            DecimalValue::I256(value) => value.to_le_bytes().to_vec(),
        }),
        ScalarValue::Utf8(value) => Kind::StringValue(value.to_string()),
        ScalarValue::Binary(value) => Kind::BytesValue(value.to_vec()),
        ScalarValue::Tuple(_) | ScalarValue::Union(_) | ScalarValue::Variant(_) => unreachable!(),
    };
    pb::ScalarValue { kind: Some(kind) }
}
