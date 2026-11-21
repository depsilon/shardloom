// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

// Original recursive converters: names and recursive calls adapted for tests.
// Mechanically checked by check_scalar_proto_reference.py.

use num_traits::ToBytes;
use num_traits::ToPrimitive;
use vortex_buffer::BufferString;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_session::VortexSession;

use crate::dtype::DType;
use crate::dtype::PType;
use crate::dtype::half::f16;
use crate::dtype::i256;
use crate::proto::scalar as pb;
use crate::proto::scalar::ListValue;
use crate::proto::scalar::UnionValue as PbUnionValue;
use crate::proto::scalar::scalar_value::Kind;
use crate::scalar::DecimalValue;
use crate::scalar::PValue;
use crate::scalar::Scalar;
use crate::scalar::ScalarValue;
use crate::scalar::UnionValue;

use super::dtype_reference;

pub(super) fn scalar_write(value: &Scalar) -> pb::Scalar {
    pb::Scalar {
        dtype: Some(
            dtype_reference::write(value.dtype()).vortex_expect("Failed to convert DType to proto"),
        ),
        value: Some(Box::new(optional_write(value.value()))),
    }
}

pub(super) fn optional_write(this: Option<&ScalarValue>) -> pb::ScalarValue {
    match this {
        None => pb::ScalarValue {
            kind: Some(Kind::NullValue(0)),
        },
        Some(this) => write(this),
    }
}

pub(super) fn write(value: &ScalarValue) -> pb::ScalarValue {
    match value {
        ScalarValue::Bool(v) => pb::ScalarValue {
            kind: Some(Kind::BoolValue(*v)),
        },
        ScalarValue::Primitive(v) => primitive_write(v),
        ScalarValue::Decimal(v) => {
            let inner_value = match v {
                DecimalValue::I8(v) => v.to_le_bytes().to_vec(),
                DecimalValue::I16(v) => v.to_le_bytes().to_vec(),
                DecimalValue::I32(v) => v.to_le_bytes().to_vec(),
                DecimalValue::I64(v) => v.to_le_bytes().to_vec(),
                DecimalValue::I128(v128) => v128.to_le_bytes().to_vec(),
                DecimalValue::I256(v256) => v256.to_le_bytes().to_vec(),
            };

            pb::ScalarValue {
                kind: Some(Kind::BytesValue(inner_value)),
            }
        }
        ScalarValue::Utf8(v) => pb::ScalarValue {
            kind: Some(Kind::StringValue(v.to_string())),
        },
        ScalarValue::Binary(v) => pb::ScalarValue {
            kind: Some(Kind::BytesValue(v.to_vec())),
        },
        ScalarValue::Tuple(v) => {
            let mut values = Vec::with_capacity(v.len());
            for elem in v.iter() {
                values.push(optional_write(elem.as_ref()));
            }
            pb::ScalarValue {
                kind: Some(Kind::ListValue(ListValue { values })),
            }
        }
        ScalarValue::Union(v) => pb::ScalarValue {
            kind: Some(Kind::UnionValue(Box::new(PbUnionValue {
                type_id: u32::from(v.type_id()),
                value: Some(Box::new(optional_write(v.child_value()))),
            }))),
        },
        ScalarValue::Variant(v) => pb::ScalarValue {
            kind: Some(Kind::VariantValue(Box::new(scalar_write(v.as_ref())))),
        },
    }
}

fn primitive_write(value: &PValue) -> pb::ScalarValue {
    match value {
        PValue::I8(v) => pb::ScalarValue {
            kind: Some(Kind::Int64Value(*v as i64)),
        },
        PValue::I16(v) => pb::ScalarValue {
            kind: Some(Kind::Int64Value(*v as i64)),
        },
        PValue::I32(v) => pb::ScalarValue {
            kind: Some(Kind::Int64Value(*v as i64)),
        },
        PValue::I64(v) => pb::ScalarValue {
            kind: Some(Kind::Int64Value(*v)),
        },
        PValue::U8(v) => pb::ScalarValue {
            kind: Some(Kind::Uint64Value(*v as u64)),
        },
        PValue::U16(v) => pb::ScalarValue {
            kind: Some(Kind::Uint64Value(*v as u64)),
        },
        PValue::U32(v) => pb::ScalarValue {
            kind: Some(Kind::Uint64Value(*v as u64)),
        },
        PValue::U64(v) => pb::ScalarValue {
            kind: Some(Kind::Uint64Value(*v)),
        },
        PValue::F16(v) => pb::ScalarValue {
            kind: Some(Kind::F16Value(v.to_bits() as u64)),
        },
        PValue::F32(v) => pb::ScalarValue {
            kind: Some(Kind::F32Value(*v)),
        },
        PValue::F64(v) => pb::ScalarValue {
            kind: Some(Kind::F64Value(*v)),
        },
    }
}

pub(super) fn scalar_read(value: &pb::Scalar, session: &VortexSession) -> VortexResult<Scalar> {
    let dtype = dtype_reference::read(
        value
            .dtype
            .as_ref()
            .ok_or_else(|| vortex_err!("Scalar missing dtype"))?,
        session,
    )?;

    let pb_scalar_value: &pb::ScalarValue = value
        .value
        .as_ref()
        .ok_or_else(|| vortex_err!("Scalar missing value"))?;

    let value: Option<ScalarValue> = read(pb_scalar_value, &dtype, session)?;

    Scalar::try_new(dtype, value)
}

pub(super) fn read(
    value: &pb::ScalarValue,
    dtype: &DType,
    session: &VortexSession,
) -> VortexResult<Option<ScalarValue>> {
    let kind = value
        .kind
        .as_ref()
        .ok_or_else(|| vortex_err!("Scalar value missing kind"))?;

    // `DType::Extension` store their serialized values using the storage `DType`.
    let dtype = match dtype {
        DType::Extension(ext) => ext.storage_dtype(),
        _ => dtype,
    };

    Ok(match kind {
        Kind::NullValue(_) => None,
        Kind::BoolValue(v) => Some(bool_from_proto(*v, dtype)?),
        Kind::Int64Value(v) => Some(int64_from_proto(*v, dtype)?),
        Kind::Uint64Value(v) => Some(uint64_from_proto(*v, dtype)?),
        Kind::F16Value(v) => Some(f16_from_proto(*v, dtype)?),
        Kind::F32Value(v) => Some(f32_from_proto(*v, dtype)?),
        Kind::F64Value(v) => Some(f64_from_proto(*v, dtype)?),
        Kind::StringValue(s) => Some(string_from_proto(s, dtype)?),
        Kind::BytesValue(b) => Some(bytes_from_proto(b, dtype)?),
        Kind::ListValue(v) => Some(list_from_proto(v, dtype, session)?),
        Kind::UnionValue(v) => Some(union_from_proto(v, dtype, session)?),
        Kind::VariantValue(v) => match dtype {
            DType::Variant(_) => Some(ScalarValue::Variant(Box::new(scalar_read(v, session)?))),
            _ => vortex_bail!("expected non-Variant scalar proto for dtype {dtype}"),
        },
    })
}

/// Deserialize a [`ScalarValue::Bool`] from a protobuf `BoolValue`.
fn bool_from_proto(v: bool, dtype: &DType) -> VortexResult<ScalarValue> {
    vortex_ensure!(
        dtype.is_boolean(),
        "expected Bool dtype for BoolValue, got {dtype}"
    );

    Ok(ScalarValue::Bool(v))
}

/// Deserialize a [`ScalarValue::Primitive`] from a protobuf `Int64Value`.
///
/// Protobuf consolidates all signed integers into `i64`, so we narrow back to the original
/// type using the provided [`DType`].
fn int64_from_proto(v: i64, dtype: &DType) -> VortexResult<ScalarValue> {
    vortex_ensure!(
        dtype.is_primitive(),
        "expected Primitive dtype for Int64Value, got {dtype}"
    );

    let pvalue = match dtype.as_ptype() {
        PType::I8 => v.to_i8().map(PValue::I8),
        PType::I16 => v.to_i16().map(PValue::I16),
        PType::I32 => v.to_i32().map(PValue::I32),
        PType::I64 => Some(PValue::I64(v)),
        // It was previously possible for unsigned types to get their stats serialised as signed,
        // so we allow casting back to unsigned for backwards compatibility.
        PType::U8 => v.to_u8().map(PValue::U8),
        PType::U16 => v.to_u16().map(PValue::U16),
        PType::U32 => v.to_u32().map(PValue::U32),
        PType::U64 => v.to_u64().map(PValue::U64),
        ftype @ (PType::F16 | PType::F32 | PType::F64) => vortex_bail!(
            "expected signed integer ptype for serialized Int64Value, got float {ftype}"
        ),
    }
    .ok_or_else(|| vortex_err!("Int64 value {v} out of range for dtype {dtype}"))?;

    Ok(ScalarValue::Primitive(pvalue))
}

/// Deserialize a [`ScalarValue::Primitive`] from a protobuf `Uint64Value`.
///
/// Protobuf consolidates all unsigned integers into `u64`, so we narrow back to the original
/// type using the provided [`DType`]. Also handles the backwards-compatible case where `f16`
/// values were serialized as `u64` (via `f16::to_bits() as u64`).
fn uint64_from_proto(v: u64, dtype: &DType) -> VortexResult<ScalarValue> {
    vortex_ensure!(
        dtype.is_primitive(),
        "expected Primitive dtype for Uint64Value, got {dtype}"
    );

    let pvalue = match dtype.as_ptype() {
        PType::U8 => v.to_u8().map(PValue::U8),
        PType::U16 => v.to_u16().map(PValue::U16),
        PType::U32 => v.to_u32().map(PValue::U32),
        PType::U64 => Some(PValue::U64(v)),
        // It was previously possible for signed types to get their stats serialised as unsigned,
        // so we allow casting back to signed for backwards compatibility.
        PType::I8 => v.to_i8().map(PValue::I8),
        PType::I16 => v.to_i16().map(PValue::I16),
        PType::I32 => v.to_i32().map(PValue::I32),
        PType::I64 => v.to_i64().map(PValue::I64),
        // f16 values used to be serialized as u64, so we need to be able to read an f16 from a u64.
        PType::F16 => v.to_u16().map(f16::from_bits).map(PValue::F16),
        ftype @ (PType::F32 | PType::F64) => {
            vortex_bail!("expected unsigned integer ptype for serialized Uint64Value, got {ftype}")
        }
    }
    .ok_or_else(|| vortex_err!("Uint64 value {v} out of range for dtype {dtype}"))?;

    Ok(ScalarValue::Primitive(pvalue))
}

/// Deserialize a [`ScalarValue::Primitive`] from a protobuf `F16Value`.
fn f16_from_proto(v: u64, dtype: &DType) -> VortexResult<ScalarValue> {
    vortex_ensure!(
        matches!(dtype, DType::Primitive(PType::F16, _)),
        "expected F16 dtype for F16Value, got {dtype}"
    );

    let bits = u16::try_from(v)
        .map_err(|_| vortex_err!("f16 bitwise representation has more than 16 bits: {v}"))?;

    Ok(ScalarValue::Primitive(PValue::F16(f16::from_bits(bits))))
}

/// Deserialize a [`ScalarValue::Primitive`] from a protobuf `F32Value`.
fn f32_from_proto(v: f32, dtype: &DType) -> VortexResult<ScalarValue> {
    vortex_ensure!(
        matches!(dtype, DType::Primitive(PType::F32, _)),
        "expected F32 dtype for F32Value, got {dtype}"
    );

    Ok(ScalarValue::Primitive(PValue::F32(v)))
}

/// Deserialize a [`ScalarValue::Primitive`] from a protobuf `F64Value`.
fn f64_from_proto(v: f64, dtype: &DType) -> VortexResult<ScalarValue> {
    vortex_ensure!(
        matches!(dtype, DType::Primitive(PType::F64, _)),
        "expected F64 dtype for F64Value, got {dtype}"
    );

    Ok(ScalarValue::Primitive(PValue::F64(v)))
}

/// Deserialize a [`ScalarValue::Utf8`] or [`ScalarValue::Binary`] from a protobuf
/// `StringValue`.
fn string_from_proto(s: &str, dtype: &DType) -> VortexResult<ScalarValue> {
    match dtype {
        DType::Utf8(_) => Ok(ScalarValue::Utf8(BufferString::from(s))),
        DType::Binary(_) => Ok(ScalarValue::Binary(ByteBuffer::copy_from(s.as_bytes()))),
        _ => vortex_bail!("expected Utf8 or Binary dtype for StringValue, got {dtype}"),
    }
}

/// Deserialize a [`ScalarValue`] from a protobuf bytes and a `DType`.
///
/// Handles [`Utf8`](ScalarValue::Utf8), [`Binary`](ScalarValue::Binary), and
/// [`Decimal`](ScalarValue::Decimal) dtypes.
fn bytes_from_proto(bytes: &[u8], dtype: &DType) -> VortexResult<ScalarValue> {
    match dtype {
        DType::Utf8(_) => Ok(ScalarValue::Utf8(BufferString::try_from(bytes)?)),
        DType::Binary(_) => Ok(ScalarValue::Binary(ByteBuffer::copy_from(bytes))),
        // TODO(connor): This is incorrect, we need to verify this matches the inner decimal_dtype.
        DType::Decimal(..) => Ok(ScalarValue::Decimal(match bytes.len() {
            1 => DecimalValue::I8(bytes[0] as i8),
            2 => DecimalValue::I16(i16::from_le_bytes(
                bytes
                    .try_into()
                    .ok()
                    .vortex_expect("Buffer has invalid number of bytes"),
            )),
            4 => DecimalValue::I32(i32::from_le_bytes(
                bytes
                    .try_into()
                    .ok()
                    .vortex_expect("Buffer has invalid number of bytes"),
            )),
            8 => DecimalValue::I64(i64::from_le_bytes(
                bytes
                    .try_into()
                    .ok()
                    .vortex_expect("Buffer has invalid number of bytes"),
            )),
            16 => DecimalValue::I128(i128::from_le_bytes(
                bytes
                    .try_into()
                    .ok()
                    .vortex_expect("Buffer has invalid number of bytes"),
            )),
            32 => DecimalValue::I256(i256::from_le_bytes(
                bytes
                    .try_into()
                    .ok()
                    .vortex_expect("Buffer has invalid number of bytes"),
            )),
            l => vortex_bail!("invalid decimal byte length: {l}"),
        })),
        _ => vortex_bail!("expected Utf8, Binary, or Decimal dtype for BytesValue, got {dtype}"),
    }
}

/// Deserialize a [`ScalarValue::Tuple`] from a protobuf `ListValue`.
fn list_from_proto(
    v: &ListValue,
    dtype: &DType,
    session: &VortexSession,
) -> VortexResult<ScalarValue> {
    let values = match dtype {
        DType::List(element_dtype, _) | DType::FixedSizeList(element_dtype, ..) => v
            .values
            .iter()
            .map(|elem| read(elem, element_dtype.as_ref(), session))
            .collect::<VortexResult<Vec<_>>>()?,
        DType::Struct(fields, _) => {
            vortex_ensure_eq!(
                v.values.len(),
                fields.nfields(),
                "ListValue has the wrong number of struct fields"
            );

            v.values
                .iter()
                .zip(fields.fields())
                .map(|(value, field_dtype)| read(value, &field_dtype, session))
                .collect::<VortexResult<Vec<_>>>()?
        }
        DType::Map(map, _) => {
            let entry_dtype = map.entries_dtype();
            v.values
                .iter()
                .map(|entry| read(entry, &entry_dtype, session))
                .collect::<VortexResult<Vec<_>>>()?
        }
        _ => vortex_bail!("expected a tuple-backed dtype for ListValue, got {dtype}"),
    };
    Ok(ScalarValue::Tuple(values))
}

/// Deserialize a present union scalar value.
fn union_from_proto(
    value: &PbUnionValue,
    dtype: &DType,
    session: &VortexSession,
) -> VortexResult<ScalarValue> {
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

    let child_dtype = variants
        .variant_by_index(child_index)
        .ok_or_else(|| vortex_err!("union type ID {type_id} resolved out of bounds"))?;

    let child_proto = value
        .value
        .as_deref()
        .ok_or_else(|| vortex_err!("UnionValue missing child value"))?;

    let child_value = read(child_proto, &child_dtype, session)?;
    Scalar::validate(&child_dtype, child_value.as_ref()).map_err(|error| {
        error.with_context(format!(
            "union type ID {type_id} has invalid child for dtype {child_dtype}"
        ))
    })?;

    Ok(ScalarValue::Union(UnionValue::new(type_id, child_value)))
}
