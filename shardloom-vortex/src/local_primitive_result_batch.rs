//! Construct final scalar columns in allocator-owned Vortex buffers.
//!
//! Values come from completed operator state, not a source replay or a JSON
//! table. The caller bounds the batch and retains its schema/array metadata.
//! Payload and validity credits live inside the buffers, including cloned slices.

use std::borrow::Cow;

use shardloom_core::{Result, ShardLoomError, StatValue};
use vortex::{
    array::{
        ArrayRef, IntoArray as _,
        arrays::{BoolArray, PrimitiveArray, StructArray, VarBinArray},
        dtype::{DType, FieldNames, PType},
        memory::{HostAllocatorRef, WritableHostBuffer},
        validity::Validity,
    },
    buffer::{Alignment, BitBuffer, Buffer, ByteBuffer},
};

use super::vortex_error;

/// Borrow text from a completed key/state whenever it already exists there.
#[derive(Clone)]
pub(super) enum Value<'a> {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Text(Cow<'a, str>),
    SharedText(vortex::buffer::BufferString),
}

impl Value<'_> {
    fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(value) => Some(value.as_ref()),
            Self::SharedText(value) => Some(value.as_str()),
            _ => None,
        }
    }

    pub(super) fn integer(bits: u64, signed: bool) -> Self {
        if signed {
            Self::Int(i64::from_ne_bytes(bits.to_ne_bytes()))
        } else {
            Self::UInt(bits)
        }
    }

    pub(super) fn into_json(self) -> Result<serde_json::Value> {
        Ok(match self {
            Self::Null => serde_json::Value::Null,
            Self::Bool(value) => value.into(),
            Self::Int(value) => value.into(),
            Self::UInt(value) => value.into(),
            Self::Float(value) => super::json_number_from_f64(value)?,
            Self::Text(value) => value.into_owned().into(),
            Self::SharedText(value) => value.as_str().into(),
        })
    }
}

/// Reports serialize values only at their terminal delivery boundary. Native
/// consumers instead receive arrays and a null values field in the descriptor.
pub(super) struct Rows {
    count: usize,
    pub(super) values: serde_json::Value,
}

pub(super) fn window<T>(values: &[T], offset: usize, count: usize) -> &[T] {
    let start = offset.min(values.len());
    &values[start..values.len().min(start.saturating_add(count))]
}

pub(super) const VISITOR_ROWS: usize = 2048;

impl Rows {
    pub(super) fn streamed(count: usize) -> Self {
        Self {
            count,
            values: serde_json::Value::Null,
        }
    }
    pub(super) const fn len(&self) -> usize {
        self.count
    }

    /// Visit completed, globally selected state without retaining a second full
    /// selection. The caller reserves these bounded references; native buffers
    /// acquire their own leases while each batch is synchronously consumed.
    pub(super) fn from_visitor<'a, T>(
        output: Option<&mut super::completed_result::CompletedRows>,
        columns: &[String],
        visit: impl FnOnce(&mut dyn FnMut(T) -> Result<()>) -> Result<()>,
        mut value: impl FnMut(&T, usize) -> Result<Value<'a>>,
    ) -> Result<Self> {
        match output {
            Some(output) if output.is_streaming() => {
                let mut batch = Vec::with_capacity(VISITOR_ROWS);
                let mut count = 0;
                visit(&mut |row| {
                    batch.push(row);
                    if batch.len() == VISITOR_ROWS {
                        output.push_values(columns, batch.len(), |row, column| {
                            value(&batch[row], column)
                        })?;
                        count = add(count, batch.len())?;
                        batch.clear();
                    }
                    Ok(())
                })?;
                if !batch.is_empty() || count == 0 {
                    output.push_values(columns, batch.len(), |row, column| {
                        value(&batch[row], column)
                    })?;
                    count = add(count, batch.len())?;
                }
                output.finish_stream()?;
                Ok(Self::streamed(count))
            }
            output => {
                let mut selected = Vec::new();
                visit(&mut |row| {
                    selected.push(row);
                    Ok(())
                })?;
                Self::from_fn(output, columns, selected.len(), |row, column| {
                    value(&selected[row], column)
                })
            }
        }
    }

    pub(super) fn from_fn<'a>(
        output: Option<&mut super::completed_result::CompletedRows>,
        columns: &[String],
        count: usize,
        mut value: impl FnMut(usize, usize) -> Result<Value<'a>>,
    ) -> Result<Self> {
        let values = if let Some(output) = output {
            output.finish_values(columns, count, value)?;
            serde_json::Value::Null
        } else {
            let mut rows = Vec::with_capacity(count);
            for row in 0..count {
                let mut values = serde_json::Map::new();
                for (column, name) in columns.iter().enumerate() {
                    values.insert(name.clone(), value(row, column)?.into_json()?);
                }
                rows.push(serde_json::Value::Object(values));
            }
            serde_json::Value::Array(rows)
        };
        Ok(Self { count, values })
    }
}

impl<'a> From<&'a StatValue> for Value<'a> {
    fn from(value: &'a StatValue) -> Self {
        match value {
            StatValue::Null => Self::Null,
            StatValue::Boolean(value) => Self::Bool(*value),
            StatValue::Int64(value) => Self::Int(*value),
            StatValue::UInt64(value) => Self::UInt(*value),
            StatValue::Float64(value) => Self::Float(*value),
            StatValue::Utf8(value) => Self::Text(Cow::Borrowed(value)),
        }
    }
}

impl From<StatValue> for Value<'_> {
    fn from(value: StatValue) -> Self {
        match value {
            StatValue::Null => Self::Null,
            StatValue::Boolean(value) => Self::Bool(value),
            StatValue::Int64(value) => Self::Int(value),
            StatValue::UInt64(value) => Self::UInt(value),
            StatValue::Float64(value) => Self::Float(value),
            StatValue::Utf8(value) => Self::Text(Cow::Owned(value)),
        }
    }
}

fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native result batch: {reason}; no fallback execution was attempted"
    ))
}

fn add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| failed("buffer size overflow"))
}

fn multiply(left: usize, right: usize) -> Result<usize> {
    left.checked_mul(right)
        .ok_or_else(|| failed("buffer size overflow"))
}

fn width(dtype: &DType) -> Result<usize> {
    match dtype {
        DType::Bool(_) => Ok(0),
        DType::Utf8(_) => Ok(8),
        DType::Primitive(ptype, _) if *ptype != PType::F16 => Ok(ptype.byte_width()),
        _ => Err(failed("output requires a declared flat scalar dtype")),
    }
}

/// Compute the exact logical buffer footprint before allocating any payload.
/// The repeatable accessor reads completed state only; UTF8 bytes are copied
/// once, during construction. Provider alignment slack is charged by the allocator.
pub(super) fn buffer_bytes<'a>(
    fields: &[(String, DType)],
    rows: usize,
    value: &mut impl FnMut(usize, usize) -> Result<Value<'a>>,
) -> Result<usize> {
    let mut bytes = 0;
    for (column, (_, dtype)) in fields.iter().enumerate() {
        let stride = width(dtype)?;
        bytes = add(
            bytes,
            if matches!(dtype, DType::Bool(_)) {
                rows.div_ceil(8)
            } else {
                multiply(
                    add(rows, usize::from(matches!(dtype, DType::Utf8(_))))?,
                    stride,
                )?
            },
        )?;
        if dtype.is_nullable() {
            bytes = add(bytes, rows.div_ceil(8))?;
        }
        if matches!(dtype, DType::Utf8(_)) {
            for row in 0..rows {
                match value(row, column)? {
                    Value::Text(text) => bytes = add(bytes, text.len())?,
                    Value::SharedText(text) => bytes = add(bytes, text.len())?,
                    Value::Null if dtype.is_nullable() => {}
                    _ => return Err(failed("UTF8 value differs from the declared dtype")),
                }
            }
        }
    }
    Ok(bytes)
}

/// Build one independently owned native batch. No row or JSON objects are kept.
pub(super) fn build<'a>(
    fields: &[(String, DType)],
    rows: usize,
    max_bytes: usize,
    allocator: &HostAllocatorRef,
    mut value: impl FnMut(usize, usize) -> Result<Value<'a>>,
) -> Result<ArrayRef> {
    if buffer_bytes(fields, rows, &mut value)? > max_bytes {
        return Err(failed(
            "complete batch exceeds its native buffer byte bound",
        ));
    }
    let mut arrays = Vec::with_capacity(fields.len());
    for (column, (_, dtype)) in fields.iter().enumerate() {
        arrays.push(build_column(dtype, rows, allocator, |row| {
            value(row, column)
        })?);
    }
    StructArray::try_new(
        fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<FieldNames>(),
        arrays,
        rows,
        Validity::NonNullable,
    )
    .map(vortex::array::IntoArray::into_array)
    .map_err(vortex_error)
}

fn allocate(
    allocator: &HostAllocatorRef,
    bytes: usize,
    alignment: usize,
) -> Result<WritableHostBuffer> {
    let mut buffer = allocator
        .allocate(bytes, Alignment::new(alignment))
        .map_err(vortex_error)?;
    buffer.as_mut_slice().fill(0);
    Ok(buffer)
}

fn set_bit(buffer: &mut WritableHostBuffer, row: usize) {
    buffer.as_mut_slice()[row / 8] |= 1 << (row % 8);
}

fn copy_text(buffer: &mut WritableHostBuffer, start: usize, value: &str) -> Result<usize> {
    let next = add(start, value.len())?;
    let destination = buffer
        .as_mut_slice()
        .get_mut(start..next)
        .ok_or_else(|| failed("completed text changed during construction"))?;
    destination.copy_from_slice(value.as_bytes());
    Ok(next)
}

fn build_column<'a>(
    dtype: &DType,
    rows: usize,
    allocator: &HostAllocatorRef,
    mut value: impl FnMut(usize) -> Result<Value<'a>>,
) -> Result<ArrayRef> {
    let stride = width(dtype)?;
    let boolean = matches!(dtype, DType::Bool(_));
    let text = matches!(dtype, DType::Utf8(_));
    let mut validity = dtype
        .is_nullable()
        .then(|| allocate(allocator, rows.div_ceil(8), 1))
        .transpose()?;
    let mut data = allocate(
        allocator,
        if boolean {
            rows.div_ceil(8)
        } else {
            multiply(rows + usize::from(text), stride)?
        },
        stride.max(1),
    )?;
    let mut text_bytes = 0;
    if text {
        for row in 0..rows {
            text_bytes = add(
                text_bytes,
                match value(row)? {
                    Value::Text(value) => value.len(),
                    Value::SharedText(value) => value.len(),
                    _ => 0,
                },
            )?;
        }
    }
    let mut text_data = text
        .then(|| allocate(allocator, text_bytes, 1))
        .transpose()?;
    let mut text_end = 0;
    for row in 0..rows {
        let value = value(row)?;
        if matches!(value, Value::Null) {
            if !dtype.is_nullable() {
                return Err(failed("null in a nonnullable output column"));
            }
        } else {
            if let Some(validity) = validity.as_mut() {
                set_bit(validity, row);
            }
            match (dtype, value) {
                (DType::Bool(_), Value::Bool(value)) => {
                    if value {
                        set_bit(&mut data, row);
                    }
                }
                (DType::Utf8(_), value @ (Value::Text(_) | Value::SharedText(_))) => {
                    text_end = copy_text(
                        text_data.as_mut().expect("UTF8 buffer allocated"),
                        text_end,
                        value.as_text().expect("text variant matched"),
                    )?;
                }
                (DType::Primitive(ptype, _), value) => {
                    write_primitive(
                        *ptype,
                        value,
                        &mut data.as_mut_slice()[row * stride..(row + 1) * stride],
                    )?;
                }
                _ => return Err(failed("value differs from the declared output dtype")),
            }
        }
        if text {
            data.as_mut_slice()[(row + 1) * 8..(row + 2) * 8]
                .copy_from_slice(&u64::try_from(text_end).map_err(vortex_error)?.to_ne_bytes());
        }
    }
    let validity = validity.map_or(Validity::NonNullable, |buffer| {
        Validity::Array(
            BoolArray::new(BitBuffer::new(buffer.freeze(), rows), Validity::NonNullable)
                .into_array(),
        )
    });
    let data = data.freeze();
    match dtype {
        DType::Bool(_) => Ok(BoolArray::new(BitBuffer::new(data, rows), validity).into_array()),
        DType::Primitive(ptype, _) => primitive(*ptype, data, validity),
        DType::Utf8(_) => {
            if text_end != text_bytes {
                return Err(failed("completed text changed during construction"));
            }
            VarBinArray::try_new(
                PrimitiveArray::new(Buffer::<u64>::from_byte_buffer(data), Validity::NonNullable)
                    .into_array(),
                text_data.expect("UTF8 buffer allocated").freeze(),
                dtype.clone(),
                validity,
            )
            .map(vortex::array::IntoArray::into_array)
            .map_err(vortex_error)
        }
        _ => Err(failed("output dtype changed during construction")),
    }
}

#[allow(clippy::cast_possible_truncation)]
fn write_primitive(ptype: PType, value: Value<'_>, output: &mut [u8]) -> Result<()> {
    macro_rules! integer {
        ($t:ty, $value:expr) => {
            output.copy_from_slice(&<$t>::try_from($value).map_err(vortex_error)?.to_ne_bytes())
        };
    }
    match (ptype, value) {
        (PType::I8, Value::Int(value)) => integer!(i8, value),
        (PType::I16, Value::Int(value)) => integer!(i16, value),
        (PType::I32, Value::Int(value)) => integer!(i32, value),
        (PType::I64, Value::Int(value)) => output.copy_from_slice(&value.to_ne_bytes()),
        (PType::U8, Value::UInt(value)) => integer!(u8, value),
        (PType::U16, Value::UInt(value)) => integer!(u16, value),
        (PType::U32, Value::UInt(value)) => integer!(u32, value),
        (PType::U64, Value::UInt(value)) => output.copy_from_slice(&value.to_ne_bytes()),
        (PType::F32, Value::Float(value))
            if value.is_finite() && f64::from(value as f32).to_bits() == value.to_bits() =>
        {
            output.copy_from_slice(&(value as f32).to_ne_bytes());
        }
        (PType::F64, Value::Float(value)) if value.is_finite() => {
            output.copy_from_slice(&value.to_ne_bytes());
        }
        _ => {
            return Err(failed(
                "numeric value differs from its finite declared dtype",
            ));
        }
    }
    Ok(())
}

fn primitive(ptype: PType, data: ByteBuffer, validity: Validity) -> Result<ArrayRef> {
    macro_rules! array {
        ($t:ty) => {
            PrimitiveArray::new(Buffer::<$t>::from_byte_buffer(data), validity).into_array()
        };
    }
    Ok(match ptype {
        PType::I8 => array!(i8),
        PType::I16 => array!(i16),
        PType::I32 => array!(i32),
        PType::I64 => array!(i64),
        PType::U8 => array!(u8),
        PType::U16 => array!(u16),
        PType::U32 => array!(u32),
        PType::U64 => array!(u64),
        PType::F32 => array!(f32),
        PType::F64 => array!(f64),
        PType::F16 => return Err(failed("float16 results are not admitted")),
    })
}
