//! Validate exact input values and count native storage before buffer allocation.

use serde_json::Value as Json;
use shardloom_core::Result;
use vortex::array::dtype::{DType, PType};

use super::{
    native_payload,
    native_typed_input::failed,
    result_batch::{self, Value},
};

pub(super) fn add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| failed("typed input size overflow"))
}

pub(super) fn multiply(left: usize, right: usize) -> Result<usize> {
    left.checked_mul(right)
        .ok_or_else(|| failed("typed input size overflow"))
}

/// This transient counter tree uses the already-admitted per-DType metadata
/// allowance. It never expands fixed-list defaults into individual child rows.
pub(super) struct Layout {
    rows: usize,
    variable: usize,
    copied: usize,
    children: Vec<Self>,
}

impl Layout {
    pub(super) fn new(dtype: &DType) -> Self {
        let children = match dtype {
            DType::Struct(fields, _) => fields.fields().map(|dtype| Self::new(&dtype)).collect(),
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => vec![Self::new(child)],
            _ => Vec::new(),
        };
        Self {
            rows: 0,
            variable: 0,
            copied: 0,
            children,
        }
    }

    pub(super) fn observe(&mut self, dtype: &DType, value: Option<&Json>) -> Result<()> {
        let Some(value) = value.filter(|value| !value.is_null()) else {
            if value.is_some() && !dtype.is_nullable() {
                return Err(failed("null in a nonnullable typed input"));
            }
            return self.defaults(dtype, 1);
        };
        self.rows = add(self.rows, 1)?;
        match dtype {
            DType::Struct(fields, _) => {
                let values = value
                    .as_object()
                    .filter(|values| values.len() == fields.nfields())
                    .ok_or_else(|| {
                        failed("struct input must match its complete declared fields")
                    })?;
                for (index, (name, dtype)) in fields.names().iter().zip(fields.fields()).enumerate()
                {
                    let value = values
                        .get(name.as_ref())
                        .ok_or_else(|| failed("struct input is missing a declared field"))?;
                    self.children[index].observe(&dtype, Some(value))?;
                }
            }
            DType::List(child, _) | DType::FixedSizeList(child, _, _) => {
                let values = value
                    .as_array()
                    .ok_or_else(|| failed("list input requires an array"))?;
                if let DType::FixedSizeList(_, size, _) = dtype
                    && values.len() != *size as usize
                {
                    return Err(failed("fixed-size list input has the wrong width"));
                }
                for value in values {
                    self.children[0].observe(child, Some(value))?;
                }
            }
            _ => {
                let value = leaf(dtype, Some(value))?;
                let bytes = match &value {
                    Value::Text(value) => value.len(),
                    Value::Binary(value) => value.len(),
                    Value::Bool(_) | Value::Null => 0,
                    _ => result_batch::width(dtype)?,
                };
                self.copied = add(self.copied, bytes)?;
                if matches!(dtype, DType::Utf8(_) | DType::Binary(_)) {
                    self.variable = add(self.variable, bytes)?;
                }
            }
        }
        Ok(())
    }

    fn defaults(&mut self, dtype: &DType, count: usize) -> Result<()> {
        self.rows = add(self.rows, count)?;
        match dtype {
            DType::Struct(fields, _) => {
                for (shape, child) in self.children.iter_mut().zip(fields.fields()) {
                    shape.defaults(&child, count)?;
                }
            }
            DType::FixedSizeList(child, size, _) => {
                self.children[0].defaults(child, multiply(count, *size as usize)?)?;
            }
            // Defaults beneath a null parent are storage initialization, not
            // copies of caller payload. Their bytes remain in the native size.
            _ => {}
        }
        Ok(())
    }

    pub(super) fn bytes(&self, dtype: &DType) -> Result<usize> {
        let validity = if dtype.is_nullable() {
            self.rows.div_ceil(8)
        } else {
            0
        };
        let data = match dtype {
            DType::Struct(fields, _) => self
                .children
                .iter()
                .zip(fields.fields())
                .try_fold(0, |bytes, (shape, child)| add(bytes, shape.bytes(&child)?))?,
            DType::List(child, _) => add(
                multiply(add(self.rows, 1)?, 8)?,
                self.children[0].bytes(child)?,
            )?,
            DType::FixedSizeList(child, _, _) => self.children[0].bytes(child)?,
            DType::Bool(_) => self.rows.div_ceil(8),
            DType::Utf8(_) | DType::Binary(_) => {
                add(multiply(add(self.rows, 1)?, 8)?, self.variable)?
            }
            _ => multiply(self.rows, result_batch::width(dtype)?)?,
        };
        add(validity, data)
    }

    pub(super) fn copied(&self) -> Result<usize> {
        self.children
            .iter()
            .try_fold(self.copied, |bytes, child| add(bytes, child.copied()?))
    }
}

pub(super) fn leaf<'a>(dtype: &DType, value: Option<&'a Json>) -> Result<Value<'a>> {
    let Some(value) = value else {
        return native_payload::default_value(dtype);
    };
    if value.is_null() {
        return if dtype.is_nullable() {
            Ok(Value::Null)
        } else {
            Err(failed("null in a nonnullable typed input"))
        };
    }
    match dtype {
        DType::Bool(_) => value
            .as_bool()
            .map(Value::Bool)
            .ok_or_else(|| failed("boolean input requires a JSON boolean")),
        DType::Utf8(_) => value
            .as_str()
            .map(|value| Value::Text(value.into()))
            .ok_or_else(|| failed("UTF8 input requires a JSON string")),
        DType::Binary(_) => {
            let raw = value
                .as_str()
                .filter(|value| value.len().is_multiple_of(2))
                .ok_or_else(|| failed("binary input requires hexadecimal bytes"))?;
            let bytes = raw
                .as_bytes()
                .as_chunks::<2>()
                .0
                .iter()
                .map(|pair| {
                    let digit = |value: u8| match value {
                        b'0'..=b'9' => Some(value - b'0'),
                        b'a'..=b'f' => Some(value - b'a' + 10),
                        b'A'..=b'F' => Some(value - b'A' + 10),
                        _ => None,
                    };
                    digit(pair[0])
                        .zip(digit(pair[1]))
                        .map(|(high, low)| high * 16 + low)
                        .ok_or_else(|| failed("binary input requires hexadecimal bytes"))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Value::Binary(bytes.into()))
        }
        DType::Decimal(decimal, _) => {
            let prefix = format!("decimal128({},{}):", decimal.precision(), decimal.scale());
            let raw = value
                .as_str()
                .and_then(|value| value.strip_prefix(&prefix))
                .ok_or_else(|| {
                    failed("decimal input does not match its declared precision and scale")
                })?;
            let digits = raw.strip_prefix('-').unwrap_or(raw);
            if digits.is_empty()
                || digits.len() > usize::from(decimal.precision())
                || !digits.as_bytes().iter().all(u8::is_ascii_digit)
            {
                return Err(failed(
                    "decimal input coefficient exceeds its declared precision",
                ));
            }
            let coefficient = raw
                .parse::<i128>()
                .map_err(|_| failed("invalid decimal input coefficient"))?;
            Ok(Value::Decimal(coefficient, *decimal))
        }
        DType::Primitive(ptype, _) => primitive(*ptype, value),
        DType::Extension(_) => primitive(
            crate::native_payload_schema::temporal_storage(dtype)
                .ok_or_else(|| failed("unsupported input temporal storage"))?,
            value,
        ),
        _ => Err(failed("typed input scalar requires an admitted leaf dtype")),
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn primitive(ptype: PType, value: &Json) -> Result<Value<'static>> {
    if ptype.is_signed_int() {
        let value = value
            .as_i64()
            .ok_or_else(|| failed("signed input requires an exact JSON integer"))?;
        let admitted = match ptype {
            PType::I8 => i8::try_from(value).is_ok(),
            PType::I16 => i16::try_from(value).is_ok(),
            PType::I32 => i32::try_from(value).is_ok(),
            PType::I64 => true,
            _ => false,
        };
        return if admitted {
            Ok(Value::Int(value))
        } else {
            Err(failed("signed input exceeds its declared width"))
        };
    }
    if ptype.is_unsigned_int() {
        let value = value
            .as_u64()
            .ok_or_else(|| failed("unsigned input requires an exact JSON integer"))?;
        let admitted = match ptype {
            PType::U8 => u8::try_from(value).is_ok(),
            PType::U16 => u16::try_from(value).is_ok(),
            PType::U32 => u32::try_from(value).is_ok(),
            PType::U64 => true,
            _ => false,
        };
        return if admitted {
            Ok(Value::UInt(value))
        } else {
            Err(failed("unsigned input exceeds its declared width"))
        };
    }
    let numeric = value
        .as_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| failed("floating input requires a finite JSON number"))?;
    // Compare in i128 so u64::MAX cannot pass via saturating float-to-u64 casts.
    let integer = value
        .as_i64()
        .map(i128::from)
        .or_else(|| value.as_u64().map(i128::from));
    if integer.is_some_and(|integer| numeric as i128 != integer)
        || (ptype == PType::F32 && f64::from(numeric as f32).to_bits() != numeric.to_bits())
        || !matches!(ptype, PType::F32 | PType::F64)
    {
        return Err(failed(
            "floating input must be exactly representable in its declared width",
        ));
    }
    Ok(Value::Float(numeric))
}
