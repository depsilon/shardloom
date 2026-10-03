//! Small column kernels for admitted numeric and Unicode scalar functions.
//! String scratch is admitted before construction and held through buffer copy.

use super::{Cell, KeyColumn, Result, Value, failed, float, integer, vortex_error};
use shardloom_exec::live_memory::MemoryLease;
use std::borrow::Cow;
use vortex::{
    array::dtype::{DType, PType},
    buffer::ByteBuffer,
};

pub(in crate::local_primitives) enum Function {
    Abs,
    Floor,
    Ceil,
    Round,
    Lower,
    Upper,
    Trim,
    Length,
    StartsWith,
    EndsWith,
    Contains,
    Regex(regex::Regex),
    Concat,
    Substr,
    Left,
    Right,
    Replace,
}

impl Function {
    pub(in crate::local_primitives) fn evaluate(
        &self,
        columns: &[KeyColumn],
        row: usize,
        dtype: &DType,
        scratch: &mut MemoryLease,
    ) -> Result<Value<'static>> {
        for column in columns {
            if column.is_null(row)? {
                return Ok(Value::Null);
            }
        }
        if matches!(self, Self::Abs | Self::Floor | Self::Ceil | Self::Round) {
            return self.numeric(&columns[0].raw_cell(row)?, dtype);
        }
        if matches!(self, Self::Concat) {
            return concat(columns, row, scratch);
        }
        let bytes = text(columns[0].cell(row)?)?;
        let value = utf8(&bytes)?;
        match self {
            Self::Length => Ok(Value::Int(
                i64::try_from(value.chars().count()).map_err(vortex_error)?,
            )),
            Self::Lower => output(scratch, growth(value.len(), 8)?, || value.to_lowercase()),
            Self::Upper => output(scratch, growth(value.len(), 8)?, || value.to_uppercase()),
            Self::Trim => output(scratch, value.len(), || value.trim().to_owned()),
            Self::Regex(regex) => Ok(Value::Bool(regex.is_match(value))),
            Self::StartsWith | Self::EndsWith | Self::Contains => {
                let needle = text(columns[1].cell(row)?)?;
                let needle = utf8(&needle)?;
                Ok(Value::Bool(match self {
                    Self::StartsWith => value.starts_with(needle),
                    Self::EndsWith => value.ends_with(needle),
                    _ => value.contains(needle),
                }))
            }
            Self::Substr | Self::Left | Self::Right => self.substring(value, columns, row, scratch),
            Self::Replace => {
                let needle = text(columns[1].cell(row)?)?;
                let replacement = text(columns[2].cell(row)?)?;
                let (needle, replacement) = (utf8(&needle)?, utf8(&replacement)?);
                if needle.is_empty() {
                    return Err(failed("replace requires a nonempty search string"));
                }
                let bound = value
                    .len()
                    .checked_add(
                        value
                            .matches(needle)
                            .count()
                            .checked_mul(replacement.len())
                            .ok_or_else(|| failed("replacement length overflow"))?,
                    )
                    .ok_or_else(|| failed("replacement length overflow"))?;
                output(scratch, growth(bound, 2)?, || {
                    value.replace(needle, replacement)
                })
            }
            _ => Err(failed(
                "scalar function binding does not match its native operands",
            )),
        }
    }

    fn numeric(&self, value: &Cell, dtype: &DType) -> Result<Value<'static>> {
        if matches!(dtype, DType::Primitive(PType::F64, _)) {
            let value = float(value)?;
            return Ok(Value::Float(match self {
                Self::Abs => value.abs(),
                Self::Floor => value.floor(),
                Self::Ceil => value.ceil(),
                Self::Round => value.round(),
                _ => unreachable!("numeric function admitted above"),
            }));
        }
        let value = integer(value)?;
        let value = if matches!(self, Self::Abs) {
            value.abs()
        } else {
            value
        };
        if matches!(dtype, DType::Primitive(PType::U64, _)) {
            Ok(Value::UInt(u64::try_from(value).map_err(vortex_error)?))
        } else {
            Ok(Value::Int(i64::try_from(value).map_err(vortex_error)?))
        }
    }

    fn substring(
        &self,
        value: &str,
        columns: &[KeyColumn],
        row: usize,
        scratch: &mut MemoryLease,
    ) -> Result<Value<'static>> {
        let argument = integer(&columns[1].cell(row)?)?;
        let (start, count) = if matches!(self, Self::Substr) {
            if argument < 1 {
                return Err(failed("substring requires a 1-based positive start"));
            }
            (
                usize::try_from(argument - 1).map_err(vortex_error)?,
                usize::try_from(integer(&columns[2].cell(row)?)?).map_err(vortex_error)?,
            )
        } else {
            let count = usize::try_from(argument).map_err(vortex_error)?;
            (
                if matches!(self, Self::Right) {
                    value.chars().count().saturating_sub(count)
                } else {
                    0
                },
                count,
            )
        };
        // Locate UTF8 boundaries without a decoded character vector.
        let begin = value
            .char_indices()
            .nth(start)
            .map_or(value.len(), |(index, _)| index);
        let end = value[begin..]
            .char_indices()
            .nth(count)
            .map_or(value.len(), |(index, _)| begin + index);
        output(scratch, end - begin, || value[begin..end].to_owned())
    }
}

fn concat(columns: &[KeyColumn], row: usize, scratch: &mut MemoryLease) -> Result<Value<'static>> {
    let mut bytes = 0usize;
    for column in columns {
        bytes = bytes
            .checked_add(text(column.cell(row)?)?.len())
            .ok_or_else(|| failed("concatenation length overflow"))?;
    }
    reserve(scratch, bytes)?;
    let mut output = String::new();
    output.try_reserve_exact(bytes).map_err(vortex_error)?;
    if output.capacity() > bytes {
        return Err(failed("concatenation allocation exceeded reservation"));
    }
    for column in columns {
        output.push_str(utf8(&text(column.cell(row)?)?)?);
    }
    Ok(Value::Text(Cow::Owned(output)))
}

fn text(value: Cell) -> Result<ByteBuffer> {
    if let Cell::Utf8(value) = value {
        Ok(value)
    } else {
        Err(failed("string function requires UTF8"))
    }
}
fn utf8(value: &ByteBuffer) -> Result<&str> {
    std::str::from_utf8(value.as_slice()).map_err(vortex_error)
}
fn growth(bytes: usize, multiplier: usize) -> Result<usize> {
    bytes
        .checked_mul(multiplier)
        .and_then(|bytes| bytes.checked_add(128))
        .ok_or_else(|| failed("scalar string scratch overflow"))
}
fn reserve(scratch: &mut MemoryLease, bytes: usize) -> Result<()> {
    let bytes = u64::try_from(bytes).map_err(vortex_error)?;
    if bytes > scratch.bytes() {
        scratch.resize(bytes)?;
    }
    Ok(())
}
fn output(
    scratch: &mut MemoryLease,
    bytes: usize,
    produce: impl FnOnce() -> String,
) -> Result<Value<'static>> {
    reserve(scratch, bytes)?;
    let output = produce();
    if output.capacity() > bytes {
        return Err(failed(
            "string kernel exceeded its admitted scratch capacity",
        ));
    }
    Ok(Value::Text(Cow::Owned(output)))
}

pub(in crate::local_primitives) fn cast(
    value: Cell,
    dtype: &DType,
    tolerant: bool,
    scratch: &mut MemoryLease,
) -> Result<Value<'static>> {
    if value == Cell::Null {
        return Ok(Value::Null);
    }
    if matches!(dtype, DType::Utf8(_)) {
        return match value {
            Cell::Utf8(bytes) => {
                let value = utf8(&bytes)?;
                output(scratch, value.len(), || value.to_owned())
            }
            Cell::Boolean(value) => output(scratch, 1024, || value.to_string()),
            Cell::NegativeInteger(value) => output(scratch, 1024, || value.to_string()),
            Cell::NonnegativeInteger(value) => output(scratch, 1024, || value.to_string()),
            Cell::Float(bits) => output(scratch, 1024, || f64::from_bits(bits).to_string()),
            Cell::Binary(_) | Cell::Decimal(..) | Cell::Date(_) | Cell::Timestamp(_) => {
                Err(failed("cast source has no admitted native scalar kernel"))
            }
            Cell::Null => unreachable!("null handled above"),
        };
    }
    // Conversion failures alone become NULL for TRY_CAST. Cancellation and
    // memory failures occur outside this branch and always propagate.
    let converted = convert(&value, dtype);
    match converted {
        Some(value) => Ok(value),
        None if tolerant => Ok(Value::Null),
        None => Err(failed(
            "value cannot be converted to the bound scalar dtype",
        )),
    }
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)] // Explicit CAST admits rounding; integer float boundaries are checked before casting.
fn convert(value: &Cell, dtype: &DType) -> Option<Value<'static>> {
    let string = if let Cell::Utf8(bytes) = value {
        Some(std::str::from_utf8(bytes.as_slice()).ok()?)
    } else {
        None
    };
    Some(match dtype {
        DType::Bool(_) => Value::Bool(match value {
            Cell::Boolean(value) => *value,
            _ => match string? {
                "true" => true,
                "false" => false,
                _ => return None,
            },
        }),
        DType::Primitive(PType::I64, _) => Value::Int(match value {
            Cell::NegativeInteger(value) => *value,
            Cell::NonnegativeInteger(value) => i64::try_from(*value).ok()?,
            Cell::Float(bits) => {
                let value = f64::from_bits(*bits);
                if value.fract() != 0.0
                    || !(-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&value)
                {
                    return None;
                }
                value as i64
            }
            _ => string?.parse().ok()?,
        }),
        DType::Primitive(PType::U64, _) => Value::UInt(match value {
            Cell::NonnegativeInteger(value) => *value,
            Cell::Float(bits) => {
                let value = f64::from_bits(*bits);
                if value.fract() != 0.0 || !(0.0..18_446_744_073_709_551_616.0).contains(&value) {
                    return None;
                }
                value as u64
            }
            _ => string?.parse().ok()?,
        }),
        DType::Primitive(PType::F64, _) => {
            let value = match value {
                Cell::Float(bits) => f64::from_bits(*bits),
                Cell::NegativeInteger(value) => *value as f64,
                Cell::NonnegativeInteger(value) => *value as f64,
                _ => string?.parse().ok()?,
            };
            if !value.is_finite() {
                return None;
            }
            Value::Float(value)
        }
        _ => return None,
    })
}
