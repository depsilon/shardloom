//! Small column kernels for admitted numeric and Unicode scalar functions.
//! String scratch is admitted before construction and held through buffer copy.

use super::{Cell, KeyColumn, Result, Value, failed, float, integer, vortex_error};
use shardloom_core::expression::{self as typed, Decimal128Operand};
use shardloom_exec::live_memory::MemoryLease;
use std::borrow::Cow;
use vortex::{
    array::dtype::{DType, DecimalDType, PType},
    buffer::{BufferString, ByteBuffer},
};

#[path = "native_relational_text.rs"]
pub(in crate::local_primitives) mod text_kernels;

#[path = "native_relational_temporal.rs"]
mod temporal;
pub(in crate::local_primitives) use temporal::Cast as TemporalCast;

#[path = "native_relational_scalar_batch.rs"]
mod batch;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::local_primitives) enum TemporalArgument {
    None,
    Integer,
    Temporal,
}

pub(in crate::local_primitives) enum Function {
    IsNan,
    IsInfinite,
    IsFinite,
    SignBit,
    Abs,
    Floor,
    Ceil,
    Round,
    Decimal {
        output: shardloom_core::decimal_output::DecimalOutput,
        operation: Option<typed::BinaryOp>,
    },
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
    ByteLength,
    EpochMinute {
        truncate: bool,
    },
    Domain {
        regex: bool,
    },
    Unhex,
    FromBase64,
    Temporal {
        kernel: Box<shardloom_core::temporal::operations::Kernel>,
        second: TemporalArgument,
    },
    JsonExtract(text_kernels::JsonPath),
    Strptime {
        format: text_kernels::TimestampFormat,
        tolerant: bool,
    },
}

impl Function {
    /// Only explicitly matched, pure bound kernels are eligible for reuse.
    pub(super) fn same_semantics(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Decimal {
                    output: left,
                    operation: lo,
                },
                Self::Decimal {
                    output: right,
                    operation: ro,
                },
            ) => left == right && lo == ro,
            (Self::Abs, Self::Abs)
            | (Self::IsNan, Self::IsNan)
            | (Self::IsInfinite, Self::IsInfinite)
            | (Self::IsFinite, Self::IsFinite)
            | (Self::SignBit, Self::SignBit)
            | (Self::Floor, Self::Floor)
            | (Self::Ceil, Self::Ceil)
            | (Self::Round, Self::Round)
            | (Self::Lower, Self::Lower)
            | (Self::Upper, Self::Upper)
            | (Self::Trim, Self::Trim)
            | (Self::Length, Self::Length)
            | (Self::StartsWith, Self::StartsWith)
            | (Self::EndsWith, Self::EndsWith)
            | (Self::Contains, Self::Contains)
            | (Self::Concat, Self::Concat)
            | (Self::Substr, Self::Substr)
            | (Self::Left, Self::Left)
            | (Self::Right, Self::Right)
            | (Self::Replace, Self::Replace)
            | (Self::ByteLength, Self::ByteLength)
            | (Self::Unhex, Self::Unhex)
            | (Self::FromBase64, Self::FromBase64) => true,
            // The binder uses the same fixed RegexBuilder limits and default
            // flags for every pattern; inline flags are part of as_str().
            (Self::Regex(left), Self::Regex(right)) => left.as_str() == right.as_str(),
            (Self::EpochMinute { truncate: left }, Self::EpochMinute { truncate: right })
            | (Self::Domain { regex: left }, Self::Domain { regex: right }) => left == right,
            (Self::JsonExtract(left), Self::JsonExtract(right)) => left == right,
            (
                Self::Strptime {
                    format: left,
                    tolerant: lt,
                },
                Self::Strptime {
                    format: right,
                    tolerant: rt,
                },
            ) => left == right && lt == rt,
            (
                Self::Temporal {
                    kernel: left,
                    second: ls,
                },
                Self::Temporal {
                    kernel: right,
                    second: rs,
                },
            ) => ls == rs && left.same_semantics(right),
            _ => false,
        }
    }

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
        if matches!(
            self,
            Self::IsNan | Self::IsInfinite | Self::IsFinite | Self::SignBit
        ) {
            return Ok(self.classify(columns[0].classification_value(row)?));
        }
        if matches!(self, Self::Abs | Self::Floor | Self::Ceil | Self::Round) {
            return self.numeric(&columns[0].raw_cell(row)?, dtype);
        }
        if let Self::Decimal { output, operation } = self {
            let operand = |column: &KeyColumn| {
                let value = column.raw_cell(row)?;
                if let Cell::Decimal(value, decimal) = value {
                    decimal_operand(value, &DType::Decimal(decimal, dtype.nullability()))
                } else {
                    Decimal128Operand::integer(integer(&value)?, 20)
                }
            };
            let left = operand(&columns[0])?;
            let value = if let Some(operation) = operation {
                output.binary(left, *operation, operand(&columns[1])?)?
            } else {
                output.rescale(left)?
            };
            return Ok(decimal_value(value));
        }
        if matches!(self, Self::Concat) {
            return concat(columns, row, scratch);
        }
        if let Self::EpochMinute { truncate } = self {
            return epoch_minute(&columns[0].raw_cell(row)?, dtype, *truncate, scratch);
        }
        if let Some(value) = self.typed(columns, row, scratch)? {
            return Ok(value);
        }
        let bytes = text(columns[0].cell(row)?)?;
        let value = utf8(&bytes)?;
        match self {
            Self::Domain { regex } => {
                let domain = if *regex {
                    crate::url_domain::shardloom_regex_domain(value)
                } else {
                    crate::url_domain::shardloom_url_domain(value)
                };
                output(scratch, domain.len(), || domain.to_owned())
            }
            Self::JsonExtract(path) => path.extract(value, scratch),
            Self::Strptime { format, tolerant } => match format.timestamp(value) {
                Ok(value) => Ok(Value::Int(value)),
                Err(_) if *tolerant => Ok(Value::Null),
                Err(error) => Err(error),
            },
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

    fn classify(&self, value: Option<f64>) -> Value<'static> {
        value.map_or(Value::Null, |value| {
            Value::Bool(match self {
                Self::IsNan => value.is_nan(),
                Self::IsInfinite => value.is_infinite(),
                Self::IsFinite => value.is_finite(),
                Self::SignBit => value.is_sign_negative(),
                _ => unreachable!("classification function admitted above"),
            })
        })
    }

    fn numeric(&self, value: &Cell, dtype: &DType) -> Result<Value<'static>> {
        if let Cell::Decimal(value, decimal) = value {
            let operand = decimal_operand(*value, &DType::Decimal(*decimal, dtype.nullability()))?;
            let value = match self {
                Self::Abs => operand.abs(),
                Self::Floor => operand.floor(),
                Self::Ceil => operand.ceil(),
                Self::Round => operand.round(),
                _ => unreachable!("numeric function admitted above"),
            };
            return Ok(decimal_value(value));
        }
        if matches!(dtype, DType::Primitive(PType::F64, _)) {
            let value = float(value)?;
            return Ok(Value::Float(
                shardloom_core::float_policy::canonicalize_f64_result(match self {
                    Self::Abs => value.abs(),
                    Self::Floor => value.floor(),
                    Self::Ceil => value.ceil(),
                    Self::Round => value.round(),
                    _ => unreachable!("numeric function admitted above"),
                }),
            ));
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

    fn typed(
        &self,
        columns: &[KeyColumn],
        row: usize,
        scratch: &mut MemoryLease,
    ) -> Result<Option<Value<'static>>> {
        let value = match self {
            Self::ByteLength => {
                let (Cell::Utf8(bytes) | Cell::Binary(bytes)) = columns[0].raw_cell(row)? else {
                    return Err(failed("byte length requires UTF8 or binary"));
                };
                Value::Int(i64::try_from(bytes.len()).map_err(vortex_error)?)
            }
            Self::Unhex | Self::FromBase64 => {
                let bytes = text(columns[0].raw_cell(row)?)?;
                let text = utf8(&bytes)?;
                let capacity = if matches!(self, Self::Unhex) {
                    text.len() / 2
                } else {
                    text.len() / 4 * 3
                };
                reserve(scratch, capacity)?;
                let decoded = if matches!(self, Self::Unhex) {
                    typed::decode_hex_bytes(text)
                } else {
                    typed::decode_standard_base64(text)
                }
                .map_err(failed)?;
                if decoded.capacity() > capacity {
                    return Err(failed(
                        "binary decoder exceeded its admitted scratch capacity",
                    ));
                }
                Value::Binary(Cow::Owned(decoded))
            }
            Self::Temporal { kernel, second } => {
                let right = match second {
                    TemporalArgument::None => None,
                    TemporalArgument::Integer => Some(integer(&columns[1].raw_cell(row)?)?),
                    TemporalArgument::Temporal => {
                        Some(i128::from(temporal_ticks(&columns[1].raw_cell(row)?)?))
                    }
                };
                Value::Int(kernel.apply(temporal_ticks(&columns[0].raw_cell(row)?)?, right)?)
            }
            _ => return Ok(None),
        };
        Ok(Some(value))
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

fn epoch_minute(
    value: &Cell,
    dtype: &DType,
    truncate: bool,
    scratch: &mut MemoryLease,
) -> Result<Value<'static>> {
    use crate::local_primitives as native;
    if let Cell::Utf8(bytes) = value {
        let value = utf8(bytes)?;
        return if truncate {
            reserve(scratch, value.len().saturating_add(3))?;
            let value = native::truncate_timestamp_string_to_minute(value)
                .ok_or_else(|| failed("minute truncation requires timestamp text"))?;
            Ok(Value::Text(Cow::Owned(value)))
        } else {
            native::parse_timestamp_minute(value)
                .map(|value| Value::UInt(u64::from(value)))
                .ok_or_else(|| failed("minute extraction requires timestamp text"))
        };
    }
    let value = integer(value)?;
    let scalar = if matches!(dtype, DType::Primitive(PType::U64, _)) {
        shardloom_core::StatValue::UInt64(u64::try_from(value).map_err(vortex_error)?)
    } else {
        shardloom_core::StatValue::Int64(i64::try_from(value).map_err(vortex_error)?)
    };
    let value = if truncate {
        native::aggregate_date_trunc_minute(&scalar)?
    } else {
        native::aggregate_extract_minute(&scalar)?
    };
    match value {
        shardloom_core::StatValue::Int64(value) => Ok(Value::Int(value)),
        shardloom_core::StatValue::UInt64(value) => Ok(Value::UInt(value)),
        _ => Err(failed("minute provider returned an invalid type")),
    }
}

fn temporal_ticks(value: &Cell) -> Result<i64> {
    match value {
        Cell::Date(value) => Ok(i64::from(*value)),
        Cell::Timestamp(value) | Cell::Temporal(value) => Ok(*value),
        _ => Err(failed(
            "temporal function requires its declared logical domain",
        )),
    }
}

pub(in crate::local_primitives) fn decimal_operand(
    value: i128,
    dtype: &DType,
) -> Result<Decimal128Operand> {
    match dtype {
        DType::Decimal(decimal, _) => Decimal128Operand::decimal(
            value,
            decimal.precision(),
            u8::try_from(decimal.scale()).map_err(vortex_error)?,
        ),
        DType::Null => Decimal128Operand::integer(value, 1),
        DType::Primitive(ptype, _) => Decimal128Operand::integer(
            value,
            match ptype {
                PType::I8 | PType::U8 => 3,
                PType::I16 | PType::U16 => 5,
                PType::I32 | PType::U32 => 10,
                PType::I64 => 19,
                PType::U64 => 20,
                _ => {
                    return Err(failed(
                        "decimal arithmetic requires decimal or integer operands",
                    ));
                }
            },
        ),
        _ => Err(failed(
            "decimal arithmetic requires decimal or integer operands",
        )),
    }
}

fn decimal_value(value: Decimal128Operand) -> Value<'static> {
    let (precision, scale) = value.precision_scale();
    Value::Decimal(
        value.value(),
        DecimalDType::new(precision, i8::try_from(scale).expect("validated scale")),
    )
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
    temporal: Option<&TemporalCast>,
    scratch: &mut MemoryLease,
    float_policy: shardloom_core::FloatPolicy,
) -> Result<Value<'static>> {
    if value == Cell::Null {
        return Ok(Value::Null);
    }
    if let Some(temporal) = temporal {
        if temporal.formats_text() {
            reserve(scratch, 1024)?;
        }
        return match temporal.evaluate(&value) {
            Ok(value) => Ok(value),
            Err(_) if tolerant => Ok(Value::Null),
            Err(error) => Err(error),
        };
    }
    // Conversion failures alone become NULL for TRY_CAST. Cancellation and
    // memory failures occur outside this branch and always propagate.
    let converted = if matches!(dtype, DType::Utf8(_) | DType::Binary(_)) {
        match value {
            Cell::Utf8(bytes) | Cell::Binary(bytes) => {
                if matches!(dtype, DType::Binary(_)) {
                    Some(Value::SharedBinary(bytes))
                } else {
                    BufferString::try_from(bytes).ok().map(Value::SharedText)
                }
            }
            value => {
                // Primitive/calendar text has a bounded representation, including
                // the full f64 and proleptic-calendar storage domains.
                reserve(scratch, 1024)?;
                let text = scalar_text(&value)?;
                if text.capacity() > 1024 {
                    return Err(failed("cast formatting exceeded scratch capacity"));
                }
                Some(if matches!(dtype, DType::Binary(_)) {
                    Value::Binary(Cow::Owned(text.into_bytes()))
                } else {
                    Value::Text(Cow::Owned(text))
                })
            }
        }
    } else if let DType::Decimal(decimal, _) = dtype {
        cast_decimal(&value, *decimal, scratch)?
    } else {
        convert(&value, dtype, float_policy)
    };
    match converted {
        Some(value) => Ok(value),
        None if tolerant => Ok(Value::Null),
        None => Err(failed(
            "value cannot be converted to the bound scalar dtype",
        )),
    }
}

fn scalar_text(value: &Cell) -> Result<String> {
    Ok(match value {
        Cell::Boolean(value) => value.to_string(),
        Cell::NegativeInteger(value) => value.to_string(),
        Cell::NonnegativeInteger(value) => value.to_string(),
        Cell::Float(bits) => f64::from_bits(*bits).to_string(),
        Cell::Decimal(value, dtype) => typed::format_decimal128_value(
            *value,
            u8::try_from(dtype.scale()).map_err(vortex_error)?,
        ),
        Cell::Date(value) => typed::format_iso_date32(*value),
        Cell::Timestamp(value) => typed::format_iso_timestamp_micros(*value),
        _ => return Err(failed("cast source has no admitted text representation")),
    })
}

fn cast_decimal(
    value: &Cell,
    dtype: DecimalDType,
    scratch: &mut MemoryLease,
) -> Result<Option<Value<'static>>> {
    let precision = dtype.precision();
    let scale = u8::try_from(dtype.scale()).map_err(vortex_error)?;
    let converted = match value {
        Cell::Decimal(value, source) => decimal_operand(
            *value,
            &DType::Decimal(*source, vortex::array::dtype::Nullability::NonNullable),
        )?
        .rescale(precision, scale)
        .ok()
        .map(Decimal128Operand::value),
        Cell::NegativeInteger(value) => Decimal128Operand::integer(i128::from(*value), 19)?
            .rescale(precision, scale)
            .ok()
            .map(Decimal128Operand::value),
        Cell::NonnegativeInteger(value) => Decimal128Operand::integer(i128::from(*value), 20)?
            .rescale(precision, scale)
            .ok()
            .map(Decimal128Operand::value),
        Cell::Utf8(bytes) => {
            reserve(
                scratch,
                growth(bytes.len(), 32)?
                    .checked_add(2048)
                    .ok_or_else(|| failed("decimal parse scratch overflow"))?,
            )?;
            typed::parse_decimal128_text(utf8(bytes)?, precision, scale).ok()
        }
        Cell::Float(bits) => {
            reserve(scratch, 1024)?;
            let text = f64::from_bits(*bits).to_string();
            reserve(
                scratch,
                growth(text.len(), 32)?
                    .checked_add(4096)
                    .ok_or_else(|| failed("decimal parse scratch overflow"))?,
            )?;
            typed::parse_decimal128_text(&text, precision, scale).ok()
        }
        _ => return Err(failed("cast source has no admitted decimal conversion")),
    };
    Ok(converted.map(|value| Value::Decimal(value, dtype)))
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)] // Explicit CAST admits rounding; integer float boundaries are checked before casting.
fn convert(
    value: &Cell,
    dtype: &DType,
    float_policy: shardloom_core::FloatPolicy,
) -> Option<Value<'static>> {
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
            Cell::Decimal(value, dtype) => i64::try_from(decimal_integral(*value, *dtype)?).ok()?,
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
            Cell::Decimal(value, dtype) => u64::try_from(decimal_integral(*value, *dtype)?).ok()?,
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
                Cell::Decimal(value, dtype) => *value as f64 / 10f64.powi(i32::from(dtype.scale())),
                _ => shardloom_core::float_policy::canonicalize_f64_result(string?.parse().ok()?),
            };
            if !float_policy.admits(value) {
                return None;
            }
            Value::Float(value)
        }
        _ => return None,
    })
}

fn decimal_integral(value: i128, dtype: DecimalDType) -> Option<i128> {
    Decimal128Operand::decimal(value, dtype.precision(), u8::try_from(dtype.scale()).ok()?)
        .ok()?
        .rescale(38, 0)
        .ok()
        .map(Decimal128Operand::value)
}
