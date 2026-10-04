//! Native scalar access only for keys, predicates and selected final values.
//! Strings remain borrowed native buffers until retained state needs a copy.

use super::{Result, StatValue, Value, failed, vortex_error};
use shardloom_core::ScalarValue;
use shardloom_exec::live_memory::{Budgeted, LiveMemoryPool, MemoryLease};
use vortex::array::{
    ArrayRef, ExecutionCtx, VortexSessionExecute as _,
    arrays::{
        Chunked, ChunkedArray, Dict, DictArray, ScalarFn, chunked::ChunkedArrayExt as _,
        dict::DictArraySlotsExt as _,
    },
    dtype::{DType, PType},
    matcher::Matcher,
};

pub(super) use super::super::result_batch::scalar_value;

struct NativeField;

impl Matcher for NativeField {
    type Match<'a> = ();

    fn try_match(array: &ArrayRef) -> Option<()> {
        (!array.is::<ScalarFn>()).then_some(())
    }
}

fn resolve_field(
    array: ArrayRef,
    context: &mut ExecutionCtx,
    metadata: &mut MemoryLease,
    depth: usize,
) -> Result<ArrayRef> {
    if depth > 24 {
        return Err(failed("native field wrapper nesting exceeds 24 levels"));
    }
    let array = array
        .execute_until::<NativeField>(context)
        .map_err(vortex_error)?;
    if let Some(dictionary) = array.as_opt::<Dict>() {
        metadata.resize(
            metadata
                .bytes()
                .checked_add(2048)
                .ok_or_else(|| failed("native dictionary metadata overflow"))?,
        )?;
        let codes = resolve_field(dictionary.codes().clone(), context, metadata, depth + 1)?;
        let values = resolve_field(dictionary.values().clone(), context, metadata, depth + 1)?;
        return DictArray::try_new(codes, values)
            .map(vortex::array::IntoArray::into_array)
            .map_err(vortex_error);
    }
    let Some(chunked) = array.as_opt::<Chunked>() else {
        return Ok(array);
    };
    // Projected scan chunks can contain lazy field wrappers in each child.
    // Resolve those children without canonicalizing their encoded value domain.
    let count = chunked.nchunks();
    let bytes = u64::try_from(count)
        .map_err(vortex_error)?
        .checked_mul(512)
        .and_then(|bytes| bytes.checked_add(1024))
        .ok_or_else(|| failed("native chunk metadata overflow"))?;
    metadata.resize(
        metadata
            .bytes()
            .checked_add(bytes)
            .ok_or_else(|| failed("native chunk metadata overflow"))?,
    )?;
    let mut chunks = Vec::new();
    chunks.try_reserve_exact(count).map_err(vortex_error)?;
    if chunks.capacity() > count {
        return Err(failed("native chunk list exceeded reserved capacity"));
    }
    for child in chunked.iter_chunks() {
        chunks.push(resolve_field(child.clone(), context, metadata, depth + 1)?);
    }
    ChunkedArray::try_new(chunks, array.dtype().clone())
        .map(vortex::array::IntoArray::into_array)
        .map_err(vortex_error)
}

pub(in crate::local_primitives) struct NativeBatch {
    columns: Vec<ArrayRef>,
    context: ExecutionCtx,
    memory: LiveMemoryPool,
    _metadata: MemoryLease,
}

impl NativeBatch {
    pub(in crate::local_primitives) fn new(
        array: &ArrayRef,
        columns: &[String],
        context: &crate::resident_session::NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let mut metadata = context
            .memory()
            .reserve(u64::try_from(columns.len().saturating_mul(512)).map_err(vortex_error)?)?;
        let mut execution = context.native_session().create_execution_ctx();
        let values = columns
            .iter()
            .map(|name| {
                let column = if array.dtype().is_struct() {
                    super::super::logical_field_from_native_array(array, name)
                } else if columns.len() == 1 {
                    Ok(array.clone())
                } else {
                    Err(failed("a non-struct source must expose one column"))
                }?;
                // Resolve native field wrappers before scalar access. Otherwise
                // the provider evaluates the whole struct scalar, including
                // unrelated temporal fields with a narrower calendar range.
                resolve_field(column, &mut execution, &mut metadata, 0)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            columns: values,
            context: execution,
            memory: context.memory().clone(),
            _metadata: metadata,
        })
    }

    pub(super) fn value(&mut self, column: usize, row: usize) -> Result<Value<'static>> {
        let array = self
            .columns
            .get(column)
            .ok_or_else(|| failed("column index is absent"))?;
        scalar_value(array, row, &mut self.context)
    }

    pub(super) fn column(&self, column: usize) -> Result<ArrayRef> {
        self.columns
            .get(column)
            .cloned()
            .ok_or_else(|| failed("column index is absent"))
    }

    pub(super) fn row(&mut self, columns: &[usize], row: usize) -> Result<OwnedRow> {
        self.row_with_padding(columns, row, 0)
    }

    pub(super) fn row_with_padding(
        &mut self,
        columns: &[usize],
        row: usize,
        padding: usize,
    ) -> Result<OwnedRow> {
        let width = columns
            .len()
            .checked_add(padding)
            .ok_or_else(|| failed("row width overflow"))?;
        let mut lease = self.memory.reserve(
            u64::try_from(width.saturating_mul(std::mem::size_of::<ScalarValue>()))
                .map_err(vortex_error)?,
        )?;
        let mut values = Vec::new();
        values.try_reserve_exact(width).map_err(vortex_error)?;
        if values.capacity() > width {
            return Err(failed("retained row exceeded reserved capacity"));
        }
        for &column in columns {
            let value = self.value(column, row)?;
            values.push(owned_scalar(
                value,
                self.columns[column].dtype(),
                &mut lease,
            )?);
        }
        values.resize(width, ScalarValue::Null);
        Ok(OwnedRow { values, lease })
    }

    pub(in crate::local_primitives) fn stat(
        &mut self,
        column: usize,
        row: usize,
    ) -> Result<OwnedStat> {
        let mut lease = self
            .memory
            .reserve(std::mem::size_of::<StatValue>() as u64)?;
        let value = owned_stat(self.value(column, row)?, &mut lease)?;
        Ok(OwnedStat(Budgeted::new(value, lease)))
    }

    pub(super) fn retained(&mut self, column: usize, row: usize) -> Result<OwnedScalar> {
        let mut lease = self
            .memory
            .reserve(std::mem::size_of::<ScalarValue>() as u64)?;
        let value = self.value(column, row)?;
        let value = owned_scalar(value, self.columns[column].dtype(), &mut lease)?;
        Ok(OwnedScalar(Budgeted::new(value, lease)))
    }

    pub(super) fn key(&mut self, columns: &[usize], row: usize) -> Result<Budgeted<String>> {
        let mut count = ByteCount::default();
        self.write_key(columns, row, &mut count)?;
        let lease = self
            .memory
            .reserve(u64::try_from(count.0).map_err(vortex_error)?)?;
        let mut key = String::new();
        key.try_reserve_exact(count.0).map_err(vortex_error)?;
        if key.capacity() > count.0 {
            return Err(failed("exact key exceeded reserved capacity"));
        }
        self.write_key(columns, row, &mut key)?;
        if key.len() != count.0 {
            return Err(failed("native key changed during construction"));
        }
        Ok(Budgeted::new(key, lease))
    }

    fn write_key(
        &mut self,
        columns: &[usize],
        row: usize,
        output: &mut impl std::fmt::Write,
    ) -> Result<()> {
        for &column in columns {
            let value = self.value(column, row)?;
            let mut count = ByteCount::default();
            let dtype = self.columns[column].dtype();
            write_value_key(&mut count, &value, dtype).map_err(vortex_error)?;
            write!(output, "|{}:", count.0).map_err(vortex_error)?;
            write_value_key(output, &value, dtype).map_err(vortex_error)?;
        }
        Ok(())
    }
}

pub(super) struct OwnedRow {
    values: Vec<ScalarValue>,
    lease: MemoryLease,
}

pub(in crate::local_primitives) struct OwnedStat(Budgeted<StatValue>);
pub(super) struct OwnedScalar(Budgeted<ScalarValue>);

impl std::borrow::Borrow<StatValue> for OwnedStat {
    fn borrow(&self) -> &StatValue {
        self.0.value()
    }
}

impl OwnedRow {
    pub(super) fn values(&self) -> &[ScalarValue] {
        &self.values
    }

    pub(super) fn replace(&mut self, column: usize, value: OwnedScalar) -> Result<()> {
        let current = self
            .values
            .get_mut(column)
            .ok_or_else(|| failed("rewrite column is absent"))?;
        let old_bytes = payload_capacity(current)? as u64;
        let (value, mut credit) = value.0.into_parts();
        self.lease.absorb(&mut credit)?;
        *current = value;
        self.lease
            .resize(self.lease.bytes() - old_bytes - std::mem::size_of::<ScalarValue>() as u64)
    }
}

fn payload_capacity(value: &ScalarValue) -> Result<usize> {
    Ok(match value {
        ScalarValue::Utf8(value) => value.capacity(),
        ScalarValue::Binary(value) => value.capacity(),
        ScalarValue::List(_) | ScalarValue::Struct(_) => {
            return Err(failed("nested retained scalar state is not admitted"));
        }
        _ => 0,
    })
}

impl OwnedScalar {
    pub(super) fn value(&self) -> &ScalarValue {
        self.0.value()
    }

    pub(super) fn from_native(
        value: Value<'_>,
        dtype: &DType,
        memory: &LiveMemoryPool,
    ) -> Result<Self> {
        let mut credit = memory.reserve(std::mem::size_of::<ScalarValue>() as u64)?;
        let value = owned_scalar(value, dtype, &mut credit)?;
        Ok(Self(Budgeted::new(value, credit)))
    }

    pub(super) fn copy(value: &ScalarValue, memory: &LiveMemoryPool) -> Result<Self> {
        let bytes = match value {
            ScalarValue::Utf8(value) => value.len(),
            ScalarValue::Binary(value) => value.len(),
            _ => 0,
        };
        Self::produce(memory, bytes, || {
            Ok(match value {
                ScalarValue::Utf8(value) => {
                    let mut copy = String::new();
                    copy.try_reserve_exact(value.len()).map_err(vortex_error)?;
                    copy.push_str(value);
                    ScalarValue::Utf8(copy)
                }
                ScalarValue::Binary(value) => {
                    let mut copy = Vec::new();
                    copy.try_reserve_exact(value.len()).map_err(vortex_error)?;
                    copy.extend_from_slice(value);
                    ScalarValue::Binary(copy)
                }
                ScalarValue::List(_) | ScalarValue::Struct(_) => {
                    return Err(failed("nested retained scalar state is not admitted"));
                }
                value => value.clone(),
            })
        })
    }

    pub(super) fn produce(
        memory: &LiveMemoryPool,
        max_payload: usize,
        build: impl FnOnce() -> Result<ScalarValue>,
    ) -> Result<Self> {
        let total = max_payload
            .checked_add(std::mem::size_of::<ScalarValue>())
            .ok_or_else(|| failed("retained scalar reservation overflow"))?;
        let mut lease = memory.reserve(u64::try_from(total).map_err(vortex_error)?)?;
        let value = build()?;
        let capacity = payload_capacity(&value)?;
        if capacity > max_payload {
            return Err(failed(
                "retained scalar exceeded its reserved payload capacity",
            ));
        }
        lease.resize((capacity + std::mem::size_of::<ScalarValue>()) as u64)?;
        Ok(Self(Budgeted::new(value, lease)))
    }
}

pub(super) fn borrowed(value: &ScalarValue) -> Result<Value<'_>> {
    Ok(match value {
        ScalarValue::Null => Value::Null,
        ScalarValue::Boolean(value) => Value::Bool(*value),
        ScalarValue::Int64(value) | ScalarValue::TimestampMicros(value) => Value::Int(*value),
        ScalarValue::UInt64(value) => Value::UInt(*value),
        ScalarValue::Float64(value) => Value::Float(*value),
        ScalarValue::Utf8(value) => Value::Text(std::borrow::Cow::Borrowed(value)),
        ScalarValue::Binary(value) => Value::Binary(std::borrow::Cow::Borrowed(value)),
        ScalarValue::Date32(value) => Value::Int(i64::from(*value)),
        ScalarValue::Decimal128 {
            value,
            precision,
            scale,
        } => {
            shardloom_core::expression::Decimal128Operand::decimal(*value, *precision, *scale)?;
            Value::Decimal(
                *value,
                vortex::array::dtype::DecimalDType::new(
                    *precision,
                    i8::try_from(*scale).map_err(vortex_error)?,
                ),
            )
        }
        _ => return Err(failed("nested retained scalar state is not admitted")),
    })
}

pub(super) fn from_stat(value: StatValue) -> ScalarValue {
    match value {
        StatValue::Null => ScalarValue::Null,
        StatValue::Boolean(value) => ScalarValue::Boolean(value),
        StatValue::Int64(value) => ScalarValue::Int64(value),
        StatValue::UInt64(value) => ScalarValue::UInt64(value),
        StatValue::Float64(value) => ScalarValue::Float64(value),
        StatValue::Utf8(value) => ScalarValue::Utf8(value),
    }
}

/// Selected conversion for an already bound lossless common type.
pub(super) fn common_value<'a>(value: Value<'a>, target: &DType) -> Result<Value<'a>> {
    use shardloom_core::expression::Decimal128Operand;
    if let DType::Decimal(target, _) = target {
        let operand = match &value {
            Value::Null => return Ok(value),
            Value::Decimal(value, source) => Decimal128Operand::decimal(
                *value,
                source.precision(),
                u8::try_from(source.scale()).map_err(vortex_error)?,
            )?,
            Value::Int(value) => Decimal128Operand::integer(i128::from(*value), 19)?,
            Value::UInt(value) => Decimal128Operand::integer(i128::from(*value), 20)?,
            _ => {
                return Err(failed(
                    "decimal common type requires an exact decimal or integer value",
                ));
            }
        };
        let converted = operand.rescale(
            target.precision(),
            u8::try_from(target.scale()).map_err(vortex_error)?,
        )?;
        return Ok(Value::Decimal(converted.value(), *target));
    }
    Ok(value)
}

pub(super) fn scalar_key(value: &ScalarValue) -> Result<String> {
    let mut count = ByteCount::default();
    write_scalar_key(&mut count, value)?;
    let mut key = String::new();
    key.try_reserve_exact(count.0).map_err(vortex_error)?;
    if key.capacity() > count.0 {
        return Err(failed("pivot key exceeded reserved capacity"));
    }
    write_scalar_key(&mut key, value)?;
    Ok(key)
}

fn write_scalar_key(output: &mut impl std::fmt::Write, value: &ScalarValue) -> Result<()> {
    match value {
        ScalarValue::Date32(value) => write!(output, "d32:{value}"),
        ScalarValue::TimestampMicros(value) => write!(output, "tsus:{value}"),
        _ => write_value_key(output, &borrowed(value)?, &DType::Null),
    }
    .map_err(vortex_error)
}

fn owned_scalar(value: Value<'_>, dtype: &DType, lease: &mut MemoryLease) -> Result<ScalarValue> {
    Ok(match value {
        Value::Null => ScalarValue::Null,
        Value::Bool(value) => ScalarValue::Boolean(value),
        Value::Int(value) => match crate::native_payload_schema::temporal_storage(dtype) {
            Some(PType::I32) => ScalarValue::Date32(i32::try_from(value).map_err(vortex_error)?),
            Some(PType::I64) => ScalarValue::TimestampMicros(value),
            _ => ScalarValue::Int64(value),
        },
        Value::UInt(value) => ScalarValue::UInt64(value),
        Value::Float(value) => ScalarValue::Float64(value),
        Value::Text(value) => ScalarValue::Utf8(copy_text(&value, lease)?),
        Value::SharedText(value) => ScalarValue::Utf8(copy_text(value.as_str(), lease)?),
        Value::Binary(value) => ScalarValue::Binary(copy_bytes(&value, lease)?),
        Value::SharedBinary(value) => ScalarValue::Binary(copy_bytes(&value, lease)?),
        Value::Decimal(value, dtype) => ScalarValue::Decimal128 {
            value,
            precision: dtype.precision(),
            scale: u8::try_from(dtype.scale()).map_err(vortex_error)?,
        },
    })
}

fn copy_bytes(value: &[u8], lease: &mut MemoryLease) -> Result<Vec<u8>> {
    lease.resize(
        lease
            .bytes()
            .checked_add(u64::try_from(value.len()).map_err(vortex_error)?)
            .ok_or_else(|| failed("binary size overflow"))?,
    )?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(value.len()).map_err(vortex_error)?;
    if bytes.capacity() > value.len() {
        return Err(failed("retained binary exceeded reserved capacity"));
    }
    bytes.extend_from_slice(value);
    Ok(bytes)
}

impl OwnedStat {
    pub(super) fn value(&self) -> &StatValue {
        self.0.value()
    }

    pub(super) fn from_scalar(value: &ScalarValue, memory: &LiveMemoryPool) -> Result<Self> {
        if matches!(
            value,
            ScalarValue::Date32(_) | ScalarValue::TimestampMicros(_)
        ) {
            return Err(failed(
                "legacy predicate requires primitive statistics scalar values",
            ));
        }
        let mut credit = memory.reserve(std::mem::size_of::<StatValue>() as u64)?;
        let value = owned_stat(borrowed(value)?, &mut credit)?;
        Ok(Self(Budgeted::new(value, credit)))
    }
}

pub(super) fn owned_stat(value: Value<'_>, lease: &mut MemoryLease) -> Result<StatValue> {
    Ok(match value {
        Value::Null => StatValue::Null,
        Value::Bool(value) => StatValue::Boolean(value),
        Value::Int(value) => StatValue::Int64(value),
        Value::UInt(value) => StatValue::UInt64(value),
        Value::Float(value) => StatValue::Float64(value),
        Value::Text(value) => StatValue::Utf8(copy_text(value.as_ref(), lease)?),
        Value::SharedText(value) => StatValue::Utf8(copy_text(value.as_str(), lease)?),
        Value::Binary(_) | Value::SharedBinary(_) | Value::Decimal(..) => {
            return Err(failed(
                "legacy predicates and primitive literals do not admit binary or decimal values",
            ));
        }
    })
}

fn copy_text(value: &str, lease: &mut MemoryLease) -> Result<String> {
    let bytes = u64::try_from(value.len()).map_err(vortex_error)?;
    lease.resize(
        lease
            .bytes()
            .checked_add(bytes)
            .ok_or_else(|| failed("string size overflow"))?,
    )?;
    let mut text = String::new();
    text.try_reserve_exact(value.len()).map_err(vortex_error)?;
    if text.capacity() > value.len() {
        return Err(failed("retained string exceeded reserved capacity"));
    }
    text.push_str(value);
    Ok(text)
}

#[derive(Default)]
struct ByteCount(usize);

impl std::fmt::Write for ByteCount {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.0 = self.0.checked_add(value.len()).ok_or(std::fmt::Error)?;
        Ok(())
    }
}

// Identical type tags, length framing and floating bit identity to the existing
// exact row-key contract. Hash equality alone never establishes key equality.
fn write_value_key(
    output: &mut impl std::fmt::Write,
    value: &Value<'_>,
    dtype: &DType,
) -> std::fmt::Result {
    match value {
        Value::Null => output.write_str("n:null"),
        Value::Bool(value) => write!(output, "b:{value}"),
        Value::Int(value) => match crate::native_payload_schema::temporal_storage(dtype) {
            Some(PType::I32) => write!(output, "d32:{value}"),
            Some(PType::I64) => write!(output, "tsus:{value}"),
            _ => write!(output, "i:{value}"),
        },
        Value::UInt(value) => write!(output, "u:{value}"),
        Value::Float(value) => write!(output, "f:{:016x}", value.to_bits()),
        Value::Text(value) => write!(output, "s:{}:{value}", value.len()),
        Value::SharedText(value) => write!(output, "s:{}:{}", value.as_str().len(), value.as_str()),
        Value::Binary(value) => write_binary_key(output, value),
        Value::SharedBinary(value) => write_binary_key(output, value),
        Value::Decimal(value, dtype) => write!(
            output,
            "dec:{}:{}:{value}",
            dtype.precision(),
            dtype.scale()
        ),
    }
}

fn write_binary_key(output: &mut impl std::fmt::Write, value: &[u8]) -> std::fmt::Result {
    write!(output, "bin:{}:", value.len())?;
    for byte in value {
        write!(output, "{byte:02x}")?;
    }
    Ok(())
}
