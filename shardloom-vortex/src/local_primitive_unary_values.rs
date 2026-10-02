//! Native scalar access only for keys, predicates and selected final values.
//! Strings remain borrowed native buffers until retained state needs a copy.

use super::{Result, StatValue, Value, failed, vortex_error};
use shardloom_exec::live_memory::{Budgeted, LiveMemoryPool, MemoryLease};
use vortex::array::{ArrayRef, ExecutionCtx, VortexSessionExecute as _};

pub(super) use super::super::result_batch::scalar_value;

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
        let metadata = context
            .memory()
            .reserve(u64::try_from(columns.len().saturating_mul(512)).map_err(vortex_error)?)?;
        let values = columns
            .iter()
            .map(|name| {
                if array.dtype().is_struct() {
                    super::super::logical_field_from_native_array(array, name)
                } else if columns.len() == 1 {
                    Ok(array.clone())
                } else {
                    Err(failed("a non-struct source must expose one column"))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            columns: values,
            context: context.native_session().create_execution_ctx(),
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
            u64::try_from(width.saturating_mul(std::mem::size_of::<StatValue>()))
                .map_err(vortex_error)?,
        )?;
        let mut values = Vec::new();
        values.try_reserve_exact(width).map_err(vortex_error)?;
        if values.capacity() > width {
            return Err(failed("retained row exceeded reserved capacity"));
        }
        for &column in columns {
            let value = self.value(column, row)?;
            values.push(owned_stat(value, &mut lease)?);
        }
        values.resize(width, StatValue::Null);
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
            write_value_key(&mut count, &value).map_err(vortex_error)?;
            write!(output, "|{}:", count.0).map_err(vortex_error)?;
            write_value_key(output, &value).map_err(vortex_error)?;
        }
        Ok(())
    }
}

pub(super) struct OwnedRow {
    values: Vec<StatValue>,
    lease: MemoryLease,
}

pub(in crate::local_primitives) struct OwnedStat(Budgeted<StatValue>);

impl std::borrow::Borrow<StatValue> for OwnedStat {
    fn borrow(&self) -> &StatValue {
        self.0.value()
    }
}

impl OwnedRow {
    pub(super) fn values(&self) -> &[StatValue] {
        &self.values
    }

    pub(super) fn replace(&mut self, column: usize, value: OwnedStat) -> Result<()> {
        let current = self
            .values
            .get_mut(column)
            .ok_or_else(|| failed("rewrite column is absent"))?;
        let old_bytes = text_capacity(current) as u64;
        let (value, mut credit) = value.0.into_parts();
        self.lease.absorb(&mut credit)?;
        *current = value;
        self.lease
            .resize(self.lease.bytes() - old_bytes - std::mem::size_of::<StatValue>() as u64)
    }
}

fn text_capacity(value: &StatValue) -> usize {
    if let StatValue::Utf8(value) = value {
        value.capacity()
    } else {
        0
    }
}

impl OwnedStat {
    pub(super) fn value(&self) -> &StatValue {
        self.0.value()
    }

    pub(super) fn copy(value: &StatValue, memory: &LiveMemoryPool) -> Result<Self> {
        let mut credit = memory.reserve(std::mem::size_of::<StatValue>() as u64)?;
        let value = owned_stat(Value::from(value), &mut credit)?;
        Ok(Self(Budgeted::new(value, credit)))
    }

    pub(super) fn produce(
        memory: &LiveMemoryPool,
        max_payload: usize,
        build: impl FnOnce() -> Result<StatValue>,
    ) -> Result<Self> {
        let total = max_payload
            .checked_add(std::mem::size_of::<StatValue>())
            .ok_or_else(|| failed("rewrite reservation overflow"))?;
        let mut lease = memory.reserve(u64::try_from(total).map_err(vortex_error)?)?;
        let value = build()?;
        let capacity = text_capacity(&value);
        if capacity > max_payload {
            return Err(failed("rewrite exceeded its reserved string capacity"));
        }
        lease.resize((capacity + std::mem::size_of::<StatValue>()) as u64)?;
        Ok(Self(Budgeted::new(value, lease)))
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
fn write_value_key(output: &mut impl std::fmt::Write, value: &Value<'_>) -> std::fmt::Result {
    match value {
        Value::Null => output.write_str("n:null"),
        Value::Bool(value) => write!(output, "b:{value}"),
        Value::Int(value) => write!(output, "i:{value}"),
        Value::UInt(value) => write!(output, "u:{value}"),
        Value::Float(value) => write!(output, "f:{:016x}", value.to_bits()),
        Value::Text(value) => write!(output, "s:{}:{value}", value.len()),
        Value::SharedText(value) => write!(output, "s:{}:{}", value.as_str().len(), value.as_str()),
    }
}
