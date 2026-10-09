//! Lossless private pivot records: exact keys, complete state and selected payloads.

use super::super::{
    NativeBatch, NativeExecutionContext, Plan, Result, Value, cells::DecimalCell, failed,
    native_payload,
};
use super::{Cell, Datum, Entry, Kind, StoredValue};
use crate::local_primitives::{
    PivotAggregateCell,
    native_capacity::ReservedVec,
    native_decimal_reduce::Total,
    native_relational_records as records,
    native_relational_sort::Spec,
    native_relational_spill::pivot::{self, Key, Row},
    result_batch,
};
use shardloom_exec::live_memory::MemoryLease;
use std::borrow::Cow;
use vortex::array::{
    ArrayRef,
    dtype::{DType, Nullability},
    memory::MemorySessionExt as _,
};

const STATE_BYTES: usize = 80;
const STATE: usize = 4;
const INDEX_VALUE: usize = 5;
const CELL_VALUE: usize = 6;

pub(super) struct Schema {
    pub(super) spec: Spec,
    names: Vec<String>,
    kind: Kind,
    first: bool,
    _metadata: MemoryLease,
}

impl Schema {
    pub(super) fn new(plan: &Plan, context: &NativeExecutionContext<'_>) -> Result<Self> {
        let first = matches!(plan.aggregate.as_str(), "first" | "first_unique");
        let payload = if first || plan.nested_extrema {
            plan.value_source.as_nullable()
        } else {
            // The private run payload uses admitted native types even when no
            // selected value is stored. Every row in this column is NULL.
            DType::Bool(Nullability::Nullable)
        };
        let metadata = context.memory().reserve(
            native_payload::metadata_bytes(&plan.index_source)?
                .checked_add(native_payload::metadata_bytes(&payload)?)
                .and_then(|bytes| bytes.checked_mul(4))
                .and_then(|bytes| bytes.checked_add(16 * 1024))
                .ok_or_else(|| failed("pivot record schema capacity overflow"))?,
        )?;
        let fields = vec![
            (
                pivot::INDEX.to_owned(),
                DType::Utf8(Nullability::NonNullable),
            ),
            (pivot::KIND.to_owned(), records::u64_type()),
            (
                pivot::DOMAIN.to_owned(),
                DType::Utf8(Nullability::NonNullable),
            ),
            (records::ORDINAL.to_owned(), records::u64_type()),
            ("state".to_owned(), DType::Binary(Nullability::NonNullable)),
            ("index_value".to_owned(), plan.index_source.as_nullable()),
            ("cell_value".to_owned(), payload),
        ];
        let names = fields.iter().map(|(name, _)| name.clone()).collect();
        let spec = records::order(
            fields,
            [pivot::INDEX, pivot::KIND, pivot::DOMAIN]
                .map(str::to_owned)
                .to_vec(),
        )?;
        Ok(Self {
            spec,
            names,
            kind: Kind::new(plan.decimal_source, plan.nested_extrema),
            first,
            _metadata: metadata,
        })
    }

    pub(super) fn build(
        &self,
        values: &[(&Key, &Entry)],
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let mut states = ReservedVec::new(context.memory())?;
        states.reserve(values.len())?;
        for (_, entry) in values {
            context.check_cancelled()?;
            states.values.push(state_bytes(&entry.value));
        }
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(self.spec.fields.len())?;
        for (column, (_, dtype)) in self.spec.fields.iter().enumerate() {
            context.check_cancelled()?;
            let array = if native_payload::is_nested(dtype) {
                native_payload::retained_column(dtype, values.len(), context, |row| {
                    payload(&values[row].1.value, column)
                        .map(Datum::native)
                        .transpose()
                })?
            } else {
                result_batch::build_column(
                    dtype,
                    values.len(),
                    &context.native_session().allocator(),
                    |row| {
                        context.check_cancelled()?;
                        let (key, entry) = values[row];
                        Ok(match column {
                            0 => Value::Text(Cow::Borrowed(&key.index)),
                            1 => Value::UInt(key.kind),
                            2 => Value::Text(Cow::Borrowed(&key.domain)),
                            3 => Value::UInt(entry.ordinal),
                            STATE => Value::Binary(Cow::Borrowed(&states.values[row])),
                            _ => {
                                if let Some(value) = payload(&entry.value, column) {
                                    super::super::borrowed(value.scalar()?)?
                                } else {
                                    Value::Null
                                }
                            }
                        })
                    },
                )?
            };
            columns.values.push(array);
        }
        records::structure(&self.spec.fields, columns, values.len())
    }

    pub(super) fn read(&self, row: &Row, context: &NativeExecutionContext<'_>) -> Result<Entry> {
        let mut batch = NativeBatch::new(row.array(), &self.names, context)?;
        let offset = row.offset();
        let Value::UInt(ordinal) = batch.value(3, offset)? else {
            return Err(failed("pivot private source ordinal changed type"));
        };
        let value = if row.kind()? == 0 {
            StoredValue::Index(
                Datum::from_batch(&mut batch, INDEX_VALUE, offset, context)?.retain(context)?,
            )
        } else if self.first {
            StoredValue::First(
                Datum::from_batch(&mut batch, CELL_VALUE, offset, context)?.retain(context)?,
            )
        } else if self.kind == Kind::Nested {
            let value = Datum::from_batch(&mut batch, CELL_VALUE, offset, context)?;
            StoredValue::Aggregate(Cell::Nested(if value.is_null()? {
                None
            } else {
                Some(value.retain(context)?)
            }))
        } else {
            let raw = batch.value(STATE, offset)?;
            let bytes = match &raw {
                Value::Binary(value) => value.as_ref(),
                Value::SharedBinary(value) => value.as_ref(),
                _ => return Err(failed("pivot private accumulator changed type")),
            };
            let bytes: &[u8; STATE_BYTES] = bytes
                .try_into()
                .map_err(|_| failed("pivot private accumulator changed width"))?;
            StoredValue::Aggregate(read_state(bytes, self.kind)?)
        };
        Ok(Entry { ordinal, value })
    }

    /// Selected scalar output shares native byte buffers and their credits. It
    /// does not borrow a Rust scalar from a cache entry that may be evicted.
    pub(super) fn scalar(
        &self,
        row: &Row,
        index: bool,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Value<'static>> {
        let column = if index { INDEX_VALUE } else { CELL_VALUE };
        let mut batch = NativeBatch::new(row.array(), &self.names, context)?;
        batch.value(column, row.offset())
    }
}

fn payload(value: &StoredValue, column: usize) -> Option<&Datum> {
    match (value, column) {
        (StoredValue::Index(value), INDEX_VALUE) | (StoredValue::First(value), CELL_VALUE) => {
            Some(value)
        }
        (StoredValue::Aggregate(Cell::Nested(value)), CELL_VALUE) => value.as_ref(),
        _ => None,
    }
}

fn state_bytes(value: &StoredValue) -> [u8; STATE_BYTES] {
    let mut bytes = [0; STATE_BYTES];
    match value {
        StoredValue::Aggregate(Cell::Primitive(cell)) => {
            bytes[..8].copy_from_slice(&cell.count.to_le_bytes());
            bytes[8..16].copy_from_slice(&cell.sum.to_bits().to_le_bytes());
            if let Some(value) = cell.min {
                bytes[40..48].copy_from_slice(&value.to_bits().to_le_bytes());
                bytes[72] |= 1;
            }
            if let Some(value) = cell.max {
                bytes[56..64].copy_from_slice(&value.to_bits().to_le_bytes());
                bytes[72] |= 2;
            }
        }
        StoredValue::Aggregate(Cell::Decimal(_, cell)) => {
            let (sum, count) = cell.total.spill_parts();
            bytes[..8].copy_from_slice(&count.to_le_bytes());
            bytes[8..40].copy_from_slice(&sum);
            if let Some(value) = cell.min {
                bytes[40..56].copy_from_slice(&value.to_le_bytes());
                bytes[72] |= 1;
            }
            if let Some(value) = cell.max {
                bytes[56..72].copy_from_slice(&value.to_le_bytes());
                bytes[72] |= 2;
            }
        }
        _ => {}
    }
    bytes
}

fn read_state(bytes: &[u8; STATE_BYTES], kind: Kind) -> Result<Cell> {
    if bytes[72] > 3 || bytes[73..].iter().any(|byte| *byte != 0) {
        return Err(failed("pivot private accumulator flags are invalid"));
    }
    let count = u64::from_le_bytes(
        bytes[..8]
            .try_into()
            .map_err(|_| failed("pivot count width changed"))?,
    );
    if count == 0 {
        return Err(failed("pivot aggregate record has no input count"));
    }
    match kind {
        Kind::Primitive => {
            if bytes[16..40]
                .iter()
                .chain(&bytes[48..56])
                .chain(&bytes[64..72])
                .any(|byte| *byte != 0)
            {
                return Err(failed("pivot primitive state has invalid reserved bytes"));
            }
            let value = |start| -> Result<f64> {
                Ok(f64::from_bits(u64::from_le_bytes(
                    bytes[start..start + 8]
                        .try_into()
                        .map_err(|_| failed("pivot floating state width changed"))?,
                )))
            };
            Ok(Cell::Primitive(PivotAggregateCell {
                count,
                sum: value(8)?,
                min: (bytes[72] & 1 != 0).then(|| value(40)).transpose()?,
                max: (bytes[72] & 2 != 0).then(|| value(56)).transpose()?,
            }))
        }
        Kind::Decimal(source) => {
            let value = |start| -> Result<i128> {
                Ok(i128::from_le_bytes(
                    bytes[start..start + 16]
                        .try_into()
                        .map_err(|_| failed("pivot decimal state width changed"))?,
                ))
            };
            Ok(Cell::Decimal(
                source,
                DecimalCell {
                    total: Total::from_spill_parts(
                        bytes[8..40]
                            .try_into()
                            .map_err(|_| failed("pivot wide state width changed"))?,
                        count,
                    ),
                    min: (bytes[72] & 1 != 0).then(|| value(40)).transpose()?,
                    max: (bytes[72] & 2 != 0).then(|| value(56)).transpose()?,
                },
            ))
        }
        Kind::Nested => Err(failed("nested pivot state requires its native payload")),
    }
}
