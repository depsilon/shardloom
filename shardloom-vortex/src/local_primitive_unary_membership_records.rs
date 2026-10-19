//! Typed unary state and identity records; exact keys never become hashes alone.

use super::super::{BoundUnary, VortexQueryPrimitiveKind as Kind, values};
use super::{
    Builder, Counts, Entry, Keep, Key, NativeBatch, NativeExecutionContext, ReservedVec, Result,
    UnaryOutput, Value, failed,
};
use crate::local_primitives::{
    logical_field_from_native_array, native_payload,
    native_relational_record_output::Results,
    native_relational_records as records,
    native_relational_sort::Spec,
    native_relational_spill::{Ordering, State, replacement},
    result_batch,
};
use shardloom_exec::live_memory::{Budgeted, MemoryLease};
use std::borrow::Cow;
use vortex::array::{
    ArrayRef, IntoArray as _,
    arrays::{Primitive, PrimitiveArray},
    dtype::{DType, Nullability, PType},
    memory::MemorySessionExt as _,
};
use vortex::buffer::Buffer;

const LAST: usize = 4;
const COUNT: usize = 5;
const PAYLOAD: usize = 6;
const IDENTITY_BYTES: u64 = 256 * 1024;

pub(super) struct Schema {
    pub(super) state: Spec,
    pub(super) output: Spec,
    pub(super) identities: Spec,
    names: Vec<String>,
    payload_indices: Vec<usize>,
    output_names: Vec<String>,
    _metadata: MemoryLease,
}

impl Schema {
    pub(super) fn new(plan: &BoundUnary, context: &NativeExecutionContext<'_>) -> Result<Self> {
        let bytes = plan
            .fields
            .iter()
            .try_fold(32_768_u64, |bytes, (name, dtype)| {
                native_payload::metadata_bytes(dtype)?
                    .checked_add(name.len() as u64 * 2)
                    .and_then(|field| field.checked_mul(8))
                    .and_then(|field| bytes.checked_add(field))
                    .ok_or_else(|| failed("membership schema capacity overflow"))
            })?;
        let metadata = context.memory().reserve(bytes)?;
        let mut fields = vec![
            (
                replacement::INDEX.into(),
                DType::Utf8(Nullability::NonNullable),
            ),
            (replacement::KIND.into(), records::u64_type()),
            (
                replacement::DOMAIN.into(),
                DType::Utf8(Nullability::NonNullable),
            ),
            (records::ORDINAL.into(), records::u64_type()),
            ("last".into(), records::u64_type()),
            ("count".into(), records::u64_type()),
        ];
        let output_names = (0..plan.fields.len())
            .map(|index| format!("payload_{index}"))
            .collect::<Vec<_>>();
        let mut output_fields = output_names
            .iter()
            .zip(&plan.fields)
            .map(|(name, (_, dtype))| (name.clone(), dtype.clone()))
            .collect::<Vec<_>>();
        let retain = plan.request.kind == Kind::DropDuplicateRows && !super::prefix(plan);
        if retain {
            fields.extend(
                output_fields
                    .iter()
                    .map(|(name, dtype)| (name.clone(), dtype.as_nullable())),
            );
        }
        let payload_indices = (PAYLOAD..fields.len()).collect();
        let names = fields.iter().map(|(name, _)| name.clone()).collect();
        output_fields.push((records::ORDINAL.into(), records::u64_type()));
        let mut state = records::order(
            fields,
            [replacement::INDEX, replacement::KIND, replacement::DOMAIN]
                .map(str::to_owned)
                .to_vec(),
        )?;
        let mut output = records::order(output_fields, vec![records::ORDINAL.into()])?;
        // A scalar payload can be replaced, removed by multiplicity, or fall
        // beyond the final output limit. Private movement must not validate it
        // earlier than the resident selector's final selected-output boundary.
        state.copy_policy = native_payload::CopyPolicy::PreserveUnobserved;
        output.copy_policy = native_payload::CopyPolicy::PreserveUnobserved;
        Ok(Self {
            state,
            output,
            identities: records::order(
                vec![
                    (
                        replacement::INDEX.into(),
                        DType::Utf8(Nullability::NonNullable),
                    ),
                    (records::ORDINAL.into(), records::u64_type()),
                ],
                vec![records::ORDINAL.into()],
            )?,
            names,
            payload_indices,
            output_names,
            _metadata: metadata,
        })
    }

    pub(super) fn build(
        &self,
        rows: &[(&Key, &Entry)],
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(self.state.fields.len())?;
        for (column, (_, dtype)) in self.state.fields.iter().enumerate() {
            context.check_cancelled()?;
            let array = if native_payload::is_nested(dtype) {
                native_payload::retained_column(dtype, rows.len(), context, |row| {
                    match &rows[row].1.row {
                        Some(row) => row
                            .native(column - PAYLOAD)
                            .cloned()
                            .map(Some)
                            .ok_or_else(|| failed("membership nested payload is absent")),
                        None => Ok(None),
                    }
                })?
            } else {
                preserved_column(dtype, rows.len(), context, |row| {
                    context.check_cancelled()?;
                    let (key, entry) = rows[row];
                    Ok(match column {
                        0 => Value::Text(Cow::Borrowed(&key.index)),
                        1 => Value::UInt(0),
                        2 => Value::Text(Cow::Borrowed("")),
                        3 => Value::UInt(entry.counts.first),
                        LAST => Value::UInt(entry.counts.last),
                        COUNT => Value::UInt(entry.counts.count),
                        _ => match &entry.row {
                            Some(row) => values::borrowed(row.scalar(column - PAYLOAD)?)?,
                            None => Value::Null,
                        },
                    })
                })?
            };
            columns.values.push(array);
        }
        records::structure(&self.state.fields, columns, rows.len())
    }

    pub(super) fn counts(
        &self,
        row: &replacement::Row,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Counts> {
        if row.kind()? != 0 {
            return Err(failed("unary membership record kind changed"));
        }
        let mut batch = NativeBatch::new(row.array(), &self.names, context)?;
        let mut unsigned = |column| match batch.value(column, row.offset())? {
            Value::UInt(value) => Ok(value),
            _ => Err(failed("unary membership ordinal or count changed type")),
        };
        let counts = Counts {
            first: unsigned(3)?,
            last: unsigned(LAST)?,
            count: unsigned(COUNT)?,
        };
        if counts.count == 0
            || counts.first > counts.last
            || counts
                .last
                .checked_sub(counts.first)
                .and_then(|span| span.checked_add(1))
                .is_none_or(|span| counts.count > span)
        {
            return Err(failed(
                "unary membership count or source ordinals are invalid",
            ));
        }
        Ok(counts)
    }

    pub(super) fn push_stored(
        &self,
        row: &replacement::Row,
        plan: &BoundUnary,
        context: &NativeExecutionContext<'_>,
        output: &mut Results<'_>,
    ) -> Result<()> {
        let counts = self.counts(row, context)?;
        if plan.request.duplicate_keep == Keep::AllDuplicates && counts.count != 1 {
            return Ok(());
        }
        let mut batch = NativeBatch::new(row.array(), &self.names, context)?;
        let payload = batch.row(&self.payload_indices, row.offset(), context)?;
        output.push(
            self.output_record(&payload, counts, plan, context)?,
            context,
        )
    }

    pub(super) fn push_entry(
        &self,
        entry: &Entry,
        plan: &BoundUnary,
        context: &NativeExecutionContext<'_>,
        output: &mut Results<'_>,
    ) -> Result<()> {
        if plan.request.duplicate_keep == Keep::AllDuplicates && entry.counts.count != 1 {
            return Ok(());
        }
        let row = entry
            .row
            .as_ref()
            .ok_or_else(|| failed("selected membership payload is absent"))?;
        output.push(
            self.output_record(row, entry.counts, plan, context)?,
            context,
        )
    }

    fn output_record(
        &self,
        row: &values::OwnedRow,
        counts: Counts,
        plan: &BoundUnary,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(self.output.fields.len())?;
        for (column, (_, dtype)) in self.output.fields[..self.output_names.len()]
            .iter()
            .enumerate()
        {
            columns.values.push(if native_payload::is_nested(dtype) {
                native_payload::retained_column(dtype, 1, context, |_| {
                    row.native(column)
                        .cloned()
                        .map(Some)
                        .ok_or_else(|| failed("selected membership nested payload is absent"))
                })?
            } else {
                preserved_column(dtype, 1, context, |_| values::borrowed(row.scalar(column)?))?
            });
        }
        let ordinal = if plan.request.duplicate_keep == Keep::Last {
            counts.last
        } else {
            counts.first
        };
        columns
            .values
            .push(records::unsigned(1, context, |_| Ok(ordinal))?);
        records::structure(&self.output.fields, columns, 1)
    }

    pub(super) fn deliver(
        &self,
        array: &ArrayRef,
        rows: usize,
        plan: &BoundUnary,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let array = array.slice(0..rows).map_err(super::super::vortex_error)?;
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(self.output_names.len())?;
        for name in &self.output_names {
            columns
                .values
                .push(logical_field_from_native_array(&array, name)?);
        }
        records::delivered(&plan.fields, columns, rows, context)
    }
}

/// Preserve floating storage already read by the resident row owner, including
/// values that a later replacement, multiplicity decision or limit can discard.
/// Every delivered public value still passes the normal finite-value builder.
#[allow(clippy::cast_possible_truncation)] // F32 values were widened exactly by native scalar access.
fn preserved_column<'a>(
    dtype: &DType,
    rows: usize,
    context: &NativeExecutionContext<'_>,
    mut value: impl FnMut(usize) -> Result<Value<'a>>,
) -> Result<ArrayRef> {
    let storage = match dtype {
        DType::Primitive(PType::F32, nullable) => DType::Primitive(PType::U32, *nullable),
        DType::Primitive(PType::F64, nullable) => DType::Primitive(PType::U64, *nullable),
        _ => {
            return result_batch::build_column(
                dtype,
                rows,
                &context.native_session().allocator(),
                value,
            );
        }
    };
    let bits = result_batch::build_column(
        &storage,
        rows,
        &context.native_session().allocator(),
        |row| {
            context.check_cancelled()?;
            match value(row)? {
                Value::Null => Ok(Value::Null),
                Value::Float(value) => Ok(Value::UInt(
                    if matches!(dtype, DType::Primitive(PType::F32, _)) {
                        u64::from((value as f32).to_bits())
                    } else {
                        value.to_bits()
                    },
                )),
                _ => Err(failed("private membership floating payload changed type")),
            }
        },
    )?;
    let validity = bits.validity().map_err(super::super::vortex_error)?;
    let primitive = bits
        .as_opt::<Primitive>()
        .ok_or_else(|| failed("private membership floating storage is absent"))?;
    Ok(match dtype {
        DType::Primitive(PType::F32, _) => PrimitiveArray::new(
            Buffer::<f32>::from_byte_buffer(primitive.to_buffer::<u32>().into_byte_buffer()),
            validity,
        )
        .into_array(),
        DType::Primitive(PType::F64, _) => PrimitiveArray::new(
            Buffer::<f64>::from_byte_buffer(primitive.to_buffer::<u64>().into_byte_buffer()),
            validity,
        )
        .into_array(),
        _ => return Err(failed("private membership floating dtype changed")),
    })
}

struct Identity {
    key: Budgeted<String>,
    ordinal: u64,
}

pub(super) struct Identities<'p, 's> {
    spec: &'p Spec,
    order: Ordering<'p, 's>,
    pending: ReservedVec<Identity>,
    bytes: u64,
    block_rows: usize,
    max_items: usize,
}

fn copy_key(key: &str, context: &NativeExecutionContext<'_>) -> Result<Budgeted<String>> {
    let credit = context.memory().reserve(key.len() as u64)?;
    let key = key.to_owned();
    if key.capacity() as u64 > credit.bytes() {
        return Err(failed("membership key copy exceeded its credit"));
    }
    Ok(Budgeted::new(key, credit))
}

impl<'p, 's> Identities<'p, 's> {
    pub(super) fn new(
        spec: &'p Spec,
        state: &'s State,
        block_rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        Ok(Self {
            spec,
            order: Ordering::new(spec, state, block_rows, context)?,
            pending: ReservedVec::new(context.memory())?,
            bytes: 0,
            block_rows,
            max_items: 0,
        })
    }

    pub(super) fn max_items(&self) -> usize {
        self.max_items
    }

    pub(super) fn push(
        &mut self,
        key: &str,
        ordinal: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        if self.pending.values.len() >= self.block_rows
            || key.len() as u64 > IDENTITY_BYTES.saturating_sub(self.bytes)
        {
            self.flush(context)?;
        }
        let key = copy_key(key, context)?;
        self.bytes = self
            .bytes
            .checked_add(key.reserved_bytes())
            .ok_or_else(|| failed("mask identity capacity overflow"))?;
        self.pending.push(Identity { key, ordinal })?;
        self.max_items = self.max_items.max(self.pending.values.len());
        if self.bytes >= IDENTITY_BYTES {
            self.flush(context)?;
        }
        Ok(())
    }

    fn flush(&mut self, context: &NativeExecutionContext<'_>) -> Result<()> {
        if self.pending.values.is_empty() {
            return Ok(());
        }
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(2)?;
        columns.values.push(result_batch::build_column(
            &self.spec.fields[0].1,
            self.pending.values.len(),
            &context.native_session().allocator(),
            |row| {
                context.check_cancelled()?;
                Ok(Value::Text(Cow::Borrowed(
                    self.pending.values[row].key.value(),
                )))
            },
        )?);
        columns.values.push(records::unsigned(
            self.pending.values.len(),
            context,
            |row| Ok(self.pending.values[row].ordinal),
        )?);
        let array = records::structure(&self.spec.fields, columns, self.pending.values.len())?;
        self.pending.values.clear();
        self.bytes = 0;
        self.order.build(array, context)
    }

    pub(super) fn finish(
        mut self,
        schema: &Schema,
        builder: &mut Builder<'_, '_>,
        keep: Keep,
        context: &NativeExecutionContext<'_>,
        output: &mut UnaryOutput<'_, '_>,
    ) -> Result<()> {
        self.flush(context)?;
        let names = schema
            .identities
            .fields
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        self.order.finish(context, self.block_rows, &mut |array| {
            let mut batch = NativeBatch::new(&array, &names, context)?;
            let mut masks = ReservedVec::new(context.memory())?;
            masks.reserve(array.len())?;
            for row in 0..array.len() {
                context.check_cancelled()?;
                let value = batch.value(0, row)?;
                let key = match &value {
                    Value::Text(value) => copy_key(value, context)?,
                    Value::SharedText(value) => copy_key(value.as_str(), context)?,
                    _ => return Err(failed("mask identity key changed type")),
                };
                let Value::UInt(ordinal) = batch.value(1, row)? else {
                    return Err(failed("mask identity ordinal changed type"));
                };
                let (index, credit) = key.into_parts();
                let key = Key {
                    index,
                    kind: 0,
                    domain: String::new(),
                };
                let counts = builder.find(&key, context)?;
                masks.values.push(if keep == Keep::Last {
                    counts.last != ordinal
                } else {
                    counts.count > 1
                });
                drop((key, credit));
            }
            output.emit(masks.values.len(), |row, _| {
                Ok(Value::Bool(masks.values[row]))
            })
        })
    }
}
