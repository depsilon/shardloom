//! Credited source, ordering and scalar-result records for bounded windows.

use super::{NativeExecutionContext, Result, failed};
use crate::local_primitives::{
    SimpleAggregateFunction as Aggregate, logical_field_from_native_array,
    native_capacity::ReservedVec,
    native_payload::{self, CopyPolicy},
    native_relational_batch::{Batch, index_array},
    native_relational_records::{self as records, ORDINAL},
    native_relational_sort,
    native_relational_spill::{Ordering, State, StoredOrder},
    native_relational_window::{Function, Group, Spec},
    native_relational_window_frame as frame,
    result_batch::{self, Value},
    vortex_error,
};
use shardloom_exec::live_memory::MemoryLease;
use std::ops::Range;
use vortex::array::{
    ArrayRef, IntoArray as _, VortexSessionExecute as _,
    arrays::StructArray,
    dtype::{DType, DecimalDType, FieldName, Nullability, PType},
    memory::MemorySessionExt as _,
};

pub(super) const DATA: &str = "data";
pub(super) const VALUE: &str = "value";

#[derive(Clone, Copy)]
pub(super) enum Kind {
    Integer,
    Count,
    Float,
    NullableFloat,
    Decimal(DecimalDType),
    Source,
}

impl Kind {
    fn new(spec: &Spec, index: usize) -> Result<Self> {
        Ok(match &spec.functions[index] {
            Function::Framed(_) => {
                let frame = spec.frames[index]
                    .as_ref()
                    .ok_or_else(|| failed("window frame is absent"))?;
                match frame.function {
                    frame::Function::Aggregate(Aggregate::Count | Aggregate::CountDistinct) => {
                        Self::Count
                    }
                    frame::Function::Aggregate(Aggregate::Sum | Aggregate::Avg) => {
                        match &spec.fields[spec.columns.len() + index].1 {
                            DType::Decimal(dtype, _) => Self::Decimal(*dtype),
                            DType::Primitive(PType::F64, Nullability::Nullable) => {
                                Self::NullableFloat
                            }
                            _ => {
                                return Err(failed(
                                    "stored window reduction has an unsupported bound output type",
                                ));
                            }
                        }
                    }
                    _ => Self::Source,
                }
            }
            Function::Lag { .. } | Function::Lead { .. } => Self::Source,
            Function::PercentRank | Function::CumeDist => Self::Float,
            _ => Self::Integer,
        })
    }

    pub(super) fn dtype(self) -> DType {
        match self {
            Self::Integer | Self::Count => records::u64_type(),
            Self::Float => DType::Primitive(PType::F64, Nullability::NonNullable),
            Self::NullableFloat => DType::Primitive(PType::F64, Nullability::Nullable),
            Self::Decimal(dtype) => DType::Decimal(dtype, Nullability::Nullable),
            Self::Source => records::u64_type().as_nullable(),
        }
    }

    fn value(self, value: &frame::Value) -> Result<Value<'static>> {
        Ok(match (self, value) {
            (Self::Integer, frame::Value::Integer(value))
            | (Self::Count, frame::Value::Count(value)) => Value::UInt(*value),
            (Self::Float, frame::Value::Float(Some(value))) => Value::Float(*value),
            (Self::NullableFloat, frame::Value::Float(value)) => {
                value.map_or(Value::Null, Value::Float)
            }
            (Self::Decimal(dtype), frame::Value::Decimal(value)) => {
                value.map_or(Value::Null, |value| Value::Decimal(value, dtype))
            }
            (Self::Source, frame::Value::Source(value)) => match value {
                None => Value::Null,
                Some(value) => Value::UInt(u64::try_from(*value).map_err(vortex_error)?),
            },
            _ => {
                return Err(failed(
                    "stored window value disagrees with its bound output type",
                ));
            }
        })
    }
}

pub(super) struct ResultLayout {
    pub(super) order: native_relational_sort::Spec,
    pub(super) kind: Kind,
    pub(super) name: String,
}

impl ResultLayout {
    pub(super) fn positions() -> Result<Self> {
        Ok(Self {
            order: records::order(
                vec![
                    (ORDINAL.into(), records::u64_type()),
                    (VALUE.into(), records::u64_type().as_nullable()),
                ],
                vec![ORDINAL.into()],
            )?,
            kind: Kind::Source,
            name: VALUE.into(),
        })
    }
}

pub(super) struct Layout {
    pub(super) source: native_relational_sort::Spec,
    pub(super) group_fields: Vec<(String, DType)>,
    pub(super) group_names: Vec<String>,
    pub(super) results: Vec<ResultLayout>,
    pub(super) peers: native_relational_sort::Spec,
    payload_names: Vec<FieldName>,
    payload_dtype: DType,
    _metadata: MemoryLease,
}

impl Layout {
    pub(super) fn new(
        spec: &Spec,
        input: &[(String, DType)],
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let needed = |name: &String| {
            spec.keys.contains(name) || spec.columns.contains(name)
            || spec.functions.iter().any(|function| matches!(function, Function::Lag { column, .. } | Function::Lead { column, .. } if column.as_str() == name))
            || spec.frames.iter().flatten().any(|frame| frame.column.as_ref() == Some(name))
        };
        let bytes = input
            .iter()
            .filter(|(name, _)| needed(name))
            .try_fold(16_384u64, |bytes, (name, dtype)| {
                native_payload::metadata_bytes(dtype)?
                    .checked_add(name.len() as u64 * 2)
                    .and_then(|field| field.checked_add(2048))
                    .and_then(|field| field.checked_mul(8))
                    .and_then(|field| bytes.checked_add(field))
                    .ok_or_else(|| failed("window descriptor capacity overflow"))
            })?
            .checked_add(
                (spec.functions.len() as u64)
                    .checked_mul(16_384)
                    .ok_or_else(|| failed("window function descriptor overflow"))?,
            )
            .ok_or_else(|| failed("window descriptor capacity overflow"))?;
        let metadata = context.memory().reserve(bytes)?;
        let fields = input
            .iter()
            .filter(|(name, _)| needed(name))
            .cloned()
            .collect::<Vec<_>>();
        let payload_names = fields
            .iter()
            .map(|(name, _)| FieldName::from(name.as_str()))
            .collect();
        let payload_dtype = if fields.is_empty() {
            DType::Bool(Nullability::NonNullable)
        } else {
            DType::struct_(fields, Nullability::Nullable)
        };
        let mut source = records::order(
            vec![
                (ORDINAL.into(), records::u64_type()),
                (DATA.into(), payload_dtype.clone()),
            ],
            vec![ORDINAL.into()],
        )?;
        source.copy_policy = CopyPolicy::PreserveUnobserved;
        let mut group_fields = Vec::with_capacity(spec.keys.len() + 1);
        let mut group_names = Vec::with_capacity(spec.keys.len() + 1);
        for (index, key) in spec.keys.iter().enumerate() {
            let dtype = input
                .iter()
                .find(|(name, _)| name == key)
                .map(|(_, dtype)| dtype.as_nullable())
                .ok_or_else(|| failed("window key dtype is absent"))?;
            let name = format!("k{index}");
            group_fields.push((name.clone(), dtype));
            group_names.push(name);
        }
        group_fields.push((ORDINAL.into(), records::u64_type()));
        group_names.push(ORDINAL.into());
        let mut results = Vec::with_capacity(spec.functions.len());
        for index in 0..spec.functions.len() {
            let kind = Kind::new(spec, index)?;
            results.push(ResultLayout {
                order: records::order(
                    vec![
                        (ORDINAL.into(), records::u64_type()),
                        (VALUE.into(), kind.dtype()),
                    ],
                    vec![ORDINAL.into()],
                )?,
                kind,
                name: format!("r{index}"),
            });
        }
        Ok(Self {
            source,
            group_fields,
            group_names,
            results,
            peers: records::order(
                vec![(ORDINAL.into(), records::u64_type())],
                vec![ORDINAL.into()],
            )?,
            payload_names,
            payload_dtype,
            _metadata: metadata,
        })
    }

    pub(super) fn source_record(
        &self,
        input: &Batch,
        range: Range<usize>,
        ordinal: u64,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let rows = range.len();
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(2)?;
        columns.values.push(records::unsigned(rows, context, |row| {
            ordinal
                .checked_add((range.start + row) as u64)
                .ok_or_else(|| failed("window original ordinal overflow"))
        })?);
        let payload = if self.payload_names.is_empty() {
            result_batch::build_column(
                &self.payload_dtype,
                rows,
                &context.native_session().allocator(),
                |_| Ok(Value::Bool(false)),
            )?
        } else {
            let indices = index_array(rows, false, context, |row| Ok(Some(range.start + row)))?;
            let mut execution = context.native_session().create_execution_ctx();
            let source = input
                .array
                .clone()
                .execute::<StructArray>(&mut execution)
                .map_err(vortex_error)?;
            let projected = source
                .project(&self.payload_names)
                .map_err(vortex_error)?
                .into_array();
            native_payload::take_with_policy(
                &projected,
                &indices,
                &self.payload_dtype,
                CopyPolicy::PreserveUnobserved,
                context,
            )?
        };
        columns.values.push(payload);
        records::structure(&self.source.fields, columns, rows)
    }

    pub(super) fn group_order(&self, group: &Group) -> Result<native_relational_sort::Spec> {
        let mut names = group
            .partition
            .iter()
            .chain(group.order.iter().map(|key| &key.key))
            .map(|&key| self.group_names[key].clone())
            .collect::<Vec<_>>();
        names.push(ORDINAL.into());
        let mut order = records::order(self.group_fields.clone(), names)?;
        for (slot, key) in order.keys[group.partition.len()..]
            .iter_mut()
            .zip(&group.order)
        {
            slot.descending = key.descending;
            slot.nulls = Some(
                key.nulls
                    .unwrap_or(crate::relational_query::VortexRelationalNullOrder::Last),
            );
        }
        order.copy_policy = CopyPolicy::PreserveUnobserved;
        Ok(order)
    }

    pub(super) fn group_record(
        &self,
        spec: &Spec,
        source: &ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let indices = index_array(source.len(), false, context, |row| Ok(Some(row)))?;
        let payload = logical_field_from_native_array(source, DATA)?;
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(self.group_fields.len())?;
        for (index, name) in spec.keys.iter().enumerate() {
            let column = logical_field_from_native_array(&payload, name)?;
            columns.values.push(native_payload::take_with_policy(
                &column,
                &indices,
                &self.group_fields[index].1,
                CopyPolicy::PreserveUnobserved,
                context,
            )?);
        }
        columns.values.push(native_payload::take(
            &logical_field_from_native_array(source, ORDINAL)?,
            &indices,
            &records::u64_type(),
            context,
        )?);
        records::structure(&self.group_fields, columns, source.len())
    }

    pub(super) fn group_results(&self, group: &Group) -> Result<native_relational_sort::Spec> {
        let fields = std::iter::once((ORDINAL.into(), records::u64_type()))
            .chain(group.functions.iter().map(|&index| {
                (
                    self.results[index].name.clone(),
                    self.results[index].kind.dtype(),
                )
            }))
            .collect();
        records::order(fields, vec![ORDINAL.into()])
    }
}

pub(super) struct Values<'p, 's> {
    layout: &'p ResultLayout,
    order: Ordering<'p, 's>,
    buffer: ReservedVec<frame::Value>,
    rows: usize,
    limit: usize,
}

impl<'p, 's> Values<'p, 's> {
    pub(super) fn new(
        layout: &'p ResultLayout,
        spill: &'s State,
        batch_rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let limit = batch_rows.clamp(1, 1024);
        let mut buffer = ReservedVec::new(context.memory())?;
        buffer.reserve(limit)?;
        Ok(Self {
            layout,
            order: Ordering::new(&layout.order, spill, limit, context)?,
            buffer,
            rows: 0,
            limit,
        })
    }

    pub(super) fn push(
        &mut self,
        value: frame::Value,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        self.layout.kind.value(&value)?;
        self.buffer.values.push(value);
        if self.buffer.values.len() == self.limit {
            self.flush(context)?;
        }
        Ok(())
    }

    fn flush(&mut self, context: &NativeExecutionContext<'_>) -> Result<()> {
        if self.buffer.values.is_empty() {
            return Ok(());
        }
        let count = self.buffer.values.len();
        let end = self
            .rows
            .checked_add(count)
            .ok_or_else(|| failed("window result row count overflow"))?;
        let mut columns = ReservedVec::new(context.memory())?;
        columns.reserve(2)?;
        columns
            .values
            .push(records::unsigned(count, context, |row| {
                u64::try_from(self.rows + row).map_err(vortex_error)
            })?);
        columns.values.push(result_batch::build_column(
            &self.layout.kind.dtype(),
            count,
            &context.native_session().allocator(),
            |row| self.layout.kind.value(&self.buffer.values[row]),
        )?);
        self.order.build(
            records::structure(&self.layout.order.fields, columns, count)?,
            context,
        )?;
        self.rows = end;
        self.buffer.values.clear();
        Ok(())
    }

    pub(super) fn finish(
        mut self,
        rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<StoredOrder<'s>> {
        self.flush(context)?;
        if self.rows != rows {
            return Err(failed(
                "window function produced a different partition length",
            ));
        }
        self.order.retain(context)
    }
}

pub(super) struct Ordinals<'p, 's> {
    spec: &'p native_relational_sort::Spec,
    order: Ordering<'p, 's>,
    values: ReservedVec<u64>,
    limit: usize,
}

impl<'p, 's> Ordinals<'p, 's> {
    pub(super) fn new(
        spec: &'p native_relational_sort::Spec,
        spill: &'s State,
        batch_rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let limit = batch_rows.clamp(1, 1024);
        let mut values = ReservedVec::new(context.memory())?;
        values.reserve(limit)?;
        Ok(Self {
            spec,
            order: Ordering::new(spec, spill, limit, context)?,
            values,
            limit,
        })
    }
    pub(super) fn push(
        &mut self,
        value: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        self.values
            .values
            .push(u64::try_from(value).map_err(vortex_error)?);
        if self.values.values.len() == self.limit {
            self.flush(context)?;
        }
        Ok(())
    }
    fn flush(&mut self, context: &NativeExecutionContext<'_>) -> Result<()> {
        if self.values.values.is_empty() {
            return Ok(());
        }
        let rows = self.values.values.len();
        let mut columns = ReservedVec::new(context.memory())?;
        columns.push(records::unsigned(rows, context, |row| {
            Ok(self.values.values[row])
        })?)?;
        self.order.build(
            records::structure(&self.spec.fields, columns, rows)?,
            context,
        )?;
        self.values.values.clear();
        Ok(())
    }
    pub(super) fn finish(
        mut self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<StoredOrder<'s>> {
        self.flush(context)?;
        self.order.retain(context)
    }
}
