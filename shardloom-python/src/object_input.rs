//! Ordinary Python values become bounded, admitted columnar construction
//! batches. JSON is used for declared type metadata only, never for cell data.

use super::native_error;
use arrow_array::{
    RecordBatch,
    builder::{
        ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
        FixedSizeListBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
        Int32Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
        TimestampMicrosecondBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
    },
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use pyo3::{
    IntoPyObjectExt as _,
    exceptions::{PyKeyError, PyTypeError, PyValueError},
    prelude::*,
    types::{
        PyBool, PyByteArray, PyBytes, PyFloat, PyInt, PyMapping, PyMemoryView, PyModule,
        PySequence, PyString, PyTuple,
    },
};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{Budgeted, MemoryLease},
};
use shardloom_vortex::resident_memory_source::MemoryBatchSourceBuilder;
use std::sync::Arc;

pub(super) const CONSTRUCTION_ROWS: usize = 2048;

pub(super) fn import_rows(
    rows: &Py<PyAny>,
    fields: Option<&[(String, String)]>,
    source: &mut MemoryBatchSourceBuilder,
    cancellation: &CancellationToken,
) -> PyResult<()> {
    let (schema, row_count) = Python::attach(|py| {
        let rows = sequence(rows.bind(py))?;
        let count = rows.len()?;
        let schema = match fields {
            Some(fields) => source.object_input_schema(fields).map_err(native_error)?,
            None => infer_schema(&rows, source, cancellation)?,
        };
        Ok::<_, PyErr>((schema, count))
    })?;
    if row_count == 0 {
        source
            .push_arrow(RecordBatch::new_empty(Arc::clone(schema.value())))
            .map_err(native_error)?;
    }
    for start in (0..row_count).step_by(CONSTRUCTION_ROWS) {
        cancellation.check().map_err(native_error)?;
        let end = start.saturating_add(CONSTRUCTION_ROWS).min(row_count);
        let batch = Python::attach(|py| {
            let rows = sequence(rows.bind(py))?;
            if rows.len()? != row_count {
                return Err(invalid("row sequence length changed during native intake"));
            }
            convert_batch(py, &rows, start..end, schema.value(), source, cancellation)
        })?;
        let (batch, _scratch) = batch.into_parts();
        // Native ownership/copying is Rust-only and runs detached. The temporary
        // construction credit stays live until the admitted copy has finished.
        source.push_arrow(batch).map_err(native_error)?;
    }
    Ok(())
}

pub(super) fn sequence<'py>(value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PySequence>> {
    if value.is_instance_of::<PyString>()
        || value.is_instance_of::<PyBytes>()
        || value.is_instance_of::<PyByteArray>()
        || value.is_instance_of::<PyMemoryView>()
    {
        return Err(PyTypeError::new_err(
            "native row/list input requires a sequence",
        ));
    }
    value
        .cast::<PySequence>()
        .cloned()
        .map_err(|_| PyTypeError::new_err("native row/list input requires a sequence"))
}

fn mapping<'py>(row: &Bound<'py, PyAny>, columns: usize) -> PyResult<Bound<'py, PyMapping>> {
    let row = row
        .cast::<PyMapping>()
        .map_err(|_| PyTypeError::new_err("native rows and structs require mappings"))?;
    if row.len()? != columns {
        return Err(invalid(
            "native mapping must match its complete declared fields",
        ));
    }
    Ok(row.clone())
}

fn field_value<'py>(row: &Bound<'py, PyMapping>, name: &str) -> PyResult<Bound<'py, PyAny>> {
    row.get_item(name).map_err(|error| {
        if error.is_instance_of::<PyKeyError>(row.py()) {
            invalid("native mapping must match its complete declared fields")
        } else {
            error
        }
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Inferred {
    Empty,
    Bool,
    Int,
    Float,
    Text,
}

impl Inferred {
    fn observe(self, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let next = if value.is_none() {
            return Ok(self);
        } else if value.is_instance_of::<PyBool>() {
            Self::Bool
        } else if value.is_instance_of::<PyInt>() {
            Self::Int
        } else if value.is_instance_of::<PyFloat>() {
            Self::Float
        } else if value.is_instance_of::<PyString>() {
            Self::Text
        } else {
            return Err(PyTypeError::new_err(
                "rich native object input requires an explicit schema",
            ));
        };
        match (self, next) {
            (Self::Empty, next) => Ok(next),
            (Self::Int | Self::Float, Self::Int | Self::Float) if self != next => Ok(Self::Float),
            (prior, next) if prior == next => Ok(prior),
            _ => Err(PyTypeError::new_err(
                "row values in a column must share one scalar type",
            )),
        }
    }

    fn dtype(self) -> DataType {
        match self {
            Self::Empty | Self::Bool => DataType::Boolean,
            Self::Int => DataType::Int64,
            Self::Float => DataType::Float64,
            Self::Text => DataType::Utf8,
        }
    }
}

fn infer_schema(
    rows: &Bound<'_, PySequence>,
    source: &MemoryBatchSourceBuilder,
    cancellation: &CancellationToken,
) -> PyResult<Budgeted<SchemaRef>> {
    if rows.is_empty()? {
        return Err(invalid(
            "empty native row input requires an explicit schema",
        ));
    }
    let first = rows.get_item(0)?;
    let first = first
        .cast::<PyMapping>()
        .map_err(|_| PyTypeError::new_err("native rows require mappings"))?;
    let count = first.len()?;
    if count == 0 {
        return Err(invalid("native input requires at least one field"));
    }
    let initial = u64::try_from(count)
        .ok()
        .and_then(|n| n.checked_mul(4096))
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(|| invalid("schema capacity overflow"))?;
    let mut credit = source.reserve_scratch(initial).map_err(native_error)?;
    let keys = first.keys()?;
    let mut names = Vec::with_capacity(count);
    for key in keys.try_iter()? {
        let key = key?;
        let name = key.cast::<PyString>()?.to_str()?;
        if name.is_empty() || name.len() > 256 {
            return Err(invalid(
                "native input field names require 1..=256 UTF8 bytes",
            ));
        }
        grow_credit(&mut credit, name.len())?;
        names.push(name.to_owned());
    }
    let mut kinds = vec![Inferred::Empty; count];
    for index in 0..rows.len()? {
        if index % 256 == 0 {
            cancellation.check().map_err(native_error)?;
            rows.py().check_signals()?;
        }
        let row = mapping(&rows.get_item(index)?, count)?;
        for (name, kind) in names.iter().zip(&mut kinds) {
            *kind = kind.observe(&field_value(&row, name)?)?;
        }
    }
    let fields = names
        .into_iter()
        .zip(kinds)
        .map(|(name, kind)| Field::new(name, kind.dtype(), true))
        .collect::<Vec<_>>();
    Ok(Budgeted::new(Arc::new(Schema::new(fields)), credit))
}

fn nodes(dtype: &DataType) -> PyResult<u64> {
    let children = match dtype {
        DataType::List(field) | DataType::FixedSizeList(field, _) => nodes(field.data_type())?,
        DataType::Struct(fields) => fields.iter().try_fold(0_u64, |total, field| {
            total
                .checked_add(nodes(field.data_type())?)
                .ok_or_else(|| invalid("schema node overflow"))
        })?,
        _ => 0,
    };
    children
        .checked_add(1)
        .ok_or_else(|| invalid("schema node overflow"))
}

pub(super) fn convert_batch(
    py: Python<'_>,
    rows: &Bound<'_, PySequence>,
    range: std::ops::Range<usize>,
    schema: &SchemaRef,
    source: &MemoryBatchSourceBuilder,
    cancellation: &CancellationToken,
) -> PyResult<Budgeted<RecordBatch>> {
    let node_count = schema.fields().iter().try_fold(0_u64, |total, field| {
        total
            .checked_add(nodes(field.data_type())?)
            .ok_or_else(|| invalid("schema node overflow"))
    })?;
    let metadata = node_count
        .checked_mul(1024)
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(|| invalid("object construction metadata overflow"))?;
    let mut conversion = Conversion {
        py,
        credit: source.reserve_scratch(metadata).map_err(native_error)?,
        source,
        decimal: PyModule::import(py, "decimal")?.getattr("Decimal")?,
        datetime: PyModule::import(py, "datetime")?,
        cancellation,
    };
    let mut columns = schema
        .fields()
        .iter()
        .map(|field| builder(field.data_type()))
        .collect::<PyResult<Vec<_>>>()?;
    for row_index in range {
        if row_index % 256 == 0 {
            cancellation.check().map_err(native_error)?;
            py.check_signals()?;
        }
        let row = mapping(&rows.get_item(row_index)?, schema.fields().len())?;
        for (field, column) in schema.fields().iter().zip(&mut columns) {
            conversion.append(
                column.as_mut(),
                field,
                &field_value(&row, field.name())?,
                false,
            )?;
        }
    }
    let arrays = columns.iter_mut().map(ArrayBuilder::finish).collect();
    let batch = RecordBatch::try_new(Arc::clone(schema), arrays)
        .map_err(|error| invalid(&error.to_string()))?;
    Ok(Budgeted::new(batch, conversion.credit))
}

fn builder(dtype: &DataType) -> PyResult<Box<dyn ArrayBuilder>> {
    macro_rules! simple {
        ($ty:ty) => {
            Box::new(<$ty>::with_capacity(0))
        };
    }
    Ok(match dtype {
        DataType::Boolean => simple!(BooleanBuilder),
        DataType::Int8 => simple!(Int8Builder),
        DataType::Int16 => simple!(Int16Builder),
        DataType::Int32 => simple!(Int32Builder),
        DataType::Int64 => simple!(Int64Builder),
        DataType::UInt8 => simple!(UInt8Builder),
        DataType::UInt16 => simple!(UInt16Builder),
        DataType::UInt32 => simple!(UInt32Builder),
        DataType::UInt64 => simple!(UInt64Builder),
        DataType::Float32 => simple!(Float32Builder),
        DataType::Float64 => simple!(Float64Builder),
        DataType::Date32 => simple!(Date32Builder),
        DataType::Timestamp(TimeUnit::Microsecond, None) => simple!(TimestampMicrosecondBuilder),
        DataType::Decimal128(precision, scale) => Box::new(
            Decimal128Builder::with_capacity(0)
                .with_precision_and_scale(*precision, *scale)
                .map_err(|e| invalid(&e.to_string()))?,
        ),
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(0, 0)),
        DataType::Binary => Box::new(BinaryBuilder::with_capacity(0, 0)),
        DataType::List(child) => Box::new(
            ListBuilder::with_capacity(builder(child.data_type())?, 0)
                .with_field(Arc::clone(child)),
        ),
        DataType::FixedSizeList(child, size) => Box::new(
            FixedSizeListBuilder::with_capacity(builder(child.data_type())?, *size, 0)
                .with_field(Arc::clone(child)),
        ),
        DataType::Struct(fields) => Box::new(StructBuilder::new(
            fields.clone(),
            fields
                .iter()
                .map(|field| builder(field.data_type()))
                .collect::<PyResult<Vec<_>>>()?,
        )),
        _ => return Err(invalid("unsupported native object construction type")),
    })
}

struct Conversion<'a, 'py> {
    py: Python<'py>,
    credit: MemoryLease,
    source: &'a MemoryBatchSourceBuilder,
    decimal: Bound<'py, PyAny>,
    datetime: Bound<'py, PyModule>,
    cancellation: &'a CancellationToken,
}

impl Conversion<'_, '_> {
    // Explicit closed type dispatch keeps object conversion separate from query
    // semantics. All capacities are admitted before Arrow builders can grow.
    #[allow(clippy::too_many_lines)]
    fn append(
        &mut self,
        output: &mut dyn ArrayBuilder,
        field: &Field,
        value: &Bound<'_, PyAny>,
        masked: bool,
    ) -> PyResult<()> {
        if value.is_none() && !masked && !field.is_nullable() {
            return Err(invalid("null in a nonnullable native input"));
        }
        grow_credit(&mut self.credit, 24)?;
        // Vortex's Arrow importer converts children independently. Masked slots
        // under a null parent therefore retain valid physical placeholders for
        // nonnullable children; their parent still makes the values invisible.
        let placeholder = masked && value.is_none() && !field.is_nullable();
        let missing = value.is_none() && !placeholder;
        macro_rules! integer {
            ($ty:ty, $native:ty) => {{
                let column = downcast::<$ty>(output)?;
                if missing {
                    column.append_null();
                } else if placeholder {
                    column.append_value(0);
                } else {
                    require_integer(value)?;
                    let value = value
                        .extract::<$native>()
                        .map_err(|_| invalid("native integer exceeds its declared width"))?;
                    column.append_value(value);
                }
            }};
        }
        match field.data_type() {
            DataType::Boolean => {
                let column = downcast::<BooleanBuilder>(output)?;
                if missing {
                    column.append_null();
                } else if placeholder {
                    column.append_value(false);
                } else if value.is_instance_of::<PyBool>() {
                    column.append_value(value.extract()?);
                } else {
                    return Err(PyTypeError::new_err(
                        "native bool input requires bool values",
                    ));
                }
            }
            DataType::Int8 => integer!(Int8Builder, i8),
            DataType::Int16 => integer!(Int16Builder, i16),
            DataType::Int32 => integer!(Int32Builder, i32),
            DataType::Int64 => integer!(Int64Builder, i64),
            DataType::UInt8 => integer!(UInt8Builder, u8),
            DataType::UInt16 => integer!(UInt16Builder, u16),
            DataType::UInt32 => integer!(UInt32Builder, u32),
            DataType::UInt64 => integer!(UInt64Builder, u64),
            DataType::Float32 => {
                let column = downcast::<Float32Builder>(output)?;
                if missing {
                    column.append_null();
                } else if placeholder {
                    column.append_value(0.0);
                } else {
                    column.append_value(float32(value)?);
                }
            }
            DataType::Float64 => {
                let column = downcast::<Float64Builder>(output)?;
                if missing {
                    column.append_null();
                } else if placeholder {
                    column.append_value(0.0);
                } else {
                    column.append_value(float64(value)?);
                }
            }
            DataType::Utf8 => {
                let column = downcast::<StringBuilder>(output)?;
                if missing {
                    column.append_null();
                } else if placeholder {
                    column.append_value("");
                } else {
                    let text = value.cast::<PyString>()?.to_str()?;
                    check_offset(column.values_slice().len(), text.len())?;
                    grow_credit(&mut self.credit, text.len())?;
                    column.append_value(text);
                }
            }
            DataType::Binary => {
                let column = downcast::<BinaryBuilder>(output)?;
                if missing {
                    column.append_null();
                } else if placeholder {
                    column.append_value([]);
                } else {
                    let bytes = self.binary(value)?;
                    check_offset(column.values_slice().len(), bytes.as_bytes().len())?;
                    column.append_value(bytes.as_bytes());
                }
            }
            DataType::Decimal128(precision, scale) => {
                let column = downcast::<Decimal128Builder>(output)?;
                if missing {
                    column.append_null();
                } else if placeholder {
                    column.append_value(0);
                } else {
                    column.append_value(self.decimal(value, *precision, *scale)?);
                }
            }
            DataType::Date32 => {
                let column = downcast::<Date32Builder>(output)?;
                if missing {
                    column.append_null();
                } else if placeholder {
                    column.append_value(0);
                } else {
                    column.append_value(
                        i32::try_from(self.temporal(value, true)?).map_err(|_| {
                            invalid("native date32 exceeds its exact storage width")
                        })?,
                    );
                }
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                let column = downcast::<TimestampMicrosecondBuilder>(output)?;
                if missing {
                    column.append_null();
                } else if placeholder {
                    column.append_value(0);
                } else {
                    column.append_value(self.temporal(value, false)?);
                }
            }
            DataType::List(child) => {
                let column = downcast::<ListBuilder<Box<dyn ArrayBuilder>>>(output)?;
                if !missing && !placeholder {
                    let values = sequence(value)?;
                    check_offset(column.values().len(), values.len()?)?;
                    self.append_sequence(column.values().as_mut(), child, &values)?;
                }
                column.append(!missing);
            }
            DataType::FixedSizeList(child, size) => {
                let column = downcast::<FixedSizeListBuilder<Box<dyn ArrayBuilder>>>(output)?;
                if missing || placeholder {
                    for index in 0..*size {
                        if index % 256 == 0 {
                            self.check()?;
                        }
                        self.append(column.values().as_mut(), child, value, true)?;
                    }
                } else {
                    let values = sequence(value)?;
                    if i32::try_from(values.len()?).ok() != Some(*size) {
                        return Err(invalid("native fixed-size list has the wrong width"));
                    }
                    self.append_sequence(column.values().as_mut(), child, &values)?;
                }
                column.append(!missing);
            }
            DataType::Struct(fields) => {
                let column = downcast::<StructBuilder>(output)?;
                let row = if missing || placeholder {
                    None
                } else {
                    Some(mapping(value, fields.len())?)
                };
                for (child, builder) in fields.iter().zip(column.field_builders_mut()) {
                    if let Some(row) = &row {
                        self.append(
                            builder.as_mut(),
                            child,
                            &field_value(row, child.name())?,
                            false,
                        )?;
                    } else {
                        self.append(builder.as_mut(), child, value, true)?;
                    }
                }
                column.append(!missing);
            }
            _ => return Err(invalid("unsupported native object construction type")),
        }
        Ok(())
    }

    fn append_sequence(
        &mut self,
        output: &mut dyn ArrayBuilder,
        field: &Field,
        values: &Bound<'_, PySequence>,
    ) -> PyResult<()> {
        for index in 0..values.len()? {
            if index % 256 == 0 {
                self.check()?;
            }
            self.append(output, field, &values.get_item(index)?, false)?;
        }
        Ok(())
    }

    fn check(&self) -> PyResult<()> {
        self.cancellation.check().map_err(native_error)?;
        self.py.check_signals()
    }

    fn binary<'py>(&mut self, value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
        if let Ok(bytes) = value.cast::<PyBytes>() {
            grow_credit(&mut self.credit, bytes.as_bytes().len())?;
            return Ok(bytes.clone());
        }
        let bytes = if value.is_instance_of::<PyMemoryView>() {
            let length = value.getattr("nbytes")?.extract::<usize>()?;
            grow_credit(&mut self.credit, length)?;
            value.call_method0("tobytes")?
        } else if value.is_instance_of::<PyByteArray>() {
            grow_credit(&mut self.credit, value.len()?)?;
            value.py().get_type::<PyBytes>().call1((value,))?
        } else {
            return Err(PyTypeError::new_err(
                "native binary input requires bytes-like values",
            ));
        };
        bytes.cast_into::<PyBytes>().map_err(Into::into)
    }

    fn decimal(&self, value: &Bound<'_, PyAny>, precision: u8, scale: i8) -> PyResult<i128> {
        if !value.is_instance(&self.decimal)?
            || !self
                .decimal
                .getattr("is_finite")?
                .call1((value,))?
                .is_truthy()?
        {
            return Err(PyTypeError::new_err(
                "native decimal input requires finite Decimal values",
            ));
        }
        // Decimal's own storage size bounds the temporary digit tuple. Call its
        // built-in method, not an overridable instance method, and admit before
        // asking Python to construct that tuple. Decimal arithmetic context is
        // never consulted and no per-cell string/JSON encoding is constructed.
        let size = self
            .decimal
            .getattr("__sizeof__")?
            .call1((value,))?
            .extract::<u64>()?;
        let bytes = size
            .checked_mul(64)
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(|| invalid("decimal conversion workspace overflow"))?;
        let _temporary = self.source.reserve_scratch(bytes).map_err(native_error)?;
        let parts = self.decimal.getattr("as_tuple")?.call1((value,))?;
        let negative = parts.getattr("sign")?.extract::<u8>()? != 0;
        let digits = parts.getattr("digits")?;
        let digits = digits.cast::<PyTuple>()?;
        let mut any = false;
        for (index, digit) in digits.iter().enumerate() {
            if index % 256 == 0 {
                self.check()?;
            }
            any |= digit.extract::<u8>()? != 0;
        }
        if !any {
            return Ok(0);
        }
        let exponent = parts.getattr("exponent")?.extract::<i64>()?;
        let shift = exponent
            .checked_add(i64::from(scale))
            .ok_or_else(|| invalid("decimal scale overflow"))?;
        let (length, zeros) = if shift < 0 {
            let cut = usize::try_from(shift.unsigned_abs())
                .map_err(|_| invalid("decimal cannot be represented at the declared scale"))?;
            if cut >= digits.len() {
                return Err(invalid(
                    "decimal cannot be represented at the declared scale",
                ));
            }
            let length = digits.len() - cut;
            for index in length..digits.len() {
                if digits.get_item(index)?.extract::<u8>()? != 0 {
                    return Err(invalid(
                        "decimal cannot be represented at the declared scale",
                    ));
                }
            }
            (length, 0)
        } else {
            (
                digits.len(),
                usize::try_from(shift).map_err(|_| invalid("decimal precision overflow"))?,
            )
        };
        if length
            .checked_add(zeros)
            .is_none_or(|size| size > usize::from(precision))
        {
            return Err(invalid("decimal exceeds its declared precision"));
        }
        let mut coefficient = 0_i128;
        for index in 0..length {
            coefficient = coefficient * 10 + i128::from(digits.get_item(index)?.extract::<u8>()?);
        }
        for _ in 0..zeros {
            coefficient *= 10;
        }
        Ok(if negative { -coefficient } else { coefficient })
    }

    fn temporal(&self, value: &Bound<'_, PyAny>, date: bool) -> PyResult<i64> {
        let datetime = self.datetime.getattr("datetime")?;
        let date_type = self.datetime.getattr("date")?;
        let is_datetime = value.is_instance(&datetime)?;
        if date && is_datetime {
            return Err(PyTypeError::new_err(
                "native date32 requires a date, not a datetime",
            ));
        }
        let converted = if date && value.is_instance(&date_type)? {
            let epoch = date_type.call1((1970, 1, 1))?;
            value
                .call_method1("__sub__", (epoch,))?
                .getattr("days")?
                .extract::<i64>()?
        } else if !date && is_datetime {
            if !value.getattr("tzinfo")?.is_none() {
                return Err(invalid("native timestamp_micros requires a naive datetime"));
            }
            let epoch = datetime.call1((1970, 1, 1))?;
            let delta = value.call_method1("__sub__", (epoch,))?;
            let days = delta.getattr("days")?.extract::<i64>()?;
            let seconds = delta.getattr("seconds")?.extract::<i64>()?;
            let micros = delta.getattr("microseconds")?.extract::<i64>()?;
            days.checked_mul(86400)
                .and_then(|n| n.checked_add(seconds))
                .and_then(|n| n.checked_mul(1_000_000))
                .and_then(|n| n.checked_add(micros))
                .ok_or_else(|| invalid("native timestamp exceeds its exact storage width"))?
        } else {
            require_integer(value)?;
            value
                .extract::<i64>()
                .map_err(|_| invalid("native temporal input exceeds its exact storage width"))?
        };
        Ok(converted)
    }
}

fn require_integer(value: &Bound<'_, PyAny>) -> PyResult<()> {
    if value.is_instance_of::<PyBool>() || !value.is_instance_of::<PyInt>() {
        return Err(PyTypeError::new_err(
            "native integer input requires int values",
        ));
    }
    Ok(())
}

fn float64(value: &Bound<'_, PyAny>) -> PyResult<f64> {
    if value.is_instance_of::<PyBool>()
        || !(value.is_instance_of::<PyInt>() || value.is_instance_of::<PyFloat>())
    {
        return Err(PyTypeError::new_err(
            "native floating input requires numeric values",
        ));
    }
    let number = value
        .extract::<f64>()
        .map_err(|_| invalid("native floating input exceeds its declared width"))?;
    if !number.is_finite()
        || (value.is_instance_of::<PyInt>()
            && !number.into_py_any(value.py())?.bind(value.py()).eq(value)?)
    {
        return Err(invalid(
            "native floating input must be finite and exactly representable",
        ));
    }
    Ok(number)
}

#[allow(clippy::cast_possible_truncation)] // Exact round-trip below is mandatory.
fn float32(value: &Bound<'_, PyAny>) -> PyResult<f32> {
    let wide = float64(value)?;
    let narrow = wide as f32;
    if f64::from(narrow).to_bits() != wide.to_bits() {
        return Err(invalid("native float32 must be exactly representable"));
    }
    Ok(narrow)
}

fn grow_credit(credit: &mut MemoryLease, logical: usize) -> PyResult<()> {
    // Four times appended bytes covers geometric Vec/bitmap growth and old/new
    // reallocation overlap. Per-node base credit covers small allocator minima,
    // builders and metadata; native intake separately owns its copied buffers.
    let bytes = u64::try_from(logical)
        .ok()
        .and_then(|n| n.checked_mul(4))
        .and_then(|n| credit.bytes().checked_add(n))
        .ok_or_else(|| invalid("object construction capacity overflow"))?;
    credit.resize(bytes).map_err(native_error)
}

fn check_offset(current: usize, addition: usize) -> PyResult<()> {
    if current
        .checked_add(addition)
        .is_none_or(|n| i32::try_from(n).is_err())
    {
        return Err(invalid(
            "single object construction batch exceeds its Arrow offset representation",
        ));
    }
    Ok(())
}

fn downcast<T: 'static>(builder: &mut dyn ArrayBuilder) -> PyResult<&mut T> {
    builder
        .as_any_mut()
        .downcast_mut::<T>()
        .ok_or_else(|| invalid("object builder disagrees with its declared type"))
}

fn invalid(message: &str) -> PyErr {
    PyValueError::new_err(message.to_owned())
}
