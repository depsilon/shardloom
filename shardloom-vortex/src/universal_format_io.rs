//! Feature-gated local compatibility-format I/O for runtime promotion.
//!
//! These helpers are compatibility input adapters, not fallback execution
//! engines. They decode admitted local file formats into `ShardLoom` scalar rows
//! and encode admitted local sink formats from `ShardLoom` scalar rows so
//! caller-owned runtime paths can emit explicit materialization/write evidence
//! and fail closed for unsupported Arrow types.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufReader, Write},
    path::Path,
    rc::Rc,
};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, RecordBatch, RecordBatchReader, StringArray,
    StringViewArray, TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    builder::{
        BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
        TimestampMicrosecondBuilder, UInt64Builder,
    },
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use shardloom_core::{Result, ScalarValue, ShardLoomError};
use std::sync::Arc;

/// Materialized scalar rows produced by a scoped local compatibility adapter.
#[derive(Debug, Clone, PartialEq)]
pub struct FlatLocalSourceTable {
    /// Column order from the source schema.
    pub header: Vec<String>,
    /// Source rows converted to `ShardLoom` scalar values.
    pub rows: Vec<BTreeMap<String, ScalarValue>>,
}

/// Read a local Parquet file into flat scalar rows for scoped runtime smokes.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the Parquet reader cannot be constructed, a column has an unsupported nested
/// or decimal Arrow type, or the row count exceeds `max_rows`.
pub fn read_flat_parquet_source(path: &Path, max_rows: usize) -> Result<FlatLocalSourceTable> {
    let file = File::open(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to open local Parquet source '{}': {error}",
            path.display()
        ))
    })?;
    let mut reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local Parquet source reader for '{}': {error}",
                path.display()
            ))
        })?
        .with_batch_size(max_rows.clamp(1, 8192))
        .build()
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to build local Parquet source reader for '{}': {error}",
                path.display()
            ))
        })?;

    read_flat_record_batch_reader(&mut reader, path, "Parquet", max_rows)
}

/// Read a local Arrow IPC file into flat scalar rows for scoped runtime smokes.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the Arrow IPC reader cannot be constructed, a column has an unsupported
/// nested, decimal, dictionary, or union Arrow type, or the row count exceeds
/// `max_rows`.
pub fn read_flat_arrow_ipc_source(path: &Path, max_rows: usize) -> Result<FlatLocalSourceTable> {
    let file = File::open(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to open local Arrow IPC source '{}': {error}",
            path.display()
        ))
    })?;
    let mut reader = arrow_ipc::reader::FileReader::try_new(file, None).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to create local Arrow IPC source reader for '{}': {error}",
            path.display()
        ))
    })?;

    read_flat_record_batch_reader(&mut reader, path, "Arrow IPC", max_rows)
}

/// Read a local Avro file into flat scalar rows for scoped runtime smokes.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the Avro reader cannot be constructed, a column has an unsupported nested,
/// logical, decimal, dictionary, or union Arrow type, or the row count exceeds
/// `max_rows`.
pub fn read_flat_avro_source(path: &Path, max_rows: usize) -> Result<FlatLocalSourceTable> {
    let file = File::open(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to open local Avro source '{}': {error}",
            path.display()
        ))
    })?;
    let mut reader = arrow_avro::reader::ReaderBuilder::new()
        .with_batch_size(max_rows.clamp(1, 8192))
        .build(BufReader::new(file))
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local Avro source reader for '{}': {error}",
                path.display()
            ))
        })?;

    read_flat_record_batch_reader(&mut reader, path, "Avro", max_rows)
}

/// Read a local ORC file into flat scalar rows for scoped runtime smokes.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the ORC reader cannot be constructed, a column has an unsupported nested,
/// decimal, dictionary, or union Arrow type, or the row count exceeds
/// `max_rows`.
pub fn read_flat_orc_source(path: &Path, max_rows: usize) -> Result<FlatLocalSourceTable> {
    let file = File::open(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to open local ORC source '{}': {error}",
            path.display()
        ))
    })?;
    let mut reader = orc_rust::ArrowReaderBuilder::try_new(file)
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local ORC source reader for '{}': {error}",
                path.display()
            ))
        })?
        .with_batch_size(max_rows.clamp(1, 8192))
        .build();

    read_flat_record_batch_reader(&mut reader, path, "ORC", max_rows)
}

fn read_flat_record_batch_reader<R>(
    reader: &mut R,
    path: &Path,
    source_label: &str,
    max_rows: usize,
) -> Result<FlatLocalSourceTable>
where
    R: RecordBatchReader,
{
    let schema = reader.schema();
    let header = source_schema_header(path, source_label, schema.as_ref())?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to read local {source_label} source batch from '{}': {error}",
                path.display()
            ))
        })?;
        if batch.num_columns() != header.len() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local {source_label} source '{}' changed column count between schema and batch",
                path.display()
            )));
        }
        for row_index in 0..batch.num_rows() {
            if rows.len() >= max_rows {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "local {source_label} source '{}' exceeds the scoped SQL local-source row limit of {max_rows}",
                    path.display()
                )));
            }
            let mut row = BTreeMap::new();
            for (column, array) in header.iter().zip(batch.columns()) {
                row.insert(
                    column.clone(),
                    arrow_scalar_to_shardloom(
                        array.as_ref(),
                        row_index,
                        column,
                        path,
                        source_label,
                    )?,
                );
            }
            rows.push(row);
        }
    }

    Ok(FlatLocalSourceTable { header, rows })
}

fn source_schema_header(path: &Path, source_label: &str, schema: &Schema) -> Result<Vec<String>> {
    let header = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    if header.is_empty() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local {source_label} source '{}' must contain at least one column",
            path.display()
        )));
    }
    let mut seen_columns = BTreeSet::new();
    for column in &header {
        if column.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local {source_label} source '{}' contains an empty column name",
                path.display()
            )));
        }
        if !seen_columns.insert(column) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local {source_label} source '{}' contains duplicate column '{column}'",
                path.display()
            )));
        }
    }
    Ok(header)
}

/// Encode flat scalar rows into local Parquet bytes for scoped runtime smokes.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when column names are invalid,
/// row shapes do not match the declared columns, a column contains mixed scalar
/// families, or a value cannot be represented by the scoped Parquet sink.
pub fn encode_flat_parquet_rows(
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>> {
    let batch = flat_rows_to_record_batch(columns, rows, "local Parquet output")?;

    let mut writer = parquet::arrow::ArrowWriter::try_new(Vec::new(), batch.schema(), None)
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local Parquet output writer: {error}"
            ))
        })?;
    writer.write(&batch).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to write local Parquet output batch: {error}"
        ))
    })?;
    writer.into_inner().map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to close local Parquet output writer: {error}"
        ))
    })
}

/// Encode flat scalar rows into local Arrow IPC bytes for scoped runtime smokes.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when column names are invalid,
/// row shapes do not match the declared columns, a column contains mixed scalar
/// families, or a value cannot be represented by the scoped Arrow IPC sink.
pub fn encode_flat_arrow_ipc_rows(
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>> {
    let batch = flat_rows_to_record_batch(columns, rows, "local Arrow IPC output")?;
    let mut writer = arrow_ipc::writer::FileWriter::try_new(Vec::new(), batch.schema().as_ref())
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local Arrow IPC output writer: {error}"
            ))
        })?;
    writer.write(&batch).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to write local Arrow IPC output batch: {error}"
        ))
    })?;
    writer.into_inner().map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to close local Arrow IPC output writer: {error}"
        ))
    })
}

/// Encode flat scalar rows into local Avro bytes for scoped runtime smokes.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when column names are invalid,
/// row shapes do not match the declared columns, a column contains mixed scalar
/// families, or a value cannot be represented by the scoped Avro sink.
pub fn encode_flat_avro_rows(
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>> {
    let batch = flat_rows_to_record_batch(columns, rows, "local Avro output")?;
    let mut writer =
        arrow_avro::writer::AvroWriter::new(Vec::new(), batch.schema().as_ref().clone()).map_err(
            |error| {
                ShardLoomError::InvalidOperation(format!(
                    "failed to create local Avro output writer: {error}"
                ))
            },
        )?;
    writer.write(&batch).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to write local Avro output batch: {error}"
        ))
    })?;
    writer.finish().map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to finish local Avro output writer: {error}"
        ))
    })?;
    Ok(writer.into_inner())
}

/// Encode flat scalar rows into local ORC bytes for scoped runtime smokes.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when column names are invalid,
/// row shapes do not match the declared columns, a column contains mixed scalar
/// families, or a value cannot be represented by the scoped ORC sink.
pub fn encode_flat_orc_rows(
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>> {
    let batch = flat_rows_to_record_batch(columns, rows, "local ORC output")?;
    let buffer = SharedBufferWriter::default();
    let retained_buffer = buffer.clone();
    let mut writer = orc_rust::ArrowWriterBuilder::new(buffer, batch.schema())
        .try_build()
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local ORC output writer: {error}"
            ))
        })?;
    writer.write(&batch).map_err(|error| {
        ShardLoomError::InvalidOperation(format!("failed to write local ORC output batch: {error}"))
    })?;
    writer.close().map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to close local ORC output writer: {error}"
        ))
    })?;
    Ok(retained_buffer.into_bytes())
}

fn flat_rows_to_record_batch(
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
    context: &str,
) -> Result<RecordBatch> {
    validate_flat_columns(columns, context)?;
    let fields_and_arrays = columns
        .iter()
        .enumerate()
        .map(|(column_index, column)| flat_output_column_array(column, column_index, rows, context))
        .collect::<Result<Vec<_>>>()?;
    let fields = fields_and_arrays
        .iter()
        .map(|(field, _array)| field.clone())
        .collect::<Vec<_>>();
    let arrays = fields_and_arrays
        .into_iter()
        .map(|(_field, array)| array)
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(Arc::clone(&schema), arrays).map_err(|error| {
        ShardLoomError::InvalidOperation(format!("failed to build {context} record batch: {error}"))
    })
}

#[derive(Clone, Default)]
struct SharedBufferWriter {
    buffer: Rc<RefCell<Vec<u8>>>,
}

impl SharedBufferWriter {
    fn into_bytes(self) -> Vec<u8> {
        match Rc::try_unwrap(self.buffer) {
            Ok(buffer) => buffer.into_inner(),
            Err(buffer) => buffer.borrow().clone(),
        }
    }
}

impl Write for SharedBufferWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.borrow_mut().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn validate_flat_columns(columns: &[String], context: &str) -> Result<()> {
    if columns.is_empty() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{context} must contain at least one column"
        )));
    }
    let mut seen_columns = BTreeSet::new();
    for column in columns {
        if column.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "{context} contains an empty column name"
            )));
        }
        if !seen_columns.insert(column) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "{context} contains duplicate column '{column}'"
            )));
        }
    }
    Ok(())
}

fn flat_output_column_array(
    column: &str,
    column_index: usize,
    rows: &[Vec<(String, ScalarValue)>],
    context: &str,
) -> Result<(Field, ArrayRef)> {
    let values = rows
        .iter()
        .map(|row| {
            let Some((name, value)) = row.get(column_index) else {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "{context} row is missing column '{column}' at index {column_index}"
                )));
            };
            if name != column {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "{context} row column mismatch at index {column_index}: expected '{column}', found '{name}'"
                )));
            }
            Ok(value)
        })
        .collect::<Result<Vec<_>>>()?;
    let kind = values
        .iter()
        .filter(|value| !matches!(value, ScalarValue::Null))
        .map(|value| scalar_family(value))
        .try_fold(None, |current, candidate| match current {
            None => Ok(Some(candidate)),
            Some(existing) if existing == candidate => Ok(Some(existing)),
            Some(existing) => Err(ShardLoomError::InvalidOperation(format!(
                "{context} column '{column}' mixes scalar families {existing} and {candidate}; scoped compatibility output requires one non-null scalar family per column"
            ))),
        })?
        .unwrap_or("utf8");
    let nullable = values
        .iter()
        .any(|value| matches!(value, ScalarValue::Null));
    match kind {
        "boolean" => Ok(parquet_bool_column(column, &values, nullable, context)?),
        "int64" => Ok(parquet_int64_column(column, &values, nullable, context)?),
        "uint64" => Ok(parquet_uint64_column(column, &values, nullable, context)?),
        "float64" => Ok(parquet_float64_column(column, &values, nullable, context)?),
        "utf8" => Ok(parquet_utf8_column(column, &values, nullable, context)?),
        "date32" => Ok(parquet_date32_column(column, &values, nullable, context)?),
        "timestamp_micros" => Ok(parquet_timestamp_micros_column(
            column, &values, nullable, context,
        )?),
        other => Err(ShardLoomError::InvalidOperation(format!(
            "{context} column '{column}' has unsupported scalar family {other}"
        ))),
    }
}

fn parquet_bool_column(
    column: &str,
    values: &[&ScalarValue],
    nullable: bool,
    context: &str,
) -> Result<(Field, ArrayRef)> {
    let mut builder = BooleanBuilder::with_capacity(values.len());
    for value in values {
        match value {
            ScalarValue::Boolean(value) => builder.append_value(*value),
            ScalarValue::Null => builder.append_null(),
            other => return Err(unexpected_sink_value(context, column, "boolean", other)),
        }
    }
    Ok((
        Field::new(column, DataType::Boolean, nullable),
        Arc::new(builder.finish()),
    ))
}

fn parquet_int64_column(
    column: &str,
    values: &[&ScalarValue],
    nullable: bool,
    context: &str,
) -> Result<(Field, ArrayRef)> {
    let mut builder = Int64Builder::with_capacity(values.len());
    for value in values {
        match value {
            ScalarValue::Int64(value) => builder.append_value(*value),
            ScalarValue::Null => builder.append_null(),
            other => return Err(unexpected_sink_value(context, column, "int64", other)),
        }
    }
    Ok((
        Field::new(column, DataType::Int64, nullable),
        Arc::new(builder.finish()),
    ))
}

fn parquet_uint64_column(
    column: &str,
    values: &[&ScalarValue],
    nullable: bool,
    context: &str,
) -> Result<(Field, ArrayRef)> {
    let mut builder = UInt64Builder::with_capacity(values.len());
    for value in values {
        match value {
            ScalarValue::UInt64(value) => builder.append_value(*value),
            ScalarValue::Null => builder.append_null(),
            other => return Err(unexpected_sink_value(context, column, "uint64", other)),
        }
    }
    Ok((
        Field::new(column, DataType::UInt64, nullable),
        Arc::new(builder.finish()),
    ))
}

fn parquet_float64_column(
    column: &str,
    values: &[&ScalarValue],
    nullable: bool,
    context: &str,
) -> Result<(Field, ArrayRef)> {
    let mut builder = Float64Builder::with_capacity(values.len());
    for value in values {
        match value {
            ScalarValue::Float64(value) => builder.append_value(*value),
            ScalarValue::Null => builder.append_null(),
            other => return Err(unexpected_sink_value(context, column, "float64", other)),
        }
    }
    Ok((
        Field::new(column, DataType::Float64, nullable),
        Arc::new(builder.finish()),
    ))
}

fn parquet_utf8_column(
    column: &str,
    values: &[&ScalarValue],
    nullable: bool,
    context: &str,
) -> Result<(Field, ArrayRef)> {
    let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 8);
    for value in values {
        match value {
            ScalarValue::Utf8(value) => builder.append_value(value),
            ScalarValue::Null => builder.append_null(),
            other => return Err(unexpected_sink_value(context, column, "utf8", other)),
        }
    }
    Ok((
        Field::new(column, DataType::Utf8, nullable),
        Arc::new(builder.finish()),
    ))
}

fn parquet_date32_column(
    column: &str,
    values: &[&ScalarValue],
    nullable: bool,
    context: &str,
) -> Result<(Field, ArrayRef)> {
    let mut builder = Date32Builder::with_capacity(values.len());
    for value in values {
        match value {
            ScalarValue::Date32(value) => builder.append_value(*value),
            ScalarValue::Null => builder.append_null(),
            other => return Err(unexpected_sink_value(context, column, "date32", other)),
        }
    }
    Ok((
        Field::new(column, DataType::Date32, nullable),
        Arc::new(builder.finish()),
    ))
}

fn parquet_timestamp_micros_column(
    column: &str,
    values: &[&ScalarValue],
    nullable: bool,
    context: &str,
) -> Result<(Field, ArrayRef)> {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(values.len());
    for value in values {
        match value {
            ScalarValue::TimestampMicros(value) => builder.append_value(*value),
            ScalarValue::Null => builder.append_null(),
            other => {
                return Err(unexpected_sink_value(
                    context,
                    column,
                    "timestamp_micros",
                    other,
                ));
            }
        }
    }
    Ok((
        Field::new(
            column,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            nullable,
        ),
        Arc::new(builder.finish()),
    ))
}

fn scalar_family(value: &ScalarValue) -> &'static str {
    match value {
        ScalarValue::Boolean(_) => "boolean",
        ScalarValue::Int64(_) => "int64",
        ScalarValue::UInt64(_) => "uint64",
        ScalarValue::Float64(_) => "float64",
        ScalarValue::Utf8(_) => "utf8",
        ScalarValue::Date32(_) => "date32",
        ScalarValue::TimestampMicros(_) => "timestamp_micros",
        ScalarValue::Null => "null",
        ScalarValue::Binary(_) => "binary",
    }
}

fn unexpected_sink_value(
    context: &str,
    column: &str,
    expected: &str,
    value: &ScalarValue,
) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "{context} column '{column}' expected {expected} but found {}",
        scalar_family(value)
    ))
}

#[allow(clippy::cast_precision_loss)]
fn arrow_scalar_to_shardloom(
    array: &dyn Array,
    row_index: usize,
    column: &str,
    path: &Path,
    source_label: &str,
) -> Result<ScalarValue> {
    if array.is_null(row_index) {
        return Ok(ScalarValue::Null);
    }
    if let Some(values) = array.as_any().downcast_ref::<BooleanArray>() {
        return Ok(ScalarValue::Boolean(values.value(row_index)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
        return Ok(ScalarValue::Int64(i64::from(values.value(row_index))));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int16Array>() {
        return Ok(ScalarValue::Int64(i64::from(values.value(row_index))));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(ScalarValue::Int64(i64::from(values.value(row_index))));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(ScalarValue::Int64(values.value(row_index)));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt8Array>() {
        return Ok(ScalarValue::UInt64(u64::from(values.value(row_index))));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt16Array>() {
        return Ok(ScalarValue::UInt64(u64::from(values.value(row_index))));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        return Ok(ScalarValue::UInt64(u64::from(values.value(row_index))));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(ScalarValue::UInt64(values.value(row_index)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
        return Ok(ScalarValue::Float64(f64::from(values.value(row_index))));
    }
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        return Ok(ScalarValue::Float64(values.value(row_index)));
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(ScalarValue::Utf8(values.value(row_index).to_string()));
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(ScalarValue::Utf8(values.value(row_index).to_string()));
    }
    if let Some(values) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(ScalarValue::Utf8(values.value(row_index).to_string()));
    }
    if let Some(values) = array.as_any().downcast_ref::<Date32Array>() {
        return Ok(ScalarValue::Date32(values.value(row_index)));
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Ok(ScalarValue::TimestampMicros(values.value(row_index)));
    }
    Err(ShardLoomError::InvalidOperation(format!(
        "local {source_label} source '{}' column '{column}' has unsupported Arrow type {:?}; scoped local-source runtime admits booleans, integers, floats, UTF-8 strings, date32, and timestamp_micros only",
        path.display(),
        array.data_type()
    )))
}
