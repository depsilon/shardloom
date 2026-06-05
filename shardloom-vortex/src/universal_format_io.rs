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
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch, RecordBatchReader, StringArray,
    StringViewArray, TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    builder::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder,
        Int64Builder, StringBuilder, TimestampMicrosecondBuilder, UInt64Builder,
    },
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use shardloom_core::{LogicalDType, Result, ScalarValue, ShardLoomError};
use std::sync::Arc;

/// Materialized scalar rows produced by a scoped local compatibility adapter.
#[derive(Debug, Clone, PartialEq)]
pub struct FlatLocalSourceTable {
    /// Column order from the source schema.
    pub header: Vec<String>,
    /// Source-schema dtype hints aligned with `header` for scoped compatibility
    /// output preservation.
    pub column_dtypes: Vec<Option<LogicalDType>>,
    /// Column order requested from the underlying local reader.
    pub reader_projection_columns: Vec<String>,
    /// Source rows converted to `ShardLoom` scalar values. Projection-capable
    /// readers may omit non-materialized columns while preserving the full
    /// source schema in `header`.
    pub rows: Vec<BTreeMap<String, ScalarValue>>,
}

/// Columnar local compatibility source produced by a scoped local adapter.
#[derive(Debug, Clone, PartialEq)]
pub struct FlatLocalColumnarSource {
    /// Column order from the source schema.
    pub header: Vec<String>,
    /// Source-schema dtype hints aligned with `header` for scoped compatibility
    /// output preservation.
    pub column_dtypes: Vec<Option<LogicalDType>>,
    /// Column order materialized for the caller.
    pub materialized_columns: Vec<String>,
    /// Column order requested from the underlying local reader.
    pub reader_projection_columns: Vec<String>,
    /// Arrow record batches preserved before scalar-row materialization.
    pub batches: Vec<RecordBatch>,
    /// Total row count across `batches`.
    pub row_count: usize,
}

/// Read a local Parquet file into flat scalar rows for scoped runtime smokes.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the Parquet reader cannot be constructed, a column has an unsupported nested
/// or decimal Arrow type, or the row count exceeds `max_rows`.
pub fn read_flat_parquet_source(path: &Path, max_rows: usize) -> Result<FlatLocalSourceTable> {
    let source = read_flat_parquet_columnar_source(path, max_rows)?;
    materialize_flat_columnar_source_to_scalar_table(&source, path, "Parquet")
}

/// Read a local Parquet file into columnar Arrow batches for scoped runtime smokes.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the Parquet reader cannot be constructed, or the row count exceeds
/// `max_rows`.
pub fn read_flat_parquet_columnar_source(
    path: &Path,
    max_rows: usize,
) -> Result<FlatLocalColumnarSource> {
    let file = open_local_source_file(path, "Parquet")?;
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

    read_flat_record_batch_reader_columnar(&mut reader, path, "Parquet", max_rows)
}

/// Read selected root columns from a local Parquet file into flat scalar rows.
///
/// The returned [`FlatLocalSourceTable::header`] remains the full source schema
/// so callers can keep binding and `SourceState` evidence tied to the original
/// input. Row maps contain only columns requested in `required_columns`.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the Parquet reader cannot be constructed, a requested projected column has an
/// unsupported nested or decimal Arrow type, or the row count exceeds `max_rows`.
pub fn read_flat_parquet_source_with_projection(
    path: &Path,
    max_rows: usize,
    required_columns: &[String],
) -> Result<FlatLocalSourceTable> {
    let source =
        read_flat_parquet_columnar_source_with_projection(path, max_rows, required_columns)?;
    materialize_flat_columnar_source_to_scalar_table(&source, path, "Parquet")
}

/// Read selected root columns from a local Parquet file into columnar Arrow
/// batches.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the Parquet reader cannot be constructed, or the row count exceeds
/// `max_rows`.
pub fn read_flat_parquet_columnar_source_with_projection(
    path: &Path,
    max_rows: usize,
    required_columns: &[String],
) -> Result<FlatLocalColumnarSource> {
    let file = open_local_source_file(path, "Parquet")?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local Parquet source reader for '{}': {error}",
                path.display()
            ))
        })?;
    let schema = builder.schema();
    let header = source_schema_header(path, "Parquet", schema.as_ref())?;
    let column_dtypes = source_schema_column_dtypes(schema.as_ref());
    let projection_indices = projection_indices_for_header(&header, required_columns);
    let projected_header = projection_header(&header, &projection_indices);
    let projection = parquet::arrow::ProjectionMask::roots(
        builder.parquet_schema(),
        projection_indices.iter().copied(),
    );
    let mut reader = builder
        .with_batch_size(max_rows.clamp(1, 8192))
        .with_projection(projection)
        .build()
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to build local Parquet source reader for '{}': {error}",
                path.display()
            ))
        })?;

    read_flat_record_batch_reader_columnar_with_header(
        &mut reader,
        path,
        "Parquet",
        max_rows,
        header,
        column_dtypes,
        &projected_header,
    )
}

/// Read a local Arrow IPC file into flat scalar rows for scoped runtime smokes.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the Arrow IPC reader cannot be constructed, a column has an unsupported
/// nested, decimal, dictionary, or union Arrow type, or the row count exceeds
/// `max_rows`.
pub fn read_flat_arrow_ipc_source(path: &Path, max_rows: usize) -> Result<FlatLocalSourceTable> {
    let source = read_flat_arrow_ipc_columnar_source(path, max_rows)?;
    materialize_flat_columnar_source_to_scalar_table(&source, path, "Arrow IPC")
}

/// Read a local Arrow IPC file into columnar Arrow batches for scoped runtime
/// smokes.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the Arrow IPC reader cannot be constructed, or the row count exceeds
/// `max_rows`.
pub fn read_flat_arrow_ipc_columnar_source(
    path: &Path,
    max_rows: usize,
) -> Result<FlatLocalColumnarSource> {
    let file = open_local_source_file(path, "Arrow IPC")?;
    let mut reader = arrow_ipc::reader::FileReader::try_new(file, None).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to create local Arrow IPC source reader for '{}': {error}",
            path.display()
        ))
    })?;

    read_flat_record_batch_reader_columnar(&mut reader, path, "Arrow IPC", max_rows)
}

/// Read selected columns from a local Arrow IPC file into flat scalar rows.
///
/// The returned [`FlatLocalSourceTable::header`] remains the full source schema
/// while row maps contain only columns requested in `required_columns`.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the Arrow IPC reader cannot be constructed, a requested projected column has
/// an unsupported nested, decimal, dictionary, or union Arrow type, or the row
/// count exceeds `max_rows`.
pub fn read_flat_arrow_ipc_source_with_projection(
    path: &Path,
    max_rows: usize,
    required_columns: &[String],
) -> Result<FlatLocalSourceTable> {
    let source =
        read_flat_arrow_ipc_columnar_source_with_projection(path, max_rows, required_columns)?;
    materialize_flat_columnar_source_to_scalar_table(&source, path, "Arrow IPC")
}

/// Read selected columns from a local Arrow IPC file into columnar Arrow
/// batches.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the Arrow IPC reader cannot be constructed, or the row count exceeds
/// `max_rows`.
pub fn read_flat_arrow_ipc_columnar_source_with_projection(
    path: &Path,
    max_rows: usize,
    required_columns: &[String],
) -> Result<FlatLocalColumnarSource> {
    let full_file = open_local_source_file(path, "Arrow IPC")?;
    let full_reader = arrow_ipc::reader::FileReader::try_new(full_file, None).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to create local Arrow IPC source reader for '{}': {error}",
            path.display()
        ))
    })?;
    let schema = full_reader.schema();
    let header = source_schema_header(path, "Arrow IPC", schema.as_ref())?;
    let column_dtypes = source_schema_column_dtypes(schema.as_ref());
    let projection_indices = projection_indices_for_header(&header, required_columns);
    let projected_header = projection_header(&header, &projection_indices);
    drop(full_reader);

    let file = open_local_source_file(path, "Arrow IPC")?;
    let mut reader = arrow_ipc::reader::FileReader::try_new(file, Some(projection_indices))
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create projected local Arrow IPC source reader for '{}': {error}",
                path.display()
            ))
        })?;

    read_flat_record_batch_reader_columnar_with_header(
        &mut reader,
        path,
        "Arrow IPC",
        max_rows,
        header,
        column_dtypes,
        &projected_header,
    )
}

/// Read a local Avro file into flat scalar rows for scoped runtime smokes.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the Avro reader cannot be constructed, a column has an unsupported nested,
/// logical, decimal, dictionary, or union Arrow type, or the row count exceeds
/// `max_rows`.
pub fn read_flat_avro_source(path: &Path, max_rows: usize) -> Result<FlatLocalSourceTable> {
    let source = read_flat_avro_columnar_source(path, max_rows)?;
    materialize_flat_columnar_source_to_scalar_table(&source, path, "Avro")
}

/// Read a local Avro file into columnar Arrow batches for scoped runtime
/// smokes.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the Avro reader cannot be constructed, or the row count exceeds `max_rows`.
pub fn read_flat_avro_columnar_source(
    path: &Path,
    max_rows: usize,
) -> Result<FlatLocalColumnarSource> {
    let file = open_local_source_file(path, "Avro")?;
    let mut reader = arrow_avro::reader::ReaderBuilder::new()
        .with_batch_size(max_rows.clamp(1, 8192))
        .build(BufReader::new(file))
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local Avro source reader for '{}': {error}",
                path.display()
            ))
        })?;

    read_flat_record_batch_reader_columnar(&mut reader, path, "Avro", max_rows)
}

/// Read selected columns from a local Avro file into flat scalar rows.
///
/// The returned [`FlatLocalSourceTable::header`] remains the full source schema
/// while row maps contain only columns requested in `required_columns`.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the Avro reader cannot be constructed, a requested projected column has an
/// unsupported nested, logical, decimal, dictionary, or union Arrow type, or the
/// row count exceeds `max_rows`.
pub fn read_flat_avro_source_with_projection(
    path: &Path,
    max_rows: usize,
    required_columns: &[String],
) -> Result<FlatLocalSourceTable> {
    let source = read_flat_avro_columnar_source_with_projection(path, max_rows, required_columns)?;
    materialize_flat_columnar_source_to_scalar_table(&source, path, "Avro")
}

/// Read selected columns from a local Avro file into columnar Arrow batches.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the Avro reader cannot be constructed, or the row count exceeds `max_rows`.
pub fn read_flat_avro_columnar_source_with_projection(
    path: &Path,
    max_rows: usize,
    required_columns: &[String],
) -> Result<FlatLocalColumnarSource> {
    let full_file = open_local_source_file(path, "Avro")?;
    let full_reader = arrow_avro::reader::ReaderBuilder::new()
        .with_batch_size(max_rows.clamp(1, 8192))
        .build(BufReader::new(full_file))
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local Avro source reader for '{}': {error}",
                path.display()
            ))
        })?;
    let schema = full_reader.schema();
    let header = source_schema_header(path, "Avro", schema.as_ref())?;
    let column_dtypes = source_schema_column_dtypes(schema.as_ref());
    let projection_indices = projection_indices_for_header(&header, required_columns);
    let projected_header = projection_header(&header, &projection_indices);
    let (reader_projection_indices, reader_projection_columns) = if projection_indices.is_empty() {
        (vec![0], projection_header(&header, &[0]))
    } else {
        (projection_indices, projected_header.clone())
    };
    drop(full_reader);

    let file = open_local_source_file(path, "Avro")?;
    let mut reader = arrow_avro::reader::ReaderBuilder::new()
        .with_batch_size(max_rows.clamp(1, 8192))
        .with_projection(reader_projection_indices)
        .build(BufReader::new(file))
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create projected local Avro source reader for '{}': {error}",
                path.display()
            ))
        })?;

    read_flat_record_batch_reader_columnar_with_reader_projection(
        &mut reader,
        path,
        "Avro",
        max_rows,
        FlatColumnarReadSchema {
            header,
            column_dtypes,
            materialized_columns: projected_header,
            reader_projection_columns,
        },
    )
}

/// Read a local ORC file into flat scalar rows for scoped runtime smokes.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the ORC reader cannot be constructed, a column has an unsupported nested,
/// decimal, dictionary, or union Arrow type, or the row count exceeds
/// `max_rows`.
pub fn read_flat_orc_source(path: &Path, max_rows: usize) -> Result<FlatLocalSourceTable> {
    let source = read_flat_orc_columnar_source(path, max_rows)?;
    materialize_flat_columnar_source_to_scalar_table(&source, path, "ORC")
}

/// Read a local ORC file into columnar Arrow batches for scoped runtime smokes.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the ORC reader cannot be constructed, or the row count exceeds `max_rows`.
pub fn read_flat_orc_columnar_source(
    path: &Path,
    max_rows: usize,
) -> Result<FlatLocalColumnarSource> {
    let file = open_local_source_file(path, "ORC")?;
    let mut reader = orc_rust::ArrowReaderBuilder::try_new(file)
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local ORC source reader for '{}': {error}",
                path.display()
            ))
        })?
        .with_batch_size(max_rows.clamp(1, 8192))
        .build();

    read_flat_record_batch_reader_columnar(&mut reader, path, "ORC", max_rows)
}

/// Read selected root columns from a local ORC file into flat scalar rows.
///
/// The returned [`FlatLocalSourceTable::header`] remains the full source schema
/// while row maps contain only columns requested in `required_columns`.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the ORC reader cannot be constructed, a requested projected column has an
/// unsupported nested, decimal, dictionary, or union Arrow type, or the row
/// count exceeds `max_rows`.
pub fn read_flat_orc_source_with_projection(
    path: &Path,
    max_rows: usize,
    required_columns: &[String],
) -> Result<FlatLocalSourceTable> {
    let source = read_flat_orc_columnar_source_with_projection(path, max_rows, required_columns)?;
    materialize_flat_columnar_source_to_scalar_table(&source, path, "ORC")
}

/// Read selected root columns from a local ORC file into columnar Arrow
/// batches.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the ORC reader cannot be constructed, or the row count exceeds `max_rows`.
pub fn read_flat_orc_columnar_source_with_projection(
    path: &Path,
    max_rows: usize,
    required_columns: &[String],
) -> Result<FlatLocalColumnarSource> {
    let file = open_local_source_file(path, "ORC")?;
    let builder = orc_rust::ArrowReaderBuilder::try_new(file).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to create local ORC source reader for '{}': {error}",
            path.display()
        ))
    })?;
    let schema = builder.schema();
    let header = source_schema_header(path, "ORC", schema.as_ref())?;
    let column_dtypes = source_schema_column_dtypes(schema.as_ref());
    let projection_indices = projection_indices_for_header(&header, required_columns);
    let projected_header = projection_header(&header, &projection_indices);
    let projection = orc_rust::projection::ProjectionMask::named_roots(
        builder.file_metadata().root_data_type(),
        &projected_header,
    );
    let mut reader = builder
        .with_batch_size(max_rows.clamp(1, 8192))
        .with_projection(projection)
        .build();

    read_flat_record_batch_reader_columnar_with_header(
        &mut reader,
        path,
        "ORC",
        max_rows,
        header,
        column_dtypes,
        &projected_header,
    )
}

fn read_flat_record_batch_reader_columnar<R>(
    reader: &mut R,
    path: &Path,
    source_label: &str,
    max_rows: usize,
) -> Result<FlatLocalColumnarSource>
where
    R: RecordBatchReader,
{
    let schema = reader.schema();
    let header = source_schema_header(path, source_label, schema.as_ref())?;
    let column_dtypes = source_schema_column_dtypes(schema.as_ref());
    let projected_header = header.clone();
    read_flat_record_batch_reader_columnar_with_header(
        reader,
        path,
        source_label,
        max_rows,
        header,
        column_dtypes,
        &projected_header,
    )
}

fn read_flat_record_batch_reader_columnar_with_header<R>(
    reader: &mut R,
    path: &Path,
    source_label: &str,
    max_rows: usize,
    header: Vec<String>,
    column_dtypes: Vec<Option<LogicalDType>>,
    projected_header: &[String],
) -> Result<FlatLocalColumnarSource>
where
    R: RecordBatchReader,
{
    let schema = FlatColumnarReadSchema {
        header,
        column_dtypes,
        materialized_columns: projected_header.to_owned(),
        reader_projection_columns: projected_header.to_owned(),
    };
    read_flat_record_batch_reader_columnar_with_reader_projection(
        reader,
        path,
        source_label,
        max_rows,
        schema,
    )
}

fn read_flat_record_batch_reader_columnar_with_reader_projection<R>(
    reader: &mut R,
    path: &Path,
    source_label: &str,
    max_rows: usize,
    schema: FlatColumnarReadSchema,
) -> Result<FlatLocalColumnarSource>
where
    R: RecordBatchReader,
{
    if schema.column_dtypes.len() != schema.header.len() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local {source_label} source '{}' produced {} source dtype hints for {} schema columns",
            path.display(),
            schema.column_dtypes.len(),
            schema.header.len()
        )));
    }
    let mut batches = Vec::new();
    let mut row_count = 0usize;
    for batch in reader {
        let batch = batch.map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to read local {source_label} source batch from '{}': {error}",
                path.display()
            ))
        })?;
        if batch.num_columns() != schema.reader_projection_columns.len() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local {source_label} source '{}' changed projected column count between schema and batch",
                path.display(),
            )));
        }
        row_count = row_count.checked_add(batch.num_rows()).ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "local {source_label} source '{}' row count overflowed usize",
                path.display()
            ))
        })?;
        if row_count > max_rows {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local {source_label} source '{}' exceeds the scoped SQL local-source row limit of {max_rows}",
                path.display()
            )));
        }
        batches.push(batch);
    }

    Ok(FlatLocalColumnarSource {
        header: schema.header,
        column_dtypes: schema.column_dtypes,
        materialized_columns: schema.materialized_columns,
        reader_projection_columns: schema.reader_projection_columns,
        batches,
        row_count,
    })
}

/// Materialize a scoped flat columnar local source into `ShardLoom` scalar rows.
///
/// This is an explicit compatibility boundary for caller-owned direct runtime
/// paths. It preserves the original source schema and reader projection
/// metadata in the returned table, but the rows are decoded scalar values for
/// ShardLoom-native expression evaluation rather than Arrow execution.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when any projected Arrow array
/// contains an unsupported scalar type for the scoped local runtime.
pub fn materialize_flat_columnar_source_to_scalar_table(
    source: &FlatLocalColumnarSource,
    path: &Path,
    source_label: &str,
) -> Result<FlatLocalSourceTable> {
    let output_columns = source
        .materialized_columns
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut rows = Vec::with_capacity(source.row_count);
    for batch in &source.batches {
        for row_index in 0..batch.num_rows() {
            let mut row = BTreeMap::new();
            for (column, array) in source.reader_projection_columns.iter().zip(batch.columns()) {
                if !output_columns.contains(column.as_str()) {
                    continue;
                }
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

    Ok(FlatLocalSourceTable {
        header: source.header.clone(),
        column_dtypes: source.column_dtypes.clone(),
        reader_projection_columns: source.reader_projection_columns.clone(),
        rows,
    })
}

fn open_local_source_file(path: &Path, source_label: &str) -> Result<File> {
    File::open(path).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to open local {source_label} source '{}': {error}",
            path.display()
        ))
    })
}

fn schema_field_names(schema: &Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

fn source_schema_header(path: &Path, source_label: &str, schema: &Schema) -> Result<Vec<String>> {
    let header = schema_field_names(schema);
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

fn source_schema_column_dtypes(schema: &Schema) -> Vec<Option<LogicalDType>> {
    schema
        .fields()
        .iter()
        .map(|field| source_schema_dtype_hint(field.data_type()))
        .collect()
}

fn source_schema_dtype_hint(data_type: &DataType) -> Option<LogicalDType> {
    match data_type {
        DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::BinaryView => Some(LogicalDType::Binary),
        DataType::Decimal128(precision, scale) => {
            let scale = u8::try_from(*scale).ok()?;
            (scale <= *precision)
                .then(|| LogicalDType::Extension(format!("decimal128({precision},{scale})")))
        }
        _ => None,
    }
}

struct FlatColumnarReadSchema {
    header: Vec<String>,
    column_dtypes: Vec<Option<LogicalDType>>,
    materialized_columns: Vec<String>,
    reader_projection_columns: Vec<String>,
}

fn projection_indices_for_header(header: &[String], required_columns: &[String]) -> Vec<usize> {
    let required = required_columns
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    header
        .iter()
        .enumerate()
        .filter_map(|(index, column)| required.contains(column.as_str()).then_some(index))
        .collect()
}

fn projection_header(header: &[String], projection_indices: &[usize]) -> Vec<String> {
    projection_indices
        .iter()
        .filter_map(|index| header.get(*index).cloned())
        .collect()
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
    encode_parquet_record_batch(&batch)
}

/// Encode flat scalar rows into local Parquet bytes with optional logical dtype hints.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when hints do not match the
/// declared columns or an admitted typed sink cannot preserve the hinted dtype.
pub fn encode_flat_parquet_rows_with_dtypes(
    columns: &[String],
    column_dtypes: &[Option<LogicalDType>],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>> {
    let batch = flat_rows_to_record_batch_with_dtypes(
        columns,
        column_dtypes,
        rows,
        "local Parquet output",
    )?;
    encode_parquet_record_batch(&batch)
}

fn encode_parquet_record_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut writer = parquet::arrow::ArrowWriter::try_new(Vec::new(), batch.schema(), None)
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local Parquet output writer: {error}"
            ))
        })?;
    writer.write(batch).map_err(|error| {
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
    encode_arrow_ipc_record_batch(&batch)
}

/// Encode flat scalar rows into local Arrow IPC bytes with optional logical dtype hints.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when hints do not match the
/// declared columns or an admitted typed sink cannot preserve the hinted dtype.
pub fn encode_flat_arrow_ipc_rows_with_dtypes(
    columns: &[String],
    column_dtypes: &[Option<LogicalDType>],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>> {
    let batch = flat_rows_to_record_batch_with_dtypes(
        columns,
        column_dtypes,
        rows,
        "local Arrow IPC output",
    )?;
    encode_arrow_ipc_record_batch(&batch)
}

fn encode_arrow_ipc_record_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut writer = arrow_ipc::writer::FileWriter::try_new(Vec::new(), batch.schema().as_ref())
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local Arrow IPC output writer: {error}"
            ))
        })?;
    writer.write(batch).map_err(|error| {
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
    let column_dtypes = vec![None; columns.len()];
    encode_flat_avro_rows_with_dtypes(columns, &column_dtypes, rows)
}

/// Encode flat scalar rows into local Avro bytes with optional logical dtype hints.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when hints do not match the
/// declared columns or an admitted typed sink cannot preserve the hinted dtype.
pub fn encode_flat_avro_rows_with_dtypes(
    columns: &[String],
    column_dtypes: &[Option<LogicalDType>],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>> {
    let batch =
        flat_rows_to_record_batch_with_dtypes(columns, column_dtypes, rows, "local Avro output")?;
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
    let column_dtypes = vec![None; columns.len()];
    encode_flat_orc_rows_with_dtypes(columns, &column_dtypes, rows)
}

/// Encode flat scalar rows into local ORC bytes with optional logical dtype hints.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when hints do not match the
/// declared columns or an admitted typed sink cannot preserve the hinted dtype.
pub fn encode_flat_orc_rows_with_dtypes(
    columns: &[String],
    column_dtypes: &[Option<LogicalDType>],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>> {
    let batch =
        flat_rows_to_record_batch_with_dtypes(columns, column_dtypes, rows, "local ORC output")?;
    validate_orc_record_batch_supported(&batch)?;
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

fn validate_orc_record_batch_supported(batch: &RecordBatch) -> Result<()> {
    for field in batch.schema().fields() {
        if matches!(
            field.data_type(),
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
        ) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local ORC output does not yet admit typed decimal128 preservation for column '{}'; orc-rust 0.8.0 can read decimal128 but its Arrow writer does not support decimal128 columns, so ShardLoom blocks before provider conversion instead of allowing a writer panic; decimal128 values are admitted through Parquet/Arrow IPC/Avro typed result boundaries and scoped local Vortex typed decimal output in this runtime slice; no fallback execution was attempted",
                field.name()
            )));
        }
    }
    Ok(())
}

fn flat_rows_to_record_batch(
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
    context: &str,
) -> Result<RecordBatch> {
    let column_dtypes = vec![None; columns.len()];
    flat_rows_to_record_batch_with_dtypes(columns, &column_dtypes, rows, context)
}

fn flat_rows_to_record_batch_with_dtypes(
    columns: &[String],
    column_dtypes: &[Option<LogicalDType>],
    rows: &[Vec<(String, ScalarValue)>],
    context: &str,
) -> Result<RecordBatch> {
    validate_flat_columns(columns, context)?;
    if column_dtypes.len() != columns.len() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{context} declared {} column dtype hints for {} columns",
            column_dtypes.len(),
            columns.len()
        )));
    }
    let fields_and_arrays = columns
        .iter()
        .enumerate()
        .map(|(column_index, column)| {
            flat_output_column_array(
                column,
                column_index,
                column_dtypes[column_index].as_ref(),
                rows,
                context,
            )
        })
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
    column_dtype: Option<&LogicalDType>,
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
    let declared_decimal = column_dtype
        .map(|dtype| decimal128_dtype_precision_scale(dtype, column, context))
        .transpose()?
        .flatten();
    let declared_binary = column_dtype.is_some_and(|dtype| matches!(dtype, LogicalDType::Binary));
    let inferred_kind = values
        .iter()
        .filter(|value| !matches!(value, ScalarValue::Null))
        .map(|value| scalar_family(value))
        .try_fold(None, |current, candidate| match current {
            None => Ok(Some(candidate)),
            Some(existing) if existing == candidate => Ok(Some(existing)),
            Some(existing) => Err(ShardLoomError::InvalidOperation(format!(
                "{context} column '{column}' mixes scalar families {existing} and {candidate}; scoped compatibility output requires one non-null scalar family per column"
            ))),
        })?;
    let kind = match (declared_decimal, declared_binary, inferred_kind) {
        (Some(_), _, Some("decimal128") | None) => "decimal128",
        (Some((precision, scale)), _, Some(other)) => {
            return Err(ShardLoomError::InvalidOperation(format!(
                "{context} column '{column}' is declared decimal128({precision},{scale}) but contains non-null {other} values; scoped typed decimal sinks require Decimal128 values or NULLs"
            )));
        }
        (None, true, Some("binary") | None) => "binary",
        (None, true, Some(other)) => {
            return Err(ShardLoomError::InvalidOperation(format!(
                "{context} column '{column}' is declared binary but contains non-null {other} values; scoped binary sinks require Binary values or NULLs"
            )));
        }
        (None, false, Some(kind)) => kind,
        (None, false, None) => "utf8",
    };
    let nullable = values
        .iter()
        .any(|value| matches!(value, ScalarValue::Null));
    match kind {
        "boolean" => Ok(parquet_bool_column(column, &values, nullable, context)?),
        "int64" => Ok(parquet_int64_column(column, &values, nullable, context)?),
        "uint64" => Ok(parquet_uint64_column(column, &values, nullable, context)?),
        "float64" => Ok(parquet_float64_column(column, &values, nullable, context)?),
        "utf8" => Ok(parquet_utf8_column(column, &values, nullable, context)?),
        "binary" => Ok(parquet_binary_column(column, &values, nullable, context)?),
        "decimal128" => Ok(parquet_decimal128_column(
            column,
            &values,
            nullable,
            context,
            declared_decimal,
        )?),
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

fn parquet_binary_column(
    column: &str,
    values: &[&ScalarValue],
    nullable: bool,
    context: &str,
) -> Result<(Field, ArrayRef)> {
    let total_bytes = values
        .iter()
        .filter_map(|value| match value {
            ScalarValue::Binary(value) => Some(value.len()),
            _ => None,
        })
        .sum();
    let mut builder = BinaryBuilder::with_capacity(values.len(), total_bytes);
    for value in values {
        match value {
            ScalarValue::Binary(value) => builder.append_value(value),
            ScalarValue::Null => builder.append_null(),
            other => return Err(unexpected_sink_value(context, column, "binary", other)),
        }
    }
    Ok((
        Field::new(column, DataType::Binary, nullable),
        Arc::new(builder.finish()),
    ))
}

fn parquet_decimal128_column(
    column: &str,
    values: &[&ScalarValue],
    nullable: bool,
    context: &str,
    declared_precision_scale: Option<(u8, u8)>,
) -> Result<(Field, ArrayRef)> {
    let (precision, scale) =
        decimal128_column_precision_scale(column, values, context, declared_precision_scale)?;
    let scale_i8 = i8::try_from(scale).map_err(|_| {
        ShardLoomError::InvalidOperation(format!(
            "{context} column '{column}' cannot preserve decimal128({precision},{scale}): scale exceeds Arrow decimal128 range"
        ))
    })?;
    let mut builder = Decimal128Builder::with_capacity(values.len())
        .with_precision_and_scale(precision, scale_i8)
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "{context} column '{column}' cannot preserve decimal128({precision},{scale}): {error}"
            ))
        })?;
    for value in values {
        match value {
            ScalarValue::Decimal128 {
                value,
                precision: value_precision,
                scale: value_scale,
            } if *value_precision == precision && *value_scale == scale => {
                builder.append_value(*value);
            }
            ScalarValue::Decimal128 {
                precision: value_precision,
                scale: value_scale,
                ..
            } => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "{context} column '{column}' mixes decimal128 precision/scale decimal128({precision},{scale}) and decimal128({value_precision},{value_scale}); scoped typed decimal sinks require one decimal dtype per column"
                )));
            }
            ScalarValue::Null => builder.append_null(),
            other => return Err(unexpected_sink_value(context, column, "decimal128", other)),
        }
    }
    Ok((
        Field::new(column, DataType::Decimal128(precision, scale_i8), nullable),
        Arc::new(builder.finish()),
    ))
}

fn decimal128_column_precision_scale(
    column: &str,
    values: &[&ScalarValue],
    context: &str,
    declared_precision_scale: Option<(u8, u8)>,
) -> Result<(u8, u8)> {
    let mut precision_scale = declared_precision_scale;
    for value in values {
        if let ScalarValue::Decimal128 {
            precision, scale, ..
        } = value
        {
            match precision_scale {
                None => precision_scale = Some((*precision, *scale)),
                Some(existing) if existing == (*precision, *scale) => {}
                Some((existing_precision, existing_scale)) => {
                    return Err(ShardLoomError::InvalidOperation(format!(
                        "{context} column '{column}' mixes decimal128 precision/scale decimal128({existing_precision},{existing_scale}) and decimal128({precision},{scale}); scoped typed decimal sinks require one decimal dtype per column"
                    )));
                }
            }
        }
    }
    precision_scale.ok_or_else(|| {
        ShardLoomError::InvalidOperation(format!(
            "{context} column '{column}' did not contain a non-null decimal128 value or declared decimal128 dtype hint"
        ))
    })
}

fn decimal128_dtype_precision_scale(
    dtype: &LogicalDType,
    column: &str,
    context: &str,
) -> Result<Option<(u8, u8)>> {
    let LogicalDType::Extension(value) = dtype else {
        return Ok(None);
    };
    if !value.starts_with("decimal128") {
        return Ok(None);
    }
    let invalid_decimal_hint = || {
        ShardLoomError::InvalidOperation(format!(
            "{context} column '{column}' has invalid decimal128 dtype hint {value:?}; decimal hints must use decimal128(precision,scale) with 1 <= precision <= 38 and scale <= precision"
        ))
    };
    let args = value
        .strip_prefix("decimal128(")
        .and_then(|rest| rest.strip_suffix(')'))
        .ok_or_else(invalid_decimal_hint)?;
    let (precision_raw, scale_raw) = args.split_once(',').ok_or_else(invalid_decimal_hint)?;
    let precision = precision_raw
        .trim()
        .parse::<u8>()
        .map_err(|_| invalid_decimal_hint())?;
    let scale = scale_raw
        .trim()
        .parse::<u8>()
        .map_err(|_| invalid_decimal_hint())?;
    if precision == 0 || precision > 38 || scale > precision {
        return Err(invalid_decimal_hint());
    }
    Ok(Some((precision, scale)))
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
        ScalarValue::Decimal128 { .. } => "decimal128",
        ScalarValue::List(_) => "list",
        ScalarValue::Struct(_) => "struct",
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
    if let Some(values) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(ScalarValue::Binary(values.value(row_index).to_vec()));
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(ScalarValue::Binary(values.value(row_index).to_vec()));
    }
    if let Some(values) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        return Ok(ScalarValue::Binary(values.value(row_index).to_vec()));
    }
    if let Some(values) = array.as_any().downcast_ref::<BinaryViewArray>() {
        return Ok(ScalarValue::Binary(values.value(row_index).to_vec()));
    }
    if let Some(values) = array.as_any().downcast_ref::<Date32Array>() {
        return Ok(ScalarValue::Date32(values.value(row_index)));
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Ok(ScalarValue::TimestampMicros(values.value(row_index)));
    }
    if let Some(values) = array.as_any().downcast_ref::<Decimal128Array>() {
        let DataType::Decimal128(precision, scale) = values.data_type() else {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local {source_label} source '{}' column '{column}' reported Decimal128Array with non-decimal Arrow type {:?}",
                path.display(),
                values.data_type()
            )));
        };
        let scale = u8::try_from(*scale).map_err(|_| {
            ShardLoomError::InvalidOperation(format!(
                "local {source_label} source '{}' column '{column}' has unsupported negative decimal128 scale {}; scoped decimal sources require 0 <= scale <= precision",
                path.display(),
                scale
            ))
        })?;
        if scale > *precision {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local {source_label} source '{}' column '{column}' has invalid decimal128({precision},{scale}) dtype; scoped decimal sources require scale <= precision",
                path.display()
            )));
        }
        return Ok(ScalarValue::Decimal128 {
            value: values.value(row_index),
            precision: *precision,
            scale,
        });
    }
    Err(ShardLoomError::InvalidOperation(format!(
        "local {source_label} source '{}' column '{column}' has unsupported Arrow type {:?}; scoped local-source runtime admits booleans, integers, floats, UTF-8 strings, binary byte arrays, decimal128, date32, and timestamp_micros only",
        path.display(),
        array.data_type()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    type FlatSinkRow = Vec<(String, ScalarValue)>;
    type FlatSinkRows = Vec<FlatSinkRow>;
    type BinarySinkEncoder = fn(&[String], &[FlatSinkRow]) -> Result<Vec<u8>>;

    fn binary_source_with_column(column: &str, array: ArrayRef) -> FlatLocalColumnarSource {
        let schema = Arc::new(Schema::new(vec![Field::new(
            column,
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        FlatLocalColumnarSource {
            header: vec![column.to_string()],
            column_dtypes: vec![Some(LogicalDType::Binary)],
            materialized_columns: vec![column.to_string()],
            reader_projection_columns: vec![column.to_string()],
            row_count: batch.num_rows(),
            batches: vec![batch],
        }
    }

    fn assert_binary_materialization(column: &str, array: ArrayRef) {
        let table = materialize_flat_columnar_source_to_scalar_table(
            &binary_source_with_column(column, array),
            Path::new("target/binary.arrow"),
            "Arrow IPC",
        )
        .expect("materialize binary column");

        assert_eq!(table.header, vec![column.to_string()]);
        assert_eq!(table.reader_projection_columns, vec![column.to_string()]);
        assert_eq!(table.rows.len(), 3);
        assert_eq!(
            table.rows[0].get(column),
            Some(&ScalarValue::Binary(vec![0x00, 0xff, 0x10]))
        );
        assert_eq!(table.rows[1].get(column), Some(&ScalarValue::Null));
        assert_eq!(
            table.rows[2].get(column),
            Some(&ScalarValue::Binary(b"raw".to_vec()))
        );
    }

    #[test]
    fn materializes_columnar_binary_source_dtypes_as_scalar_binary() {
        assert_binary_materialization(
            "payload",
            Arc::new(BinaryArray::from(vec![
                Some(&[0x00, 0xff, 0x10][..]),
                None,
                Some(&b"raw"[..]),
            ])),
        );
        assert_binary_materialization(
            "large_payload",
            Arc::new(LargeBinaryArray::from(vec![
                Some(&[0x00, 0xff, 0x10][..]),
                None,
                Some(&b"raw"[..]),
            ])),
        );
        assert_binary_materialization(
            "fixed_payload",
            Arc::new(FixedSizeBinaryArray::from(vec![
                Some(&[0x00, 0xff, 0x10][..]),
                None,
                Some(&b"raw"[..]),
            ])),
        );
        assert_binary_materialization(
            "view_payload",
            Arc::new(BinaryViewArray::from(vec![
                Some(&[0x00, 0xff, 0x10][..]),
                None,
                Some(&b"raw"[..]),
            ])),
        );
    }

    #[test]
    fn materializes_columnar_decimal_source_dtypes_as_scalar_decimal() {
        let mut builder = Decimal128Builder::with_capacity(3)
            .with_precision_and_scale(10, 2)
            .expect("decimal precision and scale");
        builder.append_value(1234);
        builder.append_null();
        builder.append_value(-500);
        let array = Arc::new(builder.finish());
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 2),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        let source = FlatLocalColumnarSource {
            header: vec!["amount".to_string()],
            column_dtypes: vec![Some(LogicalDType::Extension(
                "decimal128(10,2)".to_string(),
            ))],
            materialized_columns: vec!["amount".to_string()],
            reader_projection_columns: vec!["amount".to_string()],
            row_count: batch.num_rows(),
            batches: vec![batch],
        };

        let table = materialize_flat_columnar_source_to_scalar_table(
            &source,
            Path::new("target/decimal.arrow"),
            "Arrow IPC",
        )
        .expect("materialize decimal column");

        assert_eq!(table.header, vec!["amount".to_string()]);
        assert_eq!(table.rows.len(), 3);
        assert_eq!(
            table.rows[0].get("amount"),
            Some(&ScalarValue::Decimal128 {
                value: 1234,
                precision: 10,
                scale: 2,
            })
        );
        assert_eq!(table.rows[1].get("amount"), Some(&ScalarValue::Null));
        assert_eq!(
            table.rows[2].get("amount"),
            Some(&ScalarValue::Decimal128 {
                value: -500,
                precision: 10,
                scale: 2,
            })
        );
    }

    fn binary_sink_rows() -> (Vec<String>, FlatSinkRows) {
        (
            vec!["id".to_string(), "payload".to_string()],
            vec![
                vec![
                    ("id".to_string(), ScalarValue::Int64(1)),
                    (
                        "payload".to_string(),
                        ScalarValue::Binary(vec![0x00, 0xff, 0x10]),
                    ),
                ],
                vec![
                    ("id".to_string(), ScalarValue::Int64(2)),
                    ("payload".to_string(), ScalarValue::Null),
                ],
                vec![
                    ("id".to_string(), ScalarValue::Int64(3)),
                    ("payload".to_string(), ScalarValue::Binary(Vec::new())),
                ],
                vec![
                    ("id".to_string(), ScalarValue::Int64(4)),
                    ("payload".to_string(), ScalarValue::Binary(b"raw".to_vec())),
                ],
            ],
        )
    }

    fn unique_binary_sink_path(extension: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "shardloom-vortex-binary-sink-{}-{nanos}.{extension}",
            std::process::id()
        ))
    }

    fn assert_binary_sink_round_trip(
        extension: &str,
        encode: BinarySinkEncoder,
        read: fn(&std::path::Path, usize) -> Result<FlatLocalSourceTable>,
    ) {
        let (columns, rows) = binary_sink_rows();
        let path = unique_binary_sink_path(extension);
        let bytes = encode(&columns, &rows).expect("encode binary sink rows");
        std::fs::write(&path, bytes).expect("write binary sink artifact");

        let table = read(&path, 10).expect("read binary sink artifact");
        assert_eq!(table.header, columns);
        assert_eq!(table.rows.len(), 4);
        assert_eq!(
            table.rows[0].get("payload"),
            Some(&ScalarValue::Binary(vec![0x00, 0xff, 0x10]))
        );
        assert_eq!(table.rows[1].get("payload"), Some(&ScalarValue::Null));
        assert_eq!(
            table.rows[2].get("payload"),
            Some(&ScalarValue::Binary(Vec::new()))
        );
        assert_eq!(
            table.rows[3].get("payload"),
            Some(&ScalarValue::Binary(b"raw".to_vec()))
        );

        std::fs::remove_file(path).expect("remove binary sink artifact");
    }

    #[test]
    fn preserves_binary_rows_in_feature_gated_structured_sinks() {
        assert_binary_sink_round_trip(
            "parquet",
            encode_flat_parquet_rows,
            read_flat_parquet_source,
        );
        assert_binary_sink_round_trip(
            "arrow",
            encode_flat_arrow_ipc_rows,
            read_flat_arrow_ipc_source,
        );
        assert_binary_sink_round_trip("avro", encode_flat_avro_rows, read_flat_avro_source);
        assert_binary_sink_round_trip("orc", encode_flat_orc_rows, read_flat_orc_source);
    }

    #[test]
    fn binary_dtype_hint_builds_binary_array_for_all_null_sink_column() {
        let columns = vec!["payload".to_string()];
        let dtypes = vec![Some(LogicalDType::Binary)];
        let rows = vec![vec![("payload".to_string(), ScalarValue::Null)]];

        let batch = flat_rows_to_record_batch_with_dtypes(
            &columns,
            &dtypes,
            &rows,
            "binary dtype hint output",
        )
        .expect("binary dtype hint builds record batch");

        assert_eq!(batch.schema().field(0).data_type(), &DataType::Binary);
        assert!(batch.column(0).is_null(0));
    }

    #[test]
    fn invalid_decimal_dtype_hint_blocks_all_null_output() {
        let columns = vec!["amount".to_string()];
        let dtypes = vec![Some(LogicalDType::Extension(
            "decimal128(10,12)".to_string(),
        ))];
        let rows = vec![vec![("amount".to_string(), ScalarValue::Null)]];

        let error = flat_rows_to_record_batch_with_dtypes(&columns, &dtypes, &rows, "test output")
            .expect_err("invalid decimal hint should fail before Arrow output");

        assert!(
            error.to_string().contains(
                "test output column 'amount' has invalid decimal128 dtype hint \"decimal128(10,12)\""
            ),
            "{error}"
        );
    }

    #[test]
    fn orc_decimal_output_blocks_before_writer_conversion() {
        let columns = vec!["amount".to_string()];
        let dtypes = vec![Some(LogicalDType::Extension(
            "decimal128(10,2)".to_string(),
        ))];
        let rows = vec![vec![(
            "amount".to_string(),
            ScalarValue::Decimal128 {
                value: 1234,
                precision: 10,
                scale: 2,
            },
        )]];

        let error = encode_flat_orc_rows_with_dtypes(&columns, &dtypes, &rows)
            .expect_err("ORC decimal output remains blocked");

        assert!(
            error.to_string().contains(
                "local ORC output does not yet admit typed decimal128 preservation for column 'amount'"
            ),
            "{error}"
        );
        assert!(
            error
                .to_string()
                .contains("orc-rust 0.8.0 can read decimal128 but its Arrow writer does not support decimal128 columns"),
            "{error}"
        );
        assert!(
            error
                .to_string()
                .contains("no fallback execution was attempted"),
            "{error}"
        );
    }
}
