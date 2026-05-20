//! Feature-gated local compatibility-format readers for runtime promotion.
//!
//! These helpers are compatibility input adapters, not fallback execution
//! engines. They decode admitted local file formats into `ShardLoom` scalar rows
//! so caller-owned runtime paths can emit explicit materialization evidence and
//! fail closed for unsupported Arrow types.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::Path,
};

use arrow_array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, RecordBatchReader, StringArray, StringViewArray,
    TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use shardloom_core::{Result, ScalarValue, ShardLoomError};

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

    let schema = reader.schema();
    let header = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    if header.is_empty() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local Parquet source '{}' must contain at least one column",
            path.display()
        )));
    }
    let mut seen_columns = BTreeSet::new();
    for column in &header {
        if column.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local Parquet source '{}' contains an empty column name",
                path.display()
            )));
        }
        if !seen_columns.insert(column) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local Parquet source '{}' contains duplicate column '{column}'",
                path.display()
            )));
        }
    }

    let mut rows = Vec::new();
    for batch in &mut reader {
        let batch = batch.map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to read local Parquet source batch from '{}': {error}",
                path.display()
            ))
        })?;
        if batch.num_columns() != header.len() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local Parquet source '{}' changed column count between schema and batch",
                path.display()
            )));
        }
        for row_index in 0..batch.num_rows() {
            if rows.len() >= max_rows {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "local Parquet source '{}' exceeds the scoped SQL local-source row limit of {max_rows}",
                    path.display()
                )));
            }
            let mut row = BTreeMap::new();
            for (column, array) in header.iter().zip(batch.columns()) {
                row.insert(
                    column.clone(),
                    arrow_scalar_to_shardloom(array.as_ref(), row_index, column, path)?,
                );
            }
            rows.push(row);
        }
    }

    Ok(FlatLocalSourceTable { header, rows })
}

#[allow(clippy::cast_precision_loss)]
fn arrow_scalar_to_shardloom(
    array: &dyn Array,
    row_index: usize,
    column: &str,
    path: &Path,
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
        "local Parquet source '{}' column '{column}' has unsupported Arrow type {:?}; scoped Parquet local-source runtime admits booleans, integers, floats, UTF-8 strings, date32, and timestamp_micros only",
        path.display(),
        array.data_type()
    )))
}
