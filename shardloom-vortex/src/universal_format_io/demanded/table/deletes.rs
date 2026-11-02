//! Iceberg delete applicability and exact primitive equality keys.

use super::{DataFile, Result, Snapshot, TableField, failed, metadata, schema};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub(super) enum Content {
    Positions(BTreeMap<String, BTreeSet<u64>>),
    Equality {
        fields: Vec<TableField>,
        keys: BTreeSet<Vec<u8>>,
    },
}

pub(super) struct DeleteFile {
    file: DataFile,
    global: bool,
    pub(super) content: Content,
}

impl DeleteFile {
    pub(super) fn applies(&self, file: &DataFile) -> bool {
        let same_partition =
            self.file.spec_id == file.spec_id && self.file.partition_key == file.partition_key;
        match &self.content {
            Content::Positions(_) => same_partition && file.sequence <= self.file.sequence,
            Content::Equality { .. } => {
                (self.global || same_partition) && file.sequence < self.file.sequence
            }
        }
    }

    pub(super) fn position_deleted(&self, file: &DataFile, position: u64) -> bool {
        match &self.content {
            Content::Positions(paths) => paths
                .get(&file.location)
                .is_some_and(|positions| positions.contains(&position)),
            Content::Equality { .. } => false,
        }
    }

    pub(super) fn validate_positions(&self, file: &DataFile, count: u64) -> Result<()> {
        if self.applies(file)
            && let Content::Positions(paths) = &self.content
            && paths
                .get(&file.location)
                .and_then(BTreeSet::last)
                .is_some_and(|position| *position >= count)
        {
            return Err(failed(
                "Iceberg position delete exceeds its referenced data-file extent",
            ));
        }
        Ok(())
    }
}

pub(super) fn read(
    snapshot: &mut Snapshot,
    file: DataFile,
    ids: Option<Vec<i32>>,
    global: bool,
) -> Result<()> {
    if file
        .bytes
        .is_some_and(|bytes| std::fs::metadata(&file.path).is_ok_and(|value| value.len() != bytes))
    {
        return Err(failed("Iceberg delete-file size differs from manifest"));
    }
    let records = snapshot.metadata.records(&file.path, "parquet")?;
    let records = records.value();
    if file.records != Some(records.rows.len() as u64) {
        return Err(failed(
            "Iceberg delete-file record count differs from manifest",
        ));
    }
    validate_ids(records.schema.as_ref())?;
    let content = if let Some(ids) = ids {
        if ids.is_empty() || ids.iter().collect::<BTreeSet<_>>().len() != ids.len() {
            return Err(failed("Iceberg equality_ids must be nonempty and unique"));
        }
        let mut fields = Vec::new();
        let mut names = Vec::new();
        for id in ids {
            let field = snapshot.historical_fields.get(&id).ok_or_else(|| {
                failed("Iceberg equality-delete field ID is not in retained table schemas")
            })?;
            if matches!(
                field.field.data_type(),
                DataType::Float32 | DataType::Float64
            ) {
                return Err(failed(
                    "floating-point Iceberg equality-delete fields are forbidden",
                ));
            }
            let index = find_id(records.schema.as_ref(), id)?
                .ok_or_else(|| failed("equality-delete file is missing a required field ID"))?;
            let physical = records.schema.field(index);
            if !schema::compatible(physical.data_type(), field.field.data_type(), true) {
                return Err(failed(
                    "Iceberg equality-delete physical type differs from field identity",
                ));
            }
            names.push(physical.name().clone());
            fields.push(field.clone());
        }
        let mut keys = BTreeSet::new();
        for row in &records.rows {
            snapshot.metadata.cancellation.check()?;
            let mut key = Vec::new();
            for (field, name) in fields.iter().zip(&names) {
                key_json(
                    row.get(name)
                        .ok_or_else(|| failed("equality-delete record has no key field"))?,
                    field.field.data_type(),
                    &mut key,
                )?;
            }
            snapshot.metadata.retain(key.capacity() as u64 + 256)?;
            keys.insert(key);
        }
        snapshot.metadata.retain(fields.len() as u64 * 4096)?;
        Content::Equality { fields, keys }
    } else {
        let path = find_id(records.schema.as_ref(), 2_147_483_546)?
            .ok_or_else(|| failed("position delete is missing file_path field ID"))?;
        let pos = find_id(records.schema.as_ref(), 2_147_483_545)?
            .ok_or_else(|| failed("position delete is missing pos field ID"))?;
        if records.schema.field(path).data_type() != &DataType::Utf8
            || records.schema.field(pos).data_type() != &DataType::Int64
        {
            return Err(failed(
                "position-delete path/position types must be string/long",
            ));
        }
        let mut paths: BTreeMap<String, BTreeSet<u64>> = BTreeMap::new();
        for row in &records.rows {
            snapshot.metadata.cancellation.check()?;
            let location = metadata::string(row, records.schema.field(path).name())?;
            let position = metadata::unsigned(row, records.schema.field(pos).name())?;
            snapshot.metadata.retain(location.len() as u64 * 4 + 256)?;
            paths.entry(location.into()).or_default().insert(position);
        }
        Content::Positions(paths)
    };
    snapshot
        .metadata
        .retain(8192 + (file.location.len() + file.partition_key.len()) as u64 * 4)?;
    snapshot.deletes.try_reserve(1).map_err(failed)?;
    snapshot.deletes.push(DeleteFile {
        file,
        global,
        content,
    });
    Ok(())
}

pub(super) fn field_id(field: &Field) -> Result<Option<i32>> {
    field
        .metadata()
        .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)
        .map(|value| value.parse().map_err(failed))
        .transpose()
}

pub(super) fn validate_ids(schema: &Schema) -> Result<()> {
    validate_names(schema)?;
    let mut ids = BTreeSet::new();
    for field in schema.fields() {
        let id = field_id(field)?
            .ok_or_else(|| failed("table file requires authoritative Parquet field IDs"))?;
        if id <= 0 || !ids.insert(id) {
            return Err(failed(
                "table file contains duplicate or nonpositive field IDs",
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_names(schema: &Schema) -> Result<()> {
    for (index, field) in schema.fields().iter().enumerate() {
        if schema.index_of(field.name()).map_err(failed)? != index {
            return Err(failed("table file contains ambiguous physical field names"));
        }
    }
    Ok(())
}

pub(super) fn find_id(schema: &Schema, id: i32) -> Result<Option<usize>> {
    for (index, field) in schema.fields().iter().enumerate() {
        if field_id(field)? == Some(id) {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

fn bytes_key(value: &[u8], key: &mut Vec<u8>) -> Result<()> {
    key.try_reserve(
        value
            .len()
            .checked_add(8)
            .ok_or_else(|| failed("delete key length overflow"))?,
    )
    .map_err(failed)?;
    key.extend_from_slice(&(value.len() as u64).to_le_bytes());
    key.extend_from_slice(value);
    Ok(())
}

fn key_json(value: &Value, dtype: &DataType, key: &mut Vec<u8>) -> Result<()> {
    key.try_reserve(17).map_err(failed)?;
    if value.is_null() {
        key.push(0);
        return Ok(());
    }
    key.push(1);
    match dtype {
        DataType::Boolean => key.push(u8::from(
            value
                .as_bool()
                .ok_or_else(|| failed("invalid boolean delete key"))?,
        )),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Date32
        | DataType::Time64(_)
        | DataType::Timestamp(_, _) => key.extend_from_slice(
            &value
                .as_i64()
                .ok_or_else(|| failed("invalid integer/temporal delete key"))?
                .to_le_bytes(),
        ),
        DataType::Utf8 => bytes_key(
            value
                .as_str()
                .ok_or_else(|| failed("invalid string delete key"))?
                .as_bytes(),
            key,
        )?,
        DataType::Binary => {
            let bytes = value
                .as_array()
                .ok_or_else(|| failed("invalid binary delete key"))?
                .iter()
                .map(|v| {
                    v.as_u64()
                        .and_then(|v| u8::try_from(v).ok())
                        .ok_or_else(|| failed("invalid delete-key byte"))
                })
                .collect::<Result<Vec<_>>>()?;
            bytes_key(&bytes, key)?;
        }
        DataType::Decimal128(_, _) => key.extend_from_slice(
            &value
                .as_str()
                .ok_or_else(|| failed("invalid decimal delete key"))?
                .parse::<i128>()
                .map_err(failed)?
                .to_le_bytes(),
        ),
        _ => return Err(failed("unsupported equality-delete key type")),
    }
    Ok(())
}

pub(super) fn key_batch(
    batch: &RecordBatch,
    columns: &[Option<String>],
    row: usize,
    key: &mut Vec<u8>,
) -> Result<()> {
    key.clear();
    for name in columns {
        key.try_reserve(17).map_err(failed)?;
        let Some(name) = name else {
            key.push(0);
            continue;
        };
        let column = batch
            .column_by_name(name)
            .ok_or_else(|| failed("required equality-delete column was not read"))?;
        if column.is_null(row) {
            key.push(0);
            continue;
        }
        key.push(1);
        key_array(column.as_ref(), row, key)?;
    }
    Ok(())
}

fn key_array(array: &dyn Array, row: usize, key: &mut Vec<u8>) -> Result<()> {
    use arrow_array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int8Array, Int16Array, Int32Array,
        Int64Array, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    };
    macro_rules! integer {
        ($ty:ty) => {
            if let Some(values) = array.as_any().downcast_ref::<$ty>() {
                key.extend_from_slice(&i64::from(values.value(row)).to_le_bytes());
                return Ok(());
            }
        };
    }
    integer!(Int8Array);
    integer!(Int16Array);
    integer!(Int32Array);
    integer!(Int64Array);
    integer!(Date32Array);
    integer!(Time64MicrosecondArray);
    integer!(TimestampMicrosecondArray);
    if let Some(values) = array.as_any().downcast_ref::<BooleanArray>() {
        key.push(u8::from(values.value(row)));
        return Ok(());
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return bytes_key(values.value(row).as_bytes(), key);
    }
    if let Some(values) = array.as_any().downcast_ref::<BinaryArray>() {
        return bytes_key(values.value(row), key);
    }
    if let Some(values) = array.as_any().downcast_ref::<Decimal128Array>() {
        key.extend_from_slice(&values.value(row).to_le_bytes());
        return Ok(());
    }
    Err(failed("unsupported physical equality-delete key type"))
}
