//! Held local metadata reads and bounded compatibility-record decoding.

use super::{Result, ScalarValue, failed};
use crate::{resident_session::OwnedLocalRead, source_identity::SourceIdentity};
use arrow_array::{Array, RecordBatch};
use serde_json::Value;
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{Budgeted, LiveMemoryPool, MemoryLease},
};
use std::{
    collections::BTreeMap,
    io::Read as _,
    path::{Path, PathBuf},
    sync::Arc,
};

pub(super) struct ControlRecords {
    pub(super) rows: Vec<Value>,
    pub(super) properties: BTreeMap<String, String>,
    pub(super) schema: arrow_schema::SchemaRef,
    _avro_schema_credit: Option<Arc<dyn arrow_avro::reader::DecoderMemoryReservation>>,
}

pub(super) struct Metadata {
    pub(super) memory: LiveMemoryPool,
    pub(super) cancellation: CancellationToken,
    identities: Vec<Arc<SourceIdentity>>,
    credit: MemoryLease,
    pub(super) read_bytes: u64,
}

impl Metadata {
    pub(super) fn new(memory: &LiveMemoryPool, cancellation: &CancellationToken) -> Result<Self> {
        cancellation.check()?;
        Ok(Self {
            memory: memory.clone(),
            cancellation: cancellation.clone(),
            identities: Vec::new(),
            credit: memory.reserve(8192)?,
            read_bytes: 0,
        })
    }

    pub(super) fn retain(&mut self, bytes: u64) -> Result<()> {
        self.credit.resize(
            self.credit
                .bytes()
                .checked_add(bytes)
                .ok_or_else(|| failed("table metadata capacity overflow"))?,
        )
    }

    fn identity(&mut self, path: &Path) -> Result<Arc<SourceIdentity>> {
        self.cancellation.check()?;
        self.retain(1024 + path.as_os_str().len() as u64 * 4)?;
        let identity = Arc::new(SourceIdentity::capture(path)?);
        self.identities.try_reserve(1).map_err(failed)?;
        self.identities.push(Arc::clone(&identity));
        Ok(identity)
    }

    pub(super) fn bytes(&mut self, path: &Path) -> Result<Budgeted<Vec<u8>>> {
        let identity = self.identity(path)?;
        let mut input = identity.open_checked()?;
        let size = input.metadata().map_err(failed)?.len();
        // Control metadata is bounded independently of data-page streams.
        if size > 64 * 1024 * 1024 {
            return Err(failed("one table metadata file exceeds 64 MiB"));
        }
        let credit = self.memory.reserve(
            size.checked_add(8192)
                .ok_or_else(|| failed("metadata length overflow"))?,
        )?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(usize::try_from(size).map_err(failed)?)
            .map_err(failed)?;
        while (bytes.len() as u64) < size {
            self.cancellation.check()?;
            let before = bytes.len();
            input
                .by_ref()
                .take((size - before as u64).min(65_536))
                .read_to_end(&mut bytes)
                .map_err(failed)?;
            if bytes.len() == before {
                return Err(failed("table metadata ended before its held extent"));
            }
        }
        identity.validate()?;
        self.read_bytes = self
            .read_bytes
            .checked_add(size)
            .ok_or_else(|| failed("metadata byte counter overflow"))?;
        Ok(Budgeted::new(bytes, credit))
    }

    pub(super) fn json(&mut self, path: &Path) -> Result<Budgeted<Value>> {
        let bytes = self.bytes(path)?;
        let capacity = (bytes.value().len() as u64)
            .checked_mul(32)
            .and_then(|n| n.checked_add(8192))
            .ok_or_else(|| failed("JSON metadata capacity overflow"))?;
        let credit = self.memory.reserve(capacity)?;
        let value = serde_json::from_slice(bytes.value()).map_err(failed)?;
        self.cancellation.check()?;
        Ok(Budgeted::new(value, credit))
    }

    /// Retain only control-plane records. Data rows never pass through JSON.
    #[allow(clippy::too_many_lines)]
    pub(super) fn records(
        &mut self,
        path: &Path,
        format: &str,
    ) -> Result<Budgeted<ControlRecords>> {
        let identity = self.identity(path)?;
        let input =
            OwnedLocalRead::metadata(Arc::clone(&identity), &self.memory, &self.cancellation)?;
        let metrics = Arc::clone(input.metrics());
        let mut parquet_workspace = None;
        let mut reader = match format {
            "avro" => super::super::decoder::avro(input, None, &self.memory, &self.cancellation)?,
            "parquet" => {
                use crate::universal_format_io::demanded::source::metadata_admission;
                let admission =
                    metadata_admission::Admission::new(&self.memory, &self.cancellation)?;
                let metadata = parquet::arrow::arrow_reader::ArrowReaderMetadata::load(
                    &input,
                    crate::universal_format_io::parquet_metadata_first_reader_options()
                        .with_metadata_memory_reservation(admission.clone()),
                )
                .map_err(metadata_admission::source_error)?;
                let mut held = admission.finish()?;
                self.credit.absorb(&mut held)?;
                let rows = crate::universal_format_io::demanded::metadata_count::file_rows(
                    metadata.metadata(),
                    &self.cancellation,
                )?;
                if rows > 1_000_000 {
                    return Err(failed("one table control file exceeds 1000000 records"));
                }
                let mask = parquet::arrow::ProjectionMask::all();
                let capacity =
                    metadata
                        .metadata()
                        .row_groups()
                        .iter()
                        .try_fold(8192, |maximum, group| {
                            crate::universal_format_io::demanded::batch_work::parquet_workspace(
                                group, &mask,
                            )
                            .map(|bytes| maximum.max(bytes))
                        })?;
                // The provider visits one row group at a time. Admit its
                // largest physical workspace before constructing a decoder.
                parquet_workspace = Some(self.memory.reserve(capacity)?);
                super::super::decoder::Decoder::Parquet(
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::new_with_metadata(input, metadata)
                        .build().map_err(failed)?)
            }
            _ => return Err(failed("table control records require Avro or Parquet")),
        };
        let mut rows = Vec::new();
        let mut credit = self.memory.reserve(8192)?;
        let mut retained = 8192_u64;
        let mut properties = BTreeMap::new();
        let avro_schema_credit = match &reader {
            super::super::decoder::Decoder::Avro(reader) => reader.schema_memory_reservation(),
            _ => None,
        };
        let schema = match &reader {
            super::super::decoder::Decoder::Avro(reader) => reader.schema(),
            super::super::decoder::Decoder::Parquet(reader) => {
                arrow_array::RecordBatchReader::schema(reader)
            }
            _ => unreachable!("control decoder construction is restricted above"),
        };
        if let super::super::decoder::Decoder::Avro(reader) = &reader {
            for (key, value) in reader.avro_header().metadata() {
                let Ok(key) = std::str::from_utf8(key) else {
                    continue;
                };
                if !matches!(
                    key,
                    "schema"
                        | "schema-id"
                        | "partition-spec"
                        | "partition-spec-id"
                        | "format-version"
                        | "content"
                ) {
                    continue;
                }
                let value = std::str::from_utf8(value).map_err(failed)?;
                retained = retained
                    .checked_add((key.len() + value.len()) as u64 * 4 + 256)
                    .ok_or_else(|| failed("Avro property capacity overflow"))?;
                credit.resize(retained)?;
                if properties.insert(key.into(), value.into()).is_some() {
                    return Err(failed("duplicate table Avro property"));
                }
            }
        }
        let cost = crate::universal_format_io::demanded::batch_work::cost(&schema, &schema, &[])?;
        let mut sizing = crate::universal_format_io::demanded::batch_work::Sizing::default();
        loop {
            self.cancellation.check()?;
            let remaining = reader.prepare_rows()?.unwrap_or(u64::MAX);
            let selected = sizing.select(
                &self.memory,
                cost,
                remaining,
                crate::universal_format_io::demanded::batch_work::MAX_ROWS,
            )?;
            if let Some(selected) = selected {
                reader.set_batch_size(usize::try_from(selected.units()).map_err(failed)?)?;
            }
            let Some(batch) = reader.next() else { break };
            let batch = batch.map_err(super::super::decoder::source_error)?;
            let workspace = (batch.get_array_memory_size() as u64)
                .checked_mul(32)
                .and_then(|n| n.checked_add(8192))
                .ok_or_else(|| failed("control record workspace overflow"))?;
            credit.resize(
                retained
                    .checked_add(workspace)
                    .ok_or_else(|| failed("control metadata overflow"))?,
            )?;
            if rows
                .len()
                .checked_add(batch.num_rows())
                .is_none_or(|len| len > 1_000_000)
            {
                return Err(failed("one table control file exceeds 1000000 records"));
            }
            for row in 0..batch.num_rows() {
                self.cancellation.check()?;
                let value = record(&batch, row)?;
                retained = retained
                    .checked_add(json_bytes(&value)? + 64)
                    .ok_or_else(|| failed("control metadata capacity overflow"))?;
                credit.resize(
                    retained
                        .checked_add(workspace)
                        .ok_or_else(|| failed("control metadata overflow"))?,
                )?;
                rows.try_reserve(1).map_err(failed)?;
                rows.push(value);
            }
            if let Some(selected) = selected {
                sizing.complete(selected, &batch);
            }
            credit.resize(retained)?;
        }
        drop(reader);
        drop(parquet_workspace);
        identity.validate()?;
        self.read_bytes = self
            .read_bytes
            .checked_add(metrics.bytes.load(std::sync::atomic::Ordering::Relaxed))
            .ok_or_else(|| failed("control read counter overflow"))?;
        Ok(Budgeted::new(
            ControlRecords {
                rows,
                properties,
                schema,
                _avro_schema_credit: avro_schema_credit,
            },
            credit,
        ))
    }

    pub(super) fn validate(&self, cancellation: &CancellationToken) -> Result<()> {
        for identity in &self.identities {
            cancellation.check()?;
            identity.validate()?;
        }
        cancellation.check()
    }

    pub(super) fn validate_destination(&self, path: &Path) -> Result<()> {
        use std::os::unix::fs::MetadataExt as _;
        let path = std::path::absolute(path).map_err(failed)?;
        let output = match std::fs::metadata(&path) {
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(failed(error)),
        };
        if self.identities.iter().any(|identity| {
            path == identity.path()
                || output.as_ref().is_some_and(|value| {
                    value.dev() == identity.generation.device
                        && value.ino() == identity.generation.inode
                })
        }) {
            return Err(failed(
                "output aliases selected table metadata or delete input",
            ));
        }
        Ok(())
    }
}

pub(super) fn local_path(root: &Path, location: &str) -> Result<PathBuf> {
    let path = if let Some(value) = location.strip_prefix("file://") {
        if !value.starts_with('/') {
            return Err(failed(
                "file URI must have an empty authority and absolute path",
            ));
        }
        PathBuf::from(percent_decode(value)?)
    } else {
        if location.contains("://") || location.starts_with("//") {
            return Err(failed(
                "table reference requires a local path; catalogs and object stores are not admitted",
            ));
        }
        PathBuf::from(location)
    };
    if path.as_os_str().is_empty() {
        return Err(failed("empty table file reference"));
    }
    let path = if path.is_absolute() {
        path
    } else {
        root.join(path)
    };
    // Canonical identity deduplicates URI/path aliases and checks residency now.
    path.canonicalize().map_err(|error| {
        failed(format!(
            "referenced local table file {} is unavailable: {error}",
            path.display()
        ))
    })
}

pub(super) fn percent_decode(value: &str) -> Result<String> {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(value.len()).map_err(failed)?;
    let mut input = value.as_bytes().iter().copied();
    while let Some(byte) = input.next() {
        bytes.push(if byte == b'%' {
            let high = input.next().and_then(|n| char::from(n).to_digit(16));
            let low = input.next().and_then(|n| char::from(n).to_digit(16));
            u8::try_from(
                high.zip(low)
                    .map(|(h, l)| h * 16 + l)
                    .ok_or_else(|| failed("invalid percent-encoded local path"))?,
            )
            .map_err(failed)?
        } else {
            byte
        });
    }
    if bytes.contains(&0) {
        return Err(failed("local table path contains a NUL byte"));
    }
    String::from_utf8(bytes).map_err(failed)
}

pub(super) fn object(value: &Value) -> Result<&serde_json::Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| failed("table metadata record must be an object"))
}
pub(super) fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| failed(format!("table metadata requires string {key}")))
}
pub(super) fn integer(value: &Value, key: &str) -> Result<i64> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| failed(format!("table metadata requires integer {key}")))
}
pub(super) fn unsigned(value: &Value, key: &str) -> Result<u64> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| failed(format!("table metadata requires nonnegative integer {key}")))
}
pub(super) fn array<'a>(value: &'a Value, key: &str) -> Result<&'a [Value]> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| failed(format!("table metadata requires array {key}")))
}

fn record(batch: &RecordBatch, row: usize) -> Result<Value> {
    let mut object = serde_json::Map::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        object.insert(field.name().clone(), value(column.as_ref(), row)?);
    }
    Ok(Value::Object(object))
}

#[allow(clippy::too_many_lines)]
fn value(array: &dyn Array, row: usize) -> Result<Value> {
    use arrow_array::{
        BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Decimal128Array,
        FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
        Int64Array, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, MapArray,
        StringArray, StringViewArray, StructArray, Time64MicrosecondArray,
        TimestampMicrosecondArray, UInt32Array, UInt64Array,
    };
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    macro_rules! scalar {
        ($ty:ty) => {
            if let Some(values) = array.as_any().downcast_ref::<$ty>() {
                return Ok(Value::from(values.value(row)));
            }
        };
    }
    scalar!(BooleanArray);
    scalar!(Int8Array);
    scalar!(Int16Array);
    scalar!(Int32Array);
    scalar!(Int64Array);
    scalar!(UInt32Array);
    scalar!(UInt64Array);
    scalar!(StringArray);
    scalar!(LargeStringArray);
    scalar!(StringViewArray);
    // Partition equivalence compares IEEE bits with canonical NaNs, including
    // signed zero. JSON numbers would erase this identity.
    if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
        let v = values.value(row);
        return Ok(serde_json::json!({"f32": if v.is_nan() { 0x7fc0_0000 } else { v.to_bits() }}));
    }
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        let v = values.value(row);
        return Ok(
            serde_json::json!({"f64": if v.is_nan() { 0x7ff8_0000_0000_0000 } else { v.to_bits() }}),
        );
    }
    if let Some(values) = array.as_any().downcast_ref::<StructArray>() {
        let mut object = serde_json::Map::new();
        for (field, column) in values.fields().iter().zip(values.columns()) {
            object.insert(field.name().clone(), value(column.as_ref(), row)?);
        }
        return Ok(Value::Object(object));
    }
    if let Some(values) = array.as_any().downcast_ref::<MapArray>() {
        let entry = values.value(row);
        let mut object = serde_json::Map::new();
        for index in 0..entry.len() {
            let key = value(entry.column(0).as_ref(), index)?;
            let key = match key {
                Value::String(key) => key,
                Value::Number(key) => key.to_string(),
                _ => return Err(failed("table control map key must be a string or integer")),
            };
            if object
                .insert(key, value(entry.column(1).as_ref(), index)?)
                .is_some()
            {
                return Err(failed("table control map contains a duplicate key"));
            }
        }
        return Ok(Value::Object(object));
    }
    if let Some(values) = array.as_any().downcast_ref::<ListArray>() {
        let values = values.value(row);
        return (0..values.len())
            .map(|row| value(values.as_ref(), row))
            .collect::<Result<Vec<_>>>()
            .map(Value::Array);
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeListArray>() {
        let values = values.value(row);
        return (0..values.len())
            .map(|row| value(values.as_ref(), row))
            .collect::<Result<Vec<_>>>()
            .map(Value::Array);
    }
    macro_rules! binary {
        ($ty:ty) => {
            if let Some(values) = array.as_any().downcast_ref::<$ty>() {
                return Ok(Value::Array(
                    values
                        .value(row)
                        .iter()
                        .map(|&byte| Value::from(byte))
                        .collect(),
                ));
            }
        };
    }
    binary!(BinaryArray);
    binary!(LargeBinaryArray);
    binary!(BinaryViewArray);
    binary!(FixedSizeBinaryArray);
    // Avro logical partition values retain exact storage ticks.
    scalar!(Date32Array);
    scalar!(Time64MicrosecondArray);
    scalar!(TimestampMicrosecondArray);
    if let Some(values) = array.as_any().downcast_ref::<Decimal128Array>() {
        return Ok(Value::String(values.value(row).to_string()));
    }
    Err(failed(format!(
        "unsupported table control field type {:?}",
        array.data_type()
    )))
}

fn json_bytes(value: &Value) -> Result<u64> {
    let extra = match value {
        Value::String(value) => value.capacity() as u64,
        Value::Array(values) => {
            values
                .iter()
                .try_fold(values.capacity() as u64 * 32, |sum, value| {
                    sum.checked_add(json_bytes(value)?)
                        .ok_or_else(|| failed("JSON capacity overflow"))
                })?
        }
        Value::Object(values) => {
            values
                .iter()
                .try_fold(values.len() as u64 * 128, |sum, (key, value)| {
                    sum.checked_add(key.capacity() as u64 + json_bytes(value)?)
                        .ok_or_else(|| failed("JSON capacity overflow"))
                })?
        }
        _ => 0,
    };
    extra
        .checked_add(32)
        .ok_or_else(|| failed("JSON capacity overflow"))
}

pub(super) fn scalar_bytes(value: &ScalarValue) -> u64 {
    128 + match value {
        ScalarValue::Utf8(value) => value.capacity() as u64,
        ScalarValue::Binary(value) => value.capacity() as u64,
        _ => 0,
    }
}
