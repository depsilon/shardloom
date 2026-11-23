use std::collections::{BTreeMap, HashSet};

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type, UInt32Type, UInt64Type};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Schema};
use parquet::basic::Encoding;
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData};
use parquet::file::statistics::Statistics;
use shardloom_core::ScalarValue;

/// Maximum number of rows inspected from the first batch / admitted source
/// prefix when building content-driven column profiles.
pub const MAX_PROFILE_SAMPLE_ROWS: usize = 1024;

/// Maximum number of distinct FNV-1a hashes tracked per column while
/// estimating sample cardinality so profiling memory stays strictly bounded.
const MAX_DISTINCT_HASH_SAMPLE: usize = 256;

/// Inclusive Unix epoch-seconds window (2001-09-09T01:46:40Z ..=
/// 2100-01-01T00:00:00Z) used to recognize integer timestamp-seconds columns
/// without relying on column names.
const EPOCH_SECONDS_MIN: i64 = 1_000_000_000;
const EPOCH_SECONDS_MAX: i64 = 4_102_444_800;

/// Minimum non-empty UTF-8 byte length that triggers varied/long UTF-8 derived
/// indexing even when sampled lengths happen to match.
const LONG_UTF8_BYTE_THRESHOLD: usize = 16;

#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, PartialEq)]
pub struct FirstBatchColumnProfile {
    pub name: String,
    pub arrow_dtype: DataType,
    pub row_count: usize,
    pub null_count: usize,
    pub non_null_count: usize,
    pub min_byte_len: Option<usize>,
    pub max_byte_len: Option<usize>,
    pub looks_like_url: bool,
    pub has_varied_or_long_utf8: bool,
    pub is_native_timestamp: bool,
    pub looks_like_timestamp_text: bool,
    pub looks_like_epoch_seconds: bool,
    pub distinct_count_sample: usize,
    pub distinct_ratio: f64,
    pub dictionary_candidate: bool,
    pub parquet_dictionary_encoded: bool,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct FirstBatchProfile {
    pub sample_rows: usize,
    pub columns: Vec<FirstBatchColumnProfile>,
}

impl FirstBatchProfile {
    #[must_use]
    pub fn column(&self, name: &str) -> Option<&FirstBatchColumnProfile> {
        self.columns.iter().find(|column| column.name == name)
    }

    #[must_use]
    pub fn from_record_batch(batch: &RecordBatch) -> Self {
        let mut builder = FirstBatchProfileBuilder::new(batch.schema_ref().as_ref());
        builder.observe_record_batch(batch);
        builder.finish()
    }

    #[must_use]
    pub fn from_scalar_rows(schema: &Schema, rows: &[Vec<Option<ScalarValue>>]) -> Self {
        let mut builder = FirstBatchProfileBuilder::new(schema);
        for row in rows.iter().take(MAX_PROFILE_SAMPLE_ROWS) {
            builder.observe_scalar_row(row);
        }
        builder.finish()
    }

    #[must_use]
    pub fn from_named_scalar_rows(schema: &Schema, rows: &[Vec<(String, ScalarValue)>]) -> Self {
        let mut builder = DynamicFirstBatchProfileBuilder::new();
        for row in rows.iter().take(MAX_PROFILE_SAMPLE_ROWS) {
            builder.observe_named_row(row);
        }
        builder.finish(schema)
    }

    #[must_use]
    pub fn from_parquet_metadata(schema: &Schema, metadata: Option<&ParquetMetaData>) -> Self {
        let Some(metadata) = metadata else {
            return Self::empty_for_schema(schema, 0);
        };
        let total_rows = usize::try_from(metadata.file_metadata().num_rows()).unwrap_or(0);
        let mut acc = ParquetMetadataProfileAccumulator::default();
        for rg_idx in 0..metadata.num_row_groups() {
            let rg = metadata.row_group(rg_idx);
            let rg_rows = usize::try_from(rg.num_rows()).unwrap_or(0);
            for col_idx in 0..rg.num_columns() {
                acc.observe_column_chunk(schema, rg.column(col_idx), rg_rows);
            }
        }
        acc.finish(schema, total_rows)
    }

    fn empty_for_schema(schema: &Schema, row_count: usize) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                let dtype = field.data_type().clone();
                let is_native_timestamp = matches!(dtype, DataType::Timestamp(_, _));
                FirstBatchColumnProfile {
                    name: field.name().clone(),
                    arrow_dtype: dtype,
                    row_count,
                    null_count: 0,
                    non_null_count: 0,
                    min_byte_len: None,
                    max_byte_len: None,
                    looks_like_url: false,
                    has_varied_or_long_utf8: false,
                    is_native_timestamp,
                    looks_like_timestamp_text: false,
                    looks_like_epoch_seconds: false,
                    distinct_count_sample: 0,
                    distinct_ratio: 0.0,
                    dictionary_candidate: false,
                    parquet_dictionary_encoded: false,
                }
            })
            .collect();
        Self {
            sample_rows: 0,
            columns,
        }
    }
}

#[derive(Default)]
struct ParquetMetadataProfileAccumulator {
    dict: BTreeMap<String, bool>,
    url: BTreeMap<String, bool>,
    min_len: BTreeMap<String, usize>,
    max_len: BTreeMap<String, usize>,
    varied_or_long: BTreeMap<String, bool>,
    ts_text_ok: BTreeMap<String, bool>,
    epoch_ok: BTreeMap<String, bool>,
    null_count: BTreeMap<String, usize>,
    observed_non_null_rows: BTreeMap<String, usize>,
}

impl ParquetMetadataProfileAccumulator {
    fn observe_column_chunk(&mut self, schema: &Schema, col: &ColumnChunkMetaData, rg_rows: usize) {
        let path_parts = col.column_path().parts();
        if path_parts.len() != 1 {
            return;
        }
        let name = &path_parts[0];
        let field_dtype = schema
            .field_with_name(name)
            .ok()
            .map(arrow_schema::Field::data_type);
        if col.encodings().any(|encoding| {
            matches!(
                encoding,
                Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY
            )
        }) {
            self.dict.insert(name.clone(), true);
        }
        let Some(stats) = col.statistics() else {
            if matches!(
                field_dtype,
                Some(DataType::Int32 | DataType::Int64 | DataType::UInt32 | DataType::UInt64)
            ) {
                self.epoch_ok.insert(name.clone(), false);
            }
            return;
        };
        let chunk_nulls = stats
            .null_count_opt()
            .and_then(|n| usize::try_from(n).ok())
            .unwrap_or(0);
        let chunk_non_nulls = rg_rows.saturating_sub(chunk_nulls);
        *self.null_count.entry(name.clone()).or_insert(0) += chunk_nulls;
        *self.observed_non_null_rows.entry(name.clone()).or_insert(0) += chunk_non_nulls;
        if chunk_non_nulls == 0 && rg_rows > 0 {
            return;
        }
        self.observe_statistics(name, field_dtype, stats);
    }

    fn observe_statistics(
        &mut self,
        name: &str,
        field_dtype: Option<&DataType>,
        stats: &Statistics,
    ) {
        match (stats, field_dtype) {
            (Statistics::Int64(s), Some(DataType::Int64)) => {
                let valid = s
                    .min_opt()
                    .zip(s.max_opt())
                    .is_some_and(|(&min_v, &max_v)| {
                        (EPOCH_SECONDS_MIN..=EPOCH_SECONDS_MAX).contains(&min_v)
                            && (EPOCH_SECONDS_MIN..=EPOCH_SECONDS_MAX).contains(&max_v)
                    });
                let entry = self.epoch_ok.entry(name.to_owned()).or_insert(true);
                *entry &= valid;
            }
            (Statistics::Int64(s), Some(DataType::UInt64)) => {
                let valid = s
                    .min_opt()
                    .zip(s.max_opt())
                    .is_some_and(|(&min_v, &max_v)| {
                        let min_u = u64::from_ne_bytes(min_v.to_ne_bytes());
                        let max_u = u64::from_ne_bytes(max_v.to_ne_bytes());
                        u64::try_from(EPOCH_SECONDS_MIN)
                            .ok()
                            .zip(u64::try_from(EPOCH_SECONDS_MAX).ok())
                            .is_some_and(|(lo, hi)| {
                                (lo..=hi).contains(&min_u) && (lo..=hi).contains(&max_u)
                            })
                    });
                let entry = self.epoch_ok.entry(name.to_owned()).or_insert(true);
                *entry &= valid;
            }
            (Statistics::Int32(s), Some(DataType::Int32)) => {
                let valid = s
                    .min_opt()
                    .zip(s.max_opt())
                    .is_some_and(|(&min_v, &max_v)| {
                        (EPOCH_SECONDS_MIN..=EPOCH_SECONDS_MAX).contains(&i64::from(min_v))
                            && (EPOCH_SECONDS_MIN..=EPOCH_SECONDS_MAX).contains(&i64::from(max_v))
                    });
                let entry = self.epoch_ok.entry(name.to_owned()).or_insert(true);
                *entry &= valid;
            }
            (Statistics::Int32(s), Some(DataType::UInt32)) => {
                let valid = s
                    .min_opt()
                    .zip(s.max_opt())
                    .is_some_and(|(&min_v, &max_v)| {
                        let min_u = i64::from(u32::from_ne_bytes(min_v.to_ne_bytes()));
                        let max_u = i64::from(u32::from_ne_bytes(max_v.to_ne_bytes()));
                        (EPOCH_SECONDS_MIN..=EPOCH_SECONDS_MAX).contains(&min_u)
                            && (EPOCH_SECONDS_MIN..=EPOCH_SECONDS_MAX).contains(&max_u)
                    });
                let entry = self.epoch_ok.entry(name.to_owned()).or_insert(true);
                *entry &= valid;
            }
            (Statistics::ByteArray(s), Some(dtype)) if is_utf8_or_dict_utf8(dtype) => {
                let min_s = s.min_opt().and_then(|v| std::str::from_utf8(v.data()).ok());
                let max_s = s.max_opt().and_then(|v| std::str::from_utf8(v.data()).ok());
                if min_s.is_some_and(looks_like_url_str) || max_s.is_some_and(looks_like_url_str) {
                    self.url.insert(name.to_owned(), true);
                    self.varied_or_long.insert(name.to_owned(), true);
                }
                for sample in [min_s, max_s].into_iter().flatten() {
                    let len = sample.len();
                    self.min_len
                        .entry(name.to_owned())
                        .and_modify(|prev| *prev = (*prev).min(len))
                        .or_insert(len);
                    self.max_len
                        .entry(name.to_owned())
                        .and_modify(|prev| *prev = (*prev).max(len))
                        .or_insert(len);
                    if len >= LONG_UTF8_BYTE_THRESHOLD {
                        self.varied_or_long.insert(name.to_owned(), true);
                    }
                }
                if let (Some(a), Some(b)) = (min_s, max_s) {
                    if a.len().abs_diff(b.len()) >= 4 {
                        self.varied_or_long.insert(name.to_owned(), true);
                    }
                    let ts_valid = super::parse_timestamp_minute(a).is_some()
                        && super::parse_timestamp_minute(b).is_some();
                    let entry = self.ts_text_ok.entry(name.to_owned()).or_insert(true);
                    *entry &= ts_valid;
                }
            }
            _ => {}
        }
    }

    fn finish(self, schema: &Schema, total_rows: usize) -> FirstBatchProfile {
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                let name = field.name().clone();
                let dtype = field.data_type().clone();
                let is_native_timestamp = matches!(dtype, DataType::Timestamp(_, _));
                let null_count = self.null_count.get(&name).copied().unwrap_or(0);
                let non_null_count = self
                    .observed_non_null_rows
                    .get(&name)
                    .copied()
                    .unwrap_or_else(|| total_rows.saturating_sub(null_count));
                let looks_like_url = self.url.get(&name).copied().unwrap_or(false);
                let min_byte_len = self.min_len.get(&name).copied();
                let max_byte_len = self.max_len.get(&name).copied();
                let has_varied_or_long_utf8 =
                    self.varied_or_long.get(&name).copied().unwrap_or(false)
                        || looks_like_url
                        || max_byte_len
                            .zip(min_byte_len)
                            .is_some_and(|(max_l, min_l)| max_l.saturating_sub(min_l) >= 4)
                        || max_byte_len.is_some_and(|m| m >= LONG_UTF8_BYTE_THRESHOLD);
                let looks_like_timestamp_text =
                    non_null_count > 0 && self.ts_text_ok.get(&name).copied().unwrap_or(false);
                let looks_like_epoch_seconds =
                    non_null_count > 0 && self.epoch_ok.get(&name).copied().unwrap_or(false);
                let parquet_dictionary_encoded = self.dict.get(&name).copied().unwrap_or(false);
                FirstBatchColumnProfile {
                    name,
                    arrow_dtype: dtype,
                    row_count: total_rows,
                    null_count,
                    non_null_count,
                    min_byte_len,
                    max_byte_len,
                    looks_like_url,
                    has_varied_or_long_utf8,
                    is_native_timestamp,
                    looks_like_timestamp_text,
                    looks_like_epoch_seconds,
                    distinct_count_sample: 0,
                    distinct_ratio: if non_null_count > 0 { 1.0 } else { 0.0 },
                    dictionary_candidate: parquet_dictionary_encoded,
                    parquet_dictionary_encoded,
                }
            })
            .collect();

        FirstBatchProfile {
            sample_rows: total_rows.min(MAX_PROFILE_SAMPLE_ROWS),
            columns,
        }
    }
}

pub struct FirstBatchProfileBuilder {
    sample_rows: usize,
    columns: Vec<ColumnAccumulator>,
}

/// Incremental profiler keyed by column name for text sources (`CSV`, `JSONL`,
/// `JSON`) where the full schema is still being discovered during the single
/// admission pass.
#[derive(Default)]
pub struct DynamicFirstBatchProfileBuilder {
    sample_rows: usize,
    columns: BTreeMap<String, ColumnAccumulator>,
}

impl DynamicFirstBatchProfileBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe_named_row(&mut self, values: &[(String, ScalarValue)]) {
        if self.sample_rows >= MAX_PROFILE_SAMPLE_ROWS {
            return;
        }
        self.sample_rows += 1;
        for (name, value) in values {
            let acc = self
                .columns
                .entry(name.clone())
                .or_insert_with(|| ColumnAccumulator::new(name.clone(), DataType::Null));
            acc.observe_scalar_value(value);
        }
    }

    #[must_use]
    pub fn finish(mut self, schema: &Schema) -> FirstBatchProfile {
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                let mut profile = self
                    .columns
                    .remove(field.name())
                    .unwrap_or_else(|| {
                        ColumnAccumulator::new(field.name().clone(), field.data_type().clone())
                    })
                    .finish();
                let null_shortfall = self.sample_rows.saturating_sub(profile.row_count);
                profile.row_count = self.sample_rows;
                profile.null_count = profile.null_count.saturating_add(null_shortfall);
                profile.arrow_dtype = field.data_type().clone();
                profile.is_native_timestamp =
                    matches!(profile.arrow_dtype, DataType::Timestamp(_, _));
                profile
            })
            .collect();
        FirstBatchProfile {
            sample_rows: self.sample_rows,
            columns,
        }
    }
}

struct ColumnAccumulator {
    name: String,
    dtype: DataType,
    row_count: usize,
    null_count: usize,
    non_null_count: usize,
    min_byte_len: Option<usize>,
    max_byte_len: Option<usize>,
    url_hits: usize,
    timestamp_hits: usize,
    timestamp_failures: usize,
    epoch_hits: usize,
    epoch_failures: usize,
    distinct_hashes: HashSet<u64>,
    distinct_saturated: bool,
}

impl ColumnAccumulator {
    fn new(name: String, dtype: DataType) -> Self {
        Self {
            name,
            dtype,
            row_count: 0,
            null_count: 0,
            non_null_count: 0,
            min_byte_len: None,
            max_byte_len: None,
            url_hits: 0,
            timestamp_hits: 0,
            timestamp_failures: 0,
            epoch_hits: 0,
            epoch_failures: 0,
            distinct_hashes: HashSet::new(),
            distinct_saturated: false,
        }
    }

    fn record_hash(&mut self, bytes: &[u8]) {
        if self.distinct_saturated {
            return;
        }
        if self.distinct_hashes.len() >= MAX_DISTINCT_HASH_SAMPLE {
            self.distinct_saturated = true;
            return;
        }
        self.distinct_hashes.insert(fnv1a64(bytes));
    }

    fn observe_null(&mut self) {
        self.row_count += 1;
        self.null_count += 1;
    }

    fn observe_utf8(&mut self, value: &str) {
        self.row_count += 1;
        self.non_null_count += 1;
        let len = value.len();
        self.min_byte_len = Some(self.min_byte_len.map_or(len, |prev| prev.min(len)));
        self.max_byte_len = Some(self.max_byte_len.map_or(len, |prev| prev.max(len)));
        self.record_hash(value.as_bytes());
        if looks_like_url_str(value) {
            self.url_hits += 1;
        }
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            if super::parse_timestamp_minute(trimmed).is_some() {
                self.timestamp_hits += 1;
            } else {
                self.timestamp_failures += 1;
            }
        }
    }

    fn observe_signed_int(&mut self, value: i64) {
        self.row_count += 1;
        self.non_null_count += 1;
        self.record_hash(&value.to_le_bytes());
        if (EPOCH_SECONDS_MIN..=EPOCH_SECONDS_MAX).contains(&value) {
            self.epoch_hits += 1;
        } else {
            self.epoch_failures += 1;
        }
    }

    fn observe_unsigned_int(&mut self, value: u64) {
        self.row_count += 1;
        self.non_null_count += 1;
        self.record_hash(&value.to_le_bytes());
        if u64::try_from(EPOCH_SECONDS_MIN)
            .ok()
            .zip(u64::try_from(EPOCH_SECONDS_MAX).ok())
            .is_some_and(|(lo, hi)| (lo..=hi).contains(&value))
        {
            self.epoch_hits += 1;
        } else {
            self.epoch_failures += 1;
        }
    }

    fn observe_other_non_null(&mut self, bytes: &[u8]) {
        self.row_count += 1;
        self.non_null_count += 1;
        self.record_hash(bytes);
    }

    fn observe_scalar_value(&mut self, value: &ScalarValue) {
        match value {
            ScalarValue::Null => self.observe_null(),
            ScalarValue::Utf8(s) => self.observe_utf8(s),
            ScalarValue::Int64(v) => self.observe_signed_int(*v),
            ScalarValue::UInt64(v) => self.observe_unsigned_int(*v),
            ScalarValue::TimestampMicros(v) => {
                self.observe_other_non_null(&v.to_le_bytes());
            }
            ScalarValue::Boolean(b) => self.observe_other_non_null(&[u8::from(*b)]),
            ScalarValue::Float64(f) => self.observe_other_non_null(&f.to_le_bytes()),
            ScalarValue::Binary(b) => self.observe_other_non_null(b),
            other => self.observe_other_non_null(format!("{other:?}").as_bytes()),
        }
    }

    fn finish(self) -> FirstBatchColumnProfile {
        let is_native_timestamp = matches!(self.dtype, DataType::Timestamp(_, _));
        let looks_like_url = self.url_hits > 0;
        let has_varied_or_long_utf8 = self.non_null_count > 0
            && (looks_like_url
                || self
                    .max_byte_len
                    .is_some_and(|m| m >= LONG_UTF8_BYTE_THRESHOLD)
                || self
                    .max_byte_len
                    .zip(self.min_byte_len)
                    .is_some_and(|(max_l, min_l)| max_l.saturating_sub(min_l) >= 4));
        let looks_like_timestamp_text = self.timestamp_hits > 0 && self.timestamp_failures == 0;
        let looks_like_epoch_seconds = self.epoch_hits > 0 && self.epoch_failures == 0;
        let distinct_count_sample = self.distinct_hashes.len();
        let distinct_ratio = if self.non_null_count > 0 {
            f64::from(u32::try_from(distinct_count_sample).unwrap_or(u32::MAX))
                / f64::from(u32::try_from(self.non_null_count).unwrap_or(u32::MAX))
        } else {
            0.0
        };
        let dictionary_candidate = !self.distinct_saturated
            && self.non_null_count >= 2
            && distinct_ratio <= 0.5
            && distinct_count_sample <= 128;

        FirstBatchColumnProfile {
            name: self.name,
            arrow_dtype: self.dtype,
            row_count: self.row_count,
            null_count: self.null_count,
            non_null_count: self.non_null_count,
            min_byte_len: self.min_byte_len,
            max_byte_len: self.max_byte_len,
            looks_like_url,
            has_varied_or_long_utf8,
            is_native_timestamp,
            looks_like_timestamp_text,
            looks_like_epoch_seconds,
            distinct_count_sample,
            distinct_ratio,
            dictionary_candidate,
            parquet_dictionary_encoded: false,
        }
    }
}

impl FirstBatchProfileBuilder {
    #[must_use]
    pub fn new(schema: &Schema) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|field| ColumnAccumulator::new(field.name().clone(), field.data_type().clone()))
            .collect();
        Self {
            sample_rows: 0,
            columns,
        }
    }

    #[must_use]
    pub fn is_full(&self) -> bool {
        self.sample_rows >= MAX_PROFILE_SAMPLE_ROWS
    }

    #[must_use]
    pub fn sample_rows(&self) -> usize {
        self.sample_rows
    }

    pub fn observe_scalar_row(&mut self, values: &[Option<ScalarValue>]) {
        if self.is_full() {
            return;
        }
        self.sample_rows += 1;
        for (acc, value) in self.columns.iter_mut().zip(values.iter()) {
            match value {
                None => acc.observe_null(),
                Some(scalar) => acc.observe_scalar_value(scalar),
            }
        }
    }

    pub fn observe_record_batch(&mut self, batch: &RecordBatch) {
        if self.is_full() || batch.num_rows() == 0 {
            return;
        }
        let take = batch
            .num_rows()
            .min(MAX_PROFILE_SAMPLE_ROWS - self.sample_rows);
        self.sample_rows += take;
        for (acc, col) in self.columns.iter_mut().zip(batch.columns().iter()) {
            observe_arrow_array(acc, col, take);
        }
    }

    #[must_use]
    pub fn finish(self) -> FirstBatchProfile {
        FirstBatchProfile {
            sample_rows: self.sample_rows,
            columns: self
                .columns
                .into_iter()
                .map(ColumnAccumulator::finish)
                .collect(),
        }
    }
}

fn observe_arrow_array(acc: &mut ColumnAccumulator, array: &ArrayRef, take: usize) {
    match array.data_type() {
        DataType::Utf8 => {
            let typed = array.as_string::<i32>();
            for row in 0..take {
                if typed.is_null(row) {
                    acc.observe_null();
                } else {
                    acc.observe_utf8(typed.value(row));
                }
            }
        }
        DataType::LargeUtf8 => {
            let typed = array.as_string::<i64>();
            for row in 0..take {
                if typed.is_null(row) {
                    acc.observe_null();
                } else {
                    acc.observe_utf8(typed.value(row));
                }
            }
        }
        DataType::Utf8View => {
            let typed = array.as_string_view();
            for row in 0..take {
                if typed.is_null(row) {
                    acc.observe_null();
                } else {
                    acc.observe_utf8(typed.value(row));
                }
            }
        }
        DataType::Dictionary(key_dtype, value_dtype) if is_utf8_like(value_dtype) => {
            observe_utf8_dictionary(acc, array, key_dtype, take);
        }
        DataType::Int64 => {
            let typed = array.as_primitive::<Int64Type>();
            for row in 0..take {
                if typed.is_null(row) {
                    acc.observe_null();
                } else {
                    acc.observe_signed_int(typed.value(row));
                }
            }
        }
        DataType::UInt64 => {
            let typed = array.as_primitive::<UInt64Type>();
            for row in 0..take {
                if typed.is_null(row) {
                    acc.observe_null();
                } else {
                    acc.observe_unsigned_int(typed.value(row));
                }
            }
        }
        DataType::Int32 => {
            let typed = array.as_primitive::<Int32Type>();
            for row in 0..take {
                if typed.is_null(row) {
                    acc.observe_null();
                } else {
                    acc.observe_signed_int(i64::from(typed.value(row)));
                }
            }
        }
        DataType::UInt32 => {
            let typed = array.as_primitive::<UInt32Type>();
            for row in 0..take {
                if typed.is_null(row) {
                    acc.observe_null();
                } else {
                    acc.observe_unsigned_int(u64::from(typed.value(row)));
                }
            }
        }
        _ => {
            for row in 0..take {
                if array.is_null(row) {
                    acc.observe_null();
                } else {
                    acc.observe_other_non_null(&row.to_le_bytes());
                }
            }
        }
    }
}

fn observe_utf8_dictionary(
    acc: &mut ColumnAccumulator,
    array: &ArrayRef,
    key_dtype: &DataType,
    take: usize,
) {
    macro_rules! scan_dict {
        ($key_ty:ty) => {{
            let dict = array.as_dictionary::<$key_ty>();
            let values = dict.values();
            for row in 0..take {
                if dict.is_null(row) {
                    acc.observe_null();
                    continue;
                }
                let key = usize::try_from(dict.keys().value(row)).unwrap_or(usize::MAX);
                let text = match values.data_type() {
                    DataType::Utf8 => values
                        .as_string::<i32>()
                        .is_valid(key)
                        .then(|| values.as_string::<i32>().value(key)),
                    DataType::LargeUtf8 => values
                        .as_string::<i64>()
                        .is_valid(key)
                        .then(|| values.as_string::<i64>().value(key)),
                    DataType::Utf8View => values
                        .as_string_view()
                        .is_valid(key)
                        .then(|| values.as_string_view().value(key)),
                    _ => None,
                };
                match text {
                    Some(s) => acc.observe_utf8(s),
                    None => acc.observe_null(),
                }
            }
        }};
    }

    match key_dtype {
        DataType::Int8 => scan_dict!(arrow_array::types::Int8Type),
        DataType::Int16 => scan_dict!(arrow_array::types::Int16Type),
        DataType::Int32 => scan_dict!(Int32Type),
        DataType::Int64 => scan_dict!(Int64Type),
        DataType::UInt8 => scan_dict!(arrow_array::types::UInt8Type),
        DataType::UInt16 => scan_dict!(arrow_array::types::UInt16Type),
        DataType::UInt32 => scan_dict!(UInt32Type),
        DataType::UInt64 => scan_dict!(UInt64Type),
        _ => {
            for row in 0..take {
                if array.is_null(row) {
                    acc.observe_null();
                } else {
                    acc.observe_other_non_null(&row.to_le_bytes());
                }
            }
        }
    }
}

fn is_utf8_like(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_utf8_or_dict_utf8(dtype: &DataType) -> bool {
    match dtype {
        DataType::Dictionary(_, value) => is_utf8_like(value),
        _ => is_utf8_like(dtype),
    }
}

pub(crate) fn looks_like_url_str(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return false;
    }
    let Some(pos) = trimmed.find("://") else {
        return false;
    };
    let (scheme, rest) = trimmed.split_at(pos);
    let after = &rest[3..];
    if after.is_empty() {
        return false;
    }
    matches!(
        scheme.to_ascii_lowercase().as_str(),
        "http" | "https" | "ftp" | "ws" | "wss"
    )
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampSecondArray, UInt64Array};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use shardloom_core::ScalarValue;

    use super::{FirstBatchProfile, MAX_PROFILE_SAMPLE_ROWS};

    #[test]
    fn first_batch_profile_detects_urls_timestamps_epoch_seconds_and_dict_candidates() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Utf8, true),
            Field::new("col_b", DataType::Utf8, true),
            Field::new("col_c", DataType::Utf8, true),
            Field::new("col_d", DataType::Int64, true),
            Field::new("col_e", DataType::UInt64, true),
            Field::new("col_f", DataType::Timestamp(TimeUnit::Second, None), true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("https://example.com/one"),
                    Some("https://example.com/one"),
                    None,
                    Some("http://docs.rs/arrow/index.html"),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("2024-05-01T12:34:56Z"),
                    Some("2024-05-01T12:35:00Z"),
                    None,
                    Some("2024-05-01 12:36:10"),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("abc"),
                    Some("def"),
                    Some("ghi"),
                    Some("jkl"),
                ])),
                Arc::new(Int64Array::from(vec![
                    Some(1_700_000_040),
                    Some(1_700_000_100),
                    None,
                    Some(1_700_000_160),
                ])),
                Arc::new(UInt64Array::from(vec![
                    Some(1),
                    Some(200),
                    Some(42),
                    Some(99_999_999_999),
                ])),
                Arc::new(TimestampSecondArray::from(vec![
                    Some(10),
                    None,
                    Some(20),
                    Some(30),
                ])),
            ],
        )
        .expect("record batch");

        let profile = FirstBatchProfile::from_record_batch(&batch);
        assert_eq!(profile.sample_rows, 4);

        let col_a = profile.column("col_a").expect("col_a");
        assert!(col_a.looks_like_url);
        assert!(col_a.has_varied_or_long_utf8);
        assert!(!col_a.looks_like_timestamp_text);

        let col_b = profile.column("col_b").expect("col_b");
        assert!(!col_b.looks_like_url);
        assert!(col_b.looks_like_timestamp_text);
        assert!(col_b.has_varied_or_long_utf8);

        let col_c = profile.column("col_c").expect("col_c");
        assert!(!col_c.looks_like_url);
        assert!(!col_c.has_varied_or_long_utf8);
        assert!(!col_c.looks_like_timestamp_text);
        assert!(!col_c.dictionary_candidate);

        let col_d = profile.column("col_d").expect("col_d");
        assert!(col_d.looks_like_epoch_seconds);

        let col_e = profile.column("col_e").expect("col_e");
        assert!(!col_e.looks_like_epoch_seconds);

        let col_f = profile.column("col_f").expect("col_f");
        assert!(col_f.is_native_timestamp);
    }

    #[test]
    fn misnamed_clickbench_columns_without_matching_content_are_not_flagged() {
        let schema = Schema::new(vec![
            Field::new("URL", DataType::Utf8, false),
            Field::new("EventTime", DataType::Int64, false),
            Field::new("Referer", DataType::Utf8, false),
        ]);
        let rows = vec![
            vec![
                Some(ScalarValue::Utf8("blue".to_string())),
                Some(ScalarValue::Int64(42)),
                Some(ScalarValue::Utf8("cyan".to_string())),
            ],
            vec![
                Some(ScalarValue::Utf8("pink".to_string())),
                Some(ScalarValue::Int64(99)),
                Some(ScalarValue::Utf8("gold".to_string())),
            ],
        ];
        let profile = FirstBatchProfile::from_scalar_rows(&schema, &rows);
        assert!(!profile.column("URL").unwrap().looks_like_url);
        assert!(!profile.column("URL").unwrap().has_varied_or_long_utf8);
        assert!(
            !profile
                .column("EventTime")
                .unwrap()
                .looks_like_epoch_seconds
        );
        assert!(!profile.column("Referer").unwrap().looks_like_url);
    }

    #[test]
    fn scalar_rows_profile_caps_sample_at_max_rows_and_tracks_dictionary_candidate() {
        let schema = Schema::new(vec![Field::new("tag", DataType::Utf8, false)]);
        let rows: Vec<Vec<Option<ScalarValue>>> = (0..MAX_PROFILE_SAMPLE_ROWS + 50)
            .map(|i| vec![Some(ScalarValue::Utf8(format!("k{}", i % 3)))])
            .collect();
        let profile = FirstBatchProfile::from_scalar_rows(&schema, &rows);
        assert_eq!(profile.sample_rows, MAX_PROFILE_SAMPLE_ROWS);
        let tag = profile.column("tag").unwrap();
        assert_eq!(tag.distinct_count_sample, 3);
        assert!(tag.dictionary_candidate);
    }

    #[test]
    fn parquet_metadata_profile_handles_plain_and_dictionary_utf8_schemas() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("landing_link", DataType::Utf8, true),
            Field::new("observed_at", DataType::Int64, false),
            Field::new("short_code", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("https://docs.rs/vortex"),
                    Some("https://docs.rs/arrow"),
                    Some("https://docs.rs/vortex"),
                ])),
                Arc::new(Int64Array::from(vec![
                    1_700_000_040,
                    1_700_000_100,
                    1_700_000_160,
                ])),
                Arc::new(StringArray::from(vec!["aa", "bb", "cc"])),
            ],
        )
        .expect("parquet batch");
        let mut bytes = Vec::new();
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_dictionary_enabled(true)
            .build();
        {
            let mut writer =
                ArrowWriter::try_new(&mut bytes, Arc::clone(&schema), Some(props)).expect("writer");
            writer.write(&batch).expect("write");
            writer.close().expect("close");
        }
        let reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).expect("reader");
        let meta = reader_builder.metadata();

        let plain_profile = FirstBatchProfile::from_parquet_metadata(&schema, Some(meta));
        let landing = plain_profile.column("landing_link").expect("landing_link");
        assert!(landing.looks_like_url);
        assert!(landing.has_varied_or_long_utf8);
        assert!(landing.parquet_dictionary_encoded);
        assert!(
            plain_profile
                .column("observed_at")
                .unwrap()
                .looks_like_epoch_seconds
        );
        assert!(!plain_profile.column("short_code").unwrap().looks_like_url);

        let dict_schema = Schema::new(vec![
            Field::new(
                "landing_link",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("observed_at", DataType::Int64, false),
            Field::new("short_code", DataType::Utf8, false),
        ]);
        let dict_profile = FirstBatchProfile::from_parquet_metadata(&dict_schema, Some(meta));
        let landing_dict = dict_profile.column("landing_link").expect("landing_dict");
        assert!(landing_dict.looks_like_url);
        assert!(landing_dict.has_varied_or_long_utf8);
        assert!(landing_dict.parquet_dictionary_encoded);
    }
}
