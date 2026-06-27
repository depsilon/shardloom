//! Feature-gated local compatibility-format I/O for runtime promotion.
//!
//! These helpers are compatibility input adapters, not fallback execution
//! engines. Scoped smoke paths may decode admitted local file formats into
//! `ShardLoom` scalar rows, while product ingest paths preserve typed Arrow
//! `RecordBatch` streams for Vortex writes whenever the source adapter can
//! expose them. Compatibility sinks still encode admitted scalar rows with
//! explicit materialization/write evidence and fail closed for unsupported Arrow
//! types.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs::File,
    io::{BufReader, Write},
    path::Path,
    rc::Rc,
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, SyncSender},
    },
    thread::{self, JoinHandle},
};

use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Decimal128Array,
    DictionaryArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeListArray,
    LargeStringArray, ListArray, PrimitiveArray, RecordBatch, RecordBatchReader, StringArray,
    StringViewArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
    builder::{
        ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
        Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
        TimestampMicrosecondBuilder, UInt8Builder, UInt32Builder, UInt64Builder, make_builder,
    },
    types::{
        ArrowDictionaryKeyType, ArrowPrimitiveType, Int8Type, Int16Type, Int32Type, Int64Type,
        UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_schema::{ArrowError, DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use shardloom_core::{LogicalDType, Result, ScalarValue, ShardLoomError};

const SCOPED_COMPAT_RECORD_BATCH_ROWS: usize = 8_192;
pub const PRODUCT_COLUMNAR_STREAM_RECORD_BATCH_ROWS: usize = 65_536;
pub const PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS: usize = 262_144;
const PRODUCT_COLUMNAR_LARGE_STREAM_ROW_THRESHOLD: usize = 10_000_000;
const PARQUET_PARALLEL_ROW_GROUPS_PER_TASK: usize = 16;
const PARQUET_PARALLEL_MAX_ROW_GROUPS_PER_TASK: usize = 64;
const PARQUET_PARALLEL_TARGET_TASK_BATCH_MULTIPLE: usize = 4;
const PARQUET_ROW_GROUP_RESULT_QUEUE_BATCHES_PER_WORKER: usize = 2;
const PRODUCT_COLUMNAR_STREAM_POLICY: &str = "product_columnar_stream_batch_size_65536_rows";
const PRODUCT_COLUMNAR_LARGE_STREAM_POLICY: &str = "product_columnar_stream_batch_size_262144_rows";

#[derive(Debug, Clone, PartialEq, Eq)]
struct FlatColumnarStreamSourcePlan {
    record_batch_count_hint: Option<usize>,
    source_unit_count_hint: Option<usize>,
    source_unit_row_ranges: Option<Vec<(usize, usize)>>,
    source_unit_hint_kind: &'static str,
    stream_batch_size: usize,
    stream_policy: &'static str,
    dictionary_preservation_status: &'static str,
}

impl FlatColumnarStreamSourcePlan {
    fn source_defined_batches(record_batch_count_hint: usize) -> Self {
        Self {
            record_batch_count_hint: Some(record_batch_count_hint),
            source_unit_count_hint: Some(record_batch_count_hint),
            source_unit_row_ranges: None,
            source_unit_hint_kind: "source_defined_record_batch_count",
            stream_batch_size: 0,
            stream_policy: "source_defined_record_batches",
            dictionary_preservation_status: "source_arrow_batches_preserve_dictionary_arrays_when_present",
        }
    }

    fn product_batches(
        max_rows: usize,
        source_unit_count_hint: Option<usize>,
        source_unit_hint_kind: &'static str,
        dictionary_preservation_status: &'static str,
    ) -> Self {
        let stream_batch_size = product_columnar_stream_record_batch_rows(max_rows);
        Self {
            record_batch_count_hint: None,
            source_unit_count_hint,
            source_unit_row_ranges: None,
            source_unit_hint_kind,
            stream_batch_size,
            stream_policy: product_columnar_stream_policy(stream_batch_size),
            dictionary_preservation_status,
        }
    }
}

fn product_columnar_stream_record_batch_rows(max_rows: usize) -> usize {
    if max_rows == 0 {
        1
    } else if max_rows < PRODUCT_COLUMNAR_STREAM_RECORD_BATCH_ROWS {
        max_rows
    } else if max_rows >= PRODUCT_COLUMNAR_LARGE_STREAM_ROW_THRESHOLD {
        PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS
    } else {
        PRODUCT_COLUMNAR_STREAM_RECORD_BATCH_ROWS
    }
}

#[must_use]
pub const fn product_columnar_stream_policy(stream_batch_size: usize) -> &'static str {
    if stream_batch_size == PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS {
        PRODUCT_COLUMNAR_LARGE_STREAM_POLICY
    } else {
        PRODUCT_COLUMNAR_STREAM_POLICY
    }
}

const fn typed_text_record_batch_stream_policy(stream_batch_size: usize) -> &'static str {
    if stream_batch_size == PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS {
        "typed_text_record_batch_stream_batch_size_262144_rows"
    } else {
        "typed_text_record_batch_stream_batch_size_65536_rows"
    }
}
/// Materialized scalar rows produced by a scoped local compatibility adapter.
#[derive(Debug, Clone, PartialEq)]
pub struct FlatLocalSourceTable {
    /// Column order from the source schema.
    pub header: Vec<String>,
    /// Source-schema dtype hints aligned with `header` for scoped compatibility
    /// output preservation.
    pub column_dtypes: Vec<Option<LogicalDType>>,
    /// Source-schema Arrow dtype hints aligned with `header` for scoped typed
    /// nested output preservation. These hints are canonicalized to the
    /// `ShardLoom` scalar materialization families rather than claimed as exact
    /// source layout preservation.
    pub column_arrow_dtypes: Vec<Option<DataType>>,
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
    /// Source-schema Arrow dtype hints aligned with `header` for scoped typed
    /// nested output preservation. These hints are canonicalized to the
    /// `ShardLoom` scalar materialization families rather than claimed as exact
    /// source layout preservation.
    pub column_arrow_dtypes: Vec<Option<DataType>>,
    /// Column order materialized for the caller.
    pub materialized_columns: Vec<String>,
    /// Column order requested from the underlying local reader.
    pub reader_projection_columns: Vec<String>,
    /// Arrow record batches preserved before scalar-row materialization.
    pub batches: Vec<RecordBatch>,
    /// Total row count across `batches`.
    pub row_count: usize,
}

/// Streaming columnar local compatibility source produced by a scoped local adapter.
///
/// This is the product-ingest shape for columnar compatibility inputs: schema
/// metadata is available up front, while Arrow `RecordBatch` chunks are pulled
/// by the Vortex writer without first accumulating the whole source in memory.
pub struct FlatLocalColumnarStreamSource {
    /// Column order from the source schema.
    pub header: Vec<String>,
    /// Source-schema dtype hints aligned with `header` for scoped compatibility
    /// output preservation.
    pub column_dtypes: Vec<Option<LogicalDType>>,
    /// Source-schema Arrow dtype hints aligned with `header` for scoped typed
    /// nested output preservation.
    pub column_arrow_dtypes: Vec<Option<DataType>>,
    /// Column order materialized for the caller.
    pub materialized_columns: Vec<String>,
    /// Column order requested from the underlying local reader.
    pub reader_projection_columns: Vec<String>,
    /// Source row count when the file format exposes it without scanning.
    pub row_count_hint: Option<usize>,
    /// Reader batch count when the file format exposes it without scanning.
    pub record_batch_count_hint: Option<usize>,
    /// Product stream batch size requested from the source adapter. A value of
    /// zero means the source format owns batch boundaries, such as Arrow IPC.
    pub source_stream_batch_size: usize,
    /// Source-native work-unit count when known before streaming.
    pub source_stream_unit_count_hint: Option<usize>,
    /// Exact source-native work-unit row ranges when known before streaming.
    pub source_stream_unit_row_ranges: Option<Vec<(usize, usize)>>,
    /// Meaning of `source_stream_unit_count_hint`.
    pub source_stream_unit_hint_kind: String,
    /// Stream shaping policy applied before the Vortex writer consumes batches.
    pub source_stream_policy: String,
    /// Whether source dictionaries/typed columnar layouts can survive to the
    /// Vortex provider boundary.
    pub source_dictionary_preservation_status: String,
    /// Source-to-writer executor status for product ingest evidence.
    pub ingest_executor_status: String,
    /// Source-to-writer executor kind for product ingest evidence.
    pub ingest_executor_kind: String,
    /// Publicly requested source-to-writer parallelism.
    pub ingest_executor_requested_parallelism: usize,
    /// Parallelism actually applied by the admitted source adapter.
    pub ingest_executor_applied_parallelism: usize,
    /// Unit count when known before streaming.
    pub ingest_executor_unit_count_hint: Option<usize>,
    /// Streaming Arrow batch reader consumed by the Vortex writer.
    pub reader: Box<dyn RecordBatchReader + Send>,
}

struct CapillaryPrefetchRecordBatchReader {
    schema: SchemaRef,
    receiver: Receiver<std::result::Result<RecordBatch, ArrowError>>,
    worker: Option<JoinHandle<()>>,
}

impl CapillaryPrefetchRecordBatchReader {
    fn new(inner: Box<dyn RecordBatchReader + Send>, max_in_flight_batches: usize) -> Self {
        let schema = inner.schema();
        let (sender, receiver) = mpsc::sync_channel(max_in_flight_batches.max(1));
        let worker = thread::spawn(move || {
            for batch in inner {
                if sender.send(batch).is_err() {
                    break;
                }
            }
        });
        Self {
            schema,
            receiver,
            worker: Some(worker),
        }
    }
}

impl Iterator for CapillaryPrefetchRecordBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Ok(batch) = self.receiver.recv() {
            Some(batch)
        } else {
            if let Some(worker) = self.worker.take() {
                let _ = worker.join();
            }
            None
        }
    }
}

impl RecordBatchReader for CapillaryPrefetchRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

struct VecRecordBatchReader {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
}

impl VecRecordBatchReader {
    fn new(schema: SchemaRef, batches: VecDeque<RecordBatch>) -> Self {
        Self { schema, batches }
    }
}

impl Iterator for VecRecordBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.batches.pop_front().map(Ok)
    }
}

impl RecordBatchReader for VecRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmbeddedDerivedColumnKind {
    Utf8Length,
    UrlDomain,
    ExtractMinute,
    DateTruncMinute,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmbeddedDerivedColumnMode {
    FullAdapter,
    SourceNativeOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EmbeddedDerivedColumnSpec {
    source_index: usize,
    source_column: String,
    output_column: String,
    kind: EmbeddedDerivedColumnKind,
    output_data_type: DataType,
}

struct EmbeddedDerivedColumnRecordBatchReader {
    schema: SchemaRef,
    inner: Box<dyn RecordBatchReader + Send>,
    specs: Vec<EmbeddedDerivedColumnSpec>,
}

impl Iterator for EmbeddedDerivedColumnRecordBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|batch| {
            batch.and_then(|batch| {
                append_embedded_derived_columns_to_batch(&batch, &self.schema, &self.specs)
                    .map_err(|error| ArrowError::ExternalError(Box::new(error)))
            })
        })
    }
}

impl RecordBatchReader for EmbeddedDerivedColumnRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

struct TextRowsRecordBatchReader {
    schema: SchemaRef,
    header: Vec<String>,
    rows: Vec<Vec<(String, ScalarValue)>>,
    next_row: usize,
    batch_size: usize,
    context: String,
}

impl TextRowsRecordBatchReader {
    fn new(
        schema: SchemaRef,
        header: Vec<String>,
        rows: Vec<Vec<(String, ScalarValue)>>,
        batch_size: usize,
        context: String,
    ) -> Self {
        Self {
            schema,
            header,
            rows,
            next_row: 0,
            batch_size: batch_size.max(1),
            context,
        }
    }
}

impl Iterator for TextRowsRecordBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_row >= self.rows.len() {
            return None;
        }
        let start = self.next_row;
        let end = start.saturating_add(self.batch_size).min(self.rows.len());
        self.next_row = end;
        Some(
            flat_rows_to_record_batch_with_schema(
                Arc::clone(&self.schema),
                &self.header,
                &self.rows[start..end],
                &self.context,
            )
            .map_err(|error| ArrowError::ComputeError(error.to_string())),
        )
    }
}

impl RecordBatchReader for TextRowsRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[derive(Debug)]
struct ParquetRowGroupReadTask {
    task_index: usize,
    row_groups: Vec<usize>,
}

enum ParquetRowGroupReadResult {
    Batch {
        task_index: usize,
        batch_index: usize,
        result: std::result::Result<RecordBatch, ArrowError>,
    },
    TaskComplete {
        task_index: usize,
    },
    TaskError {
        task_index: usize,
        error: ArrowError,
    },
}

struct ParquetRowGroupParallelRecordBatchReader {
    schema: SchemaRef,
    receiver: Option<Receiver<ParquetRowGroupReadResult>>,
    workers: Vec<JoinHandle<()>>,
    next_task_index: usize,
    next_batch_index: usize,
    task_count: usize,
    pending: BTreeMap<(usize, usize), std::result::Result<RecordBatch, ArrowError>>,
    completed_tasks: BTreeSet<usize>,
    task_errors: BTreeMap<usize, ArrowError>,
}

impl ParquetRowGroupParallelRecordBatchReader {
    fn new(
        path: &Path,
        schema: SchemaRef,
        tasks: Vec<ParquetRowGroupReadTask>,
        batch_size: usize,
        applied_parallelism: usize,
    ) -> Self {
        let path = path.to_path_buf();
        let task_count = tasks.len();
        let shared_tasks = Arc::new(Mutex::new(VecDeque::from(tasks)));
        let worker_count = applied_parallelism.max(1).min(task_count.max(1));
        let result_queue_capacity =
            worker_count.max(1) * PARQUET_ROW_GROUP_RESULT_QUEUE_BATCHES_PER_WORKER;
        let (sender, receiver) = mpsc::sync_channel(result_queue_capacity.max(1));
        let mut workers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let worker_path = path.clone();
            let worker_tasks = Arc::clone(&shared_tasks);
            let worker_sender = sender.clone();
            workers.push(thread::spawn(move || {
                loop {
                    let task = {
                        let mut tasks = worker_tasks.lock().expect("Parquet task queue poisoned");
                        tasks.pop_front()
                    };
                    let Some(task) = task else {
                        break;
                    };
                    if let Err(error) = stream_parquet_row_group_batches(
                        &worker_path,
                        task.task_index,
                        task.row_groups,
                        batch_size,
                        &worker_sender,
                    ) {
                        let _ = worker_sender.send(ParquetRowGroupReadResult::TaskError {
                            task_index: task.task_index,
                            error,
                        });
                        break;
                    }
                    if worker_sender
                        .send(ParquetRowGroupReadResult::TaskComplete {
                            task_index: task.task_index,
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            }));
        }
        drop(sender);
        Self {
            schema,
            receiver: Some(receiver),
            workers,
            next_task_index: 0,
            next_batch_index: 0,
            task_count,
            pending: BTreeMap::new(),
            completed_tasks: BTreeSet::new(),
            task_errors: BTreeMap::new(),
        }
    }

    fn close_and_join(&mut self) {
        self.receiver.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

impl Iterator for ParquetRowGroupParallelRecordBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.next_task_index >= self.task_count {
                self.close_and_join();
                return None;
            }
            if let Some(error) = self.task_errors.remove(&self.next_task_index) {
                self.close_and_join();
                return Some(Err(error));
            }
            if let Some(result) = self
                .pending
                .remove(&(self.next_task_index, self.next_batch_index))
            {
                self.next_batch_index += 1;
                return Some(result);
            }
            if self.completed_tasks.remove(&self.next_task_index) {
                self.next_task_index += 1;
                self.next_batch_index = 0;
                continue;
            }
            let Some(receiver) = self.receiver.as_ref() else {
                self.close_and_join();
                return Some(Err(ArrowError::ComputeError(
                    "Parquet row-group parallel reader closed before all tasks completed"
                        .to_string(),
                )));
            };
            if let Ok(result) = receiver.recv() {
                match result {
                    ParquetRowGroupReadResult::Batch {
                        task_index,
                        batch_index,
                        result,
                    } => {
                        self.pending.insert((task_index, batch_index), result);
                    }
                    ParquetRowGroupReadResult::TaskComplete { task_index } => {
                        self.completed_tasks.insert(task_index);
                    }
                    ParquetRowGroupReadResult::TaskError { task_index, error } => {
                        self.task_errors.insert(task_index, error);
                    }
                }
            } else {
                self.close_and_join();
                return Some(Err(ArrowError::ComputeError(
                    "Parquet row-group parallel reader stopped before all tasks completed"
                        .to_string(),
                )));
            }
        }
    }
}

impl RecordBatchReader for ParquetRowGroupParallelRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for ParquetRowGroupParallelRecordBatchReader {
    fn drop(&mut self) {
        self.close_and_join();
    }
}

fn parquet_row_group_read_tasks(
    row_group_count: usize,
    row_group_rows: Option<&[usize]>,
    stream_batch_size: usize,
) -> Vec<ParquetRowGroupReadTask> {
    let Some(row_group_rows) = row_group_rows else {
        return fixed_parquet_row_group_read_tasks(row_group_count);
    };
    if row_group_rows.len() != row_group_count {
        return fixed_parquet_row_group_read_tasks(row_group_count);
    }
    let target_task_rows = stream_batch_size
        .max(PRODUCT_COLUMNAR_STREAM_RECORD_BATCH_ROWS)
        .saturating_mul(PARQUET_PARALLEL_TARGET_TASK_BATCH_MULTIPLE)
        .max(1);
    let mut tasks = Vec::new();
    let mut start = 0usize;
    while start < row_group_count {
        let mut end = start;
        let mut rows = 0usize;
        while end < row_group_count {
            let group_rows = row_group_rows[end].max(1);
            let group_count = end - start;
            if group_count > 0
                && (rows >= target_task_rows
                    || group_count >= PARQUET_PARALLEL_MAX_ROW_GROUPS_PER_TASK)
            {
                break;
            }
            rows = rows.saturating_add(group_rows);
            end += 1;
            if rows >= target_task_rows {
                break;
            }
        }
        if end == start {
            end += 1;
        }
        tasks.push(ParquetRowGroupReadTask {
            task_index: tasks.len(),
            row_groups: (start..end).collect(),
        });
        start = end;
    }
    tasks
}

fn fixed_parquet_row_group_read_tasks(row_group_count: usize) -> Vec<ParquetRowGroupReadTask> {
    let chunk_size = PARQUET_PARALLEL_ROW_GROUPS_PER_TASK.max(1);
    let task_count = row_group_count.div_ceil(chunk_size);
    let mut tasks = Vec::with_capacity(task_count);
    (0..row_group_count)
        .step_by(chunk_size)
        .enumerate()
        .for_each(|(task_index, start)| {
            let end = (start + chunk_size).min(row_group_count);
            tasks.push(ParquetRowGroupReadTask {
                task_index,
                row_groups: (start..end).collect(),
            });
        });
    tasks
}

fn parquet_row_group_task_row_ranges(
    tasks: &[ParquetRowGroupReadTask],
    row_group_ranges: Option<&[(usize, usize)]>,
) -> Option<Vec<(usize, usize)>> {
    let row_group_ranges = row_group_ranges?;
    tasks
        .iter()
        .map(|task| {
            let first = *task.row_groups.first()?;
            let last = *task.row_groups.last()?;
            let start = row_group_ranges.get(first)?.0;
            let end = row_group_ranges.get(last)?.1;
            Some((start, end))
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParquetRowGroupStreamMetadata {
    total_hint: Option<usize>,
    group_count: usize,
    group_rows: Option<Vec<usize>>,
    ranges: Option<Vec<(usize, usize)>>,
}

fn parquet_row_group_stream_metadata(
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> ParquetRowGroupStreamMetadata {
    let row_count_hint = usize::try_from(metadata.file_metadata().num_rows()).ok();
    let row_group_count = metadata.num_row_groups();
    let mut row_group_offset = 0usize;
    let mut row_group_row_ranges = Vec::with_capacity(row_group_count);
    let mut row_group_rows = Vec::with_capacity(row_group_count);
    let mut row_group_ranges_exact = true;
    for row_group_index in 0..row_group_count {
        let Ok(row_group_row_count) =
            usize::try_from(metadata.row_group(row_group_index).num_rows())
        else {
            row_group_ranges_exact = false;
            break;
        };
        let Some(row_group_end) = row_group_offset.checked_add(row_group_row_count) else {
            row_group_ranges_exact = false;
            break;
        };
        row_group_row_ranges.push((row_group_offset, row_group_end));
        row_group_rows.push(row_group_row_count);
        row_group_offset = row_group_end;
    }
    ParquetRowGroupStreamMetadata {
        total_hint: row_count_hint,
        group_count: row_group_count,
        group_rows: row_group_ranges_exact.then_some(row_group_rows),
        ranges: row_group_ranges_exact.then_some(row_group_row_ranges),
    }
}

fn parquet_row_group_source_parallelism_budget(requested_max_parallelism: usize) -> usize {
    // Reserve one lane for the Vortex writer. The remaining default lane keeps
    // source/native normalization overlapped without returning to unbounded
    // row-group buffering.
    requested_max_parallelism.saturating_sub(1)
}

fn stream_parquet_row_group_batches(
    path: &Path,
    task_index: usize,
    row_groups: Vec<usize>,
    batch_size: usize,
    sender: &SyncSender<ParquetRowGroupReadResult>,
) -> std::result::Result<(), ArrowError> {
    let file = File::open(path).map_err(ArrowError::from)?;
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| ArrowError::ParquetError(error.to_string()))?
        .with_batch_size(batch_size.max(1))
        .with_row_groups(row_groups)
        .build()
        .map_err(|error| ArrowError::ParquetError(error.to_string()))?;
    for (batch_index, batch) in reader.enumerate() {
        if sender
            .send(ParquetRowGroupReadResult::Batch {
                task_index,
                batch_index,
                result: batch,
            })
            .is_err()
        {
            break;
        }
    }
    Ok(())
}

struct RowLimitRecordBatchReader<R> {
    inner: R,
    schema: SchemaRef,
    max_rows: usize,
    row_count: usize,
    path: String,
    source_label: &'static str,
    failed: bool,
}

impl<R> RowLimitRecordBatchReader<R>
where
    R: RecordBatchReader,
{
    fn new(inner: R, max_rows: usize, path: String, source_label: &'static str) -> Self {
        let schema = inner.schema();
        Self {
            inner,
            schema,
            max_rows,
            row_count: 0,
            path,
            source_label,
            failed: false,
        }
    }
}

impl<R> Iterator for RowLimitRecordBatchReader<R>
where
    R: RecordBatchReader,
{
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        let batch = self.inner.next()?;
        match batch {
            Ok(batch) => {
                self.row_count =
                    if let Some(row_count) = self.row_count.checked_add(batch.num_rows()) {
                        row_count
                    } else {
                        self.failed = true;
                        return Some(Err(ArrowError::InvalidArgumentError(format!(
                            "local {} source '{}' row count overflowed usize",
                            self.source_label, self.path
                        ))));
                    };
                if self.row_count > self.max_rows {
                    self.failed = true;
                    return Some(Err(ArrowError::InvalidArgumentError(format!(
                        "local {} source '{}' exceeds the configured local source row budget of {}",
                        self.source_label, self.path, self.max_rows
                    ))));
                }
                Some(Ok(batch))
            }
            Err(error) => Some(Err(error)),
        }
    }
}

impl<R> RecordBatchReader for RowLimitRecordBatchReader<R>
where
    R: RecordBatchReader,
{
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn validate_known_stream_row_count(
    path: &Path,
    source_label: &str,
    row_count_hint: Option<usize>,
    max_rows: usize,
) -> Result<()> {
    if let Some(row_count) = row_count_hint
        && row_count > max_rows
    {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local {source_label} source '{}' exceeds the configured local source row budget of {max_rows}",
            path.display()
        )));
    }
    Ok(())
}

fn contiguous_batch_row_ranges(row_count: usize, batch_size: usize) -> Vec<(usize, usize)> {
    if row_count == 0 {
        return Vec::new();
    }
    let batch_size = batch_size.max(1);
    (0..row_count)
        .step_by(batch_size)
        .map(|start| {
            let end = (start + batch_size).min(row_count);
            (start, end)
        })
        .collect()
}

fn flat_columnar_stream_source_from_reader(
    schema: &Schema,
    header: Vec<String>,
    materialized_columns: Vec<String>,
    reader_projection_columns: Vec<String>,
    row_count_hint: Option<usize>,
    stream_plan: &FlatColumnarStreamSourcePlan,
    reader: Box<dyn RecordBatchReader + Send>,
) -> FlatLocalColumnarStreamSource {
    let mut source = FlatLocalColumnarStreamSource {
        column_dtypes: source_schema_column_dtypes(schema),
        column_arrow_dtypes: source_schema_column_arrow_dtypes(schema),
        header,
        materialized_columns,
        reader_projection_columns,
        row_count_hint,
        record_batch_count_hint: stream_plan.record_batch_count_hint,
        source_stream_batch_size: stream_plan.stream_batch_size,
        source_stream_unit_count_hint: stream_plan.source_unit_count_hint,
        source_stream_unit_row_ranges: stream_plan.source_unit_row_ranges.clone(),
        source_stream_unit_hint_kind: stream_plan.source_unit_hint_kind.to_string(),
        source_stream_policy: stream_plan.stream_policy.to_string(),
        source_dictionary_preservation_status: stream_plan
            .dictionary_preservation_status
            .to_string(),
        ingest_executor_status: "serial_pull_reader".to_string(),
        ingest_executor_kind: "single_source_record_batch_reader".to_string(),
        ingest_executor_requested_parallelism: 1,
        ingest_executor_applied_parallelism: 1,
        ingest_executor_unit_count_hint: stream_plan
            .source_unit_count_hint
            .or(stream_plan.record_batch_count_hint),
        reader,
    };
    source.source_dictionary_preservation_status = format!(
        "{};embedded_derived_columns=not_synthesized_source_native_columnar_adapter",
        source.source_dictionary_preservation_status
    );
    source
}

#[must_use]
pub fn with_embedded_derived_columns_columnar_stream_source(
    source: FlatLocalColumnarStreamSource,
) -> FlatLocalColumnarStreamSource {
    attach_embedded_derived_columns_to_stream_source(source, EmbeddedDerivedColumnMode::FullAdapter)
}

#[must_use]
pub fn with_source_native_embedded_derived_columns_columnar_stream_source(
    source: FlatLocalColumnarStreamSource,
) -> FlatLocalColumnarStreamSource {
    attach_embedded_derived_columns_to_stream_source(
        source,
        EmbeddedDerivedColumnMode::SourceNativeOnly,
    )
}

fn attach_embedded_derived_columns_to_stream_source(
    mut source: FlatLocalColumnarStreamSource,
    mode: EmbeddedDerivedColumnMode,
) -> FlatLocalColumnarStreamSource {
    let input_schema = source.reader.schema();
    let specs = embedded_derived_column_specs(input_schema.as_ref(), mode);
    if specs.is_empty() {
        if matches!(mode, EmbeddedDerivedColumnMode::SourceNativeOnly) {
            source.source_dictionary_preservation_status = format!(
                "{};source_native_embedded_derived_columns=not_available_for_current_arrow_layout",
                source.source_dictionary_preservation_status
            );
        }
        return source;
    }
    let output_schema = embedded_derived_column_schema(input_schema.as_ref(), &specs);
    let output_schema = Arc::new(output_schema);
    for spec in &specs {
        source.header.push(spec.output_column.clone());
        source.materialized_columns.push(spec.output_column.clone());
        source
            .reader_projection_columns
            .push(spec.output_column.clone());
    }
    source.column_dtypes = source_schema_column_dtypes(output_schema.as_ref());
    source.column_arrow_dtypes = source_schema_column_arrow_dtypes(output_schema.as_ref());
    let base_dictionary_preservation_status =
        embedded_derived_status_without_not_synthesized_marker(
            &source.source_dictionary_preservation_status,
        );
    source.source_dictionary_preservation_status = format!(
        "{};embedded_derived_columns={};embedded_derived_column_mode={}",
        base_dictionary_preservation_status,
        specs
            .iter()
            .map(|spec| spec.output_column.as_str())
            .collect::<Vec<_>>()
            .join("|"),
        match mode {
            EmbeddedDerivedColumnMode::FullAdapter => "full_adapter",
            EmbeddedDerivedColumnMode::SourceNativeOnly => {
                "source_native_dictionary_or_typed_time_only"
            }
        }
    );
    source.reader = Box::new(EmbeddedDerivedColumnRecordBatchReader {
        schema: Arc::clone(&output_schema),
        inner: source.reader,
        specs,
    });
    source
}

fn embedded_derived_status_without_not_synthesized_marker(status: &str) -> String {
    let retained = status
        .split(';')
        .filter(|part| {
            *part != "embedded_derived_columns=not_synthesized_source_native_columnar_adapter"
        })
        .collect::<Vec<_>>();
    retained.join(";")
}

fn embedded_derived_column_schema(
    input_schema: &Schema,
    specs: &[EmbeddedDerivedColumnSpec],
) -> Schema {
    let mut fields = input_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    for spec in specs {
        fields.push(Field::new(
            &spec.output_column,
            spec.output_data_type.clone(),
            true,
        ));
    }
    Schema::new(fields)
}

fn embedded_derived_column_specs(
    schema: &Schema,
    mode: EmbeddedDerivedColumnMode,
) -> Vec<EmbeddedDerivedColumnSpec> {
    let existing = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<BTreeSet<_>>();
    let mut specs = Vec::new();
    for (source_index, field) in schema.fields().iter().enumerate() {
        let source_column = field.name();
        if is_shardloom_hidden_derived_column(source_column) {
            continue;
        }
        let is_full_utf8_source = matches!(mode, EmbeddedDerivedColumnMode::FullAdapter)
            && is_utf8_arrow_dtype(field.data_type());
        let is_source_native_utf8_dictionary = is_dictionary_utf8_arrow_dtype(field.data_type());
        if is_full_utf8_source || is_source_native_utf8_dictionary {
            let length_column = shardloom_utf8_length_derived_column(source_column);
            if should_embed_utf8_length_column(source_column)
                && !existing.contains(length_column.as_str())
            {
                specs.push(EmbeddedDerivedColumnSpec {
                    source_index,
                    source_column: source_column.clone(),
                    output_column: length_column,
                    kind: EmbeddedDerivedColumnKind::Utf8Length,
                    output_data_type: embedded_utf8_length_data_type(field.data_type()),
                });
            }
            if is_url_like_column_name(source_column) {
                let domain_column = shardloom_url_domain_derived_column(source_column);
                if !existing.contains(domain_column.as_str()) {
                    let output_data_type = embedded_url_domain_data_type(field.data_type());
                    specs.push(EmbeddedDerivedColumnSpec {
                        source_index,
                        source_column: source_column.clone(),
                        output_column: domain_column,
                        kind: EmbeddedDerivedColumnKind::UrlDomain,
                        output_data_type,
                    });
                }
            }
        }
        let minute_column = shardloom_extract_minute_derived_column(source_column);
        if should_embed_extract_minute_column(source_column, field.data_type())
            && is_extract_minute_arrow_dtype(field.data_type())
            && (matches!(mode, EmbeddedDerivedColumnMode::FullAdapter)
                || is_source_native_extract_minute_arrow_dtype(field.data_type()))
            && !existing.contains(minute_column.as_str())
        {
            specs.push(EmbeddedDerivedColumnSpec {
                source_index,
                source_column: source_column.clone(),
                output_column: minute_column,
                kind: EmbeddedDerivedColumnKind::ExtractMinute,
                output_data_type: DataType::UInt8,
            });
        }
        let date_trunc_minute_column = shardloom_date_trunc_minute_derived_column(source_column);
        if should_embed_date_trunc_minute_column(source_column, field.data_type())
            && is_date_trunc_minute_arrow_dtype(field.data_type())
            && !existing.contains(date_trunc_minute_column.as_str())
        {
            specs.push(EmbeddedDerivedColumnSpec {
                source_index,
                source_column: source_column.clone(),
                output_column: date_trunc_minute_column,
                kind: EmbeddedDerivedColumnKind::DateTruncMinute,
                output_data_type: DataType::Int64,
            });
        }
    }
    specs
}

fn append_embedded_derived_columns_to_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
    specs: &[EmbeddedDerivedColumnSpec],
) -> Result<RecordBatch> {
    let mut columns = batch.columns().to_vec();
    let mut spec_index = 0;
    while spec_index < specs.len() {
        let spec = &specs[spec_index];
        if let Some(next_spec) = specs.get(spec_index + 1)
            && spec.source_index == next_spec.source_index
            && spec.kind == EmbeddedDerivedColumnKind::Utf8Length
            && next_spec.kind == EmbeddedDerivedColumnKind::UrlDomain
        {
            let source = batch.column(spec.source_index);
            let (lengths, domains) = embedded_utf8_length_and_url_domain_arrays(source)?;
            columns.push(lengths);
            columns.push(domains);
            spec_index += 2;
            continue;
        }
        if let Some(next_spec) = specs.get(spec_index + 1)
            && spec.source_index == next_spec.source_index
            && spec.kind == EmbeddedDerivedColumnKind::ExtractMinute
            && next_spec.kind == EmbeddedDerivedColumnKind::DateTruncMinute
        {
            let source = batch.column(spec.source_index);
            let (minutes, minute_buckets) = embedded_extract_and_date_trunc_minute_arrays(source)?;
            columns.push(minutes);
            columns.push(minute_buckets);
            spec_index += 2;
            continue;
        }
        let source = batch.column(spec.source_index);
        let derived = embedded_derived_column_array(source, spec)?;
        columns.push(derived);
        spec_index += 1;
    }
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to append embedded derived column RecordBatch fields: {error}; no fallback execution was attempted"
        ))
    })
}

fn embedded_derived_column_array(
    source: &ArrayRef,
    spec: &EmbeddedDerivedColumnSpec,
) -> Result<ArrayRef> {
    match spec.kind {
        EmbeddedDerivedColumnKind::Utf8Length => {
            if let Some(values) = embedded_utf8_length_array_from_dictionary(source)? {
                return Ok(values);
            }
            embedded_utf8_length_array_from_utf8_rows(source)
        }
        EmbeddedDerivedColumnKind::UrlDomain => {
            if let Some(domains) = embedded_url_domain_array_from_dictionary(source)? {
                return Ok(domains);
            }
            embedded_url_domain_array_from_utf8_rows(source)
        }
        EmbeddedDerivedColumnKind::ExtractMinute => {
            let mut minutes = UInt8Builder::with_capacity(source.len());
            for row_index in 0..source.len() {
                match extract_minute_array_value(source, row_index)? {
                    Some(minute) => minutes.append_value(minute),
                    None => minutes.append_null(),
                }
            }
            Ok(Arc::new(minutes.finish()) as ArrayRef)
        }
        EmbeddedDerivedColumnKind::DateTruncMinute => {
            let mut minute_buckets = Int64Builder::with_capacity(source.len());
            for row_index in 0..source.len() {
                match date_trunc_minute_array_value(source, row_index)? {
                    Some(minute_bucket) => minute_buckets.append_value(minute_bucket),
                    None => minute_buckets.append_null(),
                }
            }
            Ok(Arc::new(minute_buckets.finish()) as ArrayRef)
        }
    }
}

fn embedded_utf8_length_and_url_domain_arrays(source: &ArrayRef) -> Result<(ArrayRef, ArrayRef)> {
    if let Some(derived) = embedded_utf8_length_and_url_domain_arrays_from_dictionary(source)? {
        return Ok(derived);
    }
    let mut lengths = EmbeddedUtf8LengthInt32Builder::with_source_len(source.len());
    let mut domains = EmbeddedUrlDomainInt32Builder::with_source_len(source.len());
    for row_index in 0..source.len() {
        if let Some(value) = utf8_array_value(source, row_index)? {
            lengths.append_length(usize_to_u32(value.len())?)?;
            domains.append_domain(crate::url_domain::shardloom_url_domain(value))?;
        } else {
            lengths.append_null();
            domains.append_null();
        }
    }
    Ok((lengths.finish()?, domains.finish()?))
}

fn embedded_utf8_length_array_from_utf8_rows(source: &ArrayRef) -> Result<ArrayRef> {
    let mut lengths = EmbeddedUtf8LengthInt32Builder::with_source_len(source.len());
    for row_index in 0..source.len() {
        if let Some(value) = utf8_array_value(source, row_index)? {
            lengths.append_length(usize_to_u32(value.len())?)?;
        } else {
            lengths.append_null();
        }
    }
    lengths.finish()
}

fn embedded_url_domain_array_from_utf8_rows(source: &ArrayRef) -> Result<ArrayRef> {
    let mut domains = EmbeddedUrlDomainInt32Builder::with_source_len(source.len());
    for row_index in 0..source.len() {
        if let Some(value) = utf8_array_value(source, row_index)? {
            domains.append_domain(crate::url_domain::shardloom_url_domain(value))?;
        } else {
            domains.append_null();
        }
    }
    domains.finish()
}

struct EmbeddedUtf8LengthInt32Builder {
    keys: Int32Builder,
    code_by_length: rustc_hash::FxHashMap<u32, i32>,
    lengths_by_code: UInt32Builder,
}

impl EmbeddedUtf8LengthInt32Builder {
    fn with_source_len(source_len: usize) -> Self {
        let mut code_by_length = rustc_hash::FxHashMap::<u32, i32>::default();
        code_by_length.reserve(embedded_utf8_length_code_capacity(source_len));
        Self {
            keys: Int32Builder::with_capacity(source_len),
            code_by_length,
            lengths_by_code: UInt32Builder::with_capacity(embedded_utf8_length_code_capacity(
                source_len,
            )),
        }
    }

    fn append_length(&mut self, length: u32) -> Result<()> {
        let length_code = if let Some(length_code) = self.code_by_length.get(&length) {
            *length_code
        } else {
            let length_code = i32::try_from(self.code_by_length.len()).map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "embedded UTF-8 length dictionary exceeded Int32 key width: {error}; no fallback execution was attempted"
                ))
            })?;
            self.lengths_by_code.append_value(length);
            self.code_by_length.insert(length, length_code);
            length_code
        };
        self.keys.append_value(length_code);
        Ok(())
    }

    fn append_null(&mut self) {
        self.keys.append_null();
    }

    fn finish(mut self) -> Result<ArrayRef> {
        let keys = self.keys.finish();
        let length_values = Arc::new(self.lengths_by_code.finish()) as ArrayRef;
        let lengths = DictionaryArray::<Int32Type>::try_new(keys, length_values)
            .map_err(|error| embedded_derived_column_arrow_error(&error))?;
        Ok(Arc::new(lengths) as ArrayRef)
    }
}

struct EmbeddedUrlDomainInt32Builder<'a> {
    keys: Int32Builder,
    domain_code_by_domain: rustc_hash::FxHashMap<&'a str, i32>,
    domains_by_code: StringBuilder,
}

impl<'a> EmbeddedUrlDomainInt32Builder<'a> {
    fn with_source_len(source_len: usize) -> Self {
        let mut domain_code_by_domain = rustc_hash::FxHashMap::<&'a str, i32>::default();
        domain_code_by_domain.reserve(embedded_url_domain_code_capacity(source_len));
        Self {
            keys: Int32Builder::with_capacity(source_len),
            domain_code_by_domain,
            domains_by_code: StringBuilder::with_capacity(
                source_len.min(4096),
                embedded_url_domain_bytes_capacity(source_len),
            ),
        }
    }

    fn append_domain(&mut self, domain: &'a str) -> Result<()> {
        let domain_code = if let Some(domain_code) = self.domain_code_by_domain.get(domain) {
            *domain_code
        } else {
            let domain_code = i32::try_from(self.domain_code_by_domain.len()).map_err(|error| {
                ShardLoomError::InvalidOperation(format!(
                    "embedded URL-domain row dictionary exceeded Int32 key width: {error}; no fallback execution was attempted"
                ))
            })?;
            self.domains_by_code.append_value(domain);
            self.domain_code_by_domain.insert(domain, domain_code);
            domain_code
        };
        self.keys.append_value(domain_code);
        Ok(())
    }

    fn append_null(&mut self) {
        self.keys.append_null();
    }

    fn finish(mut self) -> Result<ArrayRef> {
        let keys = self.keys.finish();
        let domain_values = Arc::new(self.domains_by_code.finish()) as ArrayRef;
        let domains = DictionaryArray::<Int32Type>::try_new(keys, domain_values)
            .map_err(|error| embedded_derived_column_arrow_error(&error))?;
        Ok(Arc::new(domains) as ArrayRef)
    }
}

fn embedded_extract_and_date_trunc_minute_arrays(
    source: &ArrayRef,
) -> Result<(ArrayRef, ArrayRef)> {
    if let Some(values) = source.as_any().downcast_ref::<Int8Array>() {
        return build_embedded_minute_arrays(
            values,
            |value| integer_second_minute_i64(i64::from(value)),
            |value| Ok(integer_second_date_trunc_minute_i64(i64::from(value))),
        );
    }
    if let Some(values) = source.as_any().downcast_ref::<Int16Array>() {
        return build_embedded_minute_arrays(
            values,
            |value| integer_second_minute_i64(i64::from(value)),
            |value| Ok(integer_second_date_trunc_minute_i64(i64::from(value))),
        );
    }
    if let Some(values) = source.as_any().downcast_ref::<Int32Array>() {
        return build_embedded_minute_arrays(
            values,
            |value| integer_second_minute_i64(i64::from(value)),
            |value| Ok(integer_second_date_trunc_minute_i64(i64::from(value))),
        );
    }
    if let Some(values) = source.as_any().downcast_ref::<Int64Array>() {
        return build_embedded_minute_arrays(values, integer_second_minute_i64, |value| {
            Ok(integer_second_date_trunc_minute_i64(value))
        });
    }
    if let Some(values) = source.as_any().downcast_ref::<UInt8Array>() {
        return build_embedded_minute_arrays(
            values,
            |value| integer_second_minute_u64(u64::from(value)),
            |value| integer_second_date_trunc_minute_u64(u64::from(value)),
        );
    }
    if let Some(values) = source.as_any().downcast_ref::<UInt16Array>() {
        return build_embedded_minute_arrays(
            values,
            |value| integer_second_minute_u64(u64::from(value)),
            |value| integer_second_date_trunc_minute_u64(u64::from(value)),
        );
    }
    if let Some(values) = source.as_any().downcast_ref::<UInt32Array>() {
        return build_embedded_minute_arrays(
            values,
            |value| integer_second_minute_u64(u64::from(value)),
            |value| integer_second_date_trunc_minute_u64(u64::from(value)),
        );
    }
    if let Some(values) = source.as_any().downcast_ref::<UInt64Array>() {
        return build_embedded_minute_arrays(
            values,
            integer_second_minute_u64,
            integer_second_date_trunc_minute_u64,
        );
    }
    if let Some(values) = source.as_any().downcast_ref::<TimestampSecondArray>() {
        return build_embedded_minute_arrays(values, integer_second_minute_i64, |value| {
            Ok(integer_second_date_trunc_minute_i64(value))
        });
    }
    if let Some(values) = source.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return build_embedded_minute_arrays(values, timestamp_millis_minute, |value| {
            Ok(timestamp_millis_date_trunc_minute(value))
        });
    }
    if let Some(values) = source.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return build_embedded_minute_arrays(values, timestamp_micros_minute, |value| {
            Ok(timestamp_micros_date_trunc_minute(value))
        });
    }
    if let Some(values) = source.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return build_embedded_minute_arrays(values, timestamp_nanos_minute, |value| {
            Ok(timestamp_nanos_date_trunc_minute(value))
        });
    }
    Err(ShardLoomError::InvalidOperation(format!(
        "embedded minute-pair derived columns require a typed time-like Arrow array, got {:?}; no fallback execution was attempted",
        source.data_type()
    )))
}

fn build_embedded_minute_arrays<T, MinuteFn, BucketFn>(
    values: &PrimitiveArray<T>,
    minute: MinuteFn,
    minute_bucket: BucketFn,
) -> Result<(ArrayRef, ArrayRef)>
where
    T: ArrowPrimitiveType,
    MinuteFn: Fn(T::Native) -> u8,
    BucketFn: Fn(T::Native) -> Result<i64>,
{
    let mut minutes = UInt8Builder::with_capacity(values.len());
    let mut minute_buckets = Int64Builder::with_capacity(values.len());
    for row_index in 0..values.len() {
        if values.is_null(row_index) {
            minutes.append_null();
            minute_buckets.append_null();
        } else {
            let value = values.value(row_index);
            minutes.append_value(minute(value));
            minute_buckets.append_value(minute_bucket(value)?);
        }
    }
    Ok(finish_embedded_minute_arrays(minutes, minute_buckets))
}

fn finish_embedded_minute_arrays(
    mut minutes: UInt8Builder,
    mut minute_buckets: Int64Builder,
) -> (ArrayRef, ArrayRef) {
    (
        Arc::new(minutes.finish()) as ArrayRef,
        Arc::new(minute_buckets.finish()) as ArrayRef,
    )
}

fn embedded_derived_column_arrow_error(error: &ArrowError) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "embedded derived column dictionary build failed: {error}; no fallback execution was attempted"
    ))
}

fn embedded_utf8_length_and_url_domain_arrays_from_dictionary(
    source: &ArrayRef,
) -> Result<Option<(ArrayRef, ArrayRef)>> {
    macro_rules! try_dictionary_key_type {
        ($key_type:ty) => {
            if let Some(dictionary) = source.as_any().downcast_ref::<DictionaryArray<$key_type>>() {
                return embedded_utf8_length_and_url_domain_arrays_from_typed_dictionary(
                    dictionary,
                )
                .map(Some);
            }
        };
    }
    try_dictionary_key_type!(Int8Type);
    try_dictionary_key_type!(Int16Type);
    try_dictionary_key_type!(Int32Type);
    try_dictionary_key_type!(Int64Type);
    try_dictionary_key_type!(UInt8Type);
    try_dictionary_key_type!(UInt16Type);
    try_dictionary_key_type!(UInt32Type);
    try_dictionary_key_type!(UInt64Type);
    Ok(None)
}

fn embedded_utf8_length_and_url_domain_arrays_from_typed_dictionary<K>(
    dictionary: &DictionaryArray<K>,
) -> Result<(ArrayRef, ArrayRef)>
where
    K: ArrowDictionaryKeyType,
    K::Native: DictionaryKeyIndex,
{
    let values = dictionary.values();
    if !is_utf8_arrow_dtype(values.data_type()) {
        return Err(ShardLoomError::InvalidOperation(format!(
            "embedded URL metadata dictionary requires UTF-8 dictionary values, got {:?}; no fallback execution was attempted",
            values.data_type()
        )));
    }
    let values_have_nulls = values.null_count() > 0;
    let mut null_value_by_code = if values_have_nulls {
        Vec::with_capacity(values.len())
    } else {
        Vec::new()
    };
    let mut lengths_by_code = UInt32Builder::with_capacity(values.len());
    let mut domain_code_by_value_code = Vec::with_capacity(values.len());
    let mut domain_code_by_domain = rustc_hash::FxHashMap::<&str, K::Native>::default();
    domain_code_by_domain.reserve(embedded_url_domain_code_capacity(values.len()));
    let mut domains_by_code = StringBuilder::with_capacity(
        values.len().min(4096),
        embedded_url_domain_bytes_capacity(values.len()),
    );
    for value_index in 0..values.len() {
        if let Some(value) = utf8_array_value(values, value_index)? {
            if values_have_nulls {
                null_value_by_code.push(false);
            }
            lengths_by_code.append_value(usize_to_u32(value.len())?);
            let domain = crate::url_domain::shardloom_url_domain(value);
            let domain_code = if let Some(domain_code) = domain_code_by_domain.get(domain) {
                *domain_code
            } else {
                let domain_code =
                    K::Native::from_dictionary_key_index(domain_code_by_domain.len())?;
                domains_by_code.append_value(domain);
                domain_code_by_domain.insert(domain, domain_code);
                domain_code
            };
            domain_code_by_value_code.push(Some(domain_code));
        } else {
            null_value_by_code.push(true);
            lengths_by_code.append_value(0);
            domain_code_by_value_code.push(None);
        }
    }
    let length_values = Arc::new(lengths_by_code.finish()) as ArrayRef;
    let domain_values = Arc::new(domains_by_code.finish()) as ArrayRef;
    let length_keys = if values_have_nulls {
        dictionary_keys_with_null_value_codes_rewritten_to_null(
            dictionary,
            &null_value_by_code,
            "embedded URL metadata dictionary",
        )?
    } else {
        dictionary.keys().clone()
    };
    let lengths = DictionaryArray::<K>::try_new(length_keys, length_values)
        .map_err(|error| embedded_derived_column_arrow_error(&error))?;
    let domain_keys = dictionary_keys_remapped_to_derived_codes(
        dictionary,
        &domain_code_by_value_code,
        "embedded URL metadata compact domain dictionary",
    )?;
    let domains = DictionaryArray::<K>::try_new(domain_keys, domain_values)
        .map_err(|error| embedded_derived_column_arrow_error(&error))?;
    Ok((Arc::new(lengths) as ArrayRef, Arc::new(domains) as ArrayRef))
}

fn embedded_utf8_length_array_from_dictionary(source: &ArrayRef) -> Result<Option<ArrayRef>> {
    macro_rules! try_dictionary_key_type {
        ($key_type:ty) => {
            if let Some(dictionary) = source.as_any().downcast_ref::<DictionaryArray<$key_type>>() {
                return embedded_utf8_length_array_from_typed_dictionary(dictionary).map(Some);
            }
        };
    }
    try_dictionary_key_type!(Int8Type);
    try_dictionary_key_type!(Int16Type);
    try_dictionary_key_type!(Int32Type);
    try_dictionary_key_type!(Int64Type);
    try_dictionary_key_type!(UInt8Type);
    try_dictionary_key_type!(UInt16Type);
    try_dictionary_key_type!(UInt32Type);
    try_dictionary_key_type!(UInt64Type);
    Ok(None)
}

fn embedded_utf8_length_array_from_typed_dictionary<K>(
    dictionary: &DictionaryArray<K>,
) -> Result<ArrayRef>
where
    K: ArrowDictionaryKeyType,
    K::Native: DictionaryKeyIndex,
{
    let values = dictionary.values();
    if !is_utf8_arrow_dtype(values.data_type()) {
        return Err(ShardLoomError::InvalidOperation(format!(
            "embedded derived dictionary length requires UTF-8 dictionary values, got {:?}; no fallback execution was attempted",
            values.data_type()
        )));
    }
    let values_have_nulls = values.null_count() > 0;
    let mut null_value_by_code = if values_have_nulls {
        Vec::with_capacity(values.len())
    } else {
        Vec::new()
    };
    let mut lengths_by_code = UInt32Builder::with_capacity(values.len());
    for value_index in 0..values.len() {
        if let Some(value) = utf8_array_value(values, value_index)? {
            if values_have_nulls {
                null_value_by_code.push(false);
            }
            lengths_by_code.append_value(usize_to_u32(value.len())?);
        } else {
            null_value_by_code.push(true);
            lengths_by_code.append_value(0);
        }
    }
    let length_values = Arc::new(lengths_by_code.finish()) as ArrayRef;
    let length_keys = if values_have_nulls {
        dictionary_keys_with_null_value_codes_rewritten_to_null(
            dictionary,
            &null_value_by_code,
            "embedded derived dictionary length",
        )?
    } else {
        dictionary.keys().clone()
    };
    let derived = DictionaryArray::<K>::try_new(length_keys, length_values)
        .map_err(|error| embedded_derived_column_arrow_error(&error))?;
    Ok(Arc::new(derived) as ArrayRef)
}

fn dictionary_keys_with_null_value_codes_rewritten_to_null<K>(
    dictionary: &DictionaryArray<K>,
    null_value_by_code: &[bool],
    context: &str,
) -> Result<PrimitiveArray<K>>
where
    K: ArrowDictionaryKeyType,
    K::Native: DictionaryKeyIndex,
{
    let mut rewritten_keys = Vec::with_capacity(dictionary.len());
    for row_index in 0..dictionary.len() {
        if dictionary.is_null(row_index) {
            rewritten_keys.push(None);
            continue;
        }
        let code = dictionary_code_index(dictionary, row_index)?;
        if code >= null_value_by_code.len() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "{context} code {code} exceeded dictionary value count {}; no fallback execution was attempted",
                null_value_by_code.len()
            )));
        }
        if null_value_by_code[code] {
            rewritten_keys.push(None);
        } else {
            rewritten_keys.push(Some(dictionary.keys().value(row_index)));
        }
    }
    Ok(PrimitiveArray::<K>::from_iter(rewritten_keys))
}

fn embedded_url_domain_array_from_dictionary(source: &ArrayRef) -> Result<Option<ArrayRef>> {
    macro_rules! try_dictionary_key_type {
        ($key_type:ty) => {
            if let Some(dictionary) = source.as_any().downcast_ref::<DictionaryArray<$key_type>>() {
                return embedded_url_domain_array_from_typed_dictionary(dictionary).map(Some);
            }
        };
    }
    try_dictionary_key_type!(Int8Type);
    try_dictionary_key_type!(Int16Type);
    try_dictionary_key_type!(Int32Type);
    try_dictionary_key_type!(Int64Type);
    try_dictionary_key_type!(UInt8Type);
    try_dictionary_key_type!(UInt16Type);
    try_dictionary_key_type!(UInt32Type);
    try_dictionary_key_type!(UInt64Type);
    Ok(None)
}

fn embedded_url_domain_array_from_typed_dictionary<K>(
    dictionary: &DictionaryArray<K>,
) -> Result<ArrayRef>
where
    K: ArrowDictionaryKeyType,
    K::Native: DictionaryKeyIndex,
{
    let values = dictionary.values();
    if !is_utf8_arrow_dtype(values.data_type()) {
        return Err(ShardLoomError::InvalidOperation(format!(
            "embedded URL-domain dictionary requires UTF-8 dictionary values, got {:?}; no fallback execution was attempted",
            values.data_type()
        )));
    }
    let mut domain_code_by_value_code = Vec::with_capacity(values.len());
    let mut domain_code_by_domain = rustc_hash::FxHashMap::<&str, K::Native>::default();
    domain_code_by_domain.reserve(embedded_url_domain_code_capacity(values.len()));
    let mut domains = StringBuilder::with_capacity(
        values.len().min(4096),
        embedded_url_domain_bytes_capacity(values.len()),
    );
    for value_index in 0..values.len() {
        if let Some(value) = utf8_array_value(values, value_index)? {
            let domain = crate::url_domain::shardloom_url_domain(value);
            let domain_code = if let Some(domain_code) = domain_code_by_domain.get(domain) {
                *domain_code
            } else {
                let domain_code =
                    K::Native::from_dictionary_key_index(domain_code_by_domain.len())?;
                domains.append_value(domain);
                domain_code_by_domain.insert(domain, domain_code);
                domain_code
            };
            domain_code_by_value_code.push(Some(domain_code));
        } else {
            domain_code_by_value_code.push(None);
        }
    }
    let domain_values = Arc::new(domains.finish()) as ArrayRef;
    let domain_keys = dictionary_keys_remapped_to_derived_codes(
        dictionary,
        &domain_code_by_value_code,
        "embedded URL-domain compact dictionary",
    )?;
    let derived = DictionaryArray::<K>::try_new(domain_keys, domain_values)
        .map_err(|error| embedded_derived_column_arrow_error(&error))?;
    Ok(Arc::new(derived) as ArrayRef)
}

fn embedded_utf8_length_code_capacity(source_len: usize) -> usize {
    source_len.min(1024)
}

fn embedded_url_domain_code_capacity(source_dictionary_values: usize) -> usize {
    source_dictionary_values.min(4096)
}

fn embedded_url_domain_bytes_capacity(source_dictionary_values: usize) -> usize {
    embedded_url_domain_code_capacity(source_dictionary_values).saturating_mul(16)
}

fn dictionary_keys_remapped_to_derived_codes<K>(
    dictionary: &DictionaryArray<K>,
    derived_code_by_source_code: &[Option<K::Native>],
    context: &str,
) -> Result<PrimitiveArray<K>>
where
    K: ArrowDictionaryKeyType,
    K::Native: DictionaryKeyIndex,
{
    let mut remapped_keys = Vec::with_capacity(dictionary.len());
    for row_index in 0..dictionary.len() {
        if dictionary.is_null(row_index) {
            remapped_keys.push(None);
            continue;
        }
        let code = dictionary_code_index(dictionary, row_index)?;
        let derived_code = derived_code_by_source_code.get(code).ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "{context} source code {code} exceeded dictionary value count {}; no fallback execution was attempted",
                derived_code_by_source_code.len()
            ))
        })?;
        remapped_keys.push(*derived_code);
    }
    Ok(PrimitiveArray::<K>::from_iter(remapped_keys))
}

trait DictionaryKeyIndex: Copy {
    fn dictionary_key_index(self) -> Result<usize>;
    fn from_dictionary_key_index(index: usize) -> Result<Self>;
}

macro_rules! impl_signed_dictionary_key_index {
    ($($key_type:ty),+ $(,)?) => {
        $(
            impl DictionaryKeyIndex for $key_type {
                fn dictionary_key_index(self) -> Result<usize> {
                    usize::try_from(self).map_err(|error| {
                        ShardLoomError::InvalidOperation(format!(
                            "embedded derived column dictionary code was negative or exceeded usize: {error}; no fallback execution was attempted"
                        ))
                    })
                }

                fn from_dictionary_key_index(index: usize) -> Result<Self> {
                    <$key_type>::try_from(index).map_err(|error| {
                        ShardLoomError::InvalidOperation(format!(
                            "embedded derived column dictionary domain code exceeded key width: {error}; no fallback execution was attempted"
                        ))
                    })
                }
            }
        )+
    };
}

macro_rules! impl_unsigned_dictionary_key_index {
    ($($key_type:ty),+ $(,)?) => {
        $(
            impl DictionaryKeyIndex for $key_type {
                fn dictionary_key_index(self) -> Result<usize> {
                    usize::try_from(self).map_err(|error| {
                        ShardLoomError::InvalidOperation(format!(
                            "embedded derived column dictionary code exceeded usize: {error}; no fallback execution was attempted"
                        ))
                    })
                }

                fn from_dictionary_key_index(index: usize) -> Result<Self> {
                    <$key_type>::try_from(index).map_err(|error| {
                        ShardLoomError::InvalidOperation(format!(
                            "embedded derived column dictionary domain code exceeded key width: {error}; no fallback execution was attempted"
                        ))
                    })
                }
            }
        )+
    };
}

impl_signed_dictionary_key_index!(i8, i16, i32, i64);
impl_unsigned_dictionary_key_index!(u8, u16, u32, u64);

fn dictionary_code_index<K>(dictionary: &DictionaryArray<K>, row_index: usize) -> Result<usize>
where
    K: ArrowDictionaryKeyType,
    K::Native: DictionaryKeyIndex,
{
    let code = dictionary.keys().value(row_index);
    code.dictionary_key_index()
}

fn utf8_array_value(array: &ArrayRef, row_index: usize) -> Result<Option<&str>> {
    if array.is_null(row_index) {
        return Ok(None);
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(values.value(row_index)));
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(Some(values.value(row_index)));
    }
    if let Some(values) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(Some(values.value(row_index)));
    }
    Err(ShardLoomError::InvalidOperation(format!(
        "embedded derived column requires a UTF-8 Arrow array, got {:?}; no fallback execution was attempted",
        array.data_type()
    )))
}

fn is_utf8_arrow_dtype(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_dictionary_utf8_arrow_dtype(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Dictionary(key, value)
            if is_arrow_dictionary_key_dtype(key.as_ref()) && is_utf8_arrow_dtype(value.as_ref())
    )
}

fn is_arrow_dictionary_key_dtype(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn embedded_url_domain_data_type(source_data_type: &DataType) -> DataType {
    if let DataType::Dictionary(key, value) = source_data_type
        && is_arrow_dictionary_key_dtype(key.as_ref())
        && is_utf8_arrow_dtype(value.as_ref())
    {
        return DataType::Dictionary(key.clone(), Box::new(DataType::Utf8));
    }
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

fn embedded_utf8_length_data_type(source_data_type: &DataType) -> DataType {
    if let DataType::Dictionary(key, value) = source_data_type
        && is_arrow_dictionary_key_dtype(key.as_ref())
        && is_utf8_arrow_dtype(value.as_ref())
    {
        return DataType::Dictionary(key.clone(), Box::new(DataType::UInt32));
    }
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::UInt32))
}

fn is_extract_minute_arrow_dtype(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Timestamp(_, _)
    )
}

fn is_source_native_extract_minute_arrow_dtype(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Timestamp(_, _)
    )
}

fn is_date_trunc_minute_arrow_dtype(data_type: &DataType) -> bool {
    is_source_native_extract_minute_arrow_dtype(data_type)
}

fn is_url_like_column_name(column: &str) -> bool {
    let lower = column.to_ascii_lowercase();
    lower.contains("url") || lower.contains("referer") || lower.contains("uri")
}

fn should_embed_extract_minute_column(column: &str, data_type: &DataType) -> bool {
    let lower = column.to_ascii_lowercase();
    let known_clean_event_time = lower == "eventtime" || lower == "event_time";
    if is_utf8_arrow_dtype(data_type) {
        return known_clean_event_time;
    }
    known_clean_event_time
        || lower.ends_with("_time")
        || lower.ends_with("time")
        || lower.contains("timestamp")
}

fn should_embed_date_trunc_minute_column(column: &str, data_type: &DataType) -> bool {
    if is_utf8_arrow_dtype(data_type) {
        return false;
    }
    should_embed_extract_minute_column(column, data_type)
}

fn should_embed_utf8_length_column(column: &str) -> bool {
    let lower = column.to_ascii_lowercase();
    is_url_like_column_name(column)
        || lower.contains("search")
        || lower.contains("phrase")
        || lower.contains("title")
}

fn is_shardloom_hidden_derived_column(column: &str) -> bool {
    column.starts_with("__shardloom_derived_")
}

fn shardloom_utf8_length_derived_column(column: &str) -> String {
    format!(
        "__shardloom_derived_utf8_len_{}",
        shardloom_derived_column_token(column)
    )
}

fn shardloom_url_domain_derived_column(column: &str) -> String {
    format!(
        "__shardloom_derived_url_domain_{}",
        shardloom_derived_column_token(column)
    )
}

fn shardloom_extract_minute_derived_column(column: &str) -> String {
    format!(
        "__shardloom_derived_extract_minute_{}",
        shardloom_derived_column_token(column)
    )
}

fn shardloom_date_trunc_minute_derived_column(column: &str) -> String {
    format!(
        "__shardloom_derived_date_trunc_minute_{}",
        shardloom_derived_column_token(column)
    )
}

fn shardloom_derived_column_token(column: &str) -> String {
    let token = column
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if token.is_empty() {
        "column".to_string()
    } else {
        token
    }
}

fn usize_to_u32(value: usize) -> Result<u32> {
    u32::try_from(value).map_err(|_| {
        ShardLoomError::InvalidOperation(
            "embedded derived column byte length exceeded u32; no fallback execution was attempted"
                .to_string(),
        )
    })
}

fn extract_minute_array_value(array: &ArrayRef, row_index: usize) -> Result<Option<u8>> {
    if array.is_null(row_index) {
        return Ok(None);
    }
    if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
        return Ok(Some(integer_second_minute_i64(i64::from(
            values.value(row_index),
        ))));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int16Array>() {
        return Ok(Some(integer_second_minute_i64(i64::from(
            values.value(row_index),
        ))));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(Some(integer_second_minute_i64(i64::from(
            values.value(row_index),
        ))));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(Some(integer_second_minute_i64(values.value(row_index))));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt8Array>() {
        return Ok(Some(integer_second_minute_u64(u64::from(
            values.value(row_index),
        ))));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt16Array>() {
        return Ok(Some(integer_second_minute_u64(u64::from(
            values.value(row_index),
        ))));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        return Ok(Some(integer_second_minute_u64(u64::from(
            values.value(row_index),
        ))));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(Some(integer_second_minute_u64(values.value(row_index))));
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampSecondArray>() {
        return Ok(Some(integer_second_minute_i64(values.value(row_index))));
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return Ok(Some(timestamp_millis_minute(values.value(row_index))));
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Ok(Some(timestamp_micros_minute(values.value(row_index))));
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Ok(Some(timestamp_nanos_minute(values.value(row_index))));
    }
    if let Some(value) = utf8_array_value(array, row_index)? {
        return parse_timestamp_minute(value).map(Some).ok_or_else(|| {
            ShardLoomError::InvalidOperation(
                "embedded extract-minute derived column requires parseable timestamp text; no fallback execution was attempted"
                    .to_string(),
            )
        });
    }
    Err(ShardLoomError::InvalidOperation(format!(
        "embedded extract-minute derived column requires a time-like Arrow array, got {:?}; no fallback execution was attempted",
        array.data_type()
    )))
}

fn date_trunc_minute_array_value(array: &ArrayRef, row_index: usize) -> Result<Option<i64>> {
    if array.is_null(row_index) {
        return Ok(None);
    }
    if let Some(values) = array.as_any().downcast_ref::<Int8Array>() {
        return Ok(Some(integer_second_date_trunc_minute_i64(i64::from(
            values.value(row_index),
        ))));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int16Array>() {
        return Ok(Some(integer_second_date_trunc_minute_i64(i64::from(
            values.value(row_index),
        ))));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int32Array>() {
        return Ok(Some(integer_second_date_trunc_minute_i64(i64::from(
            values.value(row_index),
        ))));
    }
    if let Some(values) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(Some(integer_second_date_trunc_minute_i64(
            values.value(row_index),
        )));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt8Array>() {
        return Ok(Some(integer_second_date_trunc_minute_u64(u64::from(
            values.value(row_index),
        ))?));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt16Array>() {
        return Ok(Some(integer_second_date_trunc_minute_u64(u64::from(
            values.value(row_index),
        ))?));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
        return Ok(Some(integer_second_date_trunc_minute_u64(u64::from(
            values.value(row_index),
        ))?));
    }
    if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
        return Ok(Some(integer_second_date_trunc_minute_u64(
            values.value(row_index),
        )?));
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampSecondArray>() {
        return Ok(Some(integer_second_date_trunc_minute_i64(
            values.value(row_index),
        )));
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return Ok(Some(timestamp_millis_date_trunc_minute(
            values.value(row_index),
        )));
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Ok(Some(timestamp_micros_date_trunc_minute(
            values.value(row_index),
        )));
    }
    if let Some(values) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Ok(Some(timestamp_nanos_date_trunc_minute(
            values.value(row_index),
        )));
    }
    Err(ShardLoomError::InvalidOperation(format!(
        "embedded date-trunc-minute derived column requires a typed time-like Arrow array, got {:?}; no fallback execution was attempted",
        array.data_type()
    )))
}

fn integer_second_minute_i64(value: i64) -> u8 {
    u8::try_from(value.rem_euclid(3600) / 60).expect("minute is in 0..60")
}

fn integer_second_minute_u64(value: u64) -> u8 {
    u8::try_from((value % 3600) / 60).expect("minute is in 0..60")
}

fn timestamp_micros_minute(value: i64) -> u8 {
    u8::try_from(value.div_euclid(60_000_000).rem_euclid(60)).expect("minute is in 0..60")
}

fn timestamp_millis_minute(value: i64) -> u8 {
    u8::try_from(value.div_euclid(60_000).rem_euclid(60)).expect("minute is in 0..60")
}

fn timestamp_nanos_minute(value: i64) -> u8 {
    u8::try_from(value.div_euclid(60_000_000_000).rem_euclid(60)).expect("minute is in 0..60")
}

fn integer_second_date_trunc_minute_i64(value: i64) -> i64 {
    value.div_euclid(60) * 60
}

fn integer_second_date_trunc_minute_u64(value: u64) -> Result<i64> {
    let minute_bucket = (value / 60) * 60;
    i64::try_from(minute_bucket).map_err(|_| {
        ShardLoomError::InvalidOperation(
            "embedded date-trunc-minute derived column exceeded int64 range; no fallback execution was attempted"
                .to_string(),
        )
    })
}

fn timestamp_micros_date_trunc_minute(value: i64) -> i64 {
    value.div_euclid(60_000_000) * 60
}

fn timestamp_millis_date_trunc_minute(value: i64) -> i64 {
    value.div_euclid(60_000) * 60
}

fn timestamp_nanos_date_trunc_minute(value: i64) -> i64 {
    value.div_euclid(60_000_000_000) * 60
}

fn parse_timestamp_minute(value: &str) -> Option<u8> {
    let time = value.split_once('T').map_or_else(
        || value.split_whitespace().nth(1),
        |(_date, time)| Some(time),
    )?;
    let minute = time.split(':').nth(1)?.parse::<u8>().ok()?;
    (minute < 60).then_some(minute)
}

/// Wrap a product columnar ingest source in a bounded Capillary prefetch
/// pipeline when safe.
///
/// This does not introduce a new execution engine. It only overlaps the
/// admitted source adapter's `RecordBatch` production with the Vortex writer's
/// consumption and preserves source order through a bounded channel.
#[must_use]
pub fn with_capillary_prefetch_columnar_stream_source(
    source: FlatLocalColumnarStreamSource,
    requested_max_parallelism: usize,
) -> FlatLocalColumnarStreamSource {
    if columnar_stream_source_already_has_capillary_executor(&source) {
        return source;
    }
    let requested_max_parallelism = requested_max_parallelism.max(1);
    let FlatLocalColumnarStreamSource {
        header,
        column_dtypes,
        column_arrow_dtypes,
        materialized_columns,
        reader_projection_columns,
        row_count_hint,
        record_batch_count_hint,
        source_stream_batch_size,
        source_stream_unit_count_hint,
        source_stream_unit_row_ranges,
        source_stream_unit_hint_kind,
        source_stream_policy,
        source_dictionary_preservation_status,
        ingest_executor_unit_count_hint,
        reader,
        ..
    } = source;
    let source_parallelism_budget =
        columnar_prefetch_source_parallelism_budget(requested_max_parallelism);
    if source_parallelism_budget == 0 {
        return FlatLocalColumnarStreamSource {
            header,
            column_dtypes,
            column_arrow_dtypes,
            materialized_columns,
            reader_projection_columns,
            row_count_hint,
            record_batch_count_hint,
            source_stream_batch_size,
            source_stream_unit_count_hint,
            source_stream_unit_row_ranges,
            source_stream_unit_hint_kind,
            source_stream_policy,
            source_dictionary_preservation_status,
            ingest_executor_status: if requested_max_parallelism == 1 {
                "serial_pull_reader".to_string()
            } else {
                "serial_pull_reader_writer_slot_reserved".to_string()
            },
            ingest_executor_kind: if requested_max_parallelism == 1 {
                "single_source_record_batch_reader".to_string()
            } else {
                "single_source_record_batch_reader_with_writer_slot_reserved".to_string()
            },
            ingest_executor_requested_parallelism: requested_max_parallelism,
            ingest_executor_applied_parallelism: 1,
            ingest_executor_unit_count_hint,
            reader,
        };
    }
    let unit_hint = ingest_executor_unit_count_hint.or(record_batch_count_hint);
    let applied_parallelism = unit_hint.map_or(source_parallelism_budget, |unit_count| {
        source_parallelism_budget.min(unit_count.max(1))
    });
    FlatLocalColumnarStreamSource {
        header,
        column_dtypes,
        column_arrow_dtypes,
        materialized_columns,
        reader_projection_columns,
        row_count_hint,
        record_batch_count_hint,
        source_stream_batch_size,
        source_stream_unit_count_hint,
        source_stream_unit_row_ranges,
        source_stream_unit_hint_kind,
        source_stream_policy,
        source_dictionary_preservation_status,
        ingest_executor_status: "bounded_capillary_prefetch_active".to_string(),
        ingest_executor_kind: "source_reader_to_vortex_writer_prefetch_pipeline".to_string(),
        ingest_executor_requested_parallelism: requested_max_parallelism,
        ingest_executor_applied_parallelism: applied_parallelism,
        ingest_executor_unit_count_hint: unit_hint,
        reader: Box::new(CapillaryPrefetchRecordBatchReader::new(
            reader,
            applied_parallelism,
        )),
    }
}

fn columnar_stream_source_already_has_capillary_executor(
    source: &FlatLocalColumnarStreamSource,
) -> bool {
    matches!(
        source.ingest_executor_status.as_str(),
        "bounded_capillary_prefetch_active"
            | "bounded_capillary_row_group_parallel_active"
            | "bounded_capillary_row_group_parallel_writer_budgeted"
    )
}

fn columnar_prefetch_source_parallelism_budget(requested_max_parallelism: usize) -> usize {
    // Reserve one lane for the Vortex writer. Even the public default
    // `max_parallelism=2` should keep a bounded source prefetch lane active
    // rather than collapsing the product path back to serial pull.
    requested_max_parallelism.saturating_sub(1)
}

/// Convert admitted local text scalar rows into a streaming Arrow source for
/// product Vortex ingest.
///
/// This is a Universal Ingest bridge, not a fallback engine: CSV/JSON/JSONL
/// adapters still own parsing and diagnostics, then the Vortex writer consumes
/// typed Arrow `RecordBatch` units through the same streaming path as columnar
/// source adapters.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the flat rows do not match
/// the declared schema or cannot be represented by the scoped Arrow/Vortex
/// provider boundary.
#[allow(clippy::too_many_arguments)]
pub fn stream_flat_text_rows_columnar_source(
    header: Vec<String>,
    column_dtypes: Vec<Option<LogicalDType>>,
    column_arrow_dtypes: Vec<Option<DataType>>,
    materialized_columns: Vec<String>,
    reader_projection_columns: Vec<String>,
    rows: Vec<Vec<(String, ScalarValue)>>,
    requested_batch_size: usize,
    source_format_label: &str,
) -> Result<FlatLocalColumnarStreamSource> {
    let batch_size = requested_batch_size.clamp(1, PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS);
    let stream_policy = typed_text_record_batch_stream_policy(batch_size);
    let row_count = rows.len();
    let context = format!("{source_format_label} Universal Ingest typed text RecordBatch");
    if rows.is_empty() {
        let empty_batch = flat_rows_to_record_batch_with_dtypes(
            &header,
            &column_dtypes,
            &column_arrow_dtypes,
            &[],
            &context,
        )?;
        let schema = empty_batch.schema();
        return Ok(attach_embedded_derived_columns_to_stream_source(
            FlatLocalColumnarStreamSource {
                header,
                column_dtypes,
                column_arrow_dtypes,
                materialized_columns,
                reader_projection_columns,
                row_count_hint: Some(0),
                record_batch_count_hint: Some(0),
                source_stream_batch_size: batch_size,
                source_stream_unit_count_hint: Some(0),
                source_stream_unit_row_ranges: Some(Vec::new()),
                source_stream_unit_hint_kind: "text_record_batch_count".to_string(),
                source_stream_policy: stream_policy.to_string(),
                source_dictionary_preservation_status:
                    "text_typed_column_builders_preserve_declared_types_no_source_dictionary"
                        .to_string(),
                ingest_executor_status: "serial_typed_text_record_batch_builder".to_string(),
                ingest_executor_kind: "text_rows_to_arrow_record_batch_reader".to_string(),
                ingest_executor_requested_parallelism: 1,
                ingest_executor_applied_parallelism: 1,
                ingest_executor_unit_count_hint: Some(0),
                reader: Box::new(VecRecordBatchReader::new(schema, VecDeque::new())),
            },
            EmbeddedDerivedColumnMode::FullAdapter,
        ));
    }
    let record_batch_count = row_count.div_ceil(batch_size);
    let schema = infer_flat_text_rows_record_batch_schema(
        &header,
        &column_dtypes,
        &column_arrow_dtypes,
        &rows,
        &context,
    )?;
    Ok(attach_embedded_derived_columns_to_stream_source(
        FlatLocalColumnarStreamSource {
            header: header.clone(),
            column_dtypes,
            column_arrow_dtypes,
            materialized_columns,
            reader_projection_columns,
            row_count_hint: Some(row_count),
            record_batch_count_hint: Some(record_batch_count),
            source_stream_batch_size: batch_size,
            source_stream_unit_count_hint: Some(record_batch_count),
            source_stream_unit_row_ranges: Some(contiguous_batch_row_ranges(row_count, batch_size)),
            source_stream_unit_hint_kind: "text_record_batch_count".to_string(),
            source_stream_policy: stream_policy.to_string(),
            source_dictionary_preservation_status:
                "text_typed_column_builders_preserve_declared_types_no_source_dictionary"
                    .to_string(),
            ingest_executor_status: "lazy_typed_text_record_batch_builder".to_string(),
            ingest_executor_kind: "lazy_text_rows_to_arrow_record_batch_reader".to_string(),
            ingest_executor_requested_parallelism: 1,
            ingest_executor_applied_parallelism: 1,
            ingest_executor_unit_count_hint: Some(record_batch_count),
            reader: Box::new(TextRowsRecordBatchReader::new(
                schema, header, rows, batch_size, context,
            )),
        },
        EmbeddedDerivedColumnMode::FullAdapter,
    ))
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
        .with_batch_size(max_rows.clamp(1, SCOPED_COMPAT_RECORD_BATCH_ROWS))
        .build()
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to build local Parquet source reader for '{}': {error}",
                path.display()
            ))
        })?;

    read_flat_record_batch_reader_columnar(&mut reader, path, "Parquet", max_rows)
}

/// Stream a local Parquet file as Arrow batches for product Vortex ingest.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the Parquet reader cannot be constructed, or known file metadata exceeds
/// `max_rows`.
pub fn stream_flat_parquet_columnar_source(
    path: &Path,
    max_rows: usize,
) -> Result<FlatLocalColumnarStreamSource> {
    stream_flat_parquet_columnar_source_with_parallelism(path, max_rows, 1)
}

/// Stream a local Parquet file as Arrow batches using source-native row-group
/// work units when product ingest requests parallelism.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened,
/// the Parquet reader cannot be constructed, or known file metadata exceeds
/// `max_rows`.
pub fn stream_flat_parquet_columnar_source_with_parallelism(
    path: &Path,
    max_rows: usize,
    requested_max_parallelism: usize,
) -> Result<FlatLocalColumnarStreamSource> {
    let file = open_local_source_file(path, "Parquet")?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create streaming local Parquet source reader for '{}': {error}",
                path.display()
            ))
        })?;
    let schema = Arc::clone(builder.schema());
    let header = source_schema_header(path, "Parquet", schema.as_ref())?;
    let row_group_metadata = parquet_row_group_stream_metadata(builder.metadata());
    let row_count_hint = row_group_metadata.total_hint;
    validate_known_stream_row_count(path, "Parquet", row_count_hint, max_rows)?;
    let row_group_count = row_group_metadata.group_count;
    let mut stream_plan = FlatColumnarStreamSourcePlan::product_batches(
        max_rows,
        Some(row_group_count),
        "parquet_row_group_count",
        "parquet_arrow_reader_preserves_physical_columnar_values_when_provider_surfaces_dictionary",
    );
    stream_plan
        .source_unit_row_ranges
        .clone_from(&row_group_metadata.ranges);
    let requested_max_parallelism = requested_max_parallelism.max(1);
    let source_parallelism_budget =
        parquet_row_group_source_parallelism_budget(requested_max_parallelism);
    if requested_max_parallelism > 1 && row_group_count > 1 && source_parallelism_budget > 0 {
        let stream_batch_size = stream_plan.stream_batch_size;
        let tasks = parquet_row_group_read_tasks(
            row_group_count,
            row_group_metadata.group_rows.as_deref(),
            stream_batch_size,
        );
        let task_count = tasks.len();
        let applied_parallelism = source_parallelism_budget.min(task_count.max(1));
        stream_plan.source_unit_count_hint = Some(task_count);
        stream_plan.source_unit_hint_kind = "parquet_adaptive_row_group_task_count";
        stream_plan.source_unit_row_ranges =
            parquet_row_group_task_row_ranges(&tasks, row_group_metadata.ranges.as_deref());
        let mut source = flat_columnar_stream_source_from_reader(
            schema.as_ref(),
            header.clone(),
            header.clone(),
            header,
            row_count_hint,
            &stream_plan,
            Box::new(ParquetRowGroupParallelRecordBatchReader::new(
                path,
                Arc::clone(&schema),
                tasks,
                stream_batch_size,
                applied_parallelism,
            )),
        );
        source.ingest_executor_status =
            "bounded_capillary_row_group_parallel_writer_budgeted".to_string();
        source.ingest_executor_kind =
            "parquet_row_group_adaptive_coalesced_reader_to_vortex_writer_with_writer_slot_reserved"
                .to_string();
        source.ingest_executor_requested_parallelism = requested_max_parallelism;
        source.ingest_executor_applied_parallelism = applied_parallelism;
        source.ingest_executor_unit_count_hint = Some(task_count);
        return Ok(source);
    }
    let reader = builder
        .with_batch_size(stream_plan.stream_batch_size)
        .build()
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to build streaming local Parquet source reader for '{}': {error}",
                path.display()
            ))
        })?;
    let source = flat_columnar_stream_source_from_reader(
        schema.as_ref(),
        header.clone(),
        header.clone(),
        header,
        row_count_hint,
        &stream_plan,
        Box::new(reader),
    );
    Ok(with_capillary_prefetch_columnar_stream_source(
        source,
        requested_max_parallelism,
    ))
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
    let column_arrow_dtypes = source_schema_column_arrow_dtypes(schema.as_ref());
    let projection_indices = projection_indices_for_header(&header, required_columns);
    let projected_header = projection_header(&header, &projection_indices);
    let projection = parquet::arrow::ProjectionMask::roots(
        builder.parquet_schema(),
        projection_indices.iter().copied(),
    );
    let mut reader = builder
        .with_batch_size(max_rows.clamp(1, SCOPED_COMPAT_RECORD_BATCH_ROWS))
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
        column_arrow_dtypes,
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

/// Stream a local Arrow IPC file as Arrow batches for product Vortex ingest.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened
/// or the Arrow IPC reader cannot be constructed.
pub fn stream_flat_arrow_ipc_columnar_source(
    path: &Path,
    max_rows: usize,
) -> Result<FlatLocalColumnarStreamSource> {
    let file = open_local_source_file(path, "Arrow IPC")?;
    let reader = arrow_ipc::reader::FileReader::try_new(file, None).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to create streaming local Arrow IPC source reader for '{}': {error}",
            path.display()
        ))
    })?;
    let schema = reader.schema();
    let header = source_schema_header(path, "Arrow IPC", schema.as_ref())?;
    let batch_hint = reader.num_batches();
    let stream_plan = FlatColumnarStreamSourcePlan::source_defined_batches(batch_hint);
    Ok(flat_columnar_stream_source_from_reader(
        schema.as_ref(),
        header.clone(),
        header.clone(),
        header,
        None,
        &stream_plan,
        Box::new(RowLimitRecordBatchReader::new(
            reader,
            max_rows,
            path.display().to_string(),
            "Arrow IPC",
        )),
    ))
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
    let column_arrow_dtypes = source_schema_column_arrow_dtypes(schema.as_ref());
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
        column_arrow_dtypes,
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
        .with_batch_size(max_rows.clamp(1, SCOPED_COMPAT_RECORD_BATCH_ROWS))
        .build(BufReader::new(file))
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create local Avro source reader for '{}': {error}",
                path.display()
            ))
        })?;

    read_flat_record_batch_reader_columnar(&mut reader, path, "Avro", max_rows)
}

/// Stream a local Avro file as Arrow batches for product Vortex ingest.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened
/// or the Avro reader cannot be constructed.
pub fn stream_flat_avro_columnar_source(
    path: &Path,
    max_rows: usize,
) -> Result<FlatLocalColumnarStreamSource> {
    let file = open_local_source_file(path, "Avro")?;
    let stream_plan = FlatColumnarStreamSourcePlan::product_batches(
        max_rows,
        None,
        "avro_stream_record_batches_unknown_before_read",
        "avro_arrow_reader_typed_batches_preserved_dictionary_contract_not_declared",
    );
    let reader = arrow_avro::reader::ReaderBuilder::new()
        .with_batch_size(stream_plan.stream_batch_size)
        .build(BufReader::new(file))
        .map_err(|error| {
            ShardLoomError::InvalidOperation(format!(
                "failed to create streaming local Avro source reader for '{}': {error}",
                path.display()
            ))
        })?;
    let schema = reader.schema();
    let header = source_schema_header(path, "Avro", schema.as_ref())?;
    Ok(flat_columnar_stream_source_from_reader(
        schema.as_ref(),
        header.clone(),
        header.clone(),
        header,
        None,
        &stream_plan,
        Box::new(RowLimitRecordBatchReader::new(
            reader,
            max_rows,
            path.display().to_string(),
            "Avro",
        )),
    ))
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
        .with_batch_size(max_rows.clamp(1, SCOPED_COMPAT_RECORD_BATCH_ROWS))
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
    let column_arrow_dtypes = source_schema_column_arrow_dtypes(schema.as_ref());
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
        .with_batch_size(max_rows.clamp(1, SCOPED_COMPAT_RECORD_BATCH_ROWS))
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
            column_arrow_dtypes,
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
        .with_batch_size(max_rows.clamp(1, SCOPED_COMPAT_RECORD_BATCH_ROWS))
        .build();

    read_flat_record_batch_reader_columnar(&mut reader, path, "ORC", max_rows)
}

/// Stream a local ORC file as Arrow batches for product Vortex ingest.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the file cannot be opened
/// or the ORC reader cannot be constructed.
pub fn stream_flat_orc_columnar_source(
    path: &Path,
    max_rows: usize,
) -> Result<FlatLocalColumnarStreamSource> {
    let file = open_local_source_file(path, "ORC")?;
    let builder = orc_rust::ArrowReaderBuilder::try_new(file).map_err(|error| {
        ShardLoomError::InvalidOperation(format!(
            "failed to create streaming local ORC source reader for '{}': {error}",
            path.display()
        ))
    })?;
    let schema = builder.schema();
    let header = source_schema_header(path, "ORC", schema.as_ref())?;
    let stream_plan = FlatColumnarStreamSourcePlan::product_batches(
        max_rows,
        None,
        "orc_stream_record_batches_unknown_before_read",
        "orc_arrow_reader_typed_batches_preserved_dictionary_contract_not_declared",
    );
    let reader = builder
        .with_batch_size(stream_plan.stream_batch_size)
        .build();
    Ok(flat_columnar_stream_source_from_reader(
        schema.as_ref(),
        header.clone(),
        header.clone(),
        header,
        None,
        &stream_plan,
        Box::new(RowLimitRecordBatchReader::new(
            reader,
            max_rows,
            path.display().to_string(),
            "ORC",
        )),
    ))
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
    let column_arrow_dtypes = source_schema_column_arrow_dtypes(schema.as_ref());
    let projection_indices = projection_indices_for_header(&header, required_columns);
    let projected_header = projection_header(&header, &projection_indices);
    let projection = orc_rust::projection::ProjectionMask::named_roots(
        builder.file_metadata().root_data_type(),
        &projected_header,
    );
    let mut reader = builder
        .with_batch_size(max_rows.clamp(1, SCOPED_COMPAT_RECORD_BATCH_ROWS))
        .with_projection(projection)
        .build();

    read_flat_record_batch_reader_columnar_with_header(
        &mut reader,
        path,
        "ORC",
        max_rows,
        header,
        column_dtypes,
        column_arrow_dtypes,
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
    let column_arrow_dtypes = source_schema_column_arrow_dtypes(schema.as_ref());
    let projected_header = header.clone();
    read_flat_record_batch_reader_columnar_with_header(
        reader,
        path,
        source_label,
        max_rows,
        header,
        column_dtypes,
        column_arrow_dtypes,
        &projected_header,
    )
}

#[allow(clippy::too_many_arguments)]
fn read_flat_record_batch_reader_columnar_with_header<R>(
    reader: &mut R,
    path: &Path,
    source_label: &str,
    max_rows: usize,
    header: Vec<String>,
    column_dtypes: Vec<Option<LogicalDType>>,
    column_arrow_dtypes: Vec<Option<DataType>>,
    projected_header: &[String],
) -> Result<FlatLocalColumnarSource>
where
    R: RecordBatchReader,
{
    let schema = FlatColumnarReadSchema {
        header,
        column_dtypes,
        column_arrow_dtypes,
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
    if schema.column_arrow_dtypes.len() != schema.header.len() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "local {source_label} source '{}' produced {} source Arrow dtype hints for {} schema columns",
            path.display(),
            schema.column_arrow_dtypes.len(),
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
                "local {source_label} source '{}' exceeds the configured local source row budget of {max_rows}",
                path.display()
            )));
        }
        batches.push(batch);
    }

    Ok(FlatLocalColumnarSource {
        header: schema.header,
        column_dtypes: schema.column_dtypes,
        column_arrow_dtypes: schema.column_arrow_dtypes,
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
/// contains an unsupported type for the scoped local runtime.
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
        column_arrow_dtypes: source.column_arrow_dtypes.clone(),
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
        if is_shardloom_hidden_derived_column(column) {
            return Err(reserved_hidden_derived_column_error(
                column,
                &format!("local {source_label} source '{}'", path.display()),
            ));
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

fn source_schema_column_arrow_dtypes(schema: &Schema) -> Vec<Option<DataType>> {
    schema
        .fields()
        .iter()
        .map(|field| source_schema_arrow_dtype_hint(field.data_type()))
        .collect()
}

fn source_schema_dtype_hint(data_type: &DataType) -> Option<LogicalDType> {
    match data_type {
        DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::BinaryView => Some(LogicalDType::Binary),
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            Some(LogicalDType::List)
        }
        DataType::Struct(_) => Some(LogicalDType::Struct),
        DataType::Decimal128(precision, scale) => {
            let scale = u8::try_from(*scale).ok()?;
            (scale <= *precision)
                .then(|| LogicalDType::Extension(format!("decimal128({precision},{scale})")))
        }
        _ => None,
    }
}

fn source_schema_arrow_dtype_hint(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::Boolean => Some(DataType::Boolean),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            Some(DataType::Int64)
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Some(DataType::UInt64)
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Some(DataType::Float64),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some(DataType::Utf8),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::BinaryView => Some(DataType::Binary),
        DataType::Date32 => Some(DataType::Date32),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Some(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        DataType::Decimal128(precision, scale) => {
            let scale_u8 = u8::try_from(*scale).ok()?;
            (scale_u8 <= *precision).then_some(DataType::Decimal128(*precision, *scale))
        }
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            let child = source_schema_arrow_dtype_hint(field.data_type())?;
            Some(DataType::List(Arc::new(Field::new_list_field(child, true))))
        }
        DataType::Struct(fields) => fields
            .iter()
            .map(|field| {
                let dtype = source_schema_arrow_dtype_hint(field.data_type())?;
                Some(Field::new(field.name(), dtype, true))
            })
            .collect::<Option<Vec<_>>>()
            .map(|fields| DataType::Struct(Fields::from(fields))),
        _ => None,
    }
}

struct FlatColumnarReadSchema {
    header: Vec<String>,
    column_dtypes: Vec<Option<LogicalDType>>,
    column_arrow_dtypes: Vec<Option<DataType>>,
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
    let column_arrow_dtypes = vec![None; columns.len()];
    encode_flat_parquet_rows_with_arrow_dtypes(columns, column_dtypes, &column_arrow_dtypes, rows)
}

/// Encode flat scalar rows into local Parquet bytes with optional logical and
/// Arrow dtype hints.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when hints do not match the
/// declared columns or an admitted typed sink cannot preserve the hinted dtype.
pub fn encode_flat_parquet_rows_with_arrow_dtypes(
    columns: &[String],
    column_dtypes: &[Option<LogicalDType>],
    column_arrow_dtypes: &[Option<DataType>],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>> {
    let batch = flat_rows_to_record_batch_with_dtypes(
        columns,
        column_dtypes,
        column_arrow_dtypes,
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
    let column_arrow_dtypes = vec![None; columns.len()];
    encode_flat_arrow_ipc_rows_with_arrow_dtypes(columns, column_dtypes, &column_arrow_dtypes, rows)
}

/// Encode flat scalar rows into local Arrow IPC bytes with optional logical and
/// Arrow dtype hints.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when hints do not match the
/// declared columns or an admitted typed sink cannot preserve the hinted dtype.
pub fn encode_flat_arrow_ipc_rows_with_arrow_dtypes(
    columns: &[String],
    column_dtypes: &[Option<LogicalDType>],
    column_arrow_dtypes: &[Option<DataType>],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>> {
    let batch = flat_rows_to_record_batch_with_dtypes(
        columns,
        column_dtypes,
        column_arrow_dtypes,
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
    let column_arrow_dtypes = vec![None; columns.len()];
    encode_flat_avro_rows_with_arrow_dtypes(columns, column_dtypes, &column_arrow_dtypes, rows)
}

/// Encode flat scalar rows into local Avro bytes with optional logical and Arrow
/// dtype hints.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when hints do not match the
/// declared columns or an admitted typed sink cannot preserve the hinted dtype.
pub fn encode_flat_avro_rows_with_arrow_dtypes(
    columns: &[String],
    column_dtypes: &[Option<LogicalDType>],
    column_arrow_dtypes: &[Option<DataType>],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>> {
    let batch = flat_rows_to_record_batch_with_dtypes(
        columns,
        column_dtypes,
        column_arrow_dtypes,
        rows,
        "local Avro output",
    )?;
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
    let column_arrow_dtypes = vec![None; columns.len()];
    encode_flat_orc_rows_with_arrow_dtypes(columns, column_dtypes, &column_arrow_dtypes, rows)
}

/// Encode flat scalar rows into local ORC bytes with optional logical and Arrow
/// dtype hints.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when hints do not match the
/// declared columns, an admitted typed sink cannot preserve the hinted dtype, or
/// the scoped ORC writer path has not admitted the hinted type.
pub fn encode_flat_orc_rows_with_arrow_dtypes(
    columns: &[String],
    column_dtypes: &[Option<LogicalDType>],
    column_arrow_dtypes: &[Option<DataType>],
    rows: &[Vec<(String, ScalarValue)>],
) -> Result<Vec<u8>> {
    let batch = flat_rows_to_record_batch_with_dtypes(
        columns,
        column_dtypes,
        column_arrow_dtypes,
        rows,
        "local ORC output",
    )?;
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
            DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Struct(_)
        ) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "local ORC output does not yet admit typed nested preservation for column '{}'; orc-rust 0.8.0 panics on nested Arrow writer conversion with unsupported datatype, so ShardLoom blocks before provider conversion; scoped typed nested compatibility sinks are admitted through Parquet/Arrow IPC/Avro after Arrow schema inference, while ORC remains blocked until writer/readback evidence is available; no fallback execution was attempted",
                field.name()
            )));
        }
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
    let column_arrow_dtypes = vec![None; columns.len()];
    flat_rows_to_record_batch_with_dtypes(
        columns,
        &column_dtypes,
        &column_arrow_dtypes,
        rows,
        context,
    )
}

/// Build an Arrow `RecordBatch` from ordered flat scalar rows and explicit dtype hints.
///
/// This is used by scoped Universal Ingest adapters that parse small streaming
/// batches themselves but still need `ShardLoom`'s canonical scalar-to-Arrow
/// conversion, nullability, and no-fallback diagnostics. Callers should pass a
/// bounded batch, not a whole large source.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when columns, dtype hints, or
/// scalar values cannot be represented by the admitted Arrow/Vortex boundary.
pub fn flat_rows_to_record_batch_with_dtypes(
    columns: &[String],
    column_dtypes: &[Option<LogicalDType>],
    column_arrow_dtypes: &[Option<DataType>],
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
    if column_arrow_dtypes.len() != columns.len() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{context} declared {} column Arrow dtype hints for {} columns",
            column_arrow_dtypes.len(),
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
                column_arrow_dtypes[column_index].as_ref(),
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

fn infer_flat_text_rows_record_batch_schema(
    columns: &[String],
    column_dtypes: &[Option<LogicalDType>],
    column_arrow_dtypes: &[Option<DataType>],
    rows: &[Vec<(String, ScalarValue)>],
    context: &str,
) -> Result<SchemaRef> {
    validate_flat_columns(columns, context)?;
    if column_dtypes.len() != columns.len() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{context} declared {} column dtype hints for {} columns",
            column_dtypes.len(),
            columns.len()
        )));
    }
    if column_arrow_dtypes.len() != columns.len() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{context} declared {} column Arrow dtype hints for {} columns",
            column_arrow_dtypes.len(),
            columns.len()
        )));
    }
    let fields = columns
        .iter()
        .enumerate()
        .map(|(column_index, column)| {
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
            let nullable = values.iter().any(|value| matches!(value, ScalarValue::Null));
            let data_type = stable_arrow_dtype_for_flat_text_column(
                column,
                column_dtypes[column_index].as_ref(),
                column_arrow_dtypes[column_index].as_ref(),
                &values,
                context,
            )?;
            Ok(Field::new(column, data_type, nullable))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn stable_arrow_dtype_for_flat_text_column(
    column: &str,
    column_dtype: Option<&LogicalDType>,
    column_arrow_dtype: Option<&DataType>,
    values: &[&ScalarValue],
    context: &str,
) -> Result<DataType> {
    if let Some(data_type) = column_arrow_dtype {
        return Ok(data_type.clone());
    }
    if let Some(data_type) = logical_dtype_arrow_hint(column, column_dtype, values, context)? {
        return Ok(data_type);
    }
    let non_null_values = values
        .iter()
        .copied()
        .filter(|value| !matches!(value, ScalarValue::Null))
        .collect::<Vec<_>>();
    if non_null_values.is_empty() {
        return Ok(DataType::Utf8);
    }
    infer_arrow_dtype_from_scalar_values(column, &non_null_values, context)
}

fn logical_dtype_arrow_hint(
    column: &str,
    column_dtype: Option<&LogicalDType>,
    values: &[&ScalarValue],
    context: &str,
) -> Result<Option<DataType>> {
    let Some(column_dtype) = column_dtype else {
        return Ok(None);
    };
    Ok(match column_dtype {
        LogicalDType::Boolean => Some(DataType::Boolean),
        LogicalDType::Int64 => Some(DataType::Int64),
        LogicalDType::UInt64 => Some(DataType::UInt64),
        LogicalDType::Float64 => Some(DataType::Float64),
        LogicalDType::Utf8 => Some(DataType::Utf8),
        LogicalDType::Binary => Some(DataType::Binary),
        LogicalDType::Date32 => Some(DataType::Date32),
        LogicalDType::TimestampMicros => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
        LogicalDType::List | LogicalDType::Struct => {
            Some(infer_arrow_dtype_from_scalar_values(column, values, context)?)
        }
        LogicalDType::Unknown => None,
        LogicalDType::Extension(_) => decimal128_dtype_precision_scale(column_dtype, column, context)?
            .map(|(precision, scale)| {
                let scale = i8::try_from(scale).map_err(|_| {
                    ShardLoomError::InvalidOperation(format!(
                        "{context} column '{column}' cannot preserve decimal128({precision},{scale}): scale exceeds Arrow decimal128 range"
                    ))
                })?;
                Ok(DataType::Decimal128(precision, scale))
            })
            .transpose()?,
    })
}

/// Build an Arrow `RecordBatch` from ordered flat scalar rows using an existing schema.
///
/// This keeps streaming adapters schema-stable across batches when later
/// batches contain nulls or values whose inferred dtype would otherwise drift.
///
/// # Errors
/// Returns [`ShardLoomError::InvalidOperation`] when the row shape does not
/// match `schema` or a scalar cannot be appended to the target Arrow dtype.
pub fn flat_rows_to_record_batch_with_schema(
    schema: SchemaRef,
    columns: &[String],
    rows: &[Vec<(String, ScalarValue)>],
    context: &str,
) -> Result<RecordBatch> {
    validate_flat_columns(columns, context)?;
    if schema.fields().len() != columns.len() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{context} schema has {} fields for {} columns",
            schema.fields().len(),
            columns.len()
        )));
    }
    let arrays = columns
        .iter()
        .enumerate()
        .map(|(column_index, column)| {
            let field = schema.field(column_index);
            if field.name() != column {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "{context} schema field mismatch at index {column_index}: expected '{column}', found '{}'",
                    field.name()
                )));
            }
            let mut builder = make_builder(field.data_type(), rows.len());
            for row in rows {
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
                append_scalar_to_arrow_builder(
                    builder.as_mut(),
                    field.data_type(),
                    value,
                    column,
                    context,
                )?;
            }
            Ok(builder.finish())
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, arrays).map_err(|error| {
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
        if is_shardloom_hidden_derived_column(column) {
            return Err(reserved_hidden_derived_column_error(column, context));
        }
        if !seen_columns.insert(column) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "{context} contains duplicate column '{column}'"
            )));
        }
    }
    Ok(())
}

fn reserved_hidden_derived_column_error(column: &str, context: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "{context} contains reserved ShardLoom hidden derived column '{column}'; \
         columns beginning with '__shardloom_derived_' are owned by Universal Ingest and \
         cannot be supplied by callers because native rewrite routes require verified \
         source-derived values; rename the input column and retry; no fallback execution was attempted"
    ))
}

#[allow(clippy::too_many_lines)]
fn flat_output_column_array(
    column: &str,
    column_index: usize,
    column_dtype: Option<&LogicalDType>,
    column_arrow_dtype: Option<&DataType>,
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
    let declared_complex = column_dtype.and_then(|dtype| match dtype {
        LogicalDType::List => Some("list"),
        LogicalDType::Struct => Some("struct"),
        _ => None,
    });
    let declared_complex_arrow_dtype = column_arrow_dtype
        .and_then(|dtype| complex_arrow_dtype_kind(dtype).map(|kind| (kind, dtype)));
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
    let nullable = values
        .iter()
        .any(|value| matches!(value, ScalarValue::Null));
    if let Some(complex_kind) = declared_complex {
        if let Some((arrow_kind, arrow_dtype)) = declared_complex_arrow_dtype {
            if arrow_kind != complex_kind {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "{context} column '{column}' is declared {complex_kind} but has Arrow child-schema hint {arrow_dtype:?}; scoped typed complex sinks require matching logical and Arrow complex families"
                )));
            }
            return nested_arrow_column_with_data_type(
                column,
                &values,
                nullable,
                arrow_dtype,
                context,
            );
        }
        match inferred_kind {
            Some(kind) if kind == complex_kind => {
                return nested_arrow_column(column, &values, nullable, context);
            }
            Some(kind) => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "{context} column '{column}' is declared {complex_kind} but contains non-null {kind} values; scoped typed complex sinks require {complex_kind} values or NULLs"
                )));
            }
            None => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "{context} column '{column}' has output_plan_conversion_blocker=nested_sink_schema_inference_required; all values are NULL and the logical dtype hint {complex_kind} does not carry nested child schema, so ShardLoom blocks before writer conversion instead of inventing a child type; no fallback execution was attempted and external_engine_invoked=false"
                )));
            }
        }
    }
    if matches!(inferred_kind, Some("list" | "struct")) {
        if let Some((arrow_kind, arrow_dtype)) = declared_complex_arrow_dtype {
            if Some(arrow_kind) != inferred_kind {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "{context} column '{column}' inferred {inferred_kind:?} but has Arrow child-schema hint {arrow_dtype:?}; scoped typed complex sinks require matching logical and Arrow complex families"
                )));
            }
            return nested_arrow_column_with_data_type(
                column,
                &values,
                nullable,
                arrow_dtype,
                context,
            );
        }
        return nested_arrow_column(column, &values, nullable, context);
    }
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

fn nested_arrow_column(
    column: &str,
    values: &[&ScalarValue],
    nullable: bool,
    context: &str,
) -> Result<(Field, ArrayRef)> {
    let data_type = infer_arrow_dtype_from_scalar_values(column, values, context)?;
    nested_arrow_column_with_data_type(column, values, nullable, &data_type, context)
}

fn nested_arrow_column_with_data_type(
    column: &str,
    values: &[&ScalarValue],
    nullable: bool,
    data_type: &DataType,
    context: &str,
) -> Result<(Field, ArrayRef)> {
    let mut builder = make_builder(data_type, values.len());
    for value in values {
        append_scalar_to_arrow_builder(builder.as_mut(), data_type, value, column, context)?;
    }
    Ok((
        Field::new(column, data_type.clone(), nullable),
        builder.finish(),
    ))
}

fn complex_arrow_dtype_kind(data_type: &DataType) -> Option<&'static str> {
    match data_type {
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => Some("list"),
        DataType::Struct(_) => Some("struct"),
        _ => None,
    }
}

#[allow(clippy::too_many_lines)]
fn infer_arrow_dtype_from_scalar_values(
    path: &str,
    values: &[&ScalarValue],
    context: &str,
) -> Result<DataType> {
    let mut inferred_kind: Option<&'static str> = None;
    let mut primitive_dtype: Option<DataType> = None;
    let mut list_child_values = Vec::new();
    let mut struct_field_names: Option<Vec<String>> = None;
    let mut struct_field_values: Vec<Vec<&ScalarValue>> = Vec::new();

    for value in values {
        if matches!(value, ScalarValue::Null) {
            continue;
        }
        let candidate_kind = scalar_family(value);
        match inferred_kind {
            None => inferred_kind = Some(candidate_kind),
            Some(existing) if existing == candidate_kind => {}
            Some(existing) => {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "{context} nested output path '{path}' mixes scalar families {existing} and {candidate_kind}; scoped typed complex sinks require one stable Arrow dtype per nested path"
                )));
            }
        }
        match value {
            ScalarValue::List(values) => {
                list_child_values.extend(values.iter());
            }
            ScalarValue::Struct(fields) => {
                validate_struct_sink_fields(path, fields, context)?;
                let candidate_names = fields
                    .iter()
                    .map(|(name, _value)| name.clone())
                    .collect::<Vec<_>>();
                match &struct_field_names {
                    None => {
                        struct_field_values = vec![Vec::new(); candidate_names.len()];
                        struct_field_names = Some(candidate_names);
                    }
                    Some(existing_names) if existing_names == &candidate_names => {}
                    Some(existing_names) => {
                        return Err(ShardLoomError::InvalidOperation(format!(
                            "{context} nested output path '{path}' mixes struct field layouts {existing_names:?} and {candidate_names:?}; scoped typed complex sinks require stable field names and order"
                        )));
                    }
                }
                for (field_index, (_name, field_value)) in fields.iter().enumerate() {
                    struct_field_values[field_index].push(field_value);
                }
            }
            other => {
                let candidate_dtype = primitive_arrow_dtype(path, other, context)?;
                match &primitive_dtype {
                    None => primitive_dtype = Some(candidate_dtype),
                    Some(existing_dtype) if existing_dtype == &candidate_dtype => {}
                    Some(existing_dtype) => {
                        return Err(ShardLoomError::InvalidOperation(format!(
                            "{context} nested output path '{path}' mixes Arrow dtypes {existing_dtype:?} and {candidate_dtype:?}; scoped typed complex sinks require one stable Arrow dtype per nested path"
                        )));
                    }
                }
            }
        }
    }

    match inferred_kind {
        None => Err(ShardLoomError::InvalidOperation(format!(
            "{context} nested output path '{path}' has output_plan_conversion_blocker=nested_sink_schema_inference_required; all values are NULL or empty and no nested child schema is available, so ShardLoom blocks before writer conversion instead of inventing a child type; no fallback execution was attempted and external_engine_invoked=false"
        ))),
        Some("list") => {
            if list_child_values.is_empty() {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "{context} nested output path '{path}' has output_plan_conversion_blocker=nested_sink_schema_inference_required; list values are empty and no child Arrow dtype can be inferred; no fallback execution was attempted and external_engine_invoked=false"
                )));
            }
            let child_type =
                infer_arrow_dtype_from_scalar_values(&format!("{path}[]"), &list_child_values, context)?;
            Ok(DataType::List(Arc::new(Field::new_list_field(
                child_type, true,
            ))))
        }
        Some("struct") => {
            let Some(field_names) = struct_field_names else {
                return Err(ShardLoomError::InvalidOperation(format!(
                    "{context} nested output path '{path}' has output_plan_conversion_blocker=nested_sink_schema_inference_required; no struct field schema is available; no fallback execution was attempted and external_engine_invoked=false"
                )));
            };
            let fields = field_names
                .iter()
                .zip(struct_field_values.iter())
                .map(|(field_name, field_values)| {
                    let field_type = infer_arrow_dtype_from_scalar_values(
                        &format!("{path}.{field_name}"),
                        field_values,
                        context,
                    )?;
                    Ok(Field::new(field_name, field_type, true))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(DataType::Struct(Fields::from(fields)))
        }
        Some(_) => primitive_dtype.ok_or_else(|| {
            ShardLoomError::InvalidOperation(format!(
                "{context} nested output path '{path}' did not infer a primitive Arrow dtype; no fallback execution was attempted"
            ))
        }),
    }
}

fn validate_struct_sink_fields(
    path: &str,
    fields: &[(String, ScalarValue)],
    context: &str,
) -> Result<()> {
    if fields.is_empty() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{context} nested output path '{path}' contains an empty struct; scoped typed complex sinks require at least one field for deterministic Arrow schema inference"
        )));
    }
    let mut seen = BTreeSet::new();
    for (field_name, _value) in fields {
        if field_name.trim().is_empty() {
            return Err(ShardLoomError::InvalidOperation(format!(
                "{context} nested output path '{path}' contains an empty struct field name"
            )));
        }
        if !seen.insert(field_name) {
            return Err(ShardLoomError::InvalidOperation(format!(
                "{context} nested output path '{path}' contains duplicate struct field '{field_name}'"
            )));
        }
    }
    Ok(())
}

fn primitive_arrow_dtype(path: &str, value: &ScalarValue, context: &str) -> Result<DataType> {
    match value {
        ScalarValue::Boolean(_) => Ok(DataType::Boolean),
        ScalarValue::Int64(_) => Ok(DataType::Int64),
        ScalarValue::UInt64(_) => Ok(DataType::UInt64),
        ScalarValue::Float64(_) => Ok(DataType::Float64),
        ScalarValue::Utf8(_) => Ok(DataType::Utf8),
        ScalarValue::Binary(_) => Ok(DataType::Binary),
        ScalarValue::Decimal128 {
            precision, scale, ..
        } if (1..=38).contains(precision) && scale <= precision => {
            let scale = i8::try_from(*scale).map_err(|_| {
                ShardLoomError::InvalidOperation(format!(
                    "{context} nested output path '{path}' cannot preserve decimal128({precision},{scale}): scale exceeds Arrow decimal128 range"
                ))
            })?;
            Ok(DataType::Decimal128(*precision, scale))
        }
        ScalarValue::Decimal128 {
            precision, scale, ..
        } => Err(ShardLoomError::InvalidOperation(format!(
            "{context} nested output path '{path}' has invalid decimal128({precision},{scale}); scoped typed complex sinks require 1 <= precision <= 38 and scale <= precision"
        ))),
        ScalarValue::Date32(_) => Ok(DataType::Date32),
        ScalarValue::TimestampMicros(_) => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        ScalarValue::Null | ScalarValue::List(_) | ScalarValue::Struct(_) => {
            Err(ShardLoomError::InvalidOperation(format!(
                "{context} nested output path '{path}' expected a non-null primitive value for Arrow dtype inference but found {}",
                scalar_family(value)
            )))
        }
    }
}

fn append_scalar_to_arrow_builder(
    builder: &mut dyn ArrayBuilder,
    data_type: &DataType,
    value: &ScalarValue,
    path: &str,
    context: &str,
) -> Result<()> {
    if matches!(value, ScalarValue::Null) {
        return append_null_to_arrow_builder(builder, data_type, path, context);
    }
    match (data_type, value) {
        (DataType::Boolean, ScalarValue::Boolean(value)) => {
            bool_builder_mut(builder, path, context)?.append_value(*value);
        }
        (DataType::Int64, ScalarValue::Int64(value)) => {
            int64_builder_mut(builder, path, context)?.append_value(*value);
        }
        (DataType::UInt64, ScalarValue::UInt64(value)) => {
            uint64_builder_mut(builder, path, context)?.append_value(*value);
        }
        (DataType::Float64, ScalarValue::Float64(value)) => {
            float64_builder_mut(builder, path, context)?.append_value(*value);
        }
        (DataType::Utf8, ScalarValue::Utf8(value)) => {
            string_builder_mut(builder, path, context)?.append_value(value);
        }
        (DataType::Binary, ScalarValue::Binary(value)) => {
            binary_builder_mut(builder, path, context)?.append_value(value);
        }
        (
            DataType::Decimal128(precision, scale),
            ScalarValue::Decimal128 {
                value,
                precision: value_precision,
                scale: value_scale,
            },
        ) if value_precision == precision && i8::try_from(*value_scale).ok() == Some(*scale) => {
            decimal128_builder_mut(builder, path, context)?.append_value(*value);
        }
        (DataType::Date32, ScalarValue::Date32(value)) => {
            date32_builder_mut(builder, path, context)?.append_value(*value);
        }
        (DataType::Timestamp(TimeUnit::Microsecond, None), ScalarValue::TimestampMicros(value)) => {
            timestamp_micros_builder_mut(builder, path, context)?.append_value(*value);
        }
        (DataType::List(field), ScalarValue::List(values)) => {
            let list_builder = list_builder_mut(builder, path, context)?;
            for (value_index, value) in values.iter().enumerate() {
                append_scalar_to_arrow_builder(
                    list_builder.values().as_mut(),
                    field.data_type(),
                    value,
                    &format!("{path}[{value_index}]"),
                    context,
                )?;
            }
            list_builder.append(true);
        }
        (DataType::Struct(fields), ScalarValue::Struct(values)) => {
            validate_struct_value_matches_arrow_fields(path, values, fields, context)?;
            let struct_builder = struct_builder_mut(builder, path, context)?;
            for (field_index, field) in fields.iter().enumerate() {
                let (_field_name, field_value) = &values[field_index];
                let child_builder = struct_builder
                    .field_builders_mut()
                    .get_mut(field_index)
                    .ok_or_else(|| arrow_builder_downcast_error(context, path, data_type))?;
                append_scalar_to_arrow_builder(
                    child_builder.as_mut(),
                    field.data_type(),
                    field_value,
                    &format!("{path}.{}", field.name()),
                    context,
                )?;
            }
            struct_builder.append(true);
        }
        (DataType::Decimal128(precision, scale), ScalarValue::Decimal128 { .. }) => {
            return Err(ShardLoomError::InvalidOperation(format!(
                "{context} nested output path '{path}' expected decimal128({precision},{scale}) but found {}",
                value.summary()
            )));
        }
        _ => {
            return Err(ShardLoomError::InvalidOperation(format!(
                "{context} nested output path '{path}' expected Arrow dtype {:?} but found {}",
                data_type,
                scalar_family(value)
            )));
        }
    }
    Ok(())
}

fn append_null_to_arrow_builder(
    builder: &mut dyn ArrayBuilder,
    data_type: &DataType,
    path: &str,
    context: &str,
) -> Result<()> {
    match data_type {
        DataType::Boolean => bool_builder_mut(builder, path, context)?.append_null(),
        DataType::Int64 => int64_builder_mut(builder, path, context)?.append_null(),
        DataType::UInt64 => uint64_builder_mut(builder, path, context)?.append_null(),
        DataType::Float64 => float64_builder_mut(builder, path, context)?.append_null(),
        DataType::Utf8 => string_builder_mut(builder, path, context)?.append_null(),
        DataType::Binary => binary_builder_mut(builder, path, context)?.append_null(),
        DataType::Decimal128(_, _) => {
            decimal128_builder_mut(builder, path, context)?.append_null();
        }
        DataType::Date32 => date32_builder_mut(builder, path, context)?.append_null(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            timestamp_micros_builder_mut(builder, path, context)?.append_null();
        }
        DataType::List(_) => list_builder_mut(builder, path, context)?.append(false),
        DataType::Struct(fields) => {
            let struct_builder = struct_builder_mut(builder, path, context)?;
            for (field_index, field) in fields.iter().enumerate() {
                let child_builder = struct_builder
                    .field_builders_mut()
                    .get_mut(field_index)
                    .ok_or_else(|| arrow_builder_downcast_error(context, path, data_type))?;
                append_null_to_arrow_builder(
                    child_builder.as_mut(),
                    field.data_type(),
                    &format!("{path}.{}", field.name()),
                    context,
                )?;
            }
            struct_builder.append(false);
        }
        other => {
            return Err(ShardLoomError::InvalidOperation(format!(
                "{context} nested output path '{path}' has unsupported Arrow dtype {other:?}; scoped typed complex sinks admit boolean, int64, uint64, float64, utf8, binary, decimal128, date32, timestamp_micros, list, and struct only"
            )));
        }
    }
    Ok(())
}

fn validate_struct_value_matches_arrow_fields(
    path: &str,
    values: &[(String, ScalarValue)],
    fields: &Fields,
    context: &str,
) -> Result<()> {
    if values.len() != fields.len() {
        return Err(ShardLoomError::InvalidOperation(format!(
            "{context} nested output path '{path}' has {} struct fields but inferred Arrow schema has {}; scoped typed complex sinks require stable field names and order",
            values.len(),
            fields.len()
        )));
    }
    for (field_index, (value_name, _value)) in values.iter().enumerate() {
        let expected_name = fields[field_index].name();
        if value_name != expected_name {
            return Err(ShardLoomError::InvalidOperation(format!(
                "{context} nested output path '{path}' field {} expected '{expected_name}' but found '{value_name}'; scoped typed complex sinks require stable field names and order",
                field_index + 1
            )));
        }
    }
    Ok(())
}

fn bool_builder_mut<'a>(
    builder: &'a mut dyn ArrayBuilder,
    path: &str,
    context: &str,
) -> Result<&'a mut BooleanBuilder> {
    builder
        .as_any_mut()
        .downcast_mut::<BooleanBuilder>()
        .ok_or_else(|| arrow_builder_downcast_error(context, path, &DataType::Boolean))
}

fn int64_builder_mut<'a>(
    builder: &'a mut dyn ArrayBuilder,
    path: &str,
    context: &str,
) -> Result<&'a mut Int64Builder> {
    builder
        .as_any_mut()
        .downcast_mut::<Int64Builder>()
        .ok_or_else(|| arrow_builder_downcast_error(context, path, &DataType::Int64))
}

fn uint64_builder_mut<'a>(
    builder: &'a mut dyn ArrayBuilder,
    path: &str,
    context: &str,
) -> Result<&'a mut UInt64Builder> {
    builder
        .as_any_mut()
        .downcast_mut::<UInt64Builder>()
        .ok_or_else(|| arrow_builder_downcast_error(context, path, &DataType::UInt64))
}

fn float64_builder_mut<'a>(
    builder: &'a mut dyn ArrayBuilder,
    path: &str,
    context: &str,
) -> Result<&'a mut Float64Builder> {
    builder
        .as_any_mut()
        .downcast_mut::<Float64Builder>()
        .ok_or_else(|| arrow_builder_downcast_error(context, path, &DataType::Float64))
}

fn string_builder_mut<'a>(
    builder: &'a mut dyn ArrayBuilder,
    path: &str,
    context: &str,
) -> Result<&'a mut StringBuilder> {
    builder
        .as_any_mut()
        .downcast_mut::<StringBuilder>()
        .ok_or_else(|| arrow_builder_downcast_error(context, path, &DataType::Utf8))
}

fn binary_builder_mut<'a>(
    builder: &'a mut dyn ArrayBuilder,
    path: &str,
    context: &str,
) -> Result<&'a mut BinaryBuilder> {
    builder
        .as_any_mut()
        .downcast_mut::<BinaryBuilder>()
        .ok_or_else(|| arrow_builder_downcast_error(context, path, &DataType::Binary))
}

fn decimal128_builder_mut<'a>(
    builder: &'a mut dyn ArrayBuilder,
    path: &str,
    context: &str,
) -> Result<&'a mut Decimal128Builder> {
    builder
        .as_any_mut()
        .downcast_mut::<Decimal128Builder>()
        .ok_or_else(|| arrow_builder_downcast_error(context, path, &DataType::Decimal128(38, 0)))
}

fn date32_builder_mut<'a>(
    builder: &'a mut dyn ArrayBuilder,
    path: &str,
    context: &str,
) -> Result<&'a mut Date32Builder> {
    builder
        .as_any_mut()
        .downcast_mut::<Date32Builder>()
        .ok_or_else(|| arrow_builder_downcast_error(context, path, &DataType::Date32))
}

fn timestamp_micros_builder_mut<'a>(
    builder: &'a mut dyn ArrayBuilder,
    path: &str,
    context: &str,
) -> Result<&'a mut TimestampMicrosecondBuilder> {
    builder
        .as_any_mut()
        .downcast_mut::<TimestampMicrosecondBuilder>()
        .ok_or_else(|| {
            arrow_builder_downcast_error(
                context,
                path,
                &DataType::Timestamp(TimeUnit::Microsecond, None),
            )
        })
}

fn list_builder_mut<'a>(
    builder: &'a mut dyn ArrayBuilder,
    path: &str,
    context: &str,
) -> Result<&'a mut ListBuilder<Box<dyn ArrayBuilder>>> {
    builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
        .ok_or_else(|| {
            arrow_builder_downcast_error(
                context,
                path,
                &DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            )
        })
}

fn struct_builder_mut<'a>(
    builder: &'a mut dyn ArrayBuilder,
    path: &str,
    context: &str,
) -> Result<&'a mut StructBuilder> {
    builder
        .as_any_mut()
        .downcast_mut::<StructBuilder>()
        .ok_or_else(|| {
            arrow_builder_downcast_error(context, path, &DataType::Struct(Fields::empty()))
        })
}

fn arrow_builder_downcast_error(context: &str, path: &str, data_type: &DataType) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "{context} nested output path '{path}' could not access Arrow builder for dtype {data_type:?}; no fallback execution was attempted"
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

#[allow(clippy::cast_precision_loss, clippy::too_many_lines)]
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
    if let Some(values) = array.as_any().downcast_ref::<ListArray>() {
        return arrow_list_value_to_shardloom(&values.value(row_index), column, path, source_label);
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeListArray>() {
        return arrow_list_value_to_shardloom(&values.value(row_index), column, path, source_label);
    }
    if let Some(values) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        return arrow_list_value_to_shardloom(&values.value(row_index), column, path, source_label);
    }
    if let Some(values) = array.as_any().downcast_ref::<StructArray>() {
        let mut fields = Vec::with_capacity(values.num_columns());
        for (field, child) in values.fields().iter().zip(values.columns()) {
            let nested_column = format!("{column}.{}", field.name());
            fields.push((
                field.name().clone(),
                arrow_scalar_to_shardloom(
                    child.as_ref(),
                    row_index,
                    &nested_column,
                    path,
                    source_label,
                )?,
            ));
        }
        return Ok(ScalarValue::Struct(fields));
    }
    Err(ShardLoomError::InvalidOperation(format!(
        "local {source_label} source '{}' column '{column}' has unsupported Arrow type {:?}; scoped local-source runtime admits booleans, integers, floats, UTF-8 strings, binary byte arrays, decimal128, date32, timestamp_micros, list, and struct only",
        path.display(),
        array.data_type()
    )))
}

fn arrow_list_value_to_shardloom(
    value: &ArrayRef,
    column: &str,
    path: &Path,
    source_label: &str,
) -> Result<ScalarValue> {
    let mut values = Vec::with_capacity(value.len());
    for value_index in 0..value.len() {
        let nested_column = format!("{column}[{value_index}]");
        values.push(arrow_scalar_to_shardloom(
            value.as_ref(),
            value_index,
            &nested_column,
            path,
            source_label,
        )?);
    }
    Ok(ScalarValue::List(values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::ArrayAccessor as _;
    use arrow_array::builder::StringDictionaryBuilder;

    type FlatSinkRow = Vec<(String, ScalarValue)>;
    type FlatSinkRows = Vec<FlatSinkRow>;
    type BinarySinkEncoder = fn(&[String], &[FlatSinkRow]) -> Result<Vec<u8>>;
    type TypedSinkEncoder =
        fn(&[String], &[Option<LogicalDType>], &[FlatSinkRow]) -> Result<Vec<u8>>;

    #[test]
    fn embedded_url_domain_capacity_is_bounded_by_expected_domain_cardinality() {
        assert_eq!(embedded_url_domain_code_capacity(0), 0);
        assert_eq!(embedded_url_domain_code_capacity(8), 8);
        assert_eq!(embedded_url_domain_code_capacity(1_000_000), 4096);
        assert_eq!(embedded_url_domain_bytes_capacity(1_000_000), 65_536);
    }

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
            column_arrow_dtypes: vec![Some(DataType::Binary)],
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

    struct TestRecordBatchReader {
        schema: SchemaRef,
        batches: std::collections::VecDeque<RecordBatch>,
    }

    impl Iterator for TestRecordBatchReader {
        type Item = std::result::Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            self.batches.pop_front().map(Ok)
        }
    }

    impl RecordBatchReader for TestRecordBatchReader {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    #[test]
    fn product_columnar_stream_batch_size_uses_product_policy_not_smoke_cap() {
        assert_eq!(product_columnar_stream_record_batch_rows(0), 1);
        assert_eq!(product_columnar_stream_record_batch_rows(10), 10);
        assert_eq!(
            product_columnar_stream_record_batch_rows(
                PRODUCT_COLUMNAR_LARGE_STREAM_ROW_THRESHOLD - 1
            ),
            PRODUCT_COLUMNAR_STREAM_RECORD_BATCH_ROWS
        );
        assert_eq!(
            product_columnar_stream_record_batch_rows(PRODUCT_COLUMNAR_LARGE_STREAM_ROW_THRESHOLD),
            PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS
        );
        assert_eq!(
            product_columnar_stream_record_batch_rows(usize::MAX),
            PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS
        );
        assert!(PRODUCT_COLUMNAR_STREAM_RECORD_BATCH_ROWS > SCOPED_COMPAT_RECORD_BATCH_ROWS);
        assert!(
            PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS
                > PRODUCT_COLUMNAR_STREAM_RECORD_BATCH_ROWS
        );
    }

    #[test]
    fn text_rows_stream_source_builds_lazy_schema_stable_batches() {
        let header = vec!["id".to_string(), "label".to_string()];
        let rows = vec![
            vec![
                ("id".to_string(), ScalarValue::Int64(1)),
                ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
            ],
            vec![
                ("id".to_string(), ScalarValue::Int64(2)),
                ("label".to_string(), ScalarValue::Null),
            ],
            vec![
                ("id".to_string(), ScalarValue::Null),
                ("label".to_string(), ScalarValue::Utf8("gamma".to_string())),
            ],
            vec![
                ("id".to_string(), ScalarValue::Int64(4)),
                ("label".to_string(), ScalarValue::Utf8("delta".to_string())),
            ],
            vec![
                ("id".to_string(), ScalarValue::Int64(5)),
                (
                    "label".to_string(),
                    ScalarValue::Utf8("epsilon".to_string()),
                ),
            ],
        ];
        let mut source = stream_flat_text_rows_columnar_source(
            header.clone(),
            vec![None, None],
            vec![None, None],
            header.clone(),
            header,
            rows,
            2,
            "CSV",
        )
        .expect("text rows stream source");

        assert_eq!(source.row_count_hint, Some(5));
        assert_eq!(source.record_batch_count_hint, Some(3));
        assert_eq!(source.source_stream_batch_size, 2);
        assert_eq!(
            source.source_stream_policy,
            "typed_text_record_batch_stream_batch_size_65536_rows"
        );
        assert_eq!(
            source.ingest_executor_status,
            "lazy_typed_text_record_batch_builder"
        );
        assert_eq!(
            source.ingest_executor_kind,
            "lazy_text_rows_to_arrow_record_batch_reader"
        );

        let reader_schema = source.reader.schema();
        assert_eq!(reader_schema.field(0).data_type(), &DataType::Int64);
        assert!(reader_schema.field(0).is_nullable());
        assert_eq!(reader_schema.field(1).data_type(), &DataType::Utf8);
        assert!(reader_schema.field(1).is_nullable());

        let first = source
            .reader
            .next()
            .expect("first batch")
            .expect("first ok");
        let second = source
            .reader
            .next()
            .expect("second batch")
            .expect("second ok");
        let third = source
            .reader
            .next()
            .expect("third batch")
            .expect("third ok");
        assert_eq!(first.schema(), reader_schema);
        assert_eq!(second.schema(), reader_schema);
        assert_eq!(third.schema(), reader_schema);
        assert_eq!(first.num_rows(), 2);
        assert_eq!(second.num_rows(), 2);
        assert_eq!(third.num_rows(), 1);
        let second_ids = second
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("second id array");
        assert!(second_ids.is_null(0));
        assert_eq!(second_ids.value(1), 4);
        assert!(source.reader.next().is_none());
    }

    #[test]
    fn text_rows_stream_source_admits_large_product_batch_units() {
        let header = vec!["id".to_string(), "label".to_string()];
        let rows = (0..3)
            .map(|id| {
                vec![
                    ("id".to_string(), ScalarValue::Int64(id)),
                    (
                        "label".to_string(),
                        ScalarValue::Utf8(format!("label-{id}")),
                    ),
                ]
            })
            .collect::<Vec<_>>();
        let source = stream_flat_text_rows_columnar_source(
            header.clone(),
            vec![Some(LogicalDType::Int64), Some(LogicalDType::Utf8)],
            vec![Some(DataType::Int64), Some(DataType::Utf8)],
            header.clone(),
            header,
            rows,
            PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS,
            "CSV",
        )
        .expect("large product text stream source");

        assert_eq!(
            source.source_stream_batch_size,
            PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS
        );
        assert_eq!(source.record_batch_count_hint, Some(1));
        assert_eq!(
            source.source_stream_policy,
            "typed_text_record_batch_stream_batch_size_262144_rows"
        );
        assert_eq!(
            source.source_stream_unit_row_ranges.as_deref(),
            Some(&[(0, 3)][..])
        );
    }

    #[test]
    fn text_rows_stream_source_embeds_exact_hidden_string_metadata() {
        use arrow_array::DictionaryArray;

        let header = vec![
            "URL".to_string(),
            "Referer".to_string(),
            "SearchPhrase".to_string(),
            "PlainNote".to_string(),
            "raw_event_time".to_string(),
        ];
        let rows = vec![
            vec![
                (
                    "URL".to_string(),
                    ScalarValue::Utf8("https://www.example.com/a".to_string()),
                ),
                (
                    "Referer".to_string(),
                    ScalarValue::Utf8("http://www.google.com/search".to_string()),
                ),
                ("SearchPhrase".to_string(), ScalarValue::Utf8(String::new())),
                (
                    "PlainNote".to_string(),
                    ScalarValue::Utf8("plain".to_string()),
                ),
                (
                    "raw_event_time".to_string(),
                    ScalarValue::Utf8("not-a-timestamp".to_string()),
                ),
            ],
            vec![
                ("URL".to_string(), ScalarValue::Null),
                (
                    "Referer".to_string(),
                    ScalarValue::Utf8("https://docs.rs/crate".to_string()),
                ),
                (
                    "SearchPhrase".to_string(),
                    ScalarValue::Utf8("rust".to_string()),
                ),
                (
                    "PlainNote".to_string(),
                    ScalarValue::Utf8("note".to_string()),
                ),
                (
                    "raw_event_time".to_string(),
                    ScalarValue::Utf8("still dirty".to_string()),
                ),
            ],
        ];
        let mut source = stream_flat_text_rows_columnar_source(
            header.clone(),
            vec![None, None, None, None, None],
            vec![None, None, None, None, None],
            header.clone(),
            header,
            rows,
            64,
            "JSONL",
        )
        .expect("text rows stream source");

        let names = source
            .reader
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>();
        assert!(names.contains(&"__shardloom_derived_utf8_len_URL".to_string()));
        assert!(names.contains(&"__shardloom_derived_url_domain_URL".to_string()));
        assert!(names.contains(&"__shardloom_derived_utf8_len_Referer".to_string()));
        assert!(names.contains(&"__shardloom_derived_url_domain_Referer".to_string()));
        assert!(names.contains(&"__shardloom_derived_utf8_len_SearchPhrase".to_string()));
        assert!(
            !names.contains(&"__shardloom_derived_utf8_len_PlainNote".to_string()),
            "ordinary non-candidate text columns do not get hidden length columns by default"
        );
        assert!(
            !names.contains(&"__shardloom_derived_url_domain_SearchPhrase".to_string()),
            "non-URL text columns get exact length metadata, not URL-domain metadata"
        );
        assert!(
            !names.contains(&"__shardloom_derived_extract_minute_raw_event_time".to_string()),
            "dirty timestamp text columns must not get eager minute parsing metadata"
        );
        assert!(
            !names.contains(&"__shardloom_derived_date_trunc_minute_raw_event_time".to_string()),
            "dirty timestamp text columns must not get eager date-trunc metadata"
        );
        assert!(
            source
                .source_dictionary_preservation_status
                .contains("embedded_derived_columns=")
        );

        let batch = source.reader.next().expect("batch").expect("batch ok");
        let referer_len_index = names
            .iter()
            .position(|name| name == "__shardloom_derived_utf8_len_Referer")
            .expect("referer len");
        let referer_domain_index = names
            .iter()
            .position(|name| name == "__shardloom_derived_url_domain_Referer")
            .expect("referer domain");
        let search_len_index = names
            .iter()
            .position(|name| name == "__shardloom_derived_utf8_len_SearchPhrase")
            .expect("search len");
        let referer_lengths = batch
            .column(referer_len_index)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("referer dictionary lengths");
        let referer_length_values = referer_lengths
            .values()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("referer length dictionary values");
        assert_eq!(
            referer_length_values
                .value(dictionary_code_index(referer_lengths, 0).expect("row 0 length code")),
            u32::try_from("http://www.google.com/search".len()).expect("len")
        );
        assert_eq!(
            referer_length_values
                .value(dictionary_code_index(referer_lengths, 1).expect("row 1 length code")),
            u32::try_from("https://docs.rs/crate".len()).expect("len")
        );
        assert_eq!(
            referer_length_values.len(),
            2,
            "full-adapter derived lengths should be compact dictionary values, not a per-row value vector"
        );
        let referer_domains = batch
            .column(referer_domain_index)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("referer domains");
        let referer_domains = referer_domains
            .downcast_dict::<StringArray>()
            .expect("referer domain dictionary values");
        assert_eq!(referer_domains.value(0), "google.com");
        assert_eq!(referer_domains.value(1), "docs.rs");
        let search_lengths = batch
            .column(search_len_index)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("search dictionary lengths");
        let search_length_values = search_lengths
            .values()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("search length dictionary values");
        assert_eq!(
            search_length_values
                .value(dictionary_code_index(search_lengths, 0).expect("row 0 search length")),
            0
        );
        assert_eq!(
            search_length_values
                .value(dictionary_code_index(search_lengths, 1).expect("row 1 search length")),
            4
        );
    }

    #[test]
    fn source_native_dictionary_stream_embeds_url_metadata_without_row_string_synthesis() {
        use arrow_array::DictionaryArray;

        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder
            .append("https://www.google.com/search")
            .expect("append google");
        builder
            .append("https://docs.rs/crate")
            .expect("append docs");
        builder
            .append("https://www.google.com/search")
            .expect("append google repeat");
        builder.append_null();
        let url_dictionary = Arc::new(builder.finish()) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "URL",
            url_dictionary.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![url_dictionary])
            .expect("dictionary batch");
        let source = FlatLocalColumnarStreamSource {
            header: vec!["URL".to_string()],
            column_dtypes: vec![Some(LogicalDType::Utf8)],
            column_arrow_dtypes: vec![Some(schema.field(0).data_type().clone())],
            materialized_columns: vec!["URL".to_string()],
            reader_projection_columns: vec!["URL".to_string()],
            row_count_hint: Some(4),
            record_batch_count_hint: Some(1),
            source_stream_batch_size: 4,
            source_stream_unit_count_hint: Some(1),
            source_stream_unit_row_ranges: Some(vec![(0, 4)]),
            source_stream_unit_hint_kind: "test_dictionary_record_batch".to_string(),
            source_stream_policy: "test_dictionary_source_native_units".to_string(),
            source_dictionary_preservation_status: "test_dictionary_preservation_available"
                .to_string(),
            ingest_executor_status: "serial_pull_reader".to_string(),
            ingest_executor_kind: "test_dictionary_record_batch_reader".to_string(),
            ingest_executor_requested_parallelism: 1,
            ingest_executor_applied_parallelism: 1,
            ingest_executor_unit_count_hint: Some(1),
            reader: Box::new(VecRecordBatchReader::new(schema, VecDeque::from([batch]))),
        };

        let mut source = with_source_native_embedded_derived_columns_columnar_stream_source(source);

        assert!(
            source.source_dictionary_preservation_status.contains(
                "embedded_derived_column_mode=source_native_dictionary_or_typed_time_only"
            ),
            "{}",
            source.source_dictionary_preservation_status
        );
        assert!(
            !source
                .source_dictionary_preservation_status
                .contains("not_synthesized_source_native_columnar_adapter"),
            "{}",
            source.source_dictionary_preservation_status
        );
        let names = source
            .reader
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "URL".to_string(),
                "__shardloom_derived_utf8_len_URL".to_string(),
                "__shardloom_derived_url_domain_URL".to_string()
            ]
        );
        let batch = source.reader.next().expect("batch").expect("batch ok");
        let lengths = batch
            .column(1)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("dictionary-backed lengths");
        let lengths = lengths
            .downcast_dict::<UInt32Array>()
            .expect("length values");
        assert_eq!(
            lengths.value(0),
            u32::try_from("https://www.google.com/search".len()).expect("len")
        );
        assert_eq!(
            lengths.value(1),
            u32::try_from("https://docs.rs/crate".len()).expect("len")
        );
        assert_eq!(lengths.value(2), lengths.value(0));
        assert!(lengths.is_null(3));

        let domains = batch
            .column(2)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("domains");
        let domain_values = domains
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("domain values");
        assert_eq!(
            domain_values.len(),
            2,
            "hidden domain dictionary should collapse repeated URL domains"
        );
        assert_eq!(
            domain_values.value(dictionary_code_index(domains, 0).expect("row 0 domain code")),
            "google.com"
        );
        assert_eq!(
            domain_values.value(dictionary_code_index(domains, 1).expect("row 1 domain code")),
            "docs.rs"
        );
        assert_eq!(
            domain_values.value(dictionary_code_index(domains, 2).expect("row 2 domain code")),
            "google.com"
        );
        assert!(domains.is_null(3));
        assert!(source.reader.next().is_none());
    }

    #[test]
    fn source_native_dictionary_stream_preserves_non_i32_dictionary_key_metadata() {
        use arrow_array::DictionaryArray;

        let mut builder = StringDictionaryBuilder::<UInt8Type>::new();
        builder
            .append("https://example.com/a")
            .expect("append example");
        builder
            .append("https://docs.rs/crate")
            .expect("append docs");
        builder
            .append("https://example.com/a")
            .expect("append example repeat");
        let url_dictionary = Arc::new(builder.finish()) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "URL",
            url_dictionary.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![url_dictionary])
            .expect("dictionary batch");
        let source = FlatLocalColumnarStreamSource {
            header: vec!["URL".to_string()],
            column_dtypes: vec![Some(LogicalDType::Utf8)],
            column_arrow_dtypes: vec![Some(schema.field(0).data_type().clone())],
            materialized_columns: vec!["URL".to_string()],
            reader_projection_columns: vec!["URL".to_string()],
            row_count_hint: Some(3),
            record_batch_count_hint: Some(1),
            source_stream_batch_size: 3,
            source_stream_unit_count_hint: Some(1),
            source_stream_unit_row_ranges: Some(vec![(0, 3)]),
            source_stream_unit_hint_kind: "test_u8_dictionary_record_batch".to_string(),
            source_stream_policy: "test_u8_dictionary_source_native_units".to_string(),
            source_dictionary_preservation_status: "test_dictionary_preservation_available"
                .to_string(),
            ingest_executor_status: "serial_pull_reader".to_string(),
            ingest_executor_kind: "test_dictionary_record_batch_reader".to_string(),
            ingest_executor_requested_parallelism: 1,
            ingest_executor_applied_parallelism: 1,
            ingest_executor_unit_count_hint: Some(1),
            reader: Box::new(VecRecordBatchReader::new(schema, VecDeque::from([batch]))),
        };

        let mut source = with_source_native_embedded_derived_columns_columnar_stream_source(source);

        assert_eq!(
            source
                .reader
                .schema()
                .field_with_name("__shardloom_derived_utf8_len_URL")
                .expect("length field")
                .data_type(),
            &DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::UInt32))
        );
        assert_eq!(
            source
                .reader
                .schema()
                .field_with_name("__shardloom_derived_url_domain_URL")
                .expect("domain field")
                .data_type(),
            &DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8))
        );
        let batch = source.reader.next().expect("batch").expect("batch ok");
        let lengths = batch
            .column(1)
            .as_any()
            .downcast_ref::<DictionaryArray<UInt8Type>>()
            .expect("u8 length dictionary");
        let lengths = lengths
            .downcast_dict::<UInt32Array>()
            .expect("length values");
        assert_eq!(
            lengths.value(0),
            u32::try_from("https://example.com/a".len()).expect("len")
        );
        assert_eq!(
            lengths.value(1),
            u32::try_from("https://docs.rs/crate".len()).expect("len")
        );
        assert_eq!(lengths.value(2), lengths.value(0));

        let domains = batch
            .column(2)
            .as_any()
            .downcast_ref::<DictionaryArray<UInt8Type>>()
            .expect("u8 domain dictionary");
        let domain_values = domains
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("domain values");
        assert_eq!(domain_values.len(), 2);
        assert_eq!(
            domain_values.value(dictionary_code_index(domains, 0).expect("row 0 domain code")),
            "example.com"
        );
        assert_eq!(
            domain_values.value(dictionary_code_index(domains, 1).expect("row 1 domain code")),
            "docs.rs"
        );
        assert_eq!(
            domain_values.value(dictionary_code_index(domains, 2).expect("row 2 domain code")),
            "example.com"
        );
        assert!(source.reader.next().is_none());
    }

    #[test]
    fn source_native_typed_time_stream_embeds_compact_minute_metadata() {
        let event_time = Arc::new(Int64Array::from(vec![0_i64, 60, 3599, 3600])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "EventTime",
            DataType::Int64,
            true,
        )]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![event_time]).expect("time batch");
        let source = FlatLocalColumnarStreamSource {
            header: vec!["EventTime".to_string()],
            column_dtypes: vec![Some(LogicalDType::Int64)],
            column_arrow_dtypes: vec![Some(DataType::Int64)],
            materialized_columns: vec!["EventTime".to_string()],
            reader_projection_columns: vec!["EventTime".to_string()],
            row_count_hint: Some(4),
            record_batch_count_hint: Some(1),
            source_stream_batch_size: 4,
            source_stream_unit_count_hint: Some(1),
            source_stream_unit_row_ranges: Some(vec![(0, 4)]),
            source_stream_unit_hint_kind: "test_typed_time_record_batch".to_string(),
            source_stream_policy: "test_typed_time_source_native_units".to_string(),
            source_dictionary_preservation_status: "test_typed_time_source_native".to_string(),
            ingest_executor_status: "serial_pull_reader".to_string(),
            ingest_executor_kind: "test_typed_time_record_batch_reader".to_string(),
            ingest_executor_requested_parallelism: 1,
            ingest_executor_applied_parallelism: 1,
            ingest_executor_unit_count_hint: Some(1),
            reader: Box::new(VecRecordBatchReader::new(schema, VecDeque::from([batch]))),
        };

        let mut source = with_source_native_embedded_derived_columns_columnar_stream_source(source);

        let names = source
            .reader
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "EventTime".to_string(),
                "__shardloom_derived_extract_minute_EventTime".to_string(),
                "__shardloom_derived_date_trunc_minute_EventTime".to_string()
            ]
        );
        let batch = source.reader.next().expect("batch").expect("batch ok");
        let minutes = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("minute keys");
        assert_eq!(minutes.value(0), 0);
        assert_eq!(minutes.value(1), 1);
        assert_eq!(minutes.value(2), 59);
        assert_eq!(minutes.value(3), 0);
        let minute_buckets = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("minute bucket keys");
        assert_eq!(minute_buckets.value(0), 0);
        assert_eq!(minute_buckets.value(1), 60);
        assert_eq!(minute_buckets.value(2), 3_540);
        assert_eq!(minute_buckets.value(3), 3_600);
        assert!(source.reader.next().is_none());
    }

    #[test]
    fn source_native_timestamp_units_embed_compact_minute_metadata() {
        let seconds =
            Arc::new(TimestampSecondArray::from(vec![0_i64, 60, 3_599, 3_600])) as ArrayRef;
        let millis = Arc::new(TimestampMillisecondArray::from(vec![
            0_i64, 60_000, 3_599_000, 3_600_000,
        ])) as ArrayRef;
        let nanos = Arc::new(TimestampNanosecondArray::from(vec![
            0_i64,
            60_000_000_000,
            3_599_000_000_000,
            3_600_000_000_000,
        ])) as ArrayRef;
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "EventTime",
                DataType::Timestamp(TimeUnit::Second, None),
                true,
            ),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "click_timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![seconds, millis, nanos])
            .expect("timestamp batch");
        let source = FlatLocalColumnarStreamSource {
            header: vec![
                "EventTime".to_string(),
                "event_time".to_string(),
                "click_timestamp".to_string(),
            ],
            column_dtypes: vec![
                Some(LogicalDType::TimestampMicros),
                Some(LogicalDType::TimestampMicros),
                Some(LogicalDType::TimestampMicros),
            ],
            column_arrow_dtypes: vec![
                Some(DataType::Timestamp(TimeUnit::Second, None)),
                Some(DataType::Timestamp(TimeUnit::Millisecond, None)),
                Some(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            ],
            materialized_columns: vec![
                "EventTime".to_string(),
                "event_time".to_string(),
                "click_timestamp".to_string(),
            ],
            reader_projection_columns: vec![
                "EventTime".to_string(),
                "event_time".to_string(),
                "click_timestamp".to_string(),
            ],
            row_count_hint: Some(4),
            record_batch_count_hint: Some(1),
            source_stream_batch_size: 4,
            source_stream_unit_count_hint: Some(1),
            source_stream_unit_row_ranges: Some(vec![(0, 4)]),
            source_stream_unit_hint_kind: "test_timestamp_units_record_batch".to_string(),
            source_stream_policy: "test_timestamp_units_source_native_units".to_string(),
            source_dictionary_preservation_status: "test_timestamp_units_source_native".to_string(),
            ingest_executor_status: "serial_pull_reader".to_string(),
            ingest_executor_kind: "test_timestamp_units_record_batch_reader".to_string(),
            ingest_executor_requested_parallelism: 1,
            ingest_executor_applied_parallelism: 1,
            ingest_executor_unit_count_hint: Some(1),
            reader: Box::new(VecRecordBatchReader::new(schema, VecDeque::from([batch]))),
        };

        let mut source = with_source_native_embedded_derived_columns_columnar_stream_source(source);

        let names = source
            .reader
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "EventTime".to_string(),
                "event_time".to_string(),
                "click_timestamp".to_string(),
                "__shardloom_derived_extract_minute_EventTime".to_string(),
                "__shardloom_derived_date_trunc_minute_EventTime".to_string(),
                "__shardloom_derived_extract_minute_event_time".to_string(),
                "__shardloom_derived_date_trunc_minute_event_time".to_string(),
                "__shardloom_derived_extract_minute_click_timestamp".to_string(),
                "__shardloom_derived_date_trunc_minute_click_timestamp".to_string(),
            ]
        );
        let batch = source.reader.next().expect("batch").expect("batch ok");
        for column_index in [3, 5, 7] {
            let minutes = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<UInt8Array>()
                .expect("minute keys");
            assert_eq!(minutes.value(0), 0);
            assert_eq!(minutes.value(1), 1);
            assert_eq!(minutes.value(2), 59);
            assert_eq!(minutes.value(3), 0);
        }
        for column_index in [4, 6, 8] {
            let minute_buckets = batch
                .column(column_index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("minute bucket keys");
            assert_eq!(minute_buckets.value(0), 0);
            assert_eq!(minute_buckets.value(1), 60);
            assert_eq!(minute_buckets.value(2), 3_540);
            assert_eq!(minute_buckets.value(3), 3_600);
        }
        assert!(source.reader.next().is_none());
    }

    #[test]
    fn text_rows_stream_source_rejects_reserved_hidden_derived_columns() {
        let header = vec![
            "URL".to_string(),
            "__shardloom_derived_url_domain_URL".to_string(),
        ];
        let rows = vec![vec![
            (
                "URL".to_string(),
                ScalarValue::Utf8("https://example.com/path".to_string()),
            ),
            (
                "__shardloom_derived_url_domain_URL".to_string(),
                ScalarValue::Utf8("attacker-controlled.example".to_string()),
            ),
        ]];

        let error = match stream_flat_text_rows_columnar_source(
            header.clone(),
            vec![None, None],
            vec![None, None],
            header.clone(),
            header,
            rows,
            64,
            "JSONL",
        ) {
            Ok(_) => panic!("reserved hidden derived columns must be rejected"),
            Err(error) => error,
        };

        let message = error.to_string();
        assert!(
            message.contains("reserved ShardLoom hidden derived column"),
            "{message}"
        );
        assert!(message.contains("no fallback execution was attempted"));
    }

    #[test]
    fn schema_stable_record_batch_rejects_reserved_hidden_derived_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("URL", DataType::Utf8, true),
            Field::new("__shardloom_derived_url_domain_URL", DataType::Utf8, true),
        ]));
        let columns = vec![
            "URL".to_string(),
            "__shardloom_derived_url_domain_URL".to_string(),
        ];
        let rows = vec![vec![
            (
                "URL".to_string(),
                ScalarValue::Utf8("https://example.com/path".to_string()),
            ),
            (
                "__shardloom_derived_url_domain_URL".to_string(),
                ScalarValue::Utf8("attacker-controlled.example".to_string()),
            ),
        ]];

        let error =
            match flat_rows_to_record_batch_with_schema(schema, &columns, &rows, "schema batch") {
                Ok(_) => panic!("reserved hidden derived columns must be rejected"),
                Err(error) => error,
            };

        let message = error.to_string();
        assert!(
            message.contains("reserved ShardLoom hidden derived column"),
            "{message}"
        );
        assert!(message.contains("no fallback execution was attempted"));
    }

    #[test]
    fn capillary_prefetch_stream_source_preserves_order_and_records_executor() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch_1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .expect("first batch");
        let batch_2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![3]))],
        )
        .expect("second batch");
        let source = FlatLocalColumnarStreamSource {
            header: vec!["id".to_string()],
            column_dtypes: vec![Some(LogicalDType::Int64)],
            column_arrow_dtypes: vec![Some(DataType::Int64)],
            materialized_columns: vec!["id".to_string()],
            reader_projection_columns: vec!["id".to_string()],
            row_count_hint: Some(3),
            record_batch_count_hint: Some(2),
            source_stream_batch_size: 0,
            source_stream_unit_count_hint: Some(2),
            source_stream_unit_row_ranges: Some(vec![(0, 2), (2, 3)]),
            source_stream_unit_hint_kind: "test_record_batch_count".to_string(),
            source_stream_policy: "test_source_defined_record_batches".to_string(),
            source_dictionary_preservation_status: "test_not_applicable".to_string(),
            ingest_executor_status: "serial_pull_reader".to_string(),
            ingest_executor_kind: "test_record_batch_reader".to_string(),
            ingest_executor_requested_parallelism: 1,
            ingest_executor_applied_parallelism: 1,
            ingest_executor_unit_count_hint: Some(2),
            reader: Box::new(TestRecordBatchReader {
                schema,
                batches: std::collections::VecDeque::from([batch_1, batch_2]),
            }),
        };

        let mut source = with_capillary_prefetch_columnar_stream_source(source, 4);

        assert_eq!(
            source.ingest_executor_status,
            "bounded_capillary_prefetch_active"
        );
        assert_eq!(
            source.ingest_executor_kind,
            "source_reader_to_vortex_writer_prefetch_pipeline"
        );
        assert_eq!(source.ingest_executor_requested_parallelism, 4);
        assert_eq!(source.ingest_executor_applied_parallelism, 2);
        assert_eq!(source.ingest_executor_unit_count_hint, Some(2));
        assert_eq!(source.source_stream_batch_size, 0);
        assert_eq!(source.source_stream_unit_count_hint, Some(2));
        assert_eq!(
            source.source_stream_unit_row_ranges.as_deref(),
            Some(&[(0, 2), (2, 3)][..])
        );
        assert_eq!(
            source.source_stream_unit_hint_kind,
            "test_record_batch_count"
        );
        assert_eq!(
            source.source_stream_policy,
            "test_source_defined_record_batches"
        );
        assert_eq!(
            source.source_dictionary_preservation_status,
            "test_not_applicable"
        );
        let first = source
            .reader
            .next()
            .expect("first batch")
            .expect("first batch ok");
        let second = source
            .reader
            .next()
            .expect("second batch")
            .expect("second batch ok");
        assert_eq!(first.num_rows(), 2);
        assert_eq!(second.num_rows(), 1);
        assert!(source.reader.next().is_none());
    }

    #[test]
    fn capillary_prefetch_uses_default_second_lane_for_bounded_source_overlap() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("batch");
        let source = FlatLocalColumnarStreamSource {
            header: vec!["id".to_string()],
            column_dtypes: vec![Some(LogicalDType::Int64)],
            column_arrow_dtypes: vec![Some(DataType::Int64)],
            materialized_columns: vec!["id".to_string()],
            reader_projection_columns: vec!["id".to_string()],
            row_count_hint: Some(3),
            record_batch_count_hint: Some(1),
            source_stream_batch_size: 0,
            source_stream_unit_count_hint: Some(1),
            source_stream_unit_row_ranges: Some(vec![(0, 3)]),
            source_stream_unit_hint_kind: "test_record_batch_count".to_string(),
            source_stream_policy: "test_source_defined_record_batches".to_string(),
            source_dictionary_preservation_status: "test_not_applicable".to_string(),
            ingest_executor_status: "serial_pull_reader".to_string(),
            ingest_executor_kind: "test_record_batch_reader".to_string(),
            ingest_executor_requested_parallelism: 1,
            ingest_executor_applied_parallelism: 1,
            ingest_executor_unit_count_hint: Some(1),
            reader: Box::new(TestRecordBatchReader {
                schema,
                batches: std::collections::VecDeque::from([batch]),
            }),
        };

        let mut source = with_capillary_prefetch_columnar_stream_source(source, 2);

        assert_eq!(
            source.ingest_executor_status,
            "bounded_capillary_prefetch_active"
        );
        assert_eq!(
            source.ingest_executor_kind,
            "source_reader_to_vortex_writer_prefetch_pipeline"
        );
        assert_eq!(source.ingest_executor_requested_parallelism, 2);
        assert_eq!(source.ingest_executor_applied_parallelism, 1);
        assert_eq!(
            source.source_stream_unit_row_ranges.as_deref(),
            Some(&[(0, 3)][..])
        );
        let batch = source.reader.next().expect("batch").expect("batch ok");
        assert_eq!(batch.num_rows(), 3);
        assert!(source.reader.next().is_none());
    }

    #[test]
    fn capillary_prefetch_does_not_double_wrap_existing_capillary_source() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("batch");
        let source = FlatLocalColumnarStreamSource {
            header: vec!["id".to_string()],
            column_dtypes: vec![Some(LogicalDType::Int64)],
            column_arrow_dtypes: vec![Some(DataType::Int64)],
            materialized_columns: vec!["id".to_string()],
            reader_projection_columns: vec!["id".to_string()],
            row_count_hint: Some(3),
            record_batch_count_hint: Some(1),
            source_stream_batch_size: 0,
            source_stream_unit_count_hint: Some(1),
            source_stream_unit_row_ranges: Some(vec![(0, 3)]),
            source_stream_unit_hint_kind: "test_record_batch_count".to_string(),
            source_stream_policy: "test_source_defined_record_batches".to_string(),
            source_dictionary_preservation_status: "test_not_applicable".to_string(),
            ingest_executor_status: "bounded_capillary_prefetch_active".to_string(),
            ingest_executor_kind: "source_reader_to_vortex_writer_prefetch_pipeline".to_string(),
            ingest_executor_requested_parallelism: 2,
            ingest_executor_applied_parallelism: 1,
            ingest_executor_unit_count_hint: Some(1),
            reader: Box::new(TestRecordBatchReader {
                schema,
                batches: std::collections::VecDeque::from([batch]),
            }),
        };

        let mut source = with_capillary_prefetch_columnar_stream_source(source, 8);

        assert_eq!(
            source.ingest_executor_status,
            "bounded_capillary_prefetch_active"
        );
        assert_eq!(
            source.ingest_executor_kind,
            "source_reader_to_vortex_writer_prefetch_pipeline"
        );
        assert_eq!(source.ingest_executor_requested_parallelism, 2);
        assert_eq!(source.ingest_executor_applied_parallelism, 1);
        assert_eq!(source.ingest_executor_unit_count_hint, Some(1));
        let batch = source.reader.next().expect("batch").expect("batch ok");
        assert_eq!(batch.num_rows(), 3);
        assert!(source.reader.next().is_none());
    }

    #[test]
    fn parquet_row_group_task_builder_coalesces_tiny_groups_by_row_budget() {
        let rows = vec![1_024; 128];
        let tasks = parquet_row_group_read_tasks(
            rows.len(),
            Some(&rows),
            PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS,
        );

        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].task_index, 0);
        assert_eq!(tasks[0].row_groups, (0..64).collect::<Vec<_>>());
        assert_eq!(tasks[1].task_index, 1);
        assert_eq!(tasks[1].row_groups, (64..128).collect::<Vec<_>>());
    }

    #[test]
    fn parquet_row_group_task_builder_splits_large_groups_by_row_budget() {
        let rows = vec![PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS; 12];
        let tasks = parquet_row_group_read_tasks(
            rows.len(),
            Some(&rows),
            PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS,
        );

        assert_eq!(tasks.len(), 3);
        assert_eq!(tasks[0].row_groups, vec![0, 1, 2, 3]);
        assert_eq!(tasks[1].row_groups, vec![4, 5, 6, 7]);
        assert_eq!(tasks[2].row_groups, vec![8, 9, 10, 11]);
    }

    #[test]
    fn parquet_row_group_task_builder_falls_back_to_fixed_groups_without_metadata() {
        let tasks = parquet_row_group_read_tasks(
            PARQUET_PARALLEL_ROW_GROUPS_PER_TASK + 1,
            None,
            PRODUCT_COLUMNAR_LARGE_STREAM_RECORD_BATCH_ROWS,
        );

        assert_eq!(tasks.len(), 2);
        assert_eq!(
            tasks[0].row_groups,
            (0..PARQUET_PARALLEL_ROW_GROUPS_PER_TASK).collect::<Vec<_>>()
        );
        assert_eq!(
            tasks[1].row_groups,
            vec![PARQUET_PARALLEL_ROW_GROUPS_PER_TASK]
        );
    }

    #[test]
    fn parquet_row_group_task_row_ranges_report_adaptive_source_units() {
        let tasks = vec![
            ParquetRowGroupReadTask {
                task_index: 0,
                row_groups: vec![0, 1, 2],
            },
            ParquetRowGroupReadTask {
                task_index: 1,
                row_groups: vec![3, 4],
            },
        ];
        let row_group_ranges = vec![(0, 2), (2, 5), (5, 8), (8, 11), (11, 12)];

        let task_ranges = parquet_row_group_task_row_ranges(&tasks, Some(&row_group_ranges))
            .expect("task ranges");

        assert_eq!(task_ranges, vec![(0, 8), (8, 12)]);
    }

    #[test]
    fn parquet_row_group_parallel_stream_preserves_order_and_records_executor() {
        let path = std::env::temp_dir().join(format!(
            "shardloom-parquet-row-group-parallel-{}-{}.parquet",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let expected_ids: Vec<i64> = (0..40).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(expected_ids.clone()))],
        )
        .expect("test batch");
        let props = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let file = File::create(&path).expect("create parquet");
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))
                .expect("parquet writer");
        writer.write(&batch).expect("write parquet batch");
        writer.close().expect("close parquet writer");

        let mut source = stream_flat_parquet_columnar_source_with_parallelism(&path, usize::MAX, 3)
            .expect("stream parquet");

        assert_eq!(
            source.ingest_executor_status,
            "bounded_capillary_row_group_parallel_writer_budgeted"
        );
        assert_eq!(
            source.ingest_executor_kind,
            "parquet_row_group_adaptive_coalesced_reader_to_vortex_writer_with_writer_slot_reserved"
        );
        assert_eq!(source.ingest_executor_requested_parallelism, 3);
        assert_eq!(source.ingest_executor_applied_parallelism, 1);
        assert_eq!(source.ingest_executor_unit_count_hint, Some(1));
        assert_eq!(source.source_stream_unit_count_hint, Some(1));
        assert_eq!(
            source.source_stream_unit_row_ranges.as_ref().map(Vec::len),
            Some(1)
        );
        assert_eq!(
            source
                .source_stream_unit_row_ranges
                .as_ref()
                .and_then(|ranges| ranges.first().copied()),
            Some((0, 40))
        );
        assert_eq!(
            source.source_stream_unit_hint_kind,
            "parquet_adaptive_row_group_task_count"
        );
        assert_eq!(
            source.source_stream_policy,
            "product_columnar_stream_batch_size_262144_rows"
        );

        let mut ids = Vec::new();
        while let Some(batch) = source.reader.next() {
            let batch = batch.expect("row group batch");
            let id_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id array");
            for index in 0..id_array.len() {
                ids.push(id_array.value(index));
            }
        }
        assert_eq!(ids, expected_ids);

        std::fs::remove_file(path).expect("remove parquet test file");
    }

    #[test]
    fn parquet_row_group_stream_uses_default_second_lane_for_bounded_source_overlap() {
        let path = std::env::temp_dir().join(format!(
            "shardloom-parquet-row-group-writer-budget-{}-{}.parquet",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![0_i64, 1, 2, 3]))],
        )
        .expect("test batch");
        let props = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .build();
        let file = File::create(&path).expect("create parquet");
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))
                .expect("parquet writer");
        writer.write(&batch).expect("write parquet batch");
        writer.close().expect("close parquet writer");

        let mut source = stream_flat_parquet_columnar_source_with_parallelism(&path, usize::MAX, 2)
            .expect("stream parquet");

        assert_eq!(
            source.ingest_executor_status,
            "bounded_capillary_row_group_parallel_writer_budgeted"
        );
        assert_eq!(
            source.ingest_executor_kind,
            "parquet_row_group_adaptive_coalesced_reader_to_vortex_writer_with_writer_slot_reserved"
        );
        assert_eq!(source.ingest_executor_requested_parallelism, 2);
        assert_eq!(source.ingest_executor_applied_parallelism, 1);
        assert_eq!(source.ingest_executor_unit_count_hint, Some(1));
        assert_eq!(source.source_stream_unit_count_hint, Some(1));
        assert_eq!(
            source.source_stream_unit_row_ranges.as_ref().map(Vec::len),
            Some(1)
        );
        assert_eq!(
            source
                .source_stream_unit_row_ranges
                .as_ref()
                .and_then(|ranges| ranges.first().copied()),
            Some((0, 4))
        );
        assert_eq!(
            source.source_stream_unit_hint_kind,
            "parquet_adaptive_row_group_task_count"
        );
        let first = source
            .reader
            .next()
            .expect("first batch")
            .expect("first batch ok");
        assert_eq!(first.num_rows(), 4);
        assert!(source.reader.next().is_none());

        std::fs::remove_file(path).expect("remove parquet test file");
    }

    #[test]
    fn parquet_product_stream_reports_no_physical_derived_column_synthesis() {
        let path = std::env::temp_dir().join(format!(
            "shardloom-parquet-derived-posture-{}-{}.parquet",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        let schema = Arc::new(Schema::new(vec![Field::new("URL", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![
                Some("https://www.example.com/a"),
                Some("https://docs.rs/crate"),
            ]))],
        )
        .expect("test batch");
        let file = File::create(&path).expect("create parquet");
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, Arc::clone(&schema), None)
            .expect("parquet writer");
        writer.write(&batch).expect("write parquet batch");
        writer.close().expect("close parquet writer");

        let mut source = stream_flat_parquet_columnar_source_with_parallelism(&path, usize::MAX, 2)
            .expect("stream parquet");

        assert!(
            source.source_dictionary_preservation_status.contains(
                "embedded_derived_columns=not_synthesized_source_native_columnar_adapter"
            ),
            "{}",
            source.source_dictionary_preservation_status
        );
        let names = source
            .reader
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["URL".to_string()]);
        let batch = source
            .reader
            .next()
            .expect("first parquet batch")
            .expect("first parquet batch ok");
        assert_eq!(batch.num_columns(), 1);

        std::fs::remove_file(path).expect("remove parquet test file");
    }

    #[test]
    fn parquet_stream_rejects_reserved_hidden_derived_columns() {
        let path = std::env::temp_dir().join(format!(
            "shardloom-parquet-hidden-derived-collision-{}-{}.parquet",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("URL", DataType::Utf8, true),
            Field::new("__shardloom_derived_url_domain_URL", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![Some("https://example.com/path")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("attacker-controlled.example")])) as ArrayRef,
            ],
        )
        .expect("test batch");
        let file = File::create(&path).expect("create parquet");
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, Arc::clone(&schema), None)
            .expect("parquet writer");
        writer.write(&batch).expect("write parquet batch");
        writer.close().expect("close parquet writer");

        let error = match stream_flat_parquet_columnar_source_with_parallelism(&path, usize::MAX, 2)
        {
            Ok(_) => panic!("reserved hidden derived columns must be rejected"),
            Err(error) => error,
        };

        let message = error.to_string();
        assert!(
            message.contains("reserved ShardLoom hidden derived column"),
            "{message}"
        );
        assert!(message.contains("no fallback execution was attempted"));

        std::fs::remove_file(path).expect("remove parquet test file");
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
            column_arrow_dtypes: vec![Some(DataType::Decimal128(10, 2))],
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

    #[test]
    fn materializes_columnar_nested_source_dtypes_as_scalar_complex_values() {
        use arrow_array::types::Int64Type;

        let values = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2), None]),
            None,
            Some(vec![]),
        ]));
        let payload = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("label", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("alpha"), None, Some("empty")])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("amount", DataType::Int64, true)),
                Arc::new(Int64Array::from(vec![Some(8), Some(15), None])) as ArrayRef,
            ),
        ]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("values", values.data_type().clone(), true),
            Field::new("payload", payload.data_type().clone(), true),
        ]));
        let column_arrow_dtypes = source_schema_column_arrow_dtypes(schema.as_ref());
        let batch = RecordBatch::try_new(schema, vec![values, payload]).expect("record batch");
        let source = FlatLocalColumnarSource {
            header: vec!["values".to_string(), "payload".to_string()],
            column_dtypes: vec![Some(LogicalDType::List), Some(LogicalDType::Struct)],
            column_arrow_dtypes,
            materialized_columns: vec!["values".to_string(), "payload".to_string()],
            reader_projection_columns: vec!["values".to_string(), "payload".to_string()],
            row_count: batch.num_rows(),
            batches: vec![batch],
        };

        let table = materialize_flat_columnar_source_to_scalar_table(
            &source,
            Path::new("target/nested.arrow"),
            "Arrow IPC",
        )
        .expect("materialize nested columns");

        assert_eq!(table.column_dtypes, source.column_dtypes);
        assert_eq!(
            table.rows[0].get("values"),
            Some(&ScalarValue::List(vec![
                ScalarValue::Int64(1),
                ScalarValue::Int64(2),
                ScalarValue::Null,
            ]))
        );
        assert_eq!(table.rows[1].get("values"), Some(&ScalarValue::Null));
        assert_eq!(
            table.rows[2].get("values"),
            Some(&ScalarValue::List(Vec::new()))
        );
        assert_eq!(
            table.rows[0].get("payload"),
            Some(&ScalarValue::Struct(vec![
                ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
                ("amount".to_string(), ScalarValue::Int64(8)),
            ]))
        );
        assert_eq!(
            table.rows[1].get("payload"),
            Some(&ScalarValue::Struct(vec![
                ("label".to_string(), ScalarValue::Null),
                ("amount".to_string(), ScalarValue::Int64(15)),
            ]))
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

    fn unique_nested_sink_path(extension: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "shardloom-vortex-nested-sink-{}-{nanos}.{extension}",
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

    fn nested_sink_rows() -> (Vec<String>, Vec<Option<LogicalDType>>, FlatSinkRows) {
        (
            vec!["values".to_string(), "payload".to_string()],
            vec![Some(LogicalDType::List), Some(LogicalDType::Struct)],
            vec![
                vec![
                    (
                        "values".to_string(),
                        ScalarValue::List(vec![
                            ScalarValue::Int64(1),
                            ScalarValue::Int64(2),
                            ScalarValue::Null,
                        ]),
                    ),
                    (
                        "payload".to_string(),
                        ScalarValue::Struct(vec![
                            ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
                            ("amount".to_string(), ScalarValue::Int64(8)),
                        ]),
                    ),
                ],
                vec![
                    ("values".to_string(), ScalarValue::Null),
                    ("payload".to_string(), ScalarValue::Null),
                ],
                vec![
                    ("values".to_string(), ScalarValue::List(Vec::new())),
                    (
                        "payload".to_string(),
                        ScalarValue::Struct(vec![
                            ("label".to_string(), ScalarValue::Null),
                            ("amount".to_string(), ScalarValue::Int64(15)),
                        ]),
                    ),
                ],
            ],
        )
    }

    fn assert_nested_sink_round_trip(
        extension: &str,
        encode: TypedSinkEncoder,
        read: fn(&std::path::Path, usize) -> Result<FlatLocalSourceTable>,
    ) {
        let (columns, dtypes, rows) = nested_sink_rows();
        let path = unique_nested_sink_path(extension);
        let bytes = encode(&columns, &dtypes, &rows).expect("encode nested sink rows");
        std::fs::write(&path, bytes).expect("write nested sink artifact");

        let table = read(&path, 10).expect("read nested sink artifact");
        assert_eq!(table.header, columns);
        assert_eq!(table.column_dtypes, dtypes);
        assert_eq!(table.rows.len(), 3);
        assert_eq!(
            table.rows[0].get("values"),
            Some(&ScalarValue::List(vec![
                ScalarValue::Int64(1),
                ScalarValue::Int64(2),
                ScalarValue::Null,
            ]))
        );
        assert_eq!(table.rows[1].get("values"), Some(&ScalarValue::Null));
        assert_eq!(
            table.rows[2].get("values"),
            Some(&ScalarValue::List(Vec::new()))
        );
        assert_eq!(
            table.rows[0].get("payload"),
            Some(&ScalarValue::Struct(vec![
                ("label".to_string(), ScalarValue::Utf8("alpha".to_string())),
                ("amount".to_string(), ScalarValue::Int64(8)),
            ]))
        );
        assert_eq!(table.rows[1].get("payload"), Some(&ScalarValue::Null));
        assert_eq!(
            table.rows[2].get("payload"),
            Some(&ScalarValue::Struct(vec![
                ("label".to_string(), ScalarValue::Null),
                ("amount".to_string(), ScalarValue::Int64(15)),
            ]))
        );

        std::fs::remove_file(path).expect("remove nested sink artifact");
    }

    #[test]
    fn preserves_nested_rows_in_feature_gated_typed_structured_sinks() {
        assert_nested_sink_round_trip(
            "parquet",
            encode_flat_parquet_rows_with_dtypes,
            read_flat_parquet_source,
        );
        assert_nested_sink_round_trip(
            "arrow",
            encode_flat_arrow_ipc_rows_with_dtypes,
            read_flat_arrow_ipc_source,
        );
        assert_nested_sink_round_trip(
            "avro",
            encode_flat_avro_rows_with_dtypes,
            read_flat_avro_source,
        );
    }

    #[test]
    fn all_null_complex_sink_without_child_schema_blocks_before_writer_conversion() {
        let columns = vec!["values".to_string()];
        let dtypes = vec![Some(LogicalDType::List)];
        let rows = vec![vec![("values".to_string(), ScalarValue::Null)]];

        let error = flat_rows_to_record_batch_with_dtypes(
            &columns,
            &dtypes,
            &[None],
            &rows,
            "nested dtype hint output",
        )
        .expect_err("all-null complex output should require child schema evidence");

        assert!(
            error
                .to_string()
                .contains("output_plan_conversion_blocker=nested_sink_schema_inference_required"),
            "{error}"
        );
        assert!(
            error
                .to_string()
                .contains("no fallback execution was attempted")
        );
    }

    #[test]
    fn all_null_complex_sink_with_child_schema_builds_record_batch() {
        let columns = vec!["values".to_string(), "payload".to_string()];
        let dtypes = vec![Some(LogicalDType::List), Some(LogicalDType::Struct)];
        let arrow_dtypes = vec![
            Some(DataType::List(Arc::new(Field::new_list_field(
                DataType::Int64,
                true,
            )))),
            Some(DataType::Struct(Fields::from(vec![
                Field::new("label", DataType::Utf8, true),
                Field::new("amount", DataType::Int64, true),
            ]))),
        ];
        let rows = vec![vec![
            ("values".to_string(), ScalarValue::Null),
            ("payload".to_string(), ScalarValue::Null),
        ]];

        let batch = flat_rows_to_record_batch_with_dtypes(
            &columns,
            &dtypes,
            &arrow_dtypes,
            &rows,
            "nested source schema hint output",
        )
        .expect("source child schema evidence builds all-null typed nested batch");

        assert_eq!(
            batch.schema().field(0).data_type(),
            arrow_dtypes[0].as_ref().unwrap()
        );
        assert_eq!(
            batch.schema().field(1).data_type(),
            arrow_dtypes[1].as_ref().unwrap()
        );
        assert!(batch.column(0).is_null(0));
        assert!(batch.column(1).is_null(0));
    }

    #[test]
    fn binary_dtype_hint_builds_binary_array_for_all_null_sink_column() {
        let columns = vec!["payload".to_string()];
        let dtypes = vec![Some(LogicalDType::Binary)];
        let rows = vec![vec![("payload".to_string(), ScalarValue::Null)]];

        let batch = flat_rows_to_record_batch_with_dtypes(
            &columns,
            &dtypes,
            &[None],
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

        let error =
            flat_rows_to_record_batch_with_dtypes(&columns, &dtypes, &[None], &rows, "test output")
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

    #[test]
    fn orc_nested_output_blocks_before_writer_conversion() {
        let columns = vec!["tags".to_string()];
        let dtypes = vec![Some(LogicalDType::List)];
        let rows = vec![vec![(
            "tags".to_string(),
            ScalarValue::List(vec![ScalarValue::Utf8("alpha".to_string())]),
        )]];

        let error = encode_flat_orc_rows_with_dtypes(&columns, &dtypes, &rows)
            .expect_err("ORC nested output remains blocked");

        assert!(
            error.to_string().contains(
                "local ORC output does not yet admit typed nested preservation for column 'tags'"
            ),
            "{error}"
        );
        assert!(
            error.to_string().contains(
                "orc-rust 0.8.0 panics on nested Arrow writer conversion with unsupported datatype"
            ),
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
