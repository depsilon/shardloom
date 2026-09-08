//! Read-only, complete-value artifact validation. This is a testing boundary,
//! never a query execution path or an external-engine fallback.
//!
//! Equal native schemas are projected one field at a time. Bounded row windows
//! prevent provider split vectors from scaling with the complete source, while
//! independent Arrow cursors tolerate different encodings and batch boundaries.

use crate::resident_session::{PreparedVortexSource, ResidentVortexSession};
use arrow_array::ArrayRef as ArrowArrayRef;
use arrow_schema::{DataType, Field};
use futures::StreamExt as _;
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::{LiveMemoryPool, MemoryLease};
use std::{
    fs::Metadata,
    ops::Range,
    os::unix::fs::MetadataExt as _,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use vortex::{
    array::{
        ArrayRef, Columnar, ExecutionCtx, IntoArray as _, VortexSessionExecute as _,
        arrays::VarBinViewArray,
        dtype::DType,
        expr::{BoundExpression, get_item, root},
    },
    arrow::ArrowSessionExt as _,
    file::VortexFile,
    io::runtime::{BlockingRuntime as _, current::CurrentThreadRuntime},
    layout::scan::split_by::SplitBy,
    session::VortexSession,
};

/// Explicit allocation and iteration limits; they never limit compared rows.
#[derive(Clone, Copy, Debug)]
pub struct NativeArtifactComparisonLimits {
    pub memory_bytes_per_source: u64,
    pub parallelism_per_source: usize,
    pub left_batch_rows: usize,
    pub right_batch_rows: usize,
    pub window_rows: u64,
    pub arrow_batch_bytes: u64,
    pub max_value_bytes: u64,
    pub max_columns: usize,
}

impl Default for NativeArtifactComparisonLimits {
    fn default() -> Self {
        Self {
            memory_bytes_per_source: 512 << 20,
            parallelism_per_source: 2,
            left_batch_rows: 65_536,
            right_batch_rows: 65_536,
            window_rows: 8_388_608,
            arrow_batch_bytes: 16 << 20,
            max_value_bytes: 8 << 20,
            max_columns: 1024,
        }
    }
}

impl NativeArtifactComparisonLimits {
    fn validate(self) -> Result<()> {
        if self.memory_bytes_per_source == 0
            || self.parallelism_per_source == 0
            || self.parallelism_per_source > 64
            || self.left_batch_rows == 0
            || self.right_batch_rows == 0
            || self.window_rows == 0
            || self.arrow_batch_bytes == 0
            || self.max_value_bytes == 0
            || self.max_columns == 0
            || self.max_columns > 4096
            || self.window_rows > 16_777_216
            || self.left_batch_rows > 1_048_576
            || self.right_batch_rows > 1_048_576
            || self.window_rows.div_ceil(self.left_batch_rows as u64) > 65_536
            || self.window_rows.div_ceil(self.right_batch_rows as u64) > 65_536
            || self.arrow_batch_bytes.checked_mul(16).is_none()
        {
            return Err(error(
                "invalid positive bounds or bounded scan-window limits exceeded",
            ));
        }
        Ok(())
    }
}

#[derive(Default)]
struct Work {
    rows: u64,
    columns: usize,
    compared_values: u64,
    equal_spans: u64,
    left_batches: u64,
    right_batches: u64,
    peak_arrow_batch_bytes: u64,
    left_peak_native_tasks: usize,
    right_peak_native_tasks: usize,
}

struct Source<'a> {
    file: &'a VortexFile,
    session: &'a VortexSession,
    runtime: &'a CurrentThreadRuntime,
    tasks: ScanTasks,
}

#[derive(Clone, Default)]
struct ScanTasks(Arc<TaskCounts>);

#[derive(Default)]
struct TaskCounts {
    active: AtomicUsize,
    peak: AtomicUsize,
}

impl ScanTasks {
    fn admit(&self) -> ScanTaskGuard {
        let active = self.0.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.0.peak.fetch_max(active, Ordering::AcqRel);
        ScanTaskGuard(Arc::clone(&self.0))
    }
}

struct ScanTaskGuard(Arc<TaskCounts>);
impl Drop for ScanTaskGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}

struct Column {
    name: String,
    target: Field,
    projection: BoundExpression,
}

/// Compare every logical value, in order, of two held local Vortex generations.
/// Only flat scalar fields with explicit lossless Arrow targets are admitted.
/// Native dtype/name/order/nullability must match exactly. Null payload bytes,
/// physical encodings, layout boundaries, footer statistics and metadata are
/// not compared. Valid floating-point values use bit equality, including NaNs
/// and signed zero; integer values never pass through floating point.
///
/// # Errors
/// Fails on any schema/value/row mismatch, source change, unsupported dtype,
/// allocation/batch bound, provider failure or invalid option. No partial
/// equality report is returned and no artifact is written.
pub fn compare_native_artifacts(
    left_path: &Path,
    right_path: &Path,
    limits: NativeArtifactComparisonLimits,
) -> Result<serde_json::Value> {
    limits.validate()?;
    let left_path = left_path.canonicalize().map_err(provider_error)?;
    let right_path = right_path.canonicalize().map_err(provider_error)?;
    let left_metadata = left_path.metadata().map_err(provider_error)?;
    let right_metadata = right_path.metadata().map_err(provider_error)?;
    let left_owner = ResidentVortexSession::new(
        limits.memory_bytes_per_source,
        limits.parallelism_per_source,
    )?;
    let right_owner = ResidentVortexSession::new(
        limits.memory_bytes_per_source,
        limits.parallelism_per_source,
    )?;
    let left = left_owner.prepare_file(&left_path)?;
    let right = right_owner.prepare_file(&right_path)?;
    left.validate_file_metadata(&left_metadata)?;
    right.validate_file_metadata(&right_metadata)?;
    let (_, left_cpu_grant) = left.resource_limits();
    let (_, right_cpu_grant) = right.resource_limits();
    let native_task_concurrency = left_cpu_grant.min(right_cpu_grant);
    let scan_limits = NativeArtifactComparisonLimits {
        parallelism_per_source: native_task_concurrency,
        ..limits
    };
    // Two batches each hold an eightfold work grant before Arrow allocation.
    // This is a conservative conversion envelope, not an Arrow allocator hook.
    let arrow_memory = LiveMemoryPool::new(limits.arrow_batch_bytes * 16)?;
    let work = left.with_native_execution(|file, session, runtime| {
        right.with_native_execution(|right_file, right_session, right_runtime| {
            compare_sources(
                &Source {
                    file,
                    session,
                    runtime,
                    tasks: ScanTasks::default(),
                },
                &Source {
                    file: right_file,
                    session: right_session,
                    runtime: right_runtime,
                    tasks: ScanTasks::default(),
                },
                scan_limits,
                &arrow_memory,
            )
        })
    })?;
    validate_both(&left, &right, &left_metadata, &right_metadata)?;
    drop(left);
    drop(right);
    let left_runtime = left_owner.snapshot();
    let right_runtime = right_owner.snapshot();
    let memory_report = serde_json::json!({
        "native_limit_bytes_per_source": limits.memory_bytes_per_source,
        "left_peak_reserved_bytes": left_runtime.memory.peak_reserved_bytes,
        "right_peak_reserved_bytes": right_runtime.memory.peak_reserved_bytes,
        "left_final_reserved_bytes": left_runtime.memory.reserved_bytes,
        "right_final_reserved_bytes": right_runtime.memory.reserved_bytes,
        "arrow_work_limit_bytes": arrow_memory.snapshot().limit_bytes,
        "arrow_peak_reserved_bytes": arrow_memory.snapshot().peak_reserved_bytes,
        "arrow_final_reserved_bytes": arrow_memory.snapshot().reserved_bytes,
        "peak_arrow_batch_bytes": work.peak_arrow_batch_bytes,
        "arrow_batch_bytes": limits.arrow_batch_bytes,
        "max_value_bytes": limits.max_value_bytes,
        "left_batch_rows": limits.left_batch_rows, "right_batch_rows": limits.right_batch_rows,
        "window_rows": limits.window_rows, "parallelism_per_source": limits.parallelism_per_source,
        "scope": "two_reserved_native_source_allocators;one_column_and_one_Arrow_batch_per_source;bounded_row_windows_and_split_metadata;Arrow_conversion_pre_admission_and_postcheck;excludes_upstream_allocations_bypassing_host_allocator_file_footer_layout_caches_and_process_RSS"
    });
    Ok(serde_json::json!({
        "complete_value_equality": true,
        "compared_rows": work.rows, "compared_columns": work.columns,
        "compared_values": work.compared_values, "equal_spans": work.equal_spans,
        "left_native_batches": work.left_batches, "right_native_batches": work.right_batches,
        "left": identity(&left_path, &left_metadata), "right": identity(&right_path, &right_metadata),
        "left_prepared_source_opens": left_runtime.prepared_source_opens,
        "right_prepared_source_opens": right_runtime.prepared_source_opens,
        "left_completed_executions": left_runtime.completed_executions,
        "right_completed_executions": right_runtime.completed_executions,
        "native_task_concurrency_per_source": native_task_concurrency,
        "left_peak_native_split_tasks": work.left_peak_native_tasks,
        "right_peak_native_split_tasks": work.right_peak_native_tasks,
        "native_split_tasks_after_drain": 0,
        "left_provider_background_workers": left_runtime.provider_background_workers,
        "right_provider_background_workers": right_runtime.provider_background_workers,
        "native_scan_scheduler": "owned_RepeatedScan_futures_on_existing_source_runtime;bounded_ordered_polling;no_host_core_multiplier_or_extra_CPU_pool;split_futures_drop_synchronously_on_error;peak_counts_admitted_splits_not_completed_buffered_results_or_blocking_IO;all_split_tasks_drained_before_equality_report",
        "native_schema_equal": true, "source_generations_validated": true,
        "comparison_scope": "all_rows_all_columns_in_order;exact_native_dtype_names_order_and_nullability;Arrow_ArrayData_logical_equality;valid_primitive_bits_exact;null_payload_padding_dictionary_codes_and_chunk_boundaries_ignored;no_f64_integer_conversion;no_row_dictionaries;no_sampling_or_aggregate_surrogate",
        "boundary": "explicit_native_canonicalization_and_Arrow_conversion_for_artifact_validation_only;not_query_execution_or_external_engine_fallback;native_layout_statistics_and_user_metadata_not_compared;no_output_artifact_write;no_content_hash",
        "generation_scope": "held_file_descriptors;pre_post_descriptor_and_path_generation_validation_around_complete_comparison;caller_frozen_artifacts_required;not_an_atomic_filesystem_snapshot",
        "memory": memory_report
    }))
}

fn validate_both(
    left: &PreparedVortexSource,
    right: &PreparedVortexSource,
    left_metadata: &Metadata,
    right_metadata: &Metadata,
) -> Result<()> {
    left.validate_file_metadata(left_metadata)?;
    right.validate_file_metadata(right_metadata)
}

fn identity(path: &Path, metadata: &Metadata) -> serde_json::Value {
    serde_json::json!({"path": path, "bytes": metadata.len(), "device": metadata.dev(),
        "inode": metadata.ino(), "modified_seconds": metadata.mtime(),
        "modified_nanoseconds": metadata.mtime_nsec(), "changed_seconds": metadata.ctime(),
        "changed_nanoseconds": metadata.ctime_nsec()})
}

fn compare_sources(
    left: &Source<'_>,
    right: &Source<'_>,
    limits: NativeArtifactComparisonLimits,
    arrow_memory: &LiveMemoryPool,
) -> Result<Work> {
    if left.file.dtype() != right.file.dtype() {
        return Err(error(
            "native schemas differ (names/order/types/nullability)",
        ));
    }
    if left.file.row_count() != right.file.row_count() {
        return Err(error("native source row counts differ"));
    }
    let columns = columns(left, limits)?;
    let mut work = Work {
        rows: left.file.row_count(),
        columns: columns.len(),
        ..Work::default()
    };
    for column in columns {
        let mut start = 0;
        while start < work.rows {
            let end = start.saturating_add(limits.window_rows).min(work.rows);
            compare_window(
                left,
                right,
                &column,
                start..end,
                limits,
                arrow_memory,
                &mut work,
            )?;
            start = end;
        }
    }
    let expected = work
        .rows
        .checked_mul(work.columns as u64)
        .ok_or_else(|| error("value count overflow"))?;
    if work.compared_values != expected {
        return Err(error("complete source coverage failed"));
    }
    if left.tasks.0.active.load(Ordering::Acquire) != 0
        || right.tasks.0.active.load(Ordering::Acquire) != 0
    {
        return Err(error(
            "native split tasks did not drain before comparison completed",
        ));
    }
    work.left_peak_native_tasks = left.tasks.0.peak.load(Ordering::Acquire);
    work.right_peak_native_tasks = right.tasks.0.peak.load(Ordering::Acquire);
    if work.left_peak_native_tasks > limits.parallelism_per_source
        || work.right_peak_native_tasks > limits.parallelism_per_source
    {
        return Err(error(
            "native split tasks exceeded explicit concurrency admission",
        ));
    }
    Ok(work)
}

fn columns(source: &Source<'_>, limits: NativeArtifactComparisonLimits) -> Result<Vec<Column>> {
    let dtype = source.file.dtype();
    if dtype.is_nullable() {
        return Err(error("nullable struct roots are not admitted"));
    }
    let fields = dtype
        .as_struct_fields_opt()
        .ok_or_else(|| error("requires a flat struct source"))?;
    if fields.names().is_empty() || fields.names().len() > limits.max_columns {
        return Err(error("source column count exceeds admission"));
    }
    fields
        .names()
        .iter()
        .zip(fields.fields())
        .map(|(name, dtype)| {
            if name.as_ref().len() > 4096 {
                return Err(error("column name exceeds bounded report admission"));
            }
            let target = source
                .session
                .arrow()
                .to_arrow_field(name.as_ref(), &dtype)
                .map_err(provider_error)?;
            let target = match dtype {
                DType::Utf8(_) => target.with_data_type(DataType::Utf8),
                DType::Binary(_) => target.with_data_type(DataType::Binary),
                _ => target,
            };
            // Uniform lossless targets normalize physical dictionary/encoded forms.
            fixed_width(target.data_type())
                .or_else(|| match target.data_type() {
                    DataType::Utf8 | DataType::Binary => Some(0),
                    _ => None,
                })
                .ok_or_else(|| {
                    error(&format!(
                        "unsupported comparison field '{name}' of type {}",
                        target.data_type()
                    ))
                })?;
            let projection = get_item(name.clone(), root())
                .bind(source.file.dtype())
                .map_err(provider_error)?;
            Ok(Column {
                name: name.to_string(),
                target,
                projection,
            })
        })
        .collect()
}

fn fixed_width(dtype: &DataType) -> Option<u64> {
    Some(match dtype {
        DataType::Null => 0,
        DataType::Boolean | DataType::Int8 | DataType::UInt8 => 1,
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => 2,
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_) => 4,
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => 8,
        DataType::Decimal128(_, _) => 16,
        _ => return None,
    })
}

fn scan<'a>(
    source: &Source<'a>,
    column: &Column,
    range: Range<u64>,
    rows: usize,
    concurrency: usize,
) -> Result<impl Iterator<Item = vortex::error::VortexResult<ArrayRef>> + 'a> {
    // Public prepared split tasks avoid LazyScanStream's per-worker setting
    // multiplied by host available_parallelism. The window bounds the task
    // vector; buffered() admits at most this source's actual CPU grant.
    let tasks = source
        .file
        .scan()
        .map_err(provider_error)?
        .with_projection(column.projection.clone())
        .with_ordered(true)
        .with_row_range(range)
        .with_split_by(SplitBy::RowCount(rows))
        .prepare()
        .map_err(provider_error)?
        .execute(None)
        .map_err(provider_error)?;
    let counts = source.tasks.clone();
    let stream = futures::stream::iter(tasks)
        .map(move |task| {
            let guard = counts.admit();
            async move {
                let _guard = guard;
                task.await
            }
        })
        .buffered(concurrency)
        .filter_map(|chunk| futures::future::ready(chunk.transpose()));
    Ok(source.runtime.block_on_stream(stream))
}

#[allow(clippy::too_many_arguments)]
fn compare_window(
    left: &Source<'_>,
    right: &Source<'_>,
    column: &Column,
    range: Range<u64>,
    limits: NativeArtifactComparisonLimits,
    memory: &LiveMemoryPool,
    work: &mut Work,
) -> Result<()> {
    let mut left_cursor = Cursor::new(
        scan(
            left,
            column,
            range.clone(),
            limits.left_batch_rows,
            limits.parallelism_per_source,
        )?,
        left.session,
        column,
        limits.left_batch_rows,
        limits,
        memory,
    )?;
    let mut right_cursor = Cursor::new(
        scan(
            right,
            column,
            range.clone(),
            limits.right_batch_rows,
            limits.parallelism_per_source,
        )?,
        right.session,
        column,
        limits.right_batch_rows,
        limits,
        memory,
    )?;
    let mut offset = range.start;
    loop {
        let has_left = left_cursor.ensure()?;
        let has_right = right_cursor.ensure()?;
        match (has_left, has_right) {
            (false, false) => break,
            (true, true) => {}
            _ => {
                return Err(error(&format!(
                    "column '{}' stream length differs at row {offset}",
                    column.name
                )));
            }
        }
        let count = left_cursor.remaining().min(right_cursor.remaining());
        let left_slice = left_cursor.array().slice(left_cursor.offset, count);
        let right_slice = right_cursor.array().slice(right_cursor.offset, count);
        if left_slice.to_data() != right_slice.to_data() {
            let first = (0..count)
                .find(|index| {
                    left_slice.slice(*index, 1).to_data() != right_slice.slice(*index, 1).to_data()
                })
                .ok_or_else(|| error("mismatch localization failed"))?;
            return Err(error(&format!(
                "value mismatch at column '{}' row {} (zero-based)",
                column.name,
                offset + first as u64
            )));
        }
        left_cursor.offset += count;
        right_cursor.offset += count;
        offset = offset
            .checked_add(count as u64)
            .ok_or_else(|| error("row offset overflow"))?;
        work.compared_values = work
            .compared_values
            .checked_add(count as u64)
            .ok_or_else(|| error("value count overflow"))?;
        work.equal_spans += 1;
        #[cfg(test)]
        AFTER_EQUAL_SPAN.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
    }
    if offset != range.end {
        return Err(error(&format!(
            "column '{}' did not cover its full row window",
            column.name
        )));
    }
    work.left_batches += left_cursor.batches;
    work.right_batches += right_cursor.batches;
    work.peak_arrow_batch_bytes = work
        .peak_arrow_batch_bytes
        .max(left_cursor.peak_bytes)
        .max(right_cursor.peak_bytes);
    Ok(())
}

struct Cursor<'a, I> {
    iterator: I,
    session: &'a VortexSession,
    context: ExecutionCtx,
    column: &'a Column,
    max_rows: usize,
    limits: NativeArtifactComparisonLimits,
    current: Option<ArrowArrayRef>,
    offset: usize,
    batches: u64,
    peak_bytes: u64,
    _work_grant: MemoryLease,
}

impl<'a, I: Iterator<Item = vortex::error::VortexResult<ArrayRef>>> Cursor<'a, I> {
    fn new(
        iterator: I,
        session: &'a VortexSession,
        column: &'a Column,
        max_rows: usize,
        limits: NativeArtifactComparisonLimits,
        memory: &LiveMemoryPool,
    ) -> Result<Self> {
        Ok(Self {
            iterator,
            session,
            context: session.create_execution_ctx(),
            column,
            max_rows,
            limits,
            current: None,
            offset: 0,
            batches: 0,
            peak_bytes: 0,
            _work_grant: memory.reserve(limits.arrow_batch_bytes * 8)?,
        })
    }

    fn array(&self) -> &ArrowArrayRef {
        self.current.as_ref().expect("cursor was advanced")
    }
    fn remaining(&self) -> usize {
        self.array().len() - self.offset
    }

    fn ensure(&mut self) -> Result<bool> {
        if self
            .current
            .as_ref()
            .is_some_and(|array| self.offset < array.len())
        {
            return Ok(true);
        }
        self.current = None;
        self.offset = 0;
        for array in self.iterator.by_ref() {
            let array = array.map_err(provider_error)?;
            if array.is_empty() {
                continue;
            }
            if array.len() > self.max_rows {
                return Err(error("native scan exceeded admitted batch rows"));
            }
            let rows = array.len();
            let canonical =
                admitted_canonical(array, &self.column.target, &mut self.context, self.limits)?;
            let arrow = self
                .session
                .arrow()
                .execute_arrow(canonical, Some(&self.column.target), &mut self.context)
                .map_err(provider_error)?;
            if arrow.len() != rows || arrow.data_type() != self.column.target.data_type() {
                return Err(error("Arrow boundary changed admitted row count or dtype"));
            }
            let bytes = arrow.get_array_memory_size() as u64;
            if bytes > self.limits.arrow_batch_bytes {
                return Err(error(
                    "Arrow retained batch exceeds pre-admitted byte limit",
                ));
            }
            self.peak_bytes = self.peak_bytes.max(bytes);
            self.batches += 1;
            self.current = Some(arrow);
            return Ok(true);
        }
        Ok(false)
    }
}

fn admitted_canonical(
    array: ArrayRef,
    target: &Field,
    context: &mut ExecutionCtx,
    limits: NativeArtifactComparisonLimits,
) -> Result<ArrayRef> {
    let rows = array.len() as u64;
    let mut expanded = rows.div_ceil(8) + 1024;
    let canonical = if matches!(array.dtype(), DType::Utf8(_) | DType::Binary(_)) {
        let bytes = array
            .execute::<VarBinViewArray>(context)
            .map_err(provider_error)?;
        for view in bytes.views() {
            let length = u64::from(view.len());
            if length > limits.max_value_bytes {
                return Err(error("variable-width value exceeds comparison admission"));
            }
            expanded = expanded
                .checked_add(length)
                .ok_or_else(|| error("Arrow expansion overflow"))?;
        }
        expanded = expanded
            .checked_add((rows + 1) * 4)
            .ok_or_else(|| error("Arrow offsets overflow"))?;
        bytes.into_array()
    } else {
        let width = fixed_width(target.data_type())
            .ok_or_else(|| error("unsupported fixed-width Arrow target"))?;
        expanded = expanded
            .checked_add(
                rows.checked_mul(width)
                    .ok_or_else(|| error("Arrow width overflow"))?,
            )
            .ok_or_else(|| error("Arrow expansion overflow"))?;
        array
            .execute::<Columnar>(context)
            .map_err(provider_error)?
            .into_array()
    };
    if expanded > limits.arrow_batch_bytes {
        return Err(error("native batch exceeds Arrow expansion admission"));
    }
    Ok(canonical)
}

fn error(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native artifact comparison: {message}; no equality proof or fallback execution"
    ))
}
fn provider_error(error_value: impl std::fmt::Display) -> ShardLoomError {
    error(&error_value.to_string())
}

#[cfg(test)]
thread_local! {
    static AFTER_EQUAL_SPAN: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(all(test, feature = "vortex-write"))]
#[path = "native_artifact_comparison_tests.rs"]
mod tests;
