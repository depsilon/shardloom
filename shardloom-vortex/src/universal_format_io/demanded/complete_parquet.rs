//! Ordered complete-source Parquet work on the existing native ingest executor.

use super::{CompleteReader, Result, failed, generic};
use crate::ingest_source::{SourceFactory, SourceReader};
use crate::universal_format_io::{
    self as io, FlatLocalColumnarStreamSource, IngestColumnarStreamSource,
    owned_batch::{BatchResult, OwnedBatch, OwnedBatchReader},
};
use arrow_schema::SchemaRef;
use shardloom_exec::{
    adaptive_work::{AdaptiveWorkSizer, WorkCost, WorkLimits},
    compute_pool::CancellationToken,
    live_memory::MemoryLease,
};
use std::{fmt::Write as _, mem::size_of, sync::Arc};

pub(super) fn ingest(
    mut source: FlatLocalColumnarStreamSource,
    reader: CompleteReader,
    requested: usize,
) -> Result<IngestColumnarStreamSource> {
    reader.cancellation.check()?;
    let groups = &reader.prepared.row_groups;
    // Admit the group/task tables, extent annotations and their construction
    // overlap before allocating them. The full source metadata remains shared.
    let per_group = 2 * size_of::<io::ParquetRowGroupReadTask>()
        + size_of::<io::ParquetRowGroupExtent>()
        + 6 * size_of::<usize>()
        + 4 * size_of::<(usize, usize)>();
    let mut credit = reader.memory.reserve(
        (groups.len() as u64)
            .checked_mul(per_group as u64)
            .and_then(|bytes| bytes.checked_add(16_384))
            .and_then(|bytes| bytes.checked_add(annotation_name_bytes(&reader).ok()?))
            .ok_or_else(|| failed("complete Parquet task metadata capacity overflow"))?,
    )?;
    let rows = groups
        .iter()
        .map(|group| usize::try_from(group.rows).map_err(failed))
        .collect::<Result<Vec<_>>>()?;
    let ranges = groups
        .iter()
        .map(|group| {
            let start = usize::try_from(group.first_row).map_err(failed)?;
            let end = start
                .checked_add(usize::try_from(group.rows).map_err(failed)?)
                .ok_or_else(|| failed("complete Parquet row range overflow"))?;
            Ok((start, end))
        })
        .collect::<Result<Vec<_>>>()?;
    let tasks = (requested > 1 && groups.len() > 1)
        .then(|| io::parquet_row_group_read_tasks(groups.len(), Some(&rows), 8192));
    source.source_stream_unit_count_hint = Some(tasks.as_ref().map_or(groups.len(), Vec::len));
    source.source_stream_unit_row_ranges = tasks.as_ref().map_or_else(
        || Some(ranges.clone()),
        |tasks| io::parquet_row_group_task_row_ranges(tasks, Some(&ranges)),
    );
    source.source_stream_unit_hint_kind = if tasks.is_some() {
        "parquet_adaptive_row_group_task_count"
    } else {
        "parquet_row_group_count"
    }
    .into();
    if reader
        .prepared
        .source
        .other
        .as_ref()
        .is_some_and(|source| source.files.len() == 1)
    {
        let metadata = reader
            .prepared
            .source
            .parquet_metadata()
            .next()
            .ok_or_else(|| failed("complete Parquet metadata missing"))?;
        let extents = io::ParquetExtentPlan::from_metadata(metadata.metadata());
        let byte_ranges = tasks.as_ref().map_or_else(
            || extents.row_group_byte_ranges(),
            |tasks| extents.task_byte_ranges(tasks),
        );
        source = io::annotate_parquet_extent_plan_source(source, &extents, byte_ranges.as_deref());
    }
    let owner = Arc::clone(&reader.prepared.source);
    let cancellation = reader.cancellation.clone();
    let schema = Arc::clone(&reader.schema);
    let memory = reader.memory.clone();
    let output: Box<dyn OwnedBatchReader> = if let Some(tasks) = tasks {
        ordered_tasks(&mut source, &reader, tasks, requested, &mut credit)?
    } else {
        Box::new(reader)
    };
    source.source_stream_policy.push_str(
        ";source_generation_guard=held_descriptor_and_path_unix_identity_before_after_pull_and_before_publication",
    );
    Ok(IngestColumnarStreamSource::from_owned_parquet(
        source,
        Box::new(PlanReader {
            reader: Some(output),
            source: owner,
            schema,
            cancellation,
            _credit: credit,
        }),
        &memory,
    ))
}

fn ordered_tasks(
    source: &mut FlatLocalColumnarStreamSource,
    reader: &CompleteReader,
    tasks: Vec<io::ParquetRowGroupReadTask>,
    requested: usize,
    credit: &mut MemoryLease,
) -> Result<Box<dyn OwnedBatchReader>> {
    let task_count = tasks.len();
    // Factory captures, task/control records and bounded queue bookkeeping;
    // every queued payload independently retains its actual batch credits.
    let task_bytes = task_count
        .checked_mul(
            size_of::<CompleteReader>() + 2 * size_of::<SourceFactory<OwnedBatch>>() + 1024,
        )
        .and_then(|bytes| u64::try_from(bytes).ok())
        .and_then(|bytes| bytes.checked_add(credit.bytes()))
        .ok_or_else(|| failed("complete Parquet task capacity overflow"))?;
    credit.resize(task_bytes)?;
    let runtime = io::IngestRuntime::new(requested)?;
    let (window, batch_rows) = task_shape(reader, runtime.parallelism().min(task_count))?;
    let mut factories = Vec::with_capacity(task_count);
    for task in tasks {
        reader.cancellation.check()?;
        let first = *task
            .row_groups
            .first()
            .ok_or_else(|| failed("empty Parquet task"))?;
        let end = task
            .row_groups
            .last()
            .copied()
            .and_then(|last| last.checked_add(1))
            .ok_or_else(|| failed("Parquet task range overflow"))?;
        let task_reader = CompleteReader {
            prepared: Arc::clone(&reader.prepared),
            reader: Some(generic::Reader::for_owned_parquet_groups(
                first..end,
                batch_rows,
            )),
            schema: Arc::clone(&reader.schema),
            metrics: Arc::clone(&reader.metrics),
            memory: reader.memory.clone(),
            cancellation: reader.cancellation.clone(),
            rows: 0,
            max_rows: reader.max_rows,
        };
        factories.push(Box::new(move || {
            Ok(Box::new(task_reader) as Box<dyn Iterator<Item = BatchResult> + Send>)
        }) as SourceFactory<OwnedBatch>);
    }
    write!(source.source_stream_policy,
        ";parquet_ordered_task_window={window};parquet_result_queue_batches_per_task={};parquet_max_retained_decoded_batches={};parquet_source_batch_row_ceiling={batch_rows};parquet_source_byte_bound=shared_pool_reservations_not_provider_allocator_or_rss_bound",
        SourceReader::<OwnedBatch>::QUEUED_BATCHES,
        window * (SourceReader::<OwnedBatch>::QUEUED_BATCHES + 1),
    ).expect("writing to a String is infallible");
    source.ingest_executor_status = "bounded_shared_runtime_source".into();
    source.ingest_executor_kind =
        "parquet_ordered_source_tasks_on_shared_native_ingest_runtime".into();
    source.ingest_executor_requested_parallelism = requested;
    source.ingest_executor_applied_parallelism = window;
    source.ingest_executor_unit_count_hint = Some(task_count);
    source.ingest_runtime = Some(runtime.clone());
    Ok(Box::new(SourceReader::from_factories(
        Arc::clone(&reader.schema),
        runtime,
        factories,
        window,
        &reader.cancellation,
    )))
}

fn annotation_name_bytes(reader: &CompleteReader) -> Result<u64> {
    reader
        .prepared
        .source
        .parquet_metadata()
        .take(1)
        .flat_map(|metadata| metadata.metadata().row_groups().iter())
        .flat_map(parquet::file::metadata::RowGroupMetaData::columns)
        .take(io::PARQUET_EXTENT_PLAN_SAMPLE_LIMIT)
        .flat_map(|column| column.column_path().parts())
        .try_fold(0_u64, |bytes, part| {
            bytes
                .checked_add((part.len() as u64).saturating_mul(4))
                .ok_or_else(|| failed("Parquet annotation size overflow"))
        })
}

fn task_shape(reader: &CompleteReader, parallelism: usize) -> Result<(usize, usize)> {
    let source = reader
        .prepared
        .source
        .other
        .as_ref()
        .ok_or_else(|| failed("complete Parquet source owner missing"))?;
    let mut workspace = 0_u64;
    let mut batch_cost = WorkCost {
        fixed_bytes: 0,
        bytes_per_unit: 1,
    };
    let mut max_rows = 0;
    for group in &reader.prepared.row_groups {
        reader.cancellation.check()?;
        let file = &source.files[group.file];
        let metadata = file
            .parquet
            .as_ref()
            .ok_or_else(|| failed("Parquet metadata missing"))?;
        let physical = metadata.metadata().row_group(group.index);
        let mask = parquet::arrow::ProjectionMask::all();
        let bytes = physical.columns().iter().try_fold(
            super::super::batch_work::parquet_workspace(physical, &mask)?,
            |bytes, column| {
                bytes
                    .checked_add(u64::try_from(column.compressed_size()).map_err(failed)?)
                    .ok_or_else(|| failed("Parquet input workspace overflow"))
            },
        )?;
        workspace = workspace.max(bytes);
        let cost = super::super::batch_work::cost(
            &file.schema,
            &source.schema,
            &reader.prepared.source.collection.files[group.file].partitions,
        )?;
        batch_cost.fixed_bytes = batch_cost.fixed_bytes.max(cost.fixed_bytes);
        batch_cost.bytes_per_unit = batch_cost.bytes_per_unit.max(cost.bytes_per_unit);
        max_rows = max_rows.max(group.rows);
    }
    if max_rows == 0 {
        return Ok((1, 8192));
    }
    let retained = (SourceReader::<OwnedBatch>::QUEUED_BATCHES + 1) as u64;
    let cost = WorkCost {
        fixed_bytes: batch_cost
            .fixed_bytes
            .checked_mul(retained)
            .and_then(|bytes| bytes.checked_add(workspace))
            .ok_or_else(|| failed("Parquet task fixed workspace overflow"))?,
        bytes_per_unit: batch_cost
            .bytes_per_unit
            .checked_mul(retained)
            .ok_or_else(|| failed("Parquet task row workspace overflow"))?,
    };
    let shape = AdaptiveWorkSizer::default().select(
        "complete_parquet_source_tasks",
        reader.memory.snapshot(),
        cost,
        WorkLimits {
            min_units: 1,
            max_units: max_rows.min(8192),
            max_in_flight: parallelism,
        },
    )?;
    // This bounds real task admission and each decoder's maximum batch. Actual
    // reservations remain authoritative; selecting a plan is not completion.
    Ok((
        shape.in_flight(),
        usize::try_from(shape.units()).map_err(failed)?,
    ))
}

struct PlanReader {
    reader: Option<Box<dyn OwnedBatchReader>>,
    source: Arc<super::LocalColumnarSource>,
    schema: SchemaRef,
    cancellation: CancellationToken,
    // Reader drop drains tasks before releasing their shared planning capacity.
    _credit: MemoryLease,
}

impl Iterator for PlanReader {
    type Item = BatchResult;
    fn next(&mut self) -> Option<Self::Item> {
        self.reader.as_ref()?;
        let check = || {
            self.cancellation.check()?;
            self.source
                .validate_generation_controlled(&self.cancellation)
        };
        let result = check().map_or_else(
            |error| {
                Some(Err(arrow_schema::ArrowError::ExternalError(Box::new(
                    error,
                ))))
            },
            |()| {
                let result = self.reader.as_mut().expect("reader checked").next();
                if !matches!(result, Some(Err(_)))
                    && let Err(error) = check()
                {
                    return Some(Err(arrow_schema::ArrowError::ExternalError(Box::new(
                        error,
                    ))));
                }
                result
            },
        );
        if result.as_ref().is_none_or(std::result::Result::is_err) {
            self.reader.take();
        }
        result
    }
}

impl OwnedBatchReader for PlanReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
#[path = "complete_parquet_tests.rs"]
mod tests;
