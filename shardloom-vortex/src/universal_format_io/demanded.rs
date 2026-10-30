//! Local columnar adapters consume the common bound source-demand contract.
//! Parquet supplies metadata and decoding only; native operators own evaluation.

use crate::{
    resident_memory_source::ResidentMemorySource,
    resident_session::{
        LocalReadMetrics, NativeExecutionContext, OwnedLocalRead, ResidentVortexSession,
    },
};
use arrow_array::RecordBatch;
use arrow_schema::DataType;
use bytes::Bytes;
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{
            ArrowReaderMetadata, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
        },
    },
    errors::{ParquetError, Result as ParquetResult},
    file::reader::{ChunkReader, Length},
};
use shardloom_core::{PredicateExpr, Result, ShardLoomError};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::MemoryLease};
use shardloom_plan::{ProjectionRequest, SourceDemand};
use std::{
    collections::BTreeSet,
    io::Read,
    sync::{Arc, atomic::Ordering},
};

mod chunks;
mod ranked;
mod source;
mod statistics;
pub use source::LocalColumnarSource;

impl LocalColumnarSource {
    pub(crate) fn prepare(
        self: &Arc<Self>,
        demand: &SourceDemand,
        session: &ResidentVortexSession,
        cancellation: &CancellationToken,
    ) -> Result<Arc<PreparedColumnarSource>> {
        cancellation.check()?;
        if demand.max_rows.is_some() && demand.predicates.iter().any(Option::is_some) {
            return Err(failed("source prefix requires an unfiltered input demand"));
        }
        if !session.shared_memory_pool().owns(&self.metadata_credit) {
            return Err(failed(
                "local source metadata belongs to a different memory owner",
            ));
        }
        self.validate_generation_controlled(cancellation)?;
        let fields = self.metadata().schema().fields();
        let groups = self.row_groups_total;
        let count = fields
            .len()
            .checked_add(groups)
            .ok_or_else(|| failed("demand metadata overflow"))?;
        let credit = session.reserve_input_scratch(
            (count as u64)
                .checked_mul(4096)
                .ok_or_else(|| failed("demand metadata overflow"))?,
        )?;
        let mut wanted = BTreeSet::new();
        match &demand.projection {
            ProjectionRequest::All => {
                wanted.extend(fields.iter().map(|field| field.name().as_str()));
            }
            ProjectionRequest::Columns(columns) => {
                wanted.extend(columns.iter().map(shardloom_core::ColumnRef::as_str));
            }
        }
        for predicate in demand.predicates.iter().flatten() {
            predicate_columns(predicate, &mut wanted);
        }
        if wanted
            .iter()
            .any(|name| !fields.iter().any(|field| field.name() == name))
        {
            return Err(failed("demand references an unknown source column"));
        }
        let projection: Vec<_> = fields
            .iter()
            .enumerate()
            .filter_map(|(i, field)| wanted.contains(field.name().as_str()).then_some(i))
            .collect();
        if projection.is_empty() {
            return Err(failed("native input requires a row-bearing source column"));
        }
        let mut row_groups = Vec::new();
        let mut source_rows = 0_u64;
        'files: for (file_index, file) in self.files.iter().enumerate() {
            cancellation.check()?;
            for index in 0..file.metadata.metadata().num_row_groups() {
                if demand.max_rows.is_some_and(|limit| source_rows >= limit) {
                    break 'files;
                }
                let first_row = source_rows;
                let rows = u64::try_from(file.metadata.metadata().row_group(index).num_rows())
                    .map_err(failed)?;
                source_rows = source_rows
                    .checked_add(rows)
                    .ok_or_else(|| failed("source row count overflow"))?;
                if demand.predicates.is_empty()
                    || demand.predicates.iter().any(|predicate| {
                        predicate.as_ref().is_none_or(|predicate| {
                            !statistics::excluded(&file.metadata, index, predicate)
                        })
                    })
                {
                    row_groups.push(SourceGroup {
                        file: file_index,
                        index,
                        first_row,
                        rows,
                    });
                }
            }
        }
        cancellation.check()?;
        Ok(Arc::new(PreparedColumnarSource {
            source: Arc::clone(self),
            projection,
            row_groups,
            max_rows: demand.max_rows,
            metadata: credit,
        }))
    }
}

fn predicate_columns<'a>(predicate: &'a PredicateExpr, names: &mut BTreeSet<&'a str>) {
    if let Some(column) = predicate.column() {
        names.insert(column.as_str());
    }
    if let PredicateExpr::And(children) = predicate {
        for child in children {
            predicate_columns(child, names);
        }
    }
}

pub(crate) struct PreparedColumnarSource {
    pub(crate) source: Arc<LocalColumnarSource>,
    projection: Vec<usize>,
    row_groups: Vec<SourceGroup>,
    max_rows: Option<u64>,
    metadata: MemoryLease,
}

/// Positions include excluded groups and preceding files in the held inventory.
/// They identify rows only within this source generation, not durable row IDs.
#[derive(Clone, Copy)]
struct SourceGroup {
    file: usize,
    index: usize,
    first_row: u64,
    rows: u64,
}

impl PreparedColumnarSource {
    pub(crate) fn empty_schema(
        &self,
        session: &ResidentVortexSession,
        cancellation: &CancellationToken,
    ) -> Result<ResidentMemorySource> {
        let schema = Arc::new(
            self.source
                .metadata()
                .schema()
                .project(&self.projection)
                .map_err(failed)?,
        );
        session.with_native_execution_context(cancellation, |context| {
            ResidentMemorySource::from_arrow_in_context(
                session,
                RecordBatch::new_empty(schema),
                context,
                true,
            )
        })
    }

    pub(crate) fn open(
        self: &Arc<Self>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ColumnarReader> {
        context.check_cancelled()?;
        let metrics = Arc::new(LocalReadMetrics::default());
        Ok(ColumnarReader {
            prepared: Arc::clone(self),
            input: None,
            metrics,
            next_group: 0,
            active: None,
            rows: 0,
            ended: false,
        })
    }
}

/// Execution reads plus the retained source's initial metadata-admission work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnarReadReport {
    pub format: &'static str,
    pub projected_columns: usize,
    pub available_columns: usize,
    pub physical_leaf_columns: usize,
    pub row_groups_total: usize,
    pub row_groups_read: usize,
    pub rows_decoded: u64,
    /// Requested source-order prefix, independently of physical chunk bytes.
    pub requested_prefix_rows: Option<u64>,
    pub files: usize,
    /// Full-byte identity work during admission, separate from demanded reads.
    pub fingerprint_read_bytes: u64,
    pub content_fingerprints: Vec<String>,
    /// Initial source-admission reads, before execution. Repeated executions of
    /// the same descriptor repeat this fact, not a claim of new metadata I/O.
    pub metadata_read_bytes: u64,
    /// Positional execution reads, excluding separately reported binding reads.
    pub data_read_bytes: u64,
    pub data_read_requests: u64,
}

pub(crate) struct ColumnarReader {
    prepared: Arc<PreparedColumnarSource>,
    input: Option<(usize, OwnedLocalRead)>,
    metrics: Arc<LocalReadMetrics>,
    next_group: usize,
    active: Option<Decoder>,
    rows: u64,
    ended: bool,
}

struct Decoder {
    reader: ParquetRecordBatchReader,
    _credit: MemoryLease,
    first_row: u64,
    expected_rows: u64,
    emitted_rows: u64,
}

impl ColumnarReader {
    /// Consume private reader output before it can escape or be replaced by a
    /// caller. The shared intake still proves each buffer's allocation and may
    /// copy unsuitable regions; decoder provenance is not blanket adoption.
    pub(crate) fn next_native(
        &mut self,
        session: &ResidentVortexSession,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<ResidentMemorySource>> {
        self.next_native_located(session, context)
            .map(|next| next.map(|(source, _)| source))
    }

    pub(crate) fn next_native_located(
        &mut self,
        session: &ResidentVortexSession,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<(ResidentMemorySource, u64)>> {
        self.next(context)?
            .map(|(batch, first_row)| {
                ResidentMemorySource::from_arrow_with_ownership(
                    session,
                    batch,
                    context,
                    true,
                    crate::vortex_ingest::arrow_ownership::ArrowOwnershipPolicy::ControlledParquet,
                )
                .map(|source| (source, first_row))
            })
            .transpose()
    }

    fn next(&mut self, context: &NativeExecutionContext<'_>) -> Result<Option<(RecordBatch, u64)>> {
        loop {
            context.check_cancelled()?;
            if let Some((index, _)) = &self.input {
                self.prepared.source.files[*index].identity.validate()?;
            }
            if let Some(active) = &mut self.active {
                if let Some(batch) = active.reader.next() {
                    let batch = batch.map_err(failed)?;
                    let first_row = active
                        .first_row
                        .checked_add(active.emitted_rows)
                        .ok_or_else(|| failed("source position overflow"))?;
                    active.emitted_rows = active
                        .emitted_rows
                        .checked_add(batch.num_rows() as u64)
                        .filter(|rows| *rows <= active.expected_rows)
                        .ok_or_else(|| failed("decoder exceeded its admitted source extent"))?;
                    self.rows = self
                        .rows
                        .checked_add(batch.num_rows() as u64)
                        .ok_or_else(|| failed("source row count overflow"))?;
                    context.check_cancelled()?;
                    return Ok(Some((batch, first_row)));
                }
                if active.emitted_rows != active.expected_rows {
                    return Err(failed(
                        "decoder did not complete its admitted source extent",
                    ));
                }
                self.active.take();
            }
            let Some(&source_group) = self.prepared.row_groups.get(self.next_group) else {
                self.input.take();
                self.ended = true;
                return Ok(None);
            };
            let file_index = source_group.file;
            let group_index = source_group.index;
            let file = &self.prepared.source.files[file_index];
            if self
                .input
                .as_ref()
                .is_none_or(|(index, _)| *index != file_index)
            {
                self.input.take();
                self.input = Some((
                    file_index,
                    context.local_source_reader(
                        Arc::clone(&file.identity),
                        Arc::clone(&self.metrics),
                    )?,
                ));
            }
            let input = &self.input.as_ref().expect("selected file reader").1;
            let metadata = &file.metadata;
            let mask = ProjectionMask::roots(
                metadata.parquet_schema(),
                self.prepared.projection.iter().copied(),
            );
            let group = metadata.metadata().row_group(group_index);
            // Admission is based on declared uncompressed selected chunks. It
            // precedes decoder construction but does not intercept allocations
            // inside the upstream decoder and is not an RSS bound.
            let workspace = group
                .columns()
                .iter()
                .enumerate()
                .filter(|(i, _)| mask.leaf_included(*i))
                .try_fold(8192_u64, |sum, (_, column)| {
                    let size = u64::try_from(column.uncompressed_size()).map_err(failed)?;
                    sum.checked_add(size)
                        .ok_or_else(|| failed("decoder workspace overflow"))
                })?;
            let credit = context.memory().reserve(workspace)?;
            let chunks = chunks::SelectedChunks::read(input, group, &mask, context.memory())?;
            let mut builder =
                ParquetRecordBatchReaderBuilder::new_with_metadata(chunks, metadata.clone())
                    .with_projection(mask)
                    .with_row_groups(vec![group_index])
                    .with_batch_size(8192);
            if let Some(limit) = self.prepared.max_rows {
                builder = builder
                    .with_limit(usize::try_from(limit.saturating_sub(self.rows)).map_err(failed)?);
            }
            let reader = builder.build().map_err(failed)?;
            self.active = Some(Decoder {
                reader,
                _credit: credit,
                first_row: source_group.first_row,
                expected_rows: self.prepared.max_rows.map_or(source_group.rows, |limit| {
                    source_group.rows.min(limit.saturating_sub(self.rows))
                }),
                emitted_rows: 0,
            });
            self.next_group += 1;
        }
    }

    pub(crate) fn report(&self) -> Result<ColumnarReadReport> {
        if !self.ended || self.active.is_some() || self.input.is_some() {
            return Err(failed("source did not reach its declared end"));
        }
        let metadata = self.prepared.source.metadata();
        let mask = ProjectionMask::roots(
            metadata.parquet_schema(),
            self.prepared.projection.iter().copied(),
        );
        Ok(ColumnarReadReport {
            format: "parquet",
            projected_columns: self.prepared.projection.len(),
            available_columns: metadata.schema().fields().len(),
            physical_leaf_columns: (0..metadata.parquet_schema().num_columns())
                .filter(|&i| mask.leaf_included(i))
                .count(),
            row_groups_total: self.prepared.source.row_groups_total,
            row_groups_read: self.next_group,
            rows_decoded: self.rows,
            requested_prefix_rows: self.prepared.max_rows,
            files: self.prepared.source.files.len(),
            fingerprint_read_bytes: self.prepared.source.fingerprint_read_bytes,
            content_fingerprints: self.prepared.source.content_fingerprints.clone(),
            metadata_read_bytes: self.prepared.source.metadata_read_bytes,
            data_read_bytes: self.metrics.bytes.load(Ordering::Relaxed),
            data_read_requests: self.metrics.requests.load(Ordering::Relaxed),
        })
    }
}

impl Length for OwnedLocalRead {
    fn len(&self) -> u64 {
        self.len()
    }
}
impl ChunkReader for OwnedLocalRead {
    type T = ColumnarCursor;
    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        if start > self.len() {
            return Err(ParquetError::General(
                "source cursor exceeds its held length".into(),
            ));
        }
        Ok(ColumnarCursor {
            input: self.clone(),
            position: start,
        })
    }
    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        self.bytes(start, length)
            .map_err(|error| ParquetError::External(Box::new(error)))
    }
}

pub(crate) struct ColumnarCursor {
    input: OwnedLocalRead,
    position: u64,
}
impl Read for ColumnarCursor {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let count = self
            .input
            .read_at(buffer, self.position)
            .map_err(std::io::Error::other)?;
        self.position = self
            .position
            .checked_add(count as u64)
            .ok_or_else(|| std::io::Error::other("source offset overflow"))?;
        Ok(count)
    }
}

fn failed(error: impl std::fmt::Display) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native columnar source demand: {error}; no fallback execution was attempted"
    ))
}
