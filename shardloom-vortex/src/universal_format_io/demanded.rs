//! Local columnar adapters consume the common bound source-demand contract.
//! Parquet supplies metadata and decoding only; native operators own evaluation.

use crate::{
    resident_memory_source::ResidentMemorySource,
    resident_session::{
        LocalReadMetrics, NativeExecutionContext, OwnedLocalRead, ResidentVortexSession,
    },
    source_identity::SourceIdentity,
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
    path::Path,
    sync::{Arc, atomic::Ordering},
};

mod statistics;

/// A held source generation and its metadata, with no data reader or prepared file.
/// The first physical adapter is Parquet. Other formats must explicitly admit
/// their own capabilities before using this shared native batch boundary.
pub struct LocalColumnarSource {
    identity: Arc<SourceIdentity>,
    metadata: ArrowReaderMetadata,
    metadata_read_bytes: u64,
    metadata_credit: MemoryLease,
}

impl LocalColumnarSource {
    pub(crate) fn validate_generation(&self) -> Result<()> {
        self.identity.validate()
    }

    pub(crate) fn validate_destination(&self, path: &Path) -> Result<()> {
        use std::os::unix::fs::MetadataExt as _;
        self.identity.validate()?;
        match std::fs::metadata(path) {
            Ok(target)
                if target.dev() == self.identity.generation.device
                    && target.ino() == self.identity.generation.inode =>
            {
                Err(failed("source and output must be different files"))
            }
            Ok(_) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(failed(error)),
        }
    }

    /// Bind metadata without decoding row pages or persisting a native copy.
    /// # Errors
    /// Rejects unsupported formats, malformed/oversized metadata, source changes,
    /// cancellation and insufficient metadata capacity.
    pub fn admit(
        path: &Path,
        format: &str,
        memory: &shardloom_exec::live_memory::LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Arc<Self>> {
        cancellation.check()?;
        if format != "parquet" {
            return Err(failed("source has no admitted demand reader"));
        }
        let identity_credit = memory.reserve(
            (path.as_os_str().len() as u64)
                .checked_mul(4)
                .and_then(|n| n.checked_add(8192))
                .ok_or_else(|| failed("source metadata capacity overflow"))?,
        )?;
        let identity = Arc::new(SourceIdentity::capture(path)?);
        let mut file = identity.open_checked()?;
        let length = super::metadata::footer_length(&mut file, b"PAR1", 4)?;
        let mut credit = memory.reserve(
            (length as u64)
                .checked_mul(32)
                .and_then(|n| n.checked_add(8192))
                .ok_or_else(|| failed("source metadata capacity overflow"))?,
        )?;
        let reader = OwnedLocalRead::metadata(Arc::clone(&identity), memory, cancellation)?;
        let metadata =
            ArrowReaderMetadata::load(&reader, super::parquet_metadata_first_reader_options())
                .map_err(failed)?;
        let metadata_read_bytes = reader.metrics().bytes.load(Ordering::Relaxed) + 12;
        cancellation.check()?;
        identity.validate()?;
        let mut identity_credit = identity_credit;
        credit.absorb(&mut identity_credit)?;
        Ok(Arc::new(Self {
            identity,
            metadata,
            metadata_read_bytes,
            metadata_credit: credit,
        }))
    }

    pub(crate) fn prepare(
        self: &Arc<Self>,
        demand: &SourceDemand,
        session: &ResidentVortexSession,
        cancellation: &CancellationToken,
    ) -> Result<Arc<PreparedColumnarSource>> {
        cancellation.check()?;
        if !session.shared_memory_pool().owns(&self.metadata_credit) {
            return Err(failed(
                "local source metadata belongs to a different memory owner",
            ));
        }
        self.identity.validate()?;
        let fields = self.metadata.schema().fields();
        let groups = self.metadata.metadata().num_row_groups();
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
        let row_groups = (0..groups)
            .filter(|&index| {
                demand.predicates.is_empty()
                    || demand.predicates.iter().any(|predicate| {
                        predicate.as_ref().is_none_or(|predicate| {
                            !statistics::excluded(&self.metadata, index, predicate)
                        })
                    })
            })
            .collect();
        cancellation.check()?;
        Ok(Arc::new(PreparedColumnarSource {
            source: Arc::clone(self),
            projection,
            row_groups,
            _metadata: credit,
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
    row_groups: Vec<usize>,
    _metadata: MemoryLease,
}

impl PreparedColumnarSource {
    pub(crate) fn empty_schema(
        &self,
        session: &ResidentVortexSession,
        cancellation: &CancellationToken,
    ) -> Result<ResidentMemorySource> {
        let schema = Arc::new(
            self.source
                .metadata
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
        let input =
            context.local_source_reader(Arc::clone(&self.source.identity), Arc::clone(&metrics))?;
        Ok(ColumnarReader {
            prepared: Arc::clone(self),
            input,
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
    /// Initial source-admission reads, before execution. Repeated executions of
    /// the same descriptor repeat this fact, not a claim of new metadata I/O.
    pub metadata_read_bytes: u64,
    /// Positional execution reads, excluding separately reported binding reads.
    pub data_read_bytes: u64,
    pub data_read_requests: u64,
}

pub(crate) struct ColumnarReader {
    prepared: Arc<PreparedColumnarSource>,
    input: OwnedLocalRead,
    metrics: Arc<LocalReadMetrics>,
    next_group: usize,
    active: Option<(ParquetRecordBatchReader, MemoryLease)>,
    rows: u64,
    ended: bool,
}

impl ColumnarReader {
    pub(crate) fn next(
        &mut self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<RecordBatch>> {
        loop {
            context.check_cancelled()?;
            self.prepared.source.identity.validate()?;
            if let Some((reader, _)) = &mut self.active {
                if let Some(batch) = reader.next() {
                    let batch = batch.map_err(failed)?;
                    self.rows = self
                        .rows
                        .checked_add(batch.num_rows() as u64)
                        .ok_or_else(|| failed("source row count overflow"))?;
                    context.check_cancelled()?;
                    return Ok(Some(batch));
                }
                self.active.take();
            }
            let Some(&group_index) = self.prepared.row_groups.get(self.next_group) else {
                self.ended = true;
                return Ok(None);
            };
            let metadata = &self.prepared.source.metadata;
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
            let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
                self.input.clone(),
                metadata.clone(),
            )
            .with_projection(mask)
            .with_row_groups(vec![group_index])
            .with_batch_size(8192)
            .build()
            .map_err(failed)?;
            self.active = Some((reader, credit));
            self.next_group += 1;
        }
    }

    pub(crate) fn report(&self) -> Result<ColumnarReadReport> {
        if !self.ended || self.active.is_some() {
            return Err(failed("source did not reach its declared end"));
        }
        let metadata = &self.prepared.source.metadata;
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
            row_groups_total: metadata.metadata().num_row_groups(),
            row_groups_read: self.next_group,
            rows_decoded: self.rows,
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
