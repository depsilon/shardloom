//! Format adapters feed the same demand-bound native producer. Providers
//! decode formats only; predicates and every relational operator stay native.
//!
//! Projection is applied by IPC/ORC/Avro providers and by text construction.
//! This module makes no row-group pruning claim for those adapters. Provider
//! transient allocations remain outside the retained-batch accounting boundary.

use super::{
    ColumnarCursor, ColumnarReadReport, LocalColumnarSource, LocalReadMetrics,
    NativeExecutionContext, OwnedLocalRead, PreparedColumnarSource, Result, failed,
};
use crate::source_identity::SourceIdentity;
use crate::universal_format_io::{local_dataset::LocalDataset, text_stream::TextSource};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::SchemaRef;
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::{
    io::{BufReader, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[path = "generic_schema.rs"]
mod schema;

type TextFactory<'a> =
    dyn Fn(&Path, &LiveMemoryPool, &CancellationToken) -> Result<Arc<TextSource>> + 'a;

pub(super) struct Source {
    pub(super) format: &'static str,
    pub(super) schema: SchemaRef,
    pub(super) files: Vec<File>,
    pub(super) schema_discovery_read_bytes: u64,
}

pub(super) struct File {
    pub(super) path: PathBuf,
    pub(super) identity: Arc<SourceIdentity>,
    ipc_file_framing: bool,
    text: Option<Arc<TextSource>>,
    schema: SchemaRef,
    parquet: Option<super::ArrowReaderMetadata>,
}

impl LocalColumnarSource {
    /// Admit text through the existing adapter's schema/coercion provider.
    /// File generations and directory membership use the same source owner as
    /// binary formats. The factory must use the supplied pool and cancellation.
    /// # Errors
    /// Rejects unsupported formats, incompatible schemas, source changes and
    /// resource or adapter failures. This does not authorize metadata-only count.
    pub fn admit_text_with(
        path: &Path,
        format: &str,
        fingerprint: bool,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
        factory: &TextFactory<'_>,
    ) -> Result<Arc<Self>> {
        admit(
            path,
            format,
            fingerprint,
            memory,
            cancellation,
            Some(factory),
        )
    }
}

pub(super) fn admit(
    path: &Path,
    format: &str,
    fingerprint: bool,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
    text_factory: Option<&TextFactory<'_>>,
) -> Result<Arc<LocalColumnarSource>> {
    cancellation.check()?;
    let format = match format {
        "parquet" => "parquet",
        "arrow-ipc" | "arrow_ipc" | "arrow" | "ipc" | "feather" => "arrow-ipc",
        "orc" => "orc",
        "avro" => "avro",
        "csv" if text_factory.is_some() => "csv",
        "tsv" if text_factory.is_some() => "tsv",
        "json" if text_factory.is_some() => "json",
        "jsonl" | "ndjson" if text_factory.is_some() => "jsonl",
        _ => return Err(failed("source has no admitted demand reader")),
    };
    let collection = LocalDataset::admit(path, format, memory, cancellation)?;
    admit_collection(
        collection,
        format,
        fingerprint,
        memory,
        cancellation,
        text_factory,
    )
}

#[allow(clippy::too_many_lines)]
pub(super) fn admit_collection(
    collection: LocalDataset,
    format: &'static str,
    fingerprint: bool,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
    text_factory: Option<&TextFactory<'_>>,
) -> Result<Arc<LocalColumnarSource>> {
    let mut credit = memory.reserve(8192)?;
    let mut files = Vec::new();
    let mut schema: Option<SchemaRef> = None;
    let mut metadata_read_bytes = 0_u64;
    let mut schema_discovery_read_bytes = 0_u64;
    let mut fingerprint_read_bytes = 0_u64;
    let mut content_fingerprints = Vec::new();
    let mut row_groups_total = 0_usize;
    for collected in &collection.files {
        cancellation.check()?;
        credit.resize(add(
            credit.bytes(),
            collected.path.as_os_str().len() as u64 * 8 + 8192,
        )?)?;
        let path = collected.path.clone();
        let identity = Arc::new(SourceIdentity::capture(&path)?);
        let input = OwnedLocalRead::metadata(Arc::clone(&identity), memory, cancellation)?;
        let mut file = File {
            path,
            identity,
            ipc_file_framing: false,
            text: None,
            schema: Arc::new(arrow_schema::Schema::empty()),
            parquet: None,
        };
        let discovered = if let Some(factory) = text_factory {
            let text = factory(&file.path, memory, cancellation)?;
            if !text.is_owned_by(memory) {
                return Err(failed("text source belongs to a different memory owner"));
            }
            if !text.identity().same_generation(&file.identity) {
                return Err(failed(
                    "text adapter returned a different source generation",
                ));
            }
            file.identity.validate()?;
            schema_discovery_read_bytes = add(schema_discovery_read_bytes, text.discovery_bytes())?;
            let schema = text.schema();
            file.text = Some(text);
            schema
        } else {
            let workspace = metadata_workspace(&input)?;
            let _workspace = memory.reserve(workspace)?;
            match format {
                "parquet" => {
                    use super::source::metadata_admission;
                    let admission = metadata_admission::Admission::new(memory, cancellation)?;
                    let metadata = super::ArrowReaderMetadata::load(
                        &input,
                        super::super::parquet_metadata_first_reader_options()
                            .with_metadata_memory_reservation(admission.clone()),
                    )
                    .map_err(metadata_admission::source_error)?;
                    let mut held = admission.finish()?;
                    credit.absorb(&mut held)?;
                    row_groups_total = row_groups_total
                        .checked_add(metadata.metadata().num_row_groups())
                        .ok_or_else(|| failed("source row group count overflow"))?;
                    let schema = Arc::clone(metadata.schema());
                    file.parquet = Some(metadata);
                    schema
                }
                "arrow-ipc" => {
                    file.ipc_file_framing =
                        input.len() >= 6 && input.bytes(0, 6)?.as_ref() == b"ARROW1";
                    ipc_schema(&input, file.ipc_file_framing, memory)?
                }
                "orc" => orc_rust::ArrowReaderBuilder::try_new(input.clone())
                    .map_err(failed)?
                    .schema(),
                "avro" => arrow_avro::reader::ReaderBuilder::new()
                    .build(BufReader::new(cursor(input.clone())))
                    .map_err(failed)?
                    .schema(),
                _ => return Err(failed("missing native source adapter")),
            }
        };
        let bytes = schema_bytes(&discovered)?;
        credit.resize(add(
            credit.bytes(),
            bytes
                .checked_mul(3)
                .ok_or_else(|| failed("schema capacity overflow"))?,
        )?)?;
        file.schema = Arc::clone(&discovered);
        let discovered = schema::with_partitions(&discovered, &collected.partitions)?;
        schema = Some(schema::merge(
            schema.as_ref(),
            &discovered,
            collection.union_by_name,
        )?);
        metadata_read_bytes = add(
            metadata_read_bytes,
            input.metrics().bytes.load(Ordering::Relaxed),
        )?;
        if fingerprint {
            content_fingerprints.push(super::source::content_fingerprint(&input, cancellation)?);
            fingerprint_read_bytes = add(fingerprint_read_bytes, input.len())?;
        }
        file.identity.validate()?;
        files.try_reserve(1).map_err(failed)?;
        files.push(file);
    }
    let source = Arc::new(LocalColumnarSource {
        collection,
        invalidated: AtomicBool::new(false),
        files: Vec::new(),
        other: Some(Source {
            format,
            schema: schema.ok_or_else(|| failed("source inventory is empty"))?,
            files,
            schema_discovery_read_bytes,
        }),
        metadata_read_bytes,
        metadata_credit: credit,
        row_groups_total,
        fingerprint_read_bytes,
        content_fingerprints,
    });
    source.validate_generation_controlled(cancellation)?;
    Ok(source)
}

fn schema_bytes(schema: &SchemaRef) -> Result<u64> {
    schema.fields().iter().try_fold(8192_u64, |sum, field| {
        add(
            add(sum, field.name().len() as u64 * 4 + 1024)?,
            crate::native_payload_schema::arrow_file_metadata_bytes(field.data_type())?,
        )
    })
}

fn metadata_workspace(input: &OwnedLocalRead) -> Result<u64> {
    input
        .len()
        .min(64 << 10)
        .checked_mul(32)
        .and_then(|n| n.checked_add(8192))
        .ok_or_else(|| failed("source metadata workspace overflow"))
}

fn ipc_schema(input: &OwnedLocalRead, file: bool, memory: &LiveMemoryPool) -> Result<SchemaRef> {
    if !file {
        if input.len() < 8 {
            return Err(failed("IPC stream has no schema message"));
        }
        let header = input.bytes(0, 8)?;
        let length = if header[..4] == [255; 4] {
            &header[4..8]
        } else {
            &header[..4]
        };
        let length = i32::from_le_bytes(length.try_into().map_err(failed)?);
        if length <= 0 || length > 16 << 20 {
            return Err(failed("IPC stream schema exceeds metadata admission"));
        }
        let _workspace = memory.reserve(u64::try_from(length).map_err(failed)? * 32 + 8192)?;
        return arrow_ipc::reader::StreamReader::try_new(cursor(input.clone()), None)
            .map(|reader| reader.schema())
            .map_err(failed);
    }
    if input.len() < 16 {
        return Err(failed("IPC file is too short"));
    }
    let tail = input.bytes(input.len() - 10, 10)?;
    let length = u32::from_le_bytes(tail[..4].try_into().map_err(failed)?) as usize;
    if &tail[4..] != b"ARROW1"
        || length == 0
        || length > 16 << 20
        || length as u64 > input.len() - 16
    {
        return Err(failed("IPC footer is malformed or exceeds admission"));
    }
    let _workspace = memory.reserve(length as u64 * 32 + 8192)?;
    let bytes = input.bytes(input.len() - 10 - length as u64, length)?;
    let footer = arrow_ipc::root_as_footer(&bytes).map_err(failed)?;
    let schema = footer
        .schema()
        .ok_or_else(|| failed("IPC footer has no schema"))?;
    if !schema.endianness().equals_to_target_endianness() {
        return Err(failed("IPC endianness differs from native provider"));
    }
    Ok(Arc::new(
        std::panic::catch_unwind(|| arrow_ipc::convert::fb_to_schema(schema))
            .map_err(|_| failed("IPC footer contains an invalid logical schema"))?,
    ))
}

pub(super) struct Reader {
    active: Option<Box<dyn RecordBatchReader + Send>>,
    workspace: Option<MemoryLease>,
    next_file: usize,
    next_parquet_group: Option<(usize, usize)>,
    row_groups_read: usize,
    rows: u64,
    decoded_rows: u64,
    ended: bool,
}

impl Reader {
    pub(super) fn new() -> Self {
        Self {
            active: None,
            workspace: None,
            next_file: 0,
            next_parquet_group: None,
            row_groups_read: 0,
            rows: 0,
            decoded_rows: 0,
            ended: false,
        }
    }

    pub(super) fn next(
        &mut self,
        prepared: &PreparedColumnarSource,
        metrics: &Arc<LocalReadMetrics>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<(RecordBatch, u64)>> {
        self.next_with(
            prepared,
            context.memory(),
            context.cancellation(),
            &mut |identity| context.local_source_reader(identity, Arc::clone(metrics)),
        )
    }

    pub(super) fn next_preparation(
        &mut self,
        prepared: &PreparedColumnarSource,
        metrics: &Arc<LocalReadMetrics>,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Option<(RecordBatch, u64)>> {
        self.next_with(prepared, memory, cancellation, &mut |identity| {
            OwnedLocalRead::sequential(identity, memory, cancellation, Arc::clone(metrics))
        })
    }

    #[allow(clippy::too_many_lines)]
    fn next_with(
        &mut self,
        prepared: &PreparedColumnarSource,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
        open: &mut impl FnMut(Arc<SourceIdentity>) -> Result<OwnedLocalRead>,
    ) -> Result<Option<(RecordBatch, u64)>> {
        let source = prepared
            .source
            .other
            .as_ref()
            .ok_or_else(|| failed("generic source owner missing"))?;
        loop {
            cancellation.check()?;
            if self.next_file == 0 {
                prepared
                    .source
                    .validate_generation_controlled(cancellation)?;
            } else {
                source.files[self.next_file - 1].identity.validate()?;
            }
            if prepared.max_rows.is_some_and(|limit| self.rows >= limit) {
                self.active.take();
                self.workspace.take();
                self.ended = true;
                prepared
                    .source
                    .validate_generation_controlled(cancellation)?;
                return Ok(None);
            }
            if let Some(active) = &mut self.active {
                if let Some(batch) = active.next() {
                    let batch = batch.map_err(failed)?;
                    let actual_rows = batch.num_rows();
                    self.decoded_rows = add(self.decoded_rows, actual_rows as u64)?;
                    let rows = prepared.max_rows.map_or(actual_rows, |limit| {
                        actual_rows.min(usize::try_from(limit - self.rows).unwrap_or(usize::MAX))
                    });
                    let batch = if rows == actual_rows {
                        batch
                    } else {
                        batch.slice(0, rows)
                    };
                    let first = self.rows;
                    self.rows = add(self.rows, rows as u64)?;
                    // Retained Arrow buffers receive credits before native
                    // handoff; upstream decode temporaries are not an RSS bound.
                    let target = Arc::new(
                        source
                            .schema
                            .project(&prepared.projection)
                            .map_err(failed)?,
                    );
                    let partitions =
                        &prepared.source.collection.files[self.next_file - 1].partitions;
                    let batch = schema::align(batch, target, partitions, memory)?;
                    source.files[self.next_file - 1].identity.validate()?;
                    return Ok(Some((batch, first)));
                }
                self.active.take();
                self.workspace.take();
            }
            let (file_index, group_index) = self
                .next_parquet_group
                .take()
                .unwrap_or((self.next_file, 0));
            let Some(file) = source.files.get(file_index) else {
                self.ended = true;
                prepared
                    .source
                    .validate_generation_controlled(cancellation)?;
                return Ok(None);
            };
            if file_index == self.next_file {
                self.next_file += 1;
            }
            let projection =
                schema::local_projection(&file.schema, &source.schema, &prepared.projection);
            let input = open(Arc::clone(&file.identity))?;
            let shape = (projection.len() as u64)
                .checked_mul(8192 * 64)
                .and_then(|n| n.checked_add(65_536))
                .ok_or_else(|| failed("decoder shape overflow"))?;
            self.workspace = Some(memory.reserve(shape)?);
            self.active = Some(match source.format {
                "parquet" => {
                    let metadata = file
                        .parquet
                        .as_ref()
                        .ok_or_else(|| failed("Parquet metadata owner missing"))?;
                    let groups = metadata.metadata().num_row_groups();
                    if group_index >= groups {
                        self.workspace.take();
                        continue;
                    }
                    if group_index + 1 < groups {
                        self.next_parquet_group = Some((file_index, group_index + 1));
                    }
                    let mask = super::ProjectionMask::roots(metadata.parquet_schema(), projection);
                    self.row_groups_read = self
                        .row_groups_read
                        .checked_add(1)
                        .ok_or_else(|| failed("read group count overflow"))?;
                    Box::new(parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::new_with_metadata(input, metadata.clone())
                        .with_row_groups(vec![group_index]).with_projection(mask).with_batch_size(8192).build().map_err(failed)?)
                }
                "arrow-ipc" if file.ipc_file_framing => Box::new(
                    arrow_ipc::reader::FileReader::try_new(cursor(input), Some(projection))
                        .map_err(failed)?,
                ),
                "arrow-ipc" => Box::new(
                    arrow_ipc::reader::StreamReader::try_new(cursor(input), Some(projection))
                        .map_err(failed)?,
                ),
                "avro" => Box::new(
                    arrow_avro::reader::ReaderBuilder::new()
                        .with_batch_size(8192)
                        .with_projection(projection)
                        .build(BufReader::new(cursor(input)))
                        .map_err(failed)?,
                ),
                "orc" => {
                    let builder = orc_rust::ArrowReaderBuilder::try_new(input).map_err(failed)?;
                    let names = projection
                        .iter()
                        .map(|&i| file.schema.field(i).name().clone())
                        .collect::<Vec<_>>();
                    let mask = orc_rust::projection::ProjectionMask::named_roots(
                        builder.file_metadata().root_data_type(),
                        &names,
                    );
                    Box::new(builder.with_batch_size(8192).with_projection(mask).build())
                }
                "csv" | "tsv" | "json" | "jsonl" => Box::new(
                    file.text
                        .as_ref()
                        .ok_or_else(|| failed("text provider missing"))?
                        .open_with_reader(
                            &projection,
                            8192,
                            cancellation,
                            Box::new(cursor(input)),
                        )?,
                ),
                _ => return Err(failed("generic source adapter missing")),
            });
        }
    }

    pub(super) fn report(
        &self,
        prepared: &PreparedColumnarSource,
        metrics: &Arc<LocalReadMetrics>,
    ) -> Result<ColumnarReadReport> {
        if !self.ended || self.active.is_some() || self.workspace.is_some() {
            return Err(failed("source did not reach its declared end"));
        }
        let source = prepared
            .source
            .other
            .as_ref()
            .ok_or_else(|| failed("generic source owner missing"))?;
        Ok(ColumnarReadReport {
            format: source.format,
            projected_columns: prepared.projection.len(),
            available_columns: source.schema.fields().len(),
            physical_leaf_columns: source
                .files
                .iter()
                .map(|file| {
                    schema::local_projection(&file.schema, &source.schema, &prepared.projection)
                        .iter()
                        .map(|&index| schema::physical_leaves(file.schema.field(index).data_type()))
                        .sum()
                })
                .max()
                .unwrap_or(0),
            row_groups_total: prepared.source.row_groups_total,
            row_groups_read: self.row_groups_read,
            rows_decoded: self.decoded_rows,
            requested_prefix_rows: prepared.max_rows,
            files: source.files.len(),
            fingerprint_read_bytes: prepared.source.fingerprint_read_bytes,
            content_fingerprints: prepared.source.content_fingerprints.clone(),
            metadata_read_bytes: prepared.source.metadata_read_bytes,
            schema_discovery_read_bytes: source.schema_discovery_read_bytes,
            data_read_bytes: metrics.bytes.load(Ordering::Relaxed),
            data_read_requests: metrics.requests.load(Ordering::Relaxed),
            parquet_selected_chunks: false,
        })
    }
}

fn cursor(input: OwnedLocalRead) -> ColumnarCursor {
    ColumnarCursor { input, position: 0 }
}

impl Seek for ColumnarCursor {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        let next = match position {
            SeekFrom::Start(value) => i128::from(value),
            SeekFrom::Current(value) => i128::from(self.position) + i128::from(value),
            SeekFrom::End(value) => i128::from(self.input.len()) + i128::from(value),
        };
        let next = u64::try_from(next).map_err(std::io::Error::other)?;
        if next > self.input.len() {
            return Err(std::io::Error::other("source seek exceeds its held extent"));
        }
        self.position = next;
        Ok(next)
    }
}

impl orc_rust::reader::ChunkReader for OwnedLocalRead {
    type T = ColumnarCursor;
    fn len(&self) -> u64 {
        self.len()
    }
    fn get_read(&self, position: u64) -> std::io::Result<Self::T> {
        if position > self.len() {
            return Err(std::io::Error::other(
                "source offset exceeds its held extent",
            ));
        }
        Ok(ColumnarCursor {
            input: self.clone(),
            position,
        })
    }
    fn get_bytes(&self, position: u64, length: u64) -> std::io::Result<bytes::Bytes> {
        self.bytes(
            position,
            usize::try_from(length).map_err(std::io::Error::other)?,
        )
        .map_err(std::io::Error::other)
    }
}

fn add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| failed("source work counter overflow"))
}
