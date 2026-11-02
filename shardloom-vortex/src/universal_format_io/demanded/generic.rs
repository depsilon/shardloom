//! Format adapters feed the same demand-bound native producer. Providers
//! decode formats only; predicates and every relational operator stay native.
//!
//! Projection is applied by IPC/ORC/Avro providers and by text construction.
//! Parquet consumes shared mapped row-group exclusions; other formats do not.
//! Provider
//! transient allocations remain outside the retained-batch accounting boundary.

use super::{
    ColumnarCursor, ColumnarReadReport, LocalColumnarSource, LocalReadMetrics,
    NativeExecutionContext, OwnedLocalRead, PreparedColumnarSource, Result, failed,
};
use crate::source_identity::SourceIdentity;
use crate::universal_format_io::owned_batch::OwnedBatch;
use crate::universal_format_io::{local_dataset::LocalDataset, text_stream::TextSource};
use arrow_schema::SchemaRef;
use shardloom_exec::{
    adaptive_work::{WorkCost, WorkShape},
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::{
    io::{Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[path = "generic_decoder.rs"]
pub(super) mod decoder;
#[path = "generic_schema.rs"]
mod schema;
use decoder::Decoder;
#[path = "table.rs"]
mod table;

type TextFactory<'a> =
    dyn Fn(&Path, &LiveMemoryPool, &CancellationToken) -> Result<Arc<TextSource>> + 'a;

pub(super) struct Source {
    pub(super) format: &'static str,
    pub(super) schema: SchemaRef,
    pub(super) files: Vec<File>,
    pub(super) schema_discovery_read_bytes: u64,
    pub(super) preparation_dictionary_status: Option<&'static str>,
    pub(super) table: Option<Arc<table::Snapshot>>,
}

impl Source {
    pub(super) fn row_count_hint(&self, cancellation: &CancellationToken) -> Result<Option<usize>> {
        if self.table.is_some() {
            return Ok(None);
        }
        let mut rows = 0_u64;
        for file in &self.files {
            cancellation.check()?;
            let count = if let Some(metadata) = &file.parquet {
                Some(super::metadata_count::file_rows(
                    metadata.metadata(),
                    cancellation,
                )?)
            } else if let Some(metadata) = &file.orc {
                metadata.exact_number_of_rows().map_err(failed)?
            } else {
                file.text.as_ref().map(|text| text.rows() as u64)
            };
            let Some(count) = count else { return Ok(None) };
            rows = add(rows, count)?;
        }
        usize::try_from(rows).map(Some).map_err(failed)
    }
}

pub(super) struct File {
    pub(super) path: PathBuf,
    pub(super) identity: Arc<SourceIdentity>,
    text: Option<Arc<TextSource>>,
    pub(super) schema: SchemaRef,
    ipc: Option<Arc<super::ipc::Metadata>>,
    pub(super) parquet: Option<super::ArrowReaderMetadata>,
    pub(super) orc: Option<Arc<orc_rust::reader::metadata::FileMetadata>>,
    // Drop the physical schema before its retained provider structure credit.
    avro_schema_credit: Option<Arc<dyn arrow_avro::reader::DecoderMemoryReservation>>,
    table: Option<table::FileMapping>,
}

impl LocalColumnarSource {
    /// Bind a local table snapshot through its format-specific visibility rules.
    /// Declaration remains inert; this method is called under action admission.
    /// # Errors
    /// Rejects unsupported table states, changed inputs and insufficient grants.
    pub fn admit_table(
        path: &Path,
        format: &str,
        selection: &shardloom_core::input::LocalTableSelection,
        fingerprint: bool,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Arc<Self>> {
        let snapshot = table::Snapshot::admit(path, format, selection, memory, cancellation)?;
        let collection =
            LocalDataset::selected(&snapshot.root, snapshot.paths(), memory, cancellation)?;
        admit_collection_with_table(
            collection,
            "parquet",
            fingerprint,
            memory,
            cancellation,
            None,
            Some(snapshot),
        )
    }

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
    admit_collection_with_table(
        collection,
        format,
        fingerprint,
        memory,
        cancellation,
        text_factory,
        None,
    )
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
fn admit_collection_with_table(
    collection: LocalDataset,
    format: &'static str,
    fingerprint: bool,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
    text_factory: Option<&TextFactory<'_>>,
    table: Option<Arc<table::Snapshot>>,
) -> Result<Arc<LocalColumnarSource>> {
    let mut credit = memory.reserve(8192)?;
    let mut files = Vec::new();
    let mut schema: Option<SchemaRef> = table.as_ref().map(|table| Arc::clone(&table.schema));
    let mut metadata_read_bytes = table.as_ref().map_or(0, |table| table.metadata_bytes());
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
            text: None,
            schema: Arc::new(arrow_schema::Schema::empty()),
            ipc: None,
            parquet: None,
            orc: None,
            avro_schema_credit: None,
            table: None,
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
            // Avro and IPC admit physical input and source-shaped metadata
            // structures before construction through this pool.
            let _workspace = (!matches!(format, "avro" | "arrow-ipc"))
                .then(|| metadata_workspace(&input).and_then(|bytes| memory.reserve(bytes)))
                .transpose()?;
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
                    let metadata = super::ipc::discover(&input, memory, cancellation)?;
                    let schema = metadata.schema.schema().clone();
                    file.ipc = Some(metadata);
                    schema
                }
                "orc" => {
                    let (metadata, mut held) =
                        super::orc::admit(input.clone(), memory, cancellation)?;
                    credit.absorb(&mut held)?;
                    row_groups_total = row_groups_total
                        .checked_add(metadata.stripe_metadatas().len())
                        .ok_or_else(|| failed("source stripe count overflow"))?;
                    let builder = orc_rust::ArrowReaderBuilder::new_with_metadata(
                        input.clone(),
                        Arc::clone(&metadata),
                    );
                    let schema = builder.schema();
                    file.orc = Some(metadata);
                    schema
                }
                "avro" => {
                    let discovered = decoder::avro_schema(input.clone(), memory, cancellation)?;
                    file.avro_schema_credit = Some(discovered.credit);
                    discovered.schema
                }
                _ => return Err(failed("missing native source adapter")),
            }
        };
        let bytes = schema_bytes(&discovered)?;
        credit.resize(add(
            credit.bytes(),
            bytes
                // Original Avro/IPC schemas already retain provider credits;
                // partition/merged copies still need native capacity.
                .checked_mul(if matches!(format, "avro" | "arrow-ipc") {
                    2
                } else {
                    3
                })
                .ok_or_else(|| failed("schema capacity overflow"))?,
        )?)?;
        file.schema = Arc::clone(&discovered);
        if let Some(table) = &table {
            credit.resize(add(
                credit.bytes(),
                bytes
                    .checked_mul(4)
                    .ok_or_else(|| failed("table mapping capacity overflow"))?,
            )?)?;
            let metadata = file
                .parquet
                .as_ref()
                .ok_or_else(|| failed("table file requires Parquet metadata"))?;
            let rows = super::metadata_count::file_rows(metadata.metadata(), cancellation)?;
            file.table = Some(table::FileMapping::admit(
                table,
                files.len(),
                &discovered,
                rows,
            )?);
        } else {
            let discovered = schema::with_partitions(&discovered, &collected.partitions)?;
            schema = Some(schema::merge(
                schema.as_ref(),
                &discovered,
                collection.union_by_name,
            )?);
        }
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
            preparation_dictionary_status: None,
            table,
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

/// Reuse the existing single-file preparation layout policy with the retained
/// metadata. Query admission and collection schema alignment stay unchanged.
pub(super) fn preparation_layout(
    mut source: Arc<LocalColumnarSource>,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<Arc<LocalColumnarSource>> {
    if !source.collection.is_single_file()
        || source
            .other
            .as_ref()
            .is_none_or(|source| source.format != "parquet")
    {
        return Ok(source);
    }
    cancellation.check()?;
    let owned = Arc::get_mut(&mut source)
        .ok_or_else(|| failed("preparation layout requires exclusive source admission"))?;
    let format = owned.other.as_mut().expect("format checked");
    let rows = format.row_count_hint(cancellation)?;
    let file = format
        .files
        .first_mut()
        .ok_or_else(|| failed("missing preparation file"))?;
    // Keep original schema owners live while admitting the replacement schema,
    // reader field map and retained merged-schema structures.
    let mut replacement = memory.reserve(
        schema_bytes(&file.schema)?
            .checked_mul(3)
            .ok_or_else(|| failed("preparation schema capacity overflow"))?,
    )?;
    let plan = super::super::parquet_dictionary_schema_plan(&file.schema, rows);
    let old = file
        .parquet
        .as_ref()
        .ok_or_else(|| failed("missing preparation metadata"))?;
    let metadata = super::ArrowReaderMetadata::try_new(
        Arc::clone(old.metadata()),
        super::super::parquet_reader_options_with_optional_schema_hint(plan.schema_hint.as_ref()),
    )
    .map_err(super::source::metadata_admission::source_error)?;
    file.schema = Arc::clone(metadata.schema());
    file.parquet = Some(metadata);
    format.schema = Arc::clone(&file.schema);
    format.preparation_dictionary_status = Some(plan.dictionary_preservation_status);
    owned.metadata_credit.absorb(&mut replacement)?;
    owned.validate_generation_controlled(cancellation)?;
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

pub(super) struct Reader {
    active: Option<Decoder>,
    workspace: Option<MemoryLease>,
    cost: Option<WorkCost>,
    sizing: super::batch_work::Sizing,
    next_file: usize,
    next_group: usize,
    group_end: usize,
    batch_rows: usize,
    active_region: Option<super::SourceGroup>,
    region_rows: u64,
    file_row_offset: u64,
    row_groups_read: usize,
    rows: u64,
    decoded_rows: u64,
    owned_parquet_handoff: bool,
    ended: bool,
}

impl Reader {
    pub(super) fn new() -> Self {
        Self {
            active: None,
            workspace: None,
            cost: None,
            sizing: super::batch_work::Sizing::default(),
            next_file: 0,
            next_group: 0,
            group_end: usize::MAX,
            batch_rows: super::batch_work::MAX_ROWS,
            active_region: None,
            region_rows: 0,
            file_row_offset: 0,
            row_groups_read: 0,
            rows: 0,
            decoded_rows: 0,
            owned_parquet_handoff: false,
            ended: false,
        }
    }

    /// Only the sealed complete-ingest Parquet constructor selects this path.
    /// Its batch envelope retains input credits through the native conversion.
    pub(super) fn for_owned_parquet(batch_rows: usize) -> Self {
        Self {
            owned_parquet_handoff: true,
            batch_rows,
            ..Self::new()
        }
    }

    /// A complete-source task shares the immutable admission and visits only
    /// its assigned contiguous region range. No footer or schema is re-admitted.
    #[cfg(feature = "vortex-write")]
    pub(super) fn for_owned_parquet_groups(
        groups: std::ops::Range<usize>,
        batch_rows: usize,
    ) -> Self {
        Self {
            next_group: groups.start,
            group_end: groups.end,
            ..Self::for_owned_parquet(batch_rows)
        }
    }

    pub(super) fn next(
        &mut self,
        prepared: &PreparedColumnarSource,
        metrics: &Arc<LocalReadMetrics>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<(OwnedBatch, u64)>> {
        self.next_with(
            prepared,
            context.memory(),
            context.cancellation(),
            &mut |identity| context.local_source_reader(identity, Arc::clone(metrics)),
            &mut |shape| {
                context
                    .record_adaptive_work(crate::native_work_sizing::Owner::ColumnarDecode, shape);
            },
        )
    }

    pub(super) fn next_preparation(
        &mut self,
        prepared: &PreparedColumnarSource,
        metrics: &Arc<LocalReadMetrics>,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Option<(OwnedBatch, u64)>> {
        self.next_with(
            prepared,
            memory,
            cancellation,
            &mut |identity| {
                OwnedLocalRead::sequential(identity, memory, cancellation, Arc::clone(metrics))
            },
            &mut |_| {},
        )
    }

    #[allow(clippy::too_many_lines)]
    fn next_with(
        &mut self,
        prepared: &PreparedColumnarSource,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
        open: &mut impl FnMut(Arc<SourceIdentity>) -> Result<OwnedLocalRead>,
        record: &mut impl FnMut(WorkShape),
    ) -> Result<Option<(OwnedBatch, u64)>> {
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
                let block_rows = active.prepare_rows()?;
                let credited_input = active.credited_input();
                if active.can_resize()
                    && let Some(limit) = prepared.max_rows
                {
                    active
                        .set_batch_size(usize::try_from(limit - self.rows).unwrap_or(usize::MAX))?;
                }
                let selected = self
                    .cost
                    .map(|cost| {
                        let remaining = block_rows.unwrap_or_else(|| {
                            self.active_region
                                .map_or(u64::MAX, |region| region.rows - self.region_rows)
                        });
                        let remaining = prepared
                            .max_rows
                            .map_or(remaining, |limit| remaining.min(limit - self.rows));
                        self.sizing.select(memory, cost, remaining, self.batch_rows)
                    })
                    .transpose()?
                    .flatten();
                if let Some(selected) = selected {
                    active.set_batch_size(usize::try_from(selected.units()).map_err(failed)?)?;
                }
                if let Some(batch) = active.next() {
                    let batch = batch.map_err(decoder::source_error)?;
                    let actual_rows = batch.num_rows();
                    self.decoded_rows = add(self.decoded_rows, actual_rows as u64)?;
                    let rows = if source.table.is_some() {
                        actual_rows
                    } else {
                        prepared.max_rows.map_or(actual_rows, |limit| {
                            actual_rows
                                .min(usize::try_from(limit - self.rows).unwrap_or(usize::MAX))
                        })
                    };
                    let batch = if rows == actual_rows {
                        batch
                    } else {
                        batch.map(|batch| {
                            Ok::<_, shardloom_core::ShardLoomError>(batch.slice(0, rows))
                        })?
                    };
                    let physical_first = add(self.file_row_offset, self.region_rows)?;
                    let first = if source.table.is_some() {
                        self.rows
                    } else {
                        self.active_region.map_or(Ok(self.rows), |region| {
                            add(region.first_row, self.region_rows)
                        })?
                    };
                    self.region_rows = add(self.region_rows, rows as u64)?;
                    if self
                        .active_region
                        .is_some_and(|region| self.region_rows > region.rows)
                    {
                        return Err(failed("decoder exceeded its admitted source extent"));
                    }
                    // Retained Arrow buffers receive credits before native
                    // handoff; upstream decode temporaries are not an RSS bound.
                    let (target, projection_credit) =
                        schema::project(&source.schema, &prepared.projection, memory)?;
                    let partitions =
                        &prepared.source.collection.files[self.next_file - 1].partitions;
                    let batch = if let Some(table) = &source.table {
                        let mapping = source.files[self.next_file - 1]
                            .table
                            .as_ref()
                            .ok_or_else(|| failed("table field mapping is absent"))?;
                        mapping.apply(
                            table,
                            self.next_file - 1,
                            batch,
                            target,
                            &prepared.projection,
                            physical_first,
                            memory,
                            cancellation,
                        )?
                    } else if self.owned_parquet_handoff {
                        if source.format != "parquet" {
                            return Err(failed(
                                "owned Parquet handoff requires its physical provider",
                            ));
                        }
                        schema::align_owned_parquet(batch, target, partitions, memory)?
                    } else {
                        schema::align(batch, target, partitions, memory, credited_input)?
                    };
                    let batch = if source.table.is_some() {
                        if let Some(limit) = prepared.max_rows {
                            let rows = batch
                                .num_rows()
                                .min(usize::try_from(limit - self.rows).unwrap_or(usize::MAX));
                            batch.map(|batch| {
                                Ok::<_, shardloom_core::ShardLoomError>(batch.slice(0, rows))
                            })?
                        } else {
                            batch
                        }
                    } else {
                        batch
                    };
                    self.rows = add(self.rows, batch.num_rows() as u64)?;
                    let batch = if let Some(credit) = projection_credit {
                        batch.retain(credit)
                    } else {
                        batch
                    };
                    source.files[self.next_file - 1].identity.validate()?;
                    cancellation.check()?;
                    if let Some(selected) = selected.filter(|_| actual_rows > 0) {
                        self.sizing.complete(selected, &batch);
                        record(selected);
                    }
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    return Ok(Some((batch.retain(prepared.source.clone()), first)));
                }
                if self
                    .active_region
                    .is_some_and(|region| self.region_rows != region.rows)
                {
                    return Err(failed(
                        "decoder did not complete its admitted source extent",
                    ));
                }
                self.active.take();
                self.workspace.take();
            }
            self.active_region = if matches!(source.format, "parquet" | "orc") {
                let Some(&region) = prepared
                    .row_groups
                    .get(self.next_group)
                    .filter(|_| self.next_group < self.group_end)
                else {
                    self.ended = true;
                    prepared
                        .source
                        .validate_generation_controlled(cancellation)?;
                    return Ok(None);
                };
                self.next_group += 1;
                Some(region)
            } else {
                None
            };
            self.region_rows = 0;
            let file_index = self
                .active_region
                .map_or(self.next_file, |region| region.file);
            let Some(file) = source.files.get(file_index) else {
                self.ended = true;
                prepared
                    .source
                    .validate_generation_controlled(cancellation)?;
                return Ok(None);
            };
            self.next_file = file_index + 1;
            let projection = if let Some(table) = &file.table {
                table.projection(&file.schema, &prepared.projection)?
            } else {
                schema::local_projection(&file.schema, &source.schema, &prepared.projection)
            };
            self.file_row_offset =
                if let (Some(metadata), Some(region)) = (&file.parquet, self.active_region) {
                    (0..region.index).try_fold(0_u64, |sum, index| {
                        add(
                            sum,
                            u64::try_from(metadata.metadata().row_group(index).num_rows())
                                .map_err(failed)?,
                        )
                    })?
                } else {
                    0
                };
            let input = open(Arc::clone(&file.identity))?;
            self.cost = if matches!(source.format, "parquet" | "orc" | "avro") {
                let physical = file.schema.project(&projection).map_err(failed)?;
                let logical = source
                    .schema
                    .project(&prepared.projection)
                    .map_err(failed)?;
                let cost = super::batch_work::cost(
                    &physical,
                    &logical,
                    &prepared.source.collection.files[file_index].partitions,
                )?;
                Some(
                    if let (Some(_), Some(table)) = (&file.table, &source.table) {
                        table::FileMapping::cost(table, file_index, cost)?
                    } else {
                        cost
                    },
                )
            } else {
                None
            };
            self.active = Some(match source.format {
                "parquet" => {
                    let metadata = file
                        .parquet
                        .as_ref()
                        .ok_or_else(|| failed("Parquet metadata owner missing"))?;
                    let region = self
                        .active_region
                        .as_mut()
                        .ok_or_else(|| failed("Parquet region owner missing"))?;
                    let mask = super::ProjectionMask::roots(metadata.parquet_schema(), projection);
                    self.workspace = Some(memory.reserve(super::batch_work::parquet_workspace(
                        metadata.metadata().row_group(region.index),
                        &mask,
                    )?)?);
                    self.row_groups_read = self
                        .row_groups_read
                        .checked_add(1)
                        .ok_or_else(|| failed("read group count overflow"))?;
                    let mut builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::new_with_metadata(input, metadata.clone())
                        .with_row_groups(vec![region.index]).with_projection(mask).with_batch_size(self.batch_rows);
                    if let Some(limit) = prepared.max_rows.filter(|_| source.table.is_none()) {
                        region.rows = region.rows.min(limit.saturating_sub(self.rows));
                        builder = builder.with_limit(usize::try_from(region.rows).map_err(failed)?);
                    }
                    Decoder::Parquet(builder.build().map_err(failed)?)
                }
                "arrow-ipc" => Decoder::Ipc(super::ipc::Reader::new(
                    input,
                    file.ipc
                        .as_ref()
                        .ok_or_else(|| failed("IPC metadata owner missing"))?
                        .clone(),
                    &projection,
                    memory,
                    cancellation,
                )?),
                "avro" => {
                    // A full physical projection needs no generated reader
                    // schema or resolution plan. Alignment still applies the
                    // logical field order and partitions after decoding.
                    let projection =
                        (!projection.iter().copied().eq(0..file.schema.fields().len()))
                            .then_some(projection);
                    decoder::avro(input, projection, memory, cancellation)?
                }
                "orc" => {
                    let metadata = file
                        .orc
                        .as_ref()
                        .ok_or_else(|| failed("ORC metadata owner missing"))?;
                    let region = self
                        .active_region
                        .as_mut()
                        .ok_or_else(|| failed("ORC stripe owner missing"))?;
                    let stripe = &metadata.stripe_metadatas()[region.index];
                    let start = usize::try_from(stripe.offset()).map_err(failed)?;
                    let end = start
                        .checked_add(1)
                        .ok_or_else(|| failed("ORC stripe offset overflow"))?;
                    let mut builder = orc_rust::ArrowReaderBuilder::new_with_metadata(
                        input,
                        Arc::clone(metadata),
                    )
                    .with_decoder_memory(decoder::orc_memory(memory, cancellation))
                    .with_file_byte_range(start..end);
                    self.row_groups_read = self
                        .row_groups_read
                        .checked_add(1)
                        .ok_or_else(|| failed("read stripe count overflow"))?;
                    if let Some(limit) = prepared.max_rows {
                        region.rows = region.rows.min(limit.saturating_sub(self.rows));
                        let selected = usize::try_from(region.rows).map_err(failed)?;
                        let skipped = usize::try_from(stripe.number_of_rows() - region.rows)
                            .map_err(failed)?;
                        builder = builder.with_row_selection(
                            vec![
                                orc_rust::row_selection::RowSelector::select(selected),
                                orc_rust::row_selection::RowSelector::skip(skipped),
                            ]
                            .into(),
                        );
                    }
                    let names = projection
                        .iter()
                        .map(|&i| file.schema.field(i).name().clone())
                        .collect::<Vec<_>>();
                    let mask = orc_rust::projection::ProjectionMask::named_roots(
                        builder.file_metadata().root_data_type(),
                        &names,
                    );
                    Decoder::Orc(
                        builder
                            .with_batch_size(super::batch_work::MAX_ROWS)
                            .with_projection(mask)
                            .try_build()
                            .map_err(|error| decoder::source_error(error.into()))?,
                    )
                }
                "csv" | "tsv" | "json" | "jsonl" => Decoder::Text(
                    file.text
                        .as_ref()
                        .ok_or_else(|| failed("text provider missing"))?
                        .open_with_reader(&projection, 8192, cancellation, Box::new(cursor(input)))?
                        .with_downstream_cost(super::batch_work::alignment_cost(
                            &source
                                .schema
                                .project(&prepared.projection)
                                .map_err(failed)?,
                            &prepared.source.collection.files[file_index].partitions,
                        )?),
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
        let physical_leaf_columns = source.files.iter().try_fold(0, |maximum, file| {
            let projection = if let Some(mapping) = &file.table {
                mapping.projection(&file.schema, &prepared.projection)?
            } else {
                schema::local_projection(&file.schema, &source.schema, &prepared.projection)
            };
            Ok::<_, shardloom_core::ShardLoomError>(
                maximum.max(
                    projection
                        .iter()
                        .map(|&index| schema::physical_leaves(file.schema.field(index).data_type()))
                        .sum(),
                ),
            )
        })?;
        Ok(ColumnarReadReport {
            format: source.format,
            table_snapshot: source
                .table
                .as_ref()
                .map(|table| super::TableSnapshotReadReport {
                    format: table.format,
                    protocol_version: table.version,
                    snapshot: table.id.clone(),
                    location: table.root.to_string_lossy().into_owned(),
                }),
            projected_columns: prepared.projection.len(),
            available_columns: source.schema.fields().len(),
            physical_leaf_columns,
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
            adaptive_decode: self.sizing.report(),
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
