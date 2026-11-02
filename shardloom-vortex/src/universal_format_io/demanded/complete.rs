//! Explicit full-source preparation reuses the same held format readers and
//! schema alignment. It has no query demand or partial-source cache semantics.

use super::{
    LocalColumnarSource, LocalReadMetrics, PreparedColumnarSource, Result, failed, generic,
};
use crate::universal_format_io::owned_batch::{
    ArrowReader, BatchResult, OwnedBatchReader, SchemaReader,
};
use crate::universal_format_io::{
    FlatColumnarStreamSourcePlan, FlatLocalColumnarStreamSource,
    flat_columnar_stream_source_from_reader,
};
use arrow_schema::{ArrowError, SchemaRef};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::{path::Path, sync::Arc};

#[cfg(feature = "vortex-write")]
#[path = "complete_parquet.rs"]
mod parquet;

impl LocalColumnarSource {
    /// Check source aliases and collection membership before staging an output.
    /// # Errors
    /// Rejects changed inputs and destinations that would change the source.
    pub fn validate_output_destination(&self, path: &Path) -> Result<()> {
        self.validate_destination(path)
    }

    /// Admit binary files/collections for complete native preparation. This
    /// deliberately reads every source row; it does not select query regions.
    /// Text callers use `admit_text_with` with their explicit scalar codec.
    /// # Errors
    /// Rejects malformed input, unsupported formats, changes and denied credit.
    pub fn admit_complete(
        path: &Path,
        format: &str,
        fingerprint: bool,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<Arc<Self>> {
        let source = generic::admit(path, format, fingerprint, memory, cancellation, None)?;
        generic::preparation_layout(source, memory, cancellation)
    }

    /// Physical bytes in the held selected file inventory, before decoding.
    /// # Errors
    /// Rejects an overflowing aggregate source size.
    pub fn source_byte_size(&self) -> Result<u64> {
        self.inventory().try_fold(0_u64, |sum, (_, identity)| {
            sum.checked_add(identity.generation.len)
                .ok_or_else(|| failed("source size overflow"))
        })
    }

    /// Ordered complete-file digests computed only when explicitly requested.
    #[must_use]
    pub fn content_fingerprints(&self) -> &[String] {
        &self.content_fingerprints
    }

    /// Stream all admitted fields/rows to the explicit ingestion boundary.
    /// This remains validated-copy ingress. Publication must also carry the
    /// preparation binding so collection membership is checked at commit.
    /// This ordinary Arrow export retains buffer owners but cannot attach an
    /// independent metadata owner to a bufferless `RecordBatch`. For native
    /// ingestion, use `complete_ingest_stream` with the writer feature enabled.
    /// # Errors
    /// Rejects a different resource owner or a source admitted for selected
    /// chunks only; data, row-budget, cancellation and generation errors remain
    /// errors in the resulting reader, never a successful truncated source.
    pub fn complete_stream(
        self: &Arc<Self>,
        max_rows: Option<usize>,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<FlatLocalColumnarStreamSource> {
        let (mut source, reader) = self.complete_source(max_rows, memory, cancellation, false)?;
        // Explicit public Arrow export; native ingest uses the owned path below.
        source.reader = Box::new(ArrowReader(Box::new(reader)));
        Ok(source)
    }

    /// Stream complete input to the native writer while retaining schema and
    /// structural owners with each queued batch, including bufferless arrays.
    /// The privately retained Parquet producer permits the existing per-buffer
    /// transfer proof; other providers retain validated-copy admission.
    /// # Errors
    /// Returns the same source, resource, cancellation and row-budget errors as
    /// [`Self::complete_stream`].
    #[cfg(feature = "vortex-write")]
    pub fn complete_ingest_stream(
        self: &Arc<Self>,
        max_rows: Option<usize>,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<crate::universal_format_io::IngestColumnarStreamSource> {
        self.complete_ingest_stream_with_parallelism(max_rows, 1, memory, cancellation)
    }

    /// Stream complete input with ordered Parquet source tasks on the existing
    /// shared ingest runtime. The caller's CPU and memory ceilings also cover
    /// conversion and writing; this does not allocate an independent grant.
    /// Other formats retain their complete sequential provider continuation.
    /// # Errors
    /// Returns complete-source admission errors and rejects zero parallelism or
    /// an indivisible source workspace that does not fit the shared pool.
    #[cfg(feature = "vortex-write")]
    pub fn complete_ingest_stream_with_parallelism(
        self: &Arc<Self>,
        max_rows: Option<usize>,
        requested_max_parallelism: usize,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
    ) -> Result<crate::universal_format_io::IngestColumnarStreamSource> {
        if requested_max_parallelism == 0 {
            return Err(failed("max_parallelism must be positive"));
        }
        let (source, reader) = self.complete_source(max_rows, memory, cancellation, true)?;
        if self
            .other
            .as_ref()
            .is_some_and(|source| source.format == "parquet")
        {
            parquet::ingest(source, reader, requested_max_parallelism)
        } else {
            Ok(
                crate::universal_format_io::IngestColumnarStreamSource::from_owned(
                    source,
                    Box::new(reader),
                    memory,
                ),
            )
        }
    }

    fn complete_source(
        self: &Arc<Self>,
        max_rows: Option<usize>,
        memory: &LiveMemoryPool,
        cancellation: &CancellationToken,
        native_ingest: bool,
    ) -> Result<(FlatLocalColumnarStreamSource, CompleteReader)> {
        cancellation.check()?;
        if !memory.owns(&self.metadata_credit) || self.other.is_none() {
            return Err(failed(
                "complete stream requires its own full-source admission and memory owner",
            ));
        }
        self.validate_generation_controlled(cancellation)?;
        let format = self.other.as_ref().expect("complete source checked");
        let row_count_hint = format.row_count_hint(cancellation)?;
        if max_rows
            .zip(row_count_hint)
            .is_some_and(|(limit, rows)| rows > limit)
        {
            return Err(failed("complete source exceeds its declared row budget"));
        }
        let schema = self.schema();
        // Vec growth can retain up to twice the selected region count. Charge
        // that actual index shape, not a full decoder workspace per row group.
        let groups_bytes = (self.row_groups_total as u64)
            .checked_mul(2 * std::mem::size_of::<super::SourceGroup>() as u64)
            .ok_or_else(|| failed("complete source group capacity overflow"))?;
        let metadata = memory.reserve(
            (schema.fields().len() as u64)
                .checked_mul(8192)
                .and_then(|bytes| bytes.checked_add(groups_bytes))
                .and_then(|bytes| bytes.checked_add(8192))
                .ok_or_else(|| failed("complete source metadata capacity overflow"))?,
        )?;
        let header = schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        let prepared = Arc::new(PreparedColumnarSource {
            source: Arc::clone(self),
            projection: (0..header.len()).collect(),
            row_groups: self.prepare_row_groups(None, &[], cancellation)?,
            max_rows: None,
            metadata,
        });
        let owned_parquet = native_ingest && format.format == "parquet";
        let stream_plan = complete_stream_plan(format, row_count_hint, owned_parquet);
        let batch_rows = stream_plan.stream_batch_size;
        let reader = CompleteReader {
            prepared,
            reader: Some(if owned_parquet {
                // The sealed producer retains each returned batch's owners.
                generic::Reader::for_owned_parquet(batch_rows)
            } else {
                generic::Reader::new()
            }),
            schema: Arc::clone(&schema),
            metrics: Arc::new(LocalReadMetrics::default()),
            memory: memory.clone(),
            cancellation: cancellation.clone(),
            rows: 0,
            max_rows,
        };
        let mut source = flat_columnar_stream_source_from_reader(
            schema.as_ref(),
            header.clone(),
            header.clone(),
            header,
            row_count_hint,
            &stream_plan,
            Box::new(SchemaReader(Arc::clone(&schema))),
        );
        source.source_identities = self
            .inventory()
            .map(|(_, identity)| Arc::clone(identity))
            .collect();
        if let Some(identity) = self.collection.manifest_identity() {
            source.source_identities.push(identity);
        }
        let discovery = self
            .other
            .as_ref()
            .map_or(0, |source| source.schema_discovery_read_bytes);
        let shared_policy = format!(
            "complete_local_dataset_incremental_record_batch_stream_batch_size_{batch_rows};schema_discovery_read_bytes={discovery};no_partial_source_reuse"
        );
        source.source_stream_policy = if matches!(format.format, "csv" | "tsv" | "json" | "jsonl") {
            source.source_stream_unit_hint_kind = "width_bounded_text_record_batches".into();
            // Preserve text-source evidence; the generic constructor's binary
            // adapter annotation does not describe these typed text builders.
            source.source_dictionary_preservation_status =
                "text_typed_builders_no_source_dictionary".into();
            format!(
                "incremental_{}_schema_discovery_replay;record_batch_stream_batch_size_8192;single_record_and_credited_batch;{shared_policy}",
                format.format,
            )
        } else {
            shared_policy
        };
        source.ingest_executor_kind = "sequential_generation_held_local_dataset".into();
        Ok((source, reader))
    }
}

fn complete_stream_plan(
    source: &generic::Source,
    row_count_hint: Option<usize>,
    owned_parquet: bool,
) -> FlatColumnarStreamSourcePlan {
    // A native bulk writer retains one layout subtree per source batch.
    // Reuse its established product ceiling; the shared decoder still selects
    // actual batches from current headroom and observed costs. Ordinary queries
    // and public Arrow export retain their existing small ceiling.
    FlatColumnarStreamSourcePlan::product_batches(
        if owned_parquet {
            row_count_hint
                .unwrap_or(crate::universal_format_io::PRODUCT_COLUMNAR_STREAM_RECORD_BATCH_ROWS)
        } else {
            super::batch_work::MAX_ROWS
        },
        None,
        "complete_local_dataset_batches_unknown_before_read",
        source
            .preparation_dictionary_status
            .unwrap_or(match source.format {
                "csv" | "tsv" | "json" | "jsonl" => "text_typed_builders_no_source_dictionary",
                _ => "provider_dictionary_layout_preserved_when_admitted",
            }),
    )
}

struct CompleteReader {
    prepared: Arc<PreparedColumnarSource>,
    reader: Option<generic::Reader>,
    schema: SchemaRef,
    metrics: Arc<LocalReadMetrics>,
    memory: LiveMemoryPool,
    cancellation: CancellationToken,
    rows: usize,
    max_rows: Option<usize>,
}

impl Iterator for CompleteReader {
    type Item = BatchResult;
    fn next(&mut self) -> Option<Self::Item> {
        let reader = self.reader.as_mut()?;
        let result = reader
            .next_preparation(
                &self.prepared,
                &self.metrics,
                &self.memory,
                &self.cancellation,
            )
            .and_then(|item| {
                if let Some((batch, _)) = item {
                    self.rows = self
                        .rows
                        .checked_add(batch.num_rows())
                        .ok_or_else(|| failed("complete source row count overflow"))?;
                    if self.max_rows.is_some_and(|max| self.rows > max) {
                        return Err(failed("complete source exceeds its declared row budget"));
                    }
                    Ok(Some(batch))
                } else {
                    Ok(None)
                }
            });
        match result {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => {
                self.reader.take();
                None
            }
            Err(error) => {
                self.reader.take();
                Some(Err(ArrowError::ExternalError(Box::new(error))))
            }
        }
    }
}

impl OwnedBatchReader for CompleteReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
