//! Explicit full-source preparation reuses the same held format readers and
//! schema alignment. It has no query demand or partial-source cache semantics.

use super::{
    LocalColumnarSource, LocalReadMetrics, PreparedColumnarSource, Result, failed, generic,
};
use crate::universal_format_io::{
    FlatColumnarStreamSourcePlan, FlatLocalColumnarStreamSource,
    flat_columnar_stream_source_from_reader,
};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use shardloom_exec::{compute_pool::CancellationToken, live_memory::LiveMemoryPool};
use std::{path::Path, sync::Arc};

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
        generic::admit(path, format, fingerprint, memory, cancellation, None)
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
        cancellation.check()?;
        if !memory.owns(&self.metadata_credit) || self.other.is_none() {
            return Err(failed(
                "complete stream requires its own full-source admission and memory owner",
            ));
        }
        self.validate_generation_controlled(cancellation)?;
        let schema = self.schema();
        let metadata = memory.reserve(schema.fields().len() as u64 * 8192 + 8192)?;
        let header = schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        let prepared = Arc::new(PreparedColumnarSource {
            source: Arc::clone(self),
            projection: (0..header.len()).collect(),
            row_groups: Vec::new(),
            max_rows: None,
            metadata,
        });
        let reader = CompleteReader {
            prepared,
            reader: Some(generic::Reader::new()),
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
            None,
            &FlatColumnarStreamSourcePlan::product_batches(
                8192,
                None,
                "complete_local_dataset_batches_unknown_before_read",
                "provider_dictionary_layout_preserved_when_admitted",
            ),
            Box::new(reader),
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
        source.source_stream_policy = format!(
            "complete_local_dataset_incremental_record_batch_stream_batch_size_8192;schema_discovery_read_bytes={discovery};no_partial_source_reuse"
        );
        source.ingest_executor_kind = "sequential_generation_held_local_dataset".into();
        Ok(source)
    }
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
    type Item = std::result::Result<RecordBatch, ArrowError>;
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

impl RecordBatchReader for CompleteReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
