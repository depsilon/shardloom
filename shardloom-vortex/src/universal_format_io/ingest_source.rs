//! Reader provenance for the existing local ingest path. A public flat source
//! has a replaceable reader, so metadata or a flag on it cannot prove origin.

use std::{
    ops::Deref,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use arrow_schema::{ArrowError, SchemaRef};
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::compute_pool::CancellationToken;

use super::owned_batch::{BatchResult, LegacyReader, OwnedBatch, OwnedBatchReader, SchemaReader};
use super::{EmbeddedDerivedColumnMode, FlatLocalColumnarStreamSource};
use crate::ingest_source::{SourceFactory, SourceReader};
use crate::vortex_ingest::arrow_ownership::ArrowOwnershipPolicy;

/// A local ingest source whose reader cannot be replaced after admission.
/// Existing flat/custom sources retain validated-copy admission. Only the
/// private constructors retaining Parquet readers admit per-buffer ownership checks.
/// Read-only access preserves the existing source metadata inspection surface.
pub struct IngestColumnarStreamSource {
    source: FlatLocalColumnarStreamSource,
    ownership: ArrowOwnershipPolicy,
    reader: Option<Box<dyn OwnedBatchReader>>,
}

impl IngestColumnarStreamSource {
    /// Open the existing native Parquet reader, preserving generation checks,
    /// batch policy, resource declarations and cooperative cancellation.
    ///
    /// # Errors
    /// Returns the existing Parquet source, resource and cancellation errors.
    pub fn parquet(
        path: &Path,
        max_rows: usize,
        requested_max_parallelism: usize,
        batch_budget_bytes: Option<u64>,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        let source = super::stream_flat_parquet_columnar_source_with_batch_budget_controlled(
            path,
            max_rows,
            requested_max_parallelism,
            batch_budget_bytes,
            cancellation,
        )?;
        Ok(Self {
            source,
            ownership: ArrowOwnershipPolicy::ControlledParquet,
            reader: None,
        })
    }

    pub(crate) fn from_owned(
        mut source: FlatLocalColumnarStreamSource,
        reader: Box<dyn OwnedBatchReader>,
    ) -> Self {
        source.reader = Box::new(SchemaReader(reader.schema()));
        Self {
            source,
            ownership: ArrowOwnershipPolicy::ValidatedCopy,
            reader: Some(reader),
        }
    }

    /// Only complete source admission can call this with its private Parquet
    /// reader and independent batch credits. Public source metadata never grants
    /// transfer permission; each allocation still passes the existing proof.
    #[cfg(all(feature = "vortex-local-primitives", unix))]
    pub(super) fn from_owned_parquet(
        source: FlatLocalColumnarStreamSource,
        reader: Box<dyn OwnedBatchReader>,
    ) -> Self {
        let mut source = Self::from_owned(source, reader);
        source.ownership = ArrowOwnershipPolicy::ControlledParquet;
        source
    }

    /// Apply the existing full derived-column recipe without exposing the reader.
    #[must_use]
    pub fn with_embedded_derived_columns(self) -> Self {
        self.with_derived_columns(EmbeddedDerivedColumnMode::FullAdapter)
    }

    /// Apply the existing source-native derived-column recipe.
    #[must_use]
    pub fn with_source_native_embedded_derived_columns(self) -> Self {
        self.with_derived_columns(EmbeddedDerivedColumnMode::SourceNativeOnly)
    }

    /// Apply the existing lean source-native derived-column recipe.
    #[must_use]
    pub fn with_source_native_lean_runtime_embedded_derived_columns(self) -> Self {
        self.with_derived_columns(EmbeddedDerivedColumnMode::SourceNativeLeanRuntime)
    }

    fn with_derived_columns(mut self, mode: EmbeddedDerivedColumnMode) -> Self {
        if let Some(reader) = self.reader.take() {
            self.reader = Some(
                if let Some((schema, specs)) =
                    super::prepare_embedded_derived_columns(&mut self.source, mode)
                {
                    self.source.reader = Box::new(SchemaReader(Arc::clone(&schema)));
                    Box::new(DerivedReader {
                        schema,
                        reader,
                        specs,
                        timing: Arc::clone(&self.source.embedded_derived_build_micros),
                    })
                } else {
                    reader
                },
            );
        } else {
            self.source =
                super::attach_embedded_derived_columns_to_stream_source(self.source, mode);
        }
        self
    }

    /// Apply the existing bounded source prefetch and cancellation policy.
    ///
    /// # Errors
    /// Returns existing prefetch resource or cancellation errors.
    pub fn with_prefetch(
        mut self,
        requested_max_parallelism: usize,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        if let Some(reader) = self.reader.take() {
            cancellation.check()?;
            if requested_max_parallelism == 0 {
                return Err(ShardLoomError::new("max_parallelism must be positive"));
            }
            let reader: Box<dyn OwnedBatchReader> = Box::new(ControlledReader {
                schema: reader.schema(),
                reader: Some(reader),
                cancellation: cancellation.clone(),
            });
            let promote_serial = requested_max_parallelism > 1
                && self
                    .source
                    .ingest_runtime
                    .as_ref()
                    .is_some_and(|runtime| runtime.requested_parallelism() == 1);
            if !promote_serial
                && super::columnar_stream_source_already_has_capillary_executor(&self.source)
            {
                self.source.ingest_executor_requested_parallelism = self
                    .source
                    .ingest_executor_requested_parallelism
                    .max(1)
                    .min(requested_max_parallelism);
                self.reader = Some(reader);
            } else {
                let runtime = super::IngestRuntime::new(requested_max_parallelism)?;
                self.reader = Some(if runtime.parallelism() > 1 {
                    let schema = reader.schema();
                    let factory: SourceFactory<OwnedBatch> =
                        Box::new(
                            move || Ok(reader as Box<dyn Iterator<Item = BatchResult> + Send>),
                        );
                    Box::new(SourceReader::from_factories(
                        schema,
                        runtime.clone(),
                        vec![factory],
                        1,
                        cancellation,
                    ))
                } else {
                    reader
                });
                super::apply_shared_runtime_source_metadata(&mut self.source, runtime);
            }
            return Ok(self);
        }
        self.source = super::with_capillary_prefetch_columnar_stream_source_controlled(
            self.source,
            requested_max_parallelism,
            cancellation,
        )?;
        Ok(self)
    }

    pub(crate) fn into_parts(
        mut self,
    ) -> (
        FlatLocalColumnarStreamSource,
        ArrowOwnershipPolicy,
        Box<dyn OwnedBatchReader>,
    ) {
        let reader = self.reader.take().unwrap_or_else(|| {
            let schema = self.source.reader.schema();
            Box::new(LegacyReader(std::mem::replace(
                &mut self.source.reader,
                Box::new(SchemaReader(schema)),
            )))
        });
        (self.source, self.ownership, reader)
    }
}

impl From<FlatLocalColumnarStreamSource> for IngestColumnarStreamSource {
    fn from(source: FlatLocalColumnarStreamSource) -> Self {
        Self {
            source,
            ownership: ArrowOwnershipPolicy::ValidatedCopy,
            reader: None,
        }
    }
}

impl OwnedBatchReader for SourceReader<OwnedBatch> {
    fn schema(&self) -> SchemaRef {
        self.source_schema()
    }
}

struct ControlledReader {
    schema: SchemaRef,
    reader: Option<Box<dyn OwnedBatchReader>>,
    cancellation: CancellationToken,
}

impl Iterator for ControlledReader {
    type Item = BatchResult;
    fn next(&mut self) -> Option<Self::Item> {
        self.reader.as_ref()?;
        if let Err(error) = self.cancellation.check() {
            self.reader.take();
            return Some(Err(ArrowError::ExternalError(Box::new(error))));
        }
        let result = self.reader.as_mut()?.next();
        if !matches!(result, Some(Err(_)))
            && let Err(error) = self.cancellation.check()
        {
            self.reader.take();
            return Some(Err(ArrowError::ExternalError(Box::new(error))));
        }
        if result.as_ref().is_none_or(std::result::Result::is_err) {
            self.reader.take();
        }
        result
    }
}

impl OwnedBatchReader for ControlledReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

struct DerivedReader {
    schema: SchemaRef,
    reader: Box<dyn OwnedBatchReader>,
    specs: Vec<super::EmbeddedDerivedColumnSpec>,
    timing: Arc<AtomicU64>,
}

impl Iterator for DerivedReader {
    type Item = BatchResult;
    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next().map(|batch| {
            batch.and_then(|batch| {
                let start = Instant::now();
                let result = batch.map(|batch| {
                    super::append_embedded_derived_columns_to_batch(
                        batch,
                        &self.schema,
                        &self.specs,
                    )
                });
                self.timing.fetch_add(
                    super::duration_micros_u64(start.elapsed()),
                    Ordering::Relaxed,
                );
                result.map_err(|error| ArrowError::ExternalError(Box::new(error)))
            })
        })
    }
}

impl OwnedBatchReader for DerivedReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

// There is deliberately no DerefMut or caller-supplied reader transformation.
impl Deref for IngestColumnarStreamSource {
    type Target = FlatLocalColumnarStreamSource;

    fn deref(&self) -> &Self::Target {
        &self.source
    }
}
