//! Reader provenance for the existing local ingest path. A public flat source
//! has a replaceable reader, so metadata or a flag on it cannot prove origin.

use std::{ops::Deref, path::Path};

use shardloom_core::Result;
use shardloom_exec::compute_pool::CancellationToken;

use super::{EmbeddedDerivedColumnMode, FlatLocalColumnarStreamSource};
use crate::vortex_ingest::arrow_ownership::ArrowOwnershipPolicy;

/// A local ingest source whose reader cannot be replaced after admission.
/// Existing flat/custom sources retain validated-copy admission. Only the
/// constructor that opens a Parquet file admits per-buffer ownership checks.
/// Read-only access preserves the existing source metadata inspection surface.
pub struct IngestColumnarStreamSource {
    source: FlatLocalColumnarStreamSource,
    ownership: ArrowOwnershipPolicy,
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
        })
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
        self.source = super::attach_embedded_derived_columns_to_stream_source(self.source, mode);
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
        self.source = super::with_capillary_prefetch_columnar_stream_source_controlled(
            self.source,
            requested_max_parallelism,
            cancellation,
        )?;
        Ok(self)
    }

    pub(crate) fn into_parts(self) -> (FlatLocalColumnarStreamSource, ArrowOwnershipPolicy) {
        (self.source, self.ownership)
    }
}

impl From<FlatLocalColumnarStreamSource> for IngestColumnarStreamSource {
    fn from(source: FlatLocalColumnarStreamSource) -> Self {
        Self {
            source,
            ownership: ArrowOwnershipPolicy::ValidatedCopy,
        }
    }
}

// There is deliberately no DerefMut or caller-supplied reader transformation.
impl Deref for IngestColumnarStreamSource {
    type Target = FlatLocalColumnarStreamSource;

    fn deref(&self) -> &Self::Target {
        &self.source
    }
}
