//! Internal Arrow handoffs retain non-buffer owners through native conversion.
//!
//! This is a lifetime envelope, not an allocation estimator or permission to
//! adopt caller buffers. Producers must admit their allocations before building
//! a batch. The ordinary public Arrow reader remains a compatibility boundary.

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use std::{ops::Deref, sync::Arc};

pub(crate) type Owner = Option<Arc<dyn Send + Sync>>;
pub(crate) type BatchResult = std::result::Result<OwnedBatch, ArrowError>;

pub(crate) struct OwnedBatch {
    // Release arrays/schema before their independent structural reservations.
    batch: RecordBatch,
    owner: Owner,
}

impl std::fmt::Debug for OwnedBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedBatch")
            .field("batch", &self.batch)
            .field("has_owner", &self.owner.is_some())
            .finish()
    }
}

impl OwnedBatch {
    pub(crate) fn with_owner(batch: RecordBatch, owner: Arc<dyn Send + Sync>) -> Self {
        Self {
            batch,
            owner: Some(owner),
        }
    }

    pub(crate) fn retain(mut self, owner: Arc<dyn Send + Sync>) -> Self {
        self.owner = Some(match self.owner.take() {
            Some(previous) => Arc::new((previous, owner)),
            None => owner,
        });
        self
    }

    pub(crate) fn into_parts(self) -> (RecordBatch, Owner) {
        (self.batch, self.owner)
    }

    /// The caller admits any new structures/payload before this transformation.
    /// Retain the input owner on success; errors drop payload before its owner.
    pub(crate) fn map<E>(
        mut self,
        transform: impl FnOnce(&RecordBatch) -> std::result::Result<RecordBatch, E>,
    ) -> std::result::Result<Self, E> {
        self.batch = transform(&self.batch)?;
        Ok(self)
    }
}

impl From<RecordBatch> for OwnedBatch {
    fn from(batch: RecordBatch) -> Self {
        Self { batch, owner: None }
    }
}

impl Deref for OwnedBatch {
    type Target = RecordBatch;

    fn deref(&self) -> &Self::Target {
        &self.batch
    }
}

pub(crate) trait OwnedBatchReader: Iterator<Item = BatchResult> + Send {
    fn schema(&self) -> SchemaRef;
}

#[cfg(feature = "vortex-write")]
pub(crate) struct LegacyReader(pub(crate) Box<dyn RecordBatchReader + Send>);

#[cfg(feature = "vortex-write")]
impl Iterator for LegacyReader {
    type Item = BatchResult;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|batch| batch.map(OwnedBatch::from))
    }
}

#[cfg(feature = "vortex-write")]
impl OwnedBatchReader for LegacyReader {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }
}

/// Explicit ordinary Arrow export. Buffer owners survive as usual; callers
/// retaining detached metadata do not receive an internal structural guarantee.
pub(crate) struct ArrowReader(pub(crate) Box<dyn OwnedBatchReader>);

impl Iterator for ArrowReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|batch| batch.map(|batch| batch.into_parts().0))
    }
}

impl RecordBatchReader for ArrowReader {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }
}

/// Metadata-only facade inside the private ingest wrapper. It is never used to
/// execute the owned source and cannot silently masquerade as an empty source.
pub(crate) struct SchemaReader(pub(crate) SchemaRef);

impl Iterator for SchemaReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(Err(ArrowError::InvalidArgumentError(
            "owned ingest reader must be consumed through its ingest source".into(),
        )))
    }
}

impl RecordBatchReader for SchemaReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.0)
    }
}

#[cfg(all(test, feature = "vortex-write"))]
#[path = "owned_batch_tests.rs"]
mod tests;
