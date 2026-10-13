//! Native input transfer between separately admitted memory owners.
//!
//! Same-owner input stays shared. Admitted foreign physical trees retain their
//! encodings while copying buffers and metadata under the receiving grant.
//! Other physical providers use the existing, preclassified bounded materializer.
//! No Arrow export, Python rows, source execution grant or new pool is involved.

use super::{MemoryBatchSourceBuilder, ResidentMemorySource};
use crate::resident_session::{OwnedVortexResultBatch, ResidentVortexSession};
use shardloom_core::Result;
use shardloom_exec::compute_pool::CancellationToken;

#[cfg(test)]
#[path = "resident_memory_transfer_tests.rs"]
mod tests;

impl ResidentMemorySource {
    /// Use immutable native input in another admitted session. Sources already
    /// owned by that session remain shared; admitted foreign physical encodings
    /// and generated ranges retain their compact representation.
    /// The returned input has only the receiving session's allocation ownership.
    ///
    /// # Errors
    /// Rejects cancellation, unsupported native payloads or memory denial before
    /// publishing a source. Failure releases every partial receiving allocation.
    pub fn in_session(
        &self,
        session: &ResidentVortexSession,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        cancellation.check()?;
        if self.belongs_to_session(session) {
            return Ok(self.clone());
        }
        if let Some(range) = &self.0.generated_range {
            return range.in_session(&self.0, session);
        }
        let mut builder = MemoryBatchSourceBuilder::new(session, cancellation.clone())?;
        builder.push_native_copy(&self.0.array)?;
        let mut copied = builder.finish()?;
        std::sync::Arc::get_mut(&mut copied.0)
            .ok_or_else(|| {
                super::memory_error("new native transfer owner was unexpectedly shared")
            })?
            .bounds = self.0.bounds;
        Ok(copied)
    }

    /// Retain a completed result in its own session, or copy native batches into
    /// a different receiving session without a compatibility representation.
    ///
    /// # Errors
    /// Rejects inconsistent results, cancellation and receiving memory denial.
    pub fn from_completed_result_in_session(
        session: &ResidentVortexSession,
        result: &OwnedVortexResultBatch,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        cancellation.check()?;
        if session.same_owner(&result.retained_session()) {
            return Self::from_completed_result(result, cancellation);
        }
        result.validate_schema_and_rows()?;
        let mut builder = MemoryBatchSourceBuilder::new(session, cancellation.clone())?;
        if result.arrays().is_empty() {
            let empty = session.with_native_execution_context(cancellation, |context| {
                crate::local_primitives::native_payload::empty_record(result.dtype(), context)
            })?;
            builder.push_native_copy(&empty)?;
        } else {
            builder.push_native_copies(result.arrays())?;
        }
        builder.finish()
    }
}
