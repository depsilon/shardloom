//! Demand-driven Arrow delivery from the shared native batch consumer.

use super::{
    ExecutedVortexRelational, PreparedVortexRelational, ReservedVec, VortexRelationalPreparation,
    failed,
};
use crate::resident_memory_source::ResidentMemorySource;
use crate::resident_session::NativeArrowExportReport;
use arrow_array::RecordBatch;
use shardloom_core::Result;
use shardloom_exec::compute_pool::CancellationToken;
use std::sync::Arc;

/// Final query evidence plus explicit Arrow representation accounting. This
/// value is returned only after source validation and every consumer succeeded.
pub struct ExecutedVortexArrowRelational {
    pub execution: ExecutedVortexRelational,
    pub export: NativeArrowExportReport,
}

impl VortexRelationalPreparation<'_> {
    /// Declare a columnar stream's exact schema without requesting any batch.
    /// The typed empty native schema uses the same session as eventual input.
    /// # Errors
    /// Rejects unsupported schemas, foreign/duplicate declarations, cancellation
    /// and shared-memory denial before a producer is attached or consumed.
    pub fn register_arrow_batch_source(
        &mut self,
        uri: shardloom_core::DatasetUri,
        schema: arrow_schema::SchemaRef,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        self.register_batch_source(uri, |session| {
            session.with_native_execution_context(cancellation, |context| {
                ResidentMemorySource::from_arrow_in_context(
                    session,
                    RecordBatch::new_empty(schema),
                    context,
                    true,
                )
            })
        })
    }
}

impl PreparedVortexRelational {
    /// Convert and deliver one native execution batch at a time. No full-result
    /// collection, replay or row serialization occurs. The consumer controls
    /// demand and may retain returned batches under the same shared allocation.
    /// Delivered batches remain provisional until this method returns success.
    /// # Errors
    /// Propagates cancellation, source changes, consumer failure, unsupported
    /// Arrow representation and resource refusal; no completion is certified.
    pub fn for_each_arrow_batch(
        &self,
        cancellation: &CancellationToken,
        consume: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<ExecutedVortexArrowRelational> {
        self.for_each_arrow_batch_with_input(cancellation, None, consume)
    }

    pub(super) fn for_each_arrow_batch_with_input(
        &self,
        cancellation: &CancellationToken,
        input: Option<super::batch_input::Provider<'_>>,
        mut consume: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<ExecutedVortexArrowRelational> {
        let mut export = NativeArrowExportReport::default();
        let execution = self.for_each_batch_with_input(
            cancellation,
            super::BATCH_ROWS,
            input,
            |array, context| {
                let mut arrays = ReservedVec::new(context.memory())?;
                arrays.push(array)?;
                let (arrays, ownership) = arrays.into_parts();
                let result = Arc::new(self.session.own_completed_arrays(arrays, ownership)?);
                let mut stream = result.arrow_batches(cancellation.clone())?;
                let batch = stream
                    .next_batch_in_context(context)?
                    .ok_or_else(|| failed("native batch produced no Arrow representation"))?;
                if stream.next_batch_in_context(context)?.is_some() {
                    return Err(failed(
                        "one native batch produced multiple Arrow representations",
                    ));
                }
                export.append(stream.report())?;
                consume(batch)
            },
        )?;
        if export.output_rows != execution.output_rows
            || export.output_batches != execution.output_batches
        {
            return Err(failed(
                "Arrow delivery changed native execution cardinality",
            ));
        }
        export.complete = true;
        Ok(ExecutedVortexArrowRelational { execution, export })
    }
}

#[cfg(test)]
#[path = "local_primitive_relational_arrow_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "local_primitive_relational_arrow_input_tests.rs"]
mod input_tests;
