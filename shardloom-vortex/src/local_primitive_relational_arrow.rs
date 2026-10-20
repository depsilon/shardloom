//! Demand-driven Arrow delivery from the shared native batch consumer.

use super::{
    ExecutedVortexRelational, PreparedVortexRelational, VortexRelationalPreparation, failed,
};
use crate::resident_memory_source::ResidentMemorySource;
use crate::resident_session::{NativeArrowBatchDelivery, NativeArrowExportReport};
use arrow_array::RecordBatch;
use shardloom_core::Result;
use shardloom_exec::compute_pool::CancellationToken;
#[cfg(test)]
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
        consume: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<ExecutedVortexArrowRelational> {
        let mut delivery = NativeArrowBatchDelivery::new(&self.session, cancellation, consume);
        let execution = self.for_each_batch_with_input(
            cancellation,
            super::BATCH_ROWS,
            super::FileScanBatching::Output,
            input,
            |array, context| delivery.push(array, context),
        )?;
        finish_delivery(execution, delivery)
    }

    /// Initialize the finite output-window policy inside native execution.
    /// Ineligible requests retain ordinary acknowledgment at the consumer.
    /// # Errors
    /// Propagates admission, reservation, producer and consumer failures.
    pub fn for_each_arrow_batch_with_output_window(
        &self,
        cancellation: &CancellationToken,
        requested: impl FnOnce() -> bool,
        initialize: impl FnOnce(Option<super::OutputCreditWindow>) -> Result<()>,
        consume: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<ExecutedVortexArrowRelational> {
        self.for_each_arrow_batch_with_input_and_output_window(
            cancellation,
            None,
            requested,
            initialize,
            consume,
        )
    }

    pub(super) fn for_each_arrow_batch_with_input_and_output_window(
        &self,
        cancellation: &CancellationToken,
        input: Option<super::batch_input::Provider<'_>>,
        requested: impl FnOnce() -> bool,
        initialize: impl FnOnce(Option<super::OutputCreditWindow>) -> Result<()>,
        consume: impl FnMut(RecordBatch) -> Result<()>,
    ) -> Result<ExecutedVortexArrowRelational> {
        let mut delivery = NativeArrowBatchDelivery::new(&self.session, cancellation, consume);
        let execution = self.for_each_batch_with_input_and_initialize(
            cancellation,
            super::BATCH_ROWS,
            super::FileScanBatching::Output,
            input,
            || initialize(self.output_credit_window(requested())?),
            |array, context| delivery.push(array, context),
        )?;
        finish_delivery(execution, delivery)
    }
}

fn finish_delivery(
    execution: ExecutedVortexRelational,
    delivery: NativeArrowBatchDelivery<'_, impl FnMut(RecordBatch) -> Result<()>>,
) -> Result<ExecutedVortexArrowRelational> {
    let export = delivery.finish(execution.output_rows)?;
    if export.output_batches != execution.output_batches {
        return Err(failed(
            "Arrow delivery changed native execution cardinality",
        ));
    }
    Ok(ExecutedVortexArrowRelational { execution, export })
}

#[cfg(test)]
#[path = "local_primitive_relational_arrow_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "local_primitive_relational_arrow_input_tests.rs"]
mod input_tests;
