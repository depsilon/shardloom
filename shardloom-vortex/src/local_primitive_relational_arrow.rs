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
        let sampling = self.sampling_failure_guard();
        let mut delivery = NativeArrowBatchDelivery::new(&self.session, cancellation, consume);
        let execution = self.for_each_batch_with_input(
            cancellation,
            super::BATCH_ROWS,
            super::FileScanBatching::Output,
            input,
            |array, context| delivery.push(array, context),
        )?;
        let result = finish_delivery(execution, delivery)?;
        sampling.complete();
        Ok(result)
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
        let sampling = self.sampling_failure_guard();
        let mut delivery = NativeArrowBatchDelivery::new(&self.session, cancellation, consume);
        let execution = self.for_each_batch_with_input_and_initialize(
            cancellation,
            super::BATCH_ROWS,
            super::FileScanBatching::Output,
            input,
            || initialize(self.output_credit_window(requested())?),
            |array, context| delivery.push(array, context),
        )?;
        let result = finish_delivery(execution, delivery)?;
        sampling.complete();
        Ok(result)
    }

    /// Convert and serialize one bounded binary Arrow IPC batch at a time
    /// without row/JSON cell materialization.
    ///
    /// # Errors
    /// Propagates cancellation, invalid bounds, source changes, consumer
    /// failures, unsupported Arrow representations, and shared-memory refusal.
    pub fn for_each_arrow_ipc_batch(
        &self,
        cancellation: &CancellationToken,
        batch_rows: usize,
        max_batch_bytes: usize,
        consume: impl FnMut(crate::resident_session::SerializedArrowIpcBatch) -> Result<()>,
    ) -> Result<ExecutedVortexRelational> {
        self.for_each_arrow_ipc_batch_with_input(
            cancellation,
            batch_rows,
            max_batch_bytes,
            None,
            consume,
        )
    }

    pub(super) fn for_each_arrow_ipc_batch_with_input(
        &self,
        cancellation: &CancellationToken,
        batch_rows: usize,
        max_batch_bytes: usize,
        input: Option<super::batch_input::Provider<'_>>,
        mut consume: impl FnMut(crate::resident_session::SerializedArrowIpcBatch) -> Result<()>,
    ) -> Result<ExecutedVortexRelational> {
        let sampling = self.sampling_failure_guard();
        if batch_rows == 0
            || batch_rows > super::BATCH_ROWS
            || max_batch_bytes == 0
            || max_batch_bytes > 8 * 1024 * 1024
        {
            return Err(failed(
                "Arrow IPC batches require 1..=2,048 rows and 1..=8 MiB",
            ));
        }
        let mut execution = self.for_each_batch_with_input(
            cancellation,
            batch_rows,
            super::FileScanBatching::Output,
            input,
            |array, context| {
                let batch = crate::resident_session::serialize_array_arrow_ipc(
                    &array,
                    max_batch_bytes,
                    context,
                )?;
                consume(batch)
            },
        )?;
        certify_arrow_ipc_delivery(
            &mut execution.native_io_certificate,
            execution.output_rows,
            batch_rows,
        )?;
        execution.runtime = self.snapshot();
        sampling.complete();
        Ok(execution)
    }
}

fn certify_arrow_ipc_delivery(
    certificate: &mut shardloom_core::NativeIoCertificate,
    rows: u64,
    batch_rows: usize,
) -> Result<()> {
    use shardloom_core::{
        NativeIoAdapterFidelityReport, NativeIoMaterializationBoundaryReport,
        NativeIoRepresentationTransition, NativeIoSinkRequirementReport, RepresentationState,
    };
    if !certificate.is_certified() {
        return Err(failed(
            "Arrow IPC delivery requires certified native execution",
        ));
    }
    let from_state = certificate
        .representation_transitions
        .last()
        .map_or(RepresentationState::DecodedColumnar, |transition| {
            transition.to_state
        });
    certificate
        .representation_transitions
        .push(NativeIoRepresentationTransition::new(
            from_state,
            RepresentationState::DecodedColumnar,
            false,
        ));
    certificate.sink_requirement_report = NativeIoSinkRequirementReport {
        target_format: "arrow_ipc_stream".into(),
        accepts_encoded: false,
        requires_decoded_columnar: true,
        requires_rows: false,
        preserves_metadata: false,
        requires_ordering: true,
        requires_partitioning: false,
        requires_commit: false,
        supports_streaming: true,
        max_chunk_size: Some(batch_rows as u64),
        backpressure_policy: "synchronous_consumer_controls_next_batch".into(),
    };
    certificate.adapter_fidelity_report = NativeIoAdapterFidelityReport {
        adapter_id: "shardloom.resident_vortex.arrow_ipc_stream_sink.v1".into(),
        source_kind: "vortex".into(),
        sink_kind: "arrow_ipc".into(),
        metadata_preserved: false,
        statistics_preserved: false,
        encoded_representation_preserved: false,
        materialization_required: false,
        fidelity_loss: "Arrow IPC stream preserves logical Arrow types and nullability without row/JSON materialization; Vortex physical encodings and statistics are not persisted".into(),
        metadata_loss: "Vortex physical encodings, statistics and metadata not exported".into(),
        fallback_attempted: false,
    };
    certificate
        .materialization_boundaries
        .push(NativeIoMaterializationBoundaryReport {
        boundary_id: "resident_arrow_ipc_batch_sink".into(),
        from_state,
        to_state: RepresentationState::DecodedColumnar,
        required_by: "explicit_arrow_ipc_batch_consumption".into(),
        reason:
            "explicit columnar Arrow IPC framing at worker boundary; no row/JSON cell conversion"
                .into(),
        bytes_decoded: 0,
        rows_materialized: rows,
        fidelity_loss:
            "physical Vortex encodings, statistics and metadata are not part of Arrow IPC".into(),
        fallback_attempted: false,
    });
    certificate.side_effects.data_decoded |= rows > 0;
    certificate
        .source_pushdown_report
        .proof_basis
        .push_str(";complete_native_batches_to_incremental_arrow_ipc;no_query_replay=true");
    Ok(())
}

fn finish_delivery(
    execution: ExecutedVortexRelational,
    delivery: NativeArrowBatchDelivery<'_, impl FnMut(RecordBatch) -> Result<()>>,
) -> Result<ExecutedVortexArrowRelational> {
    #[cfg(test)]
    super::sample_preparation::BEFORE_ARROW_COMPLETION.with_borrow_mut(|slot| {
        if let Some(before) = slot.take() {
            before();
        }
    });
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
