//! Direct columnar intake using the existing Arrow-to-Vortex ownership boundary.

use super::{
    MemorySourceBounds, MemorySourceOwner, ResidentMemorySource, memory_error, native_error,
};
use crate::resident_session::{NativeExecutionContext, ResidentVortexSession};
use crate::vortex_ingest::{arrow_ownership, nested};
use arrow_array::RecordBatch;
use shardloom_core::Result;
use shardloom_exec::compute_pool::CancellationToken;
use std::sync::Arc;
use vortex::arrow::ArrowSessionExt as _;

impl ResidentMemorySource {
    /// Copy a caller-owned columnar batch directly into admitted native buffers.
    /// No Python rows, JSON cells, intermediate file or query engine participates.
    /// Every referenced Arrow buffer is copied because its visible capacity does
    /// not establish its producer's complete retained allocation or immutability.
    /// The native `DType` retains field names, nullability and admitted logical types;
    /// arbitrary Arrow application metadata is not retained. Extension fields fail.
    /// # Errors
    /// Rejects unsupported schemas, malformed arrays, nonfinite logical values,
    /// cancellation, overflow or shared-memory denial before publishing the source.
    pub fn from_arrow_record_batch(
        session: &ResidentVortexSession,
        batch: RecordBatch,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        session.with_native_execution_context(cancellation, |context| {
            Self::from_arrow_in_context(session, batch, context, false)
        })
    }

    /// Reuse the source consumer's already admitted execution context. A private
    /// streaming batch releases its credits before another producer demand.
    pub(crate) fn from_arrow_in_context(
        session: &ResidentVortexSession,
        batch: RecordBatch,
        context: &NativeExecutionContext<'_>,
        is_batch: bool,
    ) -> Result<Self> {
        session.validate_execution_context(context)?;
        let schema = batch.schema();
        let mut metadata = crate::native_payload_schema::reserve_names(
            context.memory(),
            schema.fields().len(),
            |index| schema.field(index).name().as_str(),
        )?;
        let mut metadata_bytes = metadata.bytes();
        for field in schema.fields() {
            context.check_cancelled()?;
            if field.metadata().contains_key("ARROW:extension:name") {
                return Err(memory_error(
                    "direct columnar intake does not admit Arrow extension fields",
                ));
            }
            metadata_bytes = metadata_bytes
                .checked_add(crate::native_payload_schema::arrow_metadata_bytes(
                    field.data_type(),
                )?)
                .ok_or_else(|| memory_error("columnar schema metadata overflow"))?;
        }
        metadata.resize(metadata_bytes)?;
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            context.check_cancelled()?;
            column.to_data().validate_full().map_err(native_error)?;
            nested::validate_values(field.name(), column.as_ref())?;
        }
        let copied = arrow_ownership::batch_copy_buffer_bytes(&batch)?;
        let metadata = Arc::new(metadata);
        let allocator = crate::owned_buffers::with_shared_credit(
            session.native_allocator(),
            Arc::clone(&metadata),
        );
        let retained = arrow_ownership::copy_batch(batch, allocator.as_ref())?;
        context.check_cancelled()?;
        let array = context
            .native_session()
            .arrow()
            .from_arrow_record_batch(retained, schema.as_ref())
            .map_err(native_error)?;
        let input_logical_bytes = usize::try_from(array.nbytes()).map_err(native_error)?;
        context.check_cancelled()?;
        Ok(Self(Arc::new(MemorySourceOwner {
            bounds: MemorySourceBounds {
                max_input_rows: array.len().max(1),
                max_input_bytes: input_logical_bytes.max(1),
                max_output_rows: array.len().max(1),
                ..MemorySourceBounds::default()
            },
            array,
            session: session.clone(),
            input_logical_bytes,
            intake_payload_bytes_copied: 0,
            intake_columnar_buffer_bytes_copied: copied,
            transfer_report: super::NativeTransferReport::default(),
            metadata: Some(metadata),
            is_batch,
            generated_range: None,
        })))
    }
}

#[cfg(test)]
#[path = "resident_memory_arrow_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "resident_memory_arrow_encoded_tests.rs"]
mod encoded_tests;

#[cfg(all(test, feature = "native-encoded-exchange-experiment"))]
#[path = "resident_memory_exchange_fixture_tests.rs"]
mod exchange_fixture_tests;
