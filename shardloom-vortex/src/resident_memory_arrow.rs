//! Direct columnar intake using the existing Arrow-to-Vortex ownership boundary.

use super::{
    MemorySourceBounds, MemorySourceOwner, ResidentMemorySource, memory_error, native_error,
};
use crate::resident_session::{NativeExecutionContext, ResidentVortexSession};
use crate::vortex_ingest::{arrow_ownership, nested};
use arrow_array::{RecordBatch, cast::AsArray as _};
use shardloom_core::Result;
use shardloom_exec::compute_pool::CancellationToken;
use std::sync::Arc;
use vortex::arrow::ArrowSessionExt as _;

#[derive(Clone, Copy)]
enum Origin {
    Caller,
    LocalFile,
}

impl ResidentMemorySource {
    /// Copy a caller-owned columnar batch directly into admitted native buffers.
    /// No Python rows, JSON cells, intermediate file or query engine participates.
    /// Every referenced Arrow buffer is copied because its visible capacity does
    /// not establish its producer's complete retained allocation or immutability.
    /// The native `DType` retains field names, nullability and admitted logical types;
    /// arbitrary Arrow application metadata is not retained. Admitted duration
    /// and dense embedding extension identity is preserved through native dtypes.
    /// # Errors
    /// Rejects unsupported schemas, malformed arrays, values outside the session's
    /// floating policy, cancellation, overflow or shared-memory denial before
    /// publishing the source.
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
        Self::from_arrow_with_ownership(
            session,
            batch,
            context,
            is_batch,
            arrow_ownership::ArrowOwnershipPolicy::ValidatedCopy,
        )
    }

    // Only the private decoder adapter grants ControlledParquet. A public
    // producer's format label or caller-owned RecordBatch cannot grant it.
    pub(crate) fn from_arrow_with_ownership(
        session: &ResidentVortexSession,
        batch: RecordBatch,
        context: &NativeExecutionContext<'_>,
        is_batch: bool,
        ownership: arrow_ownership::ArrowOwnershipPolicy,
    ) -> Result<Self> {
        Self::from_arrow_admitted(session, batch, context, is_batch, ownership, Origin::Caller)
    }

    /// File adapters retain the existing ingest dictionary contract. This does
    /// not enable the separate experimental public encoded-Arrow boundary.
    pub(crate) fn from_local_arrow_with_ownership(
        session: &ResidentVortexSession,
        batch: RecordBatch,
        context: &NativeExecutionContext<'_>,
        ownership: arrow_ownership::ArrowOwnershipPolicy,
    ) -> Result<Self> {
        Self::from_arrow_admitted(session, batch, context, true, ownership, Origin::LocalFile)
    }

    #[allow(clippy::too_many_lines)] // Keep allocation, validation and final owner publication together.
    fn from_arrow_admitted(
        session: &ResidentVortexSession,
        batch: RecordBatch,
        context: &NativeExecutionContext<'_>,
        is_batch: bool,
        ownership: arrow_ownership::ArrowOwnershipPolicy,
        origin: Origin,
    ) -> Result<Self> {
        session.validate_execution_context(context)?;
        let memory = context.memory();
        let schema = batch.schema();
        let mut metadata =
            crate::native_payload_schema::reserve_names(memory, schema.fields().len(), |index| {
                schema.field(index).name().as_str()
            })?;
        let mut metadata_bytes = metadata.bytes();
        for field in schema.fields() {
            context.check_cancelled()?;
            if !crate::native_payload_schema::arrow_field_metadata_admitted(field) {
                return Err(memory_error(
                    "direct columnar intake does not admit Arrow extension fields",
                ));
            }
            metadata_bytes = metadata_bytes
                .checked_add(schema_metadata_bytes(field, origin, context)?)
                .ok_or_else(|| memory_error("columnar schema metadata overflow"))?;
        }
        // Controlled transfers may retain arbitrarily many backing buffers at a
        // single view schema node. Charge their handle vectors before to_data
        // clones them, retaining credit with every imported buffer/child. Public
        // validated-copy intake keeps its existing schema metadata admission.
        if ownership == arrow_ownership::ArrowOwnershipPolicy::ControlledParquet {
            for column in batch.columns() {
                metadata_bytes = metadata_bytes
                    .checked_add(variadic_metadata_bytes(column.as_ref(), context)?)
                    .ok_or_else(|| memory_error("columnar buffer metadata overflow"))?;
            }
        }
        metadata.resize(metadata_bytes)?;
        // Keep the same validated physical descriptions for ownership admission.
        // Per-field metadata credit covers this vector and its child descriptors.
        let mut columns = Vec::new();
        columns
            .try_reserve_exact(batch.num_columns())
            .map_err(native_error)?;
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            context.check_cancelled()?;
            let data = column.to_data();
            let _validation = memory.reserve(nested::validation_workspace_bytes(&data)?)?;
            data.validate_full().map_err(native_error)?;
            if matches!(origin, Origin::LocalFile)
                && matches!(field.data_type(), arrow_schema::DataType::Dictionary(..))
                && context.float_policy() == shardloom_core::FloatPolicy::Strict
            {
                crate::vortex_ingest::reject_dictionary_non_finite_float_values(
                    field.name(),
                    column.as_ref(),
                )?;
            }
            nested::validate_values_with_float_policy(
                field.name(),
                column.as_ref(),
                nested::field_float_policy(field, context.float_policy()),
            )?;
            columns.push(data);
        }
        let metadata = Arc::new(metadata);
        let allocator = crate::owned_buffers::with_shared_credit(
            session.native_allocator(),
            Arc::clone(&metadata),
        );
        let rows = batch.num_rows();
        // Original arrays must release their buffer references before the
        // controlled owner asks Arrow to prove a unique standard allocation.
        drop(batch);
        let (retained, counts) = arrow_ownership::admit_batch_data_counted(
            Arc::clone(&schema),
            columns,
            rows,
            allocator.as_ref(),
            ownership,
        )?;
        let retained =
            nested::provider_batch(retained, Some(memory), &|| context.check_cancelled())?;
        context.check_cancelled()?;
        let schema = crate::native_temporal::arrow::schema(schema.as_ref())?;
        let array = context
            .native_session()
            .arrow()
            .with_preserved_decimal_storage()
            .from_arrow_record_batch(retained, &schema)
            .map_err(native_error)?;
        #[cfg(feature = "native-binary-objects")]
        crate::binary_object::validate_descriptors(&array, context)?;
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
            intake_columnar_buffer_bytes_copied: counts.copied_buffer_bytes,
            intake_columnar_copy_reasons: counts.copy_reasons,
            intake_columnar_buffer_bytes_transferred: counts.transferred_buffer_bytes,
            intake_columnar_buffer_regions_transferred: counts.transferred_buffer_regions,
            intake_columnar_allocation_bytes_transferred: counts.transferred_allocation_bytes,
            transfer_report: super::NativeTransferReport::default(),
            metadata: Some(metadata),
            is_batch,
            generated_range: None,
        })))
    }
}

fn schema_metadata_bytes(
    field: &arrow_schema::Field,
    origin: Origin,
    context: &NativeExecutionContext<'_>,
) -> Result<u64> {
    match origin {
        Origin::Caller => crate::native_payload_schema::arrow_field_metadata_bytes(field, context),
        Origin::LocalFile => {
            crate::native_payload_schema::arrow_file_metadata_bytes(field, context)
        }
    }
}

// Called only after the existing schema depth/type admission. Traversal
// borrows typed arrays; it neither clones buffer vectors nor visits row values.
fn variadic_metadata_bytes(
    array: &dyn arrow_array::Array,
    context: &NativeExecutionContext<'_>,
) -> Result<u64> {
    let mut traversal = context.memory().reserve(0)?;
    variadic_metadata_bytes_at(array, 0, &mut traversal, context)
}

fn variadic_metadata_bytes_at(
    array: &dyn arrow_array::Array,
    depth: usize,
    traversal: &mut shardloom_exec::live_memory::MemoryLease,
    context: &NativeExecutionContext<'_>,
) -> Result<u64> {
    use arrow_schema::DataType;
    context.check_cancelled()?;
    if depth > 24 {
        return Err(memory_error(
            "columnar buffer metadata exceeds schema traversal bounds",
        ));
    }
    traversal.resize(
        traversal
            .bytes()
            .checked_add(1024)
            .ok_or_else(|| memory_error("columnar metadata traversal overflow"))?,
    )?;
    let invalid = || memory_error("columnar array does not match its declared layout");
    let buffers = match array.data_type() {
        DataType::Utf8View => Some(
            array
                .as_string_view_opt()
                .ok_or_else(invalid)?
                .data_buffers()
                .len(),
        ),
        DataType::BinaryView => Some(
            array
                .as_binary_view_opt()
                .ok_or_else(invalid)?
                .data_buffers()
                .len(),
        ),
        DataType::Struct(_) => {
            return array
                .as_struct_opt()
                .ok_or_else(invalid)?
                .columns()
                .iter()
                .try_fold(0_u64, |bytes, child| {
                    bytes
                        .checked_add(variadic_metadata_bytes_at(
                            child.as_ref(),
                            depth + 1,
                            traversal,
                            context,
                        )?)
                        .ok_or_else(|| memory_error("columnar buffer metadata overflow"))
                });
        }
        _ => None,
    };
    if let Some(buffers) = buffers {
        // Covers simultaneously live Arrow/ArrayData/import handle vectors and
        // native wrappers. Payload capacities are reserved separately per region.
        return u64::try_from(buffers)
            .ok()
            .and_then(|count| count.checked_mul(512))
            .ok_or_else(|| memory_error("columnar buffer metadata overflow"));
    }
    let child = match array.data_type() {
        DataType::List(_) => Some(array.as_list_opt::<i32>().ok_or_else(invalid)?.values()),
        DataType::LargeList(_) => Some(array.as_list_opt::<i64>().ok_or_else(invalid)?.values()),
        DataType::FixedSizeList(..) => {
            Some(array.as_fixed_size_list_opt().ok_or_else(invalid)?.values())
        }
        DataType::Dictionary(..) => {
            Some(array.as_any_dictionary_opt().ok_or_else(invalid)?.values())
        }
        DataType::RunEndEncoded(..) => Some(array.as_any_ree_opt().ok_or_else(invalid)?.values()),
        _ => None,
    };
    child.map_or(Ok(0), |child| {
        variadic_metadata_bytes_at(child.as_ref(), depth + 1, traversal, context)
    })
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
