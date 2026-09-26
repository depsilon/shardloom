//! Explicit native serialization from complete owned batches into the existing
//! immutable segment source. Construction borrows one operation's CPU ownership.

use super::{
    GenerationBuildControl, GenerationBuilder, GenerationInputBounds, GenerationOwner,
    MemoryFileGeneration, MemoryFileGenerationBounds, MemoryFileGenerationLayout, generation_error,
    new_memory_generation_uri,
};
use crate::local_primitives::logical_field_from_native_array;
use crate::local_primitives::prepared_aggregate::PreparedVortexAggregate;
use crate::resident_session::{
    NativeExecutionContext, OwnedVortexResultBatch, PreparedVortexSource,
};
use shardloom_core::Result;
use shardloom_exec::compute_pool::CancellationToken;
use shardloom_exec::live_memory::Budgeted;
use std::{fmt::Write as _, sync::Arc};
use vortex::array::{
    ArrayRef, IntoArray as _,
    arrays::{ChunkedArray, StructArray},
    validity::Validity,
};

/// Explicit bounds for composing completed native results. These do not widen
/// typed external intake or JSON delivery limits. Serialized bytes include any
/// retained dictionary/backing-buffer amplification at the native boundary.
#[derive(Debug, Clone, Copy)]
pub struct MemoryFileCompositionBounds {
    pub max_rows: u64,
    pub max_columns: usize,
    pub max_batches: usize,
    pub storage: MemoryFileGenerationBounds,
    pub layout: MemoryFileGenerationLayout,
}

impl Default for MemoryFileCompositionBounds {
    fn default() -> Self {
        Self {
            max_rows: 1_048_576,
            max_columns: 1024,
            max_batches: 4096,
            storage: MemoryFileGenerationBounds {
                max_serialized_bytes: 64 * 1024 * 1024,
                max_metadata_bytes: 1024 * 1024,
            },
            layout: MemoryFileGenerationLayout::default(),
        }
    }
}

impl MemoryFileGeneration {
    /// Opaque identity for this immutable generation. It is evidence, not a
    /// filesystem path or a URI that another session can resolve.
    #[must_use]
    pub fn source_uri(&self) -> &shardloom_core::DatasetUri {
        &self.0.source_uri
    }

    pub(crate) fn retained_source(&self) -> PreparedVortexSource {
        self.0.source.clone()
    }

    pub(crate) fn annotate_aggregate_certificate(
        &self,
        certificate: &mut shardloom_core::NativeIoCertificate,
    ) -> Result<()> {
        let source = &mut certificate.source_capability_report;
        source.source_kind = "immutable_vortex_file_segments".into();
        source.adapter_id = "shardloom.resident_vortex.memory_file.v1".into();
        source.schema_discovery_status = "validated_immutable_native_footer".into();
        source.statistics_availability = "exact_footer_row_count;file_statistics_absent".into();
        let work = self.evidence();
        write!(certificate.source_pushdown_report.proof_basis,
            ";memory_generation_uri={};immutable_generation_owner_retained=true;source_specific_file_opens=0;construction_array_serializer_calls={};construction_segment_assembly_bytes_copied={};construction_footer_serializer_calls={};construction_footer_bytes={};construction_native_materialization_calls={};construction_native_materialization_rows={};cumulative_memory_segment_requests={};cumulative_memory_segment_bytes_returned={};construction_excluded_from_query_work=true;no_zero_copy_composition_claim=true",
            self.source_uri().as_str(), work.array_serializer_calls,
            work.segment_assembly_bytes_copied, work.construction_footer_serializer_calls,
            work.construction_footer_bytes, work.construction_native_materialization_calls,
            work.construction_native_materialization_rows, work.memory_segment_requests,
            work.memory_segment_bytes_returned,
        ).map_err(generation_error)?;
        Ok(())
    }

    /// Prepare the ordinary native aggregate kernels against these retained
    /// segments. The request URI must match `source_uri()`; no file is opened.
    ///
    /// # Errors
    /// Rejects mismatched provenance and the existing native aggregate schema,
    /// semantics, resource and explicit-spill admission failures.
    pub fn prepare_aggregate(
        &self,
        request: &crate::VortexQueryPrimitiveRequest,
        policy: crate::VortexLocalPrimitiveExecutionPolicy,
    ) -> Result<PreparedVortexAggregate> {
        crate::local_primitives::prepared_aggregate::prepare_memory_aggregate(
            request, policy, self, None,
        )
    }

    /// Serialize complete owned native batches into one immutable memory source.
    /// This consumes the original result; downstream scans retain the constructed
    /// segments. Serialization is explicit and is not a zero-copy claim.
    ///
    /// # Errors
    /// Rejects schema/row inconsistencies, non-Struct results, resource bounds,
    /// unsupported native serialization and cancellation before publication.
    pub fn from_owned(
        result: OwnedVortexResultBatch,
        bounds: MemoryFileCompositionBounds,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        let session = result.retained_session();
        let generation = session.with_native_execution_context(cancellation, |context| {
            Self::from_owned_in_context(&result, bounds, context)
        });
        drop(result);
        generation
    }

    pub(crate) fn from_owned_in_context(
        result: &OwnedVortexResultBatch,
        bounds: MemoryFileCompositionBounds,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let session = result.retained_session();
        session.validate_execution_context(context)?;
        context.check_general_execution()?;
        context.check_cancelled()?;
        result.validate_schema_and_rows()?;
        let array = composition_array(result, bounds, context)?;
        let builder = GenerationBuilder::with_input_bounds(
            &session,
            array.value(),
            bounds.storage,
            bounds.layout,
            GenerationBuildControl {
                cancelled: None,
                cancellation: Some(context.cancellation()),
                #[cfg(test)]
                after_leaf: None,
            },
            GenerationInputBounds {
                rows: usize::try_from(bounds.max_rows).map_err(generation_error)?,
                columns: bounds.max_columns,
            },
        )?;
        let columns = builder.columns;
        let row_groups = builder.row_groups;
        let (file, segments, row_group_offset_bytes_built, construction_footer_bytes) =
            builder.finish(context.native_session(), context.runtime(), bounds.storage)?;
        context.check_cancelled()?;
        Ok(Self(Arc::new(GenerationOwner {
            source_uri: new_memory_generation_uri()?,
            source: session.prepare_immutable_file(file),
            session,
            segments,
            input_logical_bytes: result.logical_buffer_bytes(),
            intake_payload_bytes_copied: 0,
            bounds: bounds.storage,
            geometry: bounds.layout,
            rows: usize::try_from(result.row_count()).map_err(generation_error)?,
            row_groups,
            columns,
            row_group_offset_bytes_built,
            construction_footer_bytes,
        })))
    }
}

pub(crate) fn composition_array(
    result: &OwnedVortexResultBatch,
    bounds: MemoryFileCompositionBounds,
    context: &NativeExecutionContext<'_>,
) -> Result<Budgeted<ArrayRef>> {
    let fields = result
        .dtype()
        .as_struct_fields_opt()
        .ok_or_else(|| generation_error("composition requires a Struct result"))?;
    if bounds.max_rows == 0
        || bounds.max_columns == 0
        || bounds.max_batches == 0
        || result.row_count() > bounds.max_rows
        || fields.nfields() == 0
        || fields.nfields() > bounds.max_columns
        || result.arrays().len() > bounds.max_batches
    {
        return Err(generation_error(
            "composition exceeds row, field or batch bounds",
        ));
    }
    // Cover adapter references plus each native Chunked array's offset vector.
    // Provider layout object internals and serialization scratch retain the
    // existing documented accounting boundary, separate from serialized bytes.
    let bytes = result
        .arrays()
        .len()
        .checked_add(1)
        .and_then(|count| count.checked_mul(fields.nfields()))
        .and_then(|count| count.checked_mul(std::mem::size_of::<ArrayRef>() + 8))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| generation_error("composition reference capacity overflow"))?;
    if bytes > bounds.storage.max_metadata_bytes {
        return Err(generation_error(
            "composition reference metadata bound exceeded",
        ));
    }
    let lease = context.memory().reserve(bytes)?;
    let mut columns = Vec::with_capacity(fields.nfields());
    for (name, dtype) in fields.names().iter().zip(fields.fields()) {
        context.check_cancelled()?;
        let dtype = if result.dtype().is_nullable() {
            dtype.as_nullable()
        } else {
            dtype
        };
        let mut chunks = Vec::with_capacity(result.arrays().len());
        for batch in result.arrays() {
            context.check_cancelled()?;
            chunks.push(logical_field_from_native_array(batch, name.as_ref())?);
        }
        columns.push(
            ChunkedArray::try_new(chunks, dtype)
                .map_err(generation_error)?
                .into_array(),
        );
    }
    let array = StructArray::try_new(
        fields.names().clone(),
        columns,
        usize::try_from(result.row_count()).map_err(generation_error)?,
        Validity::NonNullable,
    )
    .map_err(generation_error)?
    .into_array();
    Ok(Budgeted::new(array, lease))
}
