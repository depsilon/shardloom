//! Bounded external batches become one resident Vortex source. `ChunkedArray` owns
//! composition; the ordinary relational scan controls execution and delivery.

use super::{
    MemoryColumn, MemorySourceBounds, MemorySourceOwner, NativeTransferReport,
    ResidentMemorySource, memory_error, native_error,
};
use crate::local_primitives::native_capacity::ReservedVec;
use crate::resident_session::ResidentVortexSession;
use shardloom_core::Result;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::MemoryLease};
use std::sync::Arc;
use vortex::{
    array::{
        ArrayRef, IntoArray as _,
        arrays::{ChunkedArray, PrimitiveArray, StructArray},
        dtype::DType,
        validity::Validity,
    },
    buffer::Buffer,
};

pub(crate) const MAX_BATCH_ROWS: usize = 2048;

/// Incremental typed intake under one native memory owner. Input remains
/// resident; batch delivery does not promise source or operator-state spill.
/// A source is published only by `finish`, after every batch has been validated.
pub struct MemoryBatchSourceBuilder {
    session: ResidentVortexSession,
    batches: ReservedVec<ArrayRef>,
    metadata: MemoryLease,
    rows: usize,
    bytes: usize,
    copied: u64,
    columnar_copied: u64,
    transfer_report: NativeTransferReport,
    cancellation: CancellationToken,
}

impl MemoryBatchSourceBuilder {
    /// Start a resident source with 2,048-row intake units.
    /// Payload and growing batch metadata use the shared session grant.
    /// # Errors
    /// Rejects cancellation or metadata admission failure before allocating.
    pub fn new(session: &ResidentVortexSession, cancellation: CancellationToken) -> Result<Self> {
        cancellation.check()?;
        let metadata = session.memory().reserve(4096)?;
        Ok(Self {
            session: session.clone(),
            batches: ReservedVec::new(session.memory())?,
            metadata,
            rows: 0,
            bytes: 0,
            copied: 0,
            columnar_copied: 0,
            transfer_report: NativeTransferReport::default(),
            cancellation,
        })
    }

    /// Admit a boundary adapter's temporary frame/conversion storage before it
    /// is allocated. Keep this lease alive through conversion and buffer intake.
    /// # Errors
    /// Rejects shared-memory pressure or cancellation.
    pub fn reserve_scratch(&self, bytes: u64) -> Result<MemoryLease> {
        self.cancellation.check()?;
        self.session.memory().reserve(bytes)
    }

    /// Interpret the same pinned native field declarations used by object and
    /// batch inputs as an explicit Arrow construction boundary. Only schema
    /// metadata crosses JSON; values are constructed directly by the adapter.
    /// Top-level width uses shared admission, with the existing per-field nested
    /// schema rules. The returned credit must remain alive during construction.
    /// # Errors
    /// Rejects malformed/unsupported types, duplicate names or memory pressure.
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    pub fn object_input_schema(
        &self,
        fields: &[(String, String)],
    ) -> Result<shardloom_exec::live_memory::Budgeted<arrow_schema::SchemaRef>> {
        use arrow_schema::{Field, Schema};
        use shardloom_exec::live_memory::Budgeted;
        self.cancellation.check()?;
        let memory = self.session.memory();
        let mut metadata =
            crate::native_payload_schema::reserve_names(memory, fields.len(), |i| {
                fields[i].0.as_str()
            })?;
        let mut converted = Vec::with_capacity(fields.len());
        for (name, raw) in fields {
            self.cancellation.check()?;
            let (dtype, mut credit) =
                crate::local_primitives::native_input_schema::parse_dtype(raw, memory)?;
            let bytes = credit
                .bytes()
                .checked_mul(4)
                .ok_or_else(|| memory_error("object schema conversion metadata overflow"))?;
            credit.resize(bytes)?;
            let data_type = crate::native_arrow_conversion::data_type(&dtype).ok_or_else(|| {
                memory_error("native object input type has no admitted columnar representation")
            })?;
            converted.push(Field::new(name, data_type, dtype.is_nullable()));
            metadata.absorb(&mut credit)?;
        }
        Ok(Budgeted::new(Arc::new(Schema::new(converted)), metadata))
    }

    /// Copy one caller-owned typed batch into native allocator-owned buffers.
    /// All batches, including empty ones, must declare the exact same schema.
    /// # Errors
    /// Rejects row/byte limits, schema drift, nonfinite floats, cancellation,
    /// overflow and memory pressure before publishing any source.
    pub fn push_columns(&mut self, columns: &[MemoryColumn<'_>]) -> Result<()> {
        self.cancellation.check()?;
        // Each payload carries its own structural credit, including escaped
        // child buffers. The container and final composition have separate owners.
        let source = ResidentMemorySource::from_batch_columns(&self.session, columns)?;
        self.push_source(&source)
    }

    /// Copy an explicitly columnar input batch under the same shared allocation.
    /// The source scan selects execution batch sizes after intake; the producer's
    /// batch geometry is not an artificial total-input limit.
    /// # Errors
    /// Rejects unsupported schemas, invalid values, schema drift, cancellation,
    /// overflow or memory pressure before publishing a source.
    #[cfg(all(feature = "vortex-write", feature = "universal-format-io"))]
    pub fn push_arrow(&mut self, batch: arrow_array::RecordBatch) -> Result<()> {
        self.cancellation.check()?;
        let source = ResidentMemorySource::from_arrow_record_batch(
            &self.session,
            batch,
            &self.cancellation,
        )?;
        self.push_source(&source)
    }

    fn push_source(&mut self, source: &ResidentMemorySource) -> Result<()> {
        self.push_array(
            source.0.array.clone(),
            source.input_logical_bytes(),
            source.intake_payload_bytes_copied(),
            source.intake_columnar_buffer_bytes_copied(),
        )
    }

    /// Append complete native batches from this same session without transferring
    /// their values through a compatibility representation. Result schema and
    /// buffer ownership remain valid after the original result handle drops.
    /// # Errors
    /// Rejects foreign grants, inconsistent results, schema drift, cancellation,
    /// overflow or native structure/composition admission failure.
    #[cfg(feature = "vortex-write")]
    pub fn push_native_result(
        &mut self,
        result: &crate::resident_session::OwnedVortexResultBatch,
    ) -> Result<()> {
        use vortex::array::VortexSessionExecute as _;
        self.cancellation.check()?;
        let session = result.retained_session();
        if !self.session.same_owner(&session) {
            return Err(memory_error(
                "native result belongs to another resource owner",
            ));
        }
        result.validate_schema_and_rows()?;
        let cancellation = self.cancellation.clone();
        session.with_native_execution_context(&cancellation, |context| {
            let mut execution = context.native_session().create_execution_ctx();
            if result.arrays().is_empty() {
                let empty =
                    crate::local_primitives::native_payload::empty_record(result.dtype(), context)?;
                return self.push_array(empty, 0, 0, 0);
            }
            for array in result.arrays() {
                context.check_cancelled()?;
                let structure = array
                    .clone()
                    .execute::<StructArray>(&mut execution)
                    .map_err(native_error)?
                    .into_array();
                let bytes = usize::try_from(structure.nbytes()).map_err(native_error)?;
                self.push_array(structure, bytes, 0, 0)?;
            }
            Ok(())
        })
    }

    /// Private trusted-native transfer. Each output buffer owns receiving memory
    /// and schema credit; no source credit or caller buffer escapes into it.
    #[cfg(feature = "vortex-write")]
    pub(super) fn push_native_copy(&mut self, array: &ArrayRef) -> Result<()> {
        self.push_native_copies(std::slice::from_ref(array))
    }

    /// Transfer a completed native graph once, including buffers shared between
    /// separate result batches. Classification precedes all materialization.
    #[cfg(feature = "vortex-write")]
    pub(super) fn push_native_copies(&mut self, arrays: &[ArrayRef]) -> Result<()> {
        let session = self.session.clone();
        let cancellation = self.cancellation.clone();
        session.with_native_execution_context(&cancellation, |context| {
            if let Some(copied) = super::encoded_transfer::copy_records(
                arrays,
                context,
                super::encoded_transfer::TransferPolicy::MinimizeFixedWidthCopies,
            )? {
                self.push_transferred(copied)?;
            } else {
                for array in arrays {
                    for start in (0..array.len().max(1)).step_by(MAX_BATCH_ROWS) {
                        context.check_cancelled()?;
                        let end = start.saturating_add(MAX_BATCH_ROWS).min(array.len());
                        let input = array.slice(start..end).map_err(native_error)?;
                        let output =
                            crate::local_primitives::native_payload::detach(&input, context)?;
                        let copied = output.nbytes();
                        let bytes = usize::try_from(copied).map_err(native_error)?;
                        self.push_array(output, bytes, 0, copied)?;
                        self.transfer_report =
                            self.transfer_report.combine(NativeTransferReport {
                                materialized_batches: 1,
                                materialized_rows: (end - start) as u64,
                                ..NativeTransferReport::default()
                            })?;
                    }
                }
            }
            context.check_cancelled()
        })
    }

    #[cfg(feature = "vortex-write")]
    pub(super) fn push_transferred(
        &mut self,
        copied: super::encoded_transfer::CopiedRecords,
    ) -> Result<()> {
        let report = self.transfer_report.combine(copied.report)?;
        let mut copied_bytes = copied
            .report
            .encoded_buffer_bytes_copied
            .checked_add(copied.report.ipc_buffer_bytes_copied)
            .ok_or_else(|| memory_error("native columnar copy count overflow"))?;
        for (array, bytes) in copied.arrays.values {
            self.push_array(
                array,
                usize::try_from(bytes).map_err(native_error)?,
                0,
                copied_bytes,
            )?;
            copied_bytes = 0;
        }
        self.transfer_report = report;
        Ok(())
    }

    fn push_array(
        &mut self,
        array: ArrayRef,
        input_logical_bytes: usize,
        intake_payload_bytes_copied: u64,
        intake_columnar_buffer_bytes_copied: u64,
    ) -> Result<()> {
        if self
            .batches
            .values
            .first()
            .is_some_and(|first| first.dtype() != array.dtype())
        {
            return Err(memory_error("native batch source schema changed"));
        }
        let rows = self
            .rows
            .checked_add(array.len())
            .ok_or_else(|| memory_error("native batch source row overflow"))?;
        let bytes = self
            .bytes
            .checked_add(input_logical_bytes)
            .ok_or_else(|| memory_error("native batch source byte overflow"))?;
        let copied = self
            .copied
            .checked_add(intake_payload_bytes_copied)
            .ok_or_else(|| memory_error("native batch source copy count overflow"))?;
        let columnar_copied = self
            .columnar_copied
            .checked_add(intake_columnar_buffer_bytes_copied)
            .ok_or_else(|| memory_error("native columnar copy count overflow"))?;
        self.cancellation.check()?;
        if self.batches.values.is_empty() {
            self.batches.push(array)?;
        } else if !array.is_empty() {
            if self.batches.values[0].is_empty() {
                self.batches.values[0] = array;
            } else {
                self.batches.push(array)?;
            }
        }
        self.rows = rows;
        self.bytes = bytes;
        self.copied = copied;
        self.columnar_copied = columnar_copied;
        Ok(())
    }

    /// Publish immutable native columns without combining/copying their payloads.
    /// Even an empty source needs one typed empty input batch.
    /// # Errors
    /// Rejects absent schema, cancellation, overflow or composition admission.
    pub fn finish(mut self) -> Result<ResidentMemorySource> {
        self.cancellation.check()?;
        let fields = self
            .batches
            .values
            .first()
            .ok_or_else(|| memory_error("native batch source requires a typed batch"))?
            .dtype()
            .as_struct_fields_opt()
            .ok_or_else(|| memory_error("native batch source requires a struct"))?;
        let composition_bytes = (self.batches.values.len() as u64 + 1)
            .checked_mul(fields.nfields() as u64)
            .and_then(|bytes| bytes.checked_mul(128))
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| memory_error("native batch composition metadata overflow"))?;
        self.metadata.resize(
            self.metadata
                .bytes()
                .checked_add(composition_bytes)
                .ok_or_else(|| memory_error("native batch composition credit overflow"))?,
        )?;
        let credit = Arc::new(self.metadata);
        let mut columns = Vec::with_capacity(fields.nfields());
        for (index, dtype) in fields.fields().enumerate() {
            self.cancellation.check()?;
            let chunks = self
                .batches
                .values
                .iter()
                .map(|batch| {
                    batch
                        .slots()
                        .get(index + 1)
                        .and_then(Option::as_ref)
                        .cloned()
                        .ok_or_else(|| memory_error("native batch omitted a declared field"))
                })
                .collect::<Result<Vec<_>>>()?;
            columns.push(owned_chunks(chunks.into_iter(), dtype, &credit)?);
        }
        let array = StructArray::try_new(
            fields.names().clone(),
            columns,
            self.rows,
            Validity::NonNullable,
        )
        .map_err(native_error)?
        .into_array();
        self.cancellation.check()?;
        Ok(ResidentMemorySource(Arc::new(MemorySourceOwner {
            array,
            session: self.session,
            bounds: MemorySourceBounds {
                max_input_rows: self.rows.max(1),
                max_input_bytes: self.bytes.max(1),
                max_output_rows: self.rows.max(1),
                // Completed native/JSON output has its own boundary. A narrow
                // input may expand into chunk metadata and JSON syntax.
                max_output_bytes: MemorySourceBounds::default().max_output_bytes,
            },
            input_logical_bytes: self.bytes,
            intake_payload_bytes_copied: self.copied,
            intake_columnar_buffer_bytes_copied: self.columnar_copied,
            // This builder accepts copied public Arrow or already-native
            // results, never the private decoder's single-use intake handle.
            intake_columnar_buffer_bytes_transferred: 0,
            intake_columnar_buffer_regions_transferred: 0,
            intake_columnar_allocation_bytes_transferred: 0,
            transfer_report: self.transfer_report,
            metadata: Some(credit),
            is_batch: false,
            generated_range: None,
        })))
    }
}

pub(super) fn owned_chunks(
    chunks: impl ExactSizeIterator<Item = ArrayRef>,
    dtype: DType,
    credit: &Arc<MemoryLease>,
) -> Result<ArrayRef> {
    let array = ChunkedArray::try_new(chunks, dtype).map_err(native_error)?;
    let mut parts = array
        .try_into_parts()
        .map_err(|_| memory_error("new native batch composition is unexpectedly shared"))?;
    let offsets = parts.slots[0]
        .as_ref()
        .ok_or_else(|| memory_error("native batch composition omitted offsets"))?;
    let buffer = crate::owned_buffers::retain_shared_credit(
        offsets.buffers()[0].clone(),
        Arc::clone(credit),
    );
    parts.slots[0] = Some(
        PrimitiveArray::new(
            Buffer::<u64>::from_byte_buffer(buffer),
            Validity::NonNullable,
        )
        .into_array(),
    );
    ChunkedArray::try_from_parts(parts)
        .map(vortex::array::IntoArray::into_array)
        .map_err(native_error)
}

#[cfg(test)]
#[path = "resident_memory_batch_tests.rs"]
mod tests;
