//! Detached, capacity-proved local intake and credited native recomposition.

use super::{
    MemorySourceBounds, MemorySourceOwner, NativeTransferReport, ResidentMemorySource,
    memory_error, native_error,
};
use crate::resident_session::{NativeExecutionContext, ResidentVortexSession};
use shardloom_core::Result;
use shardloom_exec::live_memory::MemoryLease;
use std::sync::Arc;
use vortex::array::{
    ArrayRef, IntoArray as _, arrays::StructArray, dtype::FieldNames, validity::Validity,
};

pub(crate) struct DetachedColumnarBatch {
    array: ArrayRef,
    bytes: u64,
    metadata: Arc<MemoryLease>,
}

impl DetachedColumnarBatch {
    // Only the private, validated local Arrow intake issues a capacity receipt.
    pub(super) fn new(source: &ResidentMemorySource, bytes: u64) -> Self {
        Self {
            array: source.0.array.clone(),
            bytes,
            metadata: Arc::clone(
                source
                    .0
                    .metadata
                    .as_ref()
                    .expect("columnar intake metadata"),
            ),
        }
    }

    pub(crate) fn retained_bytes(&self) -> u64 {
        self.bytes
    }
    pub(crate) fn metadata_bytes(&self) -> u64 {
        self.metadata.bytes()
    }
    pub(crate) fn dtype(&self) -> &vortex::array::dtype::DType {
        self.array.dtype()
    }
    pub(crate) fn rows(&self) -> usize {
        self.array.len()
    }
    pub(crate) fn columns(&self) -> usize {
        self.array.slots().len().saturating_sub(1)
    }
    pub(crate) fn column(&self, index: usize) -> Result<&ArrayRef> {
        self.array
            .slots()
            .get(index + 1)
            .and_then(Option::as_ref)
            .ok_or_else(|| memory_error("native column coverage omitted its field"))
    }
}

impl ResidentMemorySource {
    pub(crate) fn columnar_metadata_bytes(&self) -> u64 {
        self.0.metadata.as_ref().map_or(0, |credit| credit.bytes())
    }
    /// The caller provides only slices of validated, credited native intake.
    /// Each independent field wrapper retains the new composition metadata.
    pub(crate) fn from_columnar_coverage(
        session: &ResidentVortexSession,
        context: &NativeExecutionContext<'_>,
        names: &[&str],
        rows: usize,
        extra_metadata_bytes: u64,
        newly_read: Option<&Self>,
        build: impl FnOnce(&Arc<MemoryLease>) -> Result<Vec<ArrayRef>>,
    ) -> Result<Self> {
        session.validate_execution_context(context)?;
        let mut credit =
            crate::native_payload_schema::reserve_names(context.memory(), names.len(), |index| {
                names[index]
            })?;
        let wrappers = extra_metadata_bytes
            .checked_add(credit.bytes())
            .ok_or_else(|| memory_error("native column coverage composition overflow"))?;
        credit.resize(wrappers)?;
        let metadata = Arc::new(credit);
        let columns = build(&metadata)?;
        let array = StructArray::try_new(
            FieldNames::from(names.to_vec()),
            columns,
            rows,
            Validity::NonNullable,
        )
        .map_err(native_error)?
        .into_array();
        let input_logical_bytes = usize::try_from(array.nbytes()).map_err(native_error)?;
        context.check_cancelled()?;
        let read = newly_read.map(|source| &source.0);
        Ok(Self(Arc::new(MemorySourceOwner {
            bounds: MemorySourceBounds {
                max_input_rows: rows.max(1),
                max_input_bytes: input_logical_bytes.max(1),
                max_output_rows: rows.max(1),
                ..MemorySourceBounds::default()
            },
            array,
            session: session.clone(),
            input_logical_bytes,
            intake_payload_bytes_copied: 0,
            intake_columnar_buffer_bytes_copied: read
                .map_or(0, |source| source.intake_columnar_buffer_bytes_copied),
            intake_columnar_copy_reasons: read.map_or_else(Default::default, |source| {
                source.intake_columnar_copy_reasons
            }),
            intake_columnar_buffer_bytes_transferred: read
                .map_or(0, |source| source.intake_columnar_buffer_bytes_transferred),
            intake_columnar_buffer_regions_transferred: read.map_or(0, |source| {
                source.intake_columnar_buffer_regions_transferred
            }),
            intake_columnar_allocation_bytes_transferred: read.map_or(0, |source| {
                source.intake_columnar_allocation_bytes_transferred
            }),
            transfer_report: NativeTransferReport::default(),
            metadata: Some(metadata),
            is_batch: true,
            generated_range: None,
        })))
    }
}
