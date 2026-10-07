//! Bounded external batches become one resident Vortex source. `ChunkedArray` owns
//! composition; the ordinary relational scan controls execution and delivery.

use super::{
    MemoryColumn, MemorySourceBounds, MemorySourceOwner, ResidentMemorySource, memory_error,
    native_error,
};
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

pub(crate) const MAX_BATCHES: usize = 4096;
pub(crate) const MAX_COLUMNS: usize = 128;
pub(crate) const MAX_BATCH_ROWS: usize = 2048;

/// Incremental typed intake under one native memory owner. Input remains
/// resident; batch delivery does not promise source or operator-state spill.
/// A source is published only by `finish`, after every batch has been validated.
pub struct MemoryBatchSourceBuilder {
    session: ResidentVortexSession,
    batches: Vec<ArrayRef>,
    metadata: MemoryLease,
    rows: usize,
    bytes: usize,
    copied: u64,
    cancellation: CancellationToken,
}

impl MemoryBatchSourceBuilder {
    /// Start a resident source with at most 4,096 batches of 2,048 rows and
    /// 128 columns. Payload size is admitted by the shared session grant.
    /// # Errors
    /// Rejects cancellation or metadata admission failure before allocating.
    pub fn new(session: &ResidentVortexSession, cancellation: CancellationToken) -> Result<Self> {
        cancellation.check()?;
        let metadata = session
            .memory()
            .reserve((MAX_BATCHES * std::mem::size_of::<ArrayRef>() + MAX_COLUMNS * 1024) as u64)?;
        Ok(Self {
            session: session.clone(),
            batches: Vec::with_capacity(MAX_BATCHES),
            metadata,
            rows: 0,
            bytes: 0,
            copied: 0,
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

    /// Copy one caller-owned typed batch into native allocator-owned buffers.
    /// All batches, including empty ones, must declare the exact same schema.
    /// # Errors
    /// Rejects width/row limits, schema drift, nonfinite floats, cancellation,
    /// overflow and memory pressure before publishing any source.
    pub fn push_columns(&mut self, columns: &[MemoryColumn<'_>]) -> Result<()> {
        self.cancellation.check()?;
        if self.batches.len() == MAX_BATCHES {
            return Err(memory_error("native batch source exceeds 4,096 batches"));
        }
        if columns.is_empty() || columns.len() > MAX_COLUMNS {
            return Err(memory_error("native batch source requires 1..=128 fields"));
        }
        // Covers each batch's native array metadata. Payloads use their own
        // allocator credits, and the composition credit is attached at finish.
        self.metadata.resize(
            self.metadata
                .bytes()
                .checked_add((columns.len() as u64 + 1) * 1024)
                .ok_or_else(|| memory_error("batch metadata size overflow"))?,
        )?;
        let source = ResidentMemorySource::from_columns_with_width(
            &self.session,
            columns,
            MemorySourceBounds {
                max_input_rows: MAX_BATCH_ROWS,
                ..MemorySourceBounds::default()
            },
            MAX_COLUMNS,
        )?;
        if self
            .batches
            .first()
            .is_some_and(|first| first.dtype() != source.dtype())
        {
            return Err(memory_error("native batch source schema changed"));
        }
        let rows = self
            .rows
            .checked_add(source.row_count())
            .ok_or_else(|| memory_error("native batch source row overflow"))?;
        let bytes = self
            .bytes
            .checked_add(source.input_logical_bytes())
            .ok_or_else(|| memory_error("native batch source byte overflow"))?;
        let copied = self
            .copied
            .checked_add(source.intake_payload_bytes_copied())
            .ok_or_else(|| memory_error("native batch source copy count overflow"))?;
        self.cancellation.check()?;
        self.batches.push(source.0.array.clone());
        self.rows = rows;
        self.bytes = bytes;
        self.copied = copied;
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
            .first()
            .ok_or_else(|| memory_error("native batch source requires a typed batch"))?
            .dtype()
            .as_struct_fields_opt()
            .ok_or_else(|| memory_error("native batch source requires a struct"))?;
        let composition_bytes = (self.batches.len() as u64 + 1)
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
                max_output_bytes: self.bytes.max(1),
            },
            input_logical_bytes: self.bytes,
            intake_payload_bytes_copied: self.copied,
            batch_metadata: None,
        })))
    }
}

fn owned_chunks(
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
