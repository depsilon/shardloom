//! Bounded native record compaction shared by expanding and stored operators.

use super::{
    native_capacity::ReservedVec,
    native_payload,
    native_relational_batch::{failed, index_array},
    vortex_error,
};
use crate::resident_session::NativeExecutionContext;
use shardloom_core::Result;
use vortex::array::{ArrayRef, IntoArray as _, arrays::ChunkedArray, dtype::DType};

// Capacity estimates govern batching only. Every actual owner still reserves
// against the query grant; one oversized row is admitted or denied explicitly.
const PENDING_BYTES: u64 = 256 * 1024;
const PENDING_CHUNKS: usize = 128;

pub(super) struct OutputBuffer {
    dtype: DType,
    pending: ReservedVec<ArrayRef>,
    pending_bytes: u64,
    pending_rows: usize,
    batch_rows: usize,
    chunk_metadata_bytes: u64,
    copy_policy: native_payload::CopyPolicy,
}

impl OutputBuffer {
    pub(super) fn new(
        dtype: DType,
        batch_rows: usize,
        copy_policy: native_payload::CopyPolicy,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        if batch_rows == 0 {
            return Err(failed(
                "native output compaction requires positive batch capacity",
            ));
        }
        let chunk_metadata_bytes = crate::native_payload_schema::schema_bytes(&dtype)?
            .checked_mul(2)
            .ok_or_else(|| failed("completed record metadata capacity overflow"))?;
        Ok(Self {
            dtype,
            pending: ReservedVec::new(context.memory())?,
            pending_bytes: 0,
            pending_rows: 0,
            batch_rows,
            chunk_metadata_bytes,
            copy_policy,
        })
    }

    pub(super) fn push(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        context.check_cancelled()?;
        if array.dtype() != &self.dtype {
            return Err(failed("completed native record changed its dtype"));
        }
        if array.is_empty() {
            return Ok(());
        }
        let bytes = array
            .nbytes()
            .checked_add(self.chunk_metadata_bytes)
            .ok_or_else(|| failed("completed record capacity overflow"))?;
        if bytes > PENDING_BYTES.saturating_sub(self.pending_bytes)
            || self.pending.values.len() >= PENDING_CHUNKS
            || array.len() > self.batch_rows.saturating_sub(self.pending_rows)
        {
            self.flush(context, consume)?;
        }
        // Already bounded ordinary batches remain independently owned and do
        // not need another payload copy just to pass through this buffer.
        if bytes > PENDING_BYTES || array.len() >= self.batch_rows {
            consume(array)?;
            return context.check_cancelled();
        }
        self.pending.reserve_one()?;
        self.pending_rows += array.len();
        self.pending.values.push(array);
        self.pending_bytes += bytes;
        Ok(())
    }

    pub(super) fn flush(
        &mut self,
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        context.check_cancelled()?;
        if self.pending.values.is_empty() {
            return Ok(());
        }
        let pending = std::mem::replace(&mut self.pending, ReservedVec::new(context.memory())?);
        let rows = std::mem::take(&mut self.pending_rows);
        self.pending_bytes = 0;
        let (mut arrays, ownership) = pending.into_parts();
        if arrays.len() == 1 {
            let array = arrays
                .pop()
                .ok_or_else(|| failed("completed output is absent"))?;
            drop((arrays, ownership));
            consume(array)?;
        } else {
            let chunked = ChunkedArray::try_new(arrays, self.dtype.clone())
                .map_err(vortex_error)?
                .into_array();
            let indices = index_array(rows, false, context, |row| Ok(Some(row)))?;
            let compact = native_payload::take_record_with_policy(
                &chunked,
                &indices,
                &self.dtype,
                self.copy_policy,
                context,
            )?;
            drop((chunked, ownership, indices));
            consume(compact)?;
        }
        context.check_cancelled()
    }
}
