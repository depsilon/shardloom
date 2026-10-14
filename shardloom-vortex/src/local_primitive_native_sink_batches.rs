//! Bound footer growth when a native producer emits many small result arrays.

use super::{DType, NativeExecutionContext, Result, sink_error, vortex_error};
use crate::local_primitives::{native_capacity::ReservedVec, native_payload};
use vortex::array::{ArrayRef, IntoArray as _, arrays::ChunkedArray};

const ROWS: usize = 128;
const BYTES: u64 = 256 * 1024;
// Pending fragments, incoming array, compact replacement and the writer's
// existing three-array bound can overlap during a flush.
pub(super) const INPUT_ARRAY_BOUND: usize = ROWS + 5;

pub(super) struct Batches<'a> {
    dtype: &'a DType,
    metadata_bytes: u64,
    enabled: bool,
    pending: ReservedVec<ArrayRef>,
    rows: usize,
    bytes: u64,
    pub(super) copied_bytes: u64,
    pub(super) scalar_values: u64,
}

impl<'a> Batches<'a> {
    pub(super) fn new(
        dtype: &'a DType,
        produced: bool,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        // Passthrough remains valid for native schemas outside the bounded
        // copier's admission. Batching must not narrow an existing sink path.
        let metadata = crate::native_payload_schema::schema_bytes(dtype).ok();
        Ok(Self {
            dtype,
            metadata_bytes: metadata.unwrap_or(0),
            enabled: produced && metadata.is_some(),
            pending: ReservedVec::new(context.memory())?,
            rows: 0,
            bytes: 0,
            copied_bytes: 0,
            scalar_values: 0,
        })
    }

    pub(super) fn push(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        context.check_cancelled()?;
        if array.dtype() != self.dtype {
            return Err(sink_error("native writer batch changed its admitted dtype"));
        }
        if array.is_empty() {
            return Ok(());
        }
        let bytes = array
            .nbytes()
            .checked_add(self.metadata_bytes)
            .ok_or_else(|| sink_error("native writer batch capacity overflow"))?;
        if !self.enabled || array.len() >= ROWS || bytes >= BYTES {
            self.flush(context, consume)?;
            return consume(array);
        }
        if array.len() > ROWS.saturating_sub(self.rows)
            || bytes > BYTES.saturating_sub(self.bytes)
            || self.pending.values.len() >= ROWS
        {
            self.flush(context, consume)?;
        }
        self.rows += array.len();
        self.bytes += bytes;
        self.pending.push(array)?;
        if self.rows == ROWS || self.bytes == BYTES {
            self.flush(context, consume)?;
        }
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
        self.rows = 0;
        self.bytes = 0;
        if self.pending.values.len() == 1 {
            return consume(self.pending.values.pop().expect("one pending array"));
        }
        // Chunk offsets use the upstream constructor. Credit its transient
        // offsets/schema before construction and retain the old array-vector
        // credit until the native compact copy releases all chunk owners.
        let scratch_bytes = (self.pending.values.len() as u64 + 1)
            .checked_mul(8)
            .and_then(|bytes| bytes.checked_add(self.metadata_bytes))
            .ok_or_else(|| sink_error("native writer chunk metadata overflow"))?;
        let scratch = context.memory().reserve(scratch_bytes)?;
        let pending = std::mem::replace(&mut self.pending, ReservedVec::new(context.memory())?);
        let (arrays, ownership) = pending.into_parts();
        let chunked = ChunkedArray::try_new(arrays, self.dtype.clone())
            .map_err(vortex_error)?
            .into_array();
        let (compact, scalars) = native_payload::detach_with_scalar_count(
            &chunked,
            native_payload::CopyPolicy::PreserveUnobserved,
            context,
        )?;
        self.copied_bytes = self
            .copied_bytes
            .checked_add(compact.nbytes())
            .ok_or_else(|| sink_error("native writer copy accounting overflow"))?;
        self.scalar_values = self
            .scalar_values
            .checked_add(scalars)
            .ok_or_else(|| sink_error("native writer scalar accounting overflow"))?;
        drop((chunked, ownership, scratch));
        consume(compact)
    }
}
