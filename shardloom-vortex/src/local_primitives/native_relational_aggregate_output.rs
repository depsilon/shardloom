//! Compact completed groups before retaining them for first-seen output order.

use super::{Ordering, State, failed};
use crate::{
    local_primitives::{
        native_capacity::ReservedVec, native_payload, native_relational_batch::index_array,
        native_relational_sort::Spec, vortex_error,
    },
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use vortex::array::{
    ArrayRef, IntoArray as _,
    arrays::ChunkedArray,
    dtype::{DType, Nullability},
};

// Capacity estimates govern batching only. Every actual owner still reserves
// against the query grant; one oversized row is admitted or denied explicitly.
const PENDING_BYTES: u64 = 256 * 1024;
const PENDING_ROWS: usize = 128;

pub(super) struct Results<'a> {
    order: Ordering<'a, 'a>,
    dtype: DType,
    pending: ReservedVec<ArrayRef>,
    pending_bytes: u64,
    row_metadata_bytes: u64,
}

impl<'a> Results<'a> {
    pub(super) fn new(
        spec: &'a Spec,
        state: &'a State,
        batch_rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let dtype = DType::struct_(spec.fields.clone(), Nullability::NonNullable);
        let row_metadata_bytes = crate::native_payload_schema::schema_bytes(&dtype)?
            .checked_mul(2)
            .ok_or_else(|| failed("aggregate result metadata capacity overflow"))?;
        Ok(Self {
            order: Ordering::new(spec, state, batch_rows, context)?,
            dtype,
            pending: ReservedVec::new(context.memory())?,
            pending_bytes: 0,
            row_metadata_bytes,
        })
    }

    pub(super) fn push(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        context.check_cancelled()?;
        if array.len() != 1 || array.dtype() != &self.dtype {
            return Err(failed(
                "completed aggregate group changed its row count or dtype",
            ));
        }
        let bytes = array
            .nbytes()
            .checked_add(self.row_metadata_bytes)
            .ok_or_else(|| failed("aggregate result capacity overflow"))?;
        if bytes > PENDING_BYTES.saturating_sub(self.pending_bytes)
            || self.pending.values.len() >= PENDING_ROWS
        {
            self.flush(context)?;
        }
        if bytes > PENDING_BYTES {
            return self.order.build(array, context);
        }
        self.pending.reserve_one()?;
        self.pending.values.push(array);
        self.pending_bytes = self
            .pending_bytes
            .checked_add(bytes)
            .ok_or_else(|| failed("aggregate pending output capacity overflow"))?;
        Ok(())
    }

    fn flush(&mut self, context: &NativeExecutionContext<'_>) -> Result<()> {
        if self.pending.values.is_empty() {
            return Ok(());
        }
        let pending = std::mem::replace(&mut self.pending, ReservedVec::new(context.memory())?);
        self.pending_bytes = 0;
        let rows = pending.values.len();
        let (arrays, ownership) = pending.into_parts();
        let chunked = ChunkedArray::try_new(arrays, self.dtype.clone())
            .map_err(vortex_error)?
            .into_array();
        let indices = index_array(rows, false, context, |row| Ok(Some(row)))?;
        let compact = native_payload::take_record(&chunked, &indices, &self.dtype, context)?;
        drop((chunked, ownership, indices));
        self.order.build(compact, context)
    }

    pub(super) fn finish(
        mut self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        self.flush(context)?;
        self.order.finish(context, batch_rows, consume)
    }
}
