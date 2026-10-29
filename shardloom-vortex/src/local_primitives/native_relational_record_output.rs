//! Compact completed records before retaining them for first-seen output order.

use crate::{
    local_primitives::{
        native_relational_batch::failed,
        native_relational_output_buffer::OutputBuffer,
        native_relational_sort::Spec,
        native_relational_spill::{Ordering, State},
    },
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use vortex::array::{
    ArrayRef,
    dtype::{DType, Nullability},
};

pub(super) struct Results<'a> {
    order: Ordering<'a, 'a>,
    pending: OutputBuffer,
}

impl<'a> Results<'a> {
    pub(super) fn new(
        spec: &'a Spec,
        state: &'a State,
        batch_rows: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        let dtype = DType::struct_(spec.fields.clone(), Nullability::NonNullable);
        Ok(Self {
            order: Ordering::new(spec, state, batch_rows, context)?,
            pending: OutputBuffer::new(dtype, batch_rows, spec.copy_policy, context)?,
        })
    }

    pub(super) fn push(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        if array.len() != 1 {
            return Err(failed("completed native record changed its row count"));
        }
        self.pending.push(array, context, &mut |array| {
            self.order.build(array, context)
        })
    }

    pub(super) fn finish(
        mut self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        self.pending
            .flush(context, &mut |array| self.order.build(array, context))?;
        self.order.finish(context, batch_rows, consume)
    }
}
