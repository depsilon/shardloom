//! Share adaptive output compaction when native producers emit small arrays.

use super::{DType, NativeExecutionContext, Result, sink_error};
use crate::local_primitives::native_relational_output_buffer::OutputBuffer;
use vortex::array::ArrayRef;

const ROWS: usize = 128;
// Pending fragments, incoming array, compact replacement and the writer's
// existing three-array bound can overlap during a flush.
pub(super) const INPUT_ARRAY_BOUND: usize = ROWS + 5;

pub(super) struct Batches<'a> {
    dtype: &'a DType,
    output: Option<OutputBuffer>,
}

impl<'a> Batches<'a> {
    pub(super) fn new(
        dtype: &'a DType,
        produced: bool,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Self> {
        // Native schemas outside the bounded copier's admission retain their
        // existing pass-through path. Batching does not narrow sink support.
        let output = if produced && crate::native_payload_schema::schema_bytes(dtype).is_ok() {
            Some(OutputBuffer::native_writer(dtype.clone(), ROWS, context)?)
        } else {
            None
        };
        Ok(Self { dtype, output })
    }

    pub(super) fn copy_counts(&self) -> (u64, u64) {
        self.output
            .as_ref()
            .map_or((0, 0), OutputBuffer::copy_counts)
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
        if let Some(output) = &mut self.output {
            output.push(array, context, consume)
        } else {
            consume(array)?;
            context.check_cancelled()
        }
    }

    pub(super) fn flush(
        &mut self,
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        context.check_cancelled()?;
        if let Some(output) = &mut self.output {
            output.flush(context, consume)?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "local_primitive_native_sink_batches_tests.rs"]
mod tests;
