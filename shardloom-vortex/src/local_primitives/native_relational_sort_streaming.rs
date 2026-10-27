//! A stable bounded prefix owns its payload before the next source batch.

use super::{ArrayRef, CopyPolicy, DType, NativeExecutionContext, Result, Sort, Spec, failed};
use crate::local_primitives::{native_payload, result_batch};

pub(in crate::local_primitives) struct StreamingPrefix<'a> {
    cap: usize,
    sort: Sort<'a>,
}

impl<'a> StreamingPrefix<'a> {
    pub(in crate::local_primitives) fn try_new(
        spec: &'a Spec,
        cap: Option<usize>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<Self>> {
        let Some(cap) = cap.filter(|cap| *cap <= 16_384) else {
            return Ok(None);
        };
        if spec.copy_policy != CopyPolicy::ValidateValues
            || !spec.fields.iter().all(|(_, dtype)| {
                !matches!(dtype, DType::Decimal(..)) && result_batch::width(dtype).is_ok()
            })
        {
            return Ok(None);
        }
        Ok(Some(Self {
            cap,
            sort: Sort::new(spec, context.memory())?,
        }))
    }

    /// Validate the complete incoming payload before observing ordering keys,
    /// matching streaming detachment. Previous ties precede current ties even
    /// after compaction, so the existing stable ordinal comparator is sufficient.
    pub(in crate::local_primitives) fn build(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<usize> {
        context.check_cancelled()?;
        let input_rows = array.len();
        let next_rows = self
            .sort
            .table
            .rows()
            .checked_add(input_rows)
            .ok_or_else(|| failed("streaming ordering row count overflow"))?;
        let copied = if next_rows <= self.cap * 4 {
            // Amortize compaction when a producer delivers tiny batches. Each
            // pending batch is independently owned; at most 4*K rows persist.
            let owned = native_payload::detach(&array, context)?;
            self.sort.build(owned, context)?;
            input_rows
        } else {
            native_payload::validate_flat_record(&array, &self.sort.spec.fields, context)?;
            self.sort.build(array, context)?;
            let ordinals = self.sort.ordered_prefix(context, Some(self.cap))?;
            let copied = ordinals.values.len();
            let retained = (copied != 0)
                .then(|| self.sort.gather_rows(&ordinals.values, context))
                .transpose()?;
            self.sort = Sort::new(self.sort.spec, context.memory())?;
            if let Some(retained) = retained {
                self.sort.build(retained, context)?;
            }
            copied
        };
        #[cfg(all(test, debug_assertions))]
        crate::local_primitives::native_relational_observation::record(|counts| {
            counts.sort_streaming_prefix_rows_validated += input_rows as u64;
            counts.sort_streaming_prefix_batches += 1;
            counts.sort_streaming_prefix_rows_copied += copied as u64;
            counts.sort_streaming_prefix_peak_rows = counts
                .sort_streaming_prefix_peak_rows
                .max(self.sort.table.rows() as u64);
        });
        // Only independently copied prefix/pending buffers survive this callback.
        context.check_cancelled()?;
        Ok(copied)
    }

    pub(in crate::local_primitives) fn finish(
        &self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        self.sort
            .finish_prefix(context, batch_rows, Some(self.cap), true, consume)
    }
}
