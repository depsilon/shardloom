//! Preserve contiguous membership as a native view until a gap needs indices.

use super::{
    ArrayRef, DType, NativeExecutionContext, ReservedVec, Result, failed, select_batch,
    select_range,
};
use std::ops::Range;

/// Increasing, unique membership. Ordered/repeated gathers use `select_batch`.
pub(super) enum RowSelection {
    Range(Range<usize>),
    Indices(ReservedVec<usize>),
}

impl RowSelection {
    pub(super) fn matching(
        input: Range<usize>,
        context: &NativeExecutionContext<'_>,
        mut matches: impl FnMut(usize) -> Result<bool>,
    ) -> Result<Self> {
        context.check_cancelled()?;
        if input.start > input.end {
            return Err(failed("native selection has an inverted input range"));
        }
        let mut selected = Self::Range(0..0);
        for row in input.clone() {
            if (row - input.start).is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if !matches(row)? {
                continue;
            }
            match &mut selected {
                // Iteration guarantees row < input.end, including wide ranges,
                // so the exclusive endpoint cannot overflow.
                Self::Range(range) if range.start == range.end => *range = row..row + 1,
                Self::Range(range) if range.end == row => range.end = row + 1,
                Self::Range(range) => {
                    let mut indices = ReservedVec::new(context.memory())?;
                    // Reserve once for the largest remaining selection. This
                    // never exceeds the old full-input reservation and avoids
                    // a growth overlap denying a previously admitted dense case.
                    let capacity = range
                        .len()
                        .checked_add(input.end - row)
                        .ok_or_else(|| failed("native selection capacity overflow"))?;
                    indices.reserve(capacity)?;
                    for (offset, prior) in range.clone().enumerate() {
                        if offset.is_multiple_of(1024) {
                            context.check_cancelled()?;
                        }
                        indices.values.push(prior);
                    }
                    indices.values.push(row);
                    selected = Self::Indices(indices);
                }
                Self::Indices(indices) => indices.values.push(row),
            }
        }
        context.check_cancelled()?;
        Ok(selected)
    }

    pub(super) fn is_empty(&self) -> bool {
        match self {
            Self::Range(range) => range.start >= range.end,
            Self::Indices(indices) => indices.values.is_empty(),
        }
    }

    pub(super) fn select(
        &self,
        array: &ArrayRef,
        fields: &[(String, DType)],
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        match self {
            Self::Range(range) => select_range(array, fields, range.clone(), context),
            Self::Indices(indices) => select_batch(array, fields, &indices.values, context),
        }
    }

    pub(super) fn for_each_batch(
        &self,
        array: &ArrayRef,
        fields: &[(String, DType)],
        batch_rows: usize,
        context: &NativeExecutionContext<'_>,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        context.check_cancelled()?;
        if batch_rows == 0 {
            return Err(failed("native selection requires a positive batch size"));
        }
        match self {
            Self::Range(range) => {
                for start in range.clone().step_by(batch_rows) {
                    let end = start.saturating_add(batch_rows).min(range.end);
                    consume(select_range(array, fields, start..end, context)?)?;
                }
            }
            Self::Indices(indices) => {
                for rows in indices.values.chunks(batch_rows) {
                    consume(select_batch(array, fields, rows, context)?)?;
                }
            }
        }
        context.check_cancelled()
    }
}
