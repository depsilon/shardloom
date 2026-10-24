//! Local selection ordinals keep their narrowest admitted native storage width.

use super::{
    ArrayRef, DType, NativeExecutionContext, ReservedVec, Result, failed, project_selected_batch,
    select_batch, vortex_error,
};
use crate::local_primitives::result_batch;
use vortex::array::{
    dtype::{Nullability, PType},
    memory::MemorySessionExt as _,
};
use vortex::mask::{AllOr, Mask};

enum Indices {
    Compact(ReservedVec<u16>),
    Wide(ReservedVec<usize>),
}

/// Width is fixed by the complete local source extent, never selected density.
/// All positions remain explicit, ordered and checked before insertion.
pub(super) struct LocalSelection {
    rows: Indices,
    source_rows: usize,
}

impl LocalSelection {
    pub(super) fn new(source_rows: usize, context: &NativeExecutionContext<'_>) -> Result<Self> {
        context.check_cancelled()?;
        let rows = if source_rows <= usize::from(u16::MAX) + 1 {
            let mut rows = ReservedVec::new(context.memory())?;
            rows.reserve(source_rows)?;
            #[cfg(all(test, feature = "vortex-write"))]
            if source_rows != 0 {
                super::selection_observation::owner(source_rows);
            }
            Indices::Compact(rows)
        } else {
            let mut rows = ReservedVec::new(context.memory())?;
            rows.reserve(source_rows)?;
            Indices::Wide(rows)
        };
        Ok(Self { rows, source_rows })
    }

    pub(super) fn from_mask(mask: &Mask, context: &NativeExecutionContext<'_>) -> Result<Self> {
        let mut selection = Self::new(mask.len(), context)?;
        match mask.bit_buffer() {
            AllOr::All => selection.extend(0..mask.len(), context)?,
            AllOr::None => {}
            AllOr::Some(bits) => selection.extend(bits.set_indices(), context)?,
        }
        Ok(selection)
    }

    fn extend(
        &mut self,
        rows: impl Iterator<Item = usize>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        for (index, row) in rows.enumerate() {
            if index.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            self.push(row)?;
        }
        context.check_cancelled()
    }

    pub(super) fn push(&mut self, row: usize) -> Result<()> {
        if row >= self.source_rows {
            return Err(failed("selection ordinal exceeds its local source extent"));
        }
        match &mut self.rows {
            Indices::Compact(rows) => rows.push(u16::try_from(row).map_err(vortex_error)?),
            Indices::Wide(rows) => rows.push(row),
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
        if array.len() != self.source_rows || batch_rows == 0 {
            return Err(failed(
                "selection source extent or output batch size changed",
            ));
        }
        match &self.rows {
            Indices::Compact(rows) => {
                for chunk in rows.values.chunks(batch_rows) {
                    consume(select_compact_batch(array, fields, chunk, context)?)?;
                }
            }
            Indices::Wide(rows) => {
                for chunk in rows.values.chunks(batch_rows) {
                    consume(select_batch(array, fields, chunk, context)?)?;
                }
            }
        }
        Ok(())
    }
}

fn select_compact_batch(
    array: &ArrayRef,
    fields: &[(String, DType)],
    rows: &[u16],
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    context.check_cancelled()?;
    let start = rows.first().copied().map_or(0, usize::from);
    let end = start.checked_add(rows.len());
    let mut contiguous = end.is_some();
    if contiguous {
        for (offset, row) in rows.iter().enumerate() {
            if offset.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if usize::from(*row) != start + offset {
                contiguous = false;
                break;
            }
        }
    }
    let selected = if let Some(end) = end.filter(|_| contiguous) {
        array.slice(start..end).map_err(vortex_error)?
    } else {
        let dtype = DType::Primitive(PType::U16, Nullability::NonNullable);
        let indices = result_batch::build_column(
            &dtype,
            rows.len(),
            &context.native_session().allocator(),
            |row| {
                if row.is_multiple_of(1024) {
                    context.check_cancelled()?;
                }
                Ok(result_batch::Value::UInt(u64::from(rows[row])))
            },
        )?;
        #[cfg(all(test, feature = "vortex-write"))]
        super::selection_observation::take(rows.len());
        array.take(indices).map_err(vortex_error)?
    };
    project_selected_batch(&selected, fields, context)
}

#[cfg(all(test, feature = "vortex-write"))]
#[path = "local_primitive_relational_compact_index_tests.rs"]
mod tests;
