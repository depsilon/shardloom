//! Normalize record containers without evaluating their encoded fields. These
//! temporary source views are admitted before construction and never escape
//! the receiving copy. Empty ranges retain schema without copying unused data.

use super::{MAX_ENCODING_DEPTH, add, memory_error, mul, native_error};
use crate::{
    local_primitives::native_capacity::ReservedVec, resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::MemoryLease;
use std::{ops::Range, sync::Arc};
use vortex::array::{
    ArrayRef, Canonical, IntoArray,
    arrays::{
        Chunked, Slice, SliceArray, Struct, StructArray, chunked::ChunkedArrayExt as _,
        slice::SliceArraySlotsExt as _, struct_::StructArraySlotsExt as _,
    },
    dtype::DType,
    validity::Validity,
};

pub(super) struct RecordViews {
    pub(super) roots: ReservedVec<ArrayRef>,
    metadata: MemoryLease,
}

pub(super) fn collect(
    arrays: &[ArrayRef],
    context: &NativeExecutionContext<'_>,
) -> Result<Option<RecordViews>> {
    let mut views = RecordViews {
        roots: ReservedVec::new(context.memory())?,
        metadata: context.memory().reserve(0)?,
    };
    for array in arrays {
        if !views.visit(array, 0..array.len(), 0, context)? {
            return Ok(None);
        }
    }
    Ok(Some(views))
}

impl RecordViews {
    fn push_slice_range(&mut self, structure: &StructArray, range: Range<usize>) -> Result<()> {
        let count = structure.fields().len() as u64;
        self.metadata
            .resize(add(self.metadata.bytes(), add(4096, mul(count, 1024)?)?)?)?;
        let fields = structure
            .fields()
            .iter()
            .map(|field| {
                SliceArray::try_new(field.clone(), range.clone())
                    .map(IntoArray::into_array)
                    .map_err(native_error)
            })
            .collect::<Result<Vec<_>>>()?;
        let view = StructArray::try_new(
            structure.dtype().as_struct_fields().names().clone(),
            fields,
            range.len(),
            Validity::NonNullable,
        )
        .map_err(native_error)?
        .into_array();
        self.roots.push(view)
    }

    fn visit(
        &mut self,
        root: &ArrayRef,
        initial_range: Range<usize>,
        initial_depth: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<bool> {
        let mut stack = vec![(root.clone(), initial_range, initial_depth)];
        while let Some((array, range, depth)) = stack.pop() {
            context.check_cancelled()?;
            if depth > MAX_ENCODING_DEPTH || range.start > range.end || range.end > array.len() {
                return Err(memory_error(
                    "native record view exceeds its depth or row bounds",
                ));
            }
            if !matches!(array.dtype(), DType::Struct(_, _)) || array.dtype().is_nullable() {
                return Err(memory_error(
                    "native transfer requires nonnullable record rows",
                ));
            }
            if let Some(structure) = array.as_opt::<Struct>() {
                if range == (0..array.len()) {
                    self.roots.push(array)?;
                    continue;
                }
                if !range.is_empty() {
                    self.push_slice_range(&structure.into_owned(), range)?;
                    continue;
                }
            }
            if range.is_empty() {
                let credit = crate::native_payload_schema::reserve_encoded_schema(
                    array.dtype(),
                    context.memory(),
                )?;
                self.roots.push(
                    Canonical::empty(array.dtype())
                        .into_array()
                        .with_retained_owner(Arc::new(credit))
                        .map_err(native_error)?,
                )?;
                continue;
            }
            if let Some(slice) = array.as_opt::<Slice>() {
                let parent = slice.slice_range();
                if parent.start > parent.end {
                    return Err(memory_error("native record slice has reversed coordinates"));
                }
                let start = parent
                    .start
                    .checked_add(range.start)
                    .ok_or_else(|| memory_error("native record slice coordinate overflow"))?;
                let end = parent
                    .start
                    .checked_add(range.end)
                    .ok_or_else(|| memory_error("native record slice coordinate overflow"))?;
                stack.push((slice.child().clone(), start..end, depth + 1));
                continue;
            }
            let Some(chunked) = array.as_opt::<Chunked>() else {
                // Other record encodings retain the preclassified native materializer.
                return Ok(false);
            };
            let mut offset = 0usize;
            let base = stack.len();
            for index in 0..chunked.nchunks() {
                context.check_cancelled()?;
                let chunk = chunked.chunk(index);
                let end = offset
                    .checked_add(chunk.len())
                    .ok_or_else(|| memory_error("native record chunk coordinate overflow"))?;
                let start = range.start.max(offset);
                let stop = range.end.min(end);
                if start < stop {
                    stack.push((chunk.clone(), start - offset..stop - offset, depth + 1));
                }
                offset = end;
            }
            if offset != array.len() {
                return Err(memory_error(
                    "native record chunks changed their declared length",
                ));
            }
            stack[base..].reverse();
        }
        Ok(true)
    }
}
