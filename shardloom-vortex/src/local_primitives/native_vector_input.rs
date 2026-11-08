//! Selected native spans, with coordinate and decode credits held by their owners.

use super::{ArrayRef, NativeExecutionContext, Result, check, failed, result_batch, vortex_error};
use crate::local_primitives::{NativeNumericOwner, native_list};
use shardloom_exec::live_memory::MemoryLease;
use vortex::array::{
    VortexSessionExecute as _,
    arrays::{ExtensionArray, PrimitiveArray, extension::ExtensionArrayExt as _},
    dtype::DType,
};

pub(super) fn storage(array: &ArrayRef, context: &NativeExecutionContext<'_>) -> Result<ArrayRef> {
    let DType::Extension(extension) = array.dtype() else {
        return Ok(array.clone());
    };
    context.check_cancelled()?;
    let storage = array
        .clone()
        .execute::<ExtensionArray>(&mut context.native_session().create_execution_ctx())
        .map_err(vortex_error)?
        .storage_array()
        .clone();
    if storage.len() != array.len() || storage.dtype() != extension.storage_dtype() {
        return Err(failed(
            "vector extension execution changed its storage domain",
        ));
    }
    Ok(storage)
}

pub(super) struct List {
    pub(super) elements: ArrayRef,
    coordinates: native_list::Coordinates,
    span: Option<Span>,
    _coordinates_credit: MemoryLease,
}

pub(super) struct Span {
    pub(super) owner: NativeNumericOwner,
    pub(super) has_null: bool,
    start: usize,
    count: usize,
    // Owners are destroyed before the credit that admitted their expansion.
    _credit: MemoryLease,
}

impl List {
    pub(super) fn new(array: &ArrayRef, context: &NativeExecutionContext<'_>) -> Result<Self> {
        let bytes = array
            .len()
            .checked_mul(64)
            .and_then(|n| n.checked_add(4096))
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(|| failed("vector coordinate admission overflow"))?;
        let credit = context.memory().reserve(bytes)?;
        let array = storage(array, context)?;
        let mut execution = context.native_session().create_execution_ctx();
        let (elements, coordinates) =
            native_list::Column::new(array, &mut execution)?.into_indexed(&mut execution)?;
        context.check_cancelled()?;
        Ok(Self {
            elements,
            coordinates,
            span: None,
            _coordinates_credit: credit,
        })
    }

    pub(super) fn at(&self, row: usize) -> Result<Option<(usize, usize)>> {
        self.coordinates.at(row)
    }

    pub(super) fn prepare(
        &mut self,
        start: usize,
        count: usize,
        context: &NativeExecutionContext<'_>,
    ) -> Result<&Span> {
        context.check_cancelled()?;
        if self
            .span
            .as_ref()
            .is_some_and(|span| span.start == start && span.count == count)
        {
            return Ok(self.span.as_ref().expect("matched span"));
        }
        // No widened whole-child cache. Reuse is restricted to one identical
        // immutable native span; release it before admitting its replacement.
        self.span = None;
        let end = start
            .checked_add(count)
            .filter(|end| *end <= self.elements.len())
            .ok_or_else(|| failed("vector component coordinates exceed their child"))?;
        let bytes = result_batch::width(self.elements.dtype())?
            .checked_add(1)
            .and_then(|width| count.checked_mul(width))
            .and_then(|n| n.checked_add(4096))
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(|| failed("vector component admission overflow"))?;
        let credit = context.memory().reserve(bytes)?;
        let mut execution = context.native_session().create_execution_ctx();
        let primitive = self
            .elements
            .slice(start..end)
            .map_err(vortex_error)?
            .execute::<PrimitiveArray>(&mut execution)
            .map_err(vortex_error)?;
        if primitive.len() != count || primitive.dtype() != self.elements.dtype() {
            return Err(failed("vector component execution changed its domain"));
        }
        let owner = NativeNumericOwner::new_with_float_policy(
            primitive,
            &mut execution,
            context.float_policy(),
        )?;
        let mut has_null = false;
        for index in 0..count {
            check(index, context)?;
            has_null |= owner.is_null(index)?;
        }
        self.span = Some(Span {
            owner,
            has_null,
            start,
            count,
            _credit: credit,
        });
        Ok(self.span.as_ref().expect("prepared span"))
    }
}
