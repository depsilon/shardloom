//! Visit selected logical descriptor cells in field and row order.

use super::{ArrayRef, CancellationToken, DType, LiveMemoryPool, Result, error};
use crate::{local_primitives::native_capacity::ReservedVec, native_payload_schema::Scope};
use vortex::{
    array::{
        ExecutionCtx,
        arrays::{
            ExtensionArray, FixedSizeListArray, ListViewArray, StructArray, VarBinViewArray,
            extension::ExtensionArrayExt as _, fixed_size_list::FixedSizeListArrayExt as _,
            listview::ListViewArrayExt as _, struct_::StructArrayExt as _,
        },
    },
    error::{VortexError, VortexResult},
};

enum Children {
    Struct {
        structure: StructArray,
        field: usize,
        // Set while visiting the valid rows of a classified nullable field.
        row: Option<usize>,
    },
    List(ListViewArray, usize),
    Fixed(FixedSizeListArray, usize),
}

struct Parent {
    children: Children,
    // Provider execution may replace the physical array. Retain the original
    // input until the canonical parent's children have finished.
    source: ArrayRef,
}

struct Parents(ReservedVec<Parent>);

impl Drop for Parents {
    fn drop(&mut self) {
        // Vec drops from the front, also when unwinding through a destructor.
        self.0.values.reverse();
    }
}

fn descriptor(cause: VortexError) -> shardloom_core::ShardLoomError {
    error("DESCRIPTOR", cause)
}

fn contains(dtype: &DType, scope: Scope<'_>) -> Result<bool> {
    crate::native_binary_type::contains(dtype, scope, |cause| error("SCHEMA", cause))
}

/// A null ancestor and unreachable list elements never select a payload.
pub(crate) fn visit(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
    callback: &mut impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let scope = (memory, cancellation).into();
    let mut parents = Parents(ReservedVec::new(memory)?);
    let mut next = Some(array.clone());
    loop {
        let mut parent = if let Some(array) = next.take() {
            cancellation.check()?;
            if !contains(array.dtype(), scope)? {
                continue;
            }
            let Some(children) = children(&array, ctx, cancellation, callback)? else {
                continue;
            };
            Parent {
                children,
                source: array,
            }
        } else if let Some(parent) = parents.0.values.pop() {
            parent
        } else {
            return Ok(());
        };
        if let Some(child) = parent.next(ctx, cancellation, scope)? {
            parents.0.reserve_one()?;
            parents.0.values.push(parent);
            next = Some(child);
        }
    }
}

fn children(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    cancellation: &CancellationToken,
    callback: &mut impl FnMut(&[u8]) -> Result<()>,
) -> Result<Option<Children>> {
    Ok(Some(match array.dtype() {
        DType::Extension(_) => {
            let extension = array
                .clone()
                .execute::<ExtensionArray>(ctx)
                .map_err(descriptor)?;
            let storage = extension
                .storage_array()
                .clone()
                .execute::<VarBinViewArray>(ctx)
                .map_err(descriptor)?;
            for index in 0..storage.len() {
                cancellation.check()?;
                if storage.is_valid(index, ctx).map_err(descriptor)? {
                    callback(&storage.bytes_at(index))?;
                }
            }
            return Ok(None);
        }
        DType::Struct(..) => Children::Struct {
            structure: array
                .clone()
                .execute::<StructArray>(ctx)
                .map_err(descriptor)?,
            field: 0,
            row: None,
        },
        DType::List(..) => Children::List(
            array
                .clone()
                .execute::<ListViewArray>(ctx)
                .map_err(descriptor)?,
            0,
        ),
        DType::FixedSizeList(..) => Children::Fixed(
            array
                .clone()
                .execute::<FixedSizeListArray>(ctx)
                .map_err(descriptor)?,
            0,
        ),
        _ => return Err(error("DESCRIPTOR", "unsupported object column container")),
    }))
}

fn next_valid(
    row: &mut usize,
    len: usize,
    cancellation: &CancellationToken,
    mut valid: impl FnMut(usize) -> VortexResult<bool>,
) -> Result<Option<usize>> {
    while *row < len {
        cancellation.check()?;
        let index = *row;
        *row += 1;
        if valid(index).map_err(descriptor)? {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

impl Parent {
    fn next(
        &mut self,
        ctx: &mut ExecutionCtx,
        cancellation: &CancellationToken,
        scope: Scope<'_>,
    ) -> Result<Option<ArrayRef>> {
        let len = self.source.len();
        match &mut self.children {
            Children::Struct {
                structure,
                field,
                row,
            } => {
                while let Some(child) = structure.unmasked_field_opt(*field) {
                    if row.is_none() {
                        if !contains(child.dtype(), scope)? {
                            *field += 1;
                            continue;
                        }
                        if !self.source.dtype().is_nullable() {
                            *field += 1;
                            return Ok(Some(child.clone()));
                        }
                    }
                    if let Some(index) = next_valid(row.get_or_insert(0), len, cancellation, |i| {
                        structure.is_valid(i, ctx)
                    })? {
                        return child.slice(index..index + 1).map(Some).map_err(descriptor);
                    }
                    *field += 1;
                    *row = None;
                }
                Ok(None)
            }
            Children::List(list, row) => {
                next_valid(row, len, cancellation, |i| list.is_valid(i, ctx))?
                    .map(|i| list.list_elements_at(i, ctx).map_err(descriptor))
                    .transpose()
            }
            Children::Fixed(list, row) => {
                next_valid(row, len, cancellation, |i| list.is_valid(i, ctx))?
                    .map(|i| list.fixed_size_list_elements_at(i).map_err(descriptor))
                    .transpose()
            }
        }
    }
}
