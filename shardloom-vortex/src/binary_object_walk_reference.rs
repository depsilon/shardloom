//! Original logical descriptor traversal for shallow order and error comparison.

use super::*;
use vortex::array::{
    ExecutionCtx,
    arrays::{
        ExtensionArray, FixedSizeListArray, ListViewArray, StructArray, VarBinViewArray,
        extension::ExtensionArrayExt as _, fixed_size_list::FixedSizeListArrayExt as _,
        listview::ListViewArrayExt as _, struct_::StructArrayExt as _,
    },
};

pub(crate) fn visit(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    cancellation: &CancellationToken,
    callback: &mut impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    cancellation.check()?;
    if !crate::native_binary_type::reference::contains(array.dtype())
        .map_err(|cause| error("SCHEMA", cause))?
    {
        return Ok(());
    }
    let map = |cause| error("DESCRIPTOR", cause);
    match array.dtype() {
        DType::Extension(_) => {
            let extension = array.clone().execute::<ExtensionArray>(ctx).map_err(map)?;
            let storage = extension
                .storage_array()
                .clone()
                .execute::<VarBinViewArray>(ctx)
                .map_err(map)?;
            for index in 0..storage.len() {
                cancellation.check()?;
                if storage.is_valid(index, ctx).map_err(map)? {
                    callback(&storage.bytes_at(index))?;
                }
            }
        }
        DType::Struct(_, _) => {
            let structure = array.clone().execute::<StructArray>(ctx).map_err(map)?;
            for child in structure.iter_unmasked_fields() {
                if !crate::native_binary_type::reference::contains(child.dtype())
                    .map_err(|cause| error("SCHEMA", cause))?
                {
                    continue;
                }
                if array.dtype().is_nullable() {
                    for index in 0..array.len() {
                        cancellation.check()?;
                        if structure.is_valid(index, ctx).map_err(map)? {
                            visit(
                                &child.slice(index..index + 1).map_err(map)?,
                                ctx,
                                cancellation,
                                callback,
                            )?;
                        }
                    }
                } else {
                    visit(child, ctx, cancellation, callback)?;
                }
            }
        }
        DType::List(_, _) => {
            let list = array.clone().execute::<ListViewArray>(ctx).map_err(map)?;
            for index in 0..array.len() {
                cancellation.check()?;
                if list.is_valid(index, ctx).map_err(map)? {
                    visit(
                        &list.list_elements_at(index, ctx).map_err(map)?,
                        ctx,
                        cancellation,
                        callback,
                    )?;
                }
            }
        }
        DType::FixedSizeList(_, _, _) => {
            let list = array
                .clone()
                .execute::<FixedSizeListArray>(ctx)
                .map_err(map)?;
            for index in 0..array.len() {
                cancellation.check()?;
                if list.is_valid(index, ctx).map_err(map)? {
                    visit(
                        &list.fixed_size_list_elements_at(index).map_err(map)?,
                        ctx,
                        cancellation,
                        callback,
                    )?;
                }
            }
        }
        _ => return Err(error("DESCRIPTOR", "unsupported object column container")),
    }
    Ok(())
}
