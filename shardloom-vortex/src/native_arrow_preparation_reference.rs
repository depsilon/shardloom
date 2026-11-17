//! Original native preparation, retained for shallow differential tests.

use super::{ConversionLimits, charge, error};
use crate::local_primitives::{usize_to_u64, vortex_error};
use shardloom_core::Result;
use vortex::array::arrays::extension::ExtensionArrayExt as _;
use vortex::array::{
    ArrayRef, Columnar, ExecutionCtx, IntoArray,
    arrays::fixed_size_list::{FixedSizeListArrayExt as _, FixedSizeListArraySlotsExt as _},
    arrays::listview::{ListViewArrayExt as _, ListViewArraySlotsExt as _},
    arrays::struct_::StructArrayExt as _,
    arrays::{
        ExtensionArray, FixedSizeListArray, ListViewArray, StructArray, VarBin, VarBinViewArray,
    },
    dtype::DType,
};

#[path = "native_arrow_variant_reference.rs"]
pub(super) mod variant;

pub(crate) fn canonical(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    limits: &ConversionLimits<'_>,
    expanded: &mut u64,
) -> Result<ArrayRef> {
    limits.check()?;
    charge(
        expanded,
        usize_to_u64(array.len().div_ceil(8))? + 128,
        limits,
    )?;
    match array.dtype() {
        DType::Variant(_) => variant::prepare(array, ctx, limits, expanded, 0),
        DType::Extension(_)
            if crate::native_extensions::payload_storage(array.dtype()).is_some() =>
        {
            canonical_extension(array, ctx, limits, expanded)
        }
        DType::Struct(_, _) => {
            let structure = array
                .clone()
                .execute::<StructArray>(ctx)
                .map_err(vortex_error)?;
            let children = structure
                .iter_unmasked_fields()
                .map(|child| canonical(child, ctx, limits, expanded))
                .collect::<Result<Vec<_>>>()?;
            StructArray::try_new(
                structure.names().clone(),
                children,
                array.len(),
                structure.struct_validity(),
            )
            .map(IntoArray::into_array)
            .map_err(vortex_error)
        }
        DType::List(_, _) => {
            charge(
                expanded,
                usize_to_u64(array.len())?
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(4))
                    .ok_or_else(|| error("list offset overflow"))?,
                limits,
            )?;
            let list = array
                .clone()
                .execute::<ListViewArray>(ctx)
                .map_err(vortex_error)?;
            let elements = canonical(list.elements(), ctx, limits, expanded)?;
            ListViewArray::try_new(
                elements,
                list.offsets().clone(),
                list.sizes().clone(),
                list.listview_validity(),
                ctx,
            )
            .map(IntoArray::into_array)
            .map_err(vortex_error)
        }
        DType::FixedSizeList(_, _, _) => {
            let list = array
                .clone()
                .execute::<FixedSizeListArray>(ctx)
                .map_err(vortex_error)?;
            let elements = canonical(list.elements(), ctx, limits, expanded)?;
            FixedSizeListArray::try_new(
                elements,
                list.list_size(),
                list.fixed_size_list_validity(),
                array.len(),
            )
            .map(IntoArray::into_array)
            .map_err(vortex_error)
        }
        DType::Utf8(_) | DType::Binary(_) => canonical_text(array, ctx, limits, expanded),
        DType::Bool(_) | DType::Primitive(..) | DType::Decimal(..) | DType::Extension(_) => {
            canonical_fixed(array, ctx, limits, expanded)
        }
        _ => Err(error("canonical field changed its admitted dtype")),
    }
}

fn canonical_fixed(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    limits: &ConversionLimits<'_>,
    expanded: &mut u64,
) -> Result<ArrayRef> {
    let width = match array.dtype() {
        DType::Bool(_) => 1,
        DType::Primitive(ptype, _) => ptype.byte_width() as u64,
        DType::Decimal(decimal, _) if crate::native_payload_schema::admitted_decimal(*decimal) => {
            16
        }
        DType::Extension(_) => crate::native_payload_schema::temporal_storage(array.dtype())
            .ok_or_else(|| error("unsupported temporal Arrow output dtype"))?
            .byte_width() as u64,
        _ => return Err(error("unsupported fixed-width Arrow output dtype")),
    };
    charge(
        expanded,
        usize_to_u64(array.len())?
            .checked_mul(width)
            .ok_or_else(|| error("Arrow expansion overflow"))?,
        limits,
    )?;
    array
        .clone()
        .execute::<Columnar>(ctx)
        .map(IntoArray::into_array)
        .map_err(vortex_error)
}

fn canonical_extension(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    limits: &ConversionLimits<'_>,
    expanded: &mut u64,
) -> Result<ArrayRef> {
    let extension = array
        .clone()
        .execute::<ExtensionArray>(ctx)
        .map_err(vortex_error)?;
    let storage = canonical(extension.storage_array(), ctx, limits, expanded)?;
    ExtensionArray::try_new(extension.ext_dtype().clone(), storage)
        .map(IntoArray::into_array)
        .map_err(vortex_error)
}

fn canonical_text(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    limits: &ConversionLimits<'_>,
    expanded: &mut u64,
) -> Result<ArrayRef> {
    charge(
        expanded,
        usize_to_u64(array.len())?
            .checked_add(1)
            .and_then(|n| n.checked_mul(4))
            .ok_or_else(|| error("string offset overflow"))?,
        limits,
    )?;
    let text = array
        .clone()
        .execute::<VarBinViewArray>(ctx)
        .map_err(vortex_error)?;
    let before_values = *expanded;
    for (index, view) in text.views().iter().enumerate() {
        if index.is_multiple_of(1024) {
            limits.check()?;
        }
        let len = usize::try_from(view.len()).map_err(vortex_error)?;
        if len > limits.string_bytes {
            return Err(error("native string exceeds Arrow expansion admission"));
        }
        charge(expanded, usize_to_u64(len)?, limits)?;
    }
    // A complete contiguous byte array can share its offsets and payload with
    // Arrow. Vortex slices retain the entire bytes buffer, though, and the
    // provider's offset-array export keeps that buffer even with CompactBuffers.
    // In that case use the already validated views so its native builder copies
    // only the selected values. This keeps a small conversion slice from holding
    // unrelated large rows without increasing its resource admission.
    if let Some(contiguous) = array.as_opt::<VarBin>()
        && usize_to_u64(contiguous.bytes_handle().len())? <= *expanded - before_values
    {
        Ok(array.clone())
    } else {
        Ok(text.into_array())
    }
}
