//! Inspect native Variant storage before the explicit Arrow conversion grant.

use super::{ConversionLimits, canonical, charge, error};
use crate::local_primitives::{usize_to_u64, vortex_error};
use shardloom_core::Result;
use vortex::{
    array::{
        ArrayRef, ArrayView, ExecutionCtx, IntoArray,
        arrays::chunked::ChunkedArrayExt as _,
        arrays::listview::{ListViewArrayExt as _, ListViewArraySlotsExt as _},
        arrays::masked::{MaskedArrayExt as _, MaskedArraySlotsExt as _},
        arrays::struct_::StructArrayExt as _,
        arrays::variant::VariantArraySlotsExt as _,
        arrays::{
            Chunked, ChunkedArray, Constant, ListArray, ListView, Masked, MaskedArray,
            PrimitiveArray, Struct, StructArray, Variant, VariantArray,
        },
        matcher::Matcher,
        memory::HostMemorySessionExt as _,
        validity::Validity,
    },
    buffer::{Alignment, Buffer},
    encodings::parquet_variant::{
        ParquetVariant, ParquetVariantArrayExt as _, ParquetVariantArraySlotsExt as _,
    },
};

struct Storage;
impl Matcher for Storage {
    type Match<'a> = &'a ArrayRef;
    fn try_match(array: &ArrayRef) -> Option<Self::Match<'_>> {
        (array.is::<ParquetVariant>()
            || array.is::<Variant>()
            || array.is::<Chunked>()
            || array.is::<Masked>()
            || array.is::<Constant>())
        .then_some(array)
    }
}

pub(super) fn prepare(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    limits: &ConversionLimits<'_>,
    expanded: &mut u64,
    depth: usize,
) -> Result<ArrayRef> {
    limits.check()?;
    if depth > 64 {
        return Err(error("Variant wrapper depth exceeds Arrow admission"));
    }
    let array = array
        .clone()
        .execute_until::<Storage>(ctx)
        .map_err(vortex_error)?;
    if let Some(storage) = array.as_opt::<ParquetVariant>() {
        return prepare_parquet(storage, ctx, limits, expanded);
    }
    if let Some(chunks) = array.as_opt::<Chunked>() {
        charge(
            expanded,
            (chunks.nchunks() as u64)
                .checked_mul(1024)
                .ok_or_else(|| error("Variant chunk metadata overflow"))?,
            limits,
        )?;
        let chunks = chunks
            .iter_chunks()
            .map(|chunk| prepare(chunk, ctx, limits, expanded, depth + 1))
            .collect::<Result<Vec<_>>>()?;
        return ChunkedArray::try_new(chunks, array.dtype().clone())
            .map(IntoArray::into_array)
            .map_err(vortex_error);
    }
    if let Some(variant) = array.as_opt::<Variant>() {
        let core = prepare(variant.core_storage(), ctx, limits, expanded, depth + 1)?;
        let before = *expanded;
        let shredded = variant
            .shredded()
            .map(|child| {
                let child = canonical(child, ctx, limits, expanded)?;
                logical_shredded(&child, ctx, limits, expanded)
            })
            .transpose()?;
        if let Some(shredded) = &shredded {
            let framing = usize_to_u64(array.len())?
                .checked_mul(
                    names(shredded.dtype())?
                        .checked_add(64)
                        .ok_or_else(|| error("Variant expansion overflow"))?,
                )
                .and_then(|bytes| bytes.checked_add((*expanded - before).checked_mul(8)?))
                .ok_or_else(|| error("Variant expansion overflow"))?;
            charge(expanded, framing, limits)?;
        }
        return VariantArray::try_new(core, shredded)
            .map(IntoArray::into_array)
            .map_err(vortex_error);
    }
    if let Some(masked) = array.as_opt::<Masked>() {
        let child = prepare(masked.child(), ctx, limits, expanded, depth + 1)?;
        return MaskedArray::try_new(child, masked.masked_validity(), ctx)
            .map(IntoArray::into_array)
            .map_err(vortex_error);
    }
    if let Some(constant) = array.as_opt::<Constant>() {
        let scalar = constant.scalar();
        if scalar.is_null() || scalar.as_variant().is_variant_null() == Some(true) {
            charge(
                expanded,
                usize_to_u64(array.len())?
                    .checked_mul(64)
                    .ok_or_else(|| error("Variant null expansion overflow"))?,
                limits,
            )?;
            return Ok(array);
        }
    }
    Err(error(
        "Variant export requires admitted Parquet Variant storage",
    ))
}

fn prepare_parquet(
    storage: ArrayView<'_, ParquetVariant>,
    ctx: &mut ExecutionCtx,
    limits: &ConversionLimits<'_>,
    expanded: &mut u64,
) -> Result<ArrayRef> {
    let metadata = canonical(storage.metadata(), ctx, limits, expanded)?;
    let value = storage
        .value()
        .map(|value| canonical(value, ctx, limits, expanded))
        .transpose()?;
    let before = *expanded;
    let typed = storage
        .typed_value()
        .map(|value| canonical(value, ctx, limits, expanded))
        .transpose()?;
    // Unshredding expands typed leaves and repeats schema field names in
    // each row's metadata. Admit that terminal conversion before export.
    let names = typed.as_ref().map_or(Ok(0), |typed| names(typed.dtype()))?;
    let framing = usize_to_u64(storage.len())?
        .checked_mul(
            names
                .checked_add(64)
                .ok_or_else(|| error("Variant expansion overflow"))?,
        )
        .and_then(|bytes| bytes.checked_add((*expanded - before).checked_mul(8)?))
        .ok_or_else(|| error("Variant expansion overflow"))?;
    charge(expanded, framing, limits)?;
    for row in 0..storage.len() {
        limits.check()?;
        if !storage
            .parquet_variant_validity()
            .execute_is_valid(row, ctx)
            .map_err(vortex_error)?
        {
            continue;
        }
        let metadata = storage
            .metadata()
            .execute_scalar(row, ctx)
            .map_err(vortex_error)?;
        let metadata = metadata
            .as_binary()
            .value()
            .ok_or_else(|| error("Variant metadata is null"))?;
        let value = storage
            .value()
            .map(|value| value.execute_scalar(row, ctx).map_err(vortex_error))
            .transpose()?;
        if let Some(raw) = value.as_ref().and_then(|value| value.as_binary().value()) {
            let bytes = metadata
                .len()
                .checked_add(raw.len())
                .ok_or_else(|| error("Variant validation overflow"))?;
            let workspace =
                usize::try_from(crate::native_variant::workspace(bytes)?).map_err(vortex_error)?;
            let _credit = ctx
                .session()
                .host_allocator()
                .reserve_external(workspace)
                .map_err(vortex_error)?;
            crate::native_variant::value(metadata.as_slice(), raw.as_slice(), limits.check)?;
        }
    }
    ParquetVariant::try_new(storage.parquet_variant_validity(), metadata, value, typed)
        .map(IntoArray::into_array)
        .map_err(vortex_error)
}

/// The pinned Variant provider reconstructs Parquet field shells from native
/// List/Struct encodings. Native selection produces compact `ListView` arrays instead.
/// Reframe their already contiguous coordinates with the same child owner;
/// do not decode elements or invoke the provider's generic Variant builder.
fn logical_shredded(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    limits: &ConversionLimits<'_>,
    expanded: &mut u64,
) -> Result<ArrayRef> {
    limits.check()?;
    if let Some(list) = array.as_opt::<ListView>() {
        let rows = array
            .len()
            .checked_add(1)
            .ok_or_else(|| error("Variant list size overflow"))?;
        let bytes = rows
            .checked_mul(size_of::<u64>())
            .ok_or_else(|| error("Variant list size overflow"))?;
        charge(expanded, usize_to_u64(bytes)?, limits)?;
        let mut buffer = ctx
            .session()
            .host_allocator()
            .allocate(bytes, Alignment::of::<u64>())
            .map_err(vortex_error)?;
        let offsets = buffer.as_mut_slice_typed::<u64>().map_err(vortex_error)?;
        let mut end = None;
        for (row, slot) in offsets[..array.len()].iter_mut().enumerate() {
            limits.check()?;
            let start = list
                .offsets()
                .execute_scalar(row, ctx)
                .map_err(vortex_error)?
                .as_primitive()
                .as_::<u64>()
                .ok_or_else(|| error("invalid Variant list offset"))?;
            let size = list
                .sizes()
                .execute_scalar(row, ctx)
                .map_err(vortex_error)?
                .as_primitive()
                .as_::<u64>()
                .ok_or_else(|| error("invalid Variant list size"))?;
            if end.is_some_and(|end| end != start) {
                return Err(error(
                    "canonical Variant export requires compact selected lists",
                ));
            }
            *slot = start;
            end = Some(
                start
                    .checked_add(size)
                    .ok_or_else(|| error("Variant list offset overflow"))?,
            );
        }
        offsets[array.len()] = end.unwrap_or(0);
        let offsets = PrimitiveArray::new(
            Buffer::<u64>::from_byte_buffer(buffer.freeze()),
            Validity::NonNullable,
        )
        .into_array();
        let elements = logical_shredded(list.elements(), ctx, limits, expanded)?;
        return ListArray::try_new(elements, offsets, list.listview_validity(), ctx)
            .map(IntoArray::into_array)
            .map_err(vortex_error);
    }
    if let Some(structure) = array.as_opt::<Struct>() {
        let children = structure
            .iter_unmasked_fields()
            .map(|child| logical_shredded(child, ctx, limits, expanded))
            .collect::<Result<Vec<_>>>()?;
        return StructArray::try_new(
            structure.names().clone(),
            children,
            array.len(),
            structure.struct_validity(),
        )
        .map(IntoArray::into_array)
        .map_err(vortex_error);
    }
    Ok(array.clone())
}

fn names(dtype: &vortex::array::dtype::DType) -> Result<u64> {
    use vortex::array::dtype::DType;
    match dtype {
        DType::Struct(fields, _) => {
            fields
                .names()
                .iter()
                .zip(fields.fields())
                .try_fold(0_u64, |size, (name, child)| {
                    let child_names = names(&child)?;
                    size.checked_add(name.as_ref().len() as u64)
                        .and_then(|size| size.checked_add(16))
                        .and_then(|size| size.checked_add(child_names))
                        .ok_or_else(|| error("Variant metadata expansion overflow"))
                })
        }
        DType::List(child, _) | DType::FixedSizeList(child, _, _) => names(child),
        _ => Ok(0),
    }
}
