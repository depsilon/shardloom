//! Own the pinned native string builder's views and validity allocations.
//!
//! Chunked execution uses `AppendChild`, which bypasses execute-parent kernels.
//! Decode admitted leaves first, then use the native builder with compaction
//! disabled. It keeps their data buffers and only copies views and validity.

use super::*;
use vortex::array::{
    arrays::{BoolArray, chunked::ChunkedArrayExt as _, varbinview::VarBinViewSlots},
    builders::VarBinViewBuilder,
};
use vortex::encodings::zstd::Zstd;

pub(super) fn execute(
    _child: &ArrayRef,
    parent: &ArrayRef,
    _child_idx: usize,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<ArrayRef>> {
    if !parent.is::<Chunked>()
        || !matches!(parent.dtype(), DType::Utf8(_) | DType::Binary(_))
        || !admitted_tree(parent, 0)?
    {
        return Ok(None);
    }
    let Some(memory) = ctx
        .session()
        .get_opt::<ProviderMemory>()
        .map(|v| v.memory.clone())
    else {
        return Ok(None);
    };
    decode(parent, ctx, &memory).map(Some)
}

fn admitted_tree(array: &ArrayRef, depth: usize) -> VortexResult<bool> {
    vortex_ensure!(
        depth < 24,
        "native string concatenation exceeds the depth bound"
    );
    if let Some(chunks) = array.as_opt::<Chunked>() {
        for child in chunks.iter_chunks() {
            if !admitted_tree(child, depth + 1)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    if !(array.is::<FSST>() || array.is::<Zstd>() || array.is::<VarBinView>()) || !array.is_host() {
        return Ok(false);
    }
    // The builder executes validity. Other boolean decoders need their own
    // admission before they can enter this finite concatenation boundary.
    Ok(match array.validity()? {
        Validity::Array(bitmap) => bitmap.is::<Bool>() && bitmap.is_host(),
        Validity::NonNullable | Validity::AllValid | Validity::AllInvalid => true,
    })
}

fn decode(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    memory: &LiveMemoryPool,
) -> VortexResult<ArrayRef> {
    // VarBinViewBuilder uses 16-byte alignment with no preferred over-alignment.
    // Its lazy null builder allocates at most one ceil(rows/8)-byte bit buffer.
    let views = aligned_capacity(mul(array.len(), 16)?, 16)?;
    let validity = if array.dtype().is_nullable() {
        capacity(u64::try_from(array.len().div_ceil(8)).map_err(|_| size_overflow())?)?
    } else {
        0
    };
    let mut views_credit = crate::owned_buffers::reserve(memory, add(views, validity)?)?;
    let validity_credit = views_credit
        .split(validity)
        .map_err(|error| vortex_err!("{error}"))?;
    let mut builder = VarBinViewBuilder::with_capacity(array.dtype().clone(), array.len());
    append_leaves(array, &mut builder, ctx, memory)?;
    let output = builder.finish_into_varbinview();
    let bitmap = match output.validity()? {
        Validity::Array(bitmap) => {
            let native = bitmap.as_::<Bool>();
            let buffer =
                crate::owned_buffers::retain_credit(bitmap.buffers()[0].clone(), validity_credit);
            Some(
                BoolArray::try_from_parts(
                    Bool.with_buffers(native, &[BufferHandle::new_host(buffer)])?,
                )?
                .into_array(),
            )
        }
        Validity::NonNullable | Validity::AllValid | Validity::AllInvalid => {
            drop(validity_credit);
            output.as_ref().slots()[0].clone()
        }
    };
    let mut buffers = output.as_ref().buffers();
    let views_index = buffers
        .len()
        .checked_sub(1)
        .ok_or_else(|| vortex_err!("native string builder omitted its views"))?;
    buffers[views_index] =
        crate::owned_buffers::retain_credit(buffers[views_index].clone(), views_credit);
    let handles: Vec<_> = buffers.into_iter().map(BufferHandle::new_host).collect();
    let retained = VarBinViewArray::try_from_parts(
        VarBinView
            .with_buffers(output.as_view(), &handles)?
            .with_slots(VarBinViewSlots { validity: bitmap }.into_slots()),
    )?;
    retained.statistics().inherit_from(array.statistics());
    Ok(retained.into_array())
}

fn append_leaves(
    array: &ArrayRef,
    builder: &mut VarBinViewBuilder,
    ctx: &mut ExecutionCtx,
    memory: &LiveMemoryPool,
) -> VortexResult<()> {
    if let Some(chunks) = array.as_opt::<Chunked>() {
        for child in chunks.iter_chunks() {
            append_leaves(child, builder, ctx, memory)?;
        }
        return Ok(());
    }
    if let Some(fsst) = array.as_opt::<FSST>() {
        return decode_fsst(&fsst.into_owned(), ctx, memory)?.append_to_builder(builder, ctx);
    }
    if array.is::<Zstd>() {
        // Decode through the pinned allocator-aware provider before appending.
        // Its payload credit escapes into this builder; its temporary views
        // are released after the reserved output views have been populated.
        return array
            .clone()
            .execute::<VarBinViewArray>(ctx)?
            .into_array()
            .append_to_builder(builder, ctx);
    }
    vortex_ensure!(
        array.is::<VarBinView>(),
        "native string builder received an unadmitted leaf"
    );
    array.append_to_builder(builder, ctx)
}
