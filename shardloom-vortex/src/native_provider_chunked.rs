//! Own value/view and validity byte buffers of pinned native Chunked builders.
//!
//! Chunked execution uses `AppendChild`, which bypasses execute-parent kernels.
//! Fixed-width builders retain native child append strategies. For strings,
//! decode admitted leaves first, then use the native builder with compaction
//! disabled. It keeps their data buffers and only copies views and validity.

use super::*;
use vortex::array::{
    arrays::{BoolArray, chunked::ChunkedArrayExt as _, varbinview::VarBinViewArrayExt as _},
    builders::{AdmittedVarBinViewBuilder, VarBinViewBuilder},
};
use vortex::encodings::zstd::Zstd;

#[path = "native_provider_fixed_width_builder.rs"]
mod fixed_width;

pub(super) fn execute(
    _child: &ArrayRef,
    parent: &ArrayRef,
    _child_idx: usize,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<ArrayRef>> {
    if !parent.is::<Chunked>() {
        return Ok(None);
    }
    if parent.as_::<Chunked>().nchunks() == 0 {
        return Ok(None);
    }
    let fixed_width = matches!(
        parent.dtype(),
        DType::Primitive(..) | DType::Bool(_) | DType::Decimal(..)
    );
    if !fixed_width
        && (!matches!(parent.dtype(), DType::Utf8(_) | DType::Binary(_))
            || !admitted_tree(parent, 0)?)
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
    if fixed_width {
        fixed_width::decode(parent, ctx, &memory).map(Some)
    } else {
        decode(parent, ctx, &memory).map(Some)
    }
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
    // finish uses mem::take on its views BufferMut. The empty replacement
    // requests preferred alignment capacity until the builder is dropped.
    let finish_bytes = capacity(0)?;
    let mut views_credit =
        crate::owned_buffers::reserve(memory, add(add(views, validity)?, finish_bytes)?)?;
    let validity_credit = views_credit
        .split(validity)
        .map_err(|error| vortex_err!("{error}"))?;
    let finish_credit = views_credit
        .split(finish_bytes)
        .map_err(|error| vortex_err!("{error}"))?;
    let mut builder = VarBinViewBuilder::with_admitted_buffers(
        array.dtype().clone(),
        array.len(),
        &ctx.host_allocator(),
    )?;
    append_leaves(array, &mut builder, ctx, memory)?;
    let output = builder.finish()?;
    drop(finish_credit);
    let validity = match output.validity()? {
        Validity::Array(bitmap) => {
            let native = bitmap.as_::<Bool>();
            let buffer =
                crate::owned_buffers::retain_credit(bitmap.buffers()[0].clone(), validity_credit);
            Validity::Array(
                BoolArray::try_from_parts(
                    Bool.with_buffers(native, &[BufferHandle::new_host(buffer)])?,
                )?
                .into_array(),
            )
        }
        validity @ (Validity::NonNullable | Validity::AllValid | Validity::AllInvalid) => {
            drop(validity_credit);
            validity
        }
    };
    let views =
        crate::owned_buffers::retain_credit(output.views_handle().as_host().clone(), views_credit);
    let retained = output.with_views_and_validity(
        BufferHandle::new_host(views),
        output.dtype().clone(),
        validity,
    )?;
    retained.statistics().inherit_from(array.statistics());
    Ok(retained.into_array())
}

fn append_leaves(
    array: &ArrayRef,
    builder: &mut AdmittedVarBinViewBuilder,
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
        let decoded = decode_fsst(&fsst.into_owned(), ctx, memory)?;
        return builder.append_array(&decoded.as_::<VarBinView>().into_owned(), ctx);
    }
    if array.is::<Zstd>() {
        // Decode through the pinned allocator-aware provider before appending.
        // Its payload credit escapes into this builder; its temporary views
        // are released after the reserved output views have been populated.
        return builder.append_array(&array.clone().execute::<VarBinViewArray>(ctx)?, ctx);
    }
    vortex_ensure!(
        array.is::<VarBinView>(),
        "native string builder received an unadmitted leaf"
    );
    builder.append_array(&array.as_::<VarBinView>().into_owned(), ctx)
}
