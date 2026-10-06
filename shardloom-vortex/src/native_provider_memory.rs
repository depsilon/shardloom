//! Admission for the pinned native FSST canonicalization provider.
//!
//! The session kernel wraps the existing decoder, retaining its native buffers
//! without copying bytes. This covers direct FSST execution and the finite
//! integer metadata providers below, plus concatenation of FSST, Zstd and
//! canonical string views. Other builder paths, unrelated providers, allocator bookkeeping
//! and process RSS are separate resource boundaries.

use std::{any::Any, sync::Arc};

use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::{
    array::{
        Array, ArrayParts, ArrayRef, ArraySlots, ArrayVTable, ArrayView, ExecutionCtx,
        ExecutionStep, IntoArray,
        arrays::{
            Bool, Chunked, Constant, Dict, Filter, Masked, Primitive, PrimitiveArray, ScalarFn,
            Slice, VarBinView, VarBinViewArray,
            dict::DictArraySlotsExt as _,
            filter::FilterArraySlotsExt as _,
            masked::{MaskedArrayExt as _, MaskedArraySlotsExt as _},
            scalar_fn::ScalarFnArrayExt as _,
        },
        buffer::BufferHandle,
        dtype::DType,
        match_each_integer_ptype,
        optimizer::kernels::{ArrayKernelsExt as _, ExecuteParentFn},
        scalar_fn::{
            ScalarFnVTable as _,
            fns::{cast::Cast, fill_null::FillNull},
        },
        session::ArraySessionExt as _,
        validity::Validity,
    },
    buffer::{Alignment, BufferMut},
    encodings::{
        fastlanes::{
            BitPacked, Delta, DeltaArraySlotsExt as _, FoR, RLE, RLEArrayExt as _,
            RLEArraySlotsExt as _,
        },
        fsst::{FSST, FSSTArray, FSSTArraySlotsExt as _, FSSTSlots},
        runend::RunEnd,
        sequence::Sequence,
        sparse::{Sparse, SparseArraySlotsExt as _, SparseExt as _},
        zigzag::ZigZag,
    },
    error::{VortexResult, vortex_bail, vortex_ensure, vortex_err},
    session::{SessionExt as _, SessionVar, VortexSession},
};

#[path = "native_provider_chunked.rs"]
mod chunked;

#[cfg(test)]
#[path = "native_provider_zstd_tests.rs"]
mod zstd_tests;

#[derive(Clone, Debug)]
pub(crate) struct ProviderMemory {
    memory: LiveMemoryPool,
}

impl SessionVar for ProviderMemory {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Install before constructing execution contexts, which snapshot the registry.
/// An independent spill session inherits the registered kernels and this marker,
/// then replaces only its memory owner.
pub(crate) fn install(session: &VortexSession, memory: LiveMemoryPool) {
    if session.get_opt::<ProviderMemory>().is_none() {
        session.arrays().registry().read(|arrays| {
            for id in arrays.keys() {
                session.kernels().register_execute_parent(
                    FSST.id(),
                    *id,
                    &[execute_fsst as ExecuteParentFn],
                );
                session.kernels().register_execute_parent(
                    Chunked.id(),
                    *id,
                    &[chunked::execute as ExecuteParentFn],
                );
            }
        });
    }
    session.register(ProviderMemory { memory });
}

fn execute_fsst(
    _child: &ArrayRef,
    parent: &ArrayRef,
    _child_idx: usize,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<ArrayRef>> {
    let Some(array) = parent.as_opt::<FSST>() else {
        return Ok(None);
    };
    let Some(memory) = ctx
        .session()
        .get_opt::<ProviderMemory>()
        .map(|v| v.memory.clone())
    else {
        return Ok(None);
    };
    decode_fsst(&array.into_owned(), ctx, &memory).map(Some)
}

fn decode_fsst(
    array: &FSSTArray,
    ctx: &mut ExecutionCtx,
    memory: &LiveMemoryPool,
) -> VortexResult<ArrayRef> {
    vortex_ensure!(
        array.codes_bytes_handle().is_on_host(),
        "native FSST decoding requires host code bytes"
    );
    // Admit both integer trees before executing either. Their decoded owners
    // remain local to this call; FSST retains only its original validity child.
    let scratch = add(
        integer_workspace(array.uncompressed_lengths(), 0)?.bytes,
        integer_workspace(array.codes_offsets(), 0)?.bytes,
    )?;
    let _scratch = crate::owned_buffers::reserve(memory, scratch)?;
    let lengths = decode_integer(array.uncompressed_lengths(), ctx, 0)?;
    let offsets = decode_integer(array.codes_offsets(), ctx, 0)?;
    let total = checked_lengths(&lengths)?;
    let code_len = checked_offsets(&offsets, array.codes_bytes_handle().len())?;
    vortex_ensure!(
        total <= code_len.checked_mul(8).ok_or_else(size_overflow)?,
        "native FSST decoded lengths exceed the symbol expansion bound"
    );
    // Pinned canonical.rs requests total + 7 bytes and one 16-byte view per
    // row. Both BufferMut allocations request the preferred alignment slack.
    let view_bytes = capacity(mul(array.len(), 16)?)?;
    let bytes = add(capacity(add(total, 7)?)?, view_bytes)?;
    let mut data_credit = crate::owned_buffers::reserve(memory, bytes)?;
    let view_credit = Arc::new(
        data_credit
            .split(view_bytes)
            .map_err(|error| vortex_err!("{error}"))?,
    );
    let data_credit = Arc::new(data_credit);
    // Canonical offsets prevent the provider's legacy scalar-offset context
    // from decoding them again outside the admitted call.
    let slots = FSSTSlots {
        uncompressed_lengths: lengths.into_array(),
        codes_offsets: offsets.into_array(),
        codes_validity: array.codes_validity().cloned(),
    };
    let input = Array::<FSST>::try_from_parts(
        ArrayParts::new(
            FSST,
            array.dtype().clone(),
            array.len(),
            array.data().clone(),
        )
        .with_slots(slots.into_slots()),
    )?;
    let (decoded, step) = FSST::execute(input, ctx)?.into_parts();
    vortex_ensure!(
        matches!(step, ExecutionStep::Done),
        "native FSST provider did not finish canonical execution"
    );
    let output = decoded
        .as_opt::<VarBinView>()
        .ok_or_else(|| vortex_err!("native FSST provider did not produce a variable-width view"))?;
    let buffers: Vec<BufferHandle> = decoded
        .buffers()
        .into_iter()
        .enumerate()
        .map(|(index, buffer)| {
            let credit = if index == output.data_buffers().len() {
                &view_credit
            } else {
                &data_credit
            };
            BufferHandle::new_host(crate::owned_buffers::retain_shared_credit(
                buffer,
                Arc::clone(credit),
            ))
        })
        .collect();
    let retained = VarBinViewArray::try_from_parts(VarBinView.with_buffers(output, &buffers)?)?;
    retained.statistics().inherit_from(array.statistics());
    Ok(retained.into_array())
}

/// Sum the capacity of every potentially live integer output, including COW
/// replacements. Only non-null native integer trees with these reviewed decode
/// implementations are admitted; arbitrary expressions and caches are excluded.
struct IntegerWorkspace {
    bytes: u64,
    // Some native kernels mutate a child buffer, copying it when shared. Carry
    // its alignment as well as its row count through those possible copies.
    alignment: usize,
}

fn integer_workspace(array: &ArrayRef, depth: usize) -> VortexResult<IntegerWorkspace> {
    vortex_ensure!(
        depth < 24,
        "native FSST integer metadata exceeds the depth bound"
    );
    let DType::Primitive(ptype, _) = array.dtype() else {
        vortex_bail!("native FSST metadata requires native integer arrays");
    };
    vortex_ensure!(
        ptype.is_int() && !array.dtype().is_nullable() && array.is_host(),
        "native FSST metadata requires non-null integer arrays"
    );
    if array.encoding_id() == Cast.id() {
        return nullability_cast_workspace(array, depth).map(|(input, _)| input);
    }
    if array.encoding_id() == FillNull.id() {
        let fill = array.as_::<ScalarFn>();
        vortex_ensure!(
            fill.get_child(1).is::<Constant>(),
            "native FSST null fill requires a constant"
        );
        let (input, bitmap) = nullable_integer_workspace(fill.get_child(0), depth + 1)?;
        return Ok(IntegerWorkspace {
            bytes: add(
                add(input.bytes, bitmap)?,
                aligned_capacity(mul(array.len(), ptype.byte_width())?, input.alignment)?,
            )?,
            alignment: input.alignment,
        });
    }
    if let Some(primitive) = array.as_opt::<Primitive>() {
        return Ok(IntegerWorkspace {
            bytes: 0,
            alignment: *primitive.data().buffer_handle().as_host().alignment(),
        });
    }
    let rows = if let Some(delta) = array.as_opt::<Delta>() {
        // A sliced Delta still allocates its complete padded child extent.
        delta.deltas().len()
    } else if let Some(rle) = array.as_opt::<RLE>() {
        // rle_decompress retains all touched 1024-value chunks behind a slice.
        let end = rle
            .offset()
            .checked_add(array.len())
            .ok_or_else(size_overflow)?;
        let chunks = end.div_ceil(1024) - rle.offset() / 1024;
        chunks.checked_mul(1024).ok_or_else(size_overflow)?
    } else if array.is::<Constant>()
        || array.is::<BitPacked>()
        || array.is::<FoR>()
        || array.is::<ZigZag>()
        || array.is::<Dict>()
        || array.is::<Sparse>()
        || array.is::<RunEnd>()
        || array.is::<Sequence>()
        || array.is::<Filter>()
        || array.is::<Slice>()
    {
        array.len()
    } else {
        vortex_bail!(
            "native FSST metadata decoder {} has no admitted allocation bound",
            array.encoding_id()
        );
    };
    let mut bytes = 0;
    let mut alignment = *Alignment::DEFAULT_ALIGNMENT;
    for child in array.slots().iter().flatten() {
        let child = integer_workspace(child, depth + 1)?;
        bytes = add(bytes, child.bytes)?;
        alignment = alignment.max(child.alignment);
    }
    let output = mul(rows, ptype.byte_width())?;
    bytes = add(bytes, aligned_capacity(output, alignment)?)?;
    if (array.is::<Dict>() || array.is::<Filter>()) && alignment > *Alignment::DEFAULT_ALIGNMENT {
        // Fixed-width take first gathers at the preferred alignment, then may
        // copy to the source's stronger alignment while the first buffer lives.
        bytes = add(bytes, capacity(output)?)?;
    }
    if array.is::<Filter>() {
        bytes = add(bytes, capacity(mul(rows, 8)?)?)?;
    }
    if let Some(rle) = array.as_opt::<RLE>() {
        // The provider converts its chunk offsets into a separate Vec<u64>.
        bytes = add(bytes, mul(rle.values_idx_offsets().len(), 8)?)?;
    }
    if let Some(sparse) = array.as_opt::<Sparse>()
        && sparse.patches().offset() != 0
    {
        // Sliced sparse indices are shifted through a primitive/scalar
        // subtraction before the filled output is patched.
        let indices = sparse.patch_indices();
        bytes = add(
            bytes,
            aligned_capacity(
                mul(indices.len(), indices.dtype().as_ptype().byte_width())?,
                alignment,
            )?,
        )?;
    }
    Ok(IntegerWorkspace { bytes, alignment })
}

/// FSST take fills null selected lengths with zero. Admit its native nullable
/// primitive/dictionary/masked intermediate, including the index fill and both bitmap
/// negation / copy-on-write overlaps. Other nullable decoder trees stay explicit.
fn nullable_integer_workspace(
    array: &ArrayRef,
    depth: usize,
) -> VortexResult<(IntegerWorkspace, u64)> {
    vortex_ensure!(
        depth < 24,
        "native FSST integer metadata exceeds the depth bound"
    );
    vortex_ensure!(
        array.dtype().is_int() && array.is_host(),
        "native FSST null fill requires host integers"
    );
    if !array.dtype().is_nullable() {
        return integer_workspace(array, depth).map(|workspace| (workspace, 0));
    }
    if array.encoding_id() == Cast.id() {
        return nullability_cast_workspace(array, depth);
    }
    if let Some(primitive) = array.as_opt::<Primitive>() {
        return Ok((
            IntegerWorkspace {
                bytes: 0,
                alignment: (*primitive.data().buffer_handle().as_host().alignment())
                    .max(*Alignment::DEFAULT_ALIGNMENT),
            },
            primitive_validity_copy_bytes(array)?,
        ));
    }
    if array.is::<Constant>() {
        return Ok((
            IntegerWorkspace {
                bytes: capacity(mul(array.len(), array.dtype().as_ptype().byte_width())?)?,
                alignment: *Alignment::DEFAULT_ALIGNMENT,
            },
            0,
        ));
    }
    if let Some(masked) = array.as_opt::<Masked>() {
        let (mut input, _) = nullable_integer_workspace(masked.child(), depth + 1)?;
        input.alignment = input.alignment.max(*Alignment::DEFAULT_ALIGNMENT);
        // Masked requires an all-valid child. Replacing its validity on a
        // canonical primitive borrows both buffers; only the later null fill
        // copies/inverts the outer bitmap, including retained slice overhang.
        return Ok((input, primitive_validity_copy_bytes(array)?));
    }
    let Some(dict) = array.as_opt::<Dict>() else {
        vortex_bail!(
            "native FSST nullable metadata decoder {} has no admitted allocation bound",
            array.encoding_id()
        );
    };
    let codes = dict.codes();
    let (code_workspace, bitmap) = nullable_integer_workspace(codes, depth + 1)?;
    let values = integer_workspace(dict.values(), depth + 1)?;
    let code_alignment = code_workspace.alignment.max(*Alignment::DEFAULT_ALIGNMENT);
    let alignment = values.alignment.max(*Alignment::DEFAULT_ALIGNMENT);
    let output = mul(array.len(), array.dtype().as_ptype().byte_width())?;
    let mut bytes = add(
        add(values.bytes, code_workspace.bytes)?,
        aligned_capacity(output, alignment)?,
    )?;
    if alignment > *Alignment::DEFAULT_ALIGNMENT {
        bytes = add(bytes, capacity(output)?)?;
    }
    // Fixed-width take fills null codes before gathering. The original codes
    // remain shared, so include their replacement buffer and validity inversion.
    bytes = add(
        bytes,
        aligned_capacity(
            mul(codes.len(), codes.dtype().as_ptype().byte_width())?,
            code_alignment,
        )?,
    )?;
    bytes = add(bytes, bitmap)?;
    Ok((IntegerWorkspace { bytes, alignment }, bitmap))
}

fn nullability_cast_workspace(
    array: &ArrayRef,
    depth: usize,
) -> VortexResult<(IntegerWorkspace, u64)> {
    let cast = array.as_::<ScalarFn>();
    let child = cast.get_child(0);
    vortex_ensure!(
        child.dtype().as_nonnullable() == array.dtype().as_nonnullable(),
        "native FSST metadata admits only nullability casts"
    );
    // Native primitive nullability casts reuse value/validity buffers and
    // check nulls before removing nullability. Value-changing casts stay out.
    nullable_integer_workspace(child, depth + 1)
}

fn primitive_validity_copy_bytes(array: &ArrayRef) -> VortexResult<u64> {
    match array.validity()? {
        Validity::Array(validity) => {
            vortex_ensure!(
                validity.is::<Bool>() && validity.is_host(),
                "native FSST nullable metadata requires a canonical validity bitmap"
            );
            // BitBuffer::not copies its complete byte buffer with the default
            // alignment, including when the source has stronger alignment.
            // A sliced bitmap may retain more bytes than ceil(rows/8).
            capacity(u64::try_from(validity.buffers()[0].len()).map_err(|_| size_overflow())?)
        }
        Validity::NonNullable | Validity::AllValid | Validity::AllInvalid => Ok(0),
    }
}

/// Execute only the pre-admitted integer tree. Preparing its children prevents
/// filter pushdown from creating unreviewed decoder trees or persistent mask
/// caches during this scratch-only operation.
fn decode_integer(
    array: &ArrayRef,
    ctx: &mut ExecutionCtx,
    depth: usize,
) -> VortexResult<PrimitiveArray> {
    vortex_ensure!(
        depth < 24,
        "native FSST integer metadata exceeds the depth bound"
    );
    if let Some(primitive) = array.as_opt::<Primitive>() {
        return Ok(primitive.into_owned());
    }
    if array.encoding_id() == Cast.id() {
        let cast = array.as_::<ScalarFn>();
        let child = decode_integer(cast.get_child(0), ctx, depth + 1)?;
        return Cast::new(child.into_array(), array.dtype().clone())
            .into_array()
            .execute::<PrimitiveArray>(ctx);
    }
    if array.encoding_id() == FillNull.id() {
        let fill = array.as_::<ScalarFn>();
        let input = decode_integer(fill.get_child(0), ctx, depth + 1)?;
        return FillNull::try_new(input.into_array(), fill.get_child(1).clone())?
            .into_array()
            .execute::<PrimitiveArray>(ctx);
    }
    if let Some(masked) = array.as_opt::<Masked>() {
        let child = decode_integer(masked.child(), ctx, depth + 1)?;
        // The native Masked invariant guarantees an all-valid child. Attach
        // its reviewed canonical mask directly, avoiding a lazy validity AND
        // and its unadmitted execution/cache paths.
        return Ok(PrimitiveArray::from_byte_buffer(
            child.buffer_handle().as_host().clone(),
            child.ptype(),
            masked.masked_validity(),
        ));
    }
    if let Some(filter) = array.as_opt::<Filter>() {
        let child = decode_integer(filter.child(), ctx, depth + 1)?;
        // Mask::iter reads the existing bits without filling shared lazy index
        // or range caches. Keep this temporary selection local to the decode.
        let mut indices = BufferMut::<u64>::with_capacity(array.len());
        for (row, keep) in filter.data().filter_mask().iter().enumerate() {
            if keep {
                indices.push(u64::try_from(row).map_err(|_| size_overflow())?);
            }
        }
        return child
            .into_array()
            .take(PrimitiveArray::new(indices.freeze(), Validity::NonNullable).into_array())?
            .execute::<PrimitiveArray>(ctx);
    }
    macro_rules! execute {
        ($($encoding:ident),+ $(,)?) => {
            $(if let Some(typed) = array.as_opt::<$encoding>() {
                return decode_integer_encoding($encoding, typed, ctx, depth);
            })+
        };
    }
    execute!(
        Constant, BitPacked, Delta, FoR, ZigZag, Dict, Sparse, RunEnd, RLE, Sequence, Slice
    );
    vortex_bail!(
        "native FSST metadata decoder {} has no admitted allocation bound",
        array.encoding_id()
    )
}

fn decode_integer_encoding<V: ArrayVTable>(
    vtable: V,
    array: ArrayView<'_, V>,
    ctx: &mut ExecutionCtx,
    depth: usize,
) -> VortexResult<PrimitiveArray> {
    let slots = array
        .as_ref()
        .slots()
        .iter()
        .map(|child| {
            child
                .as_ref()
                .map(|child| decode_integer(child, ctx, depth + 1).map(IntoArray::into_array))
                .transpose()
        })
        .collect::<VortexResult<ArraySlots>>()?;
    Array::<V>::try_from_parts(
        ArrayParts::new(
            vtable,
            array.dtype().clone(),
            array.len(),
            array.data().clone(),
        )
        .with_slots(slots),
    )?
    .into_array()
    .execute::<PrimitiveArray>(ctx)
}

fn checked_lengths(lengths: &PrimitiveArray) -> VortexResult<u64> {
    match_each_integer_ptype!(lengths.ptype(), |P| {
        lengths
            .as_slice::<P>()
            .iter()
            .try_fold(0u64, |total, value| {
                let value = nonnegative(*value, "length")?;
                vortex_ensure!(
                    i32::try_from(value).is_ok(),
                    "native FSST value exceeds the view length bound"
                );
                add(total, value)
            })
    })
}

fn checked_offsets(offsets: &PrimitiveArray, code_bytes: usize) -> VortexResult<u64> {
    match_each_integer_ptype!(offsets.ptype(), |P| {
        let mut first = None;
        let mut previous = 0u64;
        for value in offsets.as_slice::<P>() {
            let value = nonnegative(*value, "offset")?;
            vortex_ensure!(
                value >= previous && value <= code_bytes as u64,
                "native FSST offsets are not ordered within the code buffer"
            );
            first.get_or_insert(value);
            previous = value;
        }
        let first = first.ok_or_else(|| vortex_err!("native FSST offsets are empty"))?;
        Ok(previous - first)
    })
}

fn size_overflow() -> vortex::error::VortexError {
    vortex_err!("native decoder allocation size overflow")
}
fn nonnegative<T: TryInto<u64>>(value: T, name: &str) -> VortexResult<u64> {
    value
        .try_into()
        .map_err(|_| vortex_err!("native FSST {name} is negative"))
}
fn add(a: u64, b: u64) -> VortexResult<u64> {
    a.checked_add(b).ok_or_else(size_overflow)
}
fn mul(a: usize, b: usize) -> VortexResult<u64> {
    a.checked_mul(b)
        .and_then(|v| u64::try_from(v).ok())
        .ok_or_else(size_overflow)
}
fn capacity(bytes: u64) -> VortexResult<u64> {
    aligned_capacity(bytes, *Alignment::DEFAULT_ALIGNMENT)
}
fn aligned_capacity(bytes: u64, alignment: usize) -> VortexResult<u64> {
    usize::try_from(bytes)
        .ok()
        .and_then(|bytes| bytes.checked_add(alignment))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(size_overflow)
}

#[cfg(test)]
#[path = "native_provider_memory_tests.rs"]
mod tests;
