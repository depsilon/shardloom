//! Finite byte-buffer bounds for the pinned 0.90 physical decoder paths.
//!
//! Sum potential allocations throughout a closed tree rather than assuming
//! children are unique or their temporary outputs cannot overlap. These credits
//! live only through append; they do not cover structural objects, stack scratch,
//! allocator metadata or persistent mask caches. See the dated design contract.

use super::*;
use vortex::{
    array::arrays::{Patched, decimal::DecimalArrayExt as _, patched::PatchedArraySlotsExt as _},
    encodings::{
        alp::{ALP, ALPArrayExt as _, ALPRD},
        fastlanes::{
            BitPackedArrayExt as _, BitPackedSlots, FoRArrayExt as _, FoRArraySlotsExt as _,
            FoRReferences,
        },
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Mode {
    Append,
    Canonical,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Workspace {
    pub(super) bytes: u64,
    alignment: usize,
}

impl Workspace {
    fn empty() -> Self {
        Self {
            bytes: 0,
            alignment: Alignment::DEFAULT_ALIGNMENT.as_usize(),
        }
    }

    fn include(&mut self, other: Self) -> VortexResult<()> {
        self.bytes = add(self.bytes, other.bytes)?;
        self.alignment = self.alignment.max(other.alignment);
        Ok(())
    }

    fn allocate(&mut self, bytes: u64) -> VortexResult<()> {
        vortex_ensure!(
            isize::try_from(bytes).is_ok(),
            "native decoder scratch allocation size overflow"
        );
        self.bytes = add(self.bytes, bytes)?;
        Ok(())
    }
}

/// Missing coverage preserves existing execution; it must never become a
/// reported zero-byte guarantee or an execution-depth restriction.
pub(super) fn workspace(
    array: &ArrayRef,
    mode: Mode,
    depth: usize,
) -> VortexResult<Option<Workspace>> {
    if depth >= 24
        || !matches!(
            array.dtype(),
            DType::Primitive(..) | DType::Bool(_) | DType::Decimal(..)
        )
        || !borrowed_validity(array)?
    {
        return Ok(None);
    }
    let Some(alignment) = host_buffer_alignment(array) else {
        return Ok(None);
    };
    // Blocked widths/references have different scratch trees in 0.90 and are
    // outside this finite global-encoding allocation proof.
    if array
        .as_opt::<BitPacked>()
        .is_some_and(|packed| !packed.bit_widths().is_global())
        || array
            .as_opt::<FoR>()
            .is_some_and(|for_| !for_.references().is_global())
    {
        return Ok(None);
    }
    if canonical(array) {
        let mut result = Workspace::empty();
        if mode == Mode::Canonical {
            result.alignment = result.alignment.max(alignment);
        }
        return Ok(Some(result));
    }
    if array.is::<Constant>() {
        let mut result = Workspace::empty();
        if mode == Mode::Canonical {
            result.allocate(decoded_capacity(array, array.len(), result.alignment)?)?;
        }
        return Ok(Some(result));
    }
    if let Some(dict) = array.as_opt::<Dict>() {
        return dictionary(array, dict.codes(), dict.values(), depth);
    }
    if let Some(patched) = array.as_opt::<Patched>() {
        if !array.dtype().is_primitive() || array.dtype().is_nullable() {
            return Ok(None);
        }
        let Some(mut result) = workspace(patched.inner(), mode, depth + 1)? else {
            return Ok(None);
        };
        for child in [
            patched.lane_offsets(),
            patched.patch_indices(),
            patched.patch_values(),
        ] {
            let Some(child) = workspace(child, Mode::Canonical, depth + 1)? else {
                return Ok(None);
            };
            result.include(child)?;
        }
        if mode == Mode::Canonical {
            result.allocate(decoded_capacity(array, array.len(), result.alignment)?)?;
        }
        return Ok(Some(result));
    }
    if let Some(for_) = array.as_opt::<FoR>()
        && let FoRReferences::Global(reference) = for_.references()
        && reference.dtype().is_unsigned_int()
        && for_.encoded().is::<BitPacked>()
    {
        // The upstream fused path never canonicalizes the BitPacked child.
        let Some(mut result) = workspace(for_.encoded(), Mode::Append, depth + 1)? else {
            return Ok(None);
        };
        result.allocate(builder_capacity(array)?)?;
        return Ok(Some(result));
    }
    physical_workspace(array, mode, depth)
}

fn physical_workspace(
    array: &ArrayRef,
    mode: Mode,
    depth: usize,
) -> VortexResult<Option<Workspace>> {
    if array.is::<Delta>() && array.dtype().is_nullable() {
        // The reviewed Delta bound covers non-null integer children only.
        // 0.90 may expose canonical validity without closing the nullable
        // decoder's allocation tree, so keep this exclusion explicit.
        return Ok(None);
    }
    if let Some(sparse) = array.as_opt::<Sparse>()
        && (array.dtype().is_nullable()
            || (sparse.patches().offset() != 0 && !sparse.patch_indices().is::<Primitive>()))
    {
        // Nullable patch validity and rewrites of encoded offset indices do not
        // have a closed allocation tree in this contract.
        return Ok(None);
    }
    if let Some(alp) = array.as_opt::<ALP>()
        && array.dtype().is_nullable()
        && alp.patches().is_some()
    {
        return Ok(None);
    }
    if !(array.is::<BitPacked>()
        || array.is::<Delta>()
        || array.is::<FoR>()
        || array.is::<RLE>()
        || array.is::<ZigZag>()
        || array.is::<RunEnd>()
        || array.is::<Sequence>()
        || array.is::<ALP>()
        || array.is::<ALPRD>()
        || array.is::<Sparse>())
    {
        // In particular, physical-to-Chunked children and Zstd are excluded.
        return Ok(None);
    }
    let Some(mut result) = children(array, depth)? else {
        return Ok(None);
    };
    if array.is::<BitPacked>() {
        if mode == Mode::Canonical {
            result.allocate(builder_capacity(array)?)?;
        }
    } else if let Some(delta) = array.as_opt::<Delta>() {
        result.allocate(decoded_capacity(
            array,
            delta.deltas().len(),
            Alignment::DEFAULT_ALIGNMENT.as_usize(),
        )?)?;
    } else if let Some(rle) = array.as_opt::<RLE>() {
        let end = rle
            .offset()
            .checked_add(array.len())
            .ok_or_else(size_overflow)?;
        let chunks = end.div_ceil(1024) - rle.offset() / 1024;
        let rows = chunks.checked_mul(1024).ok_or_else(size_overflow)?;
        result.allocate(decoded_capacity(
            array,
            rows,
            Alignment::DEFAULT_ALIGNMENT.as_usize(),
        )?)?;
        // slice.iter().map().collect::<Vec<u64>>() is trusted-length.
        result.allocate(mul(rle.values_idx_offsets().len(), 8)?)?;
        if rle.indices().dtype().is_nullable() {
            // AllFalse.to_bit_buffer() allocates; a mixed canonical mask may
            // instead borrow. Reserve the potential bitmap in either case.
            result.allocate(bitmap_capacity(rle.indices().len())?)?;
        }
    } else if array.is::<RunEnd>() {
        if !matches!(array.dtype(), DType::Primitive(..) | DType::Bool(_)) {
            return Ok(None);
        }
        result.allocate(decoded_capacity(
            array,
            array.len(),
            Alignment::DEFAULT_ALIGNMENT.as_usize(),
        )?)?;
        if array.dtype().is_nullable() {
            result.allocate(bitmap_capacity(array.len())?)?;
        }
    } else if let Some(sparse) = array.as_opt::<Sparse>() {
        let output = decoded_capacity(array, array.len(), result.alignment)?;
        // Dense fill, then a possible COW replacement while applying patches.
        result.allocate(add(output, output)?)?;
        if sparse.patches().offset() != 0 {
            let indices = sparse.patch_indices();
            result.allocate(decoded_capacity(indices, indices.len(), result.alignment)?)?;
        }
        if matches!(array.dtype(), DType::Decimal(..)) {
            result.allocate(capacity(0)?)?;
        }
    } else if array.is::<ALPRD>() {
        // Decode left dictionary parts in place and write floats over right
        // parts. Both borrowed children may require COW, including alignment.
        result.allocate(aligned_capacity(mul(array.len(), 2)?, result.alignment)?)?;
        result.allocate(decoded_capacity(array, array.len(), result.alignment)?)?;
    } else {
        // FoR, ZigZag and ALP mutate their canonical child's values (possible
        // COW); Sequence creates a fresh values buffer.
        result.allocate(decoded_capacity(array, array.len(), result.alignment)?)?;
        if let Some(alp) = array.as_opt::<ALP>()
            && alp.patches().is_some()
        {
            result.allocate(decoded_capacity(array, array.len(), result.alignment)?)?;
        }
    }
    Ok(Some(result))
}

fn children(array: &ArrayRef, depth: usize) -> VortexResult<Option<Workspace>> {
    let mut result = Workspace::empty();
    for (slot, child) in array.slots().iter().enumerate() {
        if array.is::<BitPacked>() && slot == BitPackedSlots::VALIDITY_CHILD {
            // Admission already established a borrowed canonical mask or a
            // trivial validity. execute_mask does not canonicalize its constant
            // child, so there is no temporary values buffer to reserve.
            continue;
        }
        let Some(child) = child else {
            continue;
        };
        let Some(child) = workspace(child, Mode::Canonical, depth + 1)? else {
            return Ok(None);
        };
        result.include(child)?;
    }
    Ok(Some(result))
}

fn canonical(array: &ArrayRef) -> bool {
    array.is::<Primitive>() || array.is::<Bool>() || array.is::<Decimal>()
}

fn host_buffer_alignment(array: &ArrayRef) -> Option<usize> {
    // Inspect this node only: workspace visits every admitted physical child,
    // and borrowed_validity checks masks, including BitPacked's skipped slot.
    // Calling is_host here would walk each subtree again at every depth.
    if let Some(primitive) = array.as_opt::<Primitive>() {
        return primitive
            .data()
            .buffer_handle()
            .as_host_opt()
            .map(|buffer| buffer.alignment().as_usize());
    }
    if let Some(decimal) = array.as_opt::<Decimal>() {
        return decimal
            .data()
            .buffer_handle()
            .as_host_opt()
            .map(|buffer| buffer.alignment().as_usize());
    }
    if let Some(boolean) = array.as_opt::<Bool>() {
        return Bool::buffer(boolean, 0)
            .as_host_opt()
            .map(|buffer| buffer.alignment().as_usize());
    }
    let mut alignment = Alignment::DEFAULT_ALIGNMENT.as_usize();
    for buffer in array.buffer_handles() {
        alignment = alignment.max(buffer.as_host_opt()?.alignment().as_usize());
    }
    Some(alignment)
}

fn borrowed_validity(array: &ArrayRef) -> VortexResult<bool> {
    Ok(match array.validity()? {
        Validity::NonNullable | Validity::AllValid | Validity::AllInvalid => true,
        Validity::Array(bitmap) => bitmap.is::<Bool>() && bitmap.is_host(),
    })
}

fn dictionary(
    array: &ArrayRef,
    codes: &ArrayRef,
    values: &ArrayRef,
    depth: usize,
) -> VortexResult<Option<Workspace>> {
    if array.dtype().is_nullable()
        || codes.dtype().is_nullable()
        || values.dtype().is_nullable()
        || !codes.is::<Primitive>()
        || !canonical(values)
    {
        return Ok(None);
    }
    let Some(mut result) = children(array, depth)? else {
        return Ok(None);
    };
    if values.is::<Bool>() {
        // collect_bool allocates u64 words, then retains the truncated buffer.
        result.allocate(capacity(mul(array.len().div_ceil(64), 8)?)?)?;
        if !values.is_empty() && values.len() <= 4096 {
            // BitIterator is exact-size but not TrustedLen; Rust's Vec<bool>
            // (byte-per-bool) starts at max(lower + 1, MIN_NON_ZERO_CAP=8).
            result.allocate(u64::try_from(values.len().max(8)).map_err(|_| size_overflow())?)?;
        }
    } else {
        result.allocate(decoded_capacity(values, array.len(), result.alignment)?)?;
        if result.alignment > Alignment::DEFAULT_ALIGNMENT.as_usize() {
            // Native take gathers first, then aligns to the source buffer.
            result.allocate(decoded_capacity(
                values,
                array.len(),
                Alignment::DEFAULT_ALIGNMENT.as_usize(),
            )?)?;
        }
    }
    Ok(Some(result))
}

fn builder_capacity(array: &ArrayRef) -> VortexResult<u64> {
    let values = value_capacity(array.dtype(), array.len())?;
    let validity = if array.dtype().is_nullable() {
        bitmap_capacity(array.len())?
    } else {
        0
    };
    add(add(values, validity)?, capacity(0)?)
}

fn decoded_capacity(array: &ArrayRef, rows: usize, alignment: usize) -> VortexResult<u64> {
    let width = match array.dtype() {
        DType::Bool(_) => {
            return aligned_capacity(
                u64::try_from(rows.div_ceil(8)).map_err(|_| size_overflow())?,
                alignment,
            );
        }
        DType::Primitive(ptype, _) => ptype.byte_width(),
        DType::Decimal(decimal, _) => {
            if let Some(decimal) = array.as_opt::<Decimal>() {
                decimal.values_type().byte_width()
            } else if let Some(constant) = array.as_opt::<Constant>() {
                constant.scalar().as_decimal().decimal_value().map_or_else(
                    || DecimalType::smallest_decimal_value_type(decimal).byte_width(),
                    |value| value.decimal_type().byte_width(),
                )
            } else {
                DecimalType::smallest_decimal_value_type(decimal).byte_width()
            }
        }
        _ => vortex_bail!("native decoder scratch requires a fixed-width dtype"),
    };
    aligned_capacity(mul(rows, width)?, alignment)
}

#[cfg(test)]
#[path = "native_provider_decoder_scratch_tests.rs"]
mod tests;
