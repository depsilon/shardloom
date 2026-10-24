//! Data-coordinate checks required before an imported encoding can execute.
//! Wire statistics and derived patch caches never establish these invariants.

use std::cell::Cell;

use crate::resident_session::NativeExecutionContext;
use vortex::{
    array::{
        Array, ArrayParts, ArrayRef, IntoArray as _, VortexSessionExecute as _,
        arrays::PrimitiveArray, patches::Patches,
    },
    encodings::{
        fastlanes::{
            BitPacked, BitPackedArrayExt as _, BitPackedData, BitPackedSlots, BitWidthsView,
        },
        runend::{RunEnd, RunEndArraySlotsExt as _},
    },
    error::{VortexResult, vortex_err},
};

#[derive(Clone, Copy)]
pub(super) enum IndexRule {
    Dictionary(usize),
    Offsets(usize),
    RunEnds,
    PatchIndices,
    PatchRange { start: usize, end: usize },
}

#[derive(Default)]
pub(super) struct CoordinateWork {
    pub(super) arrays_executed: Cell<u64>,
    pub(super) values_checked: Cell<u64>,
}

pub(super) fn indices(
    array: &ArrayRef,
    rule: IndexRule,
    context: &NativeExecutionContext<'_>,
    work: &CoordinateWork,
) -> VortexResult<()> {
    if !array.dtype().is_int() {
        return Err(vortex_err!(
            "SL-NATIVE-IPC: coordinates require an integer dtype"
        ));
    }
    let bytes = (array.len() as u64)
        .checked_mul(32)
        .ok_or_else(|| vortex_err!("SL-NATIVE-IPC: coordinate validation workspace overflow"))?;
    let _workspace = crate::owned_buffers::reserve(context.memory(), bytes)?;
    let mut execution = context.native_session().create_execution_ctx();
    let values = array.clone().execute::<PrimitiveArray>(&mut execution)?;
    let mut prior = None;
    for index in 0..values.len() {
        context
            .check_cancelled()
            .map_err(|error| vortex_err!("{error}"))?;
        let scalar = values.as_ref().execute_scalar(index, &mut execution)?;
        if scalar.is_null() && matches!(rule, IndexRule::Dictionary(_)) {
            continue;
        }
        let value = usize::try_from(&scalar)?;
        let in_range = match rule {
            IndexRule::Dictionary(count) => value < count,
            IndexRule::Offsets(bytes) => value <= bytes,
            IndexRule::RunEnds => value > 0 && value <= (u32::MAX as usize).min(usize::MAX / 2),
            IndexRule::PatchIndices => value <= (u32::MAX as usize).min(usize::MAX / 2),
            IndexRule::PatchRange { start, end } => start <= value && value < end,
        };
        let ordered = match rule {
            IndexRule::Dictionary(_) => true,
            IndexRule::Offsets(_) => prior.is_none_or(|last| last <= value),
            _ => prior.is_none_or(|last| last < value),
        };
        if !in_range || !ordered {
            return Err(vortex_err!(
                "SL-NATIVE-IPC: invalid coordinate bounds or order"
            ));
        }
        prior = Some(value);
    }
    work.arrays_executed.set(
        work.arrays_executed
            .get()
            .checked_add(1)
            .ok_or_else(|| vortex_err!("SL-NATIVE-IPC: validation accounting overflow"))?,
    );
    work.values_checked.set(
        work.values_checked
            .get()
            .checked_add(values.len() as u64)
            .ok_or_else(|| vortex_err!("SL-NATIVE-IPC: validation accounting overflow"))?,
    );
    Ok(())
}

pub(super) fn run_end(array: &ArrayRef) -> VortexResult<()> {
    // The pinned provider permits empty ends without checking logical length.
    // Reject that corrupt shape before a parent can use the decoded child.
    if array
        .as_opt::<RunEnd>()
        .is_some_and(|run_end| !run_end.is_empty() && run_end.ends().is_empty())
    {
        return Err(vortex_err!(
            "SL-NATIVE-IPC: nonempty run-end array requires run ends"
        ));
    }
    Ok(())
}

pub(super) fn packed(
    array: ArrayRef,
    context: &NativeExecutionContext<'_>,
    work: &CoordinateWork,
) -> VortexResult<ArrayRef> {
    let Some(packed) = array.as_opt::<BitPacked>() else {
        return Ok(array);
    };
    let BitWidthsView::Global(bit_width) = packed.bit_widths() else {
        return Err(vortex_err!(
            "SL-NATIVE-IPC: blocked packed widths are outside the admitted wire contract"
        ));
    };
    if usize::from(bit_width) > array.dtype().as_ptype().byte_width() * 8 {
        return Err(vortex_err!(
            "SL-NATIVE-IPC: packed width exceeds its integer dtype"
        ));
    }
    let Some(patches) = packed.patches() else {
        return Ok(array);
    };
    let end = patches
        .offset()
        .checked_add(array.len())
        .ok_or_else(|| vortex_err!("SL-NATIVE-IPC: patch coordinate overflow"))?;
    indices(
        patches.indices(),
        IndexRule::PatchRange {
            start: patches.offset(),
            end,
        },
        context,
        work,
    )?;
    if patches.chunk_offsets().is_none() {
        return Ok(array);
    }
    // The pinned deserializer resets offset_within_chunk. Its cache is derived
    // state, so discard it and retain the validated patch indices and values.
    // This changes no logical values or BitPacked encoding identity.
    let patches = Patches::new(
        array.len(),
        patches.offset(),
        patches.indices().clone(),
        patches.values().clone(),
        None,
        &mut context.native_session().create_execution_ctx(),
    )?;
    let data = BitPackedData::try_new(
        packed.packed().clone(),
        Some(patches),
        bit_width,
        packed.offset(),
    )?;
    let mut parts = ArrayParts::new(
        BitPacked,
        array.dtype().clone(),
        array.len(),
        data,
        array.slots().iter().cloned().collect(),
    );
    parts.slots[BitPackedSlots::PATCH_CHUNK_OFFSETS] = None;
    Ok(Array::<BitPacked>::try_from_parts(parts)?.into_array())
}
