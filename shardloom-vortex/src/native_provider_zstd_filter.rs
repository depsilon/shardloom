//! Sparse text-frame selection inside the admitted native execution boundary.
//!
//! Selected frame tables retain the provider's allocator reservation. Row indices
//! and output views use that same session; the mask never acquires a lazy cache.

use super::*;
use vortex::{
    array::{
        arrays::varbinview::build_views::MAX_BUFFER_LEN, memory::HostAllocatorExt as _,
        vtable::child_to_validity,
    },
    buffer::{Buffer, ByteBuffer},
    encodings::zstd::{Zstd, ZstdSlots},
    mask::{AllOr, Mask},
};

pub(super) fn execute(
    child: &ArrayRef,
    parent: &ArrayRef,
    _child_idx: usize,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<ArrayRef>> {
    let (Some(array), Some(filter)) = (child.as_opt::<Zstd>(), parent.as_opt::<Filter>()) else {
        return Ok(None);
    };
    let Some(memory) = ctx
        .session()
        .get_opt::<ProviderMemory>()
        .map(|v| v.memory.clone())
    else {
        return Ok(None);
    };
    let Some(selection) = prepare(array, filter.data().filter_mask(), ctx)? else {
        return Ok(None);
    };
    // The view decoder splits at MAX_BUFFER_LEN. Every adjacent pair of
    // segments exceeds that extent, so this bounds their handle count even
    // when a single value itself is larger than that extent.
    let buffers = selection.payload_bytes / MAX_BUFFER_LEN;
    let buffers = buffers
        .checked_mul(2)
        .and_then(|v| v.checked_add(1))
        .ok_or_else(size_overflow)?;
    // Cover the decoder's Vec/Arc conversions and our temporary handle list.
    // The selected decoder uses the same conservative capacity; bookkeeping and
    // fixed-size array headers are outside this collection-capacity contract.
    let byte_headers = mul(buffers, 2 * size_of::<ByteBuffer>())?;
    let handle_headers = mul(buffers, 3 * size_of::<BufferHandle>())?;
    let _structure = crate::owned_buffers::reserve(
        &memory,
        add(
            add(byte_headers, handle_headers)?,
            4 * size_of::<usize>() as u64,
        )?,
    )?;
    let decoded =
        Zstd::decompress_selected(selection.array.as_::<Zstd>(), &selection.indices, ctx)?;
    vortex_ensure!(
        decoded.data_buffers().len() <= buffers,
        "Zstd view handle bound exceeded"
    );
    let output = retain_structure(&decoded, &memory)?;
    // Keep the compressed metadata and selection owner until native execution
    // and output ownership transfer have both finished, including every error return.
    drop(selection);
    Ok(Some(output))
}

pub(super) struct Selection {
    pub(super) array: ArrayRef,
    pub(super) indices: Buffer<usize>,
    payload_bytes: usize,
}

pub(super) fn prepare(
    array: ArrayView<'_, Zstd>,
    mask: &Mask,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<Selection>> {
    let upper_frames = array.as_ref().nbuffers(); // May include one dictionary handle.
    if !matches!(array.dtype(), DType::Utf8(_) | DType::Binary(_))
        || !(2..=65).contains(&upper_frames)
        || mask.true_count() > 4096
        || mask.true_count() > array.len() / 8
    {
        return Ok(None);
    }
    let validity = child_to_validity(
        array.slots()[ZstdSlots::VALIDITY].as_ref(),
        array.dtype().nullability(),
    );
    if !matches!(validity, Validity::NonNullable | Validity::AllValid) {
        return Ok(None);
    }
    let AllOr::Some(bits) = mask.bit_buffer() else {
        return Ok(None);
    };
    let data = array.data();
    if !(2..=64).contains(&data.frame_metadata().len()) {
        return Ok(None);
    }
    let mut indices = ctx.allocator().allocate_typed::<usize>(mask.true_count())?;
    let selected = indices.as_mut_slice_typed::<usize>()?;
    let mut source_indices = bits.set_indices().peekable();
    // Eligibility bounds this list independently of row count. The original
    // table is borrowed; only the selected table receives a new reservation.
    let mut frame_indices = [0usize; 64];
    let mut frame_count = 0;
    let (mut source_start, mut kept_rows, mut cursor, mut touched, mut payload_bytes) =
        (0usize, 0usize, 0, 0, 0usize);
    for (frame_index, description) in data.frame_metadata().iter().enumerate() {
        let rows = usize::try_from(description.n_values)?;
        let source_stop = source_start
            .checked_add(rows)
            .ok_or_else(|| vortex_err!("Zstd frame row count overflow"))?;
        if source_start < data.slice_stop() && source_stop > data.slice_start() {
            touched += 1;
        }
        let first = cursor;
        while let Some(&index) = source_indices.peek() {
            let source_row = data
                .slice_start()
                .checked_add(index)
                .ok_or_else(size_overflow)?;
            if source_row >= source_stop {
                break;
            }
            vortex_ensure!(
                source_row >= source_start,
                "Invalid Zstd filter row mapping"
            );
            selected[cursor] = kept_rows
                .checked_add(source_row - source_start)
                .ok_or_else(size_overflow)?;
            cursor += 1;
            source_indices.next();
        }
        if cursor != first {
            payload_bytes = payload_bytes
                .checked_add(usize::try_from(description.uncompressed_size)?)
                .ok_or_else(size_overflow)?;
            frame_indices[frame_count] = frame_index;
            frame_count += 1;
            kept_rows = kept_rows.checked_add(rows).ok_or_else(size_overflow)?;
        }
        source_start = source_stop;
    }
    vortex_ensure!(
        source_start == data.unsliced_n_rows() && cursor == selected.len(),
        "Zstd frame value counts do not cover the non-null source rows"
    );
    if frame_count >= touched || selected.is_empty() || kept_rows > array.len() {
        return Ok(None);
    }
    let data = data.select_frames_in(&frame_indices[..frame_count], &ctx.allocator())?;
    vortex_ensure!(
        data.len() == kept_rows,
        "Zstd selected frame length changed"
    );
    // Execution may read headers; this checked constructor verifies retained
    // frames and normalizes all-valid validity to the new complete frame length.
    let array = Zstd::try_new(array.dtype().clone(), data, validity)?.into_array();
    Ok(Some(Selection {
        array,
        indices: indices.freeze_typed::<usize>()?,
        payload_bytes,
    }))
}

fn retain_structure(decoded: &VarBinViewArray, memory: &LiveMemoryPool) -> VortexResult<ArrayRef> {
    // The provider already constructed only selected views. Attach collection
    // credit to every independent output buffer without copying those views.
    let bytes = add(
        mul(decoded.data_buffers().len(), size_of::<BufferHandle>())?,
        2 * size_of::<usize>() as u64,
    )?;
    let credit = Arc::new(crate::owned_buffers::reserve(memory, bytes)?);
    let buffers: Vec<_> = decoded
        .data_buffers()
        .iter()
        .map(|buffer| {
            BufferHandle::new_host(crate::owned_buffers::retain_shared_credit(
                buffer.as_host().clone(),
                Arc::clone(&credit),
            ))
        })
        .collect();
    let views = crate::owned_buffers::retain_shared_credit(
        decoded.views_handle().as_host().clone(),
        credit,
    );
    Ok(VarBinViewArray::new_handle(
        BufferHandle::new_host(views),
        Arc::from(buffers),
        decoded.dtype().clone(),
        decoded.validity()?,
    )
    .into_array())
}
