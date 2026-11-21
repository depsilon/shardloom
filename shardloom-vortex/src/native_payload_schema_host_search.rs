//! Layout and compatibility callers may have only a Vortex host allocator.

use super::{DType, NODE_BYTES, search};
use shardloom_exec::compute_pool::CancellationToken;
use vortex::{
    array::memory::{HostAllocator, HostMemoryReservation},
    error::{VortexResult, vortex_ensure, vortex_err},
};

pub(crate) fn contains_on_host(
    dtype: &DType,
    allocator: &dyn HostAllocator,
    cancellation: Option<&CancellationToken>,
    matches: impl Fn(&DType) -> bool,
) -> VortexResult<bool> {
    let mut work_capacity = None;
    let mut work = Vec::new();
    let mut frame_capacity = None;
    search(
        dtype,
        |node| {
            if let Some(token) = cancellation {
                token
                    .check()
                    .map_err(|error| vortex_err!(External: error))?;
            }
            // External reservations are opaque and indivisible. Keep each
            // visited node's work owner until the search completes.
            let credit = allocator.reserve_external(usize::from(NODE_BYTES))?;
            grow(&mut work, allocator, &mut work_capacity)?;
            work.push(credit);
            Ok(matches(node))
        },
        |frames| grow(frames, allocator, &mut frame_capacity),
        std::convert::identity,
    )
}

fn grow<T>(
    values: &mut Vec<T>,
    allocator: &dyn HostAllocator,
    credit: &mut Option<HostMemoryReservation>,
) -> VortexResult<()> {
    if values.len() < values.capacity() {
        return Ok(());
    }
    let overflow = || vortex_err!("schema traversal capacity overflow");
    let capacity = values
        .capacity()
        .max(4)
        .checked_mul(2)
        .ok_or_else(overflow)?;
    let bytes = capacity.checked_mul(size_of::<T>()).ok_or_else(overflow)?;
    let replacement = allocator.reserve_external(bytes)?;
    values
        .try_reserve_exact(capacity - values.len())
        .map_err(|error| vortex_err!("schema traversal allocation failed: {error}"))?;
    *credit = Some(replacement);
    vortex_ensure!(
        values.capacity() <= capacity,
        "schema traversal exceeded admitted capacity"
    );
    Ok(())
}
