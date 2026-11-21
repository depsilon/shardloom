//! Starting scratch cost of the native payload copier, without reading values.

use super::{DType, Result, failed};

pub(crate) fn gather_scratch_per_row(dtype: &DType) -> Result<u64> {
    gather_scratch(dtype, 0)
}

fn gather_scratch(dtype: &DType, depth: usize) -> Result<u64> {
    if depth > 64 {
        return Err(capacity_error());
    }
    // copy_rows/copy_list reserve fixed-list expansion (24 bytes per child),
    // coordinate Vec replacement overlap (3 * Option<usize>), indices, offsets
    // and validity before constructing payloads. Variable-list cardinality is
    // not known from a dtype; actual reservations still govern those domains.
    let children = match dtype {
        DType::FixedSizeList(child, count, _) => gather_scratch(child, depth + 1)?
            .checked_add(24 + 3 * std::mem::size_of::<Option<usize>>() as u64)
            .and_then(|bytes| bytes.checked_mul(u64::from(*count)))
            .ok_or_else(capacity_error)?,
        DType::List(child, _) => gather_scratch(child, depth + 1)?
            .checked_add(3 * std::mem::size_of::<Option<usize>>() as u64)
            .ok_or_else(capacity_error)?,
        DType::Struct(fields, _) => fields.fields().try_fold(0_u64, |bytes, child| {
            bytes
                .checked_add(gather_scratch(&child, depth + 1)?)
                .ok_or_else(capacity_error)
        })?,
        DType::Extension(extension) => return gather_scratch(extension.storage_dtype(), depth + 1),
        _ => 0,
    };
    children.checked_add(32).ok_or_else(capacity_error)
}

fn capacity_error() -> shardloom_core::ShardLoomError {
    failed("native payload gather workspace size overflow")
}
