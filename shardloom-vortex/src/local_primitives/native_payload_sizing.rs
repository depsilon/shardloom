//! Starting scratch cost of the native payload copier, without reading values.

use super::{DType, ReservedVec, Result, failed};
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::array::dtype::StructFields;

enum Frame<'a> {
    FixedSizeList(u32),
    List,
    Struct {
        fields: &'a StructFields,
        next: usize,
        total: u64,
        depth: usize,
    },
}

pub(crate) fn gather_scratch_per_row(dtype: &DType, memory: &LiveMemoryPool) -> Result<u64> {
    gather_scratch(dtype, memory, 64)
}

fn gather_scratch(dtype: &DType, memory: &LiveMemoryPool, max_depth: usize) -> Result<u64> {
    let mut pending = ReservedVec::new(memory)?;
    let mut next = (dtype, 0);
    loop {
        let (dtype, depth) = next;
        if depth > max_depth {
            return Err(capacity_error());
        }
        // Preserve the original postorder arithmetic and variable-cardinality
        // estimate. These estimates never replace actual concurrent credits.
        let mut complete = match dtype {
            DType::FixedSizeList(child, count, _) => {
                pending.push(Frame::FixedSizeList(*count))?;
                next = (child, depth + 1);
                continue;
            }
            DType::List(child, _) => {
                pending.push(Frame::List)?;
                next = (child, depth + 1);
                continue;
            }
            DType::Struct(fields, _) if fields.nfields() != 0 => {
                let child = fields.field_dtypes().next().expect("nonempty struct");
                pending.push(Frame::Struct {
                    fields,
                    next: 1,
                    total: 0,
                    depth: depth + 1,
                })?;
                next = (child, depth + 1);
                continue;
            }
            DType::Extension(extension) => {
                next = (extension.storage_dtype(), depth + 1);
                continue;
            }
            _ => 32,
        };
        loop {
            let Some(frame) = pending.values.pop() else {
                return Ok(complete);
            };
            complete = match frame {
                Frame::FixedSizeList(count) => complete
                    .checked_add(24 + 3 * std::mem::size_of::<Option<usize>>() as u64)
                    .and_then(|bytes| bytes.checked_mul(u64::from(count)))
                    .and_then(|bytes| bytes.checked_add(32))
                    .ok_or_else(capacity_error)?,
                Frame::List => complete
                    .checked_add(3 * std::mem::size_of::<Option<usize>>() as u64)
                    .and_then(|bytes| bytes.checked_add(32))
                    .ok_or_else(capacity_error)?,
                Frame::Struct {
                    fields,
                    next: index,
                    total,
                    depth,
                } => {
                    let total = total.checked_add(complete).ok_or_else(capacity_error)?;
                    if index < fields.nfields() {
                        let child = fields
                            .field_dtypes()
                            .nth(index)
                            .expect("admitted field index");
                        pending.values.push(Frame::Struct {
                            fields,
                            next: index + 1,
                            total,
                            depth,
                        });
                        next = (child, depth);
                        break;
                    }
                    total.checked_add(32).ok_or_else(capacity_error)?
                }
            };
        }
    }
}

fn capacity_error() -> shardloom_core::ShardLoomError {
    failed("native payload gather workspace size overflow")
}

#[cfg(test)]
#[path = "native_payload_sizing_tests.rs"]
mod tests;
