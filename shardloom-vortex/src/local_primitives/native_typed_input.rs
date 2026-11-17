//! Typed JSON is an explicit intake boundary into the existing native source.

use serde_json::Value as Json;
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::array::{
    ArrayRef,
    dtype::{DType, Nullability},
    memory::HostAllocatorRef,
    validity::Validity,
};

use super::{
    native_capacity::ReservedVec,
    native_input_schema::{parse_dtype, parse_json, reserve_json},
    native_input_values::{Layout, add, leaf, multiply},
    result_batch::{self, Value},
    vortex_error,
};

pub(crate) struct BuiltInputColumn {
    pub(crate) array: ArrayRef,
    pub(crate) logical_bytes: usize,
    pub(crate) copied_payload_bytes: u64,
}

pub(crate) fn build(
    dtype_json: &str,
    values: &[Option<&str>],
    memory: &LiveMemoryPool,
    allocator: &HostAllocatorRef,
    max_bytes: usize,
) -> Result<BuiltInputColumn> {
    let (dtype, metadata) = parse_dtype(dtype_json, memory)?;
    let allocator = crate::owned_buffers::with_credit(allocator.clone(), metadata);
    let json_bytes = values
        .iter()
        .try_fold(0, |bytes, value| add(bytes, value.map_or(4, str::len)))?;
    let _json_workspace = reserve_json(memory, json_bytes)?;
    let mut parsed = ReservedVec::new(memory)?;
    parsed.reserve(values.len())?;
    for value in values {
        parsed
            .values
            .push(value.map_or(Ok(Json::Null), parse_json)?);
    }
    let mut layout = Layout::new(&dtype, memory)?;
    for value in &parsed.values {
        layout.observe(&dtype, Some(value))?;
    }
    let logical_bytes = layout.bytes(&dtype)?;
    if logical_bytes > max_bytes {
        return Err(failed("typed native input byte bound exceeded"));
    }
    let copied_payload_bytes = u64::try_from(layout.copied()?).map_err(vortex_error)?;
    drop(layout);
    let mut cells = ReservedVec::new(memory)?;
    cells.reserve(parsed.values.len())?;
    cells.values.extend(parsed.values.iter().map(Some));
    let array = construct::construct(&dtype, &cells.values, memory, &allocator)?;
    if array.dtype() != &dtype
        || array.len() != values.len()
        || array.nbytes() != logical_bytes as u64
    {
        return Err(failed(
            "typed native input construction changed its admitted schema or size",
        ));
    }
    Ok(BuiltInputColumn {
        array,
        logical_bytes,
        copied_payload_bytes,
    })
}

#[path = "native_typed_construct.rs"]
mod construct;

fn validity(
    dtype: &DType,
    cells: &[Option<&Json>],
    allocator: &HostAllocatorRef,
) -> Result<Validity> {
    if !dtype.is_nullable() {
        return Ok(Validity::NonNullable);
    }
    Ok(Validity::Array(result_batch::build_column(
        &DType::Bool(Nullability::NonNullable),
        cells.len(),
        allocator,
        |row| {
            Ok(Value::Bool(
                cells[row].is_some_and(|value| !value.is_null()),
            ))
        },
    )?))
}

pub(super) fn failed(reason: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native typed input: {reason}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "native_typed_value_tests.rs"]
mod tests;
