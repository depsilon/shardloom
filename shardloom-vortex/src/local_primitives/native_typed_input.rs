//! Typed JSON is an explicit intake boundary into the existing native source.

use serde_json::Value as Json;
use shardloom_core::{Result, ShardLoomError};
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::array::{
    ArrayRef,
    arrays::{FixedSizeListArray, ListArray, StructArray},
    dtype::{DType, Nullability, PType},
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
    let mut layout = Layout::new(&dtype);
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
    let array = construct(&dtype, &cells.values, memory, &allocator)?;
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

fn construct(
    dtype: &DType,
    cells: &[Option<&Json>],
    memory: &LiveMemoryPool,
    allocator: &HostAllocatorRef,
) -> Result<ArrayRef> {
    match dtype {
        DType::Struct(fields, _) => {
            let mut columns = ReservedVec::new(memory)?;
            columns.reserve(fields.nfields())?;
            for (name, child) in fields.names().iter().zip(fields.fields()) {
                let mut values = ReservedVec::new(memory)?;
                values.reserve(cells.len())?;
                for value in cells {
                    values.values.push(
                        value
                            .and_then(Json::as_object)
                            .and_then(|value| value.get(name.as_ref())),
                    );
                }
                columns
                    .values
                    .push(construct(&child, &values.values, memory, allocator)?);
            }
            let validity = validity(dtype, cells, allocator)?;
            let (columns, _container_credit) = columns.into_parts();
            StructArray::try_new(fields.names().clone(), columns, cells.len(), validity)
                .map(vortex::array::IntoArray::into_array)
                .map_err(vortex_error)
        }
        DType::List(child, _) | DType::FixedSizeList(child, _, _) => {
            let mut values = ReservedVec::new(memory)?;
            let mut offsets = ReservedVec::new(memory)?;
            offsets.reserve(add(cells.len(), 1)?)?;
            offsets.values.push(0usize);
            if let DType::FixedSizeList(_, size, _) = dtype {
                values.reserve(multiply(cells.len(), *size as usize)?)?;
            }
            for value in cells {
                let items = value.and_then(Json::as_array);
                let count = if let DType::FixedSizeList(_, size, _) = dtype {
                    *size as usize
                } else {
                    items.map_or(0, Vec::len)
                };
                values.reserve(count)?;
                if let Some(items) = items {
                    values.values.extend(items.iter().map(Some));
                } else {
                    let length = add(values.values.len(), count)?;
                    values.values.resize(length, None);
                }
                offsets.values.push(values.values.len());
            }
            let child = construct(child, &values.values, memory, allocator)?;
            let validity = validity(dtype, cells, allocator)?;
            if let DType::FixedSizeList(_, size, _) = dtype {
                FixedSizeListArray::try_new(child, *size, validity, cells.len())
                    .map(vortex::array::IntoArray::into_array)
                    .map_err(vortex_error)
            } else {
                let offsets = result_batch::build_column(
                    &DType::Primitive(PType::U64, Nullability::NonNullable),
                    offsets.values.len(),
                    allocator,
                    |row| Ok(Value::UInt(offsets.values[row] as u64)),
                )?;
                ListArray::try_new(child, offsets, validity)
                    .map(vortex::array::IntoArray::into_array)
                    .map_err(vortex_error)
            }
        }
        _ => {
            result_batch::build_column(dtype, cells.len(), allocator, |row| leaf(dtype, cells[row]))
        }
    }
}

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
