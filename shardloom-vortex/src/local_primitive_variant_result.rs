//! The pinned provider's generic Variant builder is unfinished. Its admitted
//! Chunked/Constant encodings preserve mixed native scalars and round-trip through
//! the native file edition. The metadata grant travels with the required offsets.

use super::{
    ArrayRef, Buffer, DType, HostAllocatorRef, PrimitiveArray, Result, Validity, Value, add,
    allocate, failed, multiply, vortex_error,
};
use shardloom_exec::live_memory::LiveMemoryPool;
use vortex::array::{
    IntoArray,
    arrays::{ChunkedArray, ConstantArray, chunked::ChunkedSlots},
    dtype::Nullability,
    scalar::Scalar,
};

const METADATA_PER_VALUE: usize = 1024;

fn metadata_bytes(rows: usize) -> Result<usize> {
    add(multiply(rows, METADATA_PER_VALUE)?, 4096)
}

pub(super) fn footprint<'a>(
    rows: usize,
    value: &mut impl FnMut(usize) -> Result<Value<'a>>,
) -> Result<usize> {
    let mut bytes = add(metadata_bytes(rows)?, multiply(add(rows, 1)?, 8)?)?;
    for row in 0..rows {
        bytes = add(
            bytes,
            match value(row)? {
                Value::Text(text) => text.len(),
                Value::SharedText(text) => text.len(),
                _ => 8,
            },
        )?;
    }
    Ok(bytes)
}

pub(super) fn build<'a>(
    dtype: &DType,
    rows: usize,
    allocator: &HostAllocatorRef,
    memory: &LiveMemoryPool,
    value: &mut impl FnMut(usize) -> Result<Value<'a>>,
) -> Result<ArrayRef> {
    let metadata = memory.reserve(u64::try_from(metadata_bytes(rows)?).map_err(vortex_error)?)?;
    let mut offsets = allocate(allocator, multiply(add(rows, 1)?, 8)?, 8)?;
    for (index, bytes) in offsets
        .as_mut_slice()
        .as_chunks_mut::<8>()
        .0
        .iter_mut()
        .enumerate()
    {
        bytes.copy_from_slice(&u64::try_from(index).map_err(vortex_error)?.to_ne_bytes());
    }
    let mut chunks = Vec::new();
    chunks.try_reserve_exact(rows).map_err(vortex_error)?;
    for row in 0..rows {
        let value = value(row)?;
        let scalar = if matches!(value, Value::Null) {
            if !dtype.is_nullable() {
                return Err(failed("null in a nonnullable variant column"));
            }
            Scalar::null(dtype.clone())
        } else {
            Scalar::variant(scalar(value, allocator)?)
                .cast(dtype)
                .map_err(vortex_error)?
        };
        chunks.push(ConstantArray::new(scalar, 1).into_array());
    }
    let array = ChunkedArray::try_new(chunks, dtype.clone()).map_err(vortex_error)?;
    let mut parts = array
        .try_into_parts()
        .map_err(|_| failed("new variant storage was unexpectedly shared"))?;
    let offsets = crate::owned_buffers::retain_credit(offsets.freeze(), metadata);
    parts.slots[ChunkedSlots::CHUNK_OFFSETS] = Some(
        PrimitiveArray::new(
            Buffer::<u64>::from_byte_buffer(offsets),
            Validity::NonNullable,
        )
        .into_array(),
    );
    ChunkedArray::try_from_parts(parts)
        .map(IntoArray::into_array)
        .map_err(vortex_error)
}

fn scalar(value: Value<'_>, allocator: &HostAllocatorRef) -> Result<Scalar> {
    Ok(match value {
        Value::Bool(value) => Scalar::from(value),
        Value::Int(value) => Scalar::from(value),
        Value::UInt(value) => Scalar::from(value),
        Value::Float(value) => Scalar::from(value),
        value @ (Value::Text(_) | Value::SharedText(_)) => {
            let text = value
                .as_text()
                .ok_or_else(|| failed("variant text is absent"))?;
            let mut bytes = allocate(allocator, text.len(), 1)?;
            bytes.as_mut_slice().copy_from_slice(text.as_bytes());
            let text =
                vortex::buffer::BufferString::try_from(bytes.freeze()).map_err(vortex_error)?;
            Scalar::utf8(text, Nullability::NonNullable)
        }
        Value::Null => return Err(failed("variant null must preserve outer nullability")),
        Value::Binary(_) | Value::SharedBinary(_) | Value::Decimal(..) => {
            return Err(failed("binary and decimal Variant state is not admitted"));
        }
    })
}
