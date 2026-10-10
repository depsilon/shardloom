//! Compact generated input; only admitted scan intervals acquire value buffers.

use std::{ops::Range, sync::Arc};

use super::{
    MemorySourceBounds, MemorySourceOwner, ResidentMemorySource, fixed_bytes, memory_error,
    native_error,
};
use crate::resident_session::ResidentVortexSession;
use shardloom_core::Result;
use shardloom_exec::live_memory::MemoryLease;
use vortex::{
    array::{
        ArrayRef, IntoArray as _,
        arrays::{ChunkedArray, PrimitiveArray, StructArray},
        dtype::{DType, FieldNames, Nullability, PType},
        validity::Validity,
    },
    buffer::Buffer,
    encodings::sequence::Sequence,
};

/// The compact arrays are private source metadata. A generated value batch owns
/// a separate structural lease that follows its allocator-owned buffer aliases.
pub(super) struct GeneratedInt64Range {
    start: i64,
    step: i64,
    _metadata: MemoryLease,
}

pub(super) fn source(
    session: &ResidentVortexSession,
    name: &str,
    start: i64,
    step: i64,
    rows: usize,
) -> Result<ResidentMemorySource> {
    if name.is_empty() || name.len() > 256 || step == 0 {
        return Err(memory_error(
            "range requires a valid field and nonzero step",
        ));
    }
    let input_logical_bytes = rows
        .checked_mul(std::mem::size_of::<i64>())
        .and_then(|bytes| bytes.checked_add(name.len()))
        .ok_or_else(|| memory_error("range logical byte count overflow"))?;
    if rows > 0 {
        value_at(start, step, rows - 1)?;
    }
    // At most two Sequence children, their small offset array, field metadata
    // and this owner are covered before constructing the native representation.
    let metadata = session.memory().reserve(4096)?;
    let values = encoded_values(start, step, rows)?;
    let array = StructArray::try_new(
        FieldNames::from(vec![name]),
        vec![values],
        rows,
        Validity::NonNullable,
    )
    .map_err(native_error)?
    .into_array();
    Ok(ResidentMemorySource(Arc::new(MemorySourceOwner {
        array,
        session: session.clone(),
        bounds: MemorySourceBounds {
            max_input_rows: rows.max(1),
            max_input_bytes: input_logical_bytes.max(1),
            max_output_rows: usize::MAX,
            ..MemorySourceBounds::default()
        },
        input_logical_bytes,
        intake_payload_bytes_copied: 0,
        metadata: None,
        is_batch: false,
        generated_range: Some(GeneratedInt64Range {
            start,
            step,
            _metadata: metadata,
        }),
    })))
}

fn value_at(start: i64, step: i64, index: usize) -> Result<i64> {
    i64::try_from(i128::from(start) + i128::from(step) * index as i128)
        .map_err(|_| memory_error("signed range endpoint overflow"))
}

fn encoded_values(start: i64, step: i64, rows: usize) -> Result<ArrayRef> {
    if rows == 0 {
        return Ok(PrimitiveArray::new(Buffer::<i64>::empty(), Validity::NonNullable).into_array());
    }
    // Vortex validates index * step before adding the base. A valid signed
    // endpoint can cross that intermediate limit, so start a second sequence
    // with its own exact base instead of rejecting that range or wrapping it.
    let maximum_offset = if step > 0 {
        i64::MAX as u128
    } else {
        u128::from(i64::MIN.unsigned_abs())
    };
    let maximum_index = (maximum_offset / u128::from(step.unsigned_abs())).min(i64::MAX as u128);
    let chunk_rows = usize::try_from(maximum_index + 1).unwrap_or(usize::MAX);
    let first_rows = rows.min(chunk_rows);
    let first = Sequence::try_new_typed(start, step, Nullability::NonNullable, first_rows)
        .map_err(native_error)?
        .into_array();
    if first_rows == rows {
        return Ok(first);
    }
    let second = Sequence::try_new_typed(
        value_at(start, step, first_rows)?,
        step,
        Nullability::NonNullable,
        rows - first_rows,
    )
    .map_err(native_error)?
    .into_array();
    ChunkedArray::try_new(
        [first, second],
        DType::Primitive(PType::I64, Nullability::NonNullable),
    )
    .map(vortex::array::IntoArray::into_array)
    .map_err(native_error)
}

impl GeneratedInt64Range {
    pub(super) fn materialize(
        &self,
        source: &MemorySourceOwner,
        range: Range<usize>,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<ArrayRef> {
        check()?;
        if range.start > range.end || range.end > source.array.len() {
            return Err(memory_error(
                "generated range scan is outside its declared rows",
            ));
        }
        let rows = range.len();
        let metadata = Arc::new(source.session.memory().reserve(2048)?);
        let allocator =
            crate::owned_buffers::with_shared_credit(source.session.native_allocator(), metadata);
        // Do not call the general Sequence decoder: its BufferMut allocation
        // bypasses the session host allocator in the pinned Vortex provider.
        let bytes = fixed_bytes(&allocator, rows, |offset| {
            if offset.is_multiple_of(1024) {
                check()?;
            }
            value_at(self.start, self.step, range.start + offset).map(i64::to_ne_bytes)
        })?;
        let values = PrimitiveArray::new(
            Buffer::<i64>::from_byte_buffer(bytes),
            Validity::NonNullable,
        )
        .into_array();
        let names = source
            .array
            .dtype()
            .as_struct_fields_opt()
            .ok_or_else(|| memory_error("generated range omitted its native schema"))?
            .names()
            .clone();
        let array = StructArray::try_new(names, vec![values], rows, Validity::NonNullable)
            .map_err(native_error)?
            .into_array();
        check()?;
        Ok(array)
    }
}

#[cfg(test)]
#[path = "resident_memory_range_tests.rs"]
mod tests;
