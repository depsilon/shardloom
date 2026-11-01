// SPDX-License-Identifier: Apache-2.0
//! Capacity admission around the pinned two-array dictionary concatenation.
//!
//! Arrow remains the format provider. These bounds follow its specialized
//! concat branches and MutableArrayData construction, including each dense
//! union child's row preallocation and repeated list/view payloads. They are
//! conservative named capacities, not minimum required memory or process RSS.

use super::{
    DecoderMemory, DecoderMemoryReservation, array_structure,
    decoder_memory::{aligned_capacity, invalid},
    owned_record_batch::attach_array,
    schema_owned::{add, times},
};
use arrow_array::ArrayRef;
use arrow_data::{ArrayData, transform::MutableArrayData};
use arrow_schema::{ArrowError, DataType, UnionMode};
use std::{mem::size_of, sync::Arc};

#[cfg(test)]
#[path = "dictionary_delta_tests.rs"]
mod tests;

#[derive(Default)]
struct Capacity {
    retained: usize,
    temporary: usize,
}

impl Capacity {
    fn include(&mut self, other: Self) -> Result<(), ArrowError> {
        self.retained = add(self.retained, other.retained)?;
        self.temporary = add(self.temporary, other.temporary)?;
        Ok(())
    }

    fn buffer(&mut self, bytes: usize) -> Result<(), ArrowError> {
        // Vec/MutableBuffer growth is bounded by twice the requested extent
        // (plus 64-byte rounding). Keep old/new growth overlap separate.
        let capacity = times(aligned_capacity(bytes)?, 2)?;
        self.retained = add(self.retained, capacity)?;
        self.temporary = add(self.temporary, capacity)?;
        Ok(())
    }
}

fn maximum_span(data: &ArrayData, large: bool, view: bool) -> Result<usize, ArrowError> {
    macro_rules! scan {
        ($t:ty) => {{
            let values = data.buffer::<$t>(usize::from(view));
            let mut maximum = 0;
            for index in 0..data.len() {
                let length = if view {
                    values[index]
                } else {
                    values[index + 1]
                        .checked_sub(values[index])
                        .ok_or_else(|| invalid("IPC delta offset overflow"))?
                };
                maximum = maximum
                    .max(usize::try_from(length).map_err(|_| invalid("negative IPC delta span"))?);
            }
            Ok(maximum)
        }};
    }
    if large { scan!(i64) } else { scan!(i32) }
}

fn key_max(dtype: &DataType) -> Result<usize, ArrowError> {
    Ok(match dtype {
        DataType::Int8 => i8::MAX as usize,
        DataType::UInt8 => u8::MAX as usize,
        DataType::Int16 => i16::MAX as usize,
        DataType::UInt16 => u16::MAX as usize,
        DataType::Int32 => i32::MAX as usize,
        DataType::UInt32 => usize::try_from(u32::MAX).unwrap_or(usize::MAX),
        DataType::Int64 => usize::try_from(i64::MAX).unwrap_or(usize::MAX),
        DataType::UInt64 => usize::MAX,
        _ => return Err(invalid("invalid IPC dictionary key type")),
    })
}

/// `rows` bounds produced slots; `preallocated` also covers unused initial
/// child capacities in generic concat. `repeated` means arbitrary input rows
/// can be selected more than once (dense unions and nested list views).
fn capacity(
    left: &ArrayData,
    right: &ArrayData,
    rows: usize,
    preallocated: usize,
    repeated: bool,
    generic: bool,
    depth: usize,
) -> Result<Capacity, ArrowError> {
    let dtype = left.data_type();
    if dtype != right.data_type() {
        return Err(invalid("IPC delta value types differ"));
    }
    let slots = rows.max(preallocated);
    let variadic = if matches!(dtype, DataType::Utf8View | DataType::BinaryView) {
        add(
            left.buffers().len().saturating_sub(1),
            right.buffers().len().saturating_sub(1),
        )?
    } else {
        0
    };
    if variadic > u32::MAX as usize {
        return Err(invalid("IPC delta view buffer index overflow"));
    }
    let structure = array_structure::node(dtype, rows, variadic, depth)?;
    let mut result = Capacity {
        retained: structure.retained,
        temporary: structure.temporary,
    };
    // MutableArrayData's object, two input-reference/extension vectors and their
    // captured closures. The largest captured shape is bounded by ArrayData
    // plus buffer/indices. Specialized branches need fewer structures.
    result.temporary = add(
        result.temporary,
        times(
            8,
            size_of::<MutableArrayData<'_>>() + size_of::<ArrayData>() + 4 * size_of::<usize>(),
        )?,
    )?;
    if !matches!(
        dtype,
        DataType::Null | DataType::Union(_, _) | DataType::RunEndEncoded(_, _)
    ) {
        result.buffer(slots.div_ceil(8))?;
    }
    match dtype {
        DataType::Null => {}
        DataType::Boolean => result.buffer(slots.div_ceil(8))?,
        DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary => {
            let large = matches!(dtype, DataType::LargeUtf8 | DataType::LargeBinary);
            let payload = if repeated {
                times(
                    rows,
                    maximum_span(left, large, false)?.max(maximum_span(right, large, false)?),
                )?
            } else {
                add(left.buffers()[1].len(), right.buffers()[1].len())?
            };
            if !large && payload > i32::MAX as usize {
                return Err(invalid("IPC delta byte offsets exceed i32 capacity"));
            }
            result.buffer(times(add(slots, 1)?, if large { 8 } else { 4 })?)?;
            result.buffer(payload.max(if generic { preallocated } else { 0 }))?;
        }
        DataType::Utf8View | DataType::BinaryView => {
            result.buffer(times(slots, 16)?)?;
            // Variadic payloads are shared, not newly copied. node() charges
            // their Buffer handles. attach_array preserves their old owners.
        }
        DataType::FixedSizeBinary(width) => {
            let width =
                usize::try_from(*width).map_err(|_| invalid("negative IPC binary width"))?;
            result.buffer(times(slots, width)?)?;
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::Map(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_) => {
            let large = matches!(dtype, DataType::LargeList(_) | DataType::LargeListView(_));
            let view = matches!(dtype, DataType::ListView(_) | DataType::LargeListView(_));
            result.buffer(times(
                if view { slots } else { add(slots, 1)? },
                if large { 8 } else { 4 },
            )?)?;
            if view {
                result.buffer(times(slots, if large { 8 } else { 4 })?)?;
            }
            let left_child = &left.child_data()[0];
            let right_child = &right.child_data()[0];
            let child_rows = if repeated || (view && generic) {
                times(
                    rows,
                    maximum_span(left, large, view)?.max(maximum_span(right, large, view)?),
                )?
            } else {
                add(left_child.len(), right_child.len())?
            };
            if !large && child_rows > i32::MAX as usize {
                return Err(invalid("IPC delta list offsets exceed i32 capacity"));
            }
            result.include(capacity(
                left_child,
                right_child,
                child_rows,
                if generic { slots } else { 0 },
                repeated || (view && generic),
                generic,
                depth + 1,
            )?)?;
        }
        DataType::FixedSizeList(_, width) => {
            let width = usize::try_from(*width).map_err(|_| invalid("negative IPC list width"))?;
            result.include(capacity(
                &left.child_data()[0],
                &right.child_data()[0],
                times(rows, width)?,
                times(slots, width)?.max(add(
                    left.child_data()[0].len(),
                    right.child_data()[0].len(),
                )?),
                repeated,
                true,
                depth + 1,
            )?)?;
        }
        DataType::Struct(_) => {
            for (a, b) in left.child_data().iter().zip(right.child_data()) {
                result.include(capacity(
                    a,
                    b,
                    rows,
                    if generic { slots } else { 0 },
                    repeated,
                    generic,
                    depth + 1,
                )?)?;
            }
        }
        DataType::Union(_, mode) => {
            result.buffer(slots)?;
            if *mode == UnionMode::Dense {
                if rows > i32::MAX as usize {
                    return Err(invalid("IPC delta union offsets exceed i32 capacity"));
                }
                result.buffer(times(slots, 4)?)?;
            }
            for (a, b) in left.child_data().iter().zip(right.child_data()) {
                // MutableArrayData reserves N rows for EVERY dense child,
                // including empty children, before looking at selected ids.
                let child_rows = if a.is_empty() && b.is_empty() {
                    0
                } else {
                    rows
                };
                result.include(capacity(
                    a,
                    b,
                    child_rows,
                    slots,
                    repeated || *mode == UnionMode::Dense,
                    true,
                    depth + 1,
                )?)?;
            }
        }
        DataType::Dictionary(key, value) => {
            result.buffer(times(
                slots,
                key.primitive_width()
                    .ok_or_else(|| invalid("invalid IPC dictionary key width"))?,
            )?)?;
            let a = &left.child_data()[0];
            let b = &right.child_data()[0];
            let values = add(a.len(), b.len())?;
            let merge_possible = !generic
                && (value.is_primitive()
                    || matches!(
                        value.as_ref(),
                        DataType::Utf8
                            | DataType::Binary
                            | DataType::LargeUtf8
                            | DataType::LargeBinary
                    ));
            if !a.ptr_eq(b)
                && !merge_possible
                && (values.saturating_sub(1) > key_max(key)? || a.len() > key_max(key)?)
            {
                // MutableArrayData otherwise panics while creating its key
                // extension closure; reject before entering that provider API.
                return Err(ArrowError::DictionaryKeyOverflowError);
            }
            result.include(capacity(a, b, values, values, false, true, depth + 1)?)?;
            if merge_possible {
                // Both-domain masks, masked refs, interleave indices, key maps
                // and <=2*(V+128) interner buckets coexist with output values.
                let buckets = times(add(values, 128)?, 2)?;
                let bucket_width = size_of::<Option<(Option<&[u8]>, u64)>>();
                let per_value = size_of::<(usize, Option<&[u8]>)>()
                    + size_of::<(usize, usize)>()
                    + size_of::<u64>();
                result.temporary = add(
                    result.temporary,
                    add(times(buckets, bucket_width)?, times(values, per_value)?)?,
                )?;
                result.temporary = add(
                    result.temporary,
                    add(
                        aligned_capacity(a.len().div_ceil(8))?,
                        aligned_capacity(b.len().div_ceil(8))?,
                    )?,
                )?;
            }
        }
        DataType::RunEndEncoded(ends, _) => {
            if rows > key_max(ends.data_type())? {
                return Err(invalid("IPC delta run end overflow"));
            }
            let physical = if repeated {
                rows
            } else {
                add(left.child_data()[0].len(), right.child_data()[0].len())?
            };
            let reserved = if generic { slots } else { 0 };
            result.include(capacity(
                &left.child_data()[0],
                &right.child_data()[0],
                physical,
                reserved,
                repeated,
                generic,
                depth + 1,
            )?)?;
            result.include(capacity(
                &left.child_data()[1],
                &right.child_data()[1],
                physical,
                reserved,
                repeated,
                generic,
                depth + 1,
            )?)?;
            result.temporary = add(result.temporary, times(3, size_of::<i64>())?)?;
        }
        _ => result.buffer(times(
            slots,
            dtype
                .primitive_width()
                .ok_or_else(|| invalid("unsupported IPC delta value type"))?,
        )?)?,
    }
    Ok(result)
}

pub(super) fn concatenate(
    left: &ArrayRef,
    right: &ArrayRef,
    memory: &dyn DecoderMemory,
) -> Result<(ArrayRef, Arc<dyn DecoderMemoryReservation>), ArrowError> {
    if left.data_type() != right.data_type() {
        return Err(invalid("IPC delta value types differ"));
    }
    // ArrayData conversion itself allocates metadata; borrow concrete arrays
    // to admit that inspection before materializing any inspection tree.
    let left_shape = array_structure::array(left.as_ref(), 1)?;
    let right_shape = array_structure::array(right.as_ref(), 1)?;
    let inspection = memory.reserve(add(
        add(left_shape.retained, left_shape.temporary)?,
        add(right_shape.retained, right_shape.temporary)?,
    )?)?;
    let left_data = left.to_data();
    let right_data = right.to_data();
    let forecast = capacity(
        &left_data,
        &right_data,
        add(left.len(), right.len())?,
        0,
        false,
        false,
        1,
    )?;
    let credit: Arc<dyn DecoderMemoryReservation> = Arc::from(memory.reserve(forecast.retained)?);
    let temporary = memory.reserve(forecast.temporary)?;
    let combined = arrow_select::concat::concat(&[left.as_ref(), right.as_ref()])?;
    let output = attach_array(&combined, &credit, &[&left_data, &right_data])?;
    drop(combined);
    drop(left_data);
    drop(right_data);
    drop(temporary);
    drop(inspection);
    Ok((output, credit))
}
