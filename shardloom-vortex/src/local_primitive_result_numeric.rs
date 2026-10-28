//! Owned numeric result copying after native take/cast. Preserve validation
//! order, initialized NULL slots and allocator lifetime without reconstructing
//! every selected value through scalar keys and generic result values.

use super::{
    ArrayRef, DType, HostAllocatorRef, PType, PrimitiveArray, Result, Validity, allocate, failed,
    finish_column, finish_validity, multiply, set_bit, width,
};
use vortex::array::{dtype::NativePType, memory::HostBufferElement};
use vortex::{array::arrays::primitive::PrimitiveArrayExt as _, buffer::ByteBuffer, mask::Mask};

#[cfg(test)]
#[path = "local_primitive_result_numeric_tests.rs"]
mod tests;

/// Write already-admitted numeric values without reconstructing a generic
/// result value for each row. The caller owns arithmetic and finite-value
/// semantics; this owner retains schema, NULL, allocation and lifetime checks.
pub(super) fn build_column<T: NativePType + HostBufferElement>(
    dtype: &DType,
    rows: usize,
    allocator: &HostAllocatorRef,
    mut value: impl FnMut(usize) -> Result<Option<T>>,
) -> Result<ArrayRef> {
    if !matches!(dtype, DType::Primitive(ptype, _) if *ptype == T::PTYPE) {
        return Err(failed(
            "typed numeric construction changed its declared dtype",
        ));
    }
    let mut validity = dtype
        .is_nullable()
        .then(|| allocate(allocator, rows.div_ceil(8), 1))
        .transpose()?;
    let mut data = allocate(allocator, multiply(rows, size_of::<T>())?, align_of::<T>())?;
    for (row, slot) in data
        .as_mut_slice_typed::<T>()
        .map_err(super::vortex_error)?
        .iter_mut()
        .enumerate()
    {
        if let Some(value) = value(row)? {
            *slot = value;
            if let Some(validity) = &mut validity {
                set_bit(validity, row);
            }
        } else if !dtype.is_nullable() {
            return Err(failed("null in a nonnullable output column"));
        }
    }
    let validity = validity
        .map(|buffer| finish_validity(buffer.freeze(), rows, allocator))
        .transpose()?
        .unwrap_or(Validity::NonNullable);
    finish_column(dtype, rows, data.freeze(), validity, None, allocator)
}

pub(super) fn copy_column(
    dtype: &DType,
    values: &PrimitiveArray,
    valid: &Mask,
    allocator: &HostAllocatorRef,
    check_cancelled: impl FnMut() -> Result<()>,
) -> Result<ArrayRef> {
    visit_column::<true>(dtype, values, valid, allocator, check_cancelled)
        .map(|array| array.expect("copy mode constructs an owned column"))
}

/// The same ordered typed checks without allocating or writing result buffers.
pub(super) fn validate_column(
    dtype: &DType,
    values: &PrimitiveArray,
    valid: &Mask,
    allocator: &HostAllocatorRef,
    check_cancelled: impl FnMut() -> Result<()>,
) -> Result<()> {
    visit_column::<false>(dtype, values, valid, allocator, check_cancelled).map(drop)
}

fn visit_column<const COPY: bool>(
    dtype: &DType,
    values: &PrimitiveArray,
    valid: &Mask,
    allocator: &HostAllocatorRef,
    check_cancelled: impl FnMut() -> Result<()>,
) -> Result<Option<ArrayRef>> {
    let temporal = crate::native_payload_schema::temporal_storage(dtype);
    let ptype = match dtype {
        DType::Primitive(ptype, _) if *ptype != PType::F16 => *ptype,
        _ => temporal.ok_or_else(|| failed("numeric copy requires admitted numeric storage"))?,
    };
    if values.ptype() != ptype || valid.len() != values.len() {
        return Err(failed(
            "selected numeric storage changed dtype or row count",
        ));
    }
    let source = values
        .buffer_handle()
        .as_host_opt()
        .ok_or_else(|| failed("numeric copy requires selected host storage"))?;
    #[cfg(all(test, debug_assertions))]
    if COPY {
        super::super::native_relational_observation::record(|counts| {
            counts.numeric_output_copy_batches += 1;
            counts.numeric_output_copy_rows += values.len() as u64;
        });
    }
    macro_rules! visit {
        ($validate:expr) => {
            visit_validated::<COPY>(dtype, source, valid, allocator, $validate, check_cancelled)
        };
    }
    match ptype {
        PType::F32 => {
            let values = values.as_slice::<f32>();
            visit!(|row| finite(values[row].is_finite()))
        }
        PType::F64 => {
            let values = values.as_slice::<f64>();
            visit!(|row| finite(values[row].is_finite()))
        }
        PType::I32 if temporal.is_some() => {
            let values = values.as_slice::<i32>();
            visit!(|row| crate::native_temporal::validate_ticks(dtype, i64::from(values[row])))
        }
        PType::I64 if temporal.is_some() => {
            let values = values.as_slice::<i64>();
            visit!(|row| crate::native_temporal::validate_ticks(dtype, values[row]))
        }
        _ => visit!(|_| Ok(())),
    }
}

pub(super) fn finite(valid: bool) -> Result<()> {
    if valid {
        Ok(())
    } else {
        Err(failed(
            "numeric value differs from its finite declared dtype",
        ))
    }
}

fn visit_validated<const COPY: bool>(
    dtype: &DType,
    source: &ByteBuffer,
    valid: &Mask,
    allocator: &HostAllocatorRef,
    mut validate: impl FnMut(usize) -> Result<()>,
    mut check_cancelled: impl FnMut() -> Result<()>,
) -> Result<Option<ArrayRef>> {
    let rows = valid.len();
    let stride = width(dtype)?;
    let bytes = multiply(rows, stride)?;
    if source.len() != bytes {
        return Err(failed("selected numeric buffer changed byte count"));
    }
    // Keep the existing allocation and refusal order: validity, then values.
    let mut validity = (COPY && dtype.is_nullable())
        .then(|| allocate(allocator, rows.div_ceil(8), 1))
        .transpose()?;
    let mut data = COPY
        .then(|| allocate(allocator, bytes, stride))
        .transpose()?;
    if valid.all_true() {
        if let Some(validity) = validity.as_mut() {
            validity.as_mut_slice().fill(u8::MAX);
            if !rows.is_multiple_of(8) {
                *validity.as_mut_slice().last_mut().expect("partial byte") = (1 << (rows % 8)) - 1;
            }
        }
        for start in (0..rows).step_by(1024) {
            check_cancelled()?;
            let end = rows.min(start.saturating_add(1024));
            for row in start..end {
                validate(row)?;
            }
            if let Some(data) = data.as_mut() {
                data.as_mut_slice()[start * stride..end * stride]
                    .copy_from_slice(&source[start * stride..end * stride]);
            }
        }
    } else {
        for row in 0..rows {
            if row.is_multiple_of(1024) {
                check_cancelled()?;
            }
            if valid.value(row) {
                validate(row)?;
                if let Some(validity) = validity.as_mut() {
                    set_bit(validity, row);
                }
                if let Some(data) = data.as_mut() {
                    let start = row * stride;
                    data.as_mut_slice()[start..start + stride]
                        .copy_from_slice(&source[start..start + stride]);
                }
            } else if !dtype.is_nullable() {
                return Err(failed("null in a nonnullable output column"));
            }
        }
    }
    let Some(data) = data else {
        #[cfg(all(test, debug_assertions))]
        super::super::native_relational_observation::record(|counts| {
            counts.payload_numeric_validation_columns += 1;
            counts.payload_numeric_validation_rows += rows as u64;
        });
        return Ok(None);
    };
    let validity = validity
        .map(|buffer| finish_validity(buffer.freeze(), rows, allocator))
        .transpose()?
        .unwrap_or(Validity::NonNullable);
    finish_column(dtype, rows, data.freeze(), validity, None, allocator).map(Some)
}
